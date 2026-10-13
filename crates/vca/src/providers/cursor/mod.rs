use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use super::utils::diff_line_counts;
pub(crate) mod shared;

use crate::db;
use crate::db::session_store::{
    self, AcceptedChangeRecord, AgentKind, MessageRecord, SessionRecord, TokenSource,
    ToolCallRecord, UsageRecord,
};
use crate::ingest_progress::IngestProgressObserver;
use crate::path_utils::strip_file_scheme;
use shared::{
    CursorBubbleRole, CursorSessionGraph, aggregate_file_edits, build_seed_graph,
    hydrate_session_bubbles, load_cursor_session_graphs_from_rows_with_observer,
    resolve_tool_call_edits,
};

pub fn plan_composer_rows() -> Result<Vec<String>> {
    plan_composer_rows_for(crate::app::AppContext::VCA)
}

pub fn plan_composer_rows_for(context: crate::app::AppContext) -> Result<Vec<String>> {
    let vscdb_path = match crate::cursor_paths::cursor_state_path_for(context)? {
        Some(path) => path,
        None => return Ok(Vec::new()),
    };

    let vscdb = Connection::open_with_flags(
        &vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {:?} — is Cursor running?", vscdb_path))?;

    let mut stmt = vscdb.prepare("SELECT key FROM cursorDiskKV WHERE key LIKE 'composerData:%'")?;

    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .collect();

    Ok(rows)
}

pub fn ingest_planned_sessions(
    db: &Connection,
    composer_keys: &[String],
    verbose: bool,
    progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    ingest_planned_sessions_for(
        db,
        composer_keys,
        verbose,
        progress,
        crate::app::AppContext::VCA,
    )
}

pub fn ingest_planned_sessions_for(
    db: &Connection,
    composer_keys: &[String],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
    context: crate::app::AppContext,
) -> Result<usize> {
    if composer_keys.is_empty() {
        return Ok(0);
    }

    let vscdb_path = crate::cursor_paths::cursor_state_path_for(context)?.with_context(|| {
        "Cursor state.vscdb disappeared between planning and ingest".to_string()
    })?;
    let history_root = match crate::cursor_paths::cursor_history_path_for(context)? {
        Some(path) => path,
        None => {
            if verbose {
                eprintln!("[cursor] History directory not found");
            }
            PathBuf::new()
        }
    };

    if verbose {
        eprintln!("[cursor] opening {:?}", vscdb_path);
    }

    if let Some(observer) = progress.as_mut() {
        observer.set_phase("opening vscdb");
    }
    let vscdb = Connection::open_with_flags(
        &vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {:?} — is Cursor running?", vscdb_path))?;

    let history_index = if history_root.as_os_str().is_empty() {
        HashMap::new()
    } else {
        if let Some(observer) = progress.as_mut() {
            observer.set_phase("history index");
        }
        build_history_index(&history_root)
    };

    if verbose {
        eprintln!("[cursor] history index: {} file(s)", history_index.len());
    }

    if let Some(observer) = progress.as_mut() {
        observer.set_phase("composer rows");
    }
    // Planning only captures composer keys so we do not pull every large JSON blob into memory
    // before ingest starts. Values are fetched lazily here when we are ready to process.
    let composer_rows: Vec<(String, String)> = composer_keys
        .iter()
        .filter_map(|key| {
            let value: Option<String> = vscdb
                .query_row(
                    "SELECT value FROM cursorDiskKV WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .ok()
                .flatten();
            value.map(|value| (key.clone(), value))
        })
        .collect();

    ingest_planned_sessions_from_source(
        db,
        &vscdb,
        &vscdb_path.to_string_lossy(),
        &composer_rows,
        &history_index,
        verbose,
        progress,
    )
}

/// Composer-level attributes that are cheap to read from the raw JSON.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComposerAttributes {
    #[serde(default)]
    subagent_composer_ids: Vec<String>,
    #[serde(default)]
    sub_composer_ids: Vec<String>,
    #[serde(default)]
    is_best_of_n_parent: bool,
    created_on_branch: Option<String>,
    name: Option<String>,
    unified_mode: Option<String>,
}

fn composer_attributes(raw: &str) -> ComposerAttributes {
    serde_json::from_str(raw).unwrap_or_default()
}

pub(crate) fn ingest_planned_sessions_from_source(
    db: &Connection,
    vscdb: &Connection,
    source_file: &str,
    composer_rows: &[(String, String)],
    history_index: &HistoryIndex,
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    // Batch all per-session writes into a single sqlite transaction. With stock
    // defaults every small insert would trigger its own fsync, which on Linux
    // dominates wall-clock time for ingest (see paceflow.perf-stat.txt).
    // `unchecked_transaction` lets us take a transaction from a shared `&Connection`;
    // nothing else writes to this connection during ingest.
    let tx = db.unchecked_transaction()?;

    if let Some(observer) = progress.as_mut() {
        observer.set_phase("existing sessions");
    }
    // Composer JSON grows with every message, so its hash is the session
    // fingerprint. Unchanged sessions skip the expensive `bubbleId:%` scan.
    let mut attributes: HashMap<String, ComposerAttributes> = HashMap::new();
    let mut parent_of: HashMap<String, (String, AgentKind)> = HashMap::new();
    let mut fingerprints: HashMap<String, String> = HashMap::new();
    for (key, raw) in composer_rows {
        let session_id = key.strip_prefix("composerData:").unwrap_or(key.as_str());
        let attrs = composer_attributes(raw);
        for child in &attrs.subagent_composer_ids {
            parent_of.insert(child.clone(), (session_id.to_string(), AgentKind::Subagent));
        }
        for child in &attrs.sub_composer_ids {
            let kind = if attrs.is_best_of_n_parent {
                AgentKind::BestOfN
            } else {
                AgentKind::Subagent
            };
            parent_of
                .entry(child.clone())
                .or_insert((session_id.to_string(), kind));
        }
        attributes.insert(session_id.to_string(), attrs);
        fingerprints.insert(
            session_id.to_string(),
            format!("{:x}", md5::compute(raw.as_bytes())),
        );
    }

    let mut changed_rows: Vec<(String, String)> = Vec::new();
    for (key, raw) in composer_rows {
        let session_id = key.strip_prefix("composerData:").unwrap_or(key.as_str());
        let fingerprint = fingerprints.get(session_id).cloned().unwrap_or_default();
        if session_store::source_needs_ingest(db, "cursor", session_id, &fingerprint)? {
            changed_rows.push((key.clone(), raw.clone()));
        } else if let Some(observer) = progress.as_mut() {
            observer.advance(key);
        }
    }

    let mut total_rows = 0usize;
    if !changed_rows.is_empty() {
        if let Some(observer) = progress.as_mut() {
            observer.set_phase("seed graphs");
        }
        let graphs = match progress.as_mut() {
            Some(obs) => load_cursor_session_graphs_from_rows_with_observer(
                vscdb,
                source_file,
                &changed_rows,
                Some(&mut **obs),
            )?,
            None => load_cursor_session_graphs_from_rows_with_observer(
                vscdb,
                source_file,
                &changed_rows,
                None,
            )?,
        };
        let by_session: HashMap<_, _> = graphs
            .into_iter()
            .map(|graph| (graph.composer_id.clone(), graph))
            .collect();

        if let Some(observer) = progress.as_mut() {
            observer.set_phase("writing sessions");
        }
        for (key, _value) in &changed_rows {
            let session_id = key.strip_prefix("composerData:").unwrap_or(key.as_str());
            let fingerprint = fingerprints.get(session_id).cloned().unwrap_or_default();
            if let Some(graph) = by_session.get(session_id) {
                let record = build_session_record(
                    graph,
                    history_index,
                    attributes.get(session_id),
                    parent_of.get(session_id),
                    verbose,
                );
                if let Some(record) = record {
                    match session_store::store_session_record(db, &record) {
                        Ok(written) => total_rows += written,
                        Err(e) => {
                            if verbose {
                                eprintln!("[cursor] skipping session: {}", e);
                            }
                        }
                    }
                }
            }
            session_store::mark_source_ingested(
                db,
                "cursor",
                session_id,
                &fingerprint,
                &[session_id],
            )?;

            if let Some(observer) = progress.as_mut() {
                observer.advance(key);
            }
        }
    }

    total_rows += backfill_cursor_sessions_missing_usage(db, vscdb, source_file, verbose)?;

    tx.commit()?;

    Ok(total_rows)
}

/// Sessions created only via change_intel (`upsert_metadata_session`) never ran the composer ingest path,
/// so they have metadata without `fact_session_usage`. Load `composerData:<id>` when present and estimate tokens.
fn backfill_cursor_sessions_missing_usage(
    db: &Connection,
    vscdb: &Connection,
    source_file: &str,
    verbose: bool,
) -> Result<usize> {
    let mut stmt = db.prepare(
        "SELECT m.session_id FROM metadata_sessions m
         WHERE m.provider = 'cursor'
           AND NOT EXISTS (
             SELECT 1 FROM fact_session_usage u
             WHERE u.provider = 'cursor' AND u.session_id = m.session_id
           )",
    )?;
    let ids: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .collect();

    let mut written = 0usize;
    for session_id in ids {
        let key = format!("composerData:{session_id}");
        let raw: Option<String> = vscdb
            .query_row(
                "SELECT value FROM cursorDiskKV WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        let Some(raw) = raw else {
            continue;
        };
        let Some(mut seed) = build_seed_graph(&key, &raw, source_file) else {
            continue;
        };
        if let Err(e) = hydrate_session_bubbles(vscdb, &mut seed) {
            if verbose {
                eprintln!(
                    "[cursor] orphan bubble hydrate failed for {}: {}",
                    session_id, e
                );
            }
            continue;
        }
        match ingest_cursor_text_usage(
            db,
            &seed,
            seed.started_at.as_deref(),
            seed.model_name.as_deref(),
        ) {
            Ok(n) => written += n,
            Err(e) if verbose => {
                eprintln!(
                    "[cursor] orphan usage backfill failed for {}: {}",
                    session_id, e
                );
            }
            Err(_) => {}
        }
    }
    Ok(written)
}

// ── History index ─────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct HistoryEntry {
    id: String,
    #[serde(default)]
    source: String,
    timestamp: i64,
    dir_path: PathBuf,
}

pub(crate) type HistoryIndex = HashMap<String, Vec<HistoryEntry>>;

/// Current disk format version for the history index cache. Bump when the
/// on-disk shape changes incompatibly.
const HISTORY_CACHE_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct HistoryIndexCache {
    #[serde(rename = "v")]
    version: u32,
    // history_root -> (mtime_ms, size) per-dir signature.
    signatures: HashMap<String, (i128, u64)>,
    // file_path -> entries, matching `HistoryIndex`.
    entries: HashMap<String, Vec<HistoryEntry>>,
}

fn history_cache_path() -> Option<PathBuf> {
    let app_dir = crate::data_home::directory().ok()?;
    // Best-effort; callers tolerate absence.
    std::fs::create_dir_all(&app_dir).ok()?;
    Some(app_dir.join("cursor_history_index.cache.json"))
}

fn dir_signature(entries_json: &Path) -> Option<(i128, u64)> {
    // Signatures just need to be stable; the exact unit doesn't matter. We use
    // signed millis-since-epoch so that clocks which sit before UNIX_EPOCH
    // (some VMs, some embedded devices) still round-trip uniquely.
    let meta = std::fs::metadata(entries_json).ok()?;
    let size = meta.len();
    let modified = meta.modified().ok()?;
    let mtime = match modified.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i128,
        Err(e) => -(e.duration().as_millis() as i128),
    };
    Some((mtime, size))
}

fn collect_dir_paths(history_root: &Path) -> Vec<PathBuf> {
    let Ok(dir_iter) = std::fs::read_dir(history_root) else {
        return Vec::new();
    };
    dir_iter
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect()
}

fn compute_signatures(dir_paths: &[PathBuf]) -> HashMap<String, (i128, u64)> {
    dir_paths
        .iter()
        .filter_map(|dir| {
            let entries_json = dir.join("entries.json");
            let sig = dir_signature(&entries_json)?;
            Some((dir.to_string_lossy().into_owned(), sig))
        })
        .collect()
}

fn load_history_cache(path: &Path) -> Option<HistoryIndexCache> {
    let raw = std::fs::read_to_string(path).ok()?;
    let cache: HistoryIndexCache = serde_json::from_str(&raw).ok()?;
    if cache.version != HISTORY_CACHE_VERSION {
        return None;
    }
    Some(cache)
}

fn save_history_cache(path: &Path, cache: &HistoryIndexCache) {
    // Atomic-ish write: write to tmp, rename over target. Failure is non-fatal -
    // a missing/corrupt cache just forces a rebuild on the next run.
    let Ok(json) = serde_json::to_vec(cache) else {
        return;
    };
    let tmp = path.with_extension("cache.json.tmp");
    if std::fs::write(&tmp, &json).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

fn build_history_index_fresh(dir_paths: &[PathBuf]) -> HistoryIndex {
    use rayon::prelude::*;

    #[derive(Deserialize)]
    struct EntriesFile {
        resource: Option<String>,
        #[serde(default)]
        entries: Vec<RawEntry>,
    }

    #[derive(Deserialize)]
    struct RawEntry {
        id: Option<String>,
        source: Option<String>,
        timestamp: Option<i64>,
    }

    // Reading ~hundreds of small `entries.json` files serially - each with its
    // own open/read/parse - is almost entirely syscall-bound. Rayon lets the
    // kernel service those opens concurrently without any shared state.
    let per_dir: Vec<(String, Vec<HistoryEntry>)> = dir_paths
        .par_iter()
        .filter_map(|dir_path| {
            let entries_json = dir_path.join("entries.json");
            let raw = std::fs::read_to_string(&entries_json).ok()?;
            let parsed: EntriesFile = serde_json::from_str(&raw).ok()?;
            let resource = parsed.resource?;
            let file_path = strip_file_scheme(&resource);

            let mut entries: Vec<HistoryEntry> = parsed
                .entries
                .into_iter()
                .filter_map(|e| {
                    Some(HistoryEntry {
                        id: e.id?,
                        source: e.source.unwrap_or_default(),
                        timestamp: e.timestamp.unwrap_or(0),
                        dir_path: dir_path.clone(),
                    })
                })
                .collect();
            entries.sort_by_key(|e| e.timestamp);
            Some((file_path, entries))
        })
        .collect();

    let mut index: HistoryIndex = HashMap::new();
    for (file_path, entries) in per_dir {
        index.entry(file_path).or_default().extend(entries);
    }

    for v in index.values_mut() {
        v.sort_by_key(|e| e.timestamp);
    }

    index
}

fn build_history_index(history_root: &Path) -> HistoryIndex {
    let dir_paths = collect_dir_paths(history_root);
    if dir_paths.is_empty() {
        return HistoryIndex::new();
    }

    let current_sigs = compute_signatures(&dir_paths);

    // Fast path: cache file exists and every dir's (mtime, size) is unchanged.
    // The Cursor history directory grows by whole new dirs (one per edited file)
    // and rarely mutates existing `entries.json` files, so this hits on almost
    // every repeat run.
    if let Some(cache_path) = history_cache_path() {
        if let Some(cache) = load_history_cache(&cache_path)
            && cache.signatures == current_sigs
        {
            return cache.entries;
        }

        let fresh = build_history_index_fresh(&dir_paths);
        save_history_cache(
            &cache_path,
            &HistoryIndexCache {
                version: HISTORY_CACHE_VERSION,
                signatures: current_sigs,
                entries: fresh.clone(),
            },
        );
        return fresh;
    }

    // No writable home directory - skip the cache, just rebuild.
    build_history_index_fresh(&dir_paths)
}

// ── Session ingestion ─────────────────────────────────────────────────────────

fn build_session_record(
    graph: &CursorSessionGraph,
    history_index: &HistoryIndex,
    attributes: Option<&ComposerAttributes>,
    parent: Option<&(String, AgentKind)>,
    verbose: bool,
) -> Option<SessionRecord> {
    let visible_message_count = graph
        .messages()
        .iter()
        .filter(|bubble| bubble.text.as_deref().is_some_and(|text| !text.is_empty()))
        .count();
    if visible_message_count == 0 {
        return None;
    }

    let session_id = &graph.composer_id;
    let timestamp = graph.started_at.clone();
    let session_start_ms = graph.created_at_ms.unwrap_or(0);
    let session_end_ms = graph.last_updated_at_ms.unwrap_or(i64::MAX);

    // Only log per-session progress in non-TTY mode. In TTY mode the progress
    // bar already shows the current session label, and these eprintln! lines
    // collide with the `\r`-rewritten bar, making it flicker/wrap.
    if verbose && !std::io::stderr().is_terminal() {
        eprintln!(
            "[cursor] ingesting session {} ({})",
            &session_id[..session_id.len().min(8)],
            timestamp.as_deref().unwrap_or("?")
        );
    }

    let mut record = SessionRecord::new("cursor", session_id.clone());
    record.source_path = Some("cursor".to_string());
    record.project_path = graph.project_path.clone();
    record.started_at = timestamp.clone();
    record.ended_at = graph.ended_at.clone().or(timestamp.clone());
    if let Some(attributes) = attributes {
        record.git_branch = attributes.created_on_branch.clone();
        record.title = attributes
            .name
            .clone()
            .filter(|name| !name.trim().is_empty())
            .or(graph.subtitle.clone());
        record.entrypoint = attributes.unified_mode.clone();
    }
    if let Some((parent_id, kind)) = parent {
        record.parent_session_id = Some(parent_id.clone());
        record.agent_kind = *kind;
    }

    for (index, bubble) in graph.messages().iter().enumerate() {
        let role = match bubble.role {
            Some(CursorBubbleRole::User) => "user",
            Some(CursorBubbleRole::Assistant) => "assistant",
            None => continue,
        };
        let text = match &bubble.text {
            Some(text) if !text.is_empty() => text,
            _ => continue,
        };
        // Prefer the bubble's own per-message timestamp; fall back to the
        // session-level composer `createdAt` only when a bubble lacks one
        // (e.g. legacy inline `conversation` rows).
        record.messages.push(MessageRecord {
            key: bubble
                .bubble_id
                .clone()
                .unwrap_or_else(|| format!("conv:{index}")),
            ts: bubble.timestamp.clone().or(timestamp.clone()),
            role: role.to_string(),
            text: text.clone(),
            is_compaction_summary: false,
        });
    }

    record.usage = cursor_usage_records(graph);
    for call in &graph.tool_calls {
        record.tool_calls.push(ToolCallRecord {
            key: format!("{}:{}", call.bubble_id, call.call_id),
            ts: call.timestamp.clone(),
            tool_name: call.name.clone(),
            target: call.path_hints.first().cloned(),
            output_in_context: true,
            status: match call.status.as_deref() {
                Some("completed") => "ok",
                Some("error") | Some("failed") => "error",
                Some("cancelled") | Some("aborted") | Some("rejected") => "rejected",
                _ => "unknown",
            }
            .to_string(),
            ..ToolCallRecord::default()
        });
    }

    let resolved_file_edits = aggregate_file_edits(&resolve_tool_call_edits(graph));
    for edit in &resolved_file_edits {
        // Use the earliest real edit timestamp for this file; only fall back to
        // the session-level timestamp when the edit carried none.
        record.accepted_changes.push(AcceptedChangeRecord {
            abs_path: edit.abs_path.clone(),
            lines_added: edit.added_lines,
            lines_removed: edit.removed_lines,
            ts: edit.timestamp.clone().or(timestamp.clone()),
        });
    }

    if record.accepted_changes.is_empty() {
        let mut fallback_paths: Vec<String> = graph
            .legacy_targets
            .iter()
            .map(|target| target.abs_path.clone())
            .chain(
                graph
                    .partial_targets
                    .iter()
                    .map(|target| target.abs_path.clone()),
            )
            .collect();
        let mut seen = HashSet::new();
        fallback_paths.retain(|path| seen.insert(path.clone()));
        fallback_paths.sort();

        for file_path in fallback_paths {
            let (lines_added, lines_removed) =
                loc_from_history(&file_path, session_start_ms, session_end_ms, history_index);
            record.accepted_changes.push(AcceptedChangeRecord {
                abs_path: file_path,
                lines_added,
                lines_removed,
                ts: timestamp.clone(),
            });
        }
    }

    if record.accepted_changes.is_empty()
        && let (Some(added), Some(removed)) = (graph.total_lines_added, graph.total_lines_removed)
        && (added > 0 || removed > 0)
    {
        record.accepted_changes.push(AcceptedChangeRecord {
            abs_path: "__total__".to_string(),
            lines_added: added,
            lines_removed: removed,
            ts: timestamp.clone(),
        });
    }

    record.model = graph
        .model_name
        .clone()
        .or_else(|| record.primary_model_from_usage());
    Some(record)
}

/// Provider-reported bubble token counts when present; otherwise one
/// text-length estimate for the whole session.
fn cursor_usage_records(graph: &CursorSessionGraph) -> Vec<UsageRecord> {
    let reported: Vec<UsageRecord> = graph
        .messages()
        .iter()
        .filter(|bubble| bubble.input_tokens.is_some() || bubble.output_tokens.is_some())
        .enumerate()
        .map(|(index, bubble)| {
            let mut usage = UsageRecord::new(
                bubble
                    .bubble_id
                    .clone()
                    .unwrap_or_else(|| format!("bubble:{index}")),
            );
            usage.ts = bubble.timestamp.clone().or(graph.started_at.clone());
            usage.model = bubble.model_name.clone().or(graph.model_name.clone());
            usage.input_tokens = bubble.input_tokens.unwrap_or(0);
            usage.output_tokens = bubble.output_tokens.unwrap_or(0);
            usage
        })
        .collect();
    if !reported.is_empty() {
        return reported;
    }

    let mut input_tokens = 0i64;
    let mut output_tokens = 0i64;
    for bubble in graph.events_for_text_token_estimate() {
        let Some(text) = bubble.text.as_deref() else {
            continue;
        };
        let tokens = estimate_tokens_from_text(text);
        match bubble.role {
            Some(CursorBubbleRole::User) => input_tokens += tokens,
            Some(CursorBubbleRole::Assistant) => output_tokens += tokens,
            None => {}
        }
    }
    if input_tokens + output_tokens <= 0 {
        return Vec::new();
    }
    let mut usage = UsageRecord::new("estimate");
    usage.ts = graph.started_at.clone();
    usage.model = graph.model_name.clone();
    usage.input_tokens = input_tokens;
    usage.output_tokens = output_tokens;
    usage.token_source = TokenSource::EstimatedText;
    vec![usage]
}

fn ingest_cursor_text_usage(
    db: &Connection,
    graph: &CursorSessionGraph,
    timestamp: Option<&str>,
    model_name: Option<&str>,
) -> Result<usize> {
    let mut input_tokens = 0i64;
    let mut output_tokens = 0i64;
    for bubble in graph.events_for_text_token_estimate() {
        let Some(text) = bubble.text.as_deref() else {
            continue;
        };
        let tokens = estimate_tokens_from_text(text);
        match bubble.role {
            Some(CursorBubbleRole::User) => input_tokens += tokens,
            Some(CursorBubbleRole::Assistant) => output_tokens += tokens,
            None => {}
        }
    }
    let total_tokens = input_tokens + output_tokens;
    if total_tokens <= 0 {
        return Ok(0);
    }

    db::ingest_session_usage(
        db,
        "cursor",
        &graph.composer_id,
        timestamp,
        model_name,
        input_tokens,
        0,
        0,
        output_tokens,
        0,
        total_tokens,
        None,
        "estimated_text_tokens",
    )?;
    Ok(1)
}

fn estimate_tokens_from_text(text: &str) -> i64 {
    ((text.chars().count() as f64) / 4.0).round().max(0.0) as i64
}

// ── LOC from File History ─────────────────────────────────────────────────────

fn loc_from_history(
    file_path: &str,
    session_start_ms: i64,
    session_end_ms: i64,
    index: &HistoryIndex,
) -> (i64, i64) {
    let entries = match index.get(file_path) {
        Some(e) => e,
        None => return (0, 0),
    };

    let mut total_added = 0i64;
    let mut total_removed = 0i64;

    for (i, entry) in entries.iter().enumerate() {
        // Only consider "Undo *" entries (before-states) within the session range
        if !entry.source.starts_with("Undo") {
            continue;
        }
        if entry.timestamp < session_start_ms || entry.timestamp > session_end_ms {
            continue;
        }

        // before-state
        let before_path = entry.dir_path.join(&entry.id);
        let before_content = std::fs::read_to_string(&before_path).unwrap_or_default();

        // after-state: next entry in the sorted list
        let after_content = if let Some(next) = entries.get(i + 1) {
            let after_path = next.dir_path.join(&next.id);
            std::fs::read_to_string(&after_path).unwrap_or_default()
        } else {
            String::new()
        };

        let (added, removed) = diff_line_counts(&before_content, &after_content);
        total_added += added;
        total_removed += removed;
    }

    (total_added, total_removed)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{estimate_tokens_from_text, ingest_planned_sessions_from_source};
    use crate::db::init_app_schema;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[test]
    fn estimates_text_tokens_with_four_chars_per_token() {
        assert_eq!(estimate_tokens_from_text("12345678"), 2);
        assert_eq!(estimate_tokens_from_text("1234567890"), 3);
        assert_eq!(estimate_tokens_from_text(""), 0);
    }

    fn create_cursor_db(rows: &[(String, String)]) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp cursor db should be created");
        let conn = Connection::open(file.path()).expect("cursor temp db should open");
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (
                key   TEXT PRIMARY KEY,
                value TEXT
            );",
        )
        .expect("cursor temp db schema should be created");
        for (key, value) in rows {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .expect("cursor temp db row should insert");
        }
        file
    }

    #[test]
    fn session_ingest_uses_shared_tool_edits_for_latest_style_sessions() {
        let source = create_cursor_db(&[
            (
                "composerData:c_tool".to_string(),
                json!({
                    "composerId": "c_tool",
                    "conversation": [{"type": 1, "text": "refactor these files"}],
                    "filesChangedCount": 2,
                    "totalLinesAdded": 3,
                    "totalLinesRemoved": 2,
                    "originalFileStates": {
                        "file:///tmp/repo/src/plan.ts": {
                            "firstEditBubbleId": "bubble-plan",
                            "content": "old\n"
                        },
                        "file:///tmp/repo/src/assignment.ts": {
                            "firstEditBubbleId": "bubble-assign",
                            "content": "before\n"
                        }
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_tool:bubble-plan".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "edit_file_v2",
                        "toolCallId": "call-plan",
                        "params": "{\"relativeWorkspacePath\":\"/tmp/repo/src/plan.ts\",\"streamingContent\":\"@@\\n-old\\n+new\\n+another\\n\"}"
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_tool:bubble-assign".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "edit_file_v2",
                        "toolCallId": "call-assign",
                        "params": "{\"relativeWorkspacePath\":\"/tmp/repo/src/assignment.ts\",\"streamingContent\":\"@@\\n-before\\n+after\\n\"}"
                    }
                })
                .to_string(),
            ),
        ]);

        let analytics = Connection::open_in_memory().expect("analytics db should open");
        init_app_schema(&analytics).expect("analytics schema should initialize");

        let rows = vec![(
            "composerData:c_tool".to_string(),
            json!({
                "composerId": "c_tool",
                "conversation": [{"type": 1, "text": "refactor these files"}],
                "filesChangedCount": 2,
                "totalLinesAdded": 3,
                "totalLinesRemoved": 2,
                "originalFileStates": {
                    "file:///tmp/repo/src/plan.ts": {
                        "firstEditBubbleId": "bubble-plan",
                        "content": "old\n"
                    },
                    "file:///tmp/repo/src/assignment.ts": {
                        "firstEditBubbleId": "bubble-assign",
                        "content": "before\n"
                    }
                }
            })
            .to_string(),
        )];

        let source_conn = Connection::open(source.path()).expect("cursor temp db should reopen");
        let written = ingest_planned_sessions_from_source(
            &analytics,
            &source_conn,
            source.path().to_string_lossy().as_ref(),
            &rows,
            &HashMap::new(),
            false,
            None,
        )
        .expect("latest-style session should ingest");
        assert!(written >= 3);

        let mut stmt = analytics
            .prepare(
                "SELECT abs_path, lines_added, lines_removed
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='c_tool' AND source_kind='accepted_change'
                 ORDER BY abs_path",
            )
            .expect("accepted change query should prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .expect("accepted change rows should map")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("accepted change rows should collect");

        assert_eq!(
            rows,
            vec![
                ("/tmp/repo/src/assignment.ts".to_string(), 1, 1),
                ("/tmp/repo/src/plan.ts".to_string(), 2, 1),
            ]
        );
    }

    #[test]
    fn session_ingest_uses_shared_apply_patch_edits_for_older_sessions() {
        let source = create_cursor_db(&[
            (
                "composerData:c_patch".to_string(),
                json!({
                    "composerId": "c_patch",
                    "conversation": [{"type": 1, "text": "update the page object"}],
                    "filesChangedCount": 1,
                    "originalFileStates": {
                        "file:///tmp/repo/tests/pages/user-support-po.ts": {
                            "firstEditBubbleId": "bubble-patch",
                            "content": "old line\n"
                        }
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_patch:bubble-patch".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "apply_patch",
                        "toolCallId": "call-patch",
                        "params": "{\"relativeWorkspacePath\":\"tests/pages/user-support-po.ts\"}",
                        "rawArgs": "*** Begin Patch\n*** Update File: tests/pages/user-support-po.ts\n@@\n-old line\n+new line\n*** End Patch"
                    }
                })
                .to_string(),
            ),
        ]);

        let analytics = Connection::open_in_memory().expect("analytics db should open");
        init_app_schema(&analytics).expect("analytics schema should initialize");

        let rows = vec![(
            "composerData:c_patch".to_string(),
            json!({
                "composerId": "c_patch",
                "conversation": [{"type": 1, "text": "update the page object"}],
                "filesChangedCount": 1,
                "originalFileStates": {
                    "file:///tmp/repo/tests/pages/user-support-po.ts": {
                        "firstEditBubbleId": "bubble-patch",
                        "content": "old line\n"
                    }
                }
            })
            .to_string(),
        )];

        let source_conn = Connection::open(source.path()).expect("cursor temp db should reopen");
        ingest_planned_sessions_from_source(
            &analytics,
            &source_conn,
            source.path().to_string_lossy().as_ref(),
            &rows,
            &HashMap::new(),
            false,
            None,
        )
        .expect("older apply_patch session should ingest");

        let row: (String, i64, i64) = analytics
            .query_row(
                "SELECT abs_path, lines_added, lines_removed
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='c_patch' AND source_kind='accepted_change'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("older accepted change should be present");

        assert_eq!(row.0, "/tmp/repo/tests/pages/user-support-po.ts");
        assert_eq!(row.1, 1);
        assert_eq!(row.2, 1);
    }
}
