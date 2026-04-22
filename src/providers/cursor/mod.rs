use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use super::utils::diff_line_counts;
use crate::cursor_paths::{cursor_history_path, cursor_state_path};
pub(crate) mod shared;

use crate::db;
use crate::ingest_progress::IngestProgressObserver;
use crate::path_utils::strip_file_scheme;
use shared::{
    CursorBubbleRole, CursorSessionGraph, aggregate_file_edits, build_seed_graph,
    load_cursor_session_graphs_from_rows_with_observer, resolve_tool_call_edits,
};

pub fn plan_composer_rows() -> Result<Vec<String>> {
    let vscdb_path = match cursor_vscdb_path()? {
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
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    if composer_keys.is_empty() {
        return Ok(0);
    }

    let vscdb_path = cursor_vscdb_path()?.with_context(|| {
        "Cursor state.vscdb disappeared between planning and ingest".to_string()
    })?;
    let history_root = match cursor_history_root()? {
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
    // Partition composer rows into (already-ingested, new) by batch-querying
    // `metadata_sessions` once. Already-ingested sessions only need a cheap
    // metadata refresh and can skip the expensive `bubbleId:%` scan entirely.
    let candidate_ids: Vec<&str> = composer_rows
        .iter()
        .map(|(key, _)| key.strip_prefix("composerData:").unwrap_or(key.as_str()))
        .collect();
    let already_present = db::sessions_present(db, &candidate_ids)?;

    let (existing_rows, new_rows): (Vec<_>, Vec<_>) = composer_rows.iter().partition(|(key, _)| {
        let sid = key.strip_prefix("composerData:").unwrap_or(key.as_str());
        already_present.contains(sid)
    });

    let mut total_rows = 0usize;

    if let Some(observer) = progress.as_mut() {
        observer.set_phase("refreshing existing");
    }
    // Fast path: already-ingested sessions. We only need the small composer JSON
    // (`started_at`, `ended_at`, `project_path`, `model_name`) to refresh metadata;
    // we deliberately skip the per-session bubble scan and graph construction.
    for (key, raw) in &existing_rows {
        if let Some(seed) = build_seed_graph(key, raw, source_file)
            && let Err(e) = db::upsert_metadata_session_with_model(
                db,
                "cursor",
                &seed.composer_id,
                seed.project_path.as_deref(),
                seed.started_at.as_deref(),
                seed.ended_at.as_deref().or(seed.started_at.as_deref()),
                Some("cursor"),
                Some("cursor"),
                seed.model_name.as_deref(),
            )
            && verbose
        {
            eprintln!("[cursor] metadata refresh failed: {}", e);
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(key);
        }
    }

    // Slow path: new sessions get the full graph build + ingest.
    if !new_rows.is_empty() {
        if let Some(observer) = progress.as_mut() {
            observer.set_phase("seed graphs");
        }
        let new_row_owned: Vec<(String, String)> = new_rows
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let graphs = match progress.as_mut() {
            Some(obs) => load_cursor_session_graphs_from_rows_with_observer(
                vscdb,
                source_file,
                &new_row_owned,
                Some(&mut **obs),
            )?,
            None => load_cursor_session_graphs_from_rows_with_observer(
                vscdb,
                source_file,
                &new_row_owned,
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
        for (key, _value) in &new_rows {
            let session_id = key.strip_prefix("composerData:").unwrap_or(key.as_str());
            let Some(graph) = by_session.get(session_id) else {
                continue;
            };

            match ingest_session_graph(db, graph, history_index, verbose) {
                Ok(0) => {}
                Ok(n) => total_rows += n,
                Err(e) => {
                    if verbose {
                        eprintln!("[cursor] skipping session: {}", e);
                    }
                }
            }

            if let Some(observer) = progress.as_mut() {
                observer.advance(key);
            }
        }
    }

    tx.commit()?;

    Ok(total_rows)
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
    // Mirrors `db::open_at_home`: honour `PACEFLOW_HOME` first, then `$HOME`.
    let home = std::env::var_os("PACEFLOW_HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)?;
    let app_dir = home.join(".paceflow");
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

fn ingest_session_graph(
    db: &Connection,
    graph: &CursorSessionGraph,
    history_index: &HistoryIndex,
    verbose: bool,
) -> Result<usize> {
    let visible_message_count = graph
        .messages()
        .iter()
        .filter(|bubble| bubble.text.as_deref().is_some_and(|text| !text.is_empty()))
        .count();
    if visible_message_count == 0 {
        return Ok(0);
    }

    let session_id = &graph.composer_id;
    let timestamp = graph.started_at.clone();
    let last_updated = graph.ended_at.clone();
    let project_path = graph.project_path.clone();
    let model_name = graph.model_name.clone();

    if db::session_exists(db, session_id)? {
        db::upsert_metadata_session_with_model(
            db,
            "cursor",
            session_id,
            project_path.as_deref(),
            timestamp.as_deref(),
            last_updated.as_deref().or(timestamp.as_deref()),
            Some("cursor"),
            Some("cursor"),
            model_name.as_deref(),
        )?;
        return Ok(0);
    }

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

    db::begin_session_with_model(
        db,
        "cursor",
        session_id,
        project_path.as_deref(),
        timestamp.as_deref(),
        last_updated.as_deref().or(timestamp.as_deref()),
        Some("cursor"),
        model_name.as_deref(),
    )?;
    let mut written = 1usize;

    for bubble in graph.messages() {
        let role = match bubble.role {
            Some(CursorBubbleRole::User) => "user",
            Some(CursorBubbleRole::Assistant) => "assistant",
            None => continue,
        };
        let text = match &bubble.text {
            Some(text) if !text.is_empty() => text,
            _ => continue,
        };
        let words = text.split_whitespace().count();
        db::ingest_session_message(
            db,
            "cursor",
            session_id,
            role,
            text,
            words as i64,
            timestamp.as_deref(),
        )?;
        written += 1;
    }

    let mut loc_events_pushed = 0usize;
    let resolved_file_edits = aggregate_file_edits(&resolve_tool_call_edits(graph));
    for edit in &resolved_file_edits {
        db::ingest_accepted_code_change(
            db,
            "cursor",
            session_id,
            &edit.abs_path,
            edit.added_lines,
            edit.removed_lines,
            timestamp.as_deref(),
        )?;
        loc_events_pushed += 1;
        written += 1;
    }

    if loc_events_pushed == 0 {
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
            db::ingest_accepted_code_change(
                db,
                "cursor",
                session_id,
                &file_path,
                lines_added,
                lines_removed,
                timestamp.as_deref(),
            )?;
            loc_events_pushed += 1;
            written += 1;
        }
    }

    if loc_events_pushed == 0
        && let (Some(added), Some(removed)) = (graph.total_lines_added, graph.total_lines_removed)
        && (added > 0 || removed > 0)
    {
        db::ingest_accepted_code_change(
            db,
            "cursor",
            session_id,
            "__total__",
            added,
            removed,
            timestamp.as_deref(),
        )?;
        written += 1;
    }

    Ok(written)
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

fn cursor_vscdb_path() -> Result<Option<PathBuf>> {
    cursor_state_path()
}

fn cursor_history_root() -> Result<Option<PathBuf>> {
    cursor_history_path()
}

#[cfg(test)]
mod tests {
    use super::ingest_planned_sessions_from_source;
    use crate::db::init_app_schema;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

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
