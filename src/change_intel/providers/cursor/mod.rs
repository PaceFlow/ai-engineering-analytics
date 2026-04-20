use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::change_intel::line_hash::{diff_with_hashes, hash_line};
use crate::change_intel::path_resolver::{detect_repo_root, to_rel_path};
use crate::change_intel::pipeline::ProviderCodeChangeSummary;
use crate::change_intel::storage;
use crate::change_intel::types::{
    ChangeOpCandidate, LineHashCount, LineSide, ParseError, SessionInfo, WriteMode,
};
use crate::cursor_paths::cursor_state_path;
use crate::providers::cursor::shared::{
    CursorSessionGraph, load_cursor_session_graphs, resolve_tool_call_edits,
};
use crate::ingest_progress::IngestProgressObserver;
use crate::path_utils::{normalize_filesystem_path, path_to_string, strip_file_scheme};

const CURSOR_CURSOR_NAMESPACE: &str = "cursor_core_v1";
const INLINE_PARSER_NAME: &str = "cursor_inline_undo_v1";
const PARTIAL_PARSER_NAME: &str = "cursor_partial_fates_v1";
const NEW_SCHEMA_PARSER_NAME: &str = "cursor_new_schema_partial_fates_v1";
const LEGACY_DIFF_PARSER_NAME: &str = "cursor_legacy_code_block_diff_v1";
const LEGACY_CONTENT_PARSER_NAME: &str = "cursor_legacy_code_block_content_v1";
const LEGACY_TOTALS_PARSER_NAME: &str = "cursor_legacy_session_totals_v1";
const REPO_CONTENT_MAX_FILE_BYTES: u64 = 512 * 1024;
const REPO_CONTENT_SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".venv",
    "coverage",
];

#[derive(Debug, Clone)]
struct CandidateSession {
    info: SessionInfo,
    checkpoint_paths: HashSet<String>,
    strong_path_hints: HashSet<String>,
    weak_path_hints: HashSet<String>,
    original_state_contents: HashMap<String, String>,
    total_lines_added: Option<i64>,
    total_lines_removed: Option<i64>,
    partial_targets: Vec<PartialTarget>,
    legacy_targets: Vec<LegacyTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartialTarget {
    partial_id: String,
    abs_path: String,
}

#[derive(Debug, Clone)]
struct NewInlineHint {
    call_id: String,
    abs_path: String,
    timestamp: Option<String>,
    original_text_lines: Vec<String>,
}

#[derive(Debug, Clone)]
struct ResolvedNewSchemaPartial {
    abs_path: String,
    call_id: String,
    timestamp: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RepoContentIndex {
    line_hash_hits: HashMap<String, Vec<(String, i64)>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ContentMatchScore {
    abs_path: String,
    historical_removed_overlap: usize,
    live_added_overlap: usize,
    live_removed_overlap: usize,
    checkpoint_match: usize,
    strong_hint_match: usize,
    weak_hint_match: usize,
    basename_match: usize,
    path_proximity: usize,
}

impl ContentMatchScore {
    fn ranking_tuple(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize) {
        (
            self.historical_removed_overlap,
            self.live_added_overlap,
            self.live_removed_overlap,
            self.checkpoint_match,
            self.strong_hint_match,
            self.weak_hint_match,
            self.basename_match,
            self.path_proximity,
        )
    }

    fn primary_score(&self) -> usize {
        self.historical_removed_overlap * 100
            + self.live_added_overlap * 10
            + self.live_removed_overlap
    }

    fn secondary_score(&self) -> usize {
        self.checkpoint_match * 100
            + self.strong_hint_match * 10
            + self.weak_hint_match * 5
            + self.basename_match * 2
            + self.path_proximity
    }
}

#[derive(Debug, Clone)]
enum ContentResolverDecision {
    Resolved(ResolvedNewSchemaPartial),
    Unresolved { candidate_count: usize },
    NoSignal,
}

#[derive(Debug, Clone)]
struct LegacyDeferredParseError {
    call_id: String,
    timestamp: Option<String>,
    parser_name: &'static str,
    error: String,
}

#[derive(Debug, Clone)]
struct LegacyTarget {
    diff_id: Option<String>,
    abs_path: String,
    version: i32,
    code_block_idx: i32,
    timestamp: Option<String>,
    content: Option<String>,
    bubble_id: Option<String>,
}

impl LegacyTarget {
    fn call_id(&self) -> String {
        self.diff_id
            .clone()
            .or_else(|| self.bubble_id.clone())
            .unwrap_or_else(|| {
                format!(
                    "legacy-content:{}:{}:{}",
                    self.abs_path, self.version, self.code_block_idx
                )
            })
    }

    fn op_index(&self) -> i32 {
        self.version
            .saturating_mul(1000)
            .saturating_add(self.code_block_idx.max(0))
    }
}

#[allow(dead_code)]
pub fn ingest_cursor_code_changes(
    conn: &mut Connection,
    verbose: bool,
) -> Result<ProviderCodeChangeSummary> {
    let Some(vscdb_path) = cursor_vscdb_path()? else {
        return Ok(ProviderCodeChangeSummary {
            provider: "cursor".to_string(),
            ..ProviderCodeChangeSummary::default()
        });
    };

    ingest_cursor_code_changes_from_path(conn, &vscdb_path, verbose)
}

pub fn ingest_cursor_code_changes_from_sources(
    conn: &mut Connection,
    sources: &[PathBuf],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<ProviderCodeChangeSummary> {
    let mut combined = ProviderCodeChangeSummary {
        provider: "cursor".to_string(),
        ..ProviderCodeChangeSummary::default()
    };

    for source in sources {
        let summary = ingest_cursor_code_changes_from_path(conn, source, verbose)?;
        combined.sources_discovered += summary.sources_discovered;
        combined.sources_skipped += summary.sources_skipped;
        combined.tool_calls_inspected += summary.tool_calls_inspected;
        combined.ops_upserted += summary.ops_upserted;
        combined.parse_errors += summary.parse_errors;
        combined.legacy_sessions_considered += summary.legacy_sessions_considered;
        combined.legacy_entries_inspected += summary.legacy_entries_inspected;
        combined.legacy_diff_rows_found += summary.legacy_diff_rows_found;
        combined.legacy_ops_upserted += summary.legacy_ops_upserted;
        combined.legacy_parse_errors += summary.legacy_parse_errors;

        if let Some(observer) = progress.as_mut() {
            observer.advance(&source.to_string_lossy());
        }
    }

    Ok(combined)
}

pub(crate) fn ingest_cursor_code_changes_from_path(
    conn: &mut Connection,
    vscdb_path: &Path,
    verbose: bool,
) -> Result<ProviderCodeChangeSummary> {
    let mut summary = ProviderCodeChangeSummary {
        provider: "cursor".to_string(),
        sources_discovered: 1,
        ..ProviderCodeChangeSummary::default()
    };

    let source_file = vscdb_path.to_string_lossy().to_string();
    let sig = file_signature(vscdb_path)?;

    if let Some((mtime, size)) = sig {
        let cursor = storage::get_ingest_cursor(conn, CURSOR_CURSOR_NAMESPACE, &source_file)?;
        if let Some(cursor) = cursor
            && cursor.file_mtime == mtime
            && cursor.file_size == size
        {
            summary.sources_skipped += 1;
            return Ok(summary);
        }
    }

    let vscdb = Connection::open_with_flags(
        vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let graphs = load_cursor_session_graphs(&vscdb, &source_file)?;
    let graph_by_session: HashMap<String, CursorSessionGraph> = graphs
        .into_iter()
        .map(|graph| (graph.composer_id.clone(), graph))
        .collect();
    let sessions = collect_candidate_sessions_from_graphs(&graph_by_session);

    for session in sessions.values() {
        storage::upsert_change_session(&tx, &session.info)?;
    }

    let mut seen_paths_by_session: HashMap<String, HashSet<String>> = HashMap::new();
    let mut ops = Vec::new();

    ingest_tool_call_writes(
        &sessions,
        &graph_by_session,
        &mut seen_paths_by_session,
        &mut ops,
        &mut summary,
    );

    ingest_inline_undo_rows(
        &tx,
        &source_file,
        &sessions,
        &graph_by_session,
        &mut seen_paths_by_session,
        &mut ops,
        &mut summary,
        verbose,
    )?;

    ingest_partial_fates_fallback(
        &tx,
        &vscdb,
        &source_file,
        &sessions,
        &mut seen_paths_by_session,
        &mut ops,
        &mut summary,
        verbose,
    )?;

    ingest_new_schema_partial_fates(
        &tx,
        &source_file,
        &sessions,
        &graph_by_session,
        &mut seen_paths_by_session,
        &mut ops,
        &mut summary,
        verbose,
    )?;

    ingest_legacy_code_block_fallback(
        &tx,
        &graph_by_session,
        &source_file,
        &sessions,
        &mut seen_paths_by_session,
        &mut ops,
        &mut summary,
        verbose,
    )?;

    let reconcile = storage::reconcile_source_tool_writes(&tx, "cursor", &source_file, &ops)?;
    summary.ops_upserted += reconcile.ops_upserted;

    if let Some((mtime, size)) = sig {
        storage::upsert_ingest_cursor(&tx, CURSOR_CURSOR_NAMESPACE, &source_file, mtime, size)?;
    }

    tx.commit()?;

    Ok(summary)
}

fn collect_candidate_sessions(
    vscdb: &Connection,
    source_file: &str,
) -> Result<HashMap<String, CandidateSession>> {
    let mut out = HashMap::new();

    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'")?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let composer_id = parsed
            .get("composerId")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned)
            .or_else(|| key.strip_prefix("composerData:").map(ToOwned::to_owned));
        let Some(composer_id) = composer_id else {
            continue;
        };

        if !is_candidate_session(&parsed) {
            continue;
        }

        let last_seen_at = parsed
            .get("lastUpdatedAt")
            .and_then(|v| v.as_i64())
            .and_then(ms_to_iso);

        let partial_targets = extract_partial_targets(&parsed);
        let legacy_targets = extract_legacy_targets(&parsed);

        out.insert(
            composer_id.clone(),
            CandidateSession {
                info: SessionInfo {
                    provider: "cursor".to_string(),
                    session_id: composer_id,
                    source_file: source_file.to_string(),
                    session_cwd: None,
                    last_seen_at,
                },
                checkpoint_paths: HashSet::new(),
                strong_path_hints: extract_session_strong_path_hints(&parsed),
                weak_path_hints: extract_session_weak_path_hints(&parsed),
                original_state_contents: extract_original_state_contents(&parsed),
                total_lines_added: parsed.get("totalLinesAdded").and_then(|v| v.as_i64()),
                total_lines_removed: parsed.get("totalLinesRemoved").and_then(|v| v.as_i64()),
                partial_targets,
                legacy_targets,
            },
        );
    }

    Ok(out)
}

fn collect_candidate_sessions_from_graphs(
    graphs: &HashMap<String, CursorSessionGraph>,
) -> HashMap<String, CandidateSession> {
    let mut sessions = HashMap::new();

    for (session_id, graph) in graphs {
        if !graph.is_candidate_edit_session() {
            continue;
        }

        let original_state_contents = graph
            .original_file_states
            .iter()
            .filter_map(|(path, state)| {
                state
                    .content
                    .as_ref()
                    .map(|content| (path.clone(), content.clone()))
            })
            .collect();

        sessions.insert(
            session_id.clone(),
            CandidateSession {
                info: SessionInfo {
                    provider: "cursor".to_string(),
                    session_id: session_id.clone(),
                    source_file: graph.source_file.clone(),
                    session_cwd: None,
                    last_seen_at: graph.last_seen_at(),
                },
                checkpoint_paths: graph.checkpoint_paths.clone(),
                strong_path_hints: graph.strong_path_hints.clone(),
                weak_path_hints: graph.weak_path_hints.clone(),
                original_state_contents,
                total_lines_added: graph.total_lines_added,
                total_lines_removed: graph.total_lines_removed,
                partial_targets: graph
                    .partial_targets
                    .iter()
                    .map(|target| PartialTarget {
                        partial_id: target.partial_id.clone(),
                        abs_path: target.abs_path.clone(),
                    })
                    .collect(),
                legacy_targets: graph
                    .legacy_targets
                    .iter()
                    .map(|target| LegacyTarget {
                        diff_id: target.diff_id.clone(),
                        abs_path: target.abs_path.clone(),
                        version: target.version,
                        code_block_idx: target.code_block_idx,
                        timestamp: target.timestamp.clone(),
                        content: target.content.clone(),
                        bubble_id: target.bubble_id.clone(),
                    })
                    .collect(),
            },
        );
    }

    sessions
}

fn populate_checkpoint_paths(
    vscdb: &Connection,
    sessions: &mut HashMap<String, CandidateSession>,
) -> Result<()> {
    if sessions.is_empty() {
        return Ok(());
    }

    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'checkpointId:%'")?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let Some(composer_id) = composer_id_from_checkpoint_key(&key) else {
            continue;
        };

        let Some(session) = sessions.get_mut(composer_id) else {
            continue;
        };

        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let files = match parsed.get("files").and_then(|v| v.as_array()) {
            Some(v) => v,
            None => continue,
        };

        for file in files {
            let abs_path = file.get("uri").and_then(extract_file_path_from_uri_value);
            if let Some(path) = abs_path {
                session.checkpoint_paths.insert(path);
            }
        }
    }

    Ok(())
}

fn extract_session_strong_path_hints(parsed: &Value) -> HashSet<String> {
    let mut paths = HashSet::new();

    if let Some(selections) = parsed
        .get("context")
        .and_then(|v| v.get("fileSelections"))
        .and_then(|v| v.as_array())
    {
        for selection in selections {
            let Some(path) = selection
                .get("uri")
                .and_then(extract_file_path_from_uri_value)
            else {
                continue;
            };
            if !is_cursor_plan_path(&path) {
                paths.insert(path);
            }
        }
    }

    if let Some(attached) = parsed
        .get("allAttachedFileCodeChunksUris")
        .and_then(|v| v.as_array())
    {
        for uri in attached {
            let Some(uri) = uri.as_str() else {
                continue;
            };
            let path = strip_file_scheme(uri);
            if !is_cursor_plan_path(&path) {
                paths.insert(path);
            }
        }
    }

    if let Some(code_block_data) = parsed.get("codeBlockData").and_then(|v| v.as_object()) {
        for file_key in code_block_data.keys() {
            let Some(path) = extract_file_path_from_string(file_key) else {
                continue;
            };
            if !is_cursor_plan_path(&path) {
                paths.insert(path);
            }
        }
    }

    paths
}

fn extract_session_weak_path_hints(parsed: &Value) -> HashSet<String> {
    let mut paths = HashSet::new();

    if let Some(original_states) = parsed.get("originalFileStates").and_then(|v| v.as_object()) {
        for uri in original_states.keys() {
            let path = strip_file_scheme(uri);
            if !is_cursor_plan_path(&path) {
                paths.insert(path);
            }
        }
    }

    paths
}

fn extract_original_state_contents(parsed: &Value) -> HashMap<String, String> {
    let mut contents = HashMap::new();

    let Some(original_states) = parsed.get("originalFileStates").and_then(|v| v.as_object()) else {
        return contents;
    };

    for (uri, state) in original_states {
        let path = strip_file_scheme(uri);
        if is_cursor_plan_path(&path) {
            continue;
        }
        let Some(content) = state.get("content").and_then(|v| v.as_str()) else {
            continue;
        };
        contents.insert(path, content.to_string());
    }

    contents
}

fn ingest_tool_call_writes(
    sessions: &HashMap<String, CandidateSession>,
    graphs: &HashMap<String, CursorSessionGraph>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
) {
    for (session_id, graph) in graphs {
        let Some(session) = sessions.get(session_id) else {
            continue;
        };

        let resolved = resolve_tool_call_edits(graph);
        if resolved.is_empty() {
            continue;
        }

        let resolved_paths = seen_paths_by_session.entry(session_id.clone()).or_default();
        for edit in resolved {
            summary.tool_calls_inspected += 1;
            resolved_paths.insert(edit.abs_path.clone());
            ops.push(build_change_op_candidate(
                session,
                edit.call_id,
                edit.op_index,
                edit.timestamp,
                &edit.abs_path,
                edit.write_mode,
                edit.before_known,
                edit.added_lines,
                edit.removed_lines,
                &edit.parser_name,
                edit.line_hashes,
            ));
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "cursor ingestion threads database handles, mutable accumulators, and source metadata through one helper"
)]
fn ingest_inline_undo_rows(
    conn: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    graphs: &HashMap<String, CursorSessionGraph>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    for (composer_id, graph) in graphs {
        let Some(session) = sessions.get(composer_id) else {
            continue;
        };

        for row in &graph.inline_undo_rows {
            summary.tool_calls_inspected += 1;

            if !session.checkpoint_paths.is_empty()
                && !session.checkpoint_paths.contains(&row.abs_path)
            {
                continue;
            }

            let op =
                match build_inline_change_op(&row.call_id, &row.payload, &row.abs_path, session) {
                    Ok(op) => op,
                    Err(err) => {
                        insert_parse_error(
                            conn,
                            summary,
                            &session.info.session_id,
                            source_file,
                            &row.call_id,
                            row.timestamp
                                .clone()
                                .or_else(|| session.info.last_seen_at.clone()),
                            INLINE_PARSER_NAME,
                            err,
                            verbose,
                        )?;
                        continue;
                    }
                };

            ops.push(op);
            seen_paths_by_session
                .entry(composer_id.clone())
                .or_default()
                .insert(row.abs_path.clone());
        }
    }

    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "cursor fallback ingestion needs shared mutable state plus parser context in one place"
)]
fn ingest_partial_fates_fallback(
    conn: &Connection,
    vscdb: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    for (composer_id, session) in sessions {
        let mut seen_partial_keys: HashSet<String> = HashSet::new();

        for target in &session.partial_targets {
            summary.tool_calls_inspected += 1;

            if seen_paths_by_session
                .get(composer_id)
                .is_some_and(|set| set.contains(&target.abs_path))
            {
                continue;
            }

            let partial_key = format!(
                "codeBlockPartialInlineDiffFates:{}:{}",
                composer_id, target.partial_id
            );
            if !seen_partial_keys.insert(partial_key.clone()) {
                continue;
            }

            let raw_payload: Option<String> = vscdb
                .query_row(
                    "SELECT value FROM cursorDiskKV WHERE key = ?1",
                    params![partial_key],
                    |row| row.get(0),
                )
                .optional()?;

            let Some(raw_payload) = raw_payload else {
                insert_parse_error(
                    conn,
                    summary,
                    composer_id,
                    source_file,
                    &partial_key,
                    session.info.last_seen_at.clone(),
                    PARTIAL_PARSER_NAME,
                    "partial fates key not found".to_string(),
                    verbose,
                )?;
                continue;
            };

            let parsed: Value = match serde_json::from_str(&raw_payload) {
                Ok(v) => v,
                Err(e) => {
                    insert_parse_error(
                        conn,
                        summary,
                        composer_id,
                        source_file,
                        &partial_key,
                        session.info.last_seen_at.clone(),
                        PARTIAL_PARSER_NAME,
                        format!("invalid partial fates JSON: {}", e),
                        verbose,
                    )?;
                    continue;
                }
            };

            let op = match build_partial_fates_op(&partial_key, &parsed, &target.abs_path, session)
            {
                Ok(op) => op,
                Err(err) => {
                    insert_parse_error(
                        conn,
                        summary,
                        composer_id,
                        source_file,
                        &partial_key,
                        session.info.last_seen_at.clone(),
                        PARTIAL_PARSER_NAME,
                        err,
                        verbose,
                    )?;
                    continue;
                }
            };

            let Some(op) = op else {
                continue;
            };

            ops.push(op);

            seen_paths_by_session
                .entry(composer_id.clone())
                .or_default()
                .insert(target.abs_path.clone());
        }
    }

    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "new-schema cursor fallback needs the same shared mutable parser state as the legacy paths"
)]
fn ingest_new_schema_partial_fates(
    conn: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    graphs: &HashMap<String, CursorSessionGraph>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    let inline_hints_by_session = collect_new_inline_hints_from_graphs(graphs, sessions);
    let partials_by_session = collect_new_partial_fates_from_graphs(graphs, sessions);
    let mut repo_content_indexes: HashMap<String, RepoContentIndex> = HashMap::new();

    for (composer_id, session) in sessions {
        let Some(partials) = partials_by_session.get(composer_id) else {
            continue;
        };

        let explicit_partial_ids: HashSet<&str> = session
            .partial_targets
            .iter()
            .map(|target| target.partial_id.as_str())
            .collect();
        let inline_hints = inline_hints_by_session
            .get(composer_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let prior_seen_paths = seen_paths_by_session
            .get(composer_id)
            .cloned()
            .unwrap_or_default();

        let mut grouped_by_path: HashMap<String, (String, Option<String>, Vec<(String, Value)>)> =
            HashMap::new();
        let mut deferred_errors: HashMap<String, (String, Option<String>)> = HashMap::new();

        for (partial_id, payload) in partials {
            if explicit_partial_ids.contains(partial_id.as_str()) {
                continue;
            }

            summary.tool_calls_inspected += 1;

            match resolve_new_schema_partial_path(
                session,
                partial_id,
                payload,
                inline_hints,
                &prior_seen_paths,
                &mut repo_content_indexes,
            ) {
                Ok(Some(resolved)) => {
                    let entry = grouped_by_path.entry(resolved.abs_path).or_insert_with(|| {
                        (
                            resolved.call_id.clone(),
                            resolved.timestamp.clone(),
                            Vec::new(),
                        )
                    });
                    if entry.1.is_none() {
                        entry.1 = resolved.timestamp.clone();
                    }
                    if entry.0 == *partial_id && resolved.call_id != *partial_id {
                        entry.0 = resolved.call_id;
                    }
                    entry.2.push((partial_id.clone(), payload.clone()));
                }
                Ok(None) => {}
                Err(err) => {
                    deferred_errors
                        .entry(err)
                        .or_insert_with(|| (partial_id.clone(), session.info.last_seen_at.clone()));
                }
            }
        }

        for (abs_path, (call_id, timestamp, grouped_partials)) in grouped_by_path {
            if seen_paths_by_session
                .get(composer_id)
                .is_some_and(|set| set.contains(&abs_path))
            {
                continue;
            }

            let op = match build_new_schema_partial_fates_op(
                &call_id,
                &grouped_partials,
                &abs_path,
                timestamp,
                session,
            ) {
                Ok(op) => op,
                Err(err) => {
                    deferred_errors
                        .entry(err)
                        .or_insert_with(|| (call_id.clone(), session.info.last_seen_at.clone()));
                    continue;
                }
            };

            let Some(op) = op else {
                continue;
            };

            ops.push(op);
            seen_paths_by_session
                .entry(composer_id.clone())
                .or_default()
                .insert(abs_path);
        }

        for (error, (call_id, timestamp)) in deferred_errors {
            insert_parse_error(
                conn,
                summary,
                composer_id,
                source_file,
                &call_id,
                timestamp,
                NEW_SCHEMA_PARSER_NAME,
                error,
                verbose,
            )?;
        }
    }

    Ok(())
}

fn collect_new_inline_hints_from_graphs(
    graphs: &HashMap<String, CursorSessionGraph>,
    sessions: &HashMap<String, CandidateSession>,
) -> HashMap<String, Vec<NewInlineHint>> {
    let mut out: HashMap<String, Vec<NewInlineHint>> = HashMap::new();
    for (composer_id, graph) in graphs {
        if !sessions.contains_key(composer_id) {
            continue;
        }
        out.insert(
            composer_id.clone(),
            graph
                .inline_hints
                .iter()
                .map(|hint| NewInlineHint {
                    call_id: hint.call_id.clone(),
                    abs_path: hint.abs_path.clone(),
                    timestamp: hint.timestamp.clone(),
                    original_text_lines: hint.original_text_lines.clone(),
                })
                .collect(),
        );
    }

    out
}

fn collect_new_partial_fates_from_graphs(
    graphs: &HashMap<String, CursorSessionGraph>,
    sessions: &HashMap<String, CandidateSession>,
) -> HashMap<String, Vec<(String, Value)>> {
    let mut out: HashMap<String, Vec<(String, Value)>> = HashMap::new();
    for (composer_id, graph) in graphs {
        if !sessions.contains_key(composer_id) {
            continue;
        }
        out.insert(
            composer_id.clone(),
            graph
                .partial_fates
                .iter()
                .map(|(partial_id, payload)| (partial_id.clone(), payload.clone()))
                .collect(),
        );
    }

    out
}

fn resolve_new_schema_partial_path(
    session: &CandidateSession,
    partial_id: &str,
    payload: &Value,
    inline_hints: &[NewInlineHint],
    prior_seen_paths: &HashSet<String>,
    repo_content_indexes: &mut HashMap<String, RepoContentIndex>,
) -> std::result::Result<Option<ResolvedNewSchemaPartial>, String> {
    let (added_lines, removed_lines) = accepted_partial_fate_lines(payload)?;
    if added_lines.is_empty() && removed_lines.is_empty() {
        return Ok(None);
    }

    let inline_candidates: Vec<&NewInlineHint> = inline_hints
        .iter()
        .filter(|hint| !prior_seen_paths.contains(&hint.abs_path))
        .collect();

    if inline_candidates.len() == 1 {
        let hint = inline_candidates[0];
        return Ok(Some(ResolvedNewSchemaPartial {
            abs_path: hint.abs_path.clone(),
            call_id: hint.call_id.clone(),
            timestamp: hint.timestamp.clone(),
        }));
    }

    let scored_inline: Vec<((usize, usize, usize, usize, usize), &NewInlineHint)> =
        inline_candidates
            .iter()
            .map(|hint| {
                (
                    score_new_schema_inline_hint(
                        session,
                        hint,
                        &removed_lines,
                        removed_lines.is_empty(),
                    ),
                    *hint,
                )
            })
            .collect();

    if let Some((score, hint)) = scored_inline.iter().max_by_key(|(score, _)| *score) {
        let best_score = *score;
        let best_count = scored_inline
            .iter()
            .filter(|(other, _)| *other == best_score)
            .count();
        let has_non_weak_signal =
            best_score.0 > 0 || best_score.1 > 0 || best_score.2 > 0 || best_score.3 > 0;
        if best_count == 1 && has_non_weak_signal {
            return Ok(Some(ResolvedNewSchemaPartial {
                abs_path: hint.abs_path.clone(),
                call_id: hint.call_id.clone(),
                timestamp: hint.timestamp.clone(),
            }));
        }
    }

    let strong_candidates: HashSet<String> = session
        .checkpoint_paths
        .iter()
        .chain(session.strong_path_hints.iter())
        .filter(|path| !prior_seen_paths.contains(*path))
        .cloned()
        .collect();
    if strong_candidates.len() == 1 {
        let abs_path = strong_candidates
            .into_iter()
            .next()
            .expect("len checked above");
        return Ok(Some(resolve_partial_with_path(
            partial_id,
            &abs_path,
            inline_hints,
            session,
        )));
    }

    let inline_paths: HashSet<String> = inline_candidates
        .iter()
        .map(|hint| hint.abs_path.clone())
        .collect();
    if inline_paths.len() == 1 {
        let abs_path = inline_paths.into_iter().next().expect("len checked above");
        return Ok(Some(resolve_partial_with_path(
            partial_id,
            &abs_path,
            inline_hints,
            session,
        )));
    }

    let weak_candidates: HashSet<String> = session
        .weak_path_hints
        .iter()
        .filter(|path| !prior_seen_paths.contains(*path))
        .cloned()
        .collect();
    if strong_candidates.is_empty() && weak_candidates.len() == 1 {
        let abs_path = weak_candidates
            .into_iter()
            .next()
            .expect("len checked above");
        return Ok(Some(resolve_partial_with_path(
            partial_id,
            &abs_path,
            inline_hints,
            session,
        )));
    }

    let overlapping_inline_paths: HashSet<String> = scored_inline
        .iter()
        .filter(|(score, _)| score.0 > 0 || score.1 > 0)
        .map(|(_, hint)| hint.abs_path.clone())
        .collect();
    let candidate_paths: HashSet<String> = if !overlapping_inline_paths.is_empty() {
        overlapping_inline_paths
    } else {
        inline_hints
            .iter()
            .map(|hint| hint.abs_path.clone())
            .chain(session.checkpoint_paths.iter().cloned())
            .chain(session.strong_path_hints.iter().cloned())
            .chain(session.weak_path_hints.iter().cloned())
            .filter(|path| !prior_seen_paths.contains(path))
            .filter(|path| !is_cursor_plan_path(path))
            .collect()
    };

    if candidate_paths.is_empty() {
        return Ok(None);
    }

    match resolve_new_schema_partial_with_content(
        session,
        partial_id,
        &added_lines,
        &removed_lines,
        inline_hints,
        &candidate_paths,
        repo_content_indexes,
    )? {
        ContentResolverDecision::Resolved(resolved) => return Ok(Some(resolved)),
        ContentResolverDecision::NoSignal => {}
        ContentResolverDecision::Unresolved { candidate_count } => {
            return Err(format!(
                "ambiguous new-schema partial fate mapping: {} candidate paths for 1 partial rows",
                candidate_count
            ));
        }
    }

    Err(format!(
        "ambiguous new-schema partial fate mapping: {} candidate paths for 1 partial rows",
        candidate_paths.len()
    ))
}

fn resolve_new_schema_partial_with_content(
    session: &CandidateSession,
    partial_id: &str,
    added_lines: &[String],
    removed_lines: &[String],
    inline_hints: &[NewInlineHint],
    export_candidate_paths: &HashSet<String>,
    repo_content_indexes: &mut HashMap<String, RepoContentIndex>,
) -> std::result::Result<ContentResolverDecision, String> {
    let Some(repo_root) = plausible_repo_root(session, inline_hints, export_candidate_paths) else {
        return Ok(ContentResolverDecision::NoSignal);
    };

    let repo_content_index = load_repo_content_index(&repo_root, repo_content_indexes)?;
    let normalized_export_candidate_paths: HashSet<String> = export_candidate_paths
        .iter()
        .map(|path| normalize_repo_candidate_path(&repo_root, path))
        .collect();
    let added_hashes = hash_count_map_for_lines(added_lines);
    let removed_hashes = hash_count_map_for_lines(removed_lines);

    if added_hashes.is_empty() && removed_hashes.is_empty() {
        return Ok(ContentResolverDecision::NoSignal);
    }

    let mut candidate_paths = normalized_export_candidate_paths.clone();
    for path in repo_candidate_paths_from_added_lines(repo_content_index, &added_hashes) {
        candidate_paths.insert(normalize_repo_candidate_path(&repo_root, &path));
    }

    if candidate_paths.is_empty() {
        return Ok(ContentResolverDecision::NoSignal);
    }

    let mut scores: Vec<ContentMatchScore> = candidate_paths
        .iter()
        .map(|abs_path| {
            score_new_schema_content_candidate(
                session,
                abs_path,
                inline_hints,
                &removed_hashes,
                &added_hashes,
                repo_content_index,
                &normalized_export_candidate_paths,
                &repo_root,
            )
        })
        .collect();

    scores.sort_by(|a, b| {
        b.ranking_tuple()
            .cmp(&a.ranking_tuple())
            .then_with(|| a.abs_path.cmp(&b.abs_path))
    });

    let Some(winner) = scores.first() else {
        return Ok(ContentResolverDecision::NoSignal);
    };
    let runner = scores.get(1);
    if !content_match_is_confident(winner, runner) {
        return Ok(ContentResolverDecision::Unresolved {
            candidate_count: candidate_paths.len(),
        });
    }

    Ok(ContentResolverDecision::Resolved(
        resolve_partial_with_path(partial_id, &winner.abs_path, inline_hints, session),
    ))
}

fn plausible_repo_root(
    session: &CandidateSession,
    inline_hints: &[NewInlineHint],
    export_candidate_paths: &HashSet<String>,
) -> Option<String> {
    let roots: HashSet<String> = export_candidate_paths
        .iter()
        .cloned()
        .chain(session.checkpoint_paths.iter().cloned())
        .chain(session.strong_path_hints.iter().cloned())
        .chain(session.weak_path_hints.iter().cloned())
        .chain(inline_hints.iter().map(|hint| hint.abs_path.clone()))
        .filter_map(|path| detect_repo_root(Path::new(&path)).map(|root| path_to_string(&root)))
        .collect();

    if roots.len() == 1 {
        roots.into_iter().next()
    } else {
        None
    }
}

fn load_repo_content_index<'a>(
    repo_root: &str,
    repo_content_indexes: &'a mut HashMap<String, RepoContentIndex>,
) -> std::result::Result<&'a RepoContentIndex, String> {
    if !repo_content_indexes.contains_key(repo_root) {
        let index = build_repo_content_index(repo_root)?;
        repo_content_indexes.insert(repo_root.to_string(), index);
    }

    repo_content_indexes
        .get(repo_root)
        .ok_or_else(|| format!("repo content index missing for {}", repo_root))
}

fn build_repo_content_index(repo_root: &str) -> std::result::Result<RepoContentIndex, String> {
    let mut index = RepoContentIndex::default();
    index_repo_content_dir(Path::new(repo_root), &mut index)?;
    Ok(index)
}

fn index_repo_content_dir(
    dir: &Path,
    index: &mut RepoContentIndex,
) -> std::result::Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|e| {
        format!(
            "failed to read repo content directory {}: {}",
            dir.display(),
            e
        )
    })?;

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        let path = entry.path();

        if file_type.is_dir() {
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if REPO_CONTENT_SKIP_DIRS.contains(&name) {
                continue;
            }
            index_repo_content_dir(&path, index)?;
            continue;
        }

        if !file_type.is_file() || file_type.is_symlink() {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.len() > REPO_CONTENT_MAX_FILE_BYTES {
            continue;
        }

        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(_) => continue,
        };
        let file_hashes = hash_count_map_for_text(&raw);
        if file_hashes.is_empty() {
            continue;
        }

        let abs_path = path_to_string(&path);
        for (line_hash, count) in file_hashes {
            index
                .line_hash_hits
                .entry(line_hash)
                .or_default()
                .push((abs_path.clone(), count));
        }
    }

    Ok(())
}

fn repo_candidate_paths_from_added_lines(
    repo_content_index: &RepoContentIndex,
    added_hashes: &HashMap<String, i64>,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for line_hash in added_hashes.keys() {
        let Some(hits) = repo_content_index.line_hash_hits.get(line_hash) else {
            continue;
        };
        for (path, count) in hits {
            if *count > 0 {
                out.insert(path.clone());
            }
        }
    }
    out
}

fn score_new_schema_content_candidate(
    session: &CandidateSession,
    abs_path: &str,
    inline_hints: &[NewInlineHint],
    removed_hashes: &HashMap<String, i64>,
    added_hashes: &HashMap<String, i64>,
    repo_content_index: &RepoContentIndex,
    export_candidate_paths: &HashSet<String>,
    repo_root: &str,
) -> ContentMatchScore {
    let inline_removed_overlap = inline_hints
        .iter()
        .filter(|hint| normalize_repo_candidate_path(repo_root, &hint.abs_path) == abs_path)
        .map(|hint| {
            overlap_hash_counts(
                removed_hashes,
                &hash_count_map_for_lines(&hint.original_text_lines),
            )
        })
        .max()
        .unwrap_or(0);
    let original_state_removed_overlap = session
        .original_state_contents
        .iter()
        .find_map(|(path, content)| {
            (normalize_repo_candidate_path(repo_root, path) == abs_path)
                .then(|| overlap_hash_counts(removed_hashes, &hash_count_map_for_text(content)))
        })
        .unwrap_or(0);
    let historical_removed_overlap = inline_removed_overlap.max(original_state_removed_overlap);

    let live_added_overlap =
        overlap_repo_index_hash_counts(repo_content_index, abs_path, added_hashes);
    let live_removed_overlap =
        overlap_repo_index_hash_counts(repo_content_index, abs_path, removed_hashes);

    let basename_match = basename_match_against_export_candidates(abs_path, export_candidate_paths);
    let path_proximity =
        path_proximity_against_export_candidates(repo_root, abs_path, export_candidate_paths);

    ContentMatchScore {
        abs_path: abs_path.to_string(),
        historical_removed_overlap,
        live_added_overlap,
        live_removed_overlap,
        checkpoint_match: usize::from(
            session
                .checkpoint_paths
                .iter()
                .any(|path| normalize_repo_candidate_path(repo_root, path) == abs_path),
        ),
        strong_hint_match: usize::from(
            session
                .strong_path_hints
                .iter()
                .any(|path| normalize_repo_candidate_path(repo_root, path) == abs_path),
        ),
        weak_hint_match: usize::from(
            session
                .weak_path_hints
                .iter()
                .any(|path| normalize_repo_candidate_path(repo_root, path) == abs_path),
        ),
        basename_match,
        path_proximity,
    }
}

fn content_match_is_confident(
    winner: &ContentMatchScore,
    runner: Option<&ContentMatchScore>,
) -> bool {
    let winner_primary = winner.primary_score();
    if winner_primary == 0 {
        return false;
    }

    let Some(runner) = runner else {
        return winner.historical_removed_overlap > 0
            || winner.live_added_overlap > 0
            || winner.live_removed_overlap >= 2;
    };

    if winner.ranking_tuple() == runner.ranking_tuple() {
        return false;
    }

    if winner_primary > runner.primary_score() {
        return winner.historical_removed_overlap > 0
            || winner.live_added_overlap > 0
            || winner.live_removed_overlap >= 2;
    }

    winner_primary == runner.primary_score()
        && winner.secondary_score() > runner.secondary_score()
        && (winner.historical_removed_overlap > 0
            || winner.live_added_overlap >= 2
            || winner.live_removed_overlap >= 2)
}

fn overlap_repo_index_hash_counts(
    repo_content_index: &RepoContentIndex,
    abs_path: &str,
    query_hashes: &HashMap<String, i64>,
) -> usize {
    let mut matched = 0usize;
    for (line_hash, query_count) in query_hashes {
        let Some(hits) = repo_content_index.line_hash_hits.get(line_hash) else {
            continue;
        };
        let Some((_path, file_count)) = hits.iter().find(|(path, _)| path == abs_path) else {
            continue;
        };
        matched += (*query_count).min(*file_count) as usize;
    }
    matched
}

fn overlap_hash_counts(
    query_hashes: &HashMap<String, i64>,
    candidate_hashes: &HashMap<String, i64>,
) -> usize {
    query_hashes
        .iter()
        .map(|(line_hash, query_count)| {
            candidate_hashes
                .get(line_hash)
                .map(|candidate_count| (*query_count).min(*candidate_count) as usize)
                .unwrap_or(0)
        })
        .sum()
}

fn basename_match_against_export_candidates(
    abs_path: &str,
    export_candidate_paths: &HashSet<String>,
) -> usize {
    let Some(candidate_basename) = Path::new(abs_path)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return 0;
    };

    usize::from(export_candidate_paths.iter().any(|candidate| {
        candidate != abs_path
            && Path::new(candidate)
                .file_name()
                .and_then(|value| value.to_str())
                == Some(candidate_basename)
    }))
}

fn path_proximity_against_export_candidates(
    repo_root: &str,
    abs_path: &str,
    export_candidate_paths: &HashSet<String>,
) -> usize {
    export_candidate_paths
        .iter()
        .filter(|candidate| candidate.as_str() != abs_path)
        .map(|candidate| shared_repo_relative_components(repo_root, abs_path, candidate))
        .max()
        .unwrap_or(0)
}

fn shared_repo_relative_components(repo_root: &str, left: &str, right: &str) -> usize {
    let repo_root = Path::new(repo_root);
    let Ok(left_rel) = Path::new(left).strip_prefix(repo_root) else {
        return 0;
    };
    let Ok(right_rel) = Path::new(right).strip_prefix(repo_root) else {
        return 0;
    };

    left_rel
        .components()
        .zip(right_rel.components())
        .take_while(|(left_component, right_component)| left_component == right_component)
        .count()
}

fn normalize_repo_candidate_path(repo_root: &str, abs_path: &str) -> String {
    let repo_root = Path::new(repo_root);
    let abs_path_buf = PathBuf::from(abs_path);
    to_rel_path(Some(repo_root), &abs_path_buf)
        .map(|rel_path| path_to_string(&repo_root.join(rel_path)))
        .unwrap_or_else(|| abs_path.to_string())
}

fn resolve_partial_with_path(
    partial_id: &str,
    abs_path: &str,
    inline_hints: &[NewInlineHint],
    session: &CandidateSession,
) -> ResolvedNewSchemaPartial {
    if let Some(hint) = inline_hints.iter().find(|hint| hint.abs_path == abs_path) {
        return ResolvedNewSchemaPartial {
            abs_path: abs_path.to_string(),
            call_id: hint.call_id.clone(),
            timestamp: hint.timestamp.clone(),
        };
    }

    ResolvedNewSchemaPartial {
        abs_path: abs_path.to_string(),
        call_id: partial_id.to_string(),
        timestamp: session.info.last_seen_at.clone(),
    }
}

fn score_new_schema_inline_hint(
    session: &CandidateSession,
    hint: &NewInlineHint,
    removed_lines: &[String],
    partial_has_only_additions: bool,
) -> (usize, usize, usize, usize, usize) {
    let removed_overlap = removed_lines
        .iter()
        .filter(|line| {
            hint.original_text_lines
                .iter()
                .any(|candidate| candidate == *line)
        })
        .count();
    let new_file_match =
        usize::from(partial_has_only_additions && hint.original_text_lines.is_empty());
    let checkpoint_match = usize::from(session.checkpoint_paths.contains(&hint.abs_path));
    let strong_hint_match = usize::from(session.strong_path_hints.contains(&hint.abs_path));
    let weak_hint_match = usize::from(session.weak_path_hints.contains(&hint.abs_path));

    (
        removed_overlap,
        new_file_match,
        checkpoint_match,
        strong_hint_match,
        weak_hint_match,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "legacy fallback ingestion carries the same shared parser state as the other cursor ingestion helpers"
)]
fn ingest_legacy_code_block_fallback(
    conn: &Connection,
    graphs: &HashMap<String, CursorSessionGraph>,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    for (composer_id, session) in sessions {
        if session.legacy_targets.is_empty() {
            continue;
        }
        let Some(graph) = graphs.get(composer_id) else {
            continue;
        };

        summary.legacy_sessions_considered += 1;

        let mut targets = session.legacy_targets.clone();
        targets.sort_by(|a, b| {
            a.abs_path
                .cmp(&b.abs_path)
                .then(a.version.cmp(&b.version))
                .then(a.code_block_idx.cmp(&b.code_block_idx))
                .then(a.diff_id.cmp(&b.diff_id))
        });

        let mut previous_content_by_path: HashMap<String, String> = HashMap::new();
        let mut exact_legacy_paths: HashSet<String> = HashSet::new();
        let mut deferred_errors_by_path: HashMap<String, LegacyDeferredParseError> = HashMap::new();

        for target in targets {
            if seen_paths_by_session
                .get(composer_id)
                .is_some_and(|set| set.contains(&target.abs_path))
            {
                if let Some(content) = &target.content {
                    previous_content_by_path.insert(target.abs_path.clone(), content.clone());
                }
                continue;
            }

            summary.tool_calls_inspected += 1;
            summary.legacy_entries_inspected += 1;

            let op = match build_legacy_diff_op(graph, &target, session) {
                Ok(Some(op)) => {
                    summary.legacy_diff_rows_found += 1;
                    deferred_errors_by_path.remove(&target.abs_path);
                    Some(op)
                }
                Ok(None) => match build_legacy_content_fallback_op(
                    previous_content_by_path
                        .get(&target.abs_path)
                        .map(String::as_str),
                    &target,
                    session,
                ) {
                    Ok(Some(op)) => {
                        deferred_errors_by_path.remove(&target.abs_path);
                        Some(op)
                    }
                    Ok(None) => None,
                    Err(content_err) => {
                        deferred_errors_by_path
                            .entry(target.abs_path.clone())
                            .or_insert_with(|| LegacyDeferredParseError {
                                call_id: target.call_id(),
                                timestamp: target
                                    .timestamp
                                    .clone()
                                    .or_else(|| session.info.last_seen_at.clone()),
                                parser_name: LEGACY_CONTENT_PARSER_NAME,
                                error: content_err,
                            });
                        None
                    }
                },
                Err(diff_err) => match build_legacy_content_fallback_op(
                    previous_content_by_path
                        .get(&target.abs_path)
                        .map(String::as_str),
                    &target,
                    session,
                ) {
                    Ok(Some(op)) => {
                        deferred_errors_by_path.remove(&target.abs_path);
                        Some(op)
                    }
                    Ok(None) => {
                        deferred_errors_by_path
                            .entry(target.abs_path.clone())
                            .or_insert_with(|| LegacyDeferredParseError {
                                call_id: target.call_id(),
                                timestamp: target
                                    .timestamp
                                    .clone()
                                    .or_else(|| session.info.last_seen_at.clone()),
                                parser_name: LEGACY_DIFF_PARSER_NAME,
                                error: diff_err,
                            });
                        None
                    }
                    Err(content_err) => {
                        deferred_errors_by_path
                            .entry(target.abs_path.clone())
                            .or_insert_with(|| LegacyDeferredParseError {
                                call_id: target.call_id(),
                                timestamp: target
                                    .timestamp
                                    .clone()
                                    .or_else(|| session.info.last_seen_at.clone()),
                                parser_name: LEGACY_CONTENT_PARSER_NAME,
                                error: format!("{}; fallback failed: {}", diff_err, content_err),
                            });
                        None
                    }
                },
            };

            if let Some(op) = op {
                exact_legacy_paths.insert(target.abs_path.clone());
                ops.push(op);
                summary.legacy_ops_upserted += 1;
            }

            if let Some(content) = &target.content {
                previous_content_by_path.insert(target.abs_path.clone(), content.clone());
            }
        }

        if deferred_errors_by_path.len() == 1 {
            let (abs_path, deferred) = deferred_errors_by_path
                .iter()
                .next()
                .map(|(path, deferred)| (path.clone(), deferred.clone()))
                .expect("len checked above");
            if !exact_legacy_paths.contains(&abs_path)
                && !seen_paths_by_session
                    .get(composer_id)
                    .is_some_and(|set| set.contains(&abs_path))
                && let Some(op) =
                    build_legacy_session_totals_fallback_op(session, &abs_path, &deferred)
            {
                ops.push(op);
                summary.legacy_ops_upserted += 1;
                seen_paths_by_session
                    .entry(composer_id.clone())
                    .or_default()
                    .insert(abs_path);
                deferred_errors_by_path.clear();
            }
        }

        for deferred in deferred_errors_by_path.into_values() {
            insert_legacy_parse_error(
                conn,
                summary,
                &session.info.session_id,
                source_file,
                &deferred.call_id,
                deferred.timestamp,
                deferred.parser_name,
                deferred.error,
                verbose,
            )?;
        }
    }

    Ok(())
}

fn build_legacy_diff_op(
    graph: &CursorSessionGraph,
    target: &LegacyTarget,
    session: &CandidateSession,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let Some(diff_id) = &target.diff_id else {
        return Ok(None);
    };

    let Some(payload) = graph.legacy_diff_payloads.get(diff_id) else {
        return Err("legacy codeBlockDiff row not found".to_string());
    };

    let removed_lines = extract_legacy_diff_lines(payload, "originalModelDiffWrtV0")?;
    let added_lines = extract_legacy_diff_lines(payload, "newModelDiffWrtV0")?;

    if added_lines.is_empty() && removed_lines.is_empty() {
        return Ok(None);
    }

    let mut line_hashes = hash_counts_for_lines(&added_lines, LineSide::Added);
    line_hashes.extend(hash_counts_for_lines(&removed_lines, LineSide::Removed));

    Ok(Some(build_change_op_candidate(
        session,
        diff_id.to_string(),
        target.op_index(),
        target.timestamp.clone(),
        &target.abs_path,
        WriteMode::Patch,
        true,
        added_lines.len() as i64,
        removed_lines.len() as i64,
        LEGACY_DIFF_PARSER_NAME,
        line_hashes,
    )))
}

fn build_legacy_content_fallback_op(
    previous_content: Option<&str>,
    target: &LegacyTarget,
    session: &CandidateSession,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let Some(before_content) = previous_content else {
        return Ok(None);
    };
    let Some(after_content) = target.content.as_deref() else {
        return Ok(None);
    };

    let diff = diff_with_hashes(before_content, after_content);
    if diff.added_lines == 0 && diff.removed_lines == 0 {
        return Ok(None);
    }

    let call_id = target.call_id();
    Ok(Some(build_change_op_candidate(
        session,
        call_id,
        target.op_index(),
        target.timestamp.clone(),
        &target.abs_path,
        WriteMode::Patch,
        true,
        diff.added_lines,
        diff.removed_lines,
        LEGACY_CONTENT_PARSER_NAME,
        diff.line_hashes,
    )))
}

fn extract_legacy_diff_lines(
    payload: &Value,
    field: &str,
) -> std::result::Result<Vec<String>, String> {
    let hunks = payload
        .get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("legacy diff payload missing '{}' array", field))?;

    let mut lines = Vec::new();
    for hunk in hunks {
        let Some(hunk_obj) = hunk.as_object() else {
            continue;
        };
        let Some(modified) = hunk_obj.get("modified").and_then(|v| v.as_array()) else {
            continue;
        };
        for line in modified {
            let Some(line) = line.as_str() else {
                return Err(format!(
                    "legacy diff '{}' modified entry was not a string",
                    field
                ));
            };
            lines.push(line.to_string());
        }
    }

    Ok(lines)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the parsed change fields are not materialized anywhere else before constructing the candidate"
)]
fn build_change_op_candidate(
    session: &CandidateSession,
    call_id: String,
    op_index: i32,
    timestamp: Option<String>,
    abs_path: &str,
    write_mode: WriteMode,
    before_known: bool,
    added_lines: i64,
    removed_lines: i64,
    parser_name: &str,
    line_hashes: Vec<LineHashCount>,
) -> ChangeOpCandidate {
    let abs_path_buf = PathBuf::from(abs_path);
    let repo_root = detect_repo_root(&abs_path_buf);
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path_buf);

    ChangeOpCandidate {
        provider: "cursor".to_string(),
        session_id: session.info.session_id.clone(),
        source_file: session.info.source_file.clone(),
        call_id,
        op_index,
        timestamp: timestamp.or_else(|| session.info.last_seen_at.clone()),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: abs_path.to_string(),
        rel_path,
        write_mode,
        before_known,
        added_lines,
        removed_lines,
        parser_name: parser_name.to_string(),
        line_hashes,
    }
}

fn build_inline_change_op(
    call_id: &str,
    row: &Value,
    abs_path: &str,
    session: &CandidateSession,
) -> std::result::Result<ChangeOpCandidate, String> {
    let changes = row
        .get("changes")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "inline row missing 'changes' array".to_string())?;

    let new_text_lines = parse_string_array(row.get("newTextLines"));

    let mut added_lines: Vec<String> = Vec::new();
    let mut removed_lines: Vec<String> = Vec::new();

    for change in changes {
        let Some(change_obj) = change.as_object() else {
            continue;
        };

        if let Some(removed) = change_obj.get("removedTextLines") {
            removed_lines.extend(parse_string_array(Some(removed)));
        }

        if let Some(added_text_lines) = change_obj.get("addedTextLines") {
            added_lines.extend(parse_string_array(Some(added_text_lines)));
            continue;
        }

        let Some(added_range) = change_obj.get("addedRange").and_then(|v| v.as_object()) else {
            continue;
        };

        let start = added_range
            .get("startLineNumber")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| "inline change missing addedRange.startLineNumber".to_string())?;
        let end = added_range
            .get("endLineNumberExclusive")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| "inline change missing addedRange.endLineNumberExclusive".to_string())?;

        if start < 1 || end < start {
            return Err(format!(
                "invalid addedRange bounds start={} end={}",
                start, end
            ));
        }

        let start_idx = (start - 1) as usize;
        let end_idx = (end - 1) as usize;

        if end_idx > new_text_lines.len() || start_idx > end_idx {
            return Err(format!(
                "addedRange out of bounds start={} end={} newTextLines={}",
                start,
                end,
                new_text_lines.len()
            ));
        }

        added_lines.extend(new_text_lines[start_idx..end_idx].iter().cloned());
    }

    if added_lines.is_empty() && removed_lines.is_empty() {
        return Err("inline row produced zero added/removed lines".to_string());
    }

    let mut line_hashes = hash_counts_for_lines(&added_lines, LineSide::Added);
    line_hashes.extend(hash_counts_for_lines(&removed_lines, LineSide::Removed));

    let abs_path_buf = PathBuf::from(abs_path);
    let repo_root = detect_repo_root(&abs_path_buf);
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path_buf);

    Ok(ChangeOpCandidate {
        provider: "cursor".to_string(),
        session_id: session.info.session_id.clone(),
        source_file: session.info.source_file.clone(),
        call_id: call_id.to_string(),
        op_index: 0,
        timestamp: row
            .get("createdAt")
            .and_then(|v| v.as_i64())
            .and_then(ms_to_iso)
            .or_else(|| session.info.last_seen_at.clone()),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: abs_path.to_string(),
        rel_path,
        write_mode: WriteMode::Patch,
        before_known: true,
        added_lines: added_lines.len() as i64,
        removed_lines: removed_lines.len() as i64,
        parser_name: INLINE_PARSER_NAME.to_string(),
        line_hashes,
    })
}

fn build_partial_fates_op(
    call_id: &str,
    payload: &Value,
    abs_path: &str,
    session: &CandidateSession,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let (added_lines, removed_lines) = accepted_partial_fate_lines(payload)?;

    if added_lines.is_empty() && removed_lines.is_empty() {
        return Ok(None);
    }

    let mut line_hashes = hash_counts_for_lines(&added_lines, LineSide::Added);
    line_hashes.extend(hash_counts_for_lines(&removed_lines, LineSide::Removed));

    let abs_path_buf = PathBuf::from(abs_path);
    let repo_root = detect_repo_root(&abs_path_buf);
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path_buf);

    Ok(Some(ChangeOpCandidate {
        provider: "cursor".to_string(),
        session_id: session.info.session_id.clone(),
        source_file: session.info.source_file.clone(),
        call_id: call_id.to_string(),
        op_index: 0,
        timestamp: session.info.last_seen_at.clone(),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: abs_path.to_string(),
        rel_path,
        write_mode: WriteMode::Patch,
        before_known: true,
        added_lines: added_lines.len() as i64,
        removed_lines: removed_lines.len() as i64,
        parser_name: PARTIAL_PARSER_NAME.to_string(),
        line_hashes,
    }))
}

fn build_new_schema_partial_fates_op(
    call_id: &str,
    partials: &[(String, Value)],
    abs_path: &str,
    timestamp: Option<String>,
    session: &CandidateSession,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let mut added_lines = Vec::new();
    let mut removed_lines = Vec::new();

    for (_partial_id, payload) in partials {
        let (added, removed) = accepted_partial_fate_lines(payload)?;
        added_lines.extend(added);
        removed_lines.extend(removed);
    }

    if added_lines.is_empty() && removed_lines.is_empty() {
        return Ok(None);
    }

    let mut line_hashes = hash_counts_for_lines(&added_lines, LineSide::Added);
    line_hashes.extend(hash_counts_for_lines(&removed_lines, LineSide::Removed));

    let abs_path_buf = PathBuf::from(abs_path);
    let repo_root = detect_repo_root(&abs_path_buf);
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path_buf);

    Ok(Some(ChangeOpCandidate {
        provider: "cursor".to_string(),
        session_id: session.info.session_id.clone(),
        source_file: session.info.source_file.clone(),
        call_id: call_id.to_string(),
        op_index: 0,
        timestamp: timestamp.or_else(|| session.info.last_seen_at.clone()),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: abs_path.to_string(),
        rel_path,
        write_mode: WriteMode::Patch,
        before_known: true,
        added_lines: added_lines.len() as i64,
        removed_lines: removed_lines.len() as i64,
        parser_name: NEW_SCHEMA_PARSER_NAME.to_string(),
        line_hashes,
    }))
}

fn accepted_partial_fate_lines(
    payload: &Value,
) -> std::result::Result<(Vec<String>, Vec<String>), String> {
    let fates = payload
        .get("fates")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "partial fates payload missing 'fates' array".to_string())?;

    let mut added_lines: Vec<String> = Vec::new();
    let mut removed_lines: Vec<String> = Vec::new();

    for fate in fates {
        let Some(fate_obj) = fate.as_object() else {
            continue;
        };
        if fate_obj.get("fate").and_then(|v| v.as_str()) != Some("accepted") {
            continue;
        }

        if let Some(added) = fate_obj.get("addedLines") {
            added_lines.extend(parse_string_array(Some(added)));
        }
        if let Some(removed) = fate_obj.get("removedLines") {
            removed_lines.extend(parse_string_array(Some(removed)));
        }
    }

    Ok((added_lines, removed_lines))
}

fn build_legacy_session_totals_fallback_op(
    session: &CandidateSession,
    abs_path: &str,
    deferred: &LegacyDeferredParseError,
) -> Option<ChangeOpCandidate> {
    let has_original_state = session.weak_path_hints.contains(abs_path)
        || session.original_state_contents.contains_key(abs_path);
    if !has_original_state {
        return None;
    }

    let added_lines = session.total_lines_added.unwrap_or(0);
    let removed_lines = session.total_lines_removed.unwrap_or(0);
    if added_lines == 0 && removed_lines == 0 {
        return None;
    }

    Some(build_change_op_candidate(
        session,
        deferred.call_id.clone(),
        0,
        deferred.timestamp.clone(),
        abs_path,
        WriteMode::Patch,
        false,
        added_lines,
        removed_lines,
        LEGACY_TOTALS_PARSER_NAME,
        Vec::new(),
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "parse errors are recorded directly from the callsite context without allocating an intermediate struct"
)]
fn insert_parse_error(
    conn: &Connection,
    summary: &mut ProviderCodeChangeSummary,
    session_id: &str,
    source_file: &str,
    call_id: &str,
    timestamp: Option<String>,
    parser_name: &str,
    error: String,
    verbose: bool,
) -> Result<()> {
    if verbose {
        eprintln!("[change-intel] {} {} {}", parser_name, call_id, error);
    }

    storage::insert_parse_error(
        conn,
        &ParseError {
            provider: "cursor".to_string(),
            session_id: session_id.to_string(),
            source_file: source_file.to_string(),
            call_id: call_id.to_string(),
            timestamp,
            parser_name: parser_name.to_string(),
            error,
        },
    )?;
    summary.parse_errors += 1;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "legacy parse errors reuse the shared helper while incrementing legacy-specific summary counters"
)]
fn insert_legacy_parse_error(
    conn: &Connection,
    summary: &mut ProviderCodeChangeSummary,
    session_id: &str,
    source_file: &str,
    call_id: &str,
    timestamp: Option<String>,
    parser_name: &str,
    error: String,
    verbose: bool,
) -> Result<()> {
    insert_parse_error(
        conn,
        summary,
        session_id,
        source_file,
        call_id,
        timestamp,
        parser_name,
        error,
        verbose,
    )?;
    summary.legacy_parse_errors += 1;
    Ok(())
}

fn extract_partial_targets(parsed: &Value) -> Vec<PartialTarget> {
    let mut out: HashSet<PartialTarget> = HashSet::new();

    let Some(code_block_data) = parsed.get("codeBlockData").and_then(|v| v.as_object()) else {
        return Vec::new();
    };

    for (file_key, value) in code_block_data {
        let entries: Vec<&Value> = if let Some(list) = value.as_array() {
            list.iter().collect()
        } else if let Some(map) = value.as_object() {
            map.values().collect()
        } else {
            Vec::new()
        };

        for entry in entries {
            let Some(entry_obj) = entry.as_object() else {
                continue;
            };
            if entry_obj.get("status").and_then(|v| v.as_str()) != Some("accepted") {
                continue;
            }

            let Some(partial_id) = entry_obj
                .get("partialInlineDiffFatesId")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
            else {
                continue;
            };

            let abs_path = entry_obj
                .get("uri")
                .and_then(extract_file_path_from_uri_value)
                .or_else(|| extract_file_path_from_string(file_key));

            let Some(abs_path) = abs_path else {
                continue;
            };
            if is_cursor_plan_path(&abs_path) {
                continue;
            }

            out.insert(PartialTarget {
                partial_id,
                abs_path,
            });
        }
    }

    out.into_iter().collect()
}

fn extract_legacy_targets(parsed: &Value) -> Vec<LegacyTarget> {
    let Some(code_block_data) = parsed.get("codeBlockData").and_then(|v| v.as_object()) else {
        return Vec::new();
    };

    let default_timestamp = parsed
        .get("lastUpdatedAt")
        .and_then(|v| v.as_i64())
        .and_then(ms_to_iso)
        .or_else(|| {
            parsed
                .get("createdAt")
                .and_then(|v| v.as_i64())
                .and_then(ms_to_iso)
        });

    let mut targets = Vec::new();

    for (file_key, value) in code_block_data {
        let entries: Vec<&Value> = if let Some(list) = value.as_array() {
            list.iter().collect()
        } else if let Some(map) = value.as_object() {
            map.values().collect()
        } else {
            Vec::new()
        };

        for entry in entries {
            let Some(entry_obj) = entry.as_object() else {
                continue;
            };
            if entry_obj.get("status").and_then(|v| v.as_str()) != Some("accepted") {
                continue;
            }
            if entry_obj.get("isNotApplied").and_then(|v| v.as_bool()) == Some(true) {
                continue;
            }
            if entry_obj.get("isNoOp").and_then(|v| v.as_bool()) == Some(true) {
                continue;
            }

            let abs_path = entry_obj
                .get("uri")
                .and_then(extract_file_path_from_uri_value)
                .or_else(|| extract_file_path_from_string(file_key));
            let Some(abs_path) = abs_path else {
                continue;
            };
            if is_cursor_plan_path(&abs_path) {
                continue;
            }

            let diff_id = entry_obj
                .get("diffId")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned);
            let content = entry_obj
                .get("content")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned);

            if diff_id.is_none() && content.is_none() {
                continue;
            }

            targets.push(LegacyTarget {
                diff_id,
                abs_path,
                version: entry_obj
                    .get("version")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32,
                code_block_idx: entry_obj
                    .get("codeBlockIdx")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32,
                timestamp: default_timestamp.clone(),
                content,
                bubble_id: entry_obj
                    .get("bubbleId")
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned),
            });
        }
    }

    targets
}

fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };

    let Some(arr) = value.as_array() else {
        return Vec::new();
    };

    arr.iter()
        .filter_map(|v| v.as_str().map(ToOwned::to_owned))
        .collect()
}

fn hash_counts_for_lines(lines: &[String], side: LineSide) -> Vec<LineHashCount> {
    let mut counts: HashMap<String, i64> = HashMap::new();
    for line in lines {
        *counts.entry(hash_line(line)).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .map(|(line_hash, count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect()
}

fn hash_count_map_for_lines(lines: &[String]) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    for line in lines {
        if !is_contentful_content_match_line(line) {
            continue;
        }
        *counts.entry(hash_line(line)).or_insert(0) += 1;
    }
    counts
}

fn hash_count_map_for_text(text: &str) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    for line in text.lines() {
        if !is_contentful_content_match_line(line) {
            continue;
        }
        *counts.entry(hash_line(line)).or_insert(0) += 1;
    }
    counts
}

fn is_contentful_content_match_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 3 && trimmed.chars().any(char::is_alphanumeric)
}

fn is_candidate_session(parsed: &Value) -> bool {
    let files_changed = parsed
        .get("filesChangedCount")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let total_added = parsed
        .get("totalLinesAdded")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let total_removed = parsed
        .get("totalLinesRemoved")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let subtitle = parsed
        .get("subtitle")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    files_changed > 0
        || total_added > 0
        || total_removed > 0
        || subtitle.contains("edited ")
        || subtitle.contains("updated ")
        || !extract_legacy_targets(parsed).is_empty()
}

fn extract_file_path_from_uri_value(uri: &Value) -> Option<String> {
    match uri {
        Value::Object(map) => {
            if let Some(scheme) = map.get("scheme").and_then(|v| v.as_str())
                && scheme != "file"
            {
                return None;
            }

            if let Some(fs_path) = map.get("fsPath").and_then(|v| v.as_str()) {
                return Some(normalize_filesystem_path(fs_path));
            }

            if let Some(external) = map.get("external").and_then(|v| v.as_str()) {
                return extract_file_path_from_string(external);
            }

            if let Some(path) = map.get("path").and_then(|v| v.as_str()) {
                return Some(normalize_filesystem_path(path));
            }

            None
        }
        Value::String(s) => extract_file_path_from_string(s),
        _ => None,
    }
}

fn extract_file_path_from_string(raw: &str) -> Option<String> {
    if raw.starts_with("file://") {
        return Some(strip_file_scheme(raw));
    }
    if raw.starts_with("vscode-notebook-cell:") {
        return None;
    }
    if raw.contains("://") {
        return None;
    }
    Some(normalize_filesystem_path(raw))
}

fn is_cursor_plan_path(path: &str) -> bool {
    let normalized = normalize_filesystem_path(path);
    normalized.contains("/.cursor/plans/") || normalized.ends_with("/.cursor/plans")
}

fn composer_id_from_checkpoint_key(key: &str) -> Option<&str> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "checkpointId" {
        return None;
    }
    parts.next()
}

fn partial_fates_key_parts(key: &str) -> Option<(&str, &str)> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "codeBlockPartialInlineDiffFates" {
        return None;
    }
    Some((parts.next()?, parts.next()?))
}

fn file_signature(path: &Path) -> Result<Option<(i64, i64)>> {
    if !path.is_file() {
        return Ok(None);
    }

    let md = std::fs::metadata(path)?;
    let size = md.len() as i64;
    let mtime = md
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    Ok(Some((mtime, size)))
}

fn ms_to_iso(ms: i64) -> Option<String> {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339())
}

pub(crate) fn cursor_vscdb_path() -> Result<Option<PathBuf>> {
    cursor_state_path()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::{Map, json};
    use std::fs;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::change_intel::schema::init_change_intel_schema;
    use crate::db::init_app_schema;
    use crate::providers::cursor::ingest_planned_sessions_from_source;

    fn temp_db_path(label: &str) -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("paceflow_cursor_{}_{}.db", label, n))
    }

    fn create_cursor_db(path: &Path, rows: &[(String, String)]) -> Result<()> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (
                key   TEXT PRIMARY KEY,
                value TEXT
            );",
        )?;

        for (key, value) in rows {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, value],
            )?;
        }
        Ok(())
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
    }

    fn temp_repo_path(label: &str) -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("paceflow_cursor_repo_{}_{}", label, n))
    }

    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn git(args: &[&str], cwd: &Path) -> Result<()> {
        let status = Command::new("git").current_dir(cwd).args(args).status()?;
        anyhow::ensure!(
            status.success(),
            "git {:?} failed in {}",
            args,
            cwd.display()
        );
        Ok(())
    }

    fn create_git_repo(root: &Path, files: &[(&str, &str)]) -> Result<()> {
        fs::create_dir_all(root)?;
        git(&["init", "-q"], root)?;

        for (rel_path, content) in files {
            let abs_path = root.join(rel_path);
            if let Some(parent) = abs_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(abs_path, content)?;
        }

        Ok(())
    }

    fn file_uri(path: &Path) -> String {
        format!("file://{}", path_to_string(path))
    }

    fn original_file_states(entries: &[(&Path, &str)]) -> Map<String, Value> {
        let mut out = Map::new();
        for (path, content) in entries {
            out.insert(file_uri(path), json!({ "content": content }));
        }
        out
    }

    fn canonical_repo_path(path: &Path) -> String {
        let repo_root = detect_repo_root(path).expect("temporary git repo should have a repo root");
        normalize_repo_candidate_path(&path_to_string(&repo_root), &path_to_string(path))
    }

    #[test]
    fn inline_diff_ingest_and_idempotent_skip() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("inline_idempotent");
        let rows = vec![
            (
                "composerData:c1".to_string(),
                json!({
                    "composerId": "c1",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "totalLinesAdded": 2,
                    "totalLinesRemoved": 1,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "checkpointId:c1:chk1".to_string(),
                json!({
                    "files": [
                        {"uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md", "external": "file:///tmp/repo/README.md"}}
                    ]
                })
                .to_string(),
            ),
            (
                "inlineDiffUndoRedo-1".to_string(),
                json!({
                    "composerMetadata": {"composerId": "c1"},
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md", "external": "file:///tmp/repo/README.md"},
                    "createdAt": 1772694300812i64,
                    "newTextLines": ["new line"],
                    "changes": [
                        {
                            "removedTextLines": ["old line"],
                            "addedRange": {"startLineNumber": 1, "endLineNumberExclusive": 2}
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary1 = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary1.provider, "cursor");
        assert_eq!(summary1.sources_discovered, 1);
        assert_eq!(summary1.sources_skipped, 0);
        assert_eq!(summary1.ops_upserted, 1);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, INLINE_PARSER_NAME);
        assert_eq!(op_row.1, 1);
        assert_eq!(op_row.2, 1);

        let summary2 = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary2.sources_skipped, 1);

        let count: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn inline_diff_ingest_decodes_percent_encoded_windows_file_uri_paths() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("inline_windows_uri");
        let rows = vec![
            (
                "composerData:c_windows".to_string(),
                json!({
                    "composerId": "c_windows",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "inlineDiffUndoRedo-windows".to_string(),
                json!({
                    "composerMetadata": {"composerId": "c_windows"},
                    "uri": {"scheme": "file", "external": "file:///c%3A/dev/paceflow/README.md"},
                    "createdAt": 1772694300812i64,
                    "newTextLines": ["new line"],
                    "changes": [
                        {
                            "removedTextLines": ["old line"],
                            "addedRange": {"startLineNumber": 1, "endLineNumberExclusive": 2}
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);

        let abs_path: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(abs_path, "C:/dev/paceflow/README.md");

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn tool_call_edit_file_v2_ingest_creates_tool_write() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("tool_edit_file");
        let rows = vec![
            (
                "composerData:tool_edit".to_string(),
                json!({
                    "composerId": "tool_edit",
                    "conversation": [{"type": 1, "text": "refactor"}],
                    "filesChangedCount": 1,
                    "totalLinesAdded": 2,
                    "totalLinesRemoved": 1,
                    "originalFileStates": {
                        "file:///tmp/repo/src/plan.ts": {
                            "firstEditBubbleId": "bubble-plan",
                            "content": "old\n"
                        }
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:tool_edit:bubble-plan".to_string(),
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
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);

        let row: (String, String, i64, i64) = analytics.query_row(
            "SELECT abs_path, parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND session_id='tool_edit' AND source_kind='tool_write'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(row.0, "/tmp/repo/src/plan.ts");
        assert_eq!(row.1, "cursor_tool_edit_file_v2_v1");
        assert_eq!(row.2, 2);
        assert_eq!(row.3, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn session_ingest_and_change_intel_agree_on_latest_tool_edit_files() -> Result<()> {
        let analytics = Connection::open_in_memory()?;
        init_app_schema(&analytics)?;

        let source = temp_db_path("tool_edit_agreement");
        let rows = vec![
            (
                "composerData:agree".to_string(),
                json!({
                    "composerId": "agree",
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
                "bubbleId:agree:bubble-plan".to_string(),
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
                "bubbleId:agree:bubble-assign".to_string(),
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
        ];

        create_cursor_db(&source, &rows)?;
        let source_conn = Connection::open(&source)?;

        ingest_planned_sessions_from_source(
            &analytics,
            &source_conn,
            source.to_string_lossy().as_ref(),
            &[("composerData:agree".to_string(), rows[0].1.clone())],
            &HashMap::new(),
            false,
            None,
        )?;

        let mut analytics_for_change_intel = analytics;
        ingest_cursor_code_changes_from_path(&mut analytics_for_change_intel, &source, false)?;

        let session_paths = analytics_for_change_intel
            .prepare(
                "SELECT abs_path
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='agree' AND source_kind='accepted_change'
                 ORDER BY abs_path",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let tool_paths = analytics_for_change_intel
            .prepare(
                "SELECT abs_path
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='agree' AND source_kind='tool_write'
                 ORDER BY abs_path",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        assert_eq!(session_paths, tool_paths);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn invalid_added_range_records_parse_error() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("invalid_range");
        let rows = vec![
            (
                "composerData:c2".to_string(),
                json!({
                    "composerId": "c2",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "inlineDiffUndoRedo-2".to_string(),
                json!({
                    "composerMetadata": {"composerId": "c2"},
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"},
                    "createdAt": 1772694300812i64,
                    "newTextLines": ["only one"],
                    "changes": [
                        {
                            "removedTextLines": ["old"],
                            "addedRange": {"startLineNumber": 1, "endLineNumberExclusive": 3}
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 0);
        assert_eq!(summary.parse_errors, 1);

        let parse_errs: i64 = analytics.query_row(
            "SELECT COUNT(*) FROM change_parse_errors WHERE parser_name = ?1",
            params![INLINE_PARSER_NAME],
            |r| r.get(0),
        )?;
        assert_eq!(parse_errs, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn partial_fallback_supports_list_and_dict_shapes() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("partial_shapes");
        let rows = vec![
            (
                "composerData:list_sess".to_string(),
                json!({
                    "composerId": "list_sess",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {
                        "file:///tmp/repo/list.txt": [
                            {"status": "accepted", "partialInlineDiffFatesId": "p_list"}
                        ]
                    }
                })
                .to_string(),
            ),
            (
                "composerData:dict_sess".to_string(),
                json!({
                    "composerId": "dict_sess",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {
                        "file:///tmp/repo/dict.txt": {
                            "block-1": {
                                "status": "accepted",
                                "partialInlineDiffFatesId": "p_dict",
                                "uri": {"scheme": "file", "fsPath": "/tmp/repo/dict.txt"}
                            }
                        }
                    }
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:list_sess:p_list".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["old list"], "addedLines": ["new list"]}
                    ]
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:dict_sess:p_dict".to_string(),
                json!({
                    "fates": [
                        {"fate": "rejected", "removedLines": ["x"], "addedLines": ["y"]},
                        {"fate": "accepted", "removedLines": ["old dict"], "addedLines": ["new dict 1", "new dict 2"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 2);

        let mut stmt = analytics.prepare(
            "SELECT session_id, parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write'
             ORDER BY session_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?;
        let collected = rows.collect::<std::result::Result<Vec<_>, _>>()?;

        assert_eq!(collected.len(), 2);
        assert_eq!(
            collected[0],
            (
                "dict_sess".to_string(),
                PARTIAL_PARSER_NAME.to_string(),
                2,
                1
            )
        );
        assert_eq!(
            collected[1],
            (
                "list_sess".to_string(),
                PARTIAL_PARSER_NAME.to_string(),
                1,
                1
            )
        );

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn inline_wins_over_partial_for_same_file() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("inline_wins");
        let rows = vec![
            (
                "composerData:same_sess".to_string(),
                json!({
                    "composerId": "same_sess",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {
                        "file:///tmp/repo/README.md": {
                            "b1": {
                                "status": "accepted",
                                "partialInlineDiffFatesId": "p_same",
                                "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"}
                            }
                        }
                    }
                })
                .to_string(),
            ),
            (
                "checkpointId:same_sess:chk1".to_string(),
                json!({
                    "files": [
                        {"uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"}}
                    ]
                })
                .to_string(),
            ),
            (
                "inlineDiffUndoRedo-same".to_string(),
                json!({
                    "composerMetadata": {"composerId": "same_sess"},
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"},
                    "createdAt": 1772694300812i64,
                    "newTextLines": ["new inline"],
                    "changes": [
                        {
                            "removedTextLines": ["old inline"],
                            "addedRange": {"startLineNumber": 1, "endLineNumberExclusive": 2}
                        }
                    ]
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:same_sess:p_same".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["old partial"], "addedLines": ["new partial"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write' AND session_id='same_sess'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, INLINE_PARSER_NAME);
        assert_eq!(op_row.1, 1);
        assert_eq!(op_row.2, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn old_cursor_sessions_with_accepted_code_blocks_are_candidates() {
        let parsed = json!({
            "composerId": "legacy_sess",
            "createdAt": 1772694178155i64,
            "lastUpdatedAt": 1772694194894i64,
            "codeBlockData": {
                "file:///tmp/repo/legacy.txt": [
                    {
                        "status": "accepted",
                        "diffId": "legacy-diff",
                        "version": 0,
                        "codeBlockIdx": 0,
                        "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                    }
                ]
            }
        });

        assert!(is_candidate_session(&parsed));

        let targets = extract_legacy_targets(&parsed);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].diff_id.as_deref(), Some("legacy-diff"));
        assert_eq!(targets[0].abs_path, "/tmp/repo/legacy.txt");
    }

    #[test]
    fn legacy_code_block_diff_ingest_creates_tool_write_hashes() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("legacy_diff");
        let rows = vec![
            (
                "composerData:legacy_diff".to_string(),
                json!({
                    "composerId": "legacy_diff",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "codeBlockData": {
                        "file:///tmp/repo/legacy.txt": [
                            {
                                "status": "accepted",
                                "diffId": "legacy-d1",
                                "version": 0,
                                "codeBlockIdx": 0,
                                "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                            }
                        ]
                    }
                })
                .to_string(),
            ),
            (
                "codeBlockDiff:legacy_diff:legacy-d1".to_string(),
                json!({
                    "originalModelDiffWrtV0": [
                        {
                            "original": {"startLineNumber": 1, "endLineNumberExclusive": 2},
                            "modified": ["old line"]
                        }
                    ],
                    "newModelDiffWrtV0": [
                        {
                            "original": {"startLineNumber": 1, "endLineNumberExclusive": 2},
                            "modified": ["new line 1", "new line 2"]
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.legacy_sessions_considered, 1);
        assert_eq!(summary.legacy_entries_inspected, 1);
        assert_eq!(summary.legacy_diff_rows_found, 1);
        assert_eq!(summary.legacy_ops_upserted, 1);
        assert_eq!(summary.legacy_parse_errors, 0);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write' AND session_id='legacy_diff'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, LEGACY_DIFF_PARSER_NAME);
        assert_eq!(op_row.1, 2);
        assert_eq!(op_row.2, 1);

        let hash_count: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change_line_hashes h
             JOIN fact_session_code_change c ON c.id = h.code_change_id
             WHERE c.provider='cursor' AND c.session_id='legacy_diff'",
            [],
            |r| r.get(0),
        )?;
        assert!(hash_count > 0);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn legacy_content_fallback_uses_previous_accepted_content() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("legacy_content");
        let rows = vec![(
            "composerData:legacy_content".to_string(),
            json!({
                "composerId": "legacy_content",
                "createdAt": 1772694178155i64,
                "lastUpdatedAt": 1772694194894i64,
                "codeBlockData": {
                    "file:///tmp/repo/legacy.txt": [
                        {
                            "status": "accepted",
                            "version": 0,
                            "codeBlockIdx": 0,
                            "bubbleId": "b0",
                            "content": "old line\nsame line\n",
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                        },
                        {
                            "status": "accepted",
                            "version": 1,
                            "codeBlockIdx": 0,
                            "bubbleId": "b1",
                            "content": "new line\nsame line\n",
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                        }
                    ]
                }
            })
            .to_string(),
        )];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.legacy_ops_upserted, 1);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind = 'tool_write' AND session_id='legacy_content'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, LEGACY_CONTENT_PARSER_NAME);
        assert_eq!(op_row.1, 1);
        assert_eq!(op_row.2, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn inline_wins_over_legacy_for_same_file() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("inline_wins_legacy");
        let rows = vec![
            (
                "composerData:legacy_same".to_string(),
                json!({
                    "composerId": "legacy_same",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 1,
                    "codeBlockData": {
                        "file:///tmp/repo/README.md": [
                            {
                                "status": "accepted",
                                "diffId": "legacy-same-d1",
                                "version": 0,
                                "codeBlockIdx": 0,
                                "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"}
                            }
                        ]
                    }
                })
                .to_string(),
            ),
            (
                "inlineDiffUndoRedo-legacy-same".to_string(),
                json!({
                    "composerMetadata": {"composerId": "legacy_same"},
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/README.md"},
                    "createdAt": 1772694300812i64,
                    "newTextLines": ["new inline"],
                    "changes": [
                        {
                            "removedTextLines": ["old inline"],
                            "addedRange": {"startLineNumber": 1, "endLineNumberExclusive": 2}
                        }
                    ]
                })
                .to_string(),
            ),
            (
                "codeBlockDiff:legacy_same:legacy-same-d1".to_string(),
                json!({
                    "originalModelDiffWrtV0": [
                        {
                            "original": {"startLineNumber": 1, "endLineNumberExclusive": 2},
                            "modified": ["old legacy"]
                        }
                    ],
                    "newModelDiffWrtV0": [
                        {
                            "original": {"startLineNumber": 1, "endLineNumberExclusive": 2},
                            "modified": ["new legacy"]
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.legacy_ops_upserted, 0);

        let op_row: String = analytics.query_row(
            "SELECT parser_name
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='legacy_same'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, INLINE_PARSER_NAME);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn new_schema_pathless_partial_prefers_inline_removed_line_overlap() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_overlap");
        let rows = vec![
            (
                "composerData:new_schema_overlap".to_string(),
                json!({
                    "composerId": "new_schema_overlap",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": {
                        "file:///tmp/repo/src/config.py": {"content": "old config\n"},
                        "file:///tmp/repo/docs/readme.md": {"content": "other old\n"}
                    },
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "inlineDiff:overlap:config".to_string(),
                json!({
                    "diffId": "diff-config",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/src/config.py", "external": "file:///tmp/repo/src/config.py"},
                    "originalTextLines": ["old config"],
                    "composerMetadata": {
                        "composerId": "new_schema_overlap",
                        "toolCallId": "tool-config"
                    }
                })
                .to_string(),
            ),
            (
                "inlineDiff:overlap:readme".to_string(),
                json!({
                    "diffId": "diff-readme",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/docs/readme.md", "external": "file:///tmp/repo/docs/readme.md"},
                    "originalTextLines": ["other old"],
                    "composerMetadata": {
                        "composerId": "new_schema_overlap",
                        "toolCallId": "tool-readme"
                    }
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_overlap:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["old config"], "addedLines": ["new config"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: (String, String, i64, i64) = analytics.query_row(
            "SELECT abs_path, parser_name, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_overlap'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        assert_eq!(op_row.0, "/tmp/repo/src/config.py");
        assert_eq!(op_row.1, "cursor_new_schema_partial_fates_v1");
        assert_eq!(op_row.2, 1);
        assert_eq!(op_row.3, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn new_schema_pathless_partial_prefers_new_file_inline_hint() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_new_file");
        let rows = vec![
            (
                "composerData:new_schema_new_file".to_string(),
                json!({
                    "composerId": "new_schema_new_file",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": {
                        "file:///tmp/repo/src/new_file.py": {"content": ""},
                        "file:///tmp/repo/src/existing.py": {"content": "old existing\n"}
                    },
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "inlineDiff:new-file:new".to_string(),
                json!({
                    "diffId": "diff-new",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/src/new_file.py", "external": "file:///tmp/repo/src/new_file.py"},
                    "originalTextLines": [],
                    "composerMetadata": {
                        "composerId": "new_schema_new_file",
                        "toolCallId": "tool-new"
                    }
                })
                .to_string(),
            ),
            (
                "inlineDiff:new-file:existing".to_string(),
                json!({
                    "diffId": "diff-existing",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/src/existing.py", "external": "file:///tmp/repo/src/existing.py"},
                    "originalTextLines": ["old existing"],
                    "composerMetadata": {
                        "composerId": "new_schema_new_file",
                        "toolCallId": "tool-existing"
                    }
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_new_file:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": [], "addedLines": ["print('hello')"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_new_file'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, "/tmp/repo/src/new_file.py");

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn new_schema_add_only_partial_uses_repo_content_search() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_repo_add");
        let repo_root = temp_repo_path("new_schema_repo_add");
        create_git_repo(
            &repo_root,
            &[
                (
                    "src/alpha.py",
                    "def alpha():\n    return 1\nUNIQUE_REPO_ADDED_MARKER = True\n",
                ),
                ("src/beta.py", "def beta():\n    return 2\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");
        let original_states = original_file_states(&[
            (&alpha, "def alpha():\n    return 1\n"),
            (&beta, "def beta():\n    return 2\n"),
        ]);

        let rows = vec![
            (
                "composerData:new_schema_repo_add".to_string(),
                json!({
                    "composerId": "new_schema_repo_add",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": original_states,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_repo_add:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": [], "addedLines": ["UNIQUE_REPO_ADDED_MARKER = True"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_repo_add'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, canonical_repo_path(&alpha));

        cleanup(&source);
        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn new_schema_removal_only_partial_uses_original_file_states_history() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_original_state_remove");
        let repo_root = temp_repo_path("new_schema_original_state_remove");
        create_git_repo(
            &repo_root,
            &[
                ("src/alpha.py", "def alpha():\n    return current_alpha\n"),
                ("src/beta.py", "def beta():\n    return current_beta\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");
        let original_states = original_file_states(&[
            (&alpha, "legacy_unique_removed_line\nshared line\n"),
            (&beta, "different line\nshared line\n"),
        ]);

        let rows = vec![
            (
                "composerData:new_schema_original_state_remove".to_string(),
                json!({
                    "composerId": "new_schema_original_state_remove",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": original_states,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_original_state_remove:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["legacy_unique_removed_line"], "addedLines": []}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_original_state_remove'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, canonical_repo_path(&alpha));

        cleanup(&source);
        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn new_schema_mixed_partial_combines_history_and_repo_content() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_repo_mixed");
        let repo_root = temp_repo_path("new_schema_repo_mixed");
        create_git_repo(
            &repo_root,
            &[
                (
                    "src/alpha.py",
                    "def alpha():\n    return 1\nMIXED_ADDED_MARKER = True\n",
                ),
                ("src/beta.py", "def beta():\n    return 2\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");
        let original_states = original_file_states(&[
            (&alpha, "legacy_mixed_removed_line\nalpha stable\n"),
            (&beta, "beta stable\n"),
        ]);

        let rows = vec![
            (
                "composerData:new_schema_repo_mixed".to_string(),
                json!({
                    "composerId": "new_schema_repo_mixed",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": original_states,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_repo_mixed:partial-1".to_string(),
                json!({
                    "fates": [
                        {
                            "fate": "accepted",
                            "removedLines": ["legacy_mixed_removed_line"],
                            "addedLines": ["MIXED_ADDED_MARKER = True"]
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_repo_mixed'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, canonical_repo_path(&alpha));

        cleanup(&source);
        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn new_schema_repo_content_stays_unresolved_for_boilerplate_matches() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_repo_boilerplate");
        let repo_root = temp_repo_path("new_schema_repo_boilerplate");
        create_git_repo(
            &repo_root,
            &[
                ("src/alpha.py", "COMMON_SHARED_CONFIG = True\n"),
                ("src/beta.py", "COMMON_SHARED_CONFIG = True\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");
        let original_states = original_file_states(&[(&alpha, "alpha\n"), (&beta, "beta\n")]);

        let rows = vec![
            (
                "composerData:new_schema_repo_boilerplate".to_string(),
                json!({
                    "composerId": "new_schema_repo_boilerplate",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": original_states,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_repo_boilerplate:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": [], "addedLines": ["COMMON_SHARED_CONFIG = True"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 0);
        assert_eq!(summary.parse_errors, 1);

        let writes: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_repo_boilerplate'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(writes, 0);

        cleanup(&source);
        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn new_schema_repo_drift_falls_back_to_historical_removed_lines() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_repo_drift");
        let repo_root = temp_repo_path("new_schema_repo_drift");
        create_git_repo(
            &repo_root,
            &[
                ("src/alpha.py", "def alpha():\n    return drifted_alpha\n"),
                ("src/beta.py", "def beta():\n    return drifted_beta\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");
        let original_states = original_file_states(&[
            (&alpha, "legacy_unique_drift_line\nalpha stable\n"),
            (&beta, "beta stable\n"),
        ]);

        let rows = vec![
            (
                "composerData:new_schema_repo_drift".to_string(),
                json!({
                    "composerId": "new_schema_repo_drift",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 2,
                    "originalFileStates": original_states,
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_repo_drift:partial-1".to_string(),
                json!({
                    "fates": [
                        {
                            "fate": "accepted",
                            "removedLines": ["legacy_unique_drift_line"],
                            "addedLines": ["ADDED_LINE_MISSING_FROM_REPO = True"]
                        }
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: String = analytics.query_row(
            "SELECT abs_path
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_repo_drift'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(op_row, canonical_repo_path(&alpha));

        cleanup(&source);
        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn new_schema_ambiguous_partial_records_error_without_dropping_resolved_rows() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("new_schema_partial_mix");
        let rows = vec![
            (
                "composerData:new_schema_partial_mix".to_string(),
                json!({
                    "composerId": "new_schema_partial_mix",
                    "createdAt": 1772694178155i64,
                    "lastUpdatedAt": 1772694194894i64,
                    "filesChangedCount": 3,
                    "originalFileStates": {
                        "file:///tmp/repo/src/config.py": {"content": "old config\n"},
                        "file:///tmp/repo/docs/alpha.md": {"content": "shared old\n"},
                        "file:///tmp/repo/docs/beta.md": {"content": "shared old\n"}
                    },
                    "codeBlockData": {}
                })
                .to_string(),
            ),
            (
                "inlineDiff:mix:config".to_string(),
                json!({
                    "diffId": "diff-config",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/src/config.py", "external": "file:///tmp/repo/src/config.py"},
                    "originalTextLines": ["old config"],
                    "composerMetadata": {
                        "composerId": "new_schema_partial_mix",
                        "toolCallId": "tool-config"
                    }
                })
                .to_string(),
            ),
            (
                "inlineDiff:mix:alpha".to_string(),
                json!({
                    "diffId": "diff-alpha",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/docs/alpha.md", "external": "file:///tmp/repo/docs/alpha.md"},
                    "originalTextLines": ["shared old"],
                    "composerMetadata": {
                        "composerId": "new_schema_partial_mix",
                        "toolCallId": "tool-alpha"
                    }
                })
                .to_string(),
            ),
            (
                "inlineDiff:mix:beta".to_string(),
                json!({
                    "diffId": "diff-beta",
                    "uri": {"scheme": "file", "fsPath": "/tmp/repo/docs/beta.md", "external": "file:///tmp/repo/docs/beta.md"},
                    "originalTextLines": ["shared old"],
                    "composerMetadata": {
                        "composerId": "new_schema_partial_mix",
                        "toolCallId": "tool-beta"
                    }
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_partial_mix:partial-1".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["old config"], "addedLines": ["new config"]}
                    ]
                })
                .to_string(),
            ),
            (
                "codeBlockPartialInlineDiffFates:new_schema_partial_mix:partial-2".to_string(),
                json!({
                    "fates": [
                        {"fate": "accepted", "removedLines": ["shared old"], "addedLines": ["shared new"]}
                    ]
                })
                .to_string(),
            ),
        ];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 1);

        let writes: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='new_schema_partial_mix'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(writes, 1);

        let parse_errs: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM change_parse_errors
             WHERE provider='cursor' AND session_id='new_schema_partial_mix' AND parser_name='cursor_new_schema_partial_fates_v1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(parse_errs, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn new_schema_content_index_is_built_only_for_ambiguous_rows() -> Result<()> {
        let repo_root = temp_repo_path("new_schema_cache");
        create_git_repo(
            &repo_root,
            &[
                (
                    "src/alpha.py",
                    "def alpha():\n    return 1\nCACHE_SCAN_TARGET = True\n",
                ),
                ("src/beta.py", "def beta():\n    return 2\n"),
            ],
        )?;
        let alpha = repo_root.join("src/alpha.py");
        let beta = repo_root.join("src/beta.py");

        let session = CandidateSession {
            info: SessionInfo {
                provider: "cursor".to_string(),
                session_id: "cache_session".to_string(),
                source_file: "/tmp/cache.vscdb".to_string(),
                session_cwd: None,
                last_seen_at: None,
            },
            checkpoint_paths: HashSet::new(),
            strong_path_hints: HashSet::new(),
            weak_path_hints: [path_to_string(&alpha), path_to_string(&beta)]
                .into_iter()
                .collect(),
            original_state_contents: HashMap::new(),
            total_lines_added: None,
            total_lines_removed: None,
            partial_targets: Vec::new(),
            legacy_targets: Vec::new(),
        };
        let inline_hints = vec![
            NewInlineHint {
                call_id: "tool-alpha".to_string(),
                abs_path: path_to_string(&alpha),
                timestamp: None,
                original_text_lines: vec!["old alpha".to_string()],
            },
            NewInlineHint {
                call_id: "tool-beta".to_string(),
                abs_path: path_to_string(&beta),
                timestamp: None,
                original_text_lines: vec!["old beta".to_string()],
            },
        ];
        let prior_seen_paths = HashSet::new();
        let mut repo_content_indexes = HashMap::new();

        let resolved_without_scan = resolve_new_schema_partial_path(
            &session,
            "partial-resolved",
            &json!({
                "fates": [
                    {
                        "fate": "accepted",
                        "removedLines": ["old alpha"],
                        "addedLines": ["new alpha"]
                    }
                ]
            }),
            &inline_hints,
            &prior_seen_paths,
            &mut repo_content_indexes,
        )
        .map_err(anyhow::Error::msg)?;
        assert_eq!(
            resolved_without_scan.map(|resolved| resolved.abs_path),
            Some(path_to_string(&alpha))
        );
        assert!(repo_content_indexes.is_empty());

        let resolved_with_scan = resolve_new_schema_partial_path(
            &session,
            "partial-ambiguous",
            &json!({
                "fates": [
                    {
                        "fate": "accepted",
                        "removedLines": [],
                        "addedLines": ["CACHE_SCAN_TARGET = True"]
                    }
                ]
            }),
            &inline_hints,
            &prior_seen_paths,
            &mut repo_content_indexes,
        )
        .map_err(anyhow::Error::msg)?;
        assert_eq!(
            resolved_with_scan.map(|resolved| resolved.abs_path),
            Some(canonical_repo_path(&alpha))
        );
        assert_eq!(repo_content_indexes.len(), 1);

        cleanup_dir(&repo_root);
        Ok(())
    }

    #[test]
    fn single_file_missing_legacy_diff_uses_session_totals_fallback() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("legacy_single_file_totals");
        let rows = vec![(
            "composerData:legacy_single_file_totals".to_string(),
            json!({
                "composerId": "legacy_single_file_totals",
                "createdAt": 1772694178155i64,
                "lastUpdatedAt": 1772694194894i64,
                "totalLinesAdded": 5,
                "totalLinesRemoved": 2,
                "originalFileStates": {
                    "file:///tmp/repo/legacy.txt": {"content": "before"}
                },
                "codeBlockData": {
                    "file:///tmp/repo/legacy.txt": [
                        {
                            "status": "accepted",
                            "diffId": "missing-diff-row",
                            "version": 0,
                            "codeBlockIdx": 0,
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                        }
                    ]
                }
            })
            .to_string(),
        )];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(summary.parse_errors, 0);

        let op_row: (String, i64, i64, i64) = analytics.query_row(
            "SELECT parser_name, lines_added, lines_removed, before_known
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='legacy_single_file_totals'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        assert_eq!(op_row.0, "cursor_legacy_session_totals_v1");
        assert_eq!(op_row.1, 5);
        assert_eq!(op_row.2, 2);
        assert_eq!(op_row.3, 0);

        let hash_count: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change_line_hashes h
             JOIN fact_session_code_change c ON c.id = h.code_change_id
             WHERE c.provider='cursor' AND c.session_id='legacy_single_file_totals'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(hash_count, 0);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn multi_file_missing_legacy_diffs_record_one_parse_error_per_file() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("legacy_multi_file_missing");
        let rows = vec![(
            "composerData:legacy_multi_file_missing".to_string(),
            json!({
                "composerId": "legacy_multi_file_missing",
                "createdAt": 1772694178155i64,
                "lastUpdatedAt": 1772694194894i64,
                "totalLinesAdded": 7,
                "totalLinesRemoved": 3,
                "originalFileStates": {
                    "file:///tmp/repo/a.txt": {"content": "before a"},
                    "file:///tmp/repo/b.txt": {"content": "before b"}
                },
                "codeBlockData": {
                    "file:///tmp/repo/a.txt": [
                        {
                            "status": "accepted",
                            "diffId": "missing-a-1",
                            "version": 0,
                            "codeBlockIdx": 0,
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/a.txt"}
                        },
                        {
                            "status": "accepted",
                            "diffId": "missing-a-2",
                            "version": 1,
                            "codeBlockIdx": 0,
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/a.txt"}
                        }
                    ],
                    "file:///tmp/repo/b.txt": [
                        {
                            "status": "accepted",
                            "diffId": "missing-b-1",
                            "version": 0,
                            "codeBlockIdx": 0,
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/b.txt"}
                        }
                    ]
                }
            })
            .to_string(),
        )];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 0);
        assert_eq!(summary.parse_errors, 2);
        assert_eq!(summary.legacy_parse_errors, 2);

        let writes: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change
             WHERE provider='cursor' AND source_kind='tool_write' AND session_id='legacy_multi_file_missing'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(writes, 0);

        let parse_errs: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM change_parse_errors
             WHERE provider='cursor' AND session_id='legacy_multi_file_missing' AND parser_name=?1",
            params![LEGACY_DIFF_PARSER_NAME],
            |r| r.get(0),
        )?;
        assert_eq!(parse_errs, 2);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn missing_legacy_diff_records_parse_error_when_no_content_fallback_exists() -> Result<()> {
        let mut analytics = Connection::open_in_memory()?;
        init_change_intel_schema(&analytics)?;

        let source = temp_db_path("legacy_missing_diff");
        let rows = vec![(
            "composerData:legacy_missing".to_string(),
            json!({
                "composerId": "legacy_missing",
                "createdAt": 1772694178155i64,
                "lastUpdatedAt": 1772694194894i64,
                "codeBlockData": {
                    "file:///tmp/repo/legacy.txt": [
                        {
                            "status": "accepted",
                            "diffId": "missing-diff-row",
                            "version": 0,
                            "codeBlockIdx": 0,
                            "uri": {"scheme": "file", "fsPath": "/tmp/repo/legacy.txt"}
                        }
                    ]
                }
            })
            .to_string(),
        )];

        create_cursor_db(&source, &rows)?;

        let summary = ingest_cursor_code_changes_from_path(&mut analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 0);
        assert_eq!(summary.parse_errors, 1);
        assert_eq!(summary.legacy_parse_errors, 1);

        let parse_errs: i64 = analytics.query_row(
            "SELECT COUNT(*) FROM change_parse_errors WHERE parser_name = ?1",
            params![LEGACY_DIFF_PARSER_NAME],
            |r| r.get(0),
        )?;
        assert_eq!(parse_errs, 1);

        cleanup(&source);
        Ok(())
    }
}
