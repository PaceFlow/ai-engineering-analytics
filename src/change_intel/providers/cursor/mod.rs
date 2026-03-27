use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
use crate::ingest_progress::IngestProgressObserver;
use crate::path_utils::strip_file_scheme;

const CURSOR_CURSOR_NAMESPACE: &str = "cursor_core_v1";
const INLINE_PARSER_NAME: &str = "cursor_inline_undo_v1";
const PARTIAL_PARSER_NAME: &str = "cursor_partial_fates_v1";
const LEGACY_DIFF_PARSER_NAME: &str = "cursor_legacy_code_block_diff_v1";
const LEGACY_CONTENT_PARSER_NAME: &str = "cursor_legacy_code_block_content_v1";

#[derive(Debug, Clone)]
struct CandidateSession {
    info: SessionInfo,
    checkpoint_paths: HashSet<String>,
    partial_targets: Vec<PartialTarget>,
    legacy_targets: Vec<LegacyTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartialTarget {
    partial_id: String,
    abs_path: String,
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

    let mut sessions = collect_candidate_sessions(&vscdb, &source_file)?;
    populate_checkpoint_paths(&vscdb, &mut sessions)?;

    for session in sessions.values() {
        storage::upsert_change_session(&tx, &session.info)?;
    }

    let mut seen_paths_by_session: HashMap<String, HashSet<String>> = HashMap::new();
    let mut ops = Vec::new();

    ingest_inline_undo_rows(
        &tx,
        &vscdb,
        &source_file,
        &sessions,
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

    ingest_legacy_code_block_fallback(
        &tx,
        &vscdb,
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
        let (key, raw) = row?;
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
                partial_targets,
                legacy_targets,
            },
        );
    }

    Ok(out)
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
        let (key, raw) = row?;
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

#[expect(
    clippy::too_many_arguments,
    reason = "cursor ingestion threads database handles, mutable accumulators, and source metadata through one helper"
)]
fn ingest_inline_undo_rows(
    conn: &Connection,
    vscdb: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    ops: &mut Vec<ChangeOpCandidate>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    let mut stmt = vscdb
        .prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'inlineDiffUndoRedo-%'")?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = row?;
        let Some(raw) = raw else {
            continue;
        };
        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let composer_id = parsed
            .get("composerMetadata")
            .and_then(|v| v.get("composerId"))
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);
        let Some(composer_id) = composer_id else {
            continue;
        };

        let Some(session) = sessions.get(&composer_id) else {
            continue;
        };

        summary.tool_calls_inspected += 1;

        let abs_path = match parsed.get("uri").and_then(extract_file_path_from_uri_value) {
            Some(path) => path,
            None => continue,
        };

        if !session.checkpoint_paths.is_empty() && !session.checkpoint_paths.contains(&abs_path) {
            continue;
        }

        let op = match build_inline_change_op(&key, &parsed, &abs_path, session) {
            Ok(op) => op,
            Err(err) => {
                insert_parse_error(
                    conn,
                    summary,
                    &session.info.session_id,
                    source_file,
                    &key,
                    parsed
                        .get("createdAt")
                        .and_then(|v| v.as_i64())
                        .and_then(ms_to_iso)
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
            .entry(composer_id)
            .or_default()
            .insert(abs_path);
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
    reason = "legacy fallback ingestion carries the same shared parser state as the other cursor ingestion helpers"
)]
fn ingest_legacy_code_block_fallback(
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
        if session.legacy_targets.is_empty() {
            continue;
        }

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

            let op = match build_legacy_diff_op(vscdb, composer_id, &target, session) {
                Ok(Some(op)) => {
                    summary.legacy_diff_rows_found += 1;
                    Some(op)
                }
                Ok(None) => match build_legacy_content_fallback_op(
                    previous_content_by_path
                        .get(&target.abs_path)
                        .map(String::as_str),
                    &target,
                    session,
                ) {
                    Ok(op) => op,
                    Err(content_err) => {
                        insert_legacy_parse_error(
                            conn,
                            summary,
                            &session.info.session_id,
                            source_file,
                            target
                                .diff_id
                                .as_deref()
                                .or(target.bubble_id.as_deref())
                                .unwrap_or("legacy-code-block"),
                            target
                                .timestamp
                                .clone()
                                .or_else(|| session.info.last_seen_at.clone()),
                            LEGACY_CONTENT_PARSER_NAME,
                            content_err,
                            verbose,
                        )?;
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
                    Ok(Some(op)) => Some(op),
                    Ok(None) => {
                        insert_legacy_parse_error(
                            conn,
                            summary,
                            &session.info.session_id,
                            source_file,
                            target
                                .diff_id
                                .as_deref()
                                .or(target.bubble_id.as_deref())
                                .unwrap_or("legacy-code-block"),
                            target
                                .timestamp
                                .clone()
                                .or_else(|| session.info.last_seen_at.clone()),
                            LEGACY_DIFF_PARSER_NAME,
                            diff_err,
                            verbose,
                        )?;
                        None
                    }
                    Err(content_err) => {
                        insert_legacy_parse_error(
                            conn,
                            summary,
                            &session.info.session_id,
                            source_file,
                            target
                                .diff_id
                                .as_deref()
                                .or(target.bubble_id.as_deref())
                                .unwrap_or("legacy-code-block"),
                            target
                                .timestamp
                                .clone()
                                .or_else(|| session.info.last_seen_at.clone()),
                            LEGACY_CONTENT_PARSER_NAME,
                            format!("{}; fallback failed: {}", diff_err, content_err),
                            verbose,
                        )?;
                        None
                    }
                },
            };

            if let Some(op) = op {
                ops.push(op);
                summary.legacy_ops_upserted += 1;
            }

            if let Some(content) = &target.content {
                previous_content_by_path.insert(target.abs_path.clone(), content.clone());
            }
        }
    }

    Ok(())
}

fn build_legacy_diff_op(
    vscdb: &Connection,
    composer_id: &str,
    target: &LegacyTarget,
    session: &CandidateSession,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let Some(diff_id) = &target.diff_id else {
        return Ok(None);
    };

    let diff_key = format!("codeBlockDiff:{}:{}", composer_id, diff_id);
    let raw_payload: Option<String> = vscdb
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            params![diff_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;

    let Some(raw_payload) = raw_payload else {
        return Err("legacy codeBlockDiff row not found".to_string());
    };

    let payload: Value = serde_json::from_str(&raw_payload)
        .map_err(|e| format!("invalid legacy codeBlockDiff JSON: {}", e))?;

    let removed_lines = extract_legacy_diff_lines(&payload, "originalModelDiffWrtV0")?;
    let added_lines = extract_legacy_diff_lines(&payload, "newModelDiffWrtV0")?;

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
        repo_root: repo_root.map(|p| p.to_string_lossy().to_string()),
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
        repo_root: repo_root.map(|p| p.to_string_lossy().to_string()),
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
        repo_root: repo_root.map(|p| p.to_string_lossy().to_string()),
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
                return Some(fs_path.to_string());
            }

            if let Some(external) = map.get("external").and_then(|v| v.as_str()) {
                return extract_file_path_from_string(external);
            }

            if let Some(path) = map.get("path").and_then(|v| v.as_str()) {
                return Some(path.to_string());
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
    Some(raw.to_string())
}

fn composer_id_from_checkpoint_key(key: &str) -> Option<&str> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "checkpointId" {
        return None;
    }
    parts.next()
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
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::change_intel::schema::init_change_intel_schema;

    fn temp_db_path(label: &str) -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("aea_cursor_{}_{}.db", label, n))
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
