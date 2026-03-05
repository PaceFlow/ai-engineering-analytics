use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::change_intel::line_hash::hash_line;
use crate::change_intel::path_resolver::{detect_repo_root, to_rel_path};
use crate::change_intel::pipeline::ProviderCodeChangeSummary;
use crate::change_intel::storage;
use crate::change_intel::types::{ChangeOpCandidate, LineHashCount, LineSide, ParseError, SessionInfo, WriteMode};
use crate::path_utils::strip_file_scheme;

const CURSOR_CURSOR_NAMESPACE: &str = "cursor_core_v1";
const INLINE_PARSER_NAME: &str = "cursor_inline_undo_v1";
const INLINE_PARSER_VERSION: &str = "1";
const PARTIAL_PARSER_NAME: &str = "cursor_partial_fates_v1";
const PARTIAL_PARSER_VERSION: &str = "1";

#[derive(Debug, Clone)]
struct CandidateSession {
    info: SessionInfo,
    checkpoint_paths: HashSet<String>,
    partial_targets: Vec<PartialTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartialTarget {
    partial_id: String,
    abs_path: String,
}

pub fn ingest_cursor_code_changes(
    conn: &Connection,
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

pub(crate) fn ingest_cursor_code_changes_from_path(
    conn: &Connection,
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
        if let Some(cursor) = cursor {
            if cursor.file_mtime == mtime && cursor.file_size == size {
                summary.sources_skipped += 1;
                return Ok(summary);
            }
        }
    }

    let vscdb = Connection::open_with_flags(
        vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let mut sessions = collect_candidate_sessions(&vscdb, &source_file)?;
    populate_checkpoint_paths(&vscdb, &mut sessions)?;

    for session in sessions.values() {
        storage::upsert_change_session(conn, &session.info)?;
    }

    let mut seen_paths_by_session: HashMap<String, HashSet<String>> = HashMap::new();

    ingest_inline_undo_rows(
        conn,
        &vscdb,
        &source_file,
        &sessions,
        &mut seen_paths_by_session,
        &mut summary,
        verbose,
    )?;

    ingest_partial_fates_fallback(
        conn,
        &vscdb,
        &source_file,
        &sessions,
        &mut seen_paths_by_session,
        &mut summary,
        verbose,
    )?;

    if let Some((mtime, size)) = sig {
        storage::upsert_ingest_cursor(conn, CURSOR_CURSOR_NAMESPACE, &source_file, mtime, size)?;
    }

    Ok(summary)
}

fn collect_candidate_sessions(
    vscdb: &Connection,
    source_file: &str,
) -> Result<HashMap<String, CandidateSession>> {
    let mut out = HashMap::new();

    let mut stmt = vscdb.prepare(
        "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'",
    )?;

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

        let started_at = parsed
            .get("createdAt")
            .and_then(|v| v.as_i64())
            .and_then(ms_to_iso);
        let last_seen_at = parsed
            .get("lastUpdatedAt")
            .and_then(|v| v.as_i64())
            .and_then(ms_to_iso);

        let partial_targets = extract_partial_targets(&parsed);

        out.insert(
            composer_id.clone(),
            CandidateSession {
                info: SessionInfo {
                    provider: "cursor".to_string(),
                    session_id: composer_id,
                    source_file: source_file.to_string(),
                    session_cwd: None,
                    started_at,
                    last_seen_at,
                },
                checkpoint_paths: HashSet::new(),
                partial_targets,
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

    let mut stmt = vscdb.prepare(
        "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'checkpointId:%'",
    )?;

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
            let abs_path = file
                .get("uri")
                .and_then(extract_file_path_from_uri_value);
            if let Some(path) = abs_path {
                session.checkpoint_paths.insert(path);
            }
        }
    }

    Ok(())
}

fn ingest_inline_undo_rows(
    conn: &Connection,
    vscdb: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
    summary: &mut ProviderCodeChangeSummary,
    verbose: bool,
) -> Result<()> {
    let mut stmt = vscdb.prepare(
        "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'inlineDiffUndoRedo-%'",
    )?;

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

        let op_id = storage::upsert_change_op(conn, &op)?;
        storage::replace_line_hashes(conn, op_id, &op.line_hashes)?;
        summary.ops_upserted += 1;

        seen_paths_by_session
            .entry(composer_id)
            .or_default()
            .insert(abs_path);
    }

    Ok(())
}

fn ingest_partial_fates_fallback(
    conn: &Connection,
    vscdb: &Connection,
    source_file: &str,
    sessions: &HashMap<String, CandidateSession>,
    seen_paths_by_session: &mut HashMap<String, HashSet<String>>,
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

            let op = match build_partial_fates_op(&partial_key, &parsed, &target.abs_path, session) {
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

            let op_id = storage::upsert_change_op(conn, &op)?;
            storage::replace_line_hashes(conn, op_id, &op.line_hashes)?;
            summary.ops_upserted += 1;

            seen_paths_by_session
                .entry(composer_id.clone())
                .or_default()
                .insert(target.abs_path.clone());
        }
    }

    Ok(())
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
        parser_version: INLINE_PARSER_VERSION.to_string(),
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
        parser_version: PARTIAL_PARSER_VERSION.to_string(),
        line_hashes,
    }))
}

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
        eprintln!(
            "[change-intel] {} {} {}",
            parser_name,
            call_id,
            error
        );
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
}

fn extract_file_path_from_uri_value(uri: &Value) -> Option<String> {
    match uri {
        Value::Object(map) => {
            if let Some(scheme) = map.get("scheme").and_then(|v| v.as_str()) {
                if scheme != "file" {
                    return None;
                }
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

fn cursor_vscdb_path() -> Result<Option<PathBuf>> {
    #[cfg(target_os = "macos")]
    {
        let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
        let p = home
            .join("Library")
            .join("Application Support")
            .join("Cursor")
            .join("User")
            .join("globalStorage")
            .join("state.vscdb");
        if p.exists() {
            return Ok(Some(p));
        }
    }

    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    let p = home
        .join(".config")
        .join("Cursor")
        .join("User")
        .join("globalStorage")
        .join("state.vscdb");
    if p.exists() {
        return Ok(Some(p));
    }

    Ok(None)
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
        std::env::temp_dir().join(format!("vca_cursor_{}_{}.db", label, n))
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
        let analytics = Connection::open_in_memory()?;
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

        let summary1 = ingest_cursor_code_changes_from_path(&analytics, &source, false)?;
        assert_eq!(summary1.provider, "cursor");
        assert_eq!(summary1.sources_discovered, 1);
        assert_eq!(summary1.sources_skipped, 0);
        assert_eq!(summary1.ops_upserted, 1);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, added_lines, removed_lines FROM change_ops WHERE provider='cursor'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, INLINE_PARSER_NAME);
        assert_eq!(op_row.1, 1);
        assert_eq!(op_row.2, 1);

        let summary2 = ingest_cursor_code_changes_from_path(&analytics, &source, false)?;
        assert_eq!(summary2.sources_skipped, 1);

        let count: i64 = analytics.query_row(
            "SELECT COUNT(*) FROM change_ops WHERE provider='cursor'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 1);

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn invalid_added_range_records_parse_error() -> Result<()> {
        let analytics = Connection::open_in_memory()?;
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

        let summary = ingest_cursor_code_changes_from_path(&analytics, &source, false)?;
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
        let analytics = Connection::open_in_memory()?;
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

        let summary = ingest_cursor_code_changes_from_path(&analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 2);

        let mut stmt = analytics.prepare(
            "SELECT session_id, parser_name, added_lines, removed_lines FROM change_ops WHERE provider='cursor' ORDER BY session_id",
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
        assert_eq!(collected[0], ("dict_sess".to_string(), PARTIAL_PARSER_NAME.to_string(), 2, 1));
        assert_eq!(collected[1], ("list_sess".to_string(), PARTIAL_PARSER_NAME.to_string(), 1, 1));

        cleanup(&source);
        Ok(())
    }

    #[test]
    fn inline_wins_over_partial_for_same_file() -> Result<()> {
        let analytics = Connection::open_in_memory()?;
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

        let summary = ingest_cursor_code_changes_from_path(&analytics, &source, false)?;
        assert_eq!(summary.ops_upserted, 1);

        let op_row: (String, i64, i64) = analytics.query_row(
            "SELECT parser_name, added_lines, removed_lines FROM change_ops WHERE provider='cursor' AND session_id='same_sess'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(op_row.0, INLINE_PARSER_NAME);
        assert_eq!(op_row.1, 1);
        assert_eq!(op_row.2, 1);

        cleanup(&source);
        Ok(())
    }
}
