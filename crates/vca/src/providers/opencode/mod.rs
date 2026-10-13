use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, params};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::db::session_store::{
    self, AcceptedChangeRecord, AgentKind, MessageRecord, SessionRecord, ToolCallRecord,
    UsageRecord,
};
use crate::ingest_progress::IngestProgressObserver;

const PROVIDER: &str = "opencode";

#[derive(Debug, Clone)]
struct OpenCodeSession {
    id: String,
    directory: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
    model_name: Option<String>,
    parent_id: Option<String>,
    title: Option<String>,
    fingerprint: String,
    messages: Vec<OpenCodeMessage>,
    usage_events: Vec<OpenCodeUsageEvent>,
    tool_calls: Vec<ToolCallRecord>,
}

#[derive(Debug, Clone)]
struct OpenCodeMessage {
    key: String,
    role: String,
    text: String,
    timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct OpenCodeUsageEvent {
    key: String,
    timestamp: Option<String>,
    model_name: Option<String>,
    provider_id: Option<String>,
    input_tokens: i64,
    cached_input_tokens: i64,
    cache_creation_tokens: i64,
    output_tokens: i64,
    reasoning_tokens: i64,
    total_tokens: i64,
    actual_cost_usd: Option<f64>,
}

#[derive(Debug, Clone)]
struct OpenCodeAcceptedChange {
    file_path: String,
    added_lines: i64,
    removed_lines: i64,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct OpenCodeDiffEntry {
    file: String,
    patch: String,
}

#[derive(Debug, Clone)]
struct MessageRow {
    id: String,
    role: Option<String>,
    timestamp: Option<String>,
    model_name: Option<String>,
    usage: Option<OpenCodeUsageEvent>,
}

pub fn plan_db_files() -> Result<Vec<PathBuf>> {
    plan_db_files_for(crate::app::AppContext::VCA)
}

pub fn plan_db_files_for(context: crate::app::AppContext) -> Result<Vec<PathBuf>> {
    let db_path = opencode_db_path_for(context)?;
    if db_path.exists() {
        Ok(vec![db_path])
    } else {
        Ok(Vec::new())
    }
}

pub fn opencode_db_path() -> Result<PathBuf> {
    opencode_db_path_for(crate::app::AppContext::VCA)
}

pub fn opencode_db_path_for(context: crate::app::AppContext) -> Result<PathBuf> {
    if let Some(path) = context.env("OPENCODE_DB_PATH") {
        return Ok(PathBuf::from(path));
    }

    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    Ok(home
        .join(".local")
        .join("share")
        .join("opencode")
        .join("opencode.db"))
}

pub fn ingest_planned_sessions(
    db: &Connection,
    db_paths: &[PathBuf],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    let mut total_rows = 0usize;

    for db_path in db_paths {
        if verbose {
            eprint!("  {:?} ... ", db_path);
        }

        match ingest_db(db_path, db) {
            Ok(0) => {
                if verbose {
                    eprintln!("skipped (already ingested or empty)");
                }
            }
            Ok(written) => {
                if verbose {
                    eprintln!("wrote {} rows", written);
                }
                total_rows += written;
            }
            Err(error) => {
                eprintln!("Warning: skipping {:?}: {}", db_path, error);
            }
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(&db_path.to_string_lossy());
        }
    }

    Ok(total_rows)
}

fn ingest_db(path: &Path, analytics: &Connection) -> Result<usize> {
    let sessions = parse_sessions(path)?;
    let source_path = path.to_string_lossy().to_string();
    let diff_root = path
        .parent()
        .map(|parent| parent.join("storage").join("session_diff"));
    let mut written = 0usize;

    for session in sessions {
        let diff_file = diff_root
            .as_deref()
            .map(|root| root.join(format!("{}.json", session.id)));
        let fingerprint = format!(
            "{}|{}",
            session.fingerprint,
            diff_file
                .as_deref()
                .and_then(session_store::file_fingerprint)
                .unwrap_or_default()
        );
        let source_key = format!("{source_path}#{}", session.id);
        if !session_store::source_needs_ingest(analytics, PROVIDER, &source_key, &fingerprint)? {
            continue;
        }

        let changes = match diff_root.as_deref() {
            Some(diff_root) => parse_accepted_changes(diff_root, &session)?,
            None => Vec::new(),
        };
        let record = session_record(&session, &source_path, changes);
        written += session_store::store_session_record(analytics, &record)?;
        session_store::mark_source_ingested(
            analytics,
            PROVIDER,
            &source_key,
            &fingerprint,
            &[session.id.as_str()],
        )?;
    }

    Ok(written)
}

fn session_record(
    session: &OpenCodeSession,
    source_path: &str,
    changes: Vec<OpenCodeAcceptedChange>,
) -> SessionRecord {
    let mut record = SessionRecord::new(PROVIDER, session.id.clone());
    record.source_path = Some(source_path.to_string());
    record.project_path = session.directory.clone();
    record.started_at = session.started_at.clone();
    record.ended_at = session.ended_at.clone().or(session.started_at.clone());
    record.title = session.title.clone();
    if let Some(parent) = session.parent_id.as_deref().filter(|id| !id.is_empty()) {
        record.parent_session_id = Some(parent.to_string());
        record.agent_kind = AgentKind::Subagent;
    }
    record.messages = session
        .messages
        .iter()
        .map(|message| MessageRecord {
            key: message.key.clone(),
            ts: message.timestamp.clone(),
            role: message.role.clone(),
            text: message.text.clone(),
            is_compaction_summary: false,
        })
        .collect();
    record.usage = session
        .usage_events
        .iter()
        .map(|usage| {
            let mut out = UsageRecord::new(usage.key.clone());
            out.ts = usage.timestamp.clone();
            out.model = usage.model_name.clone();
            out.provider_route = usage.provider_id.clone();
            out.input_tokens = usage.input_tokens;
            out.cache_read_tokens = usage.cached_input_tokens;
            out.cache_write_tokens = usage.cache_creation_tokens;
            out.output_tokens = usage.output_tokens;
            out.reasoning_tokens = usage.reasoning_tokens;
            out.reasoning_in_output = false;
            out.reported_cost_usd = usage.actual_cost_usd;
            out
        })
        .collect();
    record.tool_calls = session.tool_calls.clone();
    let change_ts = session.ended_at.clone().or(session.started_at.clone());
    record.accepted_changes = changes
        .into_iter()
        .map(|change| AcceptedChangeRecord {
            abs_path: change.file_path,
            lines_added: change.added_lines,
            lines_removed: change.removed_lines,
            ts: change_ts.clone(),
        })
        .collect();
    record.model = record
        .primary_model_from_usage()
        .or(session.model_name.clone());
    record
}

fn table_columns(source: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = source.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns)
}

fn parse_sessions(path: &Path) -> Result<Vec<OpenCodeSession>> {
    let source = open_readonly_db(path)?;
    let session_columns = table_columns(&source, "session")?;
    let optional = |name: &str| {
        if session_columns.iter().any(|column| column == name) {
            format!("s.{name}")
        } else {
            "NULL".to_string()
        }
    };
    let sql = format!(
        "SELECT s.id, s.directory, s.time_created, s.time_updated, p.worktree, {}, {}
         FROM session s
         LEFT JOIN project p ON p.id = s.project_id
         ORDER BY s.time_created, s.id",
        optional("parent_id"),
        optional("title")
    );
    let mut stmt = source.prepare(&sql)?;

    let rows = stmt
        .query_map([], |row| {
            let directory: Option<String> = row.get(1)?;
            let worktree: Option<String> = row.get(4)?;
            Ok((
                row.get::<_, String>(0)?,
                directory.or(worktree),
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut sessions = Vec::new();
    for (id, directory, created_ms, updated_ms, parent_id, title) in rows {
        let messages = parse_message_rows(&source, &id)?;
        let model_name = latest_model_name(&messages);
        let usage_events = messages
            .iter()
            .filter_map(|message| message.usage.clone())
            .collect();
        let (parts, tool_calls, part_count) = parse_parts(&source, &id)?;
        let fingerprint = format!("{updated_ms}:{}:{part_count}", messages.len());
        let visible_messages = build_visible_messages(messages, parts);
        sessions.push(OpenCodeSession {
            id,
            directory,
            started_at: ms_to_iso(created_ms),
            ended_at: ms_to_iso(updated_ms),
            model_name,
            parent_id,
            title,
            fingerprint,
            messages: visible_messages,
            usage_events,
            tool_calls,
        });
    }

    Ok(sessions)
}

pub(crate) fn open_readonly_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

fn parse_message_rows(source: &Connection, session_id: &str) -> Result<Vec<MessageRow>> {
    let mut stmt = source.prepare(
        "SELECT id, time_created, data
         FROM message
         WHERE session_id = ?1
         ORDER BY time_created, id",
    )?;

    stmt.query_map(params![session_id], |row| {
        let id: String = row.get(0)?;
        let created_ms: i64 = row.get(1)?;
        let data: String = row.get(2)?;
        let parsed: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        let role = parsed
            .get("role")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let model_name = parsed
            .get("modelID")
            .and_then(Value::as_str)
            .or_else(|| {
                parsed
                    .get("model")
                    .and_then(|value| value.get("modelID"))
                    .and_then(Value::as_str)
            })
            .map(ToOwned::to_owned);
        let mut usage = parse_opencode_usage(&parsed);
        let timestamp = parsed
            .get("time")
            .and_then(|time| time.get("completed").or_else(|| time.get("created")))
            .and_then(Value::as_i64)
            .and_then(ms_to_iso)
            .or_else(|| ms_to_iso(created_ms));
        if let Some(usage) = usage.as_mut() {
            usage.key = id.clone();
            usage.timestamp = timestamp;
            usage.model_name = model_name.clone();
            usage.provider_id = parsed
                .get("providerID")
                .or_else(|| {
                    parsed
                        .get("model")
                        .and_then(|model| model.get("providerID"))
                })
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }

        Ok(MessageRow {
            id,
            role,
            timestamp: ms_to_iso(created_ms),
            model_name,
            usage,
        })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn parse_opencode_usage(parsed: &Value) -> Option<OpenCodeUsageEvent> {
    if parsed.get("role").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    let tokens = parsed.get("tokens")?;
    let input_tokens = tokens.get("input").and_then(Value::as_i64).unwrap_or(0);
    let output_tokens = tokens.get("output").and_then(Value::as_i64).unwrap_or(0);
    let reasoning_tokens = tokens.get("reasoning").and_then(Value::as_i64).unwrap_or(0);
    let cache = tokens.get("cache").unwrap_or(&Value::Null);
    let cached_input_tokens = cache.get("read").and_then(Value::as_i64).unwrap_or(0);
    let cache_creation_tokens = cache.get("write").and_then(Value::as_i64).unwrap_or(0);
    let total_tokens = tokens.get("total").and_then(Value::as_i64).unwrap_or(
        input_tokens
            + output_tokens
            + reasoning_tokens
            + cached_input_tokens
            + cache_creation_tokens,
    );
    let actual_cost_usd = parsed
        .get("cost")
        .and_then(Value::as_f64)
        .filter(|value| *value > 0.0);

    Some(OpenCodeUsageEvent {
        key: String::new(),
        timestamp: None,
        model_name: None,
        provider_id: None,
        input_tokens,
        cached_input_tokens,
        cache_creation_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
        actual_cost_usd,
    })
}

type TextParts = HashMap<String, Vec<(String, String)>>;

/// Text parts per message plus tool calls, and the total number of parts.
fn parse_parts(
    source: &Connection,
    session_id: &str,
) -> Result<(TextParts, Vec<ToolCallRecord>, usize)> {
    let mut stmt = source.prepare(
        "SELECT message_id, data, id
         FROM part
         WHERE session_id = ?1
         ORDER BY time_created, id",
    )?;

    let mut parts: TextParts = HashMap::new();
    let mut tool_calls = Vec::new();
    let mut count = 0usize;
    let rows = stmt.query_map(params![session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    for row in rows {
        let (message_id, data, part_id) = row?;
        count += 1;
        let parsed: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        match parsed.get("type").and_then(Value::as_str) {
            Some("text") => {
                let Some(text) = parsed.get("text").and_then(Value::as_str) else {
                    continue;
                };
                if text.trim().is_empty() {
                    continue;
                }
                parts
                    .entry(message_id)
                    .or_default()
                    .push((part_id, text.to_string()));
            }
            Some("tool") => tool_calls.push(tool_call_from_part(&part_id, &parsed)),
            _ => {}
        }
    }

    Ok((parts, tool_calls, count))
}

fn tool_call_from_part(part_id: &str, part: &Value) -> ToolCallRecord {
    let state = part.get("state").unwrap_or(&Value::Null);
    let input = state.get("input").unwrap_or(&Value::Null);
    let text = |value: &Value, key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(ToOwned::to_owned)
    };
    let target = text(input, "filePath")
        .or_else(|| text(input, "file_path"))
        .or_else(|| text(input, "path"))
        .or_else(|| text(input, "command"))
        .or_else(|| text(input, "pattern"))
        .or_else(|| text(input, "url"))
        .or_else(|| text(input, "query"))
        .or_else(|| text(input, "description"));
    let output = state
        .get("output")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let error = state.get("error").and_then(Value::as_str);
    let status = match state.get("status").and_then(Value::as_str) {
        Some("completed") => "ok",
        Some("error") => "error",
        Some("running") | Some("pending") => "unknown",
        Some(other) if other.contains("abort") => "interrupted",
        _ => "unknown",
    };
    let exit_code = state
        .get("metadata")
        .and_then(|meta| meta.get("exit").or_else(|| meta.get("exitCode")))
        .and_then(Value::as_i64);
    let time = state.get("time");
    let ms = |key: &str| {
        time.and_then(|time| time.get(key))
            .and_then(Value::as_i64)
            .and_then(ms_to_iso)
    };
    ToolCallRecord {
        key: part
            .get("callID")
            .and_then(Value::as_str)
            .unwrap_or(part_id)
            .to_string(),
        ts: ms("start"),
        ended_ts: ms("end"),
        tool_name: part
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        target,
        input_chars: input.to_string().chars().count() as i64,
        output_chars: output.chars().count() as i64,
        output_in_context: true,
        status: if exit_code.is_some_and(|code| code != 0) {
            "error".to_string()
        } else {
            status.to_string()
        },
        exit_code,
        error_head: error.map(ToOwned::to_owned).or_else(|| {
            exit_code
                .is_some_and(|code| code != 0)
                .then(|| output.chars().take(600).collect())
        }),
        child_session_id: state
            .get("metadata")
            .and_then(|meta| meta.get("sessionId"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        agent_type: text(input, "subagent_type"),
    }
}

fn build_visible_messages(messages: Vec<MessageRow>, mut parts: TextParts) -> Vec<OpenCodeMessage> {
    let mut out = Vec::new();

    for message in messages {
        let Some(role) = message.role.as_deref() else {
            continue;
        };
        if role != "user" && role != "assistant" {
            continue;
        }

        let Some(text_parts) = parts.remove(&message.id) else {
            continue;
        };
        for (part_id, text) in text_parts {
            out.push(OpenCodeMessage {
                key: part_id,
                role: role.to_string(),
                text,
                timestamp: message.timestamp.clone(),
            });
        }
    }

    out
}

fn latest_model_name(messages: &[MessageRow]) -> Option<String> {
    messages
        .iter()
        .rev()
        .filter(|message| message.role.as_deref() == Some("assistant"))
        .filter_map(|message| message.model_name.as_deref())
        .find(|model| !model.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn parse_accepted_changes(
    diff_root: &Path,
    session: &OpenCodeSession,
) -> Result<Vec<OpenCodeAcceptedChange>> {
    let diff_file = diff_root.join(format!("{}.json", session.id));
    if !diff_file.exists() {
        return Ok(Vec::new());
    }

    let raw = std::fs::read_to_string(diff_file)?;
    let entries: Vec<OpenCodeDiffEntry> = serde_json::from_str(&raw)?;
    let mut changes = Vec::new();
    for entry in entries {
        let (added_lines, removed_lines) = count_unified_patch_lines(&entry.patch);
        if added_lines == 0 && removed_lines == 0 {
            continue;
        }
        let file_path = resolve_diff_path(session.directory.as_deref(), &entry.file);
        changes.push(OpenCodeAcceptedChange {
            file_path: file_path.to_string_lossy().to_string(),
            added_lines,
            removed_lines,
        });
    }

    Ok(changes)
}

fn count_unified_patch_lines(patch: &str) -> (i64, i64) {
    let mut added = 0i64;
    let mut removed = 0i64;
    let mut in_hunk = false;
    for line in patch.lines() {
        if line.starts_with("@@") {
            in_hunk = true;
            continue;
        }
        if !in_hunk {
            continue;
        }
        if line.starts_with('+') {
            added += 1;
        } else if line.starts_with('-') {
            removed += 1;
        }
    }
    (added, removed)
}

fn resolve_diff_path(session_dir: Option<&str>, file: &str) -> PathBuf {
    let path = Path::new(file);
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(session_dir) = session_dir {
        Path::new(session_dir).join(path)
    } else {
        path.to_path_buf()
    }
}

pub(crate) fn ms_to_iso(ms: i64) -> Option<String> {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::{OpenCodeUsageEvent, ingest_db, parse_opencode_usage};
    use crate::db::init_app_schema;
    use anyhow::Result;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn parses_assistant_usage_from_message_data() {
        let usage = parse_opencode_usage(&json!({
            "role": "assistant",
            "providerID": "openai",
            "modelID": "gpt-5.5-fast",
            "cost": 0.0125,
            "tokens": {
                "input": 1199,
                "output": 28,
                "reasoning": 3,
                "cache": {"read": 130560, "write": 7}
            }
        }))
        .expect("usage should parse");

        assert_eq!(
            usage,
            OpenCodeUsageEvent {
                key: String::new(),
                timestamp: None,
                model_name: None,
                provider_id: None,
                input_tokens: 1199,
                cached_input_tokens: 130560,
                cache_creation_tokens: 7,
                output_tokens: 28,
                reasoning_tokens: 3,
                total_tokens: 131797,
                actual_cost_usd: Some(0.0125),
            }
        );
    }

    #[test]
    fn ingests_visible_messages_and_model_from_opencode_db() -> Result<()> {
        let tempdir = tempdir()?;
        let db_path = tempdir.path().join("opencode.db");
        std::fs::create_dir_all(tempdir.path().join("storage/session_diff"))?;
        let source = Connection::open(&db_path)?;
        create_opencode_schema(&source)?;
        seed_project(&source, "p1", "/tmp/repo")?;
        seed_session(
            &source,
            "s1",
            "p1",
            "/tmp/repo",
            1_777_365_898_000,
            1_777_365_904_000,
        )?;
        seed_message(
            &source,
            "m1",
            "s1",
            1_777_365_898_100,
            json!({"role":"user","model":{"providerID":"openai","modelID":"gpt-5.5-fast"}}),
        )?;
        seed_part(
            &source,
            "p1",
            "m1",
            "s1",
            1_777_365_898_101,
            json!({"type":"text","text":"please refactor this"}),
        )?;
        seed_message(
            &source,
            "m2",
            "s1",
            1_777_365_899_000,
            json!({"role":"assistant","modelID":"gpt-5.5-fast","providerID":"openai"}),
        )?;
        seed_part(
            &source,
            "p2",
            "m2",
            "s1",
            1_777_365_899_001,
            json!({"type":"tool","tool":"read"}),
        )?;
        seed_part(
            &source,
            "p3",
            "m2",
            "s1",
            1_777_365_899_002,
            json!({"type":"reasoning","text":"hidden"}),
        )?;
        seed_part(
            &source,
            "p4",
            "m2",
            "s1",
            1_777_365_899_003,
            json!({"type":"text","text":"I updated it."}),
        )?;
        std::fs::write(
            tempdir.path().join("storage/session_diff/s1.json"),
            json!([
                {
                    "file": "src/lib.rs",
                    "patch": "Index: src/lib.rs\n===================================================================\n--- src/lib.rs\n+++ src/lib.rs\n@@ -1,1 +1,2 @@\n old\n+new\n"
                }
            ])
            .to_string(),
        )?;

        let analytics = Connection::open_in_memory()?;
        init_app_schema(&analytics)?;
        let written = ingest_db(&db_path, &analytics)?;

        assert_eq!(written, 5);
        let model: String = analytics.query_row(
            "SELECT mm.model_name
             FROM metadata_sessions s
             JOIN metadata_models mm ON mm.id = s.model_id
             WHERE s.provider='opencode' AND s.session_id='s1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(model, "opencode/gpt-5.5-fast");

        let rows = load_messages(&analytics, "s1")?;
        assert_eq!(
            rows,
            vec![
                ("user".to_string(), "please refactor this".to_string()),
                ("assistant".to_string(), "I updated it.".to_string()),
            ]
        );
        let change: (String, i64, i64) = analytics.query_row(
            "SELECT abs_path, lines_added, lines_removed
             FROM fact_session_code_change
             WHERE provider='opencode' AND session_id='s1' AND source_kind='accepted_change'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            change,
            (
                std::path::Path::new("/tmp/repo")
                    .join("src/lib.rs")
                    .to_string_lossy()
                    .to_string(),
                1,
                0
            )
        );
        Ok(())
    }

    #[test]
    fn preserves_no_output_sessions_with_user_text() -> Result<()> {
        let tempdir = tempdir()?;
        let db_path = tempdir.path().join("opencode.db");
        let source = Connection::open(&db_path)?;
        create_opencode_schema(&source)?;
        seed_project(&source, "global", "/")?;
        seed_session(
            &source,
            "s2",
            "global",
            "/tmp",
            1_777_363_026_000,
            1_777_363_035_000,
        )?;
        seed_message(
            &source,
            "m1",
            "s2",
            1_777_363_026_100,
            json!({"role":"user","model":{"providerID":"openai","modelID":"gpt-5.5-fast"}}),
        )?;
        seed_part(
            &source,
            "p1",
            "m1",
            "s2",
            1_777_363_026_101,
            json!({"type":"text","text":"hi"}),
        )?;

        let analytics = Connection::open_in_memory()?;
        init_app_schema(&analytics)?;
        let written = ingest_db(&db_path, &analytics)?;

        assert_eq!(written, 2);
        let session_count: i64 = analytics.query_row(
            "SELECT COUNT(*) FROM metadata_sessions WHERE provider='opencode' AND session_id='s2'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(session_count, 1);
        let change_count: i64 = analytics.query_row(
            "SELECT COUNT(*) FROM fact_session_code_change WHERE provider='opencode' AND session_id='s2'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(change_count, 0);
        Ok(())
    }

    fn create_opencode_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE TABLE project (
                id TEXT PRIMARY KEY,
                worktree TEXT NOT NULL
            );
            CREATE TABLE session (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                directory TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL
            );
            CREATE TABLE message (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE part (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );",
        )?;
        Ok(())
    }

    fn seed_project(conn: &Connection, id: &str, worktree: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO project (id, worktree) VALUES (?1, ?2)",
            params![id, worktree],
        )?;
        Ok(())
    }

    fn seed_session(
        conn: &Connection,
        id: &str,
        project_id: &str,
        directory: &str,
        created: i64,
        updated: i64,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO session (id, project_id, directory, time_created, time_updated)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, project_id, directory, created, updated],
        )?;
        Ok(())
    }

    fn seed_message(
        conn: &Connection,
        id: &str,
        session_id: &str,
        created: i64,
        data: serde_json::Value,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data)
             VALUES (?1, ?2, ?3, ?3, ?4)",
            params![id, session_id, created, data.to_string()],
        )?;
        Ok(())
    }

    fn seed_part(
        conn: &Connection,
        id: &str,
        message_id: &str,
        session_id: &str,
        created: i64,
        data: serde_json::Value,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
            params![id, message_id, session_id, created, data.to_string()],
        )?;
        Ok(())
    }

    fn load_messages(conn: &Connection, session_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = conn.prepare(
            "SELECT role, content
             FROM fact_session_message
             WHERE provider='opencode' AND session_id=?1
             ORDER BY message_index",
        )?;
        stmt.query_map([session_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}
