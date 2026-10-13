pub(crate) mod shared;

use anyhow::Result;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

use crate::db::session_store::{self, AcceptedChangeRecord, AgentKind, SessionRecord};
use crate::ingest_progress::IngestProgressObserver;

pub fn plan_session_files() -> Result<Vec<PathBuf>> {
    shared::plan_session_files()
}

/// Main session files plus subagent transcripts, for code-change extraction.
pub fn plan_change_sources() -> Result<Vec<PathBuf>> {
    let mut sources = Vec::new();
    for main in shared::plan_session_files()? {
        let subagents = shared::discover_subagent_files(&main);
        sources.push(main);
        sources.extend(subagents);
    }
    Ok(sources)
}

/// Session id for a parsed transcript: subagent transcripts get a child id.
pub(crate) fn effective_session_id(path: &Path, parsed: &shared::ParsedClaudeSession) -> String {
    let in_subagents_dir = path
        .parent()
        .and_then(|dir| dir.file_name())
        .and_then(|name| name.to_str())
        == Some("subagents");
    match (&parsed.agent_id, in_subagents_dir) {
        (Some(agent_id), true) => subagent_session_id(&parsed.session_id, agent_id),
        _ => parsed.session_id.clone(),
    }
}

pub fn ingest_planned_sessions(
    db: &Connection,
    session_files: &[PathBuf],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    let mut total_rows = 0usize;

    for session_file in session_files {
        if verbose {
            eprint!("  {:?} ... ", session_file);
        }

        match ingest_session(session_file, db) {
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
                eprintln!("Warning: skipping {:?}: {}", session_file, error);
            }
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(&session_file.to_string_lossy());
        }
    }

    Ok(total_rows)
}

fn ingest_session(path: &Path, db: &Connection) -> Result<usize> {
    let subagent_files = shared::discover_subagent_files(path);
    let source_key = path.to_string_lossy().to_string();
    let fingerprint = combined_fingerprint(path, &subagent_files);
    if !session_store::source_needs_ingest(db, "claude", &source_key, &fingerprint)? {
        return Ok(0);
    }

    let parsed = shared::parse_session_file(path)?;
    if parsed.session_id.trim().is_empty() {
        return Ok(0);
    }
    let mut written = 0usize;
    let mut session_ids: Vec<String> = Vec::new();
    if !(parsed.visible_messages.is_empty()
        && parsed.structured_writes.is_empty()
        && parsed.usage.is_empty())
    {
        let record = main_record(&parsed);
        written += session_store::store_session_record(db, &record)?;
        session_ids.push(record.session_id.clone());

        for subagent_file in &subagent_files {
            let child = match shared::parse_session_file(subagent_file) {
                Ok(child) => child,
                Err(error) => {
                    eprintln!("Warning: skipping {:?}: {}", subagent_file, error);
                    continue;
                }
            };
            if child.visible_messages.is_empty() && child.usage.is_empty() {
                continue;
            }
            let meta = shared::read_subagent_meta(subagent_file);
            let record = subagent_record(&parsed, &child, subagent_file, meta.as_ref());
            written += session_store::store_session_record(db, &record)?;
            session_ids.push(record.session_id.clone());
        }
    }

    let ids: Vec<&str> = session_ids.iter().map(String::as_str).collect();
    session_store::mark_source_ingested(db, "claude", &source_key, &fingerprint, &ids)?;
    Ok(written)
}

fn combined_fingerprint(path: &Path, subagent_files: &[std::path::PathBuf]) -> String {
    let mut parts = vec![session_store::file_fingerprint(path).unwrap_or_default()];
    for file in subagent_files {
        parts.push(session_store::file_fingerprint(file).unwrap_or_default());
    }
    parts.join("|")
}

pub(crate) fn subagent_session_id(parent_session_id: &str, agent_id: &str) -> String {
    format!("{parent_session_id}/agent-{agent_id}")
}

fn base_record(parsed: &shared::ParsedClaudeSession, session_id: &str) -> SessionRecord {
    let mut record = SessionRecord::new("claude", session_id);
    record.source_path = Some(parsed.source_file.clone());
    record.project_path = parsed.session_cwd.clone();
    record.started_at = parsed.started_at.clone();
    record.ended_at = parsed.ended_at.clone().or(parsed.started_at.clone());
    record.git_branch = parsed.git_branch.clone();
    record.client_version = parsed.client_version.clone();
    record.entrypoint = parsed.entrypoint.clone();
    record.title = parsed.title.clone();
    record.messages = parsed.message_records();
    record.usage = parsed.usage.clone();
    record.tool_calls = parsed.tool_calls.clone();
    record.events = parsed.events.clone();
    record.context_injections = parsed.context_injections.clone();
    record.accepted_changes = parsed
        .structured_writes
        .iter()
        .map(|change| AcceptedChangeRecord {
            abs_path: change.abs_path.clone(),
            lines_added: change.added_lines,
            lines_removed: change.removed_lines,
            ts: change.timestamp.clone(),
        })
        .collect();
    record.model = record
        .primary_model_from_usage()
        .or(parsed.model_name.clone());
    record
}

fn main_record(parsed: &shared::ParsedClaudeSession) -> SessionRecord {
    let mut record = base_record(parsed, &parsed.session_id);
    // Tool results of Task/Agent calls name the child agent; store the child session id.
    for call in &mut record.tool_calls {
        if let Some(agent_id) = call.child_session_id.take() {
            call.child_session_id = Some(subagent_session_id(&parsed.session_id, &agent_id));
        }
    }
    record
}

fn subagent_record(
    parent: &shared::ParsedClaudeSession,
    child: &shared::ParsedClaudeSession,
    file: &Path,
    meta: Option<&serde_json::Value>,
) -> SessionRecord {
    let agent_id = child.agent_id.clone().unwrap_or_else(|| {
        file.file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .trim_start_matches("agent-")
            .to_string()
    });
    let session_id = subagent_session_id(&parent.session_id, &agent_id);
    let mut record = base_record(child, &session_id);
    record.source_path = Some(file.to_string_lossy().to_string());
    record.project_path = child.session_cwd.clone().or(parent.session_cwd.clone());
    record.parent_session_id = Some(parent.session_id.clone());
    record.agent_kind = AgentKind::Subagent;
    record.agent_type = meta
        .and_then(|meta| meta.get("agentType"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            parent
                .tool_calls
                .iter()
                .find(|call| call.child_session_id.as_deref() == Some(agent_id.as_str()))
                .and_then(|call| call.agent_type.clone())
        });
    record.title = meta
        .and_then(|meta| meta.get("description"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or(record.title);
    record.git_branch = record.git_branch.or(parent.git_branch.clone());
    // Subagent edits are already attributed to the parent via agent progress records
    // in older transcripts; keep accepted changes on the child where they happened.
    record
}

#[cfg(test)]
mod tests {
    use super::ingest_session;
    use crate::db::init_app_schema;
    use anyhow::Result;
    use rusqlite::Connection;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn session_ingest_filters_meta_and_tool_result_noise() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("claude-session.jsonl");
        fs::write(
            &path,
            [
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:00Z",
                    "sessionId": "claude-meta",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": "please inspect this"}
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-meta",
                    "cwd": "/tmp/repo",
                    "message": {
                        "model": "claude-opus-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Looking now."}]
                    }
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-meta",
                    "cwd": "/tmp/repo",
                    "isMeta": true,
                    "message": {"role": "user", "content": "<local-command-caveat>ignore</local-command-caveat>"}
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:03Z",
                    "sessionId": "claude-meta",
                    "cwd": "/tmp/repo",
                    "message": {
                        "role": "user",
                        "content": [{"type": "tool_result", "tool_use_id": "toolu_read", "content": "ok"}]
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )?;

        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let written = ingest_session(&path, &conn)?;
        assert_eq!(written, 3);

        let rows: Vec<(String, String)> = {
            let mut stmt = conn.prepare(
                "SELECT role, content
                 FROM fact_session_message
                 WHERE provider='claude' AND session_id='claude-meta'
                 ORDER BY message_index",
            )?;
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        assert_eq!(
            rows,
            vec![
                ("user".to_string(), "please inspect this".to_string()),
                ("assistant".to_string(), "Looking now.".to_string()),
            ]
        );
        Ok(())
    }

    #[test]
    fn session_ingest_uses_latest_real_model_not_synthetic_placeholder() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("claude-models.jsonl");
        fs::write(
            &path,
            [
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:00Z",
                    "sessionId": "claude-models",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": "please update"}
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-models",
                    "cwd": "/tmp/repo",
                    "message": {
                        "model": "<synthetic>",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "API error"}]
                    },
                    "isApiErrorMessage": true
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-models",
                    "cwd": "/tmp/repo",
                    "message": {
                        "model": "claude-opus-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Recovered."}]
                    }
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:03Z",
                    "sessionId": "claude-models",
                    "cwd": "/tmp/repo",
                    "message": {
                        "model": "claude-sonnet-4-5-20250929",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Switching models."}]
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )?;

        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        ingest_session(&path, &conn)?;

        let model_name: Option<String> = conn.query_row(
            "SELECT m.model_name
             FROM metadata_sessions s
             LEFT JOIN metadata_models m ON m.id = s.model_id
             WHERE s.provider='claude' AND s.session_id='claude-models'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            model_name.as_deref(),
            Some("claude/claude-sonnet-4-5-20250929")
        );
        Ok(())
    }
}
