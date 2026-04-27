pub(crate) mod shared;

use anyhow::Result;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

use crate::db;
use crate::ingest_progress::IngestProgressObserver;

pub fn plan_session_files() -> Result<Vec<PathBuf>> {
    shared::plan_session_files()
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
    let parsed = shared::parse_session_file(path)?;
    if parsed.session_id.trim().is_empty() {
        return Ok(0);
    }
    if parsed.visible_messages.is_empty() && parsed.structured_writes.is_empty() {
        return Ok(0);
    }

    let source_file = parsed.source_file.clone();
    if db::session_exists(db, &parsed.session_id)? {
        db::upsert_metadata_session_with_model(
            db,
            "claude",
            &parsed.session_id,
            parsed.session_cwd.as_deref(),
            parsed.started_at.as_deref(),
            parsed.ended_at.as_deref().or(parsed.started_at.as_deref()),
            Some(&source_file),
            Some("claude"),
            parsed.model_name.as_deref(),
        )?;
        return Ok(0);
    }

    db::upsert_metadata_session_with_model(
        db,
        "claude",
        &parsed.session_id,
        parsed.session_cwd.as_deref(),
        parsed.started_at.as_deref(),
        parsed.ended_at.as_deref().or(parsed.started_at.as_deref()),
        Some(&source_file),
        Some("claude"),
        parsed.model_name.as_deref(),
    )?;
    let mut written = 1usize;

    for message in parsed.visible_messages {
        let words = message.text.split_whitespace().count() as i64;
        if words == 0 {
            continue;
        }
        db::ingest_session_message(
            db,
            "claude",
            &parsed.session_id,
            message.role.as_str(),
            &message.text,
            words,
            message.timestamp.as_deref(),
        )?;
        written += 1;
    }

    for change in parsed.structured_writes {
        db::ingest_accepted_code_change(
            db,
            "claude",
            &parsed.session_id,
            &change.abs_path,
            change.added_lines,
            change.removed_lines,
            change.timestamp.as_deref(),
        )?;
        written += 1;
    }

    Ok(written)
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
