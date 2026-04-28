use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, params};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::db;
use crate::ingest_progress::IngestProgressObserver;

const PROVIDER: &str = "opencode";

#[derive(Debug, Clone)]
struct OpenCodeSession {
    id: String,
    directory: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
    model_name: Option<String>,
    messages: Vec<OpenCodeMessage>,
}

#[derive(Debug, Clone)]
struct OpenCodeMessage {
    role: String,
    text: String,
    timestamp: Option<String>,
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
}

pub fn plan_db_files() -> Result<Vec<PathBuf>> {
    let db_path = opencode_db_path()?;
    if db_path.exists() {
        Ok(vec![db_path])
    } else {
        Ok(Vec::new())
    }
}

pub fn opencode_db_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PACEFLOW_OPENCODE_DB_PATH") {
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
        let already_exists = db::session_exists(analytics, &session.id)?;
        db::upsert_metadata_session_with_model(
            analytics,
            PROVIDER,
            &session.id,
            session.directory.as_deref(),
            session.started_at.as_deref(),
            session
                .ended_at
                .as_deref()
                .or(session.started_at.as_deref()),
            Some(&source_path),
            Some(PROVIDER),
            session.model_name.as_deref(),
        )?;

        if already_exists {
            continue;
        }

        written += 1;
        for message in &session.messages {
            let words = message.text.split_whitespace().count();
            if words == 0 {
                continue;
            }
            db::ingest_session_message(
                analytics,
                PROVIDER,
                &session.id,
                &message.role,
                &message.text,
                words as i64,
                message.timestamp.as_deref(),
            )?;
            written += 1;
        }

        if let Some(diff_root) = diff_root.as_deref() {
            for change in parse_accepted_changes(diff_root, &session)? {
                db::ingest_accepted_code_change(
                    analytics,
                    PROVIDER,
                    &session.id,
                    &change.file_path,
                    change.added_lines,
                    change.removed_lines,
                    session
                        .ended_at
                        .as_deref()
                        .or(session.started_at.as_deref()),
                )?;
                written += 1;
            }
        }
    }

    Ok(written)
}

fn parse_sessions(path: &Path) -> Result<Vec<OpenCodeSession>> {
    let source = open_readonly_db(path)?;
    let mut stmt = source.prepare(
        "SELECT s.id, s.directory, s.time_created, s.time_updated, p.worktree
         FROM session s
         LEFT JOIN project p ON p.id = s.project_id
         ORDER BY s.time_created, s.id",
    )?;

    let rows = stmt
        .query_map([], |row| {
            let directory: Option<String> = row.get(1)?;
            let worktree: Option<String> = row.get(4)?;
            Ok((
                row.get::<_, String>(0)?,
                directory.or(worktree),
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut sessions = Vec::new();
    for (id, directory, created_ms, updated_ms) in rows {
        let messages = parse_message_rows(&source, &id)?;
        let model_name = latest_model_name(&messages);
        let parts = parse_text_parts(&source, &id)?;
        let visible_messages = build_visible_messages(messages, parts);
        sessions.push(OpenCodeSession {
            id,
            directory,
            started_at: ms_to_iso(created_ms),
            ended_at: ms_to_iso(updated_ms),
            model_name,
            messages: visible_messages,
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

        Ok(MessageRow {
            id,
            role,
            timestamp: ms_to_iso(created_ms),
            model_name,
        })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn parse_text_parts(source: &Connection, session_id: &str) -> Result<HashMap<String, Vec<String>>> {
    let mut stmt = source.prepare(
        "SELECT message_id, data
         FROM part
         WHERE session_id = ?1
         ORDER BY time_created, id",
    )?;

    let mut parts: HashMap<String, Vec<String>> = HashMap::new();
    let rows = stmt.query_map(params![session_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (message_id, data) = row?;
        let parsed: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        if parsed.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let Some(text) = parsed.get("text").and_then(Value::as_str) else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        parts.entry(message_id).or_default().push(text.to_string());
    }

    Ok(parts)
}

fn build_visible_messages(
    messages: Vec<MessageRow>,
    mut parts: HashMap<String, Vec<String>>,
) -> Vec<OpenCodeMessage> {
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
        for text in text_parts {
            out.push(OpenCodeMessage {
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
    use super::ingest_db;
    use crate::db::init_app_schema;
    use anyhow::Result;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use tempfile::tempdir;

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

        assert_eq!(written, 4);
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
