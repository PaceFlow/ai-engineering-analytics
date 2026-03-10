use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

use crate::change_intel::schema::init_change_intel_schema;
use crate::events::Event;
use crate::path_utils::{detect_repo_root, to_rel_path};

pub fn open() -> Result<Connection> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    let vibe_dir = home.join(".vibe");
    std::fs::create_dir_all(&vibe_dir)?;
    let db_path = vibe_dir.join("vca.db");
    let conn = Connection::open(db_path)?;
    init_schema(&conn)?;
    Ok(conn)
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS events (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_version       INTEGER NOT NULL DEFAULT 1,
            provider             TEXT    NOT NULL,
            session_id           TEXT    NOT NULL,
            event_type           TEXT    NOT NULL,
            event_subtype        TEXT,
            event_ts             TEXT,
            turn_index           INTEGER,
            event_index          INTEGER NOT NULL DEFAULT 0,
            project_path         TEXT,
            repo_root            TEXT,
            session_started_at   TEXT,
            session_last_updated TEXT,
            role                 TEXT,
            content              TEXT,
            content_words        INTEGER NOT NULL DEFAULT 0,
            content_chars        INTEGER NOT NULL DEFAULT 0,
            file_path            TEXT,
            rel_path             TEXT,
            file_name            TEXT,
            lines_added          INTEGER NOT NULL DEFAULT 0,
            lines_removed        INTEGER NOT NULL DEFAULT 0,
            tool_name            TEXT,
            call_id              TEXT,
            success              INTEGER,
            exit_code            INTEGER,
            source_file          TEXT,
            source_row           INTEGER,
            raw_payload_json     TEXT,
            ingested_at          TEXT DEFAULT (datetime('now')),
            CHECK (success IN (0,1) OR success IS NULL)
        );

        CREATE INDEX IF NOT EXISTS idx_ev_provider_session_ts
            ON events(provider, session_id, event_ts, id);
        CREATE INDEX IF NOT EXISTS idx_ev_type_ts
            ON events(event_type, event_ts);
        CREATE INDEX IF NOT EXISTS idx_ev_repo_path_ts
            ON events(repo_root, rel_path, event_ts);
        CREATE INDEX IF NOT EXISTS idx_ev_file_name_ts
            ON events(file_name, event_ts);
        CREATE INDEX IF NOT EXISTS idx_ev_session_event
            ON events(session_id, event_type);",
    )?;

    init_change_intel_schema(conn)?;
    Ok(())
}

/// Returns true if any events for this session_id already exist (dedup guard).
pub fn session_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    let evidence_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(evidence_count > 0)
}

pub fn insert_event(conn: &Connection, event: &Event) -> Result<()> {
    insert_event_row(conn, event)?;
    Ok(())
}

#[derive(Default)]
struct SessionContext {
    project_path: Option<String>,
    repo_root: Option<String>,
    session_started_at: Option<String>,
    session_last_updated: Option<String>,
}

fn load_session_context(conn: &Connection, provider: &str, session_id: &str) -> Result<SessionContext> {
    let row = conn
        .query_row(
            "SELECT project_path, repo_root, session_started_at, session_last_updated
             FROM events
             WHERE provider = ?1
               AND session_id = ?2
               AND event_type = 'chat.start'
             ORDER BY id
             LIMIT 1",
            params![provider, session_id],
            |r| {
                Ok(SessionContext {
                    project_path: r.get(0)?,
                    repo_root: r.get(1)?,
                    session_started_at: r.get(2)?,
                    session_last_updated: r.get(3)?,
                })
            },
        )
        .optional()?;

    Ok(row.unwrap_or_default())
}

fn derive_repo_root(project_path: Option<&str>, file_path: Option<&str>) -> Option<String> {
    if let Some(fp) = file_path {
        let fp_path = Path::new(fp);
        if fp_path.is_absolute() {
            if let Some(root) = detect_repo_root(fp_path) {
                return Some(root.to_string_lossy().to_string());
            }
        }
    }

    if let Some(pp) = project_path {
        let pp_path = Path::new(pp);
        if pp_path.is_absolute() {
            if let Some(root) = detect_repo_root(pp_path) {
                return Some(root.to_string_lossy().to_string());
            }
        }
    }

    None
}

fn derive_rel_and_file_name(repo_root: Option<&str>, file_path: &str) -> (Option<String>, Option<String>) {
    if file_path.trim().is_empty() || file_path == "__total__" {
        return (None, None);
    }

    let file_name = Path::new(file_path)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty());

    let rel_path = if let Some(root) = repo_root {
        let root_path = Path::new(root);
        let p = Path::new(file_path);
        if p.is_absolute() {
            to_rel_path(Some(root_path), p)
        } else {
            Some(file_path.to_string())
        }
    } else {
        None
    };

    (rel_path, file_name)
}

fn insert_event_row(conn: &Connection, event: &Event) -> Result<()> {
    match event {
        Event::ChatStart {
            session_id,
            provider,
            project_path,
            timestamp,
            last_updated,
        } => {
            let repo_root = derive_repo_root(project_path.as_deref(), None);
            let session_last_updated = last_updated.clone().or_else(|| timestamp.clone());

            conn.execute(
                "INSERT INTO events (
                    provider, session_id, event_type, event_ts,
                    project_path, repo_root, session_started_at, session_last_updated
                 ) VALUES (?1, ?2, 'chat.start', ?3, ?4, ?5, ?6, ?7)",
                params![
                    provider,
                    session_id,
                    timestamp,
                    project_path,
                    repo_root,
                    timestamp,
                    session_last_updated
                ],
            )?;
        }
        Event::Message {
            session_id,
            provider,
            role,
            content,
            content_words,
            timestamp,
        } => {
            let ctx = load_session_context(conn, provider, session_id)?;
            let event_ts = timestamp
                .clone()
                .or_else(|| ctx.session_last_updated.clone())
                .or_else(|| ctx.session_started_at.clone());
            let session_last_updated = ctx
                .session_last_updated
                .clone()
                .or_else(|| ctx.session_started_at.clone())
                .or_else(|| event_ts.clone());
            let content_capped: String = content.chars().take(4096).collect();
            let content_chars = content_capped.chars().count() as i64;

            conn.execute(
                "INSERT INTO events (
                    provider, session_id, event_type, event_ts,
                    project_path, repo_root, session_started_at, session_last_updated,
                    role, content, content_words, content_chars
                 ) VALUES (?1, ?2, 'message', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    provider,
                    session_id,
                    event_ts,
                    ctx.project_path,
                    ctx.repo_root,
                    ctx.session_started_at,
                    session_last_updated,
                    role,
                    content_capped,
                    *content_words as i64,
                    content_chars
                ],
            )?;
        }
        Event::ChangesAccepted {
            session_id,
            provider,
            file_path,
            lines_added,
            lines_removed,
            timestamp,
        } => {
            let ctx = load_session_context(conn, provider, session_id)?;
            let event_ts = timestamp
                .clone()
                .or_else(|| ctx.session_last_updated.clone())
                .or_else(|| ctx.session_started_at.clone());
            let repo_root = ctx
                .repo_root
                .clone()
                .or_else(|| derive_repo_root(ctx.project_path.as_deref(), Some(file_path)));
            let (rel_path, file_name) = derive_rel_and_file_name(repo_root.as_deref(), file_path);

            conn.execute(
                "INSERT INTO events (
                    provider, session_id, event_type, event_ts,
                    project_path, repo_root, session_started_at, session_last_updated,
                    file_path, rel_path, file_name, lines_added, lines_removed
                 ) VALUES (?1, ?2, 'changes.accepted', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    provider,
                    session_id,
                    event_ts,
                    ctx.project_path,
                    repo_root,
                    ctx.session_started_at,
                    ctx.session_last_updated,
                    file_path,
                    rel_path,
                    file_name,
                    *lines_added,
                    *lines_removed
                ],
            )?;
        }
    }

    Ok(())
}

pub struct SessionStat {
    pub session_id: String,
    pub provider: String,
    pub project_path: String,
    pub timestamp: Option<String>,
    pub last_updated: Option<String>,
    pub total_words: i64,
    pub total_loc: i64,
    pub total_added: i64,
    pub total_removed: i64,
}

pub fn query_stats(conn: &Connection) -> Result<Vec<SessionStat>> {
    let mut stmt = conn.prepare(
        "SELECT
             e.session_id,
             e.provider,
             COALESCE(MAX(CASE WHEN e.project_path IS NOT NULL AND TRIM(e.project_path) != '' THEN e.project_path END), '(unknown)') AS project_path,
             MIN(COALESCE(e.session_started_at, e.event_ts)) AS timestamp,
             MAX(COALESCE(e.session_last_updated, e.event_ts, e.session_started_at)) AS last_updated,
             COALESCE(SUM(CASE WHEN e.event_type = 'message' AND e.role = 'user' THEN e.content_words ELSE 0 END), 0) AS total_words,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted'
                               THEN e.lines_added + e.lines_removed ELSE 0 END), 0) AS total_loc,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted' THEN e.lines_added   ELSE 0 END), 0) AS total_added,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted' THEN e.lines_removed ELSE 0 END), 0) AS total_removed
         FROM events e
         GROUP BY e.session_id, e.provider
         ORDER BY MAX(COALESCE(e.session_last_updated, e.event_ts, e.session_started_at)) DESC NULLS LAST",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok(SessionStat {
            session_id: row.get(0)?,
            provider: row.get(1)?,
            project_path: row.get(2)?,
            timestamp: row.get(3)?,
            last_updated: row.get(4)?,
            total_words: row.get(5)?,
            total_loc: row.get(6)?,
            total_added: row.get(7)?,
            total_removed: row.get(8)?,
        })
    })?;

    let mut stats = Vec::new();
    for r in rows {
        stats.push(r?);
    }
    Ok(stats)
}
