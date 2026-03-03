use anyhow::Result;
use rusqlite::{Connection, params};

use crate::events::Event;

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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type    TEXT    NOT NULL,
            session_id    TEXT    NOT NULL,
            provider      TEXT    NOT NULL,
            project_path  TEXT,
            timestamp     TEXT,
            last_updated  TEXT,
            role          TEXT,
            content       TEXT,
            content_words INTEGER,
            file_path     TEXT,
            lines_added   INTEGER,
            lines_removed INTEGER,
            ingested_at   TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_session  ON events(session_id);
        CREATE INDEX IF NOT EXISTS idx_provider ON events(provider);",
    )?;
    // Migration: add last_updated column to existing DBs (ignored if already present)
    let _ = conn.execute_batch("ALTER TABLE events ADD COLUMN last_updated TEXT;");
    Ok(())
}

/// Returns true if any events for this session_id already exist (dedup guard).
pub fn session_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn insert_event(conn: &Connection, event: &Event) -> Result<()> {
    match event {
        Event::ChatStart { session_id, provider, project_path, timestamp, last_updated } => {
            conn.execute(
                "INSERT INTO events (event_type, session_id, provider, project_path, timestamp, last_updated)
                 VALUES ('chat.start', ?1, ?2, ?3, ?4, ?5)",
                params![session_id, provider, project_path, timestamp, last_updated],
            )?;
        }
        Event::Message { session_id, provider, role, content, content_words, timestamp } => {
            let capped: String = content.chars().take(4096).collect();
            conn.execute(
                "INSERT INTO events (event_type, session_id, provider, role, content, content_words, timestamp)
                 VALUES ('message', ?1, ?2, ?3, ?4, ?5, ?6)",
                params![session_id, provider, role, capped, *content_words as i64, timestamp],
            )?;
        }
        Event::ChangesAccepted { session_id, provider, file_path, lines_added, lines_removed, timestamp } => {
            conn.execute(
                "INSERT INTO events (event_type, session_id, provider, file_path, lines_added, lines_removed, timestamp)
                 VALUES ('changes.accepted', ?1, ?2, ?3, ?4, ?5, ?6)",
                params![session_id, provider, file_path, lines_added, lines_removed, timestamp],
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
             COALESCE(s.project_path, '(unknown)') AS project_path,
             s.timestamp,
             s.last_updated,
             COALESCE(SUM(CASE WHEN e.event_type = 'message' AND e.role = 'user' THEN e.content_words ELSE 0 END), 0) AS total_words,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted'
                               THEN e.lines_added + e.lines_removed ELSE 0 END), 0) AS total_loc,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted' THEN e.lines_added   ELSE 0 END), 0) AS total_added,
             COALESCE(SUM(CASE WHEN e.event_type = 'changes.accepted' THEN e.lines_removed ELSE 0 END), 0) AS total_removed
         FROM events e
         LEFT JOIN (
             SELECT session_id, project_path, timestamp, last_updated
             FROM events
             WHERE event_type = 'chat.start'
         ) s ON s.session_id = e.session_id
         GROUP BY e.session_id
         ORDER BY COALESCE(s.last_updated, s.timestamp) DESC NULLS LAST",
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
