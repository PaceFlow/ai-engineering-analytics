use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

use crate::change_intel::types::{ChangeOpCandidate, LineHashCount, ParseError, SessionInfo};
use crate::db;

#[derive(Debug, Clone)]
pub struct IngestCursor {
    pub file_mtime: i64,
    pub file_size: i64,
}

pub fn upsert_change_session(conn: &Connection, session: &SessionInfo) -> Result<()> {
    db::upsert_metadata_session(
        conn,
        &session.provider,
        &session.session_id,
        session.session_cwd.as_deref(),
        None,
        None,
        Some(&session.source_file),
    )?;
    Ok(())
}

pub fn upsert_change_op(conn: &Connection, op: &ChangeOpCandidate) -> Result<i64> {
    if let (Some(repo_root), Some(rel_path)) = (op.repo_root.as_deref(), op.rel_path.as_deref()) {
        db::upsert_metadata_file(conn, repo_root, rel_path)?;
    } else if let Some(repo_root) = op.repo_root.as_deref() {
        db::upsert_metadata_repository(conn, repo_root)?;
    }
    db::upsert_fact_tool_write(conn, op)
}

pub fn replace_line_hashes(conn: &Connection, op_id: i64, line_hashes: &[LineHashCount]) -> Result<()> {
    db::replace_fact_session_code_change_line_hashes(conn, op_id, line_hashes)
}

pub fn insert_parse_error(conn: &Connection, err: &ParseError) -> Result<()> {
    conn.execute(
        "INSERT INTO change_parse_errors (
            provider, session_id, source_file, call_id, timestamp, parser_name, error
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            err.provider,
            err.session_id,
            err.source_file,
            err.call_id,
            err.timestamp,
            err.parser_name,
            err.error,
        ],
    )?;
    Ok(())
}

pub fn get_ingest_cursor(conn: &Connection, provider: &str, source_file: &str) -> Result<Option<IngestCursor>> {
    let row = conn
        .query_row(
            "SELECT file_mtime, file_size FROM ingest_cursors
             WHERE provider = ?1 AND source_file = ?2",
            params![provider, source_file],
            |r| {
                Ok(IngestCursor {
                    file_mtime: r.get(0)?,
                    file_size: r.get(1)?,
                })
            },
        )
        .optional()?;

    Ok(row)
}

pub fn upsert_ingest_cursor(
    conn: &Connection,
    provider: &str,
    source_file: &str,
    file_mtime: i64,
    file_size: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO ingest_cursors (provider, source_file, file_mtime, file_size)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(provider, source_file) DO UPDATE SET
            file_mtime = excluded.file_mtime,
            file_size = excluded.file_size,
            last_ingested_at = datetime('now')",
        params![provider, source_file, file_mtime, file_size],
    )?;
    Ok(())
}
