use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

use crate::change_intel::types::{ChangeOpCandidate, LineHashCount, ParseError, SessionInfo};

#[derive(Debug, Clone)]
pub struct IngestCursor {
    pub file_mtime: i64,
    pub file_size: i64,
}

pub fn upsert_change_session(conn: &Connection, session: &SessionInfo) -> Result<()> {
    conn.execute(
        "INSERT INTO change_sessions (
             provider, session_id, source_file, session_cwd, started_at, last_seen_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(provider, session_id) DO UPDATE SET
             source_file = excluded.source_file,
             session_cwd = COALESCE(excluded.session_cwd, change_sessions.session_cwd),
             started_at = COALESCE(change_sessions.started_at, excluded.started_at),
             last_seen_at = CASE
                 WHEN excluded.last_seen_at IS NULL THEN change_sessions.last_seen_at
                 WHEN change_sessions.last_seen_at IS NULL THEN excluded.last_seen_at
                 WHEN excluded.last_seen_at > change_sessions.last_seen_at THEN excluded.last_seen_at
                 ELSE change_sessions.last_seen_at
             END",
        params![
            session.provider,
            session.session_id,
            session.source_file,
            session.session_cwd,
            session.started_at,
            session.last_seen_at,
        ],
    )?;
    Ok(())
}

pub fn upsert_change_op(conn: &Connection, op: &ChangeOpCandidate) -> Result<i64> {
    conn.execute(
        "INSERT INTO change_ops (
            provider, session_id, call_id, op_index, timestamp, repo_root, abs_path, rel_path,
            write_mode, before_known, added_lines, removed_lines, parser_name, parser_version
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
        ON CONFLICT(provider, session_id, call_id, op_index) DO UPDATE SET
            timestamp = excluded.timestamp,
            repo_root = excluded.repo_root,
            abs_path = excluded.abs_path,
            rel_path = excluded.rel_path,
            write_mode = excluded.write_mode,
            before_known = excluded.before_known,
            added_lines = excluded.added_lines,
            removed_lines = excluded.removed_lines,
            parser_name = excluded.parser_name,
            parser_version = excluded.parser_version",
        params![
            op.provider,
            op.session_id,
            op.call_id,
            op.op_index,
            op.timestamp,
            op.repo_root,
            op.abs_path,
            op.rel_path,
            op.write_mode.as_str(),
            op.before_known as i64,
            op.added_lines,
            op.removed_lines,
            op.parser_name,
            op.parser_version,
        ],
    )?;

    let id: i64 = conn.query_row(
        "SELECT id FROM change_ops
         WHERE provider = ?1 AND session_id = ?2 AND call_id = ?3 AND op_index = ?4",
        params![op.provider, op.session_id, op.call_id, op.op_index],
        |row| row.get(0),
    )?;

    Ok(id)
}

pub fn replace_line_hashes(conn: &Connection, op_id: i64, line_hashes: &[LineHashCount]) -> Result<()> {
    conn.execute(
        "DELETE FROM change_op_line_hashes WHERE op_id = ?1",
        params![op_id],
    )?;

    for lh in line_hashes {
        conn.execute(
            "INSERT INTO change_op_line_hashes (op_id, side, line_hash, count)
             VALUES (?1, ?2, ?3, ?4)",
            params![op_id, lh.side.as_str(), lh.line_hash, lh.count],
        )?;
    }

    Ok(())
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
