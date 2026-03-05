use anyhow::Result;
use rusqlite::Connection;

pub fn init_change_intel_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS change_sessions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            provider      TEXT    NOT NULL,
            session_id    TEXT    NOT NULL,
            source_file   TEXT    NOT NULL,
            session_cwd   TEXT,
            started_at    TEXT,
            last_seen_at  TEXT,
            ingested_at   TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, session_id)
        );

        CREATE TABLE IF NOT EXISTS change_ops (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            provider       TEXT    NOT NULL,
            session_id     TEXT    NOT NULL,
            call_id        TEXT    NOT NULL,
            op_index       INTEGER NOT NULL,
            timestamp      TEXT,
            repo_root      TEXT,
            abs_path       TEXT    NOT NULL,
            rel_path       TEXT,
            write_mode     TEXT    NOT NULL,
            before_known   INTEGER NOT NULL,
            added_lines    INTEGER NOT NULL,
            removed_lines  INTEGER NOT NULL,
            parser_name    TEXT    NOT NULL,
            parser_version TEXT    NOT NULL,
            created_at     TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, session_id, call_id, op_index)
        );

        CREATE TABLE IF NOT EXISTS change_op_line_hashes (
            op_id      INTEGER NOT NULL,
            side       TEXT    NOT NULL,
            line_hash  TEXT    NOT NULL,
            count      INTEGER NOT NULL,
            PRIMARY KEY(op_id, side, line_hash),
            FOREIGN KEY(op_id) REFERENCES change_ops(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS ingest_cursors (
            provider         TEXT    NOT NULL,
            source_file      TEXT    NOT NULL,
            file_mtime       INTEGER NOT NULL,
            file_size        INTEGER NOT NULL,
            last_ingested_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(provider, source_file)
        );

        CREATE TABLE IF NOT EXISTS change_parse_errors (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            provider     TEXT    NOT NULL,
            session_id   TEXT    NOT NULL,
            source_file  TEXT    NOT NULL,
            call_id      TEXT    NOT NULL,
            timestamp    TEXT,
            parser_name  TEXT    NOT NULL,
            error        TEXT    NOT NULL,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_change_ops_provider_repo_path_ts
            ON change_ops(provider, repo_root, rel_path, timestamp);

        CREATE INDEX IF NOT EXISTS idx_change_ops_provider_session
            ON change_ops(provider, session_id);

        CREATE INDEX IF NOT EXISTS idx_change_op_hash_side_hash
            ON change_op_line_hashes(side, line_hash);

        CREATE INDEX IF NOT EXISTS idx_parse_errors_provider_session
            ON change_parse_errors(provider, session_id);",
    )?;

    Ok(())
}
