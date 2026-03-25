use anyhow::Result;
use rusqlite::Connection;

use crate::db::init_metadata_schema;

pub fn init_change_intel_schema(conn: &Connection) -> Result<()> {
    init_metadata_schema(conn)?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS ingest_cursors (
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

        CREATE TABLE IF NOT EXISTS commit_assoc_errors (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root  TEXT NOT NULL,
            commit_sha TEXT,
            stage      TEXT NOT NULL,
            error      TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS commit_assoc_repo_state (
            repo_root               TEXT PRIMARY KEY,
            session_facts_version   INTEGER NOT NULL DEFAULT 0,
            task_branch_fingerprint TEXT,
            created_at              TEXT DEFAULT (datetime('now')),
            updated_at              TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS commit_assoc_dirty_hash (
            repo_root  TEXT NOT NULL,
            side       TEXT NOT NULL,
            line_hash  TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(repo_root, side, line_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_ingest_cursors_provider_source
            ON ingest_cursors(provider, source_file);

        CREATE INDEX IF NOT EXISTS idx_parse_errors_provider_session
            ON change_parse_errors(provider, session_id);

        CREATE INDEX IF NOT EXISTS idx_commit_assoc_dirty_hash_repo
            ON commit_assoc_dirty_hash(repo_root);",
    )?;

    Ok(())
}
