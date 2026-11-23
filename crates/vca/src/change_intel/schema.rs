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

        CREATE INDEX IF NOT EXISTS idx_ingest_cursors_provider_source
            ON ingest_cursors(provider, source_file);

        CREATE INDEX IF NOT EXISTS idx_parse_errors_provider_session
            ON change_parse_errors(provider, session_id);",
    )?;
    conn.execute_batch(&dirty_hash_table_sql("commit_assoc_dirty_hash"))?;
    prune_unscannable_dirty_hashes(conn)?;

    Ok(())
}

/// The primary key already leads with `repo_root`, so the table needs no other index.
fn dirty_hash_table_sql(name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (
            repo_root  TEXT NOT NULL,
            side       TEXT NOT NULL,
            line_hash  TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(repo_root, side, line_hash)
        );"
    )
}

/// `schema_migrations` marker: dirty hashes queued under paths that store no commits were removed.
const PRUNE_UNSCANNABLE_DIRTY_HASHES: i64 = 4;

/// Earlier versions queued every edited line for every path of a repository, including each
/// removed Multica workdir. Only paths that store commits ever read or clear that queue, so
/// the rest grew without bound (tens of millions of rows) and every re-read session added to
/// it. Keep the rows of paths with commits and drop the table holding the others: dropping
/// frees the pages without rewriting them, where a `DELETE` would rewrite the whole table.
fn prune_unscannable_dirty_hashes(conn: &Connection) -> Result<()> {
    let applied: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = ?1)",
        [PRUNE_UNSCANNABLE_DIRTY_HASHES],
        |row| row.get(0),
    )?;
    if applied {
        return Ok(());
    }
    conn.execute_batch(&format!(
        "SAVEPOINT prune_dirty_hashes;
         DROP TABLE IF EXISTS commit_assoc_dirty_hash_kept;
         {kept}
         INSERT INTO commit_assoc_dirty_hash_kept (repo_root, side, line_hash, created_at)
             SELECT repo_root, side, line_hash, created_at
             FROM commit_assoc_dirty_hash
             WHERE repo_root IN (SELECT DISTINCT repo_root FROM fact_commit);
         DROP TABLE commit_assoc_dirty_hash;
         ALTER TABLE commit_assoc_dirty_hash_kept RENAME TO commit_assoc_dirty_hash;
         INSERT OR IGNORE INTO schema_migrations(version) VALUES ({PRUNE_UNSCANNABLE_DIRTY_HASHES});
         RELEASE prune_dirty_hashes;",
        kept = dirty_hash_table_sql("commit_assoc_dirty_hash_kept"),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn dirty_roots(conn: &Connection) -> Result<Vec<(String, i64)>> {
        Ok(conn
            .prepare(
                "SELECT repo_root, COUNT(*) FROM commit_assoc_dirty_hash
                 GROUP BY repo_root ORDER BY repo_root",
            )?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }

    #[test]
    fn opening_drops_dirty_hashes_of_paths_without_commits_once() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_change_intel_schema(&conn)?;
        conn.execute_batch(
            "DELETE FROM schema_migrations WHERE version = 4;
             CREATE INDEX idx_commit_assoc_dirty_hash_repo ON commit_assoc_dirty_hash(repo_root);
             INSERT INTO fact_commit (repo_root, commit_sha, commit_time, subject) VALUES ('/scan', 'c1', '2026-11-01T00:00:00Z', 's');
             INSERT INTO commit_assoc_dirty_hash (repo_root, side, line_hash) VALUES
                 ('/scan', '+', 'h1'), ('/scan', '-', 'h2'),
                 ('/ws/issue-1/workdir', '+', 'h1'), ('/ws/issue-2/workdir', '+', 'h1');",
        )?;

        init_change_intel_schema(&conn)?;
        assert_eq!(dirty_roots(&conn)?, vec![("/scan".to_string(), 2)]);
        let old_index: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'idx_commit_assoc_dirty_hash_repo'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(old_index, 0);

        // Applied once: later rows under other paths are left to the normal queue logic.
        conn.execute(
            "INSERT INTO commit_assoc_dirty_hash (repo_root, side, line_hash) VALUES ('/other', '+', 'h9')",
            [],
        )?;
        init_change_intel_schema(&conn)?;
        assert_eq!(dirty_roots(&conn)?.len(), 2);
        Ok(())
    }

    /// A re-read session must queue its edits for the paths that store commits only, not
    /// once per worktree alias: Multica creates one workdir per issue, and the per-alias
    /// fan-out made a parser-version re-read insert tens of millions of rows.
    #[test]
    fn marking_edits_dirty_does_not_scale_with_repository_aliases() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        for issue in 0..300 {
            crate::insights::observation::set_alias(
                &conn,
                &format!("/ws/issue-{issue}/workdir/mono"),
                "git:mono",
                "provider",
            )?;
        }
        crate::insights::observation::set_alias(&conn, "/projects/mono", "git:mono", "git")?;
        conn.execute(
            "INSERT INTO fact_commit (repo_root, commit_sha, commit_time, subject) VALUES ('/projects/mono', 'c1', '2026-11-01T00:00:00Z', 's')",
            [],
        )?;
        let hashes: BTreeSet<(String, String)> = (0..50)
            .map(|i| ("+".to_string(), format!("h{i}")))
            .collect();

        crate::change_intel::storage::mark_hashes_dirty(
            &conn,
            "/ws/issue-7/workdir/mono",
            &hashes,
        )?;

        assert_eq!(
            dirty_roots(&conn)?,
            vec![("/projects/mono".to_string(), 50)]
        );
        let version: i64 = conn.query_row(
            "SELECT session_facts_version FROM commit_assoc_repo_state WHERE repo_root = '/projects/mono'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(version, 1);
        let states: i64 =
            conn.query_row("SELECT COUNT(*) FROM commit_assoc_repo_state", [], |r| {
                r.get(0)
            })?;
        assert_eq!(states, 1);
        Ok(())
    }
}
