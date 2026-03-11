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

        CREATE TABLE IF NOT EXISTS git_commits (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root     TEXT    NOT NULL,
            commit_sha    TEXT    NOT NULL,
            parent_sha    TEXT,
            commit_time   TEXT    NOT NULL,
            subject       TEXT    NOT NULL,
            total_added   INTEGER NOT NULL,
            total_removed INTEGER NOT NULL,
            ingested_at   TEXT DEFAULT (datetime('now')),
            UNIQUE(repo_root, commit_sha)
        );

        CREATE TABLE IF NOT EXISTS git_commit_file_diffs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            commit_id     INTEGER NOT NULL,
            rel_path      TEXT    NOT NULL,
            change_type   TEXT    NOT NULL,
            added_lines   INTEGER NOT NULL,
            removed_lines INTEGER NOT NULL,
            UNIQUE(commit_id, rel_path),
            FOREIGN KEY(commit_id) REFERENCES git_commits(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS git_commit_line_hashes (
            file_diff_id INTEGER NOT NULL,
            side         TEXT    NOT NULL,
            line_hash    TEXT    NOT NULL,
            count        INTEGER NOT NULL,
            PRIMARY KEY(file_diff_id, side, line_hash),
            FOREIGN KEY(file_diff_id) REFERENCES git_commit_file_diffs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS commit_ai_attributions (
            commit_id            INTEGER NOT NULL,
            algo_version         TEXT    NOT NULL,
            window_days          INTEGER NOT NULL,
            commit_total_lines   INTEGER NOT NULL,
            matched_total_lines  INTEGER NOT NULL,
            matched_added_lines  INTEGER NOT NULL,
            matched_removed_lines INTEGER NOT NULL,
            ai_share             REAL    NOT NULL,
            heavy_ai             INTEGER NOT NULL,
            computed_at          TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(commit_id, algo_version),
            FOREIGN KEY(commit_id) REFERENCES git_commits(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS commit_ai_provider_attributions (
            commit_id       INTEGER NOT NULL,
            algo_version    TEXT    NOT NULL,
            provider        TEXT    NOT NULL,
            matched_lines   REAL    NOT NULL,
            share_of_commit REAL    NOT NULL,
            share_of_ai     REAL    NOT NULL,
            computed_at     TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(commit_id, algo_version, provider),
            FOREIGN KEY(commit_id) REFERENCES git_commits(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS commit_ai_session_attributions (
            commit_id       INTEGER NOT NULL,
            algo_version    TEXT    NOT NULL,
            provider        TEXT    NOT NULL,
            session_id      TEXT    NOT NULL,
            matched_lines   REAL    NOT NULL,
            share_of_commit REAL    NOT NULL,
            share_of_ai     REAL    NOT NULL,
            computed_at     TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(commit_id, algo_version, provider, session_id),
            FOREIGN KEY(commit_id) REFERENCES git_commits(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS commit_task_attributions (
            commit_id       INTEGER NOT NULL,
            algo_version    TEXT    NOT NULL,
            branch_name     TEXT    NOT NULL,
            task_key        TEXT    NOT NULL,
            source          TEXT    NOT NULL,
            is_fallback     INTEGER NOT NULL,
            candidate_count INTEGER NOT NULL,
            distance_to_tip INTEGER,
            confidence      REAL    NOT NULL,
            computed_at     TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(commit_id, algo_version),
            FOREIGN KEY(commit_id) REFERENCES git_commits(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS commit_assoc_cursors (
            repo_root       TEXT NOT NULL,
            branch_scope    TEXT NOT NULL,
            last_head_sha   TEXT NOT NULL,
            last_commit_time TEXT,
            min_ai_ts_seen  TEXT,
            updated_at      TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(repo_root, branch_scope)
        );

        CREATE TABLE IF NOT EXISTS commit_assoc_errors (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root  TEXT NOT NULL,
            commit_sha TEXT,
            stage      TEXT NOT NULL,
            error      TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_change_ops_provider_repo_path_ts
            ON change_ops(provider, repo_root, rel_path, timestamp);

        CREATE INDEX IF NOT EXISTS idx_change_ops_repo_path_ts
            ON change_ops(repo_root, rel_path, timestamp);

        CREATE INDEX IF NOT EXISTS idx_change_ops_provider_session
            ON change_ops(provider, session_id);

        CREATE INDEX IF NOT EXISTS idx_change_op_hash_side_hash
            ON change_op_line_hashes(side, line_hash);

        CREATE INDEX IF NOT EXISTS idx_parse_errors_provider_session
            ON change_parse_errors(provider, session_id);

        CREATE INDEX IF NOT EXISTS idx_git_commits_repo_time
            ON git_commits(repo_root, commit_time);

        CREATE INDEX IF NOT EXISTS idx_git_commit_file_commit_path
            ON git_commit_file_diffs(commit_id, rel_path);

        CREATE INDEX IF NOT EXISTS idx_git_commit_hash_side_hash
            ON git_commit_line_hashes(side, line_hash);

        CREATE INDEX IF NOT EXISTS idx_commit_ai_share_heavy
            ON commit_ai_attributions(ai_share, heavy_ai);

        CREATE INDEX IF NOT EXISTS idx_commit_ai_session_provider_session
            ON commit_ai_session_attributions(provider, session_id);

        CREATE INDEX IF NOT EXISTS idx_commit_task_task_key
            ON commit_task_attributions(task_key);

        CREATE INDEX IF NOT EXISTS idx_commit_task_branch_name
            ON commit_task_attributions(branch_name);",
    )?;

    Ok(())
}
