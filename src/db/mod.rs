use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use std::env;
use std::path::Path;

use crate::change_intel::schema::init_change_intel_schema;
use crate::path_utils::{detect_repo_root, path_to_string, to_rel_path};

pub fn open() -> Result<Connection> {
    let home = env::var_os("PACEFLOW_HOME")
        .map(std::path::PathBuf::from)
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    open_at_home(&home)
}

fn open_at_home(home: &Path) -> Result<Connection> {
    let app_dir = home.join(".paceflow");
    std::fs::create_dir_all(&app_dir)?;
    let db_path = app_dir.join("paceflow.db");
    let conn = Connection::open(db_path)?;
    init_app_schema(&conn)?;
    Ok(conn)
}

pub(crate) fn init_app_schema(conn: &Connection) -> Result<()> {
    init_metadata_schema(conn)?;

    init_change_intel_schema(conn)?;
    Ok(())
}

pub(crate) fn init_metadata_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS metadata_repositories (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root      TEXT NOT NULL UNIQUE,
            repo_name      TEXT NOT NULL,
            origin_url     TEXT,
            default_branch TEXT,
            created_at     TEXT DEFAULT (datetime('now')),
            updated_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS metadata_sessions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            provider      TEXT NOT NULL,
            session_id    TEXT NOT NULL,
            repository_id INTEGER,
            model_id      INTEGER,
            project_path  TEXT,
            started_at    TEXT,
            ended_at      TEXT,
            source_path   TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            updated_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, session_id),
            FOREIGN KEY(repository_id) REFERENCES metadata_repositories(id) ON DELETE SET NULL,
            FOREIGN KEY(model_id) REFERENCES metadata_models(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS metadata_models (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            provider    TEXT NOT NULL,
            model_name  TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, model_name)
        );

        CREATE TABLE IF NOT EXISTS metadata_files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            repository_id INTEGER NOT NULL,
            relative_path TEXT NOT NULL,
            file_name     TEXT NOT NULL,
            extension     TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            updated_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(repository_id, relative_path),
            FOREIGN KEY(repository_id) REFERENCES metadata_repositories(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS metadata_tasks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            task_key    TEXT NOT NULL UNIQUE,
            task_prefix TEXT,
            task_number INTEGER,
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS metadata_branches (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            repository_id         INTEGER NOT NULL,
            branch_name           TEXT NOT NULL,
            task_id               INTEGER,
            is_default_branch     INTEGER NOT NULL DEFAULT 0,
            is_integration_branch INTEGER NOT NULL DEFAULT 0,
            created_at            TEXT DEFAULT (datetime('now')),
            updated_at            TEXT DEFAULT (datetime('now')),
            UNIQUE(repository_id, branch_name),
            FOREIGN KEY(repository_id) REFERENCES metadata_repositories(id) ON DELETE CASCADE,
            FOREIGN KEY(task_id) REFERENCES metadata_tasks(id) ON DELETE SET NULL,
            CHECK (is_default_branch IN (0,1)),
            CHECK (is_integration_branch IN (0,1))
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_sessions_repo
            ON metadata_sessions(repository_id, provider);
        CREATE INDEX IF NOT EXISTS idx_metadata_models_provider_name
            ON metadata_models(provider, model_name);
        CREATE INDEX IF NOT EXISTS idx_metadata_files_repo_path
            ON metadata_files(repository_id, relative_path);
        CREATE INDEX IF NOT EXISTS idx_metadata_branches_repo
            ON metadata_branches(repository_id, branch_name);
        CREATE INDEX IF NOT EXISTS idx_metadata_tasks_prefix_num
            ON metadata_tasks(task_prefix, task_number);

        CREATE TABLE IF NOT EXISTS fact_session_message (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            provider      TEXT NOT NULL,
            session_id    TEXT NOT NULL,
            message_index INTEGER NOT NULL,
            message_ts    TEXT,
            role          TEXT NOT NULL,
            content       TEXT NOT NULL,
            content_words INTEGER NOT NULL DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, session_id, message_index)
        );

        CREATE TABLE IF NOT EXISTS fact_session_code_change (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            provider        TEXT NOT NULL,
            session_id      TEXT NOT NULL,
            change_index    INTEGER,
            change_ts       TEXT,
            repo_root       TEXT,
            abs_path        TEXT,
            rel_path        TEXT,
            source_file     TEXT,
            lines_added     INTEGER NOT NULL DEFAULT 0,
            lines_removed   INTEGER NOT NULL DEFAULT 0,
            source_kind     TEXT NOT NULL,
            write_mode      TEXT,
            parser_name     TEXT,
            call_id         TEXT,
            op_index        INTEGER,
            before_known    INTEGER,
            created_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(provider, session_id, source_kind, call_id, op_index),
            CHECK (before_known IN (0,1) OR before_known IS NULL)
        );

        CREATE TABLE IF NOT EXISTS fact_session_code_change_line_hashes (
            code_change_id INTEGER NOT NULL,
            side           TEXT NOT NULL,
            line_hash      TEXT NOT NULL,
            count          INTEGER NOT NULL,
            PRIMARY KEY(code_change_id, side, line_hash),
            FOREIGN KEY(code_change_id) REFERENCES fact_session_code_change(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS fact_commit (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root            TEXT NOT NULL,
            commit_sha           TEXT NOT NULL,
            parent_sha           TEXT,
            commit_time          TEXT NOT NULL,
            subject              TEXT NOT NULL,
            total_added          INTEGER NOT NULL DEFAULT 0,
            total_removed        INTEGER NOT NULL DEFAULT 0,
            matched_total_lines  INTEGER NOT NULL DEFAULT 0,
            matched_added_lines  INTEGER NOT NULL DEFAULT 0,
            matched_removed_lines INTEGER NOT NULL DEFAULT 0,
            ai_share             REAL NOT NULL DEFAULT 0.0,
            heavy_ai             INTEGER NOT NULL DEFAULT 0,
            assoc_session_facts_version INTEGER NOT NULL DEFAULT 0,
            created_at           TEXT DEFAULT (datetime('now')),
            updated_at           TEXT DEFAULT (datetime('now')),
            UNIQUE(repo_root, commit_sha),
            CHECK (heavy_ai IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS fact_commit_file_change (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root     TEXT NOT NULL,
            commit_sha    TEXT NOT NULL,
            rel_path      TEXT NOT NULL,
            change_type   TEXT NOT NULL,
            added_lines   INTEGER NOT NULL DEFAULT 0,
            removed_lines INTEGER NOT NULL DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(repo_root, commit_sha, rel_path)
        );

        CREATE TABLE IF NOT EXISTS fact_commit_file_change_line_hashes (
            file_change_id INTEGER NOT NULL,
            side           TEXT NOT NULL,
            line_hash      TEXT NOT NULL,
            count          INTEGER NOT NULL,
            PRIMARY KEY(file_change_id, side, line_hash),
            FOREIGN KEY(file_change_id) REFERENCES fact_commit_file_change(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS fact_commit_session_match (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root       TEXT NOT NULL,
            commit_sha      TEXT NOT NULL,
            provider        TEXT NOT NULL,
            session_id      TEXT NOT NULL,
            matched_lines   REAL NOT NULL DEFAULT 0.0,
            share_of_commit REAL NOT NULL DEFAULT 0.0,
            share_of_ai     REAL NOT NULL DEFAULT 0.0,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(repo_root, commit_sha, provider, session_id)
        );

        CREATE TABLE IF NOT EXISTS fact_task_commit_assignment (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_root       TEXT NOT NULL,
            commit_sha      TEXT NOT NULL,
            branch_name     TEXT NOT NULL,
            task_key        TEXT NOT NULL,
            source          TEXT NOT NULL,
            is_fallback     INTEGER NOT NULL DEFAULT 0,
            candidate_count INTEGER NOT NULL DEFAULT 0,
            distance_to_tip INTEGER,
            confidence      REAL NOT NULL DEFAULT 0.0,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(repo_root, commit_sha),
            CHECK (is_fallback IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS fact_github_pull_request (
            repo_key     TEXT NOT NULL,
            pr_number    INTEGER NOT NULL,
            state        TEXT NOT NULL,
            draft_flag   INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT,
            updated_at   TEXT,
            closed_at    TEXT,
            merged_at    TEXT,
            base_ref     TEXT,
            head_ref     TEXT,
            html_url     TEXT,
            removed_hashes_complete_flag INTEGER,
            PRIMARY KEY(repo_key, pr_number),
            CHECK (draft_flag IN (0,1)),
            CHECK (removed_hashes_complete_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS fact_github_pull_request_commit (
            repo_key        TEXT NOT NULL,
            pr_number       INTEGER NOT NULL,
            commit_sha      TEXT NOT NULL,
            commit_position INTEGER,
            PRIMARY KEY(repo_key, pr_number, commit_sha)
        );

        CREATE TABLE IF NOT EXISTS fact_github_commit_pr_lookup (
            repo_key          TEXT NOT NULL,
            commit_sha        TEXT NOT NULL,
            status            TEXT NOT NULL,
            owning_pr_number  INTEGER,
            last_checked_at   TEXT NOT NULL,
            last_error        TEXT,
            PRIMARY KEY(repo_key, commit_sha)
        );

        CREATE TABLE IF NOT EXISTS fact_github_sync_state (
            repo_key                 TEXT PRIMARY KEY,
            last_commit_scan_at      TEXT,
            last_open_pr_refresh_at  TEXT,
            last_issue_scan_at       TEXT,
            last_error               TEXT,
            updated_at               TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_github_issue (
            repo_key             TEXT NOT NULL,
            issue_number         INTEGER NOT NULL,
            state                TEXT NOT NULL,
            created_at           TEXT,
            updated_at           TEXT,
            closed_at            TEXT,
            is_pull_request_flag INTEGER NOT NULL DEFAULT 0,
            bug_candidate_flag   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(repo_key, issue_number),
            CHECK (is_pull_request_flag IN (0,1)),
            CHECK (bug_candidate_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS fact_github_issue_fix_pull_request (
            repo_key        TEXT NOT NULL,
            issue_number    INTEGER NOT NULL,
            pr_number       INTEGER NOT NULL,
            linked_at       TEXT,
            PRIMARY KEY(repo_key, issue_number, pr_number)
        );

        CREATE TABLE IF NOT EXISTS fact_github_pull_request_removed_line_hash (
            repo_key     TEXT NOT NULL,
            pr_number    INTEGER NOT NULL,
            rel_path     TEXT NOT NULL,
            line_hash    TEXT NOT NULL,
            count        INTEGER NOT NULL,
            PRIMARY KEY(repo_key, pr_number, rel_path, line_hash)
        );

        CREATE TABLE IF NOT EXISTS event_session_quality (
            provider                     TEXT NOT NULL,
            session_id                   TEXT NOT NULL,
            repo_root                    TEXT,
            repo_key                     TEXT,
            member_email                 TEXT NOT NULL DEFAULT '(unknown)',
            device_id                    TEXT NOT NULL DEFAULT '(unknown)',
            model_name                   TEXT,
            started_at                   TEXT,
            ended_at                     TEXT,
            user_turn_count              INTEGER NOT NULL DEFAULT 0,
            debug_loop_flag              INTEGER NOT NULL DEFAULT 0,
            mid_session_error_paste_flag INTEGER NOT NULL DEFAULT 0,
            accepted_output_flag         INTEGER NOT NULL DEFAULT 0,
            first_accepted_change_at     TEXT,
            minutes_to_first_accepted_change REAL,
            session_commit_within_4h_flag INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(provider, session_id),
            CHECK (debug_loop_flag IN (0,1)),
            CHECK (mid_session_error_paste_flag IN (0,1)),
            CHECK (accepted_output_flag IN (0,1)),
            CHECK (session_commit_within_4h_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS event_session_productivity (
            provider                     TEXT NOT NULL,
            session_id                   TEXT NOT NULL,
            repo_root                    TEXT,
            repo_key                     TEXT,
            member_email                 TEXT NOT NULL DEFAULT '(unknown)',
            device_id                    TEXT NOT NULL DEFAULT '(unknown)',
            model_name                   TEXT,
            project_path                 TEXT,
            started_at                   TEXT,
            ended_at                     TEXT,
            accepted_lines_added         INTEGER NOT NULL DEFAULT 0,
            accepted_lines_removed       INTEGER NOT NULL DEFAULT 0,
            accepted_total_changed_lines INTEGER NOT NULL DEFAULT 0,
            user_word_count              INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(provider, session_id)
        );

        CREATE TABLE IF NOT EXISTS event_commit_outcome (
            repo_root                 TEXT NOT NULL,
            repo_key                  TEXT,
            commit_sha                TEXT NOT NULL,
            commit_time               TEXT NOT NULL,
            heavy_ai_flag             INTEGER NOT NULL DEFAULT 0,
            merged_to_mainline_flag   INTEGER NOT NULL DEFAULT 0,
            reverted_later_flag       INTEGER NOT NULL DEFAULT 0,
            total_matched_ai_lines    INTEGER NOT NULL DEFAULT 0,
            commit_total_changed_lines INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(repo_root, commit_sha),
            CHECK (heavy_ai_flag IN (0,1)),
            CHECK (merged_to_mainline_flag IN (0,1)),
            CHECK (reverted_later_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS event_commit_churn (
            repo_root                         TEXT NOT NULL,
            repo_key                          TEXT,
            commit_sha                        TEXT NOT NULL,
            ai_added_lines_reaching_mainline  INTEGER NOT NULL DEFAULT 0,
            ai_added_lines_removed_within_window INTEGER NOT NULL DEFAULT 0,
            churn_window_days                 INTEGER NOT NULL DEFAULT 14,
            PRIMARY KEY(repo_root, commit_sha)
        );

        CREATE TABLE IF NOT EXISTS event_commit_bug_signal (
            repo_root                     TEXT NOT NULL,
            repo_key                      TEXT,
            commit_sha                    TEXT NOT NULL,
            bug_after_merge_flag          INTEGER NOT NULL DEFAULT 0,
            first_bug_signal_commit_sha   TEXT,
            first_bug_signal_commit_time  TEXT,
            bug_signal_count              INTEGER NOT NULL DEFAULT 0,
            window_days                   INTEGER NOT NULL DEFAULT 60,
            signal_source                 TEXT NOT NULL DEFAULT 'git_fix_commit',
            PRIMARY KEY(repo_root, commit_sha),
            CHECK (bug_after_merge_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS event_commit_session (
            repo_root                 TEXT NOT NULL,
            repo_key                  TEXT,
            commit_sha                TEXT NOT NULL,
            provider                  TEXT NOT NULL,
            session_id                TEXT NOT NULL,
            member_email              TEXT NOT NULL DEFAULT '(unknown)',
            device_id                 TEXT NOT NULL DEFAULT '(unknown)',
            commit_time               TEXT,
            model_name                TEXT,
            matched_lines             REAL NOT NULL DEFAULT 0.0,
            share_of_commit           REAL NOT NULL DEFAULT 0.0,
            share_of_ai               REAL NOT NULL DEFAULT 0.0,
            PRIMARY KEY(repo_root, commit_sha, provider, session_id)
        );

        CREATE TABLE IF NOT EXISTS event_task_commit (
            repo_root       TEXT NOT NULL,
            repo_key        TEXT,
            task_key        TEXT NOT NULL,
            branch_name     TEXT NOT NULL,
            commit_sha      TEXT NOT NULL,
            fallback_flag   INTEGER NOT NULL DEFAULT 0,
            confidence      REAL NOT NULL DEFAULT 0.0,
            commit_time     TEXT,
            PRIMARY KEY(repo_root, task_key, commit_sha),
            CHECK (fallback_flag IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS event_task_session (
            repo_root                    TEXT NOT NULL,
            repo_key                     TEXT,
            task_key                     TEXT NOT NULL,
            branch_name                  TEXT NOT NULL,
            provider                     TEXT NOT NULL,
            session_id                   TEXT NOT NULL,
            member_email                 TEXT NOT NULL DEFAULT '(unknown)',
            device_id                    TEXT NOT NULL DEFAULT '(unknown)',
            model_name                   TEXT,
            started_at                   TEXT,
            attribution_weight           REAL NOT NULL DEFAULT 0.0,
            commit_within_window_flag    INTEGER NOT NULL DEFAULT 0,
            user_turn_count              INTEGER,
            debug_loop_flag              INTEGER,
            mid_session_error_paste_flag INTEGER,
            accepted_output_flag         INTEGER,
            first_accepted_change_at     TEXT,
            minutes_to_first_accepted_change REAL,
            PRIMARY KEY(repo_root, task_key, provider, session_id),
            CHECK (commit_within_window_flag IN (0,1)),
            CHECK (debug_loop_flag IN (0,1) OR debug_loop_flag IS NULL),
            CHECK (mid_session_error_paste_flag IN (0,1) OR mid_session_error_paste_flag IS NULL),
            CHECK (accepted_output_flag IN (0,1) OR accepted_output_flag IS NULL)
        );

        CREATE TABLE IF NOT EXISTS event_commit_pr_outcome (
            repo_root          TEXT NOT NULL,
            repo_key           TEXT NOT NULL,
            commit_sha         TEXT NOT NULL,
            lookup_status      TEXT NOT NULL,
            pr_number          INTEGER,
            pr_opened_flag     INTEGER NOT NULL DEFAULT 0,
            pr_merged_flag     INTEGER NOT NULL DEFAULT 0,
            pr_created_at      TEXT,
            pr_merged_at       TEXT,
            PRIMARY KEY(repo_root, commit_sha),
            CHECK (pr_opened_flag IN (0,1)),
            CHECK (pr_merged_flag IN (0,1))
        );

        CREATE INDEX IF NOT EXISTS idx_fact_session_message_session
            ON fact_session_message(provider, session_id, message_index);
        CREATE INDEX IF NOT EXISTS idx_fact_session_change_session
            ON fact_session_code_change(provider, session_id, source_kind);
        CREATE INDEX IF NOT EXISTS idx_fact_session_change_source
            ON fact_session_code_change(provider, source_file, source_kind);
        CREATE INDEX IF NOT EXISTS idx_fact_session_change_repo_path_provider
            ON fact_session_code_change(repo_root, rel_path, provider);
        CREATE INDEX IF NOT EXISTS idx_fact_session_change_hash
            ON fact_session_code_change_line_hashes(side, line_hash);
        CREATE INDEX IF NOT EXISTS idx_fact_commit_repo_time
            ON fact_commit(repo_root, commit_time);
        CREATE INDEX IF NOT EXISTS idx_fact_commit_file_repo_commit
            ON fact_commit_file_change(repo_root, commit_sha, rel_path);
        CREATE INDEX IF NOT EXISTS idx_fact_commit_file_hash
            ON fact_commit_file_change_line_hashes(side, line_hash);
        CREATE INDEX IF NOT EXISTS idx_fact_commit_session_match_session
            ON fact_commit_session_match(provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_fact_commit_session_match_repo_commit
            ON fact_commit_session_match(repo_root, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_fact_task_commit_assignment_task
            ON fact_task_commit_assignment(task_key);
        CREATE INDEX IF NOT EXISTS idx_fact_task_commit_assignment_repo_commit
            ON fact_task_commit_assignment(repo_root, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_fact_github_pr_commit_commit
            ON fact_github_pull_request_commit(repo_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_fact_github_lookup_status
            ON fact_github_commit_pr_lookup(repo_key, status, last_checked_at);
        CREATE INDEX IF NOT EXISTS idx_fact_github_issue_bug
            ON fact_github_issue(repo_key, bug_candidate_flag, created_at);
        CREATE INDEX IF NOT EXISTS idx_fact_github_issue_fix_pr_issue
            ON fact_github_issue_fix_pull_request(repo_key, issue_number, pr_number);
        CREATE INDEX IF NOT EXISTS idx_fact_github_pr_removed_hash
            ON fact_github_pull_request_removed_line_hash(repo_key, pr_number, rel_path, line_hash);
        CREATE INDEX IF NOT EXISTS idx_event_session_quality_sync
            ON event_session_quality(repo_key, member_email, provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_session_productivity_sync
            ON event_session_productivity(repo_key, member_email, provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_commit_outcome_repo_key
            ON event_commit_outcome(repo_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_churn_repo_key
            ON event_commit_churn(repo_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_bug_signal_repo_key
            ON event_commit_bug_signal(repo_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_session_session
            ON event_commit_session(provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_commit_session_repo_commit
            ON event_commit_session(repo_root, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_session_sync
            ON event_commit_session(repo_key, member_email, provider, session_id, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_task_commit_sync
            ON event_task_commit(repo_key, task_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_task_session_task
            ON event_task_session(task_key);
        CREATE INDEX IF NOT EXISTS idx_event_task_session_sync
            ON event_task_session(repo_key, task_key, member_email, provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_commit_outcome_repo
            ON event_commit_outcome(repo_root, commit_time);
        CREATE INDEX IF NOT EXISTS idx_event_commit_pr_outcome_repo
            ON event_commit_pr_outcome(repo_root, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_pr_outcome_repo_key
            ON event_commit_pr_outcome(repo_key, commit_sha);",
    )?;
    let _ = conn.execute_batch("ALTER TABLE metadata_sessions ADD COLUMN model_id INTEGER;");
    for statement in [
        "ALTER TABLE fact_session_code_change ADD COLUMN source_file TEXT;",
        "ALTER TABLE fact_commit ADD COLUMN assoc_session_facts_version INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE event_session_quality ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_session_quality ADD COLUMN member_email TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_session_quality ADD COLUMN device_id TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_session_quality ADD COLUMN model_name TEXT;",
        "ALTER TABLE event_session_quality ADD COLUMN accepted_output_flag INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE event_session_quality ADD COLUMN first_accepted_change_at TEXT;",
        "ALTER TABLE event_session_quality ADD COLUMN minutes_to_first_accepted_change REAL;",
        "ALTER TABLE event_session_quality ADD COLUMN session_commit_within_4h_flag INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE event_session_productivity ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_session_productivity ADD COLUMN member_email TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_session_productivity ADD COLUMN device_id TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_session_productivity ADD COLUMN model_name TEXT;",
        "ALTER TABLE event_commit_outcome ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_commit_churn ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_commit_session ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_commit_session ADD COLUMN member_email TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_commit_session ADD COLUMN device_id TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_task_commit ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_task_session ADD COLUMN repo_key TEXT;",
        "ALTER TABLE event_task_session ADD COLUMN member_email TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_task_session ADD COLUMN device_id TEXT NOT NULL DEFAULT '(unknown)';",
        "ALTER TABLE event_task_session ADD COLUMN model_name TEXT;",
        "ALTER TABLE event_task_session ADD COLUMN started_at TEXT;",
        "ALTER TABLE event_task_session ADD COLUMN accepted_output_flag INTEGER;",
        "ALTER TABLE event_task_session ADD COLUMN first_accepted_change_at TEXT;",
        "ALTER TABLE event_task_session ADD COLUMN minutes_to_first_accepted_change REAL;",
        "ALTER TABLE fact_github_sync_state ADD COLUMN last_issue_scan_at TEXT;",
        "ALTER TABLE fact_github_pull_request ADD COLUMN removed_hashes_complete_flag INTEGER;",
    ] {
        let _ = conn.execute_batch(statement);
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_metadata_sessions_model
            ON metadata_sessions(model_id, provider);",
    )?;
    Ok(())
}

/// Returns true if any session metadata already exists for this session_id.
pub fn session_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    let evidence_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM metadata_sessions WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(evidence_count > 0)
}

fn derive_repo_root(project_path: Option<&str>, file_path: Option<&str>) -> Option<String> {
    if let Some(fp) = file_path {
        let fp_path = Path::new(fp);
        if fp_path.is_absolute()
            && let Some(root) = detect_repo_root(fp_path)
        {
            return Some(path_to_string(&root));
        }
    }

    if let Some(pp) = project_path {
        let pp_path = Path::new(pp);
        if pp_path.is_absolute()
            && let Some(root) = detect_repo_root(pp_path)
        {
            return Some(path_to_string(&root));
        }
    }

    None
}

fn derive_rel_and_file_name(
    repo_root: Option<&str>,
    file_path: &str,
) -> (Option<String>, Option<String>) {
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

fn repo_name_from_root(repo_root: &str) -> String {
    Path::new(repo_root)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| repo_root.to_string())
}

fn parse_task_key_parts(task_key: &str) -> (Option<String>, Option<i64>) {
    let Some((prefix, number)) = task_key.split_once('-') else {
        return (None, None);
    };
    let prefix = prefix.trim();
    let number = number.trim();
    if prefix.is_empty() || number.is_empty() || !number.chars().all(|ch| ch.is_ascii_digit()) {
        return (None, None);
    }

    (Some(prefix.to_string()), number.parse::<i64>().ok())
}

fn is_default_branch_name(branch_name: &str) -> bool {
    matches!(branch_name, "main" | "master")
}

fn is_integration_branch_name(branch_name: &str) -> bool {
    matches!(
        branch_name,
        "main" | "master" | "staging" | "develop" | "development" | "dev"
    ) || branch_name.starts_with("release/")
}

pub fn upsert_metadata_repository(conn: &Connection, repo_root: &str) -> Result<Option<i64>> {
    let repo_root = repo_root.trim();
    if repo_root.is_empty() {
        return Ok(None);
    }

    conn.execute(
        "INSERT INTO metadata_repositories (
            repo_root, repo_name
         ) VALUES (?1, ?2)
         ON CONFLICT(repo_root) DO UPDATE SET
            repo_name = excluded.repo_name,
            updated_at = datetime('now')",
        params![repo_root, repo_name_from_root(repo_root)],
    )?;

    let repo_id = conn.query_row(
        "SELECT id FROM metadata_repositories WHERE repo_root = ?1",
        params![repo_root],
        |row| row.get(0),
    )?;

    Ok(Some(repo_id))
}

pub fn upsert_metadata_session(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    project_path: Option<&str>,
    started_at: Option<&str>,
    ended_at: Option<&str>,
    source_path: Option<&str>,
) -> Result<()> {
    upsert_metadata_session_with_model_internal(
        conn,
        provider,
        session_id,
        project_path,
        started_at,
        ended_at,
        source_path,
        None,
        None,
        false,
    )
}

pub fn upsert_metadata_model(
    conn: &Connection,
    provider: &str,
    model_name: &str,
) -> Result<Option<i64>> {
    let provider = provider.trim();
    let model_name = normalize_provider_model_name(provider, model_name);
    if provider.is_empty() || model_name.is_empty() {
        return Ok(None);
    }

    conn.execute(
        "INSERT INTO metadata_models (provider, model_name)
         VALUES (?1, ?2)
         ON CONFLICT(provider, model_name) DO UPDATE SET
            updated_at = datetime('now')",
        params![provider, model_name],
    )?;

    let model_id = conn.query_row(
        "SELECT id FROM metadata_models WHERE provider = ?1 AND model_name = ?2",
        params![provider, model_name],
        |row| row.get(0),
    )?;

    Ok(Some(model_id))
}

#[expect(
    clippy::too_many_arguments,
    reason = "session upserts mirror the metadata row plus optional model identity fields"
)]
pub fn upsert_metadata_session_with_model(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    project_path: Option<&str>,
    started_at: Option<&str>,
    ended_at: Option<&str>,
    source_path: Option<&str>,
    _model_provider: Option<&str>,
    model_name: Option<&str>,
) -> Result<()> {
    upsert_metadata_session_with_model_internal(
        conn,
        provider,
        session_id,
        project_path,
        started_at,
        ended_at,
        source_path,
        None,
        model_name,
        true,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the internal session upsert writes the full metadata payload and one policy flag in a single call"
)]
fn upsert_metadata_session_with_model_internal(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    project_path: Option<&str>,
    started_at: Option<&str>,
    ended_at: Option<&str>,
    source_path: Option<&str>,
    _model_provider: Option<&str>,
    model_name: Option<&str>,
    store_unknown_if_missing: bool,
) -> Result<()> {
    let repo_root = derive_repo_root(project_path, source_path);
    let repository_id = if let Some(repo_root) = repo_root.as_deref() {
        upsert_metadata_repository(conn, repo_root)?
    } else {
        None
    };
    let normalized_model_name = model_name
        .and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| normalize_provider_model_name(provider, trimmed))
        })
        .or_else(|| {
            store_unknown_if_missing.then(|| normalize_provider_model_name(provider, "(unknown)"))
        });
    let model_id = match normalized_model_name.as_deref() {
        Some(model_name) => upsert_metadata_model(conn, provider, model_name)?,
        None => None,
    };

    conn.execute(
        "INSERT INTO metadata_sessions (
            provider, session_id, repository_id, model_id, project_path, started_at, ended_at, source_path
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(provider, session_id) DO UPDATE SET
            repository_id = COALESCE(excluded.repository_id, metadata_sessions.repository_id),
            model_id = COALESCE(excluded.model_id, metadata_sessions.model_id),
            project_path = COALESCE(excluded.project_path, metadata_sessions.project_path),
            started_at = CASE
                WHEN excluded.started_at IS NULL THEN metadata_sessions.started_at
                WHEN metadata_sessions.started_at IS NULL THEN excluded.started_at
                WHEN excluded.started_at < metadata_sessions.started_at THEN excluded.started_at
                ELSE metadata_sessions.started_at
            END,
            ended_at = CASE
                WHEN excluded.ended_at IS NULL THEN metadata_sessions.ended_at
                WHEN metadata_sessions.ended_at IS NULL THEN excluded.ended_at
                WHEN excluded.ended_at > metadata_sessions.ended_at THEN excluded.ended_at
                ELSE metadata_sessions.ended_at
            END,
            source_path = COALESCE(excluded.source_path, metadata_sessions.source_path),
            updated_at = datetime('now')",
        params![
            provider,
            session_id,
            repository_id,
            model_id,
            project_path,
            started_at,
            ended_at,
            source_path
        ],
    )?;

    Ok(())
}

fn normalize_provider_model_name(provider: &str, model_name: &str) -> String {
    let provider = provider.trim();
    let model_name = model_name.trim();
    if provider.is_empty() || model_name.is_empty() {
        return String::new();
    }

    let expected_prefix = format!("{provider}/");
    if model_name.starts_with(&expected_prefix) {
        return model_name.to_string();
    }

    format!("{provider}/{model_name}")
}

pub fn upsert_metadata_file(
    conn: &Connection,
    repo_root: &str,
    relative_path: &str,
) -> Result<Option<i64>> {
    let repo_root = repo_root.trim();
    let relative_path = relative_path.trim();
    if repo_root.is_empty() || relative_path.is_empty() {
        return Ok(None);
    }

    let Some(repository_id) = upsert_metadata_repository(conn, repo_root)? else {
        return Ok(None);
    };
    let file_path = Path::new(relative_path);
    let file_name = file_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| relative_path.to_string());
    let extension = file_path
        .extension()
        .map(|ext| ext.to_string_lossy().to_string())
        .filter(|ext| !ext.is_empty());

    conn.execute(
        "INSERT INTO metadata_files (
            repository_id, relative_path, file_name, extension
         ) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(repository_id, relative_path) DO UPDATE SET
            file_name = excluded.file_name,
            extension = COALESCE(excluded.extension, metadata_files.extension),
            updated_at = datetime('now')",
        params![repository_id, relative_path, file_name, extension],
    )?;

    let file_id = conn.query_row(
        "SELECT id
         FROM metadata_files
         WHERE repository_id = ?1 AND relative_path = ?2",
        params![repository_id, relative_path],
        |row| row.get(0),
    )?;

    Ok(Some(file_id))
}

pub fn upsert_metadata_task(conn: &Connection, task_key: &str) -> Result<Option<i64>> {
    let task_key = task_key.trim();
    if task_key.is_empty() {
        return Ok(None);
    }

    let (task_prefix, task_number) = parse_task_key_parts(task_key);
    if task_prefix.is_none() || task_number.is_none() {
        return Ok(None);
    }

    conn.execute(
        "INSERT INTO metadata_tasks (
            task_key, task_prefix, task_number
         ) VALUES (?1, ?2, ?3)
         ON CONFLICT(task_key) DO UPDATE SET
            task_prefix = COALESCE(excluded.task_prefix, metadata_tasks.task_prefix),
            task_number = COALESCE(excluded.task_number, metadata_tasks.task_number),
            updated_at = datetime('now')",
        params![task_key, task_prefix, task_number],
    )?;

    let task_id = conn.query_row(
        "SELECT id FROM metadata_tasks WHERE task_key = ?1",
        params![task_key],
        |row| row.get(0),
    )?;

    Ok(Some(task_id))
}

pub fn upsert_metadata_branch(
    conn: &Connection,
    repo_root: &str,
    branch_name: &str,
    task_key: Option<&str>,
) -> Result<()> {
    let repo_root = repo_root.trim();
    let branch_name = branch_name.trim();
    if repo_root.is_empty() || branch_name.is_empty() {
        return Ok(());
    }

    let Some(repository_id) = upsert_metadata_repository(conn, repo_root)? else {
        return Ok(());
    };
    let task_id = match task_key {
        Some(task_key) => upsert_metadata_task(conn, task_key)?,
        None => None,
    };

    conn.execute(
        "INSERT INTO metadata_branches (
            repository_id, branch_name, task_id, is_default_branch, is_integration_branch
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(repository_id, branch_name) DO UPDATE SET
            task_id = COALESCE(excluded.task_id, metadata_branches.task_id),
            is_default_branch = MAX(metadata_branches.is_default_branch, excluded.is_default_branch),
            is_integration_branch = MAX(metadata_branches.is_integration_branch, excluded.is_integration_branch),
            updated_at = datetime('now')",
        params![
            repository_id,
            branch_name,
            task_id,
            is_default_branch_name(branch_name) as i64,
            is_integration_branch_name(branch_name) as i64
        ],
    )?;

    Ok(())
}

#[derive(Default)]
pub struct SessionMetadata {
    pub project_path: Option<String>,
    pub repo_root: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
}

pub fn load_session_metadata(
    conn: &Connection,
    provider: &str,
    session_id: &str,
) -> Result<SessionMetadata> {
    let row = conn
        .query_row(
            "SELECT s.project_path, r.repo_root, s.started_at, s.ended_at
             FROM metadata_sessions s
             LEFT JOIN metadata_repositories r ON r.id = s.repository_id
             WHERE s.provider = ?1 AND s.session_id = ?2",
            params![provider, session_id],
            |row| {
                Ok(SessionMetadata {
                    project_path: row.get(0)?,
                    repo_root: row.get(1)?,
                    started_at: row.get(2)?,
                    ended_at: row.get(3)?,
                })
            },
        )
        .optional()?;

    Ok(row.unwrap_or_default())
}

#[expect(
    clippy::too_many_arguments,
    reason = "session starts write the same session metadata fields as the general upsert helper"
)]
pub fn begin_session_with_model(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    project_path: Option<&str>,
    started_at: Option<&str>,
    ended_at: Option<&str>,
    model_provider: Option<&str>,
    model_name: Option<&str>,
) -> Result<()> {
    upsert_metadata_session_with_model(
        conn,
        provider,
        session_id,
        project_path,
        started_at,
        ended_at,
        None,
        model_provider,
        model_name,
    )
}

pub fn ingest_session_message(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    role: &str,
    content: &str,
    content_words: i64,
    timestamp: Option<&str>,
) -> Result<()> {
    let metadata = load_session_metadata(conn, provider, session_id)?;
    upsert_metadata_session(
        conn,
        provider,
        session_id,
        metadata.project_path.as_deref(),
        metadata.started_at.as_deref(),
        timestamp.or(metadata.ended_at.as_deref()),
        None,
    )?;
    insert_fact_session_message(
        conn,
        provider,
        session_id,
        timestamp,
        role,
        content,
        content_words,
    )
}

pub fn ingest_accepted_code_change(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    file_path: &str,
    lines_added: i64,
    lines_removed: i64,
    timestamp: Option<&str>,
) -> Result<()> {
    let metadata = load_session_metadata(conn, provider, session_id)?;
    let repo_root = metadata
        .repo_root
        .clone()
        .or_else(|| derive_repo_root(metadata.project_path.as_deref(), Some(file_path)));
    let (rel_path, _) = derive_rel_and_file_name(repo_root.as_deref(), file_path);

    upsert_metadata_session(
        conn,
        provider,
        session_id,
        metadata.project_path.as_deref(),
        metadata.started_at.as_deref(),
        timestamp.or(metadata.ended_at.as_deref()),
        Some(file_path),
    )?;
    if let (Some(repo_root), Some(rel_path)) = (repo_root.as_deref(), rel_path.as_deref()) {
        upsert_metadata_file(conn, repo_root, rel_path)?;
    }
    insert_fact_accepted_code_change(
        conn,
        provider,
        session_id,
        timestamp,
        repo_root.as_deref(),
        file_path,
        rel_path.as_deref(),
        lines_added,
        lines_removed,
    )
}

fn next_message_index(conn: &Connection, provider: &str, session_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT COALESCE(MAX(message_index), -1) + 1
         FROM fact_session_message
         WHERE provider = ?1 AND session_id = ?2",
        params![provider, session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn next_accepted_change_index(conn: &Connection, provider: &str, session_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT COALESCE(MAX(change_index), -1) + 1
         FROM fact_session_code_change
         WHERE provider = ?1
           AND session_id = ?2
           AND source_kind = 'accepted_change'",
        params![provider, session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

pub fn insert_fact_session_message(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    message_ts: Option<&str>,
    role: &str,
    content: &str,
    content_words: i64,
) -> Result<()> {
    let message_index = next_message_index(conn, provider, session_id)?;
    conn.execute(
        "INSERT INTO fact_session_message (
            provider, session_id, message_index, message_ts, role, content, content_words
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            provider,
            session_id,
            message_index,
            message_ts,
            role,
            content,
            content_words
        ],
    )?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "accepted code change facts map directly to persisted analytics columns"
)]
pub fn insert_fact_accepted_code_change(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    change_ts: Option<&str>,
    repo_root: Option<&str>,
    abs_path: &str,
    rel_path: Option<&str>,
    lines_added: i64,
    lines_removed: i64,
) -> Result<()> {
    let change_index = next_accepted_change_index(conn, provider, session_id)?;
    conn.execute(
        "INSERT INTO fact_session_code_change (
            provider, session_id, change_index, change_ts, repo_root, abs_path, rel_path, source_file,
            lines_added, lines_removed, source_kind
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, 'accepted_change')",
        params![
            provider,
            session_id,
            change_index,
            change_ts,
            repo_root,
            abs_path,
            rel_path,
            lines_added,
            lines_removed
        ],
    )?;
    Ok(())
}

pub fn upsert_fact_tool_write(
    conn: &Connection,
    op: &crate::change_intel::types::ChangeOpCandidate,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO fact_session_code_change (
            provider, session_id, change_index, change_ts, repo_root, abs_path, rel_path, source_file,
            lines_added, lines_removed, source_kind, write_mode, parser_name, call_id, op_index, before_known
         ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'tool_write', ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(provider, session_id, source_kind, call_id, op_index) DO UPDATE SET
            change_ts = excluded.change_ts,
            repo_root = excluded.repo_root,
            abs_path = excluded.abs_path,
            rel_path = excluded.rel_path,
            source_file = excluded.source_file,
            lines_added = excluded.lines_added,
            lines_removed = excluded.lines_removed,
            write_mode = excluded.write_mode,
            parser_name = excluded.parser_name,
            before_known = excluded.before_known",
        params![
            op.provider,
            op.session_id,
            op.timestamp,
            op.repo_root,
            op.abs_path,
            op.rel_path,
            op.source_file,
            op.added_lines,
            op.removed_lines,
            op.write_mode.as_str(),
            op.parser_name,
            op.call_id,
            op.op_index,
            op.before_known as i64
        ],
    )?;

    conn.query_row(
        "SELECT id
         FROM fact_session_code_change
         WHERE provider = ?1
           AND session_id = ?2
           AND source_kind = 'tool_write'
           AND call_id = ?3
           AND op_index = ?4",
        params![op.provider, op.session_id, op.call_id, op.op_index],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

#[derive(Debug, Clone)]
pub struct ToolWriteSnapshot {
    pub id: i64,
    pub session_id: String,
    pub call_id: String,
    pub op_index: i32,
    pub repo_root: Option<String>,
    pub rel_path: Option<String>,
    pub line_hashes: Vec<crate::change_intel::types::LineHashCount>,
}

pub fn list_fact_tool_writes_by_source(
    conn: &Connection,
    provider: &str,
    source_file: &str,
) -> Result<Vec<ToolWriteSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT
            id, provider, session_id, source_file, call_id, op_index, change_ts, repo_root,
            abs_path, rel_path, write_mode, parser_name, before_known, lines_added, lines_removed
         FROM fact_session_code_change
         WHERE provider = ?1
           AND source_kind = 'tool_write'
           AND source_file = ?2
         ORDER BY session_id, call_id, op_index",
    )?;

    let rows = stmt.query_map(params![provider, source_file], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(4)?,
            row.get::<_, i32>(5)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(9)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (id, session_id, call_id, op_index, repo_root, rel_path) = row?;
        out.push(ToolWriteSnapshot {
            id,
            session_id,
            call_id,
            op_index,
            repo_root,
            rel_path,
            line_hashes: load_fact_session_code_change_line_hashes(conn, id)?,
        });
    }

    Ok(out)
}

pub fn load_fact_session_code_change_line_hashes(
    conn: &Connection,
    code_change_id: i64,
) -> Result<Vec<crate::change_intel::types::LineHashCount>> {
    let mut stmt = conn.prepare(
        "SELECT side, line_hash, count
         FROM fact_session_code_change_line_hashes
         WHERE code_change_id = ?1
         ORDER BY side, line_hash",
    )?;
    let rows = stmt.query_map(params![code_change_id], |row| {
        let side_raw: String = row.get(0)?;
        let side = match side_raw.as_str() {
            "+" => crate::change_intel::types::LineSide::Added,
            "-" => crate::change_intel::types::LineSide::Removed,
            other => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unsupported line side '{other}'"),
                    )),
                ));
            }
        };
        Ok(crate::change_intel::types::LineHashCount {
            side,
            line_hash: row.get(1)?,
            count: row.get(2)?,
        })
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn delete_fact_session_code_change_by_id(conn: &Connection, code_change_id: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM fact_session_code_change WHERE id = ?1",
        params![code_change_id],
    )?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "commit fact upserts intentionally mirror the stored commit columns"
)]
pub fn upsert_fact_commit(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    parent_sha: Option<&str>,
    commit_time: &str,
    subject: &str,
    total_added: i64,
    total_removed: i64,
) -> Result<()> {
    upsert_metadata_repository(conn, repo_root)?;
    conn.execute(
        "INSERT INTO fact_commit (
            repo_root, commit_sha, parent_sha, commit_time, subject, total_added, total_removed,
            assoc_session_facts_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
         ON CONFLICT(repo_root, commit_sha) DO UPDATE SET
            parent_sha = excluded.parent_sha,
            commit_time = excluded.commit_time,
            subject = excluded.subject,
            total_added = excluded.total_added,
            total_removed = excluded.total_removed,
            updated_at = datetime('now')",
        params![
            repo_root,
            commit_sha,
            parent_sha,
            commit_time,
            subject,
            total_added,
            total_removed
        ],
    )?;
    Ok(())
}

pub fn replace_fact_commit_file_changes(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    files: &[(String, String, i64, i64)],
) -> Result<()> {
    conn.execute(
        "DELETE FROM fact_commit_file_change
         WHERE repo_root = ?1 AND commit_sha = ?2",
        params![repo_root, commit_sha],
    )?;

    for (rel_path, change_type, added_lines, removed_lines) in files {
        upsert_metadata_file(conn, repo_root, rel_path)?;
        conn.execute(
            "INSERT INTO fact_commit_file_change (
                repo_root, commit_sha, rel_path, change_type, added_lines, removed_lines
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                repo_root,
                commit_sha,
                rel_path,
                change_type,
                added_lines,
                removed_lines
            ],
        )?;
    }

    Ok(())
}

pub fn replace_fact_session_code_change_line_hashes(
    conn: &Connection,
    code_change_id: i64,
    line_hashes: &[crate::change_intel::types::LineHashCount],
) -> Result<()> {
    conn.execute(
        "DELETE FROM fact_session_code_change_line_hashes WHERE code_change_id = ?1",
        params![code_change_id],
    )?;
    for line_hash in line_hashes {
        conn.execute(
            "INSERT INTO fact_session_code_change_line_hashes (
                code_change_id, side, line_hash, count
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                code_change_id,
                line_hash.side.as_str(),
                line_hash.line_hash,
                line_hash.count
            ],
        )?;
    }
    Ok(())
}

pub fn replace_fact_commit_file_change_line_hashes(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    rel_path: &str,
    line_hashes: &[crate::change_intel::types::LineHashCount],
) -> Result<()> {
    let file_change_id: i64 = conn.query_row(
        "SELECT id
         FROM fact_commit_file_change
         WHERE repo_root = ?1 AND commit_sha = ?2 AND rel_path = ?3",
        params![repo_root, commit_sha, rel_path],
        |row| row.get(0),
    )?;

    conn.execute(
        "DELETE FROM fact_commit_file_change_line_hashes WHERE file_change_id = ?1",
        params![file_change_id],
    )?;
    for line_hash in line_hashes {
        conn.execute(
            "INSERT INTO fact_commit_file_change_line_hashes (
                file_change_id, side, line_hash, count
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                file_change_id,
                line_hash.side.as_str(),
                line_hash.line_hash,
                line_hash.count
            ],
        )?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "commit attribution updates write a fixed set of derived analytics fields together"
)]
pub fn update_fact_commit_attribution(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    matched_total_lines: i64,
    matched_added_lines: i64,
    matched_removed_lines: i64,
    ai_share: f64,
    heavy_ai: bool,
    assoc_session_facts_version: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE fact_commit
         SET matched_total_lines = ?3,
             matched_added_lines = ?4,
             matched_removed_lines = ?5,
             ai_share = ?6,
             heavy_ai = ?7,
             assoc_session_facts_version = ?8,
             updated_at = datetime('now')
         WHERE repo_root = ?1 AND commit_sha = ?2",
        params![
            repo_root,
            commit_sha,
            matched_total_lines,
            matched_added_lines,
            matched_removed_lines,
            ai_share,
            heavy_ai as i64,
            assoc_session_facts_version
        ],
    )?;
    Ok(())
}

pub fn replace_fact_commit_session_matches(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    rows: &[(String, String, f64, f64, f64)],
) -> Result<()> {
    conn.execute(
        "DELETE FROM fact_commit_session_match
         WHERE repo_root = ?1 AND commit_sha = ?2",
        params![repo_root, commit_sha],
    )?;

    for (provider, session_id, matched_lines, share_of_commit, share_of_ai) in rows {
        conn.execute(
            "INSERT INTO fact_commit_session_match (
                repo_root, commit_sha, provider, session_id, matched_lines, share_of_commit, share_of_ai
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                repo_root,
                commit_sha,
                provider,
                session_id,
                matched_lines,
                share_of_commit,
                share_of_ai
            ],
        )?;
    }

    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "task assignment facts are written as one denormalized row"
)]
pub fn upsert_fact_task_commit_assignment(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    branch_name: &str,
    task_key: &str,
    source: &str,
    is_fallback: bool,
    candidate_count: i64,
    distance_to_tip: Option<i64>,
    confidence: f64,
) -> Result<()> {
    upsert_metadata_branch(conn, repo_root, branch_name, Some(task_key))?;
    conn.execute(
        "INSERT INTO fact_task_commit_assignment (
            repo_root, commit_sha, branch_name, task_key, source, is_fallback,
            candidate_count, distance_to_tip, confidence
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(repo_root, commit_sha) DO UPDATE SET
            branch_name = excluded.branch_name,
            task_key = excluded.task_key,
            source = excluded.source,
            is_fallback = excluded.is_fallback,
            candidate_count = excluded.candidate_count,
            distance_to_tip = excluded.distance_to_tip,
            confidence = excluded.confidence,
            updated_at = datetime('now')",
        params![
            repo_root,
            commit_sha,
            branch_name,
            task_key,
            source,
            is_fallback as i64,
            candidate_count,
            distance_to_tip,
            confidence
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct ScopedEnvVar {
        key: &'static str,
        original: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    fn open_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        Ok(conn)
    }

    #[test]
    fn metadata_tasks_only_store_ticket_keys() -> Result<()> {
        let conn = open_test_db()?;

        assert!(upsert_metadata_task(&conn, "PAC-123")?.is_some());
        assert!(upsert_metadata_task(&conn, "main")?.is_none());
        assert!(upsert_metadata_task(&conn, "(unknown)")?.is_none());

        let task_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM metadata_tasks", [], |row| row.get(0))?;
        assert_eq!(task_count, 1);

        Ok(())
    }

    #[test]
    fn metadata_sessions_link_to_detected_repository() -> Result<()> {
        let conn = open_test_db()?;
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("sample-repo");
        std::fs::create_dir_all(&repo_root)?;
        let status = std::process::Command::new("git")
            .current_dir(&repo_root)
            .args(["init", "-q"])
            .status()?;
        assert!(status.success());

        upsert_metadata_session(
            &conn,
            "codex",
            "session-1",
            Some(&repo_root.to_string_lossy()),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
        )?;
        upsert_metadata_file(&conn, &repo_root.to_string_lossy(), "src/lib.rs")?;

        let repo_row: (String, String) = conn.query_row(
            "SELECT repo_root, repo_name FROM metadata_repositories",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(
            repo_row.0,
            std::fs::canonicalize(&repo_root)?.to_string_lossy()
        );
        assert_eq!(repo_row.1, "sample-repo");

        let session_row: (Option<i64>, Option<String>, Option<String>) = conn.query_row(
            "SELECT repository_id, started_at, ended_at
             FROM metadata_sessions
             WHERE provider = 'codex' AND session_id = 'session-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert!(session_row.0.is_some());
        assert_eq!(session_row.1.as_deref(), Some("2026-03-17T09:00:00Z"));
        assert_eq!(session_row.2.as_deref(), Some("2026-03-17T09:30:00Z"));

        let file_row: (String, String) = conn.query_row(
            "SELECT relative_path, file_name FROM metadata_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(file_row.0, "src/lib.rs");
        assert_eq!(file_row.1, "lib.rs");

        Ok(())
    }

    #[test]
    fn metadata_sessions_store_provider_qualified_models_including_unknown() -> Result<()> {
        let conn = open_test_db()?;

        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "session-known",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        upsert_metadata_session_with_model(
            &conn,
            "cursor",
            "session-unknown",
            Some("/tmp/repo"),
            Some("2026-03-17T10:00:00Z"),
            Some("2026-03-17T10:30:00Z"),
            None,
            Some("cursor"),
            None,
        )?;

        let known_model: String = conn.query_row(
            "SELECT mm.model_name
             FROM metadata_sessions ms
             JOIN metadata_models mm ON mm.id = ms.model_id
             WHERE ms.provider = 'codex' AND ms.session_id = 'session-known'",
            [],
            |row| row.get(0),
        )?;
        let unknown_model: String = conn.query_row(
            "SELECT mm.model_name
             FROM metadata_sessions ms
             JOIN metadata_models mm ON mm.id = ms.model_id
             WHERE ms.provider = 'cursor' AND ms.session_id = 'session-unknown'",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(known_model, "codex/gpt-5");
        assert_eq!(unknown_model, "cursor/(unknown)");
        Ok(())
    }

    #[test]
    fn session_updates_do_not_overwrite_detected_model_with_unknown() -> Result<()> {
        let conn = open_test_db()?;

        begin_session_with_model(
            &conn,
            "codex",
            "session-1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            None,
        )?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "session-1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:35:00Z"),
            None,
            None,
            Some("gpt-5"),
        )?;
        upsert_metadata_session(
            &conn,
            "codex",
            "session-1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:40:00Z"),
            None,
        )?;

        let model_name: String = conn.query_row(
            "SELECT mm.model_name
             FROM metadata_sessions ms
             JOIN metadata_models mm ON mm.id = ms.model_id
             WHERE ms.provider = 'codex' AND ms.session_id = 'session-1'",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(model_name, "codex/gpt-5");
        Ok(())
    }

    #[test]
    fn metadata_schema_migrates_existing_sessions_table_with_model_id() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE metadata_repositories (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_root      TEXT NOT NULL UNIQUE,
                repo_name      TEXT NOT NULL,
                origin_url     TEXT,
                default_branch TEXT,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE metadata_sessions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                provider      TEXT NOT NULL,
                session_id    TEXT NOT NULL,
                repository_id INTEGER,
                project_path  TEXT,
                started_at    TEXT,
                ended_at      TEXT,
                source_path   TEXT,
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                UNIQUE(provider, session_id),
                FOREIGN KEY(repository_id) REFERENCES metadata_repositories(id) ON DELETE SET NULL
            );",
        )?;

        init_metadata_schema(&conn)?;

        let has_model_id: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM pragma_table_info('metadata_sessions')
             WHERE name = 'model_id'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(has_model_id, 1);

        conn.execute(
            "INSERT INTO metadata_sessions (provider, session_id, model_id)
             VALUES ('codex', 's1', 1)",
            [],
        )?;

        Ok(())
    }

    #[test]
    fn metadata_schema_creates_github_pr_tables() -> Result<()> {
        let conn = open_test_db()?;

        for table in [
            "fact_github_pull_request",
            "fact_github_pull_request_commit",
            "fact_github_commit_pr_lookup",
            "fact_github_sync_state",
            "fact_github_issue",
            "fact_github_issue_fix_pull_request",
            "fact_github_pull_request_removed_line_hash",
            "event_commit_pr_outcome",
            "event_commit_bug_signal",
        ] {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*)
                 FROM sqlite_master
                 WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )?;
            assert_eq!(count, 1, "missing table {table}");
        }

        Ok(())
    }

    #[test]
    fn open_uses_paceflow_home_env_and_db_path() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());

        let _conn = open()?;

        assert!(
            tempdir
                .path()
                .join(".paceflow")
                .join("paceflow.db")
                .is_file()
        );
        Ok(())
    }
}
