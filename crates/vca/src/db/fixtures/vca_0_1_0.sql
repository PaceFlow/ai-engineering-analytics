-- Original VCA schema, before the AEA rename.
CREATE TABLE IF NOT EXISTS metadata_repositories (
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
        CREATE INDEX IF NOT EXISTS idx_event_session_quality_sync
            ON event_session_quality(repo_key, member_email, provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_session_productivity_sync
            ON event_session_productivity(repo_key, member_email, provider, session_id);
        CREATE INDEX IF NOT EXISTS idx_event_commit_outcome_repo_key
            ON event_commit_outcome(repo_key, commit_sha);
        CREATE INDEX IF NOT EXISTS idx_event_commit_churn_repo_key
            ON event_commit_churn(repo_key, commit_sha);
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
CREATE INDEX IF NOT EXISTS idx_metadata_sessions_model
            ON metadata_sessions(model_id, provider);
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
            ON commit_assoc_dirty_hash(repo_root);