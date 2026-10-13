//! Derived insight tables. All of them are rebuilt from facts and can be
//! dropped at any time; manual overrides and classifier results live in
//! separate tables that are never rebuilt.

pub(crate) const INSIGHTS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_turn (
    provider            TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    turn_index          INTEGER NOT NULL,
    root_session_id     TEXT NOT NULL,
    started_at          TEXT,
    ended_at            TEXT,
    user_text_head      TEXT,
    user_chars          INTEGER NOT NULL DEFAULT 0,
    assistant_chars     INTEGER NOT NULL DEFAULT 0,
    tool_calls          INTEGER NOT NULL DEFAULT 0,
    edit_calls          INTEGER NOT NULL DEFAULT 0,
    validation_runs     INTEGER NOT NULL DEFAULT 0,
    validation_failures INTEGER NOT NULL DEFAULT 0,
    usage_events        INTEGER NOT NULL DEFAULT 0,
    tokens_total        INTEGER NOT NULL DEFAULT 0,
    api_equiv_usd       REAL NOT NULL DEFAULT 0,
    model_names         TEXT,
    PRIMARY KEY(provider, session_id, turn_index)
);

CREATE TABLE IF NOT EXISTS derived_usage_event (
    usage_id          INTEGER PRIMARY KEY,
    provider          TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    root_session_id   TEXT NOT NULL,
    usage_ts          TEXT,
    model_name        TEXT,
    turn_index        INTEGER,
    root_turn_index   INTEGER,
    task_index        INTEGER,
    fresh_tokens      INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens     INTEGER NOT NULL DEFAULT 0,
    total_tokens      INTEGER NOT NULL DEFAULT 0,
    context_tokens    INTEGER NOT NULL DEFAULT 0,
    api_equiv_usd     REAL,
    reported_cost_usd REAL,
    price_id          TEXT,
    price_match       TEXT,
    token_source      TEXT NOT NULL,
    background_flag   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS derived_task (
    provider                    TEXT NOT NULL,
    session_id                  TEXT NOT NULL,
    task_index                  INTEGER NOT NULL,
    segmentation_source         TEXT NOT NULL,
    first_turn                  INTEGER,
    last_turn                   INTEGER,
    completion_turn             INTEGER,
    turn_list                   TEXT,
    title                       TEXT,
    started_at                  TEXT,
    completed_at                TEXT,
    last_activity_at            TEXT,
    completion_source           TEXT NOT NULL,
    wall_minutes_to_complete    REAL,
    active_minutes_to_complete  REAL,
    active_minutes_total        REAL,
    agent_minutes               REAL,
    user_minutes                REAL,
    turns                       INTEGER NOT NULL DEFAULT 0,
    attempts                    INTEGER NOT NULL DEFAULT 0,
    validations                 INTEGER NOT NULL DEFAULT 0,
    validation_failures         INTEGER NOT NULL DEFAULT 0,
    loop_attempts               INTEGER NOT NULL DEFAULT 0,
    edit_calls                  INTEGER NOT NULL DEFAULT 0,
    lines_changed               INTEGER NOT NULL DEFAULT 0,
    files_touched               INTEGER NOT NULL DEFAULT 0,
    subagents                   INTEGER NOT NULL DEFAULT 0,
    primary_model               TEXT,
    models                      TEXT,
    model_changes               INTEGER NOT NULL DEFAULT 0,
    harness                     TEXT NOT NULL,
    repo_root                   TEXT,
    git_branch                  TEXT,
    tokens_total                INTEGER NOT NULL DEFAULT 0,
    fresh_tokens                INTEGER NOT NULL DEFAULT 0,
    cost_total_usd              REAL,
    cost_to_complete_usd        REAL,
    post_completion_cost_usd    REAL,
    subagent_cost_usd           REAL,
    unpriced_tokens             INTEGER NOT NULL DEFAULT 0,
    estimated_tokens            INTEGER NOT NULL DEFAULT 0,
    commits                     INTEGER NOT NULL DEFAULT 0,
    content_digest              TEXT,
    PRIMARY KEY(provider, session_id, task_index)
);

CREATE TABLE IF NOT EXISTS derived_attempt (
    provider          TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    task_index        INTEGER NOT NULL,
    attempt_index     INTEGER NOT NULL,
    turn_index        INTEGER,
    started_at        TEXT,
    ended_at          TEXT,
    edit_calls        INTEGER NOT NULL DEFAULT 0,
    lines_changed     INTEGER NOT NULL DEFAULT 0,
    validation_kind   TEXT,
    validation_result TEXT NOT NULL,
    error_signature   TEXT,
    ended_by          TEXT NOT NULL,
    loop_flag         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(provider, session_id, task_index, attempt_index)
);

CREATE TABLE IF NOT EXISTS task_turn_assignment (
    provider      TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    turn_index    INTEGER NOT NULL,
    task_index    INTEGER NOT NULL,
    source        TEXT NOT NULL,
    confidence    REAL,
    PRIMARY KEY(provider, session_id, turn_index, source)
);

CREATE TABLE IF NOT EXISTS task_completion_mark (
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    task_index      INTEGER NOT NULL,
    completion_turn INTEGER,
    source          TEXT NOT NULL,
    confidence      REAL,
    PRIMARY KEY(provider, session_id, task_index, source)
);

CREATE TABLE IF NOT EXISTS classifier_run (
    run_id               INTEGER PRIMARY KEY,
    started_at           TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at          TEXT,
    model                TEXT NOT NULL,
    endpoint             TEXT NOT NULL,
    question_set_version TEXT NOT NULL,
    level                TEXT NOT NULL,
    units_planned        INTEGER NOT NULL DEFAULT 0,
    units_sent           INTEGER NOT NULL DEFAULT 0,
    units_cached         INTEGER NOT NULL DEFAULT 0,
    input_tokens         INTEGER NOT NULL DEFAULT 0,
    cost_usd             REAL NOT NULL DEFAULT 0,
    status               TEXT NOT NULL,
    error                TEXT
);

CREATE TABLE IF NOT EXISTS classifier_request (
    request_id     INTEGER PRIMARY KEY,
    run_id         INTEGER NOT NULL,
    unit_kind      TEXT NOT NULL,
    unit_key       TEXT NOT NULL,
    cache_key      TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    est_tokens     INTEGER NOT NULL DEFAULT 0,
    input_tokens   INTEGER,
    cost_usd       REAL,
    http_status    INTEGER,
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    degraded_level TEXT,
    redactions     INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS classification_result (
    id                 INTEGER PRIMARY KEY,
    unit_kind          TEXT NOT NULL,
    unit_key           TEXT NOT NULL,
    question_id        TEXT NOT NULL,
    question_version   TEXT NOT NULL,
    source             TEXT NOT NULL,
    label              TEXT,
    score              REAL,
    probability        REAL,
    probabilities_json TEXT,
    confidence         REAL,
    band               TEXT NOT NULL,
    model_version      TEXT,
    cache_key          TEXT NOT NULL,
    run_id             INTEGER,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    superseded_by      INTEGER,
    UNIQUE(unit_kind, unit_key, question_id, source, cache_key)
);

CREATE TABLE IF NOT EXISTS classification_override (
    unit_kind   TEXT NOT NULL,
    unit_key    TEXT NOT NULL,
    question_id TEXT NOT NULL,
    label       TEXT NOT NULL,
    note        TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(unit_kind, unit_key, question_id)
);

CREATE TABLE IF NOT EXISTS classification_gold (
    unit_kind   TEXT NOT NULL,
    unit_key    TEXT NOT NULL,
    question_id TEXT NOT NULL,
    label       TEXT NOT NULL,
    origin      TEXT NOT NULL,
    labeled_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(unit_kind, unit_key, question_id, origin)
);

CREATE INDEX IF NOT EXISTS idx_classification_result_unit
    ON classification_result(unit_kind, unit_key, question_id);

-- Effective label per unit and question: override > classifier (non-uncertain,
-- newest) > heuristic. Uncertain classifier answers stay visible via `band`.
CREATE VIEW IF NOT EXISTS view_label_effective AS
WITH ranked AS (
    SELECT unit_kind, unit_key, question_id, label, score, probability, confidence, band,
           source,
           ROW_NUMBER() OVER (
               PARTITION BY unit_kind, unit_key, question_id
               ORDER BY CASE
                            WHEN source = 'jev' AND band IN ('confident', 'tentative') THEN 0
                            WHEN source = 'jev' THEN 2
                            ELSE 1
                        END,
                        id DESC
           ) AS rank
    FROM classification_result
    WHERE superseded_by IS NULL
)
SELECT o.unit_kind, o.unit_key, o.question_id, o.label, NULL AS score, NULL AS probability,
       1.0 AS confidence, 'override' AS band, 'override' AS source
FROM classification_override o
UNION ALL
SELECT r.unit_kind, r.unit_key, r.question_id, r.label, r.score, r.probability, r.confidence,
       r.band, r.source
FROM ranked r
WHERE r.rank = 1
  AND NOT EXISTS (
      SELECT 1 FROM classification_override o
      WHERE o.unit_kind = r.unit_kind AND o.unit_key = r.unit_key
        AND o.question_id = r.question_id
  );

CREATE INDEX IF NOT EXISTS idx_derived_usage_event_ts ON derived_usage_event(usage_ts);
CREATE INDEX IF NOT EXISTS idx_derived_usage_event_root
    ON derived_usage_event(provider, root_session_id, root_turn_index);
CREATE INDEX IF NOT EXISTS idx_derived_task_started ON derived_task(started_at);
";
