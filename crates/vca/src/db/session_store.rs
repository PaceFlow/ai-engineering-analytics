//! Normalized per-session records and their storage.
//!
//! Providers parse their native sources into a [`SessionRecord`]. Storing a
//! record replaces every session-layer fact for that session (messages, usage,
//! accepted changes, tool calls, events, context injections, rate-limit
//! snapshots) in one transaction, keyed by natural keys. This lets a session
//! that grows after its first ingest be re-read in full, and makes usage
//! de-duplication a property of the record rather than of each call site.
//!
//! Change-intel `tool_write` rows and their line hashes are deliberately not
//! touched here: they have their own reconciliation path that tracks dirty
//! hashes for commit association.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

/// Bump when parsers start extracting new facts so existing sources are re-read.
pub const SESSION_INGEST_VERSION: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentKind {
    Main,
    Subagent,
    Sidechain,
    BestOfN,
    Fork,
}

impl AgentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Subagent => "subagent",
            Self::Sidechain => "sidechain",
            Self::BestOfN => "best_of_n",
            Self::Fork => "fork",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenSource {
    Reported,
    EstimatedText,
}

impl TokenSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::EstimatedText => "estimated_text",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UsageRecord {
    /// Natural key unique within the session (API message id, turn id, ...).
    pub key: String,
    pub ts: Option<String>,
    pub model: Option<String>,
    /// Fresh (uncached) input tokens.
    pub input_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    /// Portion of `cache_write_tokens` written with a 1 hour TTL.
    pub cache_write_1h_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
    /// True when `output_tokens` already contains `reasoning_tokens` (Codex).
    pub reasoning_in_output: bool,
    pub reported_cost_usd: Option<f64>,
    pub token_source: TokenSource,
    pub service_tier: Option<String>,
    pub provider_route: Option<String>,
    pub stop_reason: Option<String>,
    /// Context window of the model when the provider reports it.
    pub context_window: Option<i64>,
}

impl UsageRecord {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            ts: None,
            model: None,
            input_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            cache_write_1h_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            reasoning_in_output: false,
            reported_cost_usd: None,
            token_source: TokenSource::Reported,
            service_tier: None,
            provider_route: None,
            stop_reason: None,
            context_window: None,
        }
    }

    pub fn total_tokens(&self) -> i64 {
        let reasoning_extra = if self.reasoning_in_output {
            0
        } else {
            self.reasoning_tokens
        };
        self.input_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
            + self.output_tokens
            + reasoning_extra
    }

    /// Tokens present in the model context for this call (input side).
    pub fn context_tokens(&self) -> i64 {
        self.input_tokens + self.cache_read_tokens + self.cache_write_tokens
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageRecord {
    pub key: String,
    pub ts: Option<String>,
    pub role: String,
    pub text: String,
    pub is_compaction_summary: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ToolCallRecord {
    pub key: String,
    pub ts: Option<String>,
    pub ended_ts: Option<String>,
    pub tool_name: String,
    /// Primary target: path, glob, command, url or query.
    pub target: Option<String>,
    pub input_chars: i64,
    pub output_chars: i64,
    /// False when the provider marks the output as not sent to the model.
    pub output_in_context: bool,
    /// `ok|error|rejected|interrupted|unknown`.
    pub status: String,
    pub exit_code: Option<i64>,
    /// First lines of error output, kept local for error signatures.
    pub error_head: Option<String>,
    pub child_session_id: Option<String>,
    pub agent_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedChangeRecord {
    pub abs_path: String,
    pub lines_added: i64,
    pub lines_removed: i64,
    pub ts: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionEventRecord {
    pub ts: Option<String>,
    /// `compaction|clear|model_change|turn_aborted|api_error|task_complete`.
    pub kind: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextInjectionRecord {
    pub key: String,
    pub ts: Option<String>,
    /// `instructions|tool_definitions|skill_listing|mcp_instructions|agent_listing|environment|reminder|attachment_other`.
    pub kind: String,
    pub source: Option<String>,
    pub chars: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitRecord {
    pub ts: String,
    pub limit_id: String,
    /// `primary|secondary`.
    pub window_kind: String,
    pub window_minutes: Option<i64>,
    pub used_percent: f64,
    pub resets_at: Option<String>,
    pub plan_type: Option<String>,
    pub credits_balance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub provider: String,
    pub session_id: String,
    pub source_path: Option<String>,
    pub project_path: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    /// Primary model (most output tokens), used for legacy per-session columns.
    pub model: Option<String>,
    pub parent_session_id: Option<String>,
    pub agent_kind: AgentKind,
    pub agent_type: Option<String>,
    pub title: Option<String>,
    pub git_branch: Option<String>,
    pub client_version: Option<String>,
    pub entrypoint: Option<String>,
    pub messages: Vec<MessageRecord>,
    pub usage: Vec<UsageRecord>,
    pub tool_calls: Vec<ToolCallRecord>,
    pub accepted_changes: Vec<AcceptedChangeRecord>,
    pub events: Vec<SessionEventRecord>,
    pub context_injections: Vec<ContextInjectionRecord>,
    pub rate_limits: Vec<RateLimitRecord>,
}

impl SessionRecord {
    pub fn new(provider: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            session_id: session_id.into(),
            source_path: None,
            project_path: None,
            started_at: None,
            ended_at: None,
            model: None,
            parent_session_id: None,
            agent_kind: AgentKind::Main,
            agent_type: None,
            title: None,
            git_branch: None,
            client_version: None,
            entrypoint: None,
            messages: Vec::new(),
            usage: Vec::new(),
            tool_calls: Vec::new(),
            accepted_changes: Vec::new(),
            events: Vec::new(),
            context_injections: Vec::new(),
            rate_limits: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.usage.is_empty()
            && self.accepted_changes.is_empty()
            && self.tool_calls.is_empty()
    }

    /// Model with the most output tokens, falling back to the last seen model.
    pub fn primary_model_from_usage(&self) -> Option<String> {
        let mut totals: Vec<(String, i64)> = Vec::new();
        for usage in &self.usage {
            let Some(model) = usage.model.as_deref() else {
                continue;
            };
            match totals.iter_mut().find(|(name, _)| name == model) {
                Some((_, total)) => *total += usage.output_tokens + usage.input_tokens,
                None => totals.push((model.to_string(), usage.output_tokens + usage.input_tokens)),
            }
        }
        totals
            .iter()
            .max_by_key(|(_, total)| *total)
            .map(|(name, _)| name.clone())
            .or_else(|| {
                self.usage
                    .iter()
                    .rev()
                    .find_map(|usage| usage.model.clone())
            })
    }

    /// Drop duplicate usage keys, keeping the last occurrence (streaming
    /// transcripts repeat a message id with growing output counts).
    pub fn dedupe_usage(&mut self) {
        let mut seen = std::collections::HashSet::new();
        let mut kept = Vec::with_capacity(self.usage.len());
        for usage in self.usage.drain(..).rev() {
            if seen.insert(usage.key.clone()) {
                kept.push(usage);
            }
        }
        kept.reverse();
        self.usage = kept;
    }
}

pub(crate) const SESSION_STORE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS session_source_state (
    provider       TEXT NOT NULL,
    source_key     TEXT NOT NULL,
    fingerprint    TEXT NOT NULL,
    ingest_version INTEGER NOT NULL,
    session_ids    TEXT NOT NULL DEFAULT '',
    updated_at     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(provider, source_key)
);

CREATE TABLE IF NOT EXISTS fact_session_tool_call (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    provider          TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    call_key          TEXT NOT NULL,
    call_index        INTEGER NOT NULL,
    ts                TEXT,
    ended_ts          TEXT,
    tool_name         TEXT NOT NULL,
    tool_category     TEXT NOT NULL,
    validation_kind   TEXT,
    target            TEXT,
    input_chars       INTEGER NOT NULL DEFAULT 0,
    output_chars      INTEGER NOT NULL DEFAULT 0,
    output_in_context INTEGER NOT NULL DEFAULT 1,
    status            TEXT NOT NULL DEFAULT 'unknown',
    exit_code         INTEGER,
    error_signature   TEXT,
    child_session_id  TEXT,
    agent_type        TEXT,
    UNIQUE(provider, session_id, call_key)
);

CREATE TABLE IF NOT EXISTS fact_session_event (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    ts          TEXT,
    kind        TEXT NOT NULL,
    detail      TEXT,
    UNIQUE(provider, session_id, event_index)
);

CREATE TABLE IF NOT EXISTS fact_session_context_injection (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    item_key    TEXT NOT NULL,
    ts          TEXT,
    kind        TEXT NOT NULL,
    source      TEXT,
    chars       INTEGER NOT NULL DEFAULT 0,
    UNIQUE(provider, session_id, item_key)
);

CREATE TABLE IF NOT EXISTS fact_rate_limit_snapshot (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    ts              TEXT NOT NULL,
    limit_id        TEXT NOT NULL,
    window_kind     TEXT NOT NULL,
    window_minutes  INTEGER,
    used_percent    REAL NOT NULL,
    resets_at       TEXT,
    plan_type       TEXT,
    credits_balance REAL,
    source          TEXT NOT NULL DEFAULT 'provider_log',
    UNIQUE(provider, session_id, limit_id, window_kind, ts)
);

CREATE TABLE IF NOT EXISTS fact_quota_observation (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    kind        TEXT NOT NULL,
    plan_label  TEXT,
    note        TEXT,
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fact_session_tool_call_session
    ON fact_session_tool_call(provider, session_id, call_index);
CREATE INDEX IF NOT EXISTS idx_fact_session_event_session
    ON fact_session_event(provider, session_id, event_index);
CREATE INDEX IF NOT EXISTS idx_fact_rate_limit_snapshot_ts
    ON fact_rate_limit_snapshot(provider, limit_id, window_kind, ts);
CREATE INDEX IF NOT EXISTS idx_fact_session_usage_ts
    ON fact_session_usage(usage_ts);
CREATE INDEX IF NOT EXISTS idx_metadata_sessions_parent
    ON metadata_sessions(provider, parent_session_id);
CREATE VIEW IF NOT EXISTS view_session_tree AS
    SELECT provider,
           session_id,
           COALESCE(NULLIF(root_session_id, ''), session_id) AS root_session_id,
           parent_session_id,
           COALESCE(agent_kind, 'main') AS agent_kind
    FROM metadata_sessions;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_session_usage_key
    ON fact_session_usage(provider, session_id, usage_key) WHERE usage_key IS NOT NULL;
";

pub(crate) const SESSION_STORE_COLUMNS: &[&str] = &[
    "ALTER TABLE metadata_sessions ADD COLUMN parent_session_id TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN root_session_id TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN agent_kind TEXT NOT NULL DEFAULT 'main';",
    "ALTER TABLE metadata_sessions ADD COLUMN agent_type TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN title TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN git_branch TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN client_version TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN entrypoint TEXT;",
    "ALTER TABLE metadata_sessions ADD COLUMN source_status TEXT NOT NULL DEFAULT 'ok';",
    "ALTER TABLE metadata_sessions ADD COLUMN ingest_version INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE fact_session_usage ADD COLUMN usage_key TEXT;",
    "ALTER TABLE fact_session_usage ADD COLUMN token_source TEXT NOT NULL DEFAULT 'legacy';",
    "ALTER TABLE fact_session_usage ADD COLUMN reasoning_in_output INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE fact_session_usage ADD COLUMN service_tier TEXT;",
    "ALTER TABLE fact_session_usage ADD COLUMN provider_route TEXT;",
    "ALTER TABLE fact_session_usage ADD COLUMN stop_reason TEXT;",
    "ALTER TABLE fact_session_usage ADD COLUMN context_window INTEGER;",
    "ALTER TABLE fact_session_usage ADD COLUMN ts_quality TEXT NOT NULL DEFAULT 'session';",
    "ALTER TABLE fact_session_usage ADD COLUMN cache_creation_1h_tokens INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE fact_session_message ADD COLUMN message_key TEXT;",
    "ALTER TABLE fact_session_message ADD COLUMN is_compaction_summary INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE fact_session_message ADD COLUMN chars INTEGER;",
];

/// One-time data migrations, recorded in `schema_migrations`.
pub(crate) fn apply_data_migrations(conn: &Connection) -> Result<()> {
    let applied: Option<i64> = conn
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = 2",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if applied.is_none() {
        // Codex reports reasoning inside output_tokens; earlier versions billed it twice.
        conn.execute_batch(
            "UPDATE fact_session_usage SET reasoning_in_output = 1 WHERE provider = 'codex';
             INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);",
        )?;
    }
    Ok(())
}

/// Returns true when the source must be (re)parsed.
pub fn source_needs_ingest(
    conn: &Connection,
    provider: &str,
    source_key: &str,
    fingerprint: &str,
) -> Result<bool> {
    let state: Option<(String, i64)> = conn
        .query_row(
            "SELECT fingerprint, ingest_version FROM session_source_state
             WHERE provider = ?1 AND source_key = ?2",
            params![provider, source_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(match state {
        Some((stored, version)) => stored != fingerprint || version != SESSION_INGEST_VERSION,
        None => true,
    })
}

pub fn mark_source_ingested(
    conn: &Connection,
    provider: &str,
    source_key: &str,
    fingerprint: &str,
    session_ids: &[&str],
) -> Result<()> {
    conn.execute(
        "INSERT INTO session_source_state(provider, source_key, fingerprint, ingest_version, session_ids)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(provider, source_key) DO UPDATE SET
            fingerprint = excluded.fingerprint,
            ingest_version = excluded.ingest_version,
            session_ids = excluded.session_ids,
            updated_at = datetime('now')",
        params![
            provider,
            source_key,
            fingerprint,
            SESSION_INGEST_VERSION,
            session_ids.join("\n")
        ],
    )?;
    Ok(())
}

/// File fingerprint: mtime (ns) + size.
pub fn file_fingerprint(path: &std::path::Path) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Some(format!("{mtime}:{}", meta.len()))
}

/// Replace all session-layer facts for `record` with its contents.
///
/// The caller decides the transaction scope; nested savepoints keep this safe
/// to call inside a provider-level transaction.
pub fn store_session_record(conn: &Connection, record: &SessionRecord) -> Result<usize> {
    conn.execute_batch("SAVEPOINT store_session_record")?;
    let result = store_session_record_inner(conn, record);
    match result {
        Ok(written) => {
            conn.execute_batch("RELEASE store_session_record")?;
            Ok(written)
        }
        Err(error) => {
            conn.execute_batch("ROLLBACK TO store_session_record; RELEASE store_session_record")?;
            Err(error)
        }
    }
}

fn store_session_record_inner(conn: &Connection, record: &SessionRecord) -> Result<usize> {
    let provider = record.provider.as_str();
    let session_id = record.session_id.as_str();
    let primary_model = record
        .model
        .clone()
        .or_else(|| record.primary_model_from_usage());

    super::upsert_metadata_session_with_model(
        conn,
        provider,
        session_id,
        record.project_path.as_deref(),
        record.started_at.as_deref(),
        record.ended_at.as_deref().or(record.started_at.as_deref()),
        record.source_path.as_deref(),
        None,
        primary_model.as_deref(),
    )?;
    if let Some(model) = primary_model.as_deref() {
        // Re-parsed sessions take the freshly computed primary model.
        let model_id = super::upsert_metadata_model(
            conn,
            provider,
            &super::normalize_provider_model_name(provider, model),
        )?;
        conn.execute(
            "UPDATE metadata_sessions SET model_id = ?3 WHERE provider = ?1 AND session_id = ?2",
            params![provider, session_id, model_id],
        )?;
    }
    conn.execute(
        "UPDATE metadata_sessions SET
            parent_session_id = ?3,
            agent_kind = ?4,
            agent_type = COALESCE(?5, agent_type),
            title = COALESCE(?6, title),
            git_branch = COALESCE(?7, git_branch),
            client_version = COALESCE(?8, client_version),
            entrypoint = COALESCE(?9, entrypoint),
            root_session_id = CASE
                WHEN ?3 IS NULL THEN session_id
                ELSE COALESCE(
                    (SELECT COALESCE(NULLIF(p.root_session_id, ''), p.session_id)
                     FROM metadata_sessions p
                     WHERE p.provider = ?1 AND p.session_id = ?3),
                    ?3)
            END,
            source_status = 'ok',
            ingest_version = ?10,
            updated_at = datetime('now')
         WHERE provider = ?1 AND session_id = ?2",
        params![
            provider,
            session_id,
            record.parent_session_id,
            record.agent_kind.as_str(),
            record.agent_type,
            record.title,
            record.git_branch,
            record.client_version,
            record.entrypoint,
            SESSION_INGEST_VERSION
        ],
    )?;

    for table in [
        "fact_session_message",
        "fact_session_usage",
        "fact_session_tool_call",
        "fact_session_event",
        "fact_session_context_injection",
        "fact_rate_limit_snapshot",
    ] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE provider = ?1 AND session_id = ?2"),
            params![provider, session_id],
        )?;
    }
    conn.execute(
        "DELETE FROM fact_session_code_change
         WHERE provider = ?1 AND session_id = ?2 AND source_kind = 'accepted_change'",
        params![provider, session_id],
    )?;

    let mut written = 1usize;

    let mut message_stmt = conn.prepare_cached(
        "INSERT INTO fact_session_message (
            provider, session_id, message_index, message_ts, role, content, content_words,
            message_key, is_compaction_summary, chars
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    let mut message_index = 0i64;
    for message in &record.messages {
        let words = message.text.split_whitespace().count() as i64;
        if words == 0 {
            continue;
        }
        message_stmt.execute(params![
            provider,
            session_id,
            message_index,
            message.ts,
            message.role,
            message.text,
            words,
            message.key,
            message.is_compaction_summary as i64,
            message.text.chars().count() as i64
        ])?;
        message_index += 1;
        written += 1;
    }

    let mut usage_stmt = conn.prepare_cached(
        "INSERT INTO fact_session_usage (
            provider, session_id, usage_index, usage_ts, model_name, input_tokens,
            cached_input_tokens, cache_creation_tokens, output_tokens, reasoning_tokens,
            total_tokens, actual_cost_usd, cost_source, usage_key, token_source,
            reasoning_in_output, service_tier, provider_route, stop_reason, context_window,
            ts_quality, cache_creation_1h_tokens
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                   ?18, ?19, ?20, ?21, ?22)",
    )?;
    for (usage_index, usage) in record.usage.iter().enumerate() {
        let cost_source = if usage.reported_cost_usd.is_some_and(|cost| cost > 0.0) {
            "actual"
        } else if usage.token_source == TokenSource::EstimatedText {
            "estimated_text_tokens"
        } else {
            "estimated_from_tokens"
        };
        usage_stmt.execute(params![
            provider,
            session_id,
            usage_index as i64,
            usage.ts.as_deref().or(record.ended_at.as_deref()),
            usage.model.as_deref().or(primary_model.as_deref()),
            usage.input_tokens.max(0),
            usage.cache_read_tokens.max(0),
            usage.cache_write_tokens.max(0),
            usage.output_tokens.max(0),
            usage.reasoning_tokens.max(0),
            usage.total_tokens().max(0),
            usage.reported_cost_usd,
            cost_source,
            usage.key,
            usage.token_source.as_str(),
            usage.reasoning_in_output as i64,
            usage.service_tier,
            usage.provider_route,
            usage.stop_reason,
            usage.context_window,
            if usage.ts.is_some() {
                "event"
            } else {
                "session"
            },
            usage.cache_write_1h_tokens.max(0)
        ])?;
        written += 1;
    }

    let mut tool_stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO fact_session_tool_call (
            provider, session_id, call_key, call_index, ts, ended_ts, tool_name, tool_category,
            validation_kind, target, input_chars, output_chars, output_in_context, status,
            exit_code, error_signature, child_session_id, agent_type
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
    )?;
    for (call_index, call) in record.tool_calls.iter().enumerate() {
        let category =
            crate::insights::tools::tool_category(&call.tool_name, call.target.as_deref());
        let validation_kind = if category == "exec" || category == "validate" {
            call.target
                .as_deref()
                .and_then(crate::insights::tools::classify_command)
        } else {
            None
        };
        let category = if validation_kind.is_some() {
            "validate"
        } else {
            category
        };
        let signature = call
            .error_head
            .as_deref()
            .and_then(crate::insights::tools::error_signature);
        tool_stmt.execute(params![
            provider,
            session_id,
            call.key,
            call_index as i64,
            call.ts,
            call.ended_ts,
            call.tool_name,
            category,
            validation_kind,
            call.target
                .as_deref()
                .map(|value| truncate_chars(value, 400)),
            call.input_chars,
            call.output_chars,
            call.output_in_context as i64,
            call.status,
            call.exit_code,
            signature,
            call.child_session_id,
            call.agent_type
        ])?;
        written += 1;
    }

    let mut event_stmt = conn.prepare_cached(
        "INSERT INTO fact_session_event (provider, session_id, event_index, ts, kind, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for (event_index, event) in record.events.iter().enumerate() {
        event_stmt.execute(params![
            provider,
            session_id,
            event_index as i64,
            event.ts,
            event.kind,
            event.detail
        ])?;
    }

    let mut injection_stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO fact_session_context_injection (
            provider, session_id, item_key, ts, kind, source, chars
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    for item in &record.context_injections {
        injection_stmt.execute(params![
            provider,
            session_id,
            item.key,
            item.ts,
            item.kind,
            item.source,
            item.chars
        ])?;
    }

    let mut rate_stmt = conn.prepare_cached(
        "INSERT OR IGNORE INTO fact_rate_limit_snapshot (
            provider, session_id, ts, limit_id, window_kind, window_minutes, used_percent,
            resets_at, plan_type, credits_balance
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    for snapshot in &record.rate_limits {
        rate_stmt.execute(params![
            provider,
            session_id,
            snapshot.ts,
            snapshot.limit_id,
            snapshot.window_kind,
            snapshot.window_minutes,
            snapshot.used_percent,
            snapshot.resets_at,
            snapshot.plan_type,
            snapshot.credits_balance
        ])?;
    }

    for change in &record.accepted_changes {
        super::ingest_accepted_code_change(
            conn,
            provider,
            session_id,
            &change.abs_path,
            change.lines_added,
            change.lines_removed,
            change.ts.as_deref(),
        )?;
        written += 1;
    }

    Ok(written)
}

pub(crate) fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    value.chars().take(max).collect()
}

/// Resolve `root_session_id` through parent chains (parents may be ingested
/// after their children). Sessions whose parent is unknown stay their own root
/// only when they have no parent at all.
pub fn refresh_session_roots(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE metadata_sessions SET root_session_id = session_id
         WHERE parent_session_id IS NULL OR parent_session_id = ''",
        [],
    )?;
    for _ in 0..8 {
        let changed = conn.execute(
            "UPDATE metadata_sessions AS child SET root_session_id = (
                 SELECT COALESCE(NULLIF(parent.root_session_id, ''), parent.session_id)
                 FROM metadata_sessions parent
                 WHERE parent.provider = child.provider
                   AND parent.session_id = child.parent_session_id
             )
             WHERE child.parent_session_id IS NOT NULL AND child.parent_session_id != ''
               AND EXISTS (
                 SELECT 1 FROM metadata_sessions parent
                 WHERE parent.provider = child.provider
                   AND parent.session_id = child.parent_session_id
                   AND COALESCE(NULLIF(parent.root_session_id, ''), parent.session_id)
                       IS NOT COALESCE(child.root_session_id, '')
               )",
            [],
        )?;
        if changed == 0 {
            break;
        }
    }
    // Children whose parent was never ingested are reported as their own roots.
    conn.execute(
        "UPDATE metadata_sessions AS child
         SET root_session_id = session_id
         WHERE parent_session_id IS NOT NULL AND parent_session_id != ''
           AND NOT EXISTS (
             SELECT 1 FROM metadata_sessions parent
             WHERE parent.provider = child.provider
               AND parent.session_id = child.parent_session_id
           )",
        [],
    )?;
    Ok(())
}

/// Mark sessions whose source file no longer exists; their facts are kept.
pub fn mark_missing_sources(conn: &Connection, provider: &str) -> Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT session_id, source_path FROM metadata_sessions
         WHERE provider = ?1 AND source_path IS NOT NULL AND source_status = 'ok'",
    )?;
    let rows = stmt
        .query_map(params![provider], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut marked = 0;
    for (session_id, source_path) in rows {
        if source_path.contains("://") || source_path.contains(".vscdb") {
            continue;
        }
        if !std::path::Path::new(&source_path).exists() {
            conn.execute(
                "UPDATE metadata_sessions SET source_status = 'source_missing'
                 WHERE provider = ?1 AND session_id = ?2",
                params![provider, session_id],
            )?;
            marked += 1;
        }
    }
    Ok(marked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;

    fn usage(key: &str, output: i64) -> UsageRecord {
        UsageRecord {
            output_tokens: output,
            input_tokens: 10,
            ..UsageRecord::new(key)
        }
    }

    #[test]
    fn dedupe_usage_keeps_last_occurrence() {
        let mut record = SessionRecord::new("claude", "s1");
        record.usage = vec![usage("m1", 1), usage("m1", 5), usage("m2", 3)];
        record.dedupe_usage();
        assert_eq!(record.usage.len(), 2);
        assert_eq!(record.usage[0].key, "m1");
        assert_eq!(record.usage[0].output_tokens, 5);
        assert_eq!(record.usage[1].key, "m2");
    }

    #[test]
    fn reasoning_in_output_is_not_added_to_total() {
        let mut codex = UsageRecord::new("t1");
        codex.input_tokens = 100;
        codex.output_tokens = 50;
        codex.reasoning_tokens = 40;
        codex.reasoning_in_output = true;
        assert_eq!(codex.total_tokens(), 150);
        codex.reasoning_in_output = false;
        assert_eq!(codex.total_tokens(), 190);
    }

    #[test]
    fn storing_a_record_twice_replaces_facts() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let mut record = SessionRecord::new("claude", "s1");
        record.started_at = Some("2026-01-01T00:00:00Z".into());
        record.usage = vec![usage("m1", 5)];
        record.messages.push(MessageRecord {
            key: "u1".into(),
            ts: Some("2026-01-01T00:00:00Z".into()),
            role: "user".into(),
            text: "hello world".into(),
            is_compaction_summary: false,
        });
        store_session_record(&conn, &record)?;
        record.usage.push(usage("m2", 7));
        store_session_record(&conn, &record)?;

        let usage_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM fact_session_usage WHERE session_id = 's1'",
            [],
            |row| row.get(0),
        )?;
        let message_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM fact_session_message WHERE session_id = 's1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(usage_rows, 2);
        assert_eq!(message_rows, 1);
        Ok(())
    }

    #[test]
    fn source_state_tracks_fingerprint_and_version() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        assert!(source_needs_ingest(&conn, "claude", "/a.jsonl", "1:2")?);
        mark_source_ingested(&conn, "claude", "/a.jsonl", "1:2", &["s1"])?;
        assert!(!source_needs_ingest(&conn, "claude", "/a.jsonl", "1:2")?);
        assert!(source_needs_ingest(&conn, "claude", "/a.jsonl", "1:3")?);
        Ok(())
    }
}
