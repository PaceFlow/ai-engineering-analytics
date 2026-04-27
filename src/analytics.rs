use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use regex::Regex;
use rusqlite::{Connection, Row, TransactionBehavior, params, types::ValueRef};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Instant;

use crate::change_intel::commit_assoc::git_scan::load_commit_diff;
use crate::change_intel::types::LineSide;
use crate::cli::{EventCategory, EventStreamArgs, EventStreamKind, GroupBy, ReportArgs};
use crate::ingest_progress::IngestProgressObserver;
use crate::sync_identity;

const C2_STRICT_WEAK_RATIO: f64 = 0.20;
const C2_MIN_RATIO: f64 = 0.80;
const C2_MIN_MATCHED_LINES: i64 = 30;
const C2_WINNER_MARGIN: f64 = 0.20;
const CHURN_WINDOW_DAYS: i64 = 14;
const BUG_AFTER_MERGE_WINDOW_DAYS: i64 = 60;
const REPORTING_VIEWS_SQL: &str = r#"
DROP VIEW IF EXISTS view_session_metrics_base;
DROP VIEW IF EXISTS view_task_session_metrics_base;
DROP VIEW IF EXISTS view_branch_session_metrics_base;
DROP VIEW IF EXISTS view_change_metrics_base;
DROP VIEW IF EXISTS view_commit_session_metrics_base;
DROP VIEW IF EXISTS view_task_commit_metrics_base;
DROP VIEW IF EXISTS view_branch_commit_metrics_base;
DROP VIEW IF EXISTS view_session_productivity;

CREATE VIEW view_session_metrics_base AS
SELECT
    sq.provider,
    sq.session_id,
    COALESCE(sq.repo_root, ep.repo_root, '(unknown)') AS repo_root,
    CASE
        WHEN COALESCE(NULLIF(TRIM(sq.model_name), ''), NULLIF(TRIM(ep.model_name), '')) IS NULL
            THEN sq.provider || '/(unknown)'
        WHEN COALESCE(NULLIF(TRIM(sq.model_name), ''), NULLIF(TRIM(ep.model_name), '')) LIKE sq.provider || '/%'
            THEN COALESCE(NULLIF(TRIM(sq.model_name), ''), NULLIF(TRIM(ep.model_name), ''))
        ELSE sq.provider || '/' || COALESCE(NULLIF(TRIM(sq.model_name), ''), NULLIF(TRIM(ep.model_name), ''))
    END AS model_name,
    COALESCE(sq.started_at, ep.started_at) AS started_at,
    date(
        COALESCE(sq.started_at, ep.started_at),
        '-' || ((CAST(strftime('%w', COALESCE(sq.started_at, ep.started_at)) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    sq.user_turn_count,
    sq.debug_loop_flag,
    sq.mid_session_error_paste_flag,
    COALESCE(sq.accepted_output_flag, 0) AS accepted_output_flag,
    sq.first_accepted_change_at,
    sq.minutes_to_first_accepted_change,
    COALESCE(sq.session_commit_within_4h_flag, 0) AS session_commit_within_4h_flag
FROM event_session_quality sq
LEFT JOIN event_session_productivity ep
  ON ep.provider = sq.provider
 AND ep.session_id = sq.session_id;

CREATE VIEW view_task_session_metrics_base AS
SELECT
    ts.repo_root,
    ts.task_key,
    ts.branch_name,
    ts.provider,
    ts.session_id,
    CASE
        WHEN NULLIF(TRIM(ts.model_name), '') IS NULL THEN ts.provider || '/(unknown)'
        WHEN ts.model_name LIKE ts.provider || '/%' THEN ts.model_name
        ELSE ts.provider || '/' || ts.model_name
    END AS model_name,
    ts.started_at AS started_at,
    date(
        ts.started_at,
        '-' || ((CAST(strftime('%w', ts.started_at) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    ts.attribution_weight,
    ts.user_turn_count,
    ts.debug_loop_flag,
    ts.mid_session_error_paste_flag,
    COALESCE(ts.accepted_output_flag, 0) AS accepted_output_flag,
    ts.first_accepted_change_at,
    ts.minutes_to_first_accepted_change,
    ts.commit_within_window_flag
FROM event_task_session ts;

CREATE VIEW view_branch_session_metrics_base AS
SELECT
    tc.repo_root,
    tc.branch_name,
    cs.provider,
    cs.session_id,
    CASE
        WHEN NULLIF(TRIM(MAX(cs.model_name)), '') IS NULL THEN cs.provider || '/(unknown)'
        WHEN MAX(cs.model_name) LIKE cs.provider || '/%' THEN MAX(cs.model_name)
        ELSE cs.provider || '/' || MAX(cs.model_name)
    END AS model_name,
    MAX(sq.started_at) AS started_at,
    date(
        MAX(sq.started_at),
        '-' || ((CAST(strftime('%w', MAX(sq.started_at)) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    SUM(cs.matched_lines) AS attribution_weight,
    MAX(sq.user_turn_count) AS user_turn_count,
    MAX(sq.debug_loop_flag) AS debug_loop_flag,
    MAX(sq.mid_session_error_paste_flag) AS mid_session_error_paste_flag,
    COALESCE(MAX(sq.accepted_output_flag), 0) AS accepted_output_flag,
    MAX(sq.first_accepted_change_at) AS first_accepted_change_at,
    MAX(sq.minutes_to_first_accepted_change) AS minutes_to_first_accepted_change,
    MAX(
        CASE
            WHEN sq.started_at IS NOT NULL
             AND cs.commit_time IS NOT NULL
             AND julianday(cs.commit_time) >= julianday(sq.started_at)
             AND julianday(cs.commit_time) <= julianday(COALESCE(sq.ended_at, sq.started_at), '+4 hours')
            THEN 1 ELSE 0
        END
    ) AS commit_within_window_flag
FROM event_task_commit tc
JOIN event_commit_session cs
  ON cs.repo_root = tc.repo_root
 AND cs.commit_sha = tc.commit_sha
LEFT JOIN event_session_quality sq
  ON sq.provider = cs.provider
 AND sq.session_id = cs.session_id
GROUP BY tc.repo_root, tc.branch_name, cs.provider, cs.session_id;

CREATE VIEW view_change_metrics_base AS
SELECT
    o.repo_root,
    o.commit_sha,
    o.commit_time,
    date(
        o.commit_time,
        '-' || ((CAST(strftime('%w', o.commit_time) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    o.heavy_ai_flag,
    o.merged_to_mainline_flag,
    o.reverted_later_flag,
    o.commit_total_changed_lines,
    c.ai_added_lines_reaching_mainline,
    c.ai_added_lines_removed_within_window,
    COALESCE(b.bug_after_merge_flag, 0) AS bug_after_merge_flag,
    COALESCE(b.bug_signal_count, 0) AS bug_signal_count,
    b.first_bug_signal_commit_sha,
    b.first_bug_signal_commit_time
FROM event_commit_outcome o
JOIN event_commit_churn c
  ON c.repo_root = o.repo_root
 AND c.commit_sha = o.commit_sha
LEFT JOIN event_commit_bug_signal b
  ON b.repo_root = o.repo_root
 AND b.commit_sha = o.commit_sha;

CREATE VIEW view_commit_session_metrics_base AS
SELECT
    cs.repo_root,
    cs.commit_sha,
    cs.provider,
    cs.session_id,
    CASE
        WHEN NULLIF(TRIM(cs.model_name), '') IS NULL THEN cs.provider || '/(unknown)'
        WHEN cs.model_name LIKE cs.provider || '/%' THEN cs.model_name
        ELSE cs.provider || '/' || cs.model_name
    END AS model_name,
    cs.commit_time,
    date(
        cs.commit_time,
        '-' || ((CAST(strftime('%w', cs.commit_time) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    cs.matched_lines,
    cs.share_of_commit,
    cs.share_of_ai,
    o.heavy_ai_flag,
    o.merged_to_mainline_flag,
    o.reverted_later_flag,
    o.commit_total_changed_lines,
    c.ai_added_lines_reaching_mainline,
    c.ai_added_lines_removed_within_window,
    COALESCE(b.bug_after_merge_flag, 0) AS bug_after_merge_flag,
    COALESCE(b.bug_signal_count, 0) AS bug_signal_count,
    b.first_bug_signal_commit_sha,
    b.first_bug_signal_commit_time
FROM event_commit_session cs
JOIN event_commit_outcome o
  ON o.repo_root = cs.repo_root
 AND o.commit_sha = cs.commit_sha
JOIN event_commit_churn c
  ON c.repo_root = cs.repo_root
 AND c.commit_sha = cs.commit_sha
LEFT JOIN event_commit_bug_signal b
  ON b.repo_root = cs.repo_root
 AND b.commit_sha = cs.commit_sha;

CREATE VIEW view_task_commit_metrics_base AS
SELECT
    tc.repo_root,
    tc.task_key,
    tc.branch_name,
    tc.commit_sha,
    tc.fallback_flag,
    tc.confidence,
    tc.commit_time,
    date(
        tc.commit_time,
        '-' || ((CAST(strftime('%w', tc.commit_time) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    o.heavy_ai_flag,
    o.merged_to_mainline_flag,
    o.reverted_later_flag,
    o.commit_total_changed_lines,
    c.ai_added_lines_reaching_mainline,
    c.ai_added_lines_removed_within_window,
    COALESCE(b.bug_after_merge_flag, 0) AS bug_after_merge_flag,
    COALESCE(b.bug_signal_count, 0) AS bug_signal_count,
    b.first_bug_signal_commit_sha,
    b.first_bug_signal_commit_time
FROM event_task_commit tc
LEFT JOIN event_commit_outcome o
  ON o.repo_root = tc.repo_root
 AND o.commit_sha = tc.commit_sha
LEFT JOIN event_commit_churn c
  ON c.repo_root = tc.repo_root
 AND c.commit_sha = tc.commit_sha
LEFT JOIN event_commit_bug_signal b
  ON b.repo_root = tc.repo_root
 AND b.commit_sha = tc.commit_sha;

CREATE VIEW view_branch_commit_metrics_base AS
SELECT
    tc.repo_root,
    tc.branch_name,
    tc.commit_sha,
    tc.fallback_flag,
    tc.confidence,
    tc.commit_time,
    date(
        tc.commit_time,
        '-' || ((CAST(strftime('%w', tc.commit_time) AS INTEGER) + 6) % 7) || ' days'
    ) AS week_start,
    o.heavy_ai_flag,
    o.merged_to_mainline_flag,
    o.reverted_later_flag,
    o.commit_total_changed_lines,
    c.ai_added_lines_reaching_mainline,
    c.ai_added_lines_removed_within_window,
    COALESCE(b.bug_after_merge_flag, 0) AS bug_after_merge_flag,
    COALESCE(b.bug_signal_count, 0) AS bug_signal_count,
    b.first_bug_signal_commit_sha,
    b.first_bug_signal_commit_time
FROM event_task_commit tc
LEFT JOIN event_commit_outcome o
  ON o.repo_root = tc.repo_root
 AND o.commit_sha = tc.commit_sha
LEFT JOIN event_commit_churn c
  ON c.repo_root = tc.repo_root
 AND c.commit_sha = tc.commit_sha
LEFT JOIN event_commit_bug_signal b
  ON b.repo_root = tc.repo_root
 AND b.commit_sha = tc.commit_sha;

CREATE VIEW view_session_productivity AS
SELECT
    ep.provider,
    CASE
        WHEN NULLIF(TRIM(ep.model_name), '') IS NULL THEN ep.provider || '/(unknown)'
        WHEN ep.model_name LIKE ep.provider || '/%' THEN ep.model_name
        ELSE ep.provider || '/' || ep.model_name
    END AS model_name,
    ep.session_id,
    COALESCE(ep.repo_root, '(unknown)') AS repo_root,
    COALESCE(NULLIF(TRIM(ep.project_path), ''), '(unknown)') AS project_path,
    COALESCE(ep.ended_at, ep.started_at) AS last_active,
    ep.user_word_count,
    ep.accepted_total_changed_lines AS total_loc,
    ep.accepted_lines_added AS total_added,
    ep.accepted_lines_removed AS total_removed
FROM event_session_productivity ep
ORDER BY last_active DESC;
"#;

#[derive(Debug, Clone)]
pub struct RatioMetric {
    pub numerator: i64,
    pub denominator: i64,
}

impl RatioMetric {
    pub fn percent(&self) -> Option<f64> {
        if self.denominator > 0 {
            Some(self.numerator as f64 / self.denominator as f64 * 100.0)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReportQueryOptions {
    pub implicit_model_default: bool,
}

#[derive(Debug, Clone)]
pub struct SessionListRow {
    pub provider: String,
    pub model: String,
    pub session_id: String,
    pub project_path: String,
    pub last_active: Option<String>,
    pub total_words: i64,
    pub total_loc: i64,
    pub total_added: i64,
    pub total_removed: i64,
}

#[derive(Debug, Clone)]
pub struct SessionReportRow {
    pub week_start: Option<String>,
    pub group_value: Option<String>,
    pub branch_name: Option<String>,
    pub session_count: i64,
    pub s2_avg: Option<f64>,
    pub avg_minutes_to_first_accepted_change: Option<f64>,
    pub debug_loop_rate: RatioMetric,
    pub s6_rate: RatioMetric,
    pub s9_rate: RatioMetric,
    pub no_output_session_rate: RatioMetric,
}

#[derive(Debug, Clone)]
pub struct ChangeReportRow {
    pub week_start: Option<String>,
    pub group_value: Option<String>,
    pub branch_name: Option<String>,
    pub repo_root: Option<String>,
    pub commit_count: i64,
    pub heavy_commit_count: i64,
    /// Heavy commits on `git:github.com/...` remotes (PR metrics denominator for coverage).
    pub github_pr_heavy_eligible: i64,
    /// Subset of [`Self::github_pr_heavy_eligible`] with finished PR lookup (`resolved` / `no_pr`).
    pub github_pr_heavy_ready: i64,
    pub pr_reach_rate: RatioMetric,
    pub merge_rate: RatioMetric,
    pub pr_merge_rate: RatioMetric,
    pub github_pr_metrics_available: bool,
    /// Sum of `fact_commit.total_added` / `total_removed` for commits in this row (task-grouped delivery).
    pub task_branch_lines_added: i64,
    pub task_branch_lines_removed: i64,
}

#[derive(Debug, Clone)]
pub struct LifecycleReportRow {
    pub week_start: Option<String>,
    pub group_value: Option<String>,
    pub branch_name: Option<String>,
    pub heavy_commit_count: i64,
    pub code_churn_rate: RatioMetric,
    pub bug_after_merge_rate: RatioMetric,
    pub revert_rate: RatioMetric,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventStreamRow {
    pub stream_type: String,
    pub category: String,
    pub event_time: Option<String>,
    pub repo_root: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub task_key: Option<String>,
    pub branch_name: Option<String>,
    pub fields: JsonValue,
    #[serde(skip)]
    sort_identity: Vec<String>,
}

#[derive(Debug, Clone)]
struct SessionMessage {
    role: String,
    content: String,
}

#[derive(Debug, Clone)]
struct SessionTurn {
    user_text: String,
    assistant_text: String,
}

#[derive(Debug, Clone, Default)]
struct SessionConversationSummary {
    user_turn_count: i64,
    debug_loop_flag: i64,
    mid_session_error_paste_flag: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SessionKey {
    provider: String,
    session_id: String,
}

#[derive(Debug, Clone)]
struct CandidateCommit {
    repo_root: String,
    commit_sha: String,
    commit_time: DateTime<Utc>,
    heavy_ai: bool,
    matched_total_lines: i64,
    commit_total_lines: i64,
}

pub struct CommitRefreshSummary {
    pub repos_total: usize,
    pub repos_processed: usize,
    pub commits_total: usize,
    pub commits_processed: usize,
    pub elapsed_ms: u128,
}

type PathHashCounts = HashMap<String, HashMap<String, i64>>;
type CommitPathHashCounts = HashMap<String, PathHashCounts>;
type RemovedHashIndex = HashMap<(String, String), i64>;

#[derive(Debug, Clone)]
struct TimedLineHashChange {
    commit_time: DateTime<Utc>,
    rel_path: String,
    line_hash: String,
    count: i64,
}

#[derive(Debug, Clone)]
struct BugFixCandidate {
    commit_sha: String,
    commit_time: DateTime<Utc>,
    removed_hashes: PathHashCounts,
}

#[derive(Debug, Clone)]
struct IssueLinkedBugSignalCandidate {
    signal_ref: String,
    signal_time: DateTime<Utc>,
    window_anchor_time: DateTime<Utc>,
    removed_hashes: PathHashCounts,
}

#[derive(Debug, Clone)]
struct DerivedCommitEvent {
    reverted_later: bool,
    merged_to_mainline: bool,
    budget: PathHashCounts,
    ai_added_lines_reaching_mainline: i64,
    ai_added_lines_removed_within_window: i64,
    bug_after_merge: bool,
    first_bug_signal_commit_sha: Option<String>,
    first_bug_signal_commit_time: Option<DateTime<Utc>>,
    bug_signal_count: i64,
    bug_signal_sources: BTreeSet<String>,
}

fn record_bug_signal(
    event: &mut DerivedCommitEvent,
    signal_ref: String,
    signal_time: DateTime<Utc>,
    source: &str,
) {
    event.bug_after_merge = true;
    event.bug_signal_count += 1;
    event.bug_signal_sources.insert(source.to_string());

    let should_replace = match event.first_bug_signal_commit_time {
        None => true,
        Some(existing) => {
            signal_time < existing
                || (signal_time == existing
                    && event
                        .first_bug_signal_commit_sha
                        .as_deref()
                        .map(|existing_ref| signal_ref.as_str() < existing_ref)
                        .unwrap_or(true))
        }
    };

    if should_replace {
        event.first_bug_signal_commit_sha = Some(signal_ref);
        event.first_bug_signal_commit_time = Some(signal_time);
    }
}

fn bug_signal_source(event: &DerivedCommitEvent) -> String {
    if event.bug_signal_sources.is_empty() {
        "git_fix_commit".to_string()
    } else {
        event
            .bug_signal_sources
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("+")
    }
}

#[derive(Debug, Clone)]
struct MainlineIndex {
    by_path_hash: HashMap<(String, String), i64>,
    by_hash_paths: HashMap<String, HashMap<String, i64>>,
}

impl MainlineIndex {
    fn new() -> Self {
        Self {
            by_path_hash: HashMap::new(),
            by_hash_paths: HashMap::new(),
        }
    }

    fn add_max(&mut self, path: &str, line_hash: &str, count: i64) {
        let entry = self
            .by_path_hash
            .entry((path.to_string(), line_hash.to_string()))
            .or_insert(0);
        if count > *entry {
            *entry = count;
        }

        let by_path = self.by_hash_paths.entry(line_hash.to_string()).or_default();
        let path_entry = by_path.entry(path.to_string()).or_insert(0);
        if count > *path_entry {
            *path_entry = count;
        }
    }

    fn strict_match(&self, path: &str, line_hash: &str, budget: i64) -> i64 {
        let avail = self
            .by_path_hash
            .get(&(path.to_string(), line_hash.to_string()))
            .copied()
            .unwrap_or(0);
        budget.min(avail)
    }
}

pub fn refresh_session_events(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM event_session_quality", [])?;
    conn.execute("DELETE FROM event_session_productivity", [])?;

    let messages = load_session_messages(conn)?;
    let device_id = sync_identity::device_id();
    let mut conversation_by_session = BTreeMap::new();
    for (session, messages) in messages {
        let turns = build_session_turns(&messages);
        let user_turns = turns
            .iter()
            .filter(|turn| !turn.user_text.trim().is_empty())
            .count() as i64;
        if user_turns <= 0 {
            continue;
        }

        conversation_by_session.insert(
            session,
            SessionConversationSummary {
                user_turn_count: user_turns,
                debug_loop_flag: is_debug_loop_session(&turns) as i64,
                mid_session_error_paste_flag: has_mid_session_error_paste(&messages) as i64,
            },
        );
    }

    let mut stmt = conn.prepare(
        "SELECT
             s.provider,
             s.session_id,
             r.repo_root,
             CASE
                 WHEN mm.model_name IS NULL OR TRIM(mm.model_name) = '' THEN s.provider || '/(unknown)'
                 WHEN mm.model_name LIKE s.provider || '/%' THEN mm.model_name
                 ELSE s.provider || '/' || mm.model_name
             END AS model_name,
             COALESCE(NULLIF(TRIM(s.project_path), ''), '(unknown)') AS project_path,
             s.started_at AS session_started_at,
             s.ended_at AS session_ended_at,
             COALESCE(sig.min_signal_ts, s.started_at) AS productivity_started_at,
             CASE
                 WHEN s.provider = 'cursor' THEN COALESCE(sig.max_signal_ts, s.ended_at, s.started_at)
                 ELSE COALESCE(sig.max_signal_ts, s.started_at)
             END AS productivity_ended_at,
             COALESCE(msg.user_word_count, 0) AS user_word_count,
             COALESCE(ch.added_lines, 0) AS added_lines,
             COALESCE(ch.removed_lines, 0) AS removed_lines,
             COALESCE(out.accepted_output_flag, 0) AS accepted_output_flag,
             out.first_accepted_change_at,
             CASE
                 WHEN COALESCE(s.started_at, sig.min_signal_ts) IS NOT NULL
                  AND out.first_accepted_change_at IS NOT NULL
                 THEN (julianday(out.first_accepted_change_at) - julianday(COALESCE(s.started_at, sig.min_signal_ts))) * 24.0 * 60.0
             END AS minutes_to_first_accepted_change,
             CASE
                 WHEN EXISTS (
                     SELECT 1
                     FROM fact_commit_session_match sm
                     JOIN fact_commit c
                       ON c.repo_root = sm.repo_root
                      AND c.commit_sha = sm.commit_sha
                     WHERE sm.provider = s.provider
                       AND sm.session_id = s.session_id
                       AND COALESCE(s.started_at, sig.min_signal_ts) IS NOT NULL
                       AND julianday(c.commit_time) >= julianday(COALESCE(s.started_at, sig.min_signal_ts))
                       AND julianday(c.commit_time) <= julianday(COALESCE(
                           s.ended_at,
                           s.started_at,
                           CASE
                               WHEN s.provider = 'cursor' THEN COALESCE(sig.max_signal_ts, s.ended_at, s.started_at)
                               ELSE COALESCE(sig.max_signal_ts, s.started_at)
                           END,
                           COALESCE(sig.min_signal_ts, s.started_at)
                       ), '+4 hours')
                 )
                 THEN 1 ELSE 0
             END AS session_commit_within_4h_flag
         FROM metadata_sessions s
         LEFT JOIN metadata_repositories r ON r.id = s.repository_id
         LEFT JOIN metadata_models mm ON mm.id = s.model_id
         LEFT JOIN (
             SELECT provider, session_id, SUM(content_words) AS user_word_count
             FROM fact_session_message
             WHERE role = 'user'
             GROUP BY provider, session_id
         ) msg
           ON msg.provider = s.provider
          AND msg.session_id = s.session_id
         LEFT JOIN (
             SELECT provider, session_id,
                    SUM(lines_added) AS added_lines,
                    SUM(lines_removed) AS removed_lines
             FROM fact_session_code_change
             WHERE source_kind = 'accepted_change'
             GROUP BY provider, session_id
         ) ch
           ON ch.provider = s.provider
          AND ch.session_id = s.session_id
         LEFT JOIN (
             SELECT
                 provider,
                 session_id,
                 MIN(
                     CASE
                         WHEN change_ts IS NOT NULL
                          AND source_kind IN ('accepted_change', 'tool_write')
                          AND (lines_added > 0 OR lines_removed > 0)
                         THEN change_ts
                     END
                 ) AS first_accepted_change_at,
                 MAX(
                     CASE
                         WHEN source_kind IN ('accepted_change', 'tool_write')
                          AND (lines_added > 0 OR lines_removed > 0)
                         THEN 1 ELSE 0
                     END
                 ) AS accepted_output_flag
             FROM fact_session_code_change
             GROUP BY provider, session_id
         ) out
           ON out.provider = s.provider
          AND out.session_id = s.session_id
         LEFT JOIN (
             SELECT provider, session_id,
                    MIN(ts) AS min_signal_ts,
                    MAX(ts) AS max_signal_ts
             FROM (
                 SELECT provider, session_id, message_ts AS ts
                 FROM fact_session_message
                 WHERE message_ts IS NOT NULL
                 UNION ALL
                 SELECT provider, session_id, change_ts AS ts
                 FROM fact_session_code_change
                 WHERE source_kind = 'accepted_change' AND change_ts IS NOT NULL
             ) signal
             GROUP BY provider, session_id
         ) sig
           ON sig.provider = s.provider
          AND sig.session_id = s.session_id
         WHERE s.started_at IS NOT NULL
            OR msg.user_word_count IS NOT NULL
            OR ch.added_lines IS NOT NULL
            OR ch.removed_lines IS NOT NULL
            OR out.accepted_output_flag IS NOT NULL
         ORDER BY productivity_ended_at DESC NULLS LAST",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, i64>(12)?,
            row.get::<_, Option<String>>(13)?,
            row.get::<_, Option<f64>>(14)?,
            row.get::<_, i64>(15)?,
        ))
    })?;

    for row in rows {
        let (
            provider,
            session_id,
            repo_root,
            model_name,
            project_path,
            session_started_at,
            session_ended_at,
            productivity_started_at,
            productivity_ended_at,
            user_word_count,
            added,
            removed,
            accepted_output_flag,
            first_accepted_change_at,
            minutes_to_first_accepted_change,
            session_commit_within_4h_flag,
        ) = row?;
        let repo_key = sync_identity::repo_key_for_repo_root(repo_root.as_deref());
        let member_email = sync_identity::member_email_for_repo(repo_root.as_deref());
        if let Some(summary) = conversation_by_session.get(&SessionKey {
            provider: provider.clone(),
            session_id: session_id.clone(),
        }) {
            conn.execute(
                "INSERT INTO event_session_quality (
                    provider, session_id, repo_root, repo_key, member_email, device_id, model_name, started_at, ended_at, user_turn_count,
                    debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                    first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    provider,
                    session_id,
                    repo_root,
                    repo_key,
                    member_email,
                    device_id,
                    model_name,
                    session_started_at,
                    session_ended_at,
                    summary.user_turn_count,
                    summary.debug_loop_flag,
                    summary.mid_session_error_paste_flag,
                    accepted_output_flag,
                    first_accepted_change_at,
                    minutes_to_first_accepted_change,
                    session_commit_within_4h_flag
                ],
            )?;
        }
        conn.execute(
            "INSERT INTO event_session_productivity (
                provider, session_id, repo_root, repo_key, member_email, device_id, model_name, project_path, started_at, ended_at,
                accepted_lines_added, accepted_lines_removed, accepted_total_changed_lines, user_word_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                provider,
                session_id,
                repo_root,
                repo_key,
                member_email,
                device_id,
                model_name,
                project_path,
                productivity_started_at,
                productivity_ended_at,
                added,
                removed,
                added + removed,
                user_word_count
            ],
        )?;
    }

    Ok(())
}

pub fn refresh_commit_events(
    conn: &mut Connection,
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<CommitRefreshSummary> {
    let started = Instant::now();
    let commits = load_fact_commits(conn)?;
    let commits_total = commits.len();
    let mut by_repo: BTreeMap<String, Vec<CandidateCommit>> = BTreeMap::new();
    for commit in commits {
        by_repo
            .entry(commit.repo_root.clone())
            .or_default()
            .push(commit);
    }

    let repos_total = by_repo.len();
    if verbose {
        println!("  repos={} commits={}", repos_total, commits_total);
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute("DELETE FROM event_commit_outcome", [])?;
    tx.execute("DELETE FROM event_commit_churn", [])?;
    tx.execute("DELETE FROM event_commit_bug_signal", [])?;
    tx.execute("DELETE FROM event_commit_session", [])?;
    tx.execute("DELETE FROM event_task_commit", [])?;
    tx.execute("DELETE FROM event_task_session", [])?;

    let mut commits_processed = 0usize;
    let mut repos_processed = 0usize;
    let mut insert_outcome = tx.prepare_cached(
        "INSERT INTO event_commit_outcome (
            repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
            reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;
    let mut insert_churn = tx.prepare_cached(
        "INSERT INTO event_commit_churn (
            repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
            ai_added_lines_removed_within_window, churn_window_days
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    let mut insert_bug_signal = tx.prepare_cached(
        "INSERT INTO event_commit_bug_signal (
            repo_root, repo_key, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
            first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;

    for (repo_index, (repo_root, mut repo_commits)) in by_repo.into_iter().enumerate() {
        if !Path::new(&repo_root).join(".git").exists() {
            if verbose {
                println!(
                    "  [{}/{}] {} skipped (not a git repo)",
                    repo_index + 1,
                    repos_total,
                    shorten_repo(&repo_root)
                );
            }
            if let Some(observer) = progress.as_deref_mut() {
                observer.advance(&format!("{} cache skipped", shorten_repo(&repo_root)));
            }
            continue;
        }
        let repo_key = sync_identity::repo_key_for_repo_root(Some(&repo_root));

        repo_commits.sort_by(|a, b| {
            a.commit_time
                .cmp(&b.commit_time)
                .then_with(|| a.commit_sha.cmp(&b.commit_sha))
        });
        let repo_started = Instant::now();
        if verbose {
            println!(
                "  [{}/{}] {} commits={}",
                repo_index + 1,
                repos_total,
                shorten_repo(&repo_root),
                repo_commits.len()
            );
        }

        let derived = derive_repo_commit_events(&tx, &repo_root, &repo_commits, verbose)?;
        repos_processed += 1;
        if let Some(observer) = progress.as_deref_mut() {
            observer.advance(&format!("{} caches ready", shorten_repo(&repo_root)));
        }

        for (commit_index, commit) in repo_commits.iter().enumerate() {
            let event = derived
                .get(&commit.commit_sha)
                .ok_or_else(|| anyhow!("missing derived commit event for {}", commit.commit_sha))?;

            insert_outcome.execute(params![
                repo_root,
                repo_key,
                commit.commit_sha,
                commit
                    .commit_time
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
                commit.heavy_ai as i64,
                event.merged_to_mainline as i64,
                event.reverted_later as i64,
                commit.matched_total_lines,
                commit.commit_total_lines
            ])?;

            insert_churn.execute(params![
                repo_root,
                repo_key,
                commit.commit_sha,
                event.ai_added_lines_reaching_mainline,
                event.ai_added_lines_removed_within_window,
                CHURN_WINDOW_DAYS
            ])?;
            insert_bug_signal.execute(params![
                repo_root,
                repo_key,
                commit.commit_sha,
                event.bug_after_merge as i64,
                event.first_bug_signal_commit_sha.as_deref(),
                event
                    .first_bug_signal_commit_time
                    .as_ref()
                    .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true)),
                event.bug_signal_count,
                BUG_AFTER_MERGE_WINDOW_DAYS,
                bug_signal_source(event)
            ])?;

            commits_processed += 1;
            if let Some(observer) = progress.as_deref_mut() {
                observer.advance(commit.commit_sha.as_str());
            }
            if verbose && (commit_index + 1) % 100 == 0 {
                println!(
                    "    {} processed {}/{} commits",
                    shorten_repo(&repo_root),
                    commit_index + 1,
                    repo_commits.len()
                );
            }
        }

        if verbose {
            println!(
                "  done {} commits={} elapsed={}",
                shorten_repo(&repo_root),
                repo_commits.len(),
                format_elapsed(repo_started.elapsed().as_millis())
            );
        }
    }
    drop(insert_outcome);
    drop(insert_churn);
    drop(insert_bug_signal);

    tx.execute(
        "INSERT INTO event_commit_session (
            repo_root, repo_key, commit_sha, provider, session_id, member_email, device_id, commit_time, model_name,
            matched_lines, share_of_commit, share_of_ai
         )
         SELECT
            sm.repo_root,
            NULL,
            sm.commit_sha,
            sm.provider,
            sm.session_id,
            '(unknown)',
            '(unknown)',
            c.commit_time,
            CASE
                WHEN mm.model_name IS NULL OR TRIM(mm.model_name) = '' THEN sm.provider || '/(unknown)'
                WHEN mm.model_name LIKE sm.provider || '/%' THEN mm.model_name
                ELSE sm.provider || '/' || mm.model_name
            END AS model_name,
            sm.matched_lines,
           sm.share_of_commit,
            sm.share_of_ai
         FROM fact_commit_session_match sm
         LEFT JOIN fact_commit c
           ON c.repo_root = sm.repo_root
          AND c.commit_sha = sm.commit_sha
         LEFT JOIN metadata_sessions ms
           ON ms.provider = sm.provider
          AND ms.session_id = sm.session_id
         LEFT JOIN metadata_models mm
           ON mm.id = ms.model_id",
        [],
    )?;
    {
        let mut stmt = tx.prepare(
            "SELECT rowid, repo_root
             FROM event_commit_session",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (rowid, repo_root) = row?;
            let repo_key = sync_identity::repo_key_for_repo_root(Some(&repo_root));
            let member_email = sync_identity::member_email_for_repo(Some(&repo_root));
            let device_id = sync_identity::device_id();
            tx.execute(
                "UPDATE event_commit_session
                 SET repo_key = ?1, member_email = ?2, device_id = ?3
                 WHERE rowid = ?4",
                params![repo_key, member_email, device_id, rowid],
            )?;
        }
    }

    tx.execute(
        "INSERT INTO event_task_commit (
            repo_root, repo_key, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
         )
         SELECT
            a.repo_root,
            NULL,
            a.task_key,
            a.branch_name,
            a.commit_sha,
            a.is_fallback,
            a.confidence,
            c.commit_time
         FROM fact_task_commit_assignment a
         LEFT JOIN fact_commit c
           ON c.repo_root = a.repo_root
          AND c.commit_sha = a.commit_sha",
        [],
    )?;
    {
        let mut stmt = tx.prepare(
            "SELECT rowid, repo_root
             FROM event_task_commit",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (rowid, repo_root) = row?;
            let repo_key = sync_identity::repo_key_for_repo_root(Some(&repo_root));
            tx.execute(
                "UPDATE event_task_commit
                 SET repo_key = ?1
                 WHERE rowid = ?2",
                params![repo_key, rowid],
            )?;
        }
    }

    refresh_task_session_events(&tx)?;
    tx.commit()?;

    Ok(CommitRefreshSummary {
        repos_total,
        repos_processed,
        commits_total,
        commits_processed,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

pub fn create_reporting_views(conn: &Connection) -> Result<()> {
    conn.execute_batch(REPORTING_VIEWS_SQL)?;
    Ok(())
}

pub fn query_session_list_rows(
    conn: &Connection,
    args: &ReportArgs,
) -> Result<Vec<SessionListRow>> {
    let mut sql = String::from(
        "SELECT provider, model_name, session_id, project_path, last_active, user_word_count, total_loc, total_added, total_removed
         FROM view_session_productivity",
    );
    let conditions = session_list_conditions(args);
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY last_active DESC");
    sql.push_str(&format!(" LIMIT {}", args.limit.max(1)));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(SessionListRow {
            provider: row.get(0)?,
            model: row.get(1)?,
            session_id: row.get(2)?,
            project_path: row.get(3)?,
            last_active: row.get(4)?,
            total_words: row.get(5)?,
            total_loc: row.get(6)?,
            total_added: row.get(7)?,
            total_removed: row.get(8)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub fn query_session_report(conn: &Connection, args: &ReportArgs) -> Result<Vec<SessionReportRow>> {
    query_session_report_with_options(conn, args, ReportQueryOptions::default())
}

pub fn query_session_report_with_options(
    conn: &Connection,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Result<Vec<SessionReportRow>> {
    let use_task_base = matches!(args.group_by, Some(GroupBy::Task)) || args.task.is_some();
    let use_branch_base =
        !use_task_base && (matches!(args.group_by, Some(GroupBy::Branch)) || args.branch.is_some());
    let use_attribution_weight = use_task_base || use_branch_base;
    let source = if use_task_base {
        "view_task_session_metrics_base"
    } else if use_branch_base {
        "view_branch_session_metrics_base"
    } else {
        "view_session_metrics_base"
    };
    let timestamp_col = "started_at";
    let mut select = vec![];
    let mut group = vec![];

    if args.weekly {
        select.push("week_start".to_string());
        group.push("week_start".to_string());
    } else {
        select.push("NULL AS week_start".to_string());
    }

    let mut branch_selected = false;
    if let Some(group_expr) = session_group_expr(args.group_by) {
        select.push(format!("{group_expr} AS group_value"));
        group.push(group_expr.to_string());
        if matches!(args.group_by, Some(GroupBy::Task)) {
            select.push("branch_name".to_string());
            group.push("branch_name".to_string());
            branch_selected = true;
        } else {
            select.push("NULL AS branch_name".to_string());
        }
    } else {
        select.push("NULL AS group_value".to_string());
        select.push("NULL AS branch_name".to_string());
    }
    if !branch_selected && !select.iter().any(|item| item.contains("branch_name")) {
        select.push("NULL AS branch_name".to_string());
    }

    if use_attribution_weight {
        let weight = "CASE WHEN attribution_weight > 0 THEN attribution_weight ELSE 1 END";
        select.push("COUNT(*) AS session_count".to_string());
        select.push(format!(
            "SUM(CASE WHEN user_turn_count IS NOT NULL THEN {weight} * user_turn_count ELSE 0 END) /
             NULLIF(SUM(CASE WHEN user_turn_count IS NOT NULL THEN {weight} ELSE 0 END), 0) AS s2_avg"
        ));
        select.push(format!(
            "SUM(CASE WHEN minutes_to_first_accepted_change IS NOT NULL THEN {weight} * minutes_to_first_accepted_change ELSE 0 END) /
             NULLIF(SUM(CASE WHEN minutes_to_first_accepted_change IS NOT NULL THEN {weight} ELSE 0 END), 0) AS time_to_first_accept_avg"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN debug_loop_flag = 1 THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s4_n"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN debug_loop_flag IS NOT NULL THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s4_d"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN mid_session_error_paste_flag = 1 THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s6_n"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN mid_session_error_paste_flag IS NOT NULL THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s6_d"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN commit_within_window_flag = 1 THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s9_n"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN commit_within_window_flag IS NOT NULL THEN {weight} ELSE 0 END), 0) AS INTEGER) AS s9_d"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN user_turn_count IS NOT NULL AND accepted_output_flag = 0 THEN {weight} ELSE 0 END), 0) AS INTEGER) AS no_output_n"
        ));
        select.push(format!(
            "CAST(ROUND(SUM(CASE WHEN user_turn_count IS NOT NULL THEN {weight} ELSE 0 END), 0) AS INTEGER) AS no_output_d"
        ));
    } else {
        select.push("COUNT(*) AS session_count".to_string());
        select.push("AVG(CASE WHEN user_turn_count IS NOT NULL THEN CAST(user_turn_count AS REAL) END) AS s2_avg".to_string());
        select.push(
            "AVG(CASE WHEN minutes_to_first_accepted_change IS NOT NULL THEN CAST(minutes_to_first_accepted_change AS REAL) END) AS time_to_first_accept_avg"
                .to_string(),
        );
        select.push(
            "COALESCE(SUM(CASE WHEN debug_loop_flag = 1 THEN 1 ELSE 0 END), 0) AS s4_n".to_string(),
        );
        select.push("COUNT(CASE WHEN debug_loop_flag IS NOT NULL THEN 1 END) AS s4_d".to_string());
        select.push("COALESCE(SUM(CASE WHEN mid_session_error_paste_flag = 1 THEN 1 ELSE 0 END), 0) AS s6_n".to_string());
        select.push(
            "COUNT(CASE WHEN mid_session_error_paste_flag IS NOT NULL THEN 1 END) AS s6_d"
                .to_string(),
        );
        select.push("COALESCE(SUM(CASE WHEN session_commit_within_4h_flag = 1 THEN 1 ELSE 0 END), 0) AS s9_n".to_string());
        select.push(
            "COUNT(CASE WHEN session_commit_within_4h_flag IS NOT NULL THEN 1 END) AS s9_d"
                .to_string(),
        );
        select.push(
            "COALESCE(SUM(CASE WHEN user_turn_count IS NOT NULL AND accepted_output_flag = 0 THEN 1 ELSE 0 END), 0) AS no_output_n"
                .to_string(),
        );
        select.push(
            "COUNT(CASE WHEN user_turn_count IS NOT NULL THEN 1 END) AS no_output_d".to_string(),
        );
    }

    let mut sql = format!("SELECT {} FROM {}", select.join(", "), source);
    let conditions = build_conditions(
        args,
        timestamp_col,
        use_task_base,
        use_task_base || use_branch_base,
        true,
        true,
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    let apply_sql_limit = !group.is_empty()
        && !matches!(args.group_by, Some(GroupBy::Task))
        && !options.implicit_model_default;
    if !group.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&group.join(", "));
        sql.push_str(" ORDER BY ");
        sql.push_str(&group.join(", "));
        if apply_sql_limit {
            sql.push_str(&format!(" LIMIT {}", args.limit.max(1)));
        }
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(SessionReportRow {
            week_start: row.get(0)?,
            group_value: row.get(1)?,
            branch_name: row.get(2)?,
            session_count: row.get(3)?,
            s2_avg: row.get(4)?,
            avg_minutes_to_first_accepted_change: row.get(5)?,
            debug_loop_rate: ratio_metric(row.get(6)?, row.get(7)?),
            s6_rate: ratio_metric(row.get(8)?, row.get(9)?),
            s9_rate: ratio_metric(row.get(10)?, row.get(11)?),
            no_output_session_rate: ratio_metric(row.get(12)?, row.get(13)?),
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        let row = row?;
        if row.session_count == 0 {
            continue;
        }
        if matches!(args.group_by, Some(GroupBy::Task))
            && !is_reportable_task(&row.group_value, row.branch_name.as_deref())
        {
            continue;
        }
        out.push(row);
        if matches!(args.group_by, Some(GroupBy::Task)) && out.len() >= args.limit.max(1) {
            break;
        }
    }
    if matches!(args.group_by, Some(GroupBy::Task)) {
        sort_rows_by_task_key_natural(
            &mut out,
            |row| row.week_start.as_deref(),
            |row| row.group_value.as_deref(),
            |row| row.branch_name.as_deref(),
        );
    }
    Ok(apply_session_report_options(out, args, options))
}

pub fn query_change_report(conn: &Connection, args: &ReportArgs) -> Result<Vec<ChangeReportRow>> {
    query_change_report_with_options(conn, args, ReportQueryOptions::default())
}

pub fn query_change_report_with_options(
    conn: &Connection,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Result<Vec<ChangeReportRow>> {
    let source = change_lifecycle_source(args);
    let use_commit_session_base = source == "view_commit_session_metrics_base";
    let timestamp_col = "commit_time";
    let mut select = vec![];
    let mut group = vec![];
    let mut base_sql = format!("SELECT * FROM {}", source);
    let conditions = build_change_report_conditions(args, timestamp_col, source);
    if !conditions.is_empty() {
        base_sql.push_str(" WHERE ");
        base_sql.push_str(&conditions.join(" AND "));
    }

    if args.weekly {
        select.push("base.week_start".to_string());
        group.push("base.week_start".to_string());
    } else {
        select.push("NULL AS week_start".to_string());
    }
    if matches!(args.group_by, Some(GroupBy::Task) | Some(GroupBy::Branch)) {
        select.push("base.repo_root".to_string());
        group.push("base.repo_root".to_string());
    } else {
        select.push("NULL AS repo_root".to_string());
    }

    let mut branch_selected = false;
    if let Some(group_expr) = change_report_group_expr(args.group_by) {
        select.push(format!("{group_expr} AS group_value"));
        group.push(group_expr.to_string());
        if matches!(args.group_by, Some(GroupBy::Task)) {
            select.push("base.branch_name".to_string());
            group.push("base.branch_name".to_string());
            branch_selected = true;
        } else {
            select.push("NULL AS branch_name".to_string());
        }
    } else {
        select.push("NULL AS group_value".to_string());
        select.push("NULL AS branch_name".to_string());
    }
    if !branch_selected && !select.iter().any(|item| item.contains("branch_name")) {
        select.push("NULL AS branch_name".to_string());
    }

    if use_commit_session_base {
        select.push("COUNT(DISTINCT base.commit_sha) AS commit_count".to_string());
        select.push(
            "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 THEN base.commit_sha END) AS heavy_commit_count"
                .to_string(),
        );
        select.push("COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND base.merged_to_mainline_flag = 1 THEN base.commit_sha END) AS c2_n".to_string());
        select.push(
            "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 THEN base.commit_sha END) AS c2_d"
                .to_string(),
        );
    } else {
        select.push("COUNT(*) AS commit_count".to_string());
        select.push(
            "COALESCE(SUM(CASE WHEN base.heavy_ai_flag = 1 THEN 1 ELSE 0 END), 0) AS heavy_commit_count"
                .to_string(),
        );
        select.push("COALESCE(SUM(CASE WHEN base.heavy_ai_flag = 1 AND base.merged_to_mainline_flag = 1 THEN 1 ELSE 0 END), 0) AS c2_n".to_string());
        select.push(
            "COALESCE(SUM(CASE WHEN base.heavy_ai_flag = 1 THEN 1 ELSE 0 END), 0) AS c2_d"
                .to_string(),
        );
    }
    select.push(
        "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND eo.repo_key LIKE 'git:github.com/%' THEN base.commit_sha END) AS c1_d"
            .to_string(),
    );
    select.push(
        "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND eo.repo_key LIKE 'git:github.com/%' AND lu.status IN ('resolved', 'no_pr') THEN base.commit_sha END) AS github_ready_count"
            .to_string(),
    );
    select.push(
        "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND eo.repo_key LIKE 'git:github.com/%' AND pr.pr_opened_flag = 1 THEN base.commit_sha END) AS c1_n"
            .to_string(),
    );
    select.push(
        "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND eo.repo_key LIKE 'git:github.com/%' AND pr.pr_opened_flag = 1 AND pr.pr_merged_flag = 1 THEN base.commit_sha END) AS c3_n"
            .to_string(),
    );
    select.push(
        "COUNT(DISTINCT CASE WHEN base.heavy_ai_flag = 1 AND eo.repo_key LIKE 'git:github.com/%' AND pr.pr_opened_flag = 1 THEN base.commit_sha END) AS c3_d"
            .to_string(),
    );
    select.push("COALESCE(SUM(fc.total_added), 0) AS task_branch_lines_added".to_string());
    select.push("COALESCE(SUM(fc.total_removed), 0) AS task_branch_lines_removed".to_string());

    let mut sql = format!(
        "SELECT {} FROM ({}) base
         LEFT JOIN event_commit_outcome eo
           ON eo.repo_root = base.repo_root
          AND eo.commit_sha = base.commit_sha
         LEFT JOIN fact_github_commit_pr_lookup lu
           ON lu.repo_key = eo.repo_key
          AND lu.commit_sha = base.commit_sha
         LEFT JOIN event_commit_pr_outcome pr
           ON pr.repo_root = base.repo_root
          AND pr.commit_sha = base.commit_sha
         LEFT JOIN fact_commit fc
           ON fc.repo_root = base.repo_root
          AND fc.commit_sha = base.commit_sha",
        select.join(", "),
        base_sql
    );
    let apply_sql_limit = !group.is_empty()
        && !matches!(args.group_by, Some(GroupBy::Task))
        && !options.implicit_model_default;
    if !group.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&group.join(", "));
        sql.push_str(" ORDER BY ");
        sql.push_str(&group.join(", "));
        if apply_sql_limit {
            sql.push_str(&format!(" LIMIT {}", args.limit.max(1)));
        }
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(ChangeReportRow {
            week_start: row.get(0)?,
            repo_root: row.get(1)?,
            group_value: row.get(2)?,
            branch_name: row.get(3)?,
            commit_count: row.get(4)?,
            heavy_commit_count: row.get(5)?,
            github_pr_heavy_eligible: row.get(8)?,
            github_pr_heavy_ready: row.get(9)?,
            pr_reach_rate: ratio_metric(row.get(10)?, row.get(9)?),
            merge_rate: ratio_metric(row.get(6)?, row.get(7)?),
            pr_merge_rate: ratio_metric(row.get(11)?, row.get(12)?),
            github_pr_metrics_available: {
                let eligible: i64 = row.get(8)?;
                let ready: i64 = row.get(9)?;
                eligible > 0 && ready > 0
            },
            task_branch_lines_added: row.get(13)?,
            task_branch_lines_removed: row.get(14)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        let row = row?;
        if matches!(args.group_by, Some(GroupBy::Task))
            && !is_reportable_task(&row.group_value, row.branch_name.as_deref())
        {
            continue;
        }
        out.push(row);
        if matches!(args.group_by, Some(GroupBy::Task)) && out.len() >= args.limit.max(1) {
            break;
        }
    }
    if matches!(args.group_by, Some(GroupBy::Task)) {
        sort_rows_by_task_key_natural(
            &mut out,
            |row| row.week_start.as_deref(),
            |row| row.group_value.as_deref(),
            |row| row.branch_name.as_deref(),
        );
    }
    Ok(apply_change_report_options(out, args, options))
}

pub fn query_lifecycle_report(
    conn: &Connection,
    args: &ReportArgs,
) -> Result<Vec<LifecycleReportRow>> {
    query_lifecycle_report_with_options(conn, args, ReportQueryOptions::default())
}

pub fn query_lifecycle_report_with_options(
    conn: &Connection,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Result<Vec<LifecycleReportRow>> {
    let source = change_lifecycle_source(args);
    let use_commit_session_base = source == "view_commit_session_metrics_base";
    let timestamp_col = "commit_time";
    let mut select = vec![];
    let mut group = vec![];

    if args.weekly {
        select.push("week_start".to_string());
        group.push("week_start".to_string());
    } else {
        select.push("NULL AS week_start".to_string());
    }

    let mut branch_selected = false;
    if let Some(group_expr) = change_lifecycle_group_expr(args.group_by) {
        select.push(format!("{group_expr} AS group_value"));
        group.push(group_expr.to_string());
        if matches!(args.group_by, Some(GroupBy::Task)) {
            select.push("branch_name".to_string());
            group.push("branch_name".to_string());
            branch_selected = true;
        } else {
            select.push("NULL AS branch_name".to_string());
        }
    } else {
        select.push("NULL AS group_value".to_string());
        select.push("NULL AS branch_name".to_string());
    }
    if !branch_selected && !select.iter().any(|item| item.contains("branch_name")) {
        select.push("NULL AS branch_name".to_string());
    }

    if use_commit_session_base {
        select.push(
            "COUNT(DISTINCT CASE WHEN heavy_ai_flag = 1 THEN commit_sha END) AS heavy_commit_count"
                .to_string(),
        );
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 THEN share_of_ai * ai_added_lines_removed_within_window ELSE 0 END), 0) AS INTEGER) AS l1_n".to_string());
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 THEN share_of_ai * ai_added_lines_reaching_mainline ELSE 0 END), 0) AS INTEGER) AS l1_d".to_string());
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 AND merged_to_mainline_flag = 1 AND bug_after_merge_flag = 1 THEN share_of_ai ELSE 0 END), 0) AS INTEGER) AS l3_n".to_string());
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 AND merged_to_mainline_flag = 1 THEN share_of_ai ELSE 0 END), 0) AS INTEGER) AS l3_d".to_string());
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 AND reverted_later_flag = 1 THEN share_of_ai ELSE 0 END), 0) AS INTEGER) AS l4_n".to_string());
        select.push("CAST(ROUND(SUM(CASE WHEN heavy_ai_flag = 1 THEN share_of_ai ELSE 0 END), 0) AS INTEGER) AS l4_d".to_string());
    } else {
        select.push(
            "COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 THEN 1 ELSE 0 END), 0) AS heavy_commit_count"
                .to_string(),
        );
        select.push("COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 THEN ai_added_lines_removed_within_window ELSE 0 END), 0) AS l1_n".to_string());
        select.push("COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 THEN ai_added_lines_reaching_mainline ELSE 0 END), 0) AS l1_d".to_string());
        select.push("COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 AND merged_to_mainline_flag = 1 AND bug_after_merge_flag = 1 THEN 1 ELSE 0 END), 0) AS l3_n".to_string());
        select.push("COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 AND merged_to_mainline_flag = 1 THEN 1 ELSE 0 END), 0) AS l3_d".to_string());
        select.push("COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 THEN reverted_later_flag ELSE 0 END), 0) AS l4_n".to_string());
        select.push(
            "COALESCE(SUM(CASE WHEN heavy_ai_flag = 1 THEN 1 ELSE 0 END), 0) AS l4_d".to_string(),
        );
    }

    let mut sql = format!("SELECT {} FROM {}", select.join(", "), source);
    let conditions = build_change_report_conditions(args, timestamp_col, source);
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    let apply_sql_limit = !group.is_empty()
        && !matches!(args.group_by, Some(GroupBy::Task))
        && !options.implicit_model_default;
    if !group.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&group.join(", "));
        sql.push_str(" ORDER BY ");
        sql.push_str(&group.join(", "));
        if apply_sql_limit {
            sql.push_str(&format!(" LIMIT {}", args.limit.max(1)));
        }
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(LifecycleReportRow {
            week_start: row.get(0)?,
            group_value: row.get(1)?,
            branch_name: row.get(2)?,
            heavy_commit_count: row.get(3)?,
            code_churn_rate: ratio_metric(row.get(4)?, row.get(5)?),
            bug_after_merge_rate: ratio_metric(row.get(6)?, row.get(7)?),
            revert_rate: ratio_metric(row.get(8)?, row.get(9)?),
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        let row = row?;
        if matches!(args.group_by, Some(GroupBy::Task))
            && !is_reportable_task(&row.group_value, row.branch_name.as_deref())
        {
            continue;
        }
        out.push(row);
        if matches!(args.group_by, Some(GroupBy::Task)) && out.len() >= args.limit.max(1) {
            break;
        }
    }
    if matches!(args.group_by, Some(GroupBy::Task)) {
        sort_rows_by_task_key_natural(
            &mut out,
            |row| row.week_start.as_deref(),
            |row| row.group_value.as_deref(),
            |row| row.branch_name.as_deref(),
        );
    }
    Ok(apply_lifecycle_report_options(out, args, options))
}

fn apply_session_report_options(
    rows: Vec<SessionReportRow>,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Vec<SessionReportRow> {
    if !should_apply_implicit_model_default(args, options) {
        return rows;
    }

    let filtered = filter_or_fallback(rows, |row: &SessionReportRow| row.session_count >= 3);
    limit_rows(sort_session_rows(filtered), args.limit)
}

fn apply_change_report_options(
    rows: Vec<ChangeReportRow>,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Vec<ChangeReportRow> {
    if !should_apply_implicit_model_default(args, options) {
        return rows;
    }

    let filtered = filter_or_fallback(rows, |row: &ChangeReportRow| {
        row.heavy_commit_count >= 3 && row.group_value.as_deref() != Some("human/(unknown)")
    });
    limit_rows(sort_change_rows(filtered), args.limit)
}

fn apply_lifecycle_report_options(
    rows: Vec<LifecycleReportRow>,
    args: &ReportArgs,
    options: ReportQueryOptions,
) -> Vec<LifecycleReportRow> {
    if !should_apply_implicit_model_default(args, options) {
        return rows;
    }

    let filtered = filter_or_fallback(rows, |row: &LifecycleReportRow| {
        row.heavy_commit_count >= 3 && row.group_value.as_deref() != Some("human/(unknown)")
    });
    limit_rows(sort_lifecycle_rows(filtered), args.limit)
}

fn should_apply_implicit_model_default(args: &ReportArgs, options: ReportQueryOptions) -> bool {
    options.implicit_model_default && !args.weekly && matches!(args.group_by, Some(GroupBy::Model))
}

fn filter_or_fallback<T, F>(rows: Vec<T>, mut predicate: F) -> Vec<T>
where
    T: Clone,
    F: FnMut(&T) -> bool,
{
    let filtered = rows
        .iter()
        .filter(|row| predicate(row))
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() { rows } else { filtered }
}

fn sort_session_rows(mut rows: Vec<SessionReportRow>) -> Vec<SessionReportRow> {
    rows.sort_by(|left, right| {
        right
            .session_count
            .cmp(&left.session_count)
            .then_with(|| {
                ratio_value(&right.no_output_session_rate)
                    .partial_cmp(&ratio_value(&left.no_output_session_rate))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.group_value.cmp(&right.group_value))
    });
    rows
}

fn sort_change_rows(mut rows: Vec<ChangeReportRow>) -> Vec<ChangeReportRow> {
    rows.sort_by(|left, right| {
        right
            .heavy_commit_count
            .cmp(&left.heavy_commit_count)
            .then_with(|| {
                ratio_value(&left.merge_rate)
                    .partial_cmp(&ratio_value(&right.merge_rate))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.group_value.cmp(&right.group_value))
    });
    rows
}

fn sort_lifecycle_rows(mut rows: Vec<LifecycleReportRow>) -> Vec<LifecycleReportRow> {
    rows.sort_by(|left, right| {
        right
            .heavy_commit_count
            .cmp(&left.heavy_commit_count)
            .then_with(|| {
                ratio_value(&right.bug_after_merge_rate)
                    .partial_cmp(&ratio_value(&left.bug_after_merge_rate))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.group_value.cmp(&right.group_value))
    });
    rows
}

fn ratio_value(metric: &RatioMetric) -> f64 {
    metric.percent().unwrap_or(-1.0)
}

/// Returns an ordering key for a task_key that sorts naturally: the prefix is
/// compared case-insensitively and any trailing digit run is compared
/// numerically, so `PAC-9 < PAC-10 < PAC-100`. Keys that don't match
/// `<letters>(-|_)<digits>` fall back to a lexicographic comparison by
/// pushing them after parseable keys.
fn task_key_sort_key(key: Option<&str>) -> (bool, String, u64, String) {
    let raw = key.unwrap_or("").to_string();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i > 0 && i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'_') {
        let prefix = raw[..i].to_ascii_uppercase();
        let rest = &raw[i + 1..];
        let digit_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
        if digit_len > 0
            && let Ok(number) = rest[..digit_len].parse::<u64>()
        {
            return (false, prefix, number, raw);
        }
    }
    (true, String::new(), u64::MAX, raw)
}

fn sort_rows_by_task_key_natural<T, FW, FT, FB>(
    rows: &mut [T],
    week_of: FW,
    task_key_of: FT,
    branch_of: FB,
) where
    FW: Fn(&T) -> Option<&str>,
    FT: Fn(&T) -> Option<&str>,
    FB: Fn(&T) -> Option<&str>,
{
    rows.sort_by(|left, right| {
        let left_key = (
            week_of(left).unwrap_or("").to_string(),
            task_key_sort_key(task_key_of(left)),
            branch_of(left).unwrap_or("").to_string(),
        );
        let right_key = (
            week_of(right).unwrap_or("").to_string(),
            task_key_sort_key(task_key_of(right)),
            branch_of(right).unwrap_or("").to_string(),
        );
        left_key.cmp(&right_key)
    });
}

fn limit_rows<T>(mut rows: Vec<T>, limit: usize) -> Vec<T> {
    rows.truncate(limit.max(1));
    rows
}

pub fn query_event_stream(
    conn: &Connection,
    args: &EventStreamArgs,
) -> Result<Vec<EventStreamRow>> {
    let mut rows = Vec::new();
    for stream in selected_event_streams(args) {
        rows.extend(load_event_stream_rows(
            conn,
            stream,
            effective_event_stream_category(args.category, stream),
            args,
        )?);
    }

    rows.sort_by(|left, right| {
        let left_time = left.event_time.as_deref();
        let right_time = right.event_time.as_deref();
        left_time
            .is_none()
            .cmp(&right_time.is_none())
            .then_with(|| left_time.cmp(&right_time))
            .then_with(|| left.stream_type.cmp(&right.stream_type))
            .then_with(|| left.sort_identity.cmp(&right.sort_identity))
    });

    if let Some(limit) = args.limit {
        rows.truncate(limit);
    }

    Ok(rows)
}

fn ratio_metric(numerator: i64, denominator: i64) -> RatioMetric {
    RatioMetric {
        numerator,
        denominator,
    }
}

fn selected_event_streams(args: &EventStreamArgs) -> Vec<EventStreamKind> {
    if args.stream != EventStreamKind::All {
        return vec![args.stream];
    }

    match args.category {
        EventCategory::Session => vec![
            EventStreamKind::SessionBase,
            EventStreamKind::TaskSessionBase,
        ],
        EventCategory::Delivery | EventCategory::Quality => vec![
            EventStreamKind::ChangeBase,
            EventStreamKind::CommitSessionBase,
            EventStreamKind::TaskCommitBase,
        ],
        EventCategory::All => vec![
            EventStreamKind::SessionBase,
            EventStreamKind::TaskSessionBase,
            EventStreamKind::ChangeBase,
            EventStreamKind::CommitSessionBase,
            EventStreamKind::TaskCommitBase,
        ],
    }
}

fn load_event_stream_rows(
    conn: &Connection,
    stream: EventStreamKind,
    category: EventCategory,
    args: &EventStreamArgs,
) -> Result<Vec<EventStreamRow>> {
    let Some(conditions) = build_event_stream_conditions(args, stream) else {
        return Ok(Vec::new());
    };

    let mut sql = format!("SELECT * FROM {}", event_stream_view_name(stream));
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    let mut stmt = conn.prepare(&sql)?;
    let column_names = stmt
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let fields = row_to_json_object(row, &column_names)?;
        result.push(build_event_stream_row(stream, category, fields));
    }

    Ok(result)
}

fn build_event_stream_row(
    stream: EventStreamKind,
    category: EventCategory,
    fields: JsonMap<String, JsonValue>,
) -> EventStreamRow {
    let event_time = event_stream_field_string(&fields, event_stream_time_column(stream));
    let repo_root = event_stream_field_string(&fields, "repo_root");
    let provider = event_stream_field_string(&fields, "provider");
    let model = event_stream_field_string(&fields, "model_name");
    let task_key = event_stream_field_string(&fields, "task_key");
    let branch_name = event_stream_field_string(&fields, "branch_name");
    let sort_identity = event_stream_sort_identity(stream, &fields);

    EventStreamRow {
        stream_type: event_stream_name(stream).to_string(),
        category: event_category_name(category).to_string(),
        event_time,
        repo_root,
        provider,
        model,
        task_key,
        branch_name,
        fields: JsonValue::Object(fields),
        sort_identity,
    }
}

fn row_to_json_object(
    row: &Row<'_>,
    column_names: &[String],
) -> Result<JsonMap<String, JsonValue>> {
    let mut fields = JsonMap::new();
    for (index, column_name) in column_names.iter().enumerate() {
        let value = row.get_ref(index)?;
        fields.insert(column_name.clone(), sqlite_value_to_json(value));
    }
    Ok(fields)
}

fn sqlite_value_to_json(value: ValueRef<'_>) -> JsonValue {
    match value {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Integer(value) => JsonValue::from(value),
        ValueRef::Real(value) => JsonValue::from(value),
        ValueRef::Text(value) => JsonValue::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => {
            JsonValue::String(value.iter().map(|byte| format!("{byte:02x}")).collect())
        }
    }
}

fn event_stream_field_string(fields: &JsonMap<String, JsonValue>, key: &str) -> Option<String> {
    fields
        .get(key)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

fn event_stream_sort_identity(
    stream: EventStreamKind,
    fields: &JsonMap<String, JsonValue>,
) -> Vec<String> {
    match stream {
        EventStreamKind::SessionBase => vec![
            event_stream_field_string(fields, "provider").unwrap_or_default(),
            event_stream_field_string(fields, "session_id").unwrap_or_default(),
        ],
        EventStreamKind::TaskSessionBase => vec![
            event_stream_field_string(fields, "repo_root").unwrap_or_default(),
            event_stream_field_string(fields, "task_key").unwrap_or_default(),
            event_stream_field_string(fields, "provider").unwrap_or_default(),
            event_stream_field_string(fields, "session_id").unwrap_or_default(),
        ],
        EventStreamKind::ChangeBase => vec![
            event_stream_field_string(fields, "repo_root").unwrap_or_default(),
            event_stream_field_string(fields, "commit_sha").unwrap_or_default(),
        ],
        EventStreamKind::CommitSessionBase => vec![
            event_stream_field_string(fields, "repo_root").unwrap_or_default(),
            event_stream_field_string(fields, "commit_sha").unwrap_or_default(),
            event_stream_field_string(fields, "provider").unwrap_or_default(),
            event_stream_field_string(fields, "session_id").unwrap_or_default(),
        ],
        EventStreamKind::TaskCommitBase => vec![
            event_stream_field_string(fields, "repo_root").unwrap_or_default(),
            event_stream_field_string(fields, "task_key").unwrap_or_default(),
            event_stream_field_string(fields, "commit_sha").unwrap_or_default(),
        ],
        EventStreamKind::All => Vec::new(),
    }
}

fn event_stream_view_name(stream: EventStreamKind) -> &'static str {
    match stream {
        EventStreamKind::SessionBase => "view_session_metrics_base",
        EventStreamKind::TaskSessionBase => "view_task_session_metrics_base",
        EventStreamKind::ChangeBase => "view_change_metrics_base",
        EventStreamKind::CommitSessionBase => "view_commit_session_metrics_base",
        EventStreamKind::TaskCommitBase => "view_task_commit_metrics_base",
        EventStreamKind::All => unreachable!("all is expanded before querying"),
    }
}

fn event_stream_name(stream: EventStreamKind) -> &'static str {
    match stream {
        EventStreamKind::SessionBase => "session-base",
        EventStreamKind::TaskSessionBase => "task-session-base",
        EventStreamKind::ChangeBase => "change-base",
        EventStreamKind::CommitSessionBase => "commit-session-base",
        EventStreamKind::TaskCommitBase => "task-commit-base",
        EventStreamKind::All => "all",
    }
}

fn effective_event_stream_category(
    requested: EventCategory,
    stream: EventStreamKind,
) -> EventCategory {
    match requested {
        EventCategory::All => match stream {
            EventStreamKind::SessionBase | EventStreamKind::TaskSessionBase => {
                EventCategory::Session
            }
            EventStreamKind::ChangeBase
            | EventStreamKind::CommitSessionBase
            | EventStreamKind::TaskCommitBase => EventCategory::Delivery,
            EventStreamKind::All => EventCategory::All,
        },
        _ => requested,
    }
}

fn event_category_name(category: EventCategory) -> &'static str {
    match category {
        EventCategory::Session => "session",
        EventCategory::Delivery => "delivery",
        EventCategory::Quality => "quality",
        EventCategory::All => "all",
    }
}

fn event_stream_time_column(stream: EventStreamKind) -> &'static str {
    match stream {
        EventStreamKind::SessionBase | EventStreamKind::TaskSessionBase => "started_at",
        EventStreamKind::ChangeBase
        | EventStreamKind::CommitSessionBase
        | EventStreamKind::TaskCommitBase => "commit_time",
        EventStreamKind::All => unreachable!("all is expanded before querying"),
    }
}

fn build_event_stream_conditions(
    args: &EventStreamArgs,
    stream: EventStreamKind,
) -> Option<Vec<String>> {
    let provider_capable = matches!(
        stream,
        EventStreamKind::SessionBase
            | EventStreamKind::TaskSessionBase
            | EventStreamKind::CommitSessionBase
    );
    let task_capable = matches!(
        stream,
        EventStreamKind::TaskSessionBase | EventStreamKind::TaskCommitBase
    );
    let model_capable = matches!(
        stream,
        EventStreamKind::SessionBase
            | EventStreamKind::TaskSessionBase
            | EventStreamKind::CommitSessionBase
    );

    if args.provider.is_some() && !provider_capable {
        return None;
    }
    if args.task.is_some() && !task_capable {
        return None;
    }
    if args.model.is_some() && !model_capable {
        return None;
    }

    let mut conditions = Vec::new();
    if let Some(repo) = args.repo.as_deref() {
        conditions.push(format!("repo_root = {}", sql_literal(repo)));
    }
    if let Some(provider) = args.provider.as_deref() {
        conditions.push(format!("provider = {}", sql_literal(provider)));
    }
    if let Some(task) = args.task.as_deref() {
        conditions.push(format!("task_key = {}", sql_literal(task)));
    }
    if let Some(model) = args.model.as_deref() {
        conditions.push(format!(
            "COALESCE(model_name, '(unknown)') = {}",
            sql_literal(model)
        ));
    }
    let time_col = event_stream_time_column(stream);
    if let Some(from) = args.from.as_deref() {
        conditions.push(format!("date({time_col}) >= date({})", sql_literal(from)));
    }
    if let Some(to) = args.to.as_deref() {
        conditions.push(format!("date({time_col}) <= date({})", sql_literal(to)));
    }

    Some(conditions)
}

fn session_group_expr(group_by: Option<GroupBy>) -> Option<&'static str> {
    match group_by {
        Some(GroupBy::Repo) => Some("COALESCE(repo_root, '(unknown)')"),
        Some(GroupBy::Provider) => Some("provider"),
        Some(GroupBy::Task) => Some("task_key"),
        Some(GroupBy::Branch) => Some("branch_name"),
        Some(GroupBy::Model) => Some("COALESCE(model_name, '(unknown)')"),
        None => None,
    }
}

fn change_lifecycle_group_expr(group_by: Option<GroupBy>) -> Option<&'static str> {
    match group_by {
        Some(GroupBy::Repo) => Some("COALESCE(repo_root, '(unknown)')"),
        Some(GroupBy::Provider) => Some("provider"),
        Some(GroupBy::Task) => Some("task_key"),
        Some(GroupBy::Branch) => Some("branch_name"),
        Some(GroupBy::Model) => Some("COALESCE(model_name, '(unknown)')"),
        None => None,
    }
}

fn change_report_group_expr(group_by: Option<GroupBy>) -> Option<&'static str> {
    match group_by {
        Some(GroupBy::Repo) => Some("COALESCE(base.repo_root, '(unknown)')"),
        Some(GroupBy::Provider) => Some("base.provider"),
        Some(GroupBy::Task) => Some("base.task_key"),
        Some(GroupBy::Branch) => Some("base.branch_name"),
        Some(GroupBy::Model) => Some("COALESCE(base.model_name, '(unknown)')"),
        None => None,
    }
}

fn change_lifecycle_source(args: &ReportArgs) -> &'static str {
    if matches!(args.group_by, Some(GroupBy::Task)) || args.task.is_some() {
        "view_task_commit_metrics_base"
    } else if matches!(args.group_by, Some(GroupBy::Branch)) || args.branch.is_some() {
        "view_branch_commit_metrics_base"
    } else if matches!(args.group_by, Some(GroupBy::Provider | GroupBy::Model))
        || args.provider.is_some()
        || args.model.is_some()
    {
        "view_commit_session_metrics_base"
    } else {
        "view_change_metrics_base"
    }
}

fn build_conditions(
    args: &ReportArgs,
    timestamp_col: &str,
    task_capable: bool,
    branch_capable: bool,
    provider_capable: bool,
    model_capable: bool,
) -> Vec<String> {
    let mut conditions = Vec::new();
    if let Some(repo) = args.repo.as_deref() {
        conditions.push(format!("repo_root = {}", sql_literal(repo)));
    }
    if let Some(provider) = args.provider.as_deref()
        && provider_capable
    {
        conditions.push(format!("provider = {}", sql_literal(provider)));
    }
    if let Some(task) = args.task.as_deref()
        && task_capable
    {
        conditions.push(format!("task_key = {}", sql_literal(task)));
    }
    if let Some(branch) = args.branch.as_deref()
        && branch_capable
    {
        conditions.push(format!("branch_name = {}", sql_literal(branch)));
    }
    if let Some(model) = args.model.as_deref()
        && model_capable
    {
        conditions.push(format!(
            "COALESCE(model_name, '(unknown)') = {}",
            sql_literal(model)
        ));
    }
    if let Some(from) = args.from.as_deref() {
        conditions.push(format!(
            "date({timestamp_col}) >= date({})",
            sql_literal(from)
        ));
    }
    if let Some(to) = args.to.as_deref() {
        conditions.push(format!(
            "date({timestamp_col}) <= date({})",
            sql_literal(to)
        ));
    }
    conditions
}

fn normalized_commit_session_model_expr(alias: &str) -> String {
    format!(
        "CASE \
            WHEN NULLIF(TRIM({alias}.model_name), '') IS NULL THEN {alias}.provider || '/(unknown)' \
            WHEN {alias}.model_name LIKE {alias}.provider || '/%' THEN {alias}.model_name \
            ELSE {alias}.provider || '/' || {alias}.model_name \
         END"
    )
}

fn task_commit_session_filter_condition(
    outer_table: &str,
    provider: Option<&str>,
    model: Option<&str>,
) -> Option<String> {
    if provider.is_none() && model.is_none() {
        return None;
    }

    let mut clauses = vec![
        format!("cs_filter.repo_root = {outer_table}.repo_root"),
        format!("cs_filter.commit_sha = {outer_table}.commit_sha"),
    ];
    if let Some(provider) = provider {
        clauses.push(format!("cs_filter.provider = {}", sql_literal(provider)));
    }
    if let Some(model) = model {
        clauses.push(format!(
            "{} = {}",
            normalized_commit_session_model_expr("cs_filter"),
            sql_literal(model)
        ));
    }

    Some(format!(
        "EXISTS (SELECT 1 FROM event_commit_session cs_filter WHERE {})",
        clauses.join(" AND ")
    ))
}

fn build_change_report_conditions(
    args: &ReportArgs,
    timestamp_col: &str,
    source: &str,
) -> Vec<String> {
    let task_capable = source == "view_task_commit_metrics_base";
    let branch_capable = matches!(
        source,
        "view_task_commit_metrics_base" | "view_branch_commit_metrics_base"
    );
    let direct_provider_capable = source == "view_commit_session_metrics_base";
    let direct_model_capable = source == "view_commit_session_metrics_base";

    let mut conditions = build_conditions(
        args,
        timestamp_col,
        task_capable,
        branch_capable,
        direct_provider_capable,
        direct_model_capable,
    );

    if matches!(
        source,
        "view_task_commit_metrics_base" | "view_branch_commit_metrics_base"
    ) && let Some(exists_sql) = task_commit_session_filter_condition(
        source,
        args.provider.as_deref(),
        args.model.as_deref(),
    ) {
        conditions.push(exists_sql);
    }

    conditions
}

fn session_list_conditions(args: &ReportArgs) -> Vec<String> {
    let mut conditions = Vec::new();
    if let Some(repo) = args.repo.as_deref() {
        conditions.push(format!("repo_root = {}", sql_literal(repo)));
    }
    if let Some(provider) = args.provider.as_deref() {
        conditions.push(format!("provider = {}", sql_literal(provider)));
    }
    if let Some(model) = args.model.as_deref() {
        conditions.push(format!("model_name = {}", sql_literal(model)));
    }
    if let Some(from) = args.from.as_deref() {
        conditions.push(format!("date(last_active) >= date({})", sql_literal(from)));
    }
    if let Some(to) = args.to.as_deref() {
        conditions.push(format!("date(last_active) <= date({})", sql_literal(to)));
    }
    if let Some(task) = args.task.as_deref() {
        conditions.push(format!(
            "EXISTS (
                 SELECT 1
                 FROM event_task_session ts
                 WHERE ts.provider = view_session_productivity.provider
                   AND ts.session_id = view_session_productivity.session_id
                   AND ts.task_key = {}
             )",
            sql_literal(task)
        ));
    }
    if let Some(branch) = args.branch.as_deref() {
        conditions.push(format!(
            "EXISTS (
                 SELECT 1
                 FROM view_branch_session_metrics_base bs
                 WHERE bs.provider = view_session_productivity.provider
                   AND bs.session_id = view_session_productivity.session_id
                   AND bs.branch_name = {}
             )",
            sql_literal(branch)
        ));
    }
    conditions
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_reportable_task(task_key: &Option<String>, branch_name: Option<&str>) -> bool {
    task_key.as_deref().map(looks_like_task_id).unwrap_or(false)
        && !branch_name.map(is_integration_branch).unwrap_or(false)
}

fn refresh_task_session_events(conn: &Connection) -> Result<()> {
    // Group by the same key as event_task_session's PRIMARY KEY so we can't
    // ever emit two rows that collide on insert. A single task_key can span
    // multiple branch names (e.g. `pac-595-foo` and `pac-595-bar` both yield
    // `PAC-595`); when the same session contributed to commits on both
    // branches we collapse them into one row here and pick a deterministic
    // branch_name via MIN.
    let mut stmt = conn.prepare(
        "SELECT
            tc.repo_root,
            MAX(COALESCE(tc.repo_key, cs.repo_key, sq.repo_key)) AS repo_key,
            tc.task_key,
            MIN(tc.branch_name) AS branch_name,
            cs.provider,
            cs.session_id,
            MAX(COALESCE(cs.member_email, sq.member_email, '(unknown)')) AS member_email,
            MAX(COALESCE(cs.device_id, sq.device_id, '(unknown)')) AS device_id,
            MAX(cs.model_name) AS model_name,
            MAX(sq.started_at) AS started_at,
            SUM(cs.matched_lines) AS attribution_weight,
            MAX(
                CASE
                    WHEN sq.started_at IS NOT NULL
                     AND cs.commit_time IS NOT NULL
                     AND julianday(cs.commit_time) >= julianday(sq.started_at)
                     AND julianday(cs.commit_time) <= julianday(COALESCE(sq.ended_at, sq.started_at), '+4 hours')
                    THEN 1 ELSE 0
                END
            ) AS commit_within_window_flag,
            MAX(sq.user_turn_count) AS user_turn_count,
            MAX(sq.debug_loop_flag) AS debug_loop_flag,
            MAX(sq.mid_session_error_paste_flag) AS mid_session_error_paste_flag,
            MAX(sq.accepted_output_flag) AS accepted_output_flag,
            MAX(sq.first_accepted_change_at) AS first_accepted_change_at,
            MAX(sq.minutes_to_first_accepted_change) AS minutes_to_first_accepted_change
         FROM event_task_commit tc
         JOIN event_commit_session cs
           ON cs.repo_root = tc.repo_root
          AND cs.commit_sha = tc.commit_sha
         LEFT JOIN event_session_quality sq
           ON sq.provider = cs.provider
          AND sq.session_id = cs.session_id
         GROUP BY tc.repo_root, tc.task_key, cs.provider, cs.session_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, f64>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, Option<i64>>(12)?,
            row.get::<_, Option<i64>>(13)?,
            row.get::<_, Option<i64>>(14)?,
            row.get::<_, Option<i64>>(15)?,
            row.get::<_, Option<String>>(16)?,
            row.get::<_, Option<f64>>(17)?,
        ))
    })?;

    for row in rows {
        let (
            repo_root,
            repo_key,
            task_key,
            branch_name,
            provider,
            session_id,
            member_email,
            device_id,
            model_name,
            started_at,
            attribution_weight,
            commit_within_window_flag,
            user_turn_count,
            debug_loop_flag,
            mid_session_error_paste_flag,
            accepted_output_flag,
            first_accepted_change_at,
            minutes_to_first_accepted_change,
        ) = row?;

        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, repo_key, task_key, branch_name, provider, session_id, member_email, device_id, model_name, started_at,
                attribution_weight, commit_within_window_flag, user_turn_count, debug_loop_flag,
                mid_session_error_paste_flag, accepted_output_flag, first_accepted_change_at,
                minutes_to_first_accepted_change
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            params![
                repo_root,
                repo_key,
                task_key,
                branch_name,
                provider,
                session_id,
                member_email,
                device_id,
                model_name,
                started_at,
                attribution_weight,
                commit_within_window_flag,
                user_turn_count,
                debug_loop_flag,
                mid_session_error_paste_flag,
                accepted_output_flag,
                first_accepted_change_at,
                minutes_to_first_accepted_change
            ],
        )?;
    }

    Ok(())
}

fn load_session_messages(conn: &Connection) -> Result<BTreeMap<SessionKey, Vec<SessionMessage>>> {
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, role, content
         FROM fact_session_message
         WHERE role IN ('user', 'assistant')
         ORDER BY provider, session_id, message_index, id",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;

    let mut out = BTreeMap::new();
    for row in rows {
        let (provider, session_id, role, content) = row?;
        out.entry(SessionKey {
            provider,
            session_id,
        })
        .or_insert_with(Vec::new)
        .push(SessionMessage { role, content });
    }
    Ok(out)
}

fn load_fact_commits(conn: &Connection) -> Result<Vec<CandidateCommit>> {
    let mut stmt = conn.prepare(
        "SELECT repo_root, commit_sha, commit_time, heavy_ai, matched_total_lines, (total_added + total_removed) AS total_lines
         FROM fact_commit
         ORDER BY repo_root, commit_time",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (repo_root, commit_sha, commit_time_raw, heavy_ai, matched_total_lines, total_lines) =
            row?;
        let commit_time = DateTime::parse_from_rfc3339(&commit_time_raw)
            .map_err(|e| anyhow!("invalid commit_time '{}': {}", commit_time_raw, e))?
            .with_timezone(&Utc);
        out.push(CandidateCommit {
            repo_root,
            commit_sha,
            commit_time,
            heavy_ai: heavy_ai != 0,
            matched_total_lines,
            commit_total_lines: total_lines,
        });
    }
    Ok(out)
}

fn derive_repo_commit_events(
    conn: &Connection,
    repo_root: &str,
    commits: &[CandidateCommit],
    verbose: bool,
) -> Result<HashMap<String, DerivedCommitEvent>> {
    if commits.is_empty() {
        return Ok(HashMap::new());
    }

    let cache_started = Instant::now();
    let main_ref = resolve_mainline_ref(repo_root)?;
    let revert_map = build_revert_map(repo_root)?;
    let merge_commit_set = load_merge_commit_set(repo_root)?;
    let mainline_commit_set = if let Some(ref_name) = main_ref.as_ref() {
        load_ref_commit_set(repo_root, ref_name)?
    } else {
        HashSet::new()
    };
    let commit_added_hashes = load_commit_added_hashes(conn, repo_root)?;
    let session_added_availability = load_session_added_availability(conn, repo_root)?;

    let earliest_commit_time = commits
        .iter()
        .map(|commit| commit.commit_time)
        .min()
        .ok_or_else(|| anyhow!("missing earliest commit time"))?;
    let latest_commit_time = commits
        .iter()
        .map(|commit| commit.commit_time)
        .max()
        .ok_or_else(|| anyhow!("missing latest commit time"))?;

    let (mainline_added_events, mainline_removed_events) = if let Some(ref_name) = main_ref.as_ref()
    {
        load_mainline_hash_events(
            repo_root,
            ref_name,
            &earliest_commit_time,
            &(latest_commit_time + Duration::days(CHURN_WINDOW_DAYS)),
        )?
    } else {
        (Vec::new(), Vec::new())
    };

    if verbose {
        println!(
            "    {} caches built: commit_added={} session_availability={} mainline_added={} mainline_removed={} elapsed={}",
            shorten_repo(repo_root),
            commit_added_hashes.len(),
            session_added_availability.len(),
            mainline_added_events.len(),
            mainline_removed_events.len(),
            format_elapsed(cache_started.elapsed().as_millis())
        );
    }

    let mut derived = derive_commit_events_from_preloaded(
        commits,
        &revert_map,
        &merge_commit_set,
        &mainline_commit_set,
        &commit_added_hashes,
        &session_added_availability,
        &mainline_added_events,
        &mainline_removed_events,
    )?;
    annotate_bug_after_merge_signals(conn, repo_root, commits, &mut derived)?;
    let repo_key = sync_identity::repo_key_for_repo_root(Some(repo_root)).unwrap_or_default();
    annotate_issue_linked_bug_after_merge_signals(conn, &repo_key, commits, &mut derived)?;
    Ok(derived)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the preloaded commit-analysis caches are passed separately to avoid building an extra bundle"
)]
fn derive_commit_events_from_preloaded(
    commits: &[CandidateCommit],
    revert_map: &HashMap<String, DateTime<Utc>>,
    merge_commit_set: &HashSet<String>,
    mainline_commit_set: &HashSet<String>,
    commit_added_hashes: &CommitPathHashCounts,
    session_added_availability: &PathHashCounts,
    mainline_added_events: &[TimedLineHashChange],
    mainline_removed_events: &[TimedLineHashChange],
) -> Result<HashMap<String, DerivedCommitEvent>> {
    let mut out = HashMap::new();
    let mut descending_commits = commits.to_vec();
    descending_commits.sort_by(|a, b| {
        b.commit_time
            .cmp(&a.commit_time)
            .then_with(|| b.commit_sha.cmp(&a.commit_sha))
    });

    let mut added_index = MainlineIndex::new();
    let mut added_event_index = 0usize;
    for commit in &descending_commits {
        while added_event_index < mainline_added_events.len()
            && mainline_added_events[added_event_index].commit_time >= commit.commit_time
        {
            let event = &mainline_added_events[added_event_index];
            added_index.add_max(&event.rel_path, &event.line_hash, event.count);
            added_event_index += 1;
        }

        if merge_commit_set.contains(&commit.commit_sha) {
            continue;
        }

        let budget = build_ai_added_budget_from_preloaded(
            commit_added_hashes,
            session_added_availability,
            &commit.commit_sha,
        );
        let budget_total = budget_total(&budget);
        let reverted_later = revert_map
            .get(&commit.commit_sha)
            .map(|revert_ts| *revert_ts > commit.commit_time)
            .unwrap_or(false);
        let merged_to_mainline = if mainline_commit_set.contains(&commit.commit_sha) {
            true
        } else {
            budget_total > 0 && is_content_merged_in_index(&budget, &added_index)
        };

        out.insert(
            commit.commit_sha.clone(),
            DerivedCommitEvent {
                reverted_later,
                merged_to_mainline,
                budget,
                ai_added_lines_reaching_mainline: if merged_to_mainline { budget_total } else { 0 },
                ai_added_lines_removed_within_window: 0,
                bug_after_merge: false,
                first_bug_signal_commit_sha: None,
                first_bug_signal_commit_time: None,
                bug_signal_count: 0,
                bug_signal_sources: BTreeSet::new(),
            },
        );
    }

    let mut removed_index: RemovedHashIndex = HashMap::new();
    let mut removed_add_index = 0usize;
    let mut removed_drop_index = 0usize;
    for commit in commits {
        let window_end = commit.commit_time + Duration::days(CHURN_WINDOW_DAYS);
        while removed_add_index < mainline_removed_events.len()
            && mainline_removed_events[removed_add_index].commit_time <= window_end
        {
            let event = &mainline_removed_events[removed_add_index];
            let key = (event.rel_path.clone(), event.line_hash.clone());
            *removed_index.entry(key).or_insert(0) += event.count;
            removed_add_index += 1;
        }

        while removed_drop_index < mainline_removed_events.len()
            && mainline_removed_events[removed_drop_index].commit_time < commit.commit_time
        {
            let event = &mainline_removed_events[removed_drop_index];
            let key = (event.rel_path.clone(), event.line_hash.clone());
            if let Some(value) = removed_index.get_mut(&key) {
                *value -= event.count;
                if *value <= 0 {
                    removed_index.remove(&key);
                }
            }
            removed_drop_index += 1;
        }

        let Some(derived) = out.get_mut(&commit.commit_sha) else {
            continue;
        };
        if derived.merged_to_mainline && derived.ai_added_lines_reaching_mainline > 0 {
            derived.ai_added_lines_removed_within_window =
                compute_churn(&derived.budget, &removed_index);
        }
    }

    Ok(out)
}

fn annotate_bug_after_merge_signals(
    conn: &Connection,
    repo_root: &str,
    commits: &[CandidateCommit],
    derived: &mut HashMap<String, DerivedCommitEvent>,
) -> Result<()> {
    let commit_messages = load_repo_commit_messages(repo_root)?;
    let commit_removed_hashes = load_commit_removed_hashes(conn, repo_root)?;
    annotate_bug_after_merge_signals_from_data(
        commits,
        &commit_messages,
        &commit_removed_hashes,
        derived,
    );
    Ok(())
}

fn annotate_bug_after_merge_signals_from_data(
    commits: &[CandidateCommit],
    commit_messages: &HashMap<String, String>,
    commit_removed_hashes: &CommitPathHashCounts,
    derived: &mut HashMap<String, DerivedCommitEvent>,
) {
    let mut ordered_commits = commits.to_vec();
    ordered_commits.sort_by(|a, b| {
        a.commit_time
            .cmp(&b.commit_time)
            .then_with(|| a.commit_sha.cmp(&b.commit_sha))
    });

    let mut fix_candidates = Vec::new();
    for commit in &ordered_commits {
        let Some(message) = commit_messages.get(&commit.commit_sha) else {
            continue;
        };
        let Some(removed_hashes) = commit_removed_hashes.get(&commit.commit_sha) else {
            continue;
        };
        if budget_total(removed_hashes) <= 0
            || !is_fix_like_commit_message(message)
            || is_revert_commit_message(message)
        {
            continue;
        }
        fix_candidates.push(BugFixCandidate {
            commit_sha: commit.commit_sha.clone(),
            commit_time: commit.commit_time,
            removed_hashes: removed_hashes.clone(),
        });
    }

    for commit in &ordered_commits {
        let Some(event) = derived.get_mut(&commit.commit_sha) else {
            continue;
        };
        if !commit.heavy_ai || !event.merged_to_mainline || budget_total(&event.budget) <= 0 {
            continue;
        }

        let window_end = commit.commit_time + Duration::days(BUG_AFTER_MERGE_WINDOW_DAYS);
        for candidate in &fix_candidates {
            if candidate.commit_time <= commit.commit_time {
                continue;
            }
            if candidate.commit_time > window_end {
                break;
            }
            if exact_hash_overlap_count(&event.budget, &candidate.removed_hashes) <= 0 {
                continue;
            }
            record_bug_signal(
                event,
                candidate.commit_sha.clone(),
                candidate.commit_time,
                "git_fix_commit",
            );
        }
    }
}

fn annotate_issue_linked_bug_after_merge_signals(
    conn: &Connection,
    repo_key: &str,
    commits: &[CandidateCommit],
    derived: &mut HashMap<String, DerivedCommitEvent>,
) -> Result<()> {
    if !repo_key.starts_with("git:github.com/") {
        return Ok(());
    }
    let candidates = load_issue_linked_bug_signal_candidates(conn, repo_key)?;
    annotate_issue_linked_bug_after_merge_signals_from_data(commits, &candidates, derived);
    Ok(())
}

fn annotate_issue_linked_bug_after_merge_signals_from_data(
    commits: &[CandidateCommit],
    candidates: &[IssueLinkedBugSignalCandidate],
    derived: &mut HashMap<String, DerivedCommitEvent>,
) {
    if candidates.is_empty() {
        return;
    }

    let mut ordered_signals = candidates.to_vec();
    ordered_signals.sort_by(|left, right| {
        left.window_anchor_time
            .cmp(&right.window_anchor_time)
            .then_with(|| left.signal_ref.cmp(&right.signal_ref))
    });

    let mut ordered_commits = commits.to_vec();
    ordered_commits.sort_by(|a, b| {
        a.commit_time
            .cmp(&b.commit_time)
            .then_with(|| a.commit_sha.cmp(&b.commit_sha))
    });

    for commit in &ordered_commits {
        let Some(event) = derived.get_mut(&commit.commit_sha) else {
            continue;
        };
        if !commit.heavy_ai || !event.merged_to_mainline || budget_total(&event.budget) <= 0 {
            continue;
        }

        let window_end = commit.commit_time + Duration::days(BUG_AFTER_MERGE_WINDOW_DAYS);
        for candidate in &ordered_signals {
            if candidate.window_anchor_time <= commit.commit_time {
                continue;
            }
            if candidate.window_anchor_time > window_end {
                break;
            }
            if exact_hash_overlap_count(&event.budget, &candidate.removed_hashes) <= 0 {
                continue;
            }
            record_bug_signal(
                event,
                candidate.signal_ref.clone(),
                candidate.signal_time,
                "github_issue_fix_pr_exact_hash",
            );
        }
    }
}

fn build_session_turns(messages: &[SessionMessage]) -> Vec<SessionTurn> {
    let mut turns = Vec::new();
    let mut current_user: Option<String> = None;
    let mut assistant_parts: Vec<&str> = Vec::new();

    for message in messages {
        match message.role.as_str() {
            "user" => {
                if let Some(user_text) = current_user.take() {
                    turns.push(SessionTurn {
                        user_text,
                        assistant_text: assistant_parts.join("\n"),
                    });
                    assistant_parts.clear();
                }
                current_user = Some(message.content.clone());
            }
            "assistant" if current_user.is_some() => {
                assistant_parts.push(message.content.as_str());
            }
            _ => {}
        }
    }

    if let Some(user_text) = current_user {
        turns.push(SessionTurn {
            user_text,
            assistant_text: assistant_parts.join("\n"),
        });
    }

    turns
}

fn has_mid_session_error_paste(messages: &[SessionMessage]) -> bool {
    let mut user_message_index = 0usize;
    for message in messages {
        if message.role != "user" {
            continue;
        }
        user_message_index += 1;
        if user_message_index <= 1 {
            continue;
        }
        if contains_error_paste_signal(&message.content) {
            return true;
        }
    }
    false
}

fn contains_error_paste_signal(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    const STRONG_MARKERS: [&str; 17] = [
        "traceback (most recent call last):",
        "error ts",
        "typeerror:",
        "referenceerror:",
        "syntaxerror:",
        "runtimeerror:",
        "cannot find module",
        "module not found",
        "build failed",
        "test failed",
        "tests failed",
        "compilation failed",
        "failed with exit code",
        "panic:",
        "assertionerror",
        "exception:",
        "stack trace",
    ];
    if STRONG_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return true;
    }
    if lower.contains("error[") || lower.contains("error:") || lower.contains("traceback") {
        return true;
    }
    if lower.contains(" failed")
        && (lower.contains("test")
            || lower.contains("build")
            || lower.contains("compile")
            || lower.contains("lint"))
    {
        return true;
    }
    contains_numbered_errors(&lower)
}

fn contains_numbered_errors(lower: &str) -> bool {
    let mut tokens = lower.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        let numeric = token
            .trim_matches(|c: char| !c.is_ascii_digit())
            .parse::<usize>()
            .ok();
        if numeric.is_none() {
            continue;
        }
        let Some(next) = tokens.peek() else {
            break;
        };
        let next_clean = next.trim_matches(|c: char| !c.is_ascii_alphabetic());
        if next_clean == "error" || next_clean == "errors" {
            return true;
        }
    }
    false
}

fn is_debug_loop_session(turns: &[SessionTurn]) -> bool {
    const LOOP_THRESHOLD: i64 = 5;
    let mut signature_counts: HashMap<String, i64> = HashMap::new();
    let mut previous_signature: Option<String> = None;
    for turn in turns {
        if turn.user_text.trim().is_empty() || turn.assistant_text.trim().is_empty() {
            continue;
        }
        if let Some(signature) =
            extract_error_signature(&turn.user_text, previous_signature.as_deref())
        {
            let count = signature_counts.entry(signature.clone()).or_insert(0);
            *count += 1;
            if *count >= LOOP_THRESHOLD {
                return true;
            }
            previous_signature = Some(signature);
        } else {
            previous_signature = None;
        }
    }
    false
}

fn extract_error_signature(user_text: &str, previous_signature: Option<&str>) -> Option<String> {
    let lower = user_text.to_ascii_lowercase();
    if is_error_continuation(&lower)
        && let Some(prev) = previous_signature
    {
        return Some(prev.to_string());
    }
    if !contains_debug_keyword(&lower) {
        return None;
    }
    if let Some(line) = first_error_line(user_text) {
        let signature = normalize_signature(line);
        if !signature.is_empty() {
            return Some(signature);
        }
    }
    let signature = normalize_signature(user_text);
    if signature.is_empty() {
        None
    } else {
        Some(signature)
    }
}

fn contains_debug_keyword(text_lower: &str) -> bool {
    const KEYWORDS: [&str; 20] = [
        " error ",
        "error:",
        "exception",
        "traceback",
        "stack trace",
        "undefined",
        "not found",
        "failed",
        "failure",
        "cannot",
        "can't",
        "panic",
        "crash",
        "typeerror",
        "referenceerror",
        "syntaxerror",
        "runtimeerror",
        "build failed",
        "test failed",
        "compile error",
    ];
    let padded = format!(" {} ", text_lower);
    KEYWORDS.iter().any(|kw| padded.contains(kw))
}

fn is_error_continuation(text_lower: &str) -> bool {
    const PHRASES: [&str; 11] = [
        "same error",
        "same issue",
        "still failing",
        "still fails",
        "still broken",
        "not fixed",
        "didn't work",
        "didnt work",
        "doesn't work",
        "doesnt work",
        "still not working",
    ];
    PHRASES.iter().any(|phrase| text_lower.contains(phrase))
}

fn first_error_line(text: &str) -> Option<&str> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if contains_debug_keyword(&trimmed.to_ascii_lowercase()) {
            return Some(trimmed);
        }
    }
    None
}

fn normalize_signature(text: &str) -> String {
    let mut tokens = Vec::new();
    for raw in text.split_whitespace() {
        let trimmed = raw.trim_matches(|c: char| {
            !c.is_ascii_alphanumeric()
                && c != '/'
                && c != '\\'
                && c != '_'
                && c != '.'
                && c != ':'
                && c != '-'
        });
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        let normalized = if lower.starts_with("http://") || lower.starts_with("https://") {
            "<url>".to_string()
        } else if lower.contains('/') || lower.contains('\\') {
            "<path>".to_string()
        } else if looks_like_hex_hash(&lower) {
            "<sha>".to_string()
        } else {
            collapse_digits(&lower)
        };
        tokens.push(normalized);
        if tokens.len() >= 18 {
            break;
        }
    }
    tokens.join(" ")
}

fn collapse_digits(token: &str) -> String {
    let mut out = String::with_capacity(token.len());
    let mut in_digits = false;
    for ch in token.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push('#');
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(ch);
        }
    }
    out
}

fn looks_like_hex_hash(token: &str) -> bool {
    let len = token.len();
    (8..=64).contains(&len) && token.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn shorten_repo(path: &str) -> String {
    if let Some(home) = dirs::home_dir()
        && let Some(shortened) = strip_home_prefix(path, &home)
    {
        return shortened;
    }
    path.to_string()
}

fn strip_home_prefix(path: &str, home: &Path) -> Option<String> {
    let path_obj = Path::new(path);
    if let Ok(stripped) = path_obj.strip_prefix(home) {
        if stripped.as_os_str().is_empty() {
            return Some("~".to_string());
        }
        return Some(format!("~/{}", stripped.display()));
    }

    let normalized_home = std::fs::canonicalize(home).ok()?;
    let normalized_path = std::fs::canonicalize(path_obj).ok()?;
    let stripped = normalized_path.strip_prefix(&normalized_home).ok()?;
    if stripped.as_os_str().is_empty() {
        return Some("~".to_string());
    }
    Some(format!("~/{}", stripped.display()))
}

fn format_elapsed(elapsed_ms: u128) -> String {
    format!("{:.1}s", elapsed_ms as f64 / 1000.0)
}

fn resolve_mainline_ref(repo_root: &str) -> Result<Option<String>> {
    let candidates = [
        "refs/heads/main",
        "refs/heads/master",
        "refs/remotes/origin/main",
        "refs/remotes/origin/master",
    ];
    for candidate in candidates {
        if git_ref_exists(repo_root, candidate)? {
            return Ok(Some(candidate.to_string()));
        }
    }
    Ok(None)
}

fn git_ref_exists(repo_root: &str, reference: &str) -> Result<bool> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("show-ref")
        .arg("--verify")
        .arg("--quiet")
        .arg(reference)
        .status()?;
    Ok(status.success())
}

fn load_merge_commit_set(repo_root: &str) -> Result<HashSet<String>> {
    let out = run_git_capture(
        repo_root,
        &[
            "rev-list".to_string(),
            "--min-parents=2".to_string(),
            "--all".to_string(),
        ],
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn load_ref_commit_set(repo_root: &str, reference: &str) -> Result<HashSet<String>> {
    let out = run_git_capture(repo_root, &["rev-list".to_string(), reference.to_string()])?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn build_revert_map(repo_root: &str) -> Result<HashMap<String, DateTime<Utc>>> {
    let out = run_git_capture(
        repo_root,
        &[
            "log".to_string(),
            "--all".to_string(),
            "--pretty=format:%H%x1f%cI%x1f%B%x1e".to_string(),
        ],
    )?;
    let mut out_map = HashMap::new();
    for record in out.split('\u{1e}') {
        let rec = record.trim();
        if rec.is_empty() {
            continue;
        }
        let mut parts = rec.splitn(3, '\u{1f}');
        let _sha = parts.next();
        let commit_time_raw = parts.next();
        let body = parts.next();
        let (Some(commit_time_raw), Some(body)) = (commit_time_raw, body) else {
            continue;
        };
        let commit_time = match DateTime::parse_from_rfc3339(commit_time_raw) {
            Ok(value) => value.with_timezone(&Utc),
            Err(_) => continue,
        };
        for target in extract_reverted_shas(body) {
            out_map
                .entry(target)
                .and_modify(|existing| {
                    if commit_time < *existing {
                        *existing = commit_time;
                    }
                })
                .or_insert(commit_time);
        }
    }
    Ok(out_map)
}

fn extract_reverted_shas(body: &str) -> Vec<String> {
    let needle = "This reverts commit ";
    let mut out = Vec::new();
    for line in body.lines() {
        let Some(start) = line.find(needle) else {
            continue;
        };
        let suffix = &line[start + needle.len()..];
        let sha: String = suffix
            .chars()
            .take_while(|ch| ch.is_ascii_hexdigit())
            .collect();
        if sha.len() == 40 {
            out.push(sha.to_lowercase());
        }
    }
    out
}

fn load_repo_commit_messages(repo_root: &str) -> Result<HashMap<String, String>> {
    let out = run_git_capture(
        repo_root,
        &[
            "log".to_string(),
            "--all".to_string(),
            "--pretty=format:%H%x1f%B%x1e".to_string(),
        ],
    )?;
    let mut messages = HashMap::new();
    for record in out.split('\u{1e}') {
        let rec = record.trim();
        if rec.is_empty() {
            continue;
        }
        let mut parts = rec.splitn(2, '\u{1f}');
        let Some(commit_sha) = parts.next() else {
            continue;
        };
        let Some(message) = parts.next() else {
            continue;
        };
        messages.insert(commit_sha.to_string(), message.trim().to_string());
    }
    Ok(messages)
}

fn is_fix_like_commit_message(message: &str) -> bool {
    static FIX_KEYWORD_RE: OnceLock<Regex> = OnceLock::new();
    let regex = FIX_KEYWORD_RE.get_or_init(|| {
        Regex::new(r"(?i)\b(fix|fixes|fixed|bug|bugs|hotfix|hotfixes|patch|patches|broken|regression|regressions)\b")
            .expect("valid fix keyword regex")
    });
    regex.is_match(message)
}

fn is_revert_commit_message(message: &str) -> bool {
    let trimmed = message.trim_start();
    trimmed.starts_with("Revert ")
        || trimmed.starts_with("revert ")
        || trimmed.contains("This reverts commit ")
}

fn load_issue_linked_bug_signal_candidates(
    conn: &Connection,
    repo_key: &str,
) -> Result<Vec<IssueLinkedBugSignalCandidate>> {
    let mut stmt = conn.prepare(
        "SELECT i.issue_number, pr.merged_at, ipr.pr_number, h.rel_path, h.line_hash, h.count
         FROM fact_github_issue i
         JOIN fact_github_issue_fix_pull_request ipr
           ON ipr.repo_key = i.repo_key
          AND ipr.issue_number = i.issue_number
         JOIN fact_github_pull_request pr
           ON pr.repo_key = ipr.repo_key
          AND pr.pr_number = ipr.pr_number
         JOIN fact_github_pull_request_removed_line_hash h
           ON h.repo_key = pr.repo_key
          AND h.pr_number = pr.pr_number
         WHERE i.repo_key = ?1
           AND i.bug_candidate_flag = 1
           AND i.is_pull_request_flag = 0
           AND pr.merged_at IS NOT NULL
           AND pr.removed_hashes_complete_flag = 1
         ORDER BY i.issue_number, ipr.pr_number, h.rel_path, h.line_hash",
    )?;
    let rows = stmt.query_map(params![repo_key], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;

    let mut grouped = BTreeMap::<(i64, i64), IssueLinkedBugSignalCandidate>::new();
    for row in rows {
        let (issue_number, merged_at_raw, pr_number, rel_path, line_hash, count) = row?;
        let Some(merged_at_raw) = merged_at_raw else {
            continue;
        };
        let merged_at = DateTime::parse_from_rfc3339(&merged_at_raw)
            .map_err(|e| anyhow!("invalid pr merged_at '{}': {}", merged_at_raw, e))?
            .with_timezone(&Utc);

        let entry = grouped.entry((issue_number, pr_number)).or_insert_with(|| {
            IssueLinkedBugSignalCandidate {
                signal_ref: format!("issue#{issue_number}/pr#{pr_number}"),
                signal_time: merged_at,
                window_anchor_time: merged_at,
                removed_hashes: PathHashCounts::new(),
            }
        });
        entry
            .removed_hashes
            .entry(rel_path)
            .or_default()
            .entry(line_hash)
            .and_modify(|value| *value += count)
            .or_insert(count);
    }

    Ok(grouped.into_values().collect())
}

fn load_commit_added_hashes(conn: &Connection, repo_root: &str) -> Result<CommitPathHashCounts> {
    let mut stmt = conn.prepare(
        "SELECT f.commit_sha, f.rel_path, h.line_hash, h.count
         FROM fact_commit_file_change f
         JOIN fact_commit_file_change_line_hashes h ON h.file_change_id = f.id
         WHERE f.repo_root = ?1
           AND h.side = '+'",
    )?;
    let rows = stmt.query_map(params![repo_root], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;

    let mut out = CommitPathHashCounts::new();
    for row in rows {
        let (commit_sha, rel_path, line_hash, count) = row?;
        out.entry(commit_sha)
            .or_default()
            .entry(rel_path)
            .or_default()
            .entry(line_hash)
            .and_modify(|value| *value += count)
            .or_insert(count);
    }
    Ok(out)
}

fn load_commit_removed_hashes(conn: &Connection, repo_root: &str) -> Result<CommitPathHashCounts> {
    let mut stmt = conn.prepare(
        "SELECT f.commit_sha, f.rel_path, h.line_hash, h.count
         FROM fact_commit_file_change f
         JOIN fact_commit_file_change_line_hashes h ON h.file_change_id = f.id
         WHERE f.repo_root = ?1
           AND h.side = '-'",
    )?;
    let rows = stmt.query_map(params![repo_root], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;

    let mut out = CommitPathHashCounts::new();
    for row in rows {
        let (commit_sha, rel_path, line_hash, count) = row?;
        out.entry(commit_sha)
            .or_default()
            .entry(rel_path)
            .or_default()
            .entry(line_hash)
            .and_modify(|value| *value += count)
            .or_insert(count);
    }
    Ok(out)
}

fn load_session_added_availability(conn: &Connection, repo_root: &str) -> Result<PathHashCounts> {
    let mut stmt = conn.prepare(
        "SELECT rel_path, line_hash, SUM(provider_max) AS avail_total
         FROM (
            SELECT co.rel_path AS rel_path, hol.line_hash AS line_hash, co.provider AS provider, MAX(hol.count) AS provider_max
            FROM fact_session_code_change co
            JOIN fact_session_code_change_line_hashes hol ON hol.code_change_id = co.id
            WHERE co.repo_root = ?1
              AND co.rel_path IS NOT NULL
              AND hol.side = '+'
            GROUP BY co.rel_path, hol.line_hash, co.provider
         )
         GROUP BY rel_path, line_hash",
    )?;
    let rows = stmt.query_map(params![repo_root], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    let mut out = PathHashCounts::new();
    for row in rows {
        let (rel_path, line_hash, count) = row?;
        out.entry(rel_path).or_default().insert(line_hash, count);
    }
    Ok(out)
}

fn build_ai_added_budget_from_preloaded(
    commit_added_hashes: &CommitPathHashCounts,
    session_added_availability: &PathHashCounts,
    commit_sha: &str,
) -> PathHashCounts {
    let Some(commit_hashes) = commit_added_hashes.get(commit_sha) else {
        return PathHashCounts::new();
    };

    let mut out = PathHashCounts::new();
    for (rel_path, hashes) in commit_hashes {
        let Some(avail_hashes) = session_added_availability.get(rel_path) else {
            continue;
        };
        for (line_hash, commit_count) in hashes {
            let budget = (*commit_count).min(*avail_hashes.get(line_hash).unwrap_or(&0));
            if budget > 0 {
                out.entry(rel_path.clone())
                    .or_default()
                    .entry(line_hash.clone())
                    .and_modify(|value| *value += budget)
                    .or_insert(budget);
            }
        }
    }
    out
}

fn budget_total(budget: &HashMap<String, HashMap<String, i64>>) -> i64 {
    budget
        .values()
        .flat_map(|inner| inner.values())
        .copied()
        .sum()
}

fn is_content_merged_in_index(budget: &PathHashCounts, index: &MainlineIndex) -> bool {
    let total_budget = budget_total(budget);
    if total_budget <= 0 {
        return false;
    }
    let matched = match_budget_to_mainline(budget, index);
    let ratio = matched as f64 / total_budget as f64;
    matched >= C2_MIN_MATCHED_LINES && ratio >= C2_MIN_RATIO
}

fn load_mainline_hash_events(
    repo_root: &str,
    main_ref: &str,
    since: &DateTime<Utc>,
    until: &DateTime<Utc>,
) -> Result<(Vec<TimedLineHashChange>, Vec<TimedLineHashChange>)> {
    let since_iso = since.to_rfc3339_opts(SecondsFormat::Millis, true);
    let until_iso = until.to_rfc3339_opts(SecondsFormat::Millis, true);
    let commits = list_commits_on_ref(repo_root, main_ref, Some(&since_iso), Some(&until_iso))?;
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for sha in commits {
        let diff = load_commit_diff(repo_root, &sha)?;
        let commit_time = DateTime::parse_from_rfc3339(&diff.commit_time)
            .map_err(|e| anyhow!("invalid mainline commit time '{}': {}", diff.commit_time, e))?
            .with_timezone(&Utc);
        for file in diff.file_diffs {
            for line_hash in file.line_hashes {
                let event = TimedLineHashChange {
                    commit_time,
                    rel_path: file.rel_path.clone(),
                    line_hash: line_hash.line_hash.clone(),
                    count: line_hash.count,
                };
                match line_hash.side {
                    LineSide::Added => added.push(event),
                    LineSide::Removed => removed.push(event),
                }
            }
        }
    }
    added.sort_by(|a, b| {
        b.commit_time
            .cmp(&a.commit_time)
            .then_with(|| b.rel_path.cmp(&a.rel_path))
    });
    removed.sort_by(|a, b| {
        a.commit_time
            .cmp(&b.commit_time)
            .then_with(|| a.rel_path.cmp(&b.rel_path))
    });
    Ok((added, removed))
}

fn compute_churn(
    budget: &HashMap<String, HashMap<String, i64>>,
    removed_index: &HashMap<(String, String), i64>,
) -> i64 {
    let mut churn = 0i64;
    for (path, hashes) in budget {
        for (line_hash, count) in hashes {
            let removed = removed_index
                .get(&(path.clone(), line_hash.clone()))
                .copied()
                .unwrap_or(0);
            churn += (*count).min(removed);
        }
    }
    churn
}

fn exact_hash_overlap_count(left: &PathHashCounts, right: &PathHashCounts) -> i64 {
    let mut total = 0i64;
    for (path, left_hashes) in left {
        let Some(right_hashes) = right.get(path) else {
            continue;
        };
        for (line_hash, left_count) in left_hashes {
            let right_count = right_hashes.get(line_hash).copied().unwrap_or(0);
            total += (*left_count).min(right_count);
        }
    }
    total
}

fn match_budget_to_mainline(
    budget: &HashMap<String, HashMap<String, i64>>,
    index: &MainlineIndex,
) -> i64 {
    let mut matched_total = 0i64;
    for (path, hashes) in budget {
        let file_total: i64 = hashes.values().copied().sum();
        if file_total <= 0 {
            continue;
        }

        let mut strict = 0i64;
        for (line_hash, budget_count) in hashes {
            strict += index.strict_match(path, line_hash, *budget_count);
        }
        let strict_ratio = strict as f64 / file_total as f64;
        let mut selected = strict;

        if strict_ratio < C2_STRICT_WEAK_RATIO
            && let Some(alias_path) = choose_alias_path(path, hashes, file_total, index)
        {
            let mut alias_matched = 0i64;
            for (line_hash, budget_count) in hashes {
                alias_matched += index.strict_match(&alias_path, line_hash, *budget_count);
            }
            if alias_matched > selected {
                selected = alias_matched;
            }
        }

        matched_total += selected;
    }
    matched_total
}

fn choose_alias_path(
    strict_path: &str,
    hashes: &HashMap<String, i64>,
    file_total: i64,
    index: &MainlineIndex,
) -> Option<String> {
    let mut scores = HashMap::new();
    for (line_hash, budget_count) in hashes {
        if let Some(paths) = index.by_hash_paths.get(line_hash) {
            for (candidate_path, candidate_count) in paths {
                if candidate_path == strict_path {
                    continue;
                }
                let matched = (*budget_count).min(*candidate_count);
                if matched > 0 {
                    *scores.entry(candidate_path.clone()).or_insert(0) += matched;
                }
            }
        }
    }

    if scores.is_empty() {
        return None;
    }

    let mut candidates: Vec<(String, i64)> = scores.into_iter().collect();
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let strict_base = basename(strict_path).to_string();
    let mut filename_candidates: Vec<(String, i64)> = candidates
        .iter()
        .filter(|(path, _)| basename(path) == strict_base)
        .cloned()
        .collect();
    filename_candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    if let Some(path) = pick_confident_alias(&filename_candidates, file_total) {
        return Some(path);
    }
    pick_confident_alias(&candidates, file_total)
}

fn pick_confident_alias(candidates: &[(String, i64)], file_total: i64) -> Option<String> {
    if candidates.is_empty() || file_total <= 0 {
        return None;
    }
    let (winner_path, winner_matched) = &candidates[0];
    let runner_matched = candidates.get(1).map(|(_, matched)| *matched).unwrap_or(0);
    let winner_ratio = *winner_matched as f64 / file_total as f64;
    let runner_ratio = runner_matched as f64 / file_total as f64;
    if winner_ratio >= C2_MIN_RATIO
        && *winner_matched >= C2_MIN_MATCHED_LINES
        && (winner_ratio - runner_ratio) >= C2_WINNER_MARGIN
    {
        Some(winner_path.clone())
    } else {
        None
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn list_commits_on_ref(
    repo_root: &str,
    reference: &str,
    since: Option<&str>,
    until: Option<&str>,
) -> Result<Vec<String>> {
    let mut args = vec!["rev-list".to_string(), "--reverse".to_string()];
    if let Some(since) = since {
        args.push(format!("--since={since}"));
    }
    if let Some(until) = until {
        args.push(format!("--until={until}"));
    }
    args.push(reference.to_string());
    let out = run_git_capture(repo_root, &args)?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn run_git_capture(repo_root: &str, args: &[String]) -> Result<String> {
    let output = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=false")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn looks_like_task_id(task_key: &str) -> bool {
    let bytes = task_key.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_uppercase() {
        i += 1;
    }
    if i == 0 || i >= bytes.len() || bytes[i] != b'-' {
        return false;
    }

    let mut j = i + 1;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }

    j > i + 1 && j == bytes.len()
}

fn is_integration_branch(branch_name: &str) -> bool {
    matches!(
        branch_name,
        "staging" | "main" | "master" | "develop" | "(unknown)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_intel::line_hash::hash_line;
    use crate::db::{upsert_metadata_session, upsert_metadata_session_with_model};

    fn open_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        Ok(conn)
    }

    #[test]
    fn task_key_sort_key_orders_numbers_naturally_not_lexicographically() {
        let mut keys = vec!["PAC-100", "PAC-9", "PAC-10", "PAC-1", "PAC-583", "PAC-95"];
        keys.sort_by_key(|k| task_key_sort_key(Some(k)));
        assert_eq!(
            keys,
            vec!["PAC-1", "PAC-9", "PAC-10", "PAC-95", "PAC-100", "PAC-583"]
        );
    }

    #[test]
    fn task_key_sort_key_groups_prefixes_together_and_uppercases() {
        let mut keys = vec!["pac-10", "ABC-5", "PAC-2", "abc-100"];
        keys.sort_by_key(|k| task_key_sort_key(Some(k)));
        assert_eq!(keys, vec!["ABC-5", "abc-100", "PAC-2", "pac-10"]);
    }

    #[test]
    fn task_key_sort_key_pushes_unparseable_keys_to_the_end() {
        let mut keys = vec!["staging", "PAC-10", "(unknown)", "PAC-1"];
        keys.sort_by_key(|k| task_key_sort_key(Some(k)));
        assert_eq!(keys, vec!["PAC-1", "PAC-10", "(unknown)", "staging"]);
    }

    #[test]
    fn task_key_sort_key_handles_underscore_separator() {
        let mut keys = vec!["PAC_837", "PAC-10", "PAC_9"];
        keys.sort_by_key(|k| task_key_sort_key(Some(k)));
        assert_eq!(keys, vec!["PAC_9", "PAC-10", "PAC_837"]);
    }

    #[test]
    fn reporting_view_sql_depends_only_on_event_tables() {
        let sql = REPORTING_VIEWS_SQL.to_lowercase();
        assert!(
            !sql.contains("fact_"),
            "reporting views should not reference fact tables"
        );
        assert!(
            !sql.contains("metadata_"),
            "reporting views should not reference metadata tables"
        );
    }

    fn insert_commit_file_hashes(
        conn: &Connection,
        repo_root: &str,
        commit_sha: &str,
        rel_path: &str,
        hashes: &[(&str, i64)],
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO fact_commit_file_change (
                repo_root, commit_sha, rel_path, change_type, added_lines, removed_lines
             ) VALUES (?1, ?2, ?3, 'modify', 0, 0)",
            params![repo_root, commit_sha, rel_path],
        )?;
        let file_change_id = conn.last_insert_rowid();
        for (line_hash, count) in hashes {
            conn.execute(
                "INSERT INTO fact_commit_file_change_line_hashes (file_change_id, side, line_hash, count)
                 VALUES (?1, '+', ?2, ?3)",
                params![file_change_id, line_hash, count],
            )?;
        }
        Ok(())
    }

    fn insert_session_change_hashes(
        conn: &Connection,
        provider: &str,
        session_id: &str,
        repo_root: &str,
        rel_path: &str,
        hashes: &[(&str, i64)],
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO fact_session_code_change (
                provider, session_id, source_kind, repo_root, rel_path, lines_added, lines_removed
             ) VALUES (?1, ?2, 'test', ?3, ?4, 0, 0)",
            params![provider, session_id, repo_root, rel_path],
        )?;
        let code_change_id = conn.last_insert_rowid();
        for (line_hash, count) in hashes {
            conn.execute(
                "INSERT INTO fact_session_code_change_line_hashes (code_change_id, side, line_hash, count)
                 VALUES (?1, '+', ?2, ?3)",
                params![code_change_id, line_hash, count],
            )?;
        }
        Ok(())
    }

    fn budget_reference(
        conn: &Connection,
        repo_root: &str,
        commit_sha: &str,
    ) -> Result<PathHashCounts> {
        let mut commit_stmt = conn.prepare(
            "SELECT f.rel_path, h.line_hash, h.count
             FROM fact_commit_file_change f
             JOIN fact_commit_file_change_line_hashes h ON h.file_change_id = f.id
             WHERE f.repo_root = ?1 AND f.commit_sha = ?2 AND h.side = '+'",
        )?;
        let mut avail_stmt = conn.prepare(
            "SELECT MAX(hol.count) AS avail
             FROM fact_session_code_change co
             JOIN fact_session_code_change_line_hashes hol ON hol.code_change_id = co.id
             WHERE co.repo_root = ?1
               AND co.rel_path = ?2
               AND hol.side = '+'
               AND hol.line_hash = ?3
             GROUP BY co.provider",
        )?;

        let rows = commit_stmt.query_map(params![repo_root, commit_sha], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut out = PathHashCounts::new();
        for row in rows {
            let (rel_path, line_hash, commit_count) = row?;
            let avail_rows = avail_stmt
                .query_map(params![repo_root, rel_path, line_hash], |r| {
                    r.get::<_, i64>(0)
                })?;
            let mut avail_total = 0i64;
            for avail in avail_rows {
                avail_total += avail?;
            }
            let budget = commit_count.min(avail_total);
            if budget > 0 {
                out.entry(rel_path)
                    .or_default()
                    .entry(line_hash)
                    .and_modify(|value| *value += budget)
                    .or_insert(budget);
            }
        }
        Ok(out)
    }

    fn parse_ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn derived_event(merged_to_mainline: bool) -> DerivedCommitEvent {
        DerivedCommitEvent {
            reverted_later: false,
            merged_to_mainline,
            budget: PathHashCounts::new(),
            ai_added_lines_reaching_mainline: 0,
            ai_added_lines_removed_within_window: 0,
            bug_after_merge: false,
            first_bug_signal_commit_sha: None,
            first_bug_signal_commit_time: None,
            bug_signal_count: 0,
            bug_signal_sources: BTreeSet::new(),
        }
    }

    #[test]
    fn session_event_materialization_detects_debug_loop_and_mid_session_error() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session(
            &conn,
            "codex",
            "s1",
            None,
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
        )?;

        let user_messages = [
            "Need help with parser",
            "TypeError: parser exploded at line 12",
            "same error",
            "still failing",
            "doesnt work, same error",
            "TypeError: parser exploded at line 12",
        ];
        for (index, text) in user_messages.iter().enumerate() {
            conn.execute(
                "INSERT INTO fact_session_message (provider, session_id, message_index, role, content, content_words)
                 VALUES ('codex', 's1', ?1, 'user', ?2, 3)",
                params![(index as i64) * 2, text],
            )?;
            conn.execute(
                "INSERT INTO fact_session_message (provider, session_id, message_index, role, content, content_words)
                 VALUES ('codex', 's1', ?1, 'assistant', 'try again', 2)",
                params![(index as i64) * 2 + 1],
            )?;
        }

        refresh_session_events(&conn)?;

        let row: (i64, i64, i64) = conn.query_row(
            "SELECT user_turn_count, debug_loop_flag, mid_session_error_paste_flag
             FROM event_session_quality
             WHERE provider = 'codex' AND session_id = 's1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(row, (6, 1, 1));
        Ok(())
    }

    #[test]
    fn reporting_views_aggregate_task_metrics() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        upsert_metadata_session(
            &conn,
            "cursor",
            "s2",
            Some("/tmp/repo"),
            Some("2026-03-17T09:10:00Z"),
            Some("2026-03-17T09:45:00Z"),
            None,
        )?;
        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, task_key, branch_name, provider, session_id, attribution_weight,
                commit_within_window_flag, user_turn_count, debug_loop_flag, mid_session_error_paste_flag
             ) VALUES
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'codex', 's1', 2.0, 1, 4, 0, 0),
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'cursor', 's2', 1.0, 0, 10, 1, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time)
             VALUES ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'abc', 0, 1.0, '2026-03-17T10:00:00Z')",
            [],
        )?;

        create_reporting_views(&conn)?;
        let row = query_session_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: Some(GroupBy::Task),
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: Some("PAC-1".to_string()),
                branch: None,
                model: None,
                limit: 50,
            },
        )?;
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].session_count, 2);
        assert_eq!(row[0].s2_avg, Some(6.0));
        assert_eq!(row[0].debug_loop_rate.numerator, 1);
        assert_eq!(row[0].debug_loop_rate.denominator, 3);
        assert_eq!(row[0].s6_rate.numerator, 1);
        assert_eq!(row[0].s6_rate.denominator, 3);
        assert_eq!(row[0].s9_rate.numerator, 2);
        assert_eq!(row[0].s9_rate.denominator, 3);
        assert_eq!(row[0].avg_minutes_to_first_accepted_change, None);
        assert_eq!(row[0].no_output_session_rate.numerator, 3);
        assert_eq!(row[0].no_output_session_rate.denominator, 3);
        Ok(())
    }

    #[test]
    fn task_grouped_change_and_lifecycle_reports_apply_limit_after_task_filtering() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', '(unknown)', '(unknown)', 'junk1', 1, 0.5, '2026-03-17T09:00:00Z'),
                ('/tmp/repo', 'main', 'main', 'junk2', 1, 0.5, '2026-03-17T09:01:00Z'),
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'good1', 0, 1.0, '2026-03-17T09:02:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'junk1', '2026-03-17T09:00:00Z', 0, 0, 0, 0, 10),
                ('/tmp/repo', 'junk2', '2026-03-17T09:01:00Z', 0, 1, 0, 0, 12),
                ('/tmp/repo', 'good1', '2026-03-17T09:02:00Z', 1, 1, 0, 8, 16)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'junk1', 0, 0, 14),
                ('/tmp/repo', 'junk2', 0, 0, 14),
                ('/tmp/repo', 'good1', 10, 2, 14)",
            [],
        )?;

        create_reporting_views(&conn)?;

        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Task),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 1,
        };

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(change_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(change_rows[0].branch_name.as_deref(), Some("PAC-1-branch"));

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(lifecycle_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(
            lifecycle_rows[0].branch_name.as_deref(),
            Some("PAC-1-branch")
        );
        Ok(())
    }

    #[test]
    fn task_grouped_change_and_lifecycle_reports_can_filter_by_provider() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'claude1', 0, 1.0, '2026-03-17T09:02:00Z'),
                ('/tmp/repo', 'PAC-2', 'PAC-2-branch', 'codex1', 0, 1.0, '2026-03-17T09:03:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'claude1', '2026-03-17T09:02:00Z', 1, 1, 0, 8, 16),
                ('/tmp/repo', 'codex1', '2026-03-17T09:03:00Z', 1, 0, 0, 12, 18)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'claude1', 10, 2, 14),
                ('/tmp/repo', 'codex1', 6, 1, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES
                ('/tmp/repo', 'claude1', 'claude', 's-claude', '2026-03-17T09:02:00Z', 'claude-opus-4-6', 8.0, 0.5, 1.0),
                ('/tmp/repo', 'codex1', 'codex', 's-codex', '2026-03-17T09:03:00Z', 'gpt-5.4', 12.0, 0.67, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Task),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: Some("claude".to_string()),
            task: None,
            branch: None,
            model: None,
            limit: 10,
        };

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(change_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(change_rows[0].commit_count, 1);

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(lifecycle_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(lifecycle_rows[0].heavy_commit_count, 1);
        Ok(())
    }

    #[test]
    fn task_grouped_change_and_lifecycle_reports_can_filter_by_model() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'claude1', 0, 1.0, '2026-03-17T09:02:00Z'),
                ('/tmp/repo', 'PAC-2', 'PAC-2-branch', 'claude2', 0, 1.0, '2026-03-17T09:03:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'claude1', '2026-03-17T09:02:00Z', 1, 1, 0, 8, 16),
                ('/tmp/repo', 'claude2', '2026-03-17T09:03:00Z', 1, 1, 0, 7, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'claude1', 10, 2, 14),
                ('/tmp/repo', 'claude2', 9, 1, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES
                ('/tmp/repo', 'claude1', 'claude', 's-claude-1', '2026-03-17T09:02:00Z', 'claude-opus-4-6', 8.0, 0.5, 1.0),
                ('/tmp/repo', 'claude2', 'claude', 's-claude-2', '2026-03-17T09:03:00Z', 'claude-sonnet-4-5-20250929', 7.0, 0.5, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Task),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: Some("claude/claude-opus-4-6".to_string()),
            limit: 10,
        };

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(change_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(change_rows[0].commit_count, 1);

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(lifecycle_rows[0].group_value.as_deref(), Some("PAC-1"));
        assert_eq!(lifecycle_rows[0].heavy_commit_count, 1);
        Ok(())
    }

    #[test]
    fn branch_grouped_change_and_lifecycle_reports_can_filter_by_provider() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'claude1', 0, 1.0, '2026-03-17T09:02:00Z'),
                ('/tmp/repo', 'fix/codex-flow', 'fix/codex-flow', 'codex1', 0, 1.0, '2026-03-17T09:03:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'claude1', '2026-03-17T09:02:00Z', 1, 1, 0, 8, 16),
                ('/tmp/repo', 'codex1', '2026-03-17T09:03:00Z', 1, 0, 0, 12, 18)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'claude1', 10, 2, 14),
                ('/tmp/repo', 'codex1', 6, 1, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES
                ('/tmp/repo', 'claude1', 'claude', 's-claude', '2026-03-17T09:02:00Z', 'claude-opus-4-6', 8.0, 0.5, 1.0),
                ('/tmp/repo', 'codex1', 'codex', 's-codex', '2026-03-17T09:03:00Z', 'gpt-5.4', 12.0, 0.67, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Branch),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: Some("claude".to_string()),
            task: None,
            branch: None,
            model: None,
            limit: 10,
        };

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(
            change_rows[0].group_value.as_deref(),
            Some("fix/claude-flow")
        );
        assert_eq!(change_rows[0].commit_count, 1);

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(
            lifecycle_rows[0].group_value.as_deref(),
            Some("fix/claude-flow")
        );
        assert_eq!(lifecycle_rows[0].heavy_commit_count, 1);
        Ok(())
    }

    #[test]
    fn branch_grouped_session_report_uses_weighted_attribution() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES
                ('claude', 's-heavy', '/tmp/repo', 'claude-opus-4-6', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 2, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0, 1),
                ('claude', 's-light', '/tmp/repo', 'claude-opus-4-6', '2026-03-17T09:01:00Z', '2026-03-17T09:31:00Z', 10, 0, 0, 1, '2026-03-17T09:06:00Z', 5.0, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'c-heavy', 0, 1.0, '2026-03-17T09:20:00Z'),
                ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'c-light', 0, 1.0, '2026-03-17T09:21:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES
                ('/tmp/repo', 'c-heavy', 'claude', 's-heavy', '2026-03-17T09:20:00Z', 'claude-opus-4-6', 10.0, 0.5, 1.0),
                ('/tmp/repo', 'c-light', 'claude', 's-light', '2026-03-17T09:21:00Z', 'claude-opus-4-6', 1.0, 0.5, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: Some(GroupBy::Branch),
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: Some("claude".to_string()),
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].group_value.as_deref(), Some("fix/claude-flow"));
        assert_eq!(rows[0].session_count, 2);
        assert!(
            rows[0]
                .s2_avg
                .map(|value| (value - (30.0 / 11.0)).abs() < 0.001)
                .unwrap_or(false)
        );
        Ok(())
    }

    #[test]
    fn branch_filters_apply_across_reports_with_provider_and_model() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES
                ('claude', 's-claude', '/tmp/repo', 'claude-opus-4-6', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 3, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0, 1),
                ('codex', 's-codex', '/tmp/repo', 'gpt-5.4', '2026-03-17T09:10:00Z', '2026-03-17T09:40:00Z', 6, 1, 0, 1, '2026-03-17T09:15:00Z', 5.0, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'claude1', 0, 1.0, '2026-03-17T09:20:00Z'),
                ('/tmp/repo', 'fix/codex-flow', 'fix/codex-flow', 'codex1', 0, 1.0, '2026-03-17T09:21:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'claude1', '2026-03-17T09:20:00Z', 1, 1, 0, 8, 16),
                ('/tmp/repo', 'codex1', '2026-03-17T09:21:00Z', 1, 0, 0, 12, 20)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'claude1', 10, 2, 14),
                ('/tmp/repo', 'codex1', 8, 1, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES
                ('/tmp/repo', 'claude1', 'claude', 's-claude', '2026-03-17T09:20:00Z', 'claude-opus-4-6', 8.0, 0.5, 1.0),
                ('/tmp/repo', 'codex1', 'codex', 's-codex', '2026-03-17T09:21:00Z', 'gpt-5.4', 12.0, 0.6, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: None,
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: Some("claude".to_string()),
            task: None,
            branch: Some("fix/claude-flow".to_string()),
            model: Some("claude/claude-opus-4-6".to_string()),
            limit: 10,
        };

        let session_rows = query_session_report(&conn, &args)?;
        assert_eq!(session_rows.len(), 1);
        assert_eq!(session_rows[0].session_count, 1);

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(change_rows[0].commit_count, 1);

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(lifecycle_rows[0].heavy_commit_count, 1);
        Ok(())
    }

    #[test]
    fn task_grouped_reports_exclude_non_ticket_branches_even_when_branch_data_exists() -> Result<()>
    {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, task_key, branch_name, provider, session_id, model_name, started_at,
                attribution_weight, commit_within_window_flag, user_turn_count, debug_loop_flag,
                mid_session_error_paste_flag, accepted_output_flag, first_accepted_change_at,
                minutes_to_first_accepted_change
             ) VALUES ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'claude', 's1', 'claude-opus-4-6', '2026-03-17T09:00:00Z', 1.0, 1, 3, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES ('/tmp/repo', 'fix/claude-flow', 'fix/claude-flow', 'claude1', 0, 1.0, '2026-03-17T09:20:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'claude1', '2026-03-17T09:20:00Z', 1, 1, 0, 8, 16)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'claude1', 10, 2, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'claude1', 'claude', 's1', '2026-03-17T09:20:00Z', 'claude-opus-4-6', 8.0, 0.5, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Task),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: Some("claude".to_string()),
            task: None,
            branch: None,
            model: None,
            limit: 10,
        };

        assert!(query_session_report(&conn, &args)?.is_empty());
        assert!(query_change_report(&conn, &args)?.is_empty());
        assert!(query_lifecycle_report(&conn, &args)?.is_empty());
        Ok(())
    }

    #[test]
    fn lifecycle_report_computes_bug_after_merge_rate_from_event_signals() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'c1', '2026-03-17T09:00:00Z', 1, 1, 0, 20, 30),
                ('/tmp/repo', 'c2', '2026-03-18T09:00:00Z', 1, 1, 0, 18, 24),
                ('/tmp/repo', 'c3', '2026-03-19T09:00:00Z', 1, 0, 0, 18, 24)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'c1', 12, 2, 14),
                ('/tmp/repo', 'c2', 10, 1, 14),
                ('/tmp/repo', 'c3', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_bug_signal (
                repo_root, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
                first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
             ) VALUES
                ('/tmp/repo', 'c1', 1, 'fix1', '2026-03-20T10:00:00Z', 1, 60, 'git_fix_commit'),
                ('/tmp/repo', 'c2', 0, NULL, NULL, 0, 60, 'git_fix_commit'),
                ('/tmp/repo', 'c3', 1, 'fix2', '2026-03-21T10:00:00Z', 1, 60, 'git_fix_commit')",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_lifecycle_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].heavy_commit_count, 3);
        assert_eq!(rows[0].bug_after_merge_rate.numerator, 1);
        assert_eq!(rows[0].bug_after_merge_rate.denominator, 2);
        Ok(())
    }

    #[test]
    fn change_report_computes_github_pr_rates_when_lookup_is_complete() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c1', '2026-03-17T09:00:00Z', 1, 1, 0, 20, 30),
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c2', '2026-03-17T10:00:00Z', 1, 0, 0, 22, 28)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c1', 18, 1, 14),
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c2', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES
                ('git:github.com/PaceFlow/repo', 'c1', 'resolved', 11, '2026-03-17T11:00:00Z', NULL),
                ('git:github.com/PaceFlow/repo', 'c2', 'no_pr', NULL, '2026-03-17T11:00:00Z', NULL)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_pr_outcome (
                repo_root, repo_key, commit_sha, lookup_status, pr_number, pr_opened_flag,
                pr_merged_flag, pr_created_at, pr_merged_at
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c1', 'resolved', 11, 1, 1, '2026-03-17T08:00:00Z', '2026-03-18T08:00:00Z'),
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c2', 'no_pr', NULL, 0, 0, NULL, NULL)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_change_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert!(rows[0].github_pr_metrics_available);
        assert_eq!(rows[0].pr_reach_rate.numerator, 1);
        assert_eq!(rows[0].pr_reach_rate.denominator, 2);
        assert_eq!(rows[0].merge_rate.numerator, 1);
        assert_eq!(rows[0].merge_rate.denominator, 2);
        assert_eq!(rows[0].pr_merge_rate.numerator, 1);
        assert_eq!(rows[0].pr_merge_rate.denominator, 1);
        Ok(())
    }

    #[test]
    fn change_report_marks_github_pr_metrics_unavailable_when_lookup_is_incomplete() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c1', '2026-03-17T09:00:00Z', 1, 1, 0, 20, 30)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'c1', 18, 1, 14)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_change_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].github_pr_metrics_available);
        assert_eq!(rows[0].github_pr_heavy_eligible, 1);
        assert_eq!(rows[0].github_pr_heavy_ready, 0);
        assert_eq!(rows[0].pr_reach_rate.denominator, 0);
        assert_eq!(rows[0].pr_reach_rate.numerator, 0);
        Ok(())
    }

    #[test]
    fn session_report_computes_time_to_first_accepted_change_and_no_output_rate() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        upsert_metadata_session(
            &conn,
            "cursor",
            "s2",
            Some("/tmp/repo"),
            Some("2026-03-17T09:10:00Z"),
            Some("2026-03-17T09:45:00Z"),
            None,
        )?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES
                ('codex', 's1', '/tmp/repo', 'codex/gpt-5', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 4, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0, 0),
                ('cursor', 's2', '/tmp/repo', 'cursor/(unknown)', '2026-03-17T09:10:00Z', '2026-03-17T09:45:00Z', 2, 0, 0, 0, NULL, NULL, 0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 50,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert!(
            rows[0]
                .avg_minutes_to_first_accepted_change
                .map(|value| (value - 5.0).abs() < 0.001)
                .unwrap_or(false)
        );
        assert_eq!(rows[0].no_output_session_rate.numerator, 1);
        assert_eq!(rows[0].no_output_session_rate.denominator, 2);
        Ok(())
    }

    #[test]
    fn session_report_excludes_missing_change_timestamps_from_first_change_average() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
        )?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES ('codex', 's1', '/tmp/repo', 'codex/(unknown)', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 4, 0, 0, 1, NULL, NULL, 0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 50,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].avg_minutes_to_first_accepted_change, None);
        assert_eq!(rows[0].no_output_session_rate.numerator, 0);
        assert_eq!(rows[0].no_output_session_rate.denominator, 1);
        Ok(())
    }

    #[test]
    fn implicit_model_default_session_report_filters_small_models() -> Result<()> {
        let conn = open_test_db()?;
        for index in 0..5 {
            conn.execute(
                "INSERT INTO event_session_quality (
                    provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                    debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                    first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
                 ) VALUES (?1, ?2, '/tmp/repo', 'codex/gpt-5.4', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 3, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0, 1)",
                params!["codex", format!("high-{index}")],
            )?;
        }
        for index in 0..3 {
            conn.execute(
                "INSERT INTO event_session_quality (
                    provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                    debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                    first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
                 ) VALUES (?1, ?2, '/tmp/repo', 'codex/gpt-5.3-codex', '2026-03-18T09:00:00Z', '2026-03-18T09:30:00Z', 3, 0, 0, 1, '2026-03-18T09:07:00Z', 7.0, 1)",
                params!["codex", format!("mid-{index}")],
            )?;
        }
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES ('cursor', 'low-0', '/tmp/repo', 'cursor/default', '2026-03-19T09:00:00Z', '2026-03-19T09:30:00Z', 2, 0, 0, 1, '2026-03-19T09:03:00Z', 3.0, 0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Model),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let filtered = query_session_report_with_options(
            &conn,
            &args,
            ReportQueryOptions {
                implicit_model_default: true,
            },
        )?;
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].group_value.as_deref(), Some("codex/gpt-5.4"));
        assert_eq!(
            filtered[1].group_value.as_deref(),
            Some("codex/gpt-5.3-codex")
        );

        let explicit = query_session_report(&conn, &args)?;
        assert_eq!(explicit.len(), 3);
        assert!(
            explicit
                .iter()
                .any(|row| row.group_value.as_deref() == Some("cursor/default"))
        );
        Ok(())
    }

    #[test]
    fn session_list_rows_use_metadata_project_path_without_ambiguity() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        conn.execute(
            "INSERT INTO event_session_productivity (
                provider, session_id, repo_root, project_path, started_at, ended_at,
                accepted_lines_added, accepted_lines_removed, accepted_total_changed_lines, user_word_count
             ) VALUES ('codex', 's1', '/tmp/repo', '/tmp/repo', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 10, 2, 12, 100)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_list_rows(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].project_path, "/tmp/repo");
        assert_eq!(rows[0].last_active.as_deref(), Some("2026-03-17T09:30:00Z"));
        Ok(())
    }

    #[test]
    fn preloaded_budget_matches_reference_semantics() -> Result<()> {
        let conn = open_test_db()?;
        let repo_root = "/tmp/repo";
        insert_commit_file_hashes(
            &conn,
            repo_root,
            "c1",
            "src/lib.rs",
            &[("h1", 3), ("h2", 2)],
        )?;
        insert_session_change_hashes(
            &conn,
            "codex",
            "s1",
            repo_root,
            "src/lib.rs",
            &[("h1", 5), ("h2", 1)],
        )?;
        insert_session_change_hashes(&conn, "cursor", "s2", repo_root, "src/lib.rs", &[("h1", 2)])?;
        insert_session_change_hashes(
            &conn,
            "cursor",
            "s3",
            repo_root,
            "src/lib.rs",
            &[("h1", 1), ("h2", 4)],
        )?;

        let commit_added_hashes = load_commit_added_hashes(&conn, repo_root)?;
        let availability = load_session_added_availability(&conn, repo_root)?;
        let optimized =
            build_ai_added_budget_from_preloaded(&commit_added_hashes, &availability, "c1");
        let reference = budget_reference(&conn, repo_root, "c1")?;

        assert_eq!(optimized, reference);
        Ok(())
    }

    #[test]
    fn chronological_commit_derivation_matches_naive_reference() -> Result<()> {
        let commits = vec![
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "a".to_string(),
                commit_time: parse_ts("2026-03-01T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 10,
                commit_total_lines: 10,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "b".to_string(),
                commit_time: parse_ts("2026-03-05T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 10,
                commit_total_lines: 10,
            },
        ];
        let revert_map = HashMap::from([("a".to_string(), parse_ts("2026-03-08T10:00:00Z"))]);
        let merge_commit_set = HashSet::new();
        let mainline_commit_set = HashSet::from(["b".to_string()]);
        let commit_added_hashes = HashMap::from([
            (
                "a".to_string(),
                HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([("h1".to_string(), 20), ("h2".to_string(), 15)]),
                )]),
            ),
            (
                "b".to_string(),
                HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([("h1".to_string(), 35)]),
                )]),
            ),
        ]);
        let availability = HashMap::from([(
            "src/lib.rs".to_string(),
            HashMap::from([("h1".to_string(), 20), ("h2".to_string(), 15)]),
        )]);
        let mainline_added_events = vec![
            TimedLineHashChange {
                commit_time: parse_ts("2026-03-06T09:00:00Z"),
                rel_path: "src/lib.rs".to_string(),
                line_hash: "h1".to_string(),
                count: 20,
            },
            TimedLineHashChange {
                commit_time: parse_ts("2026-03-06T09:00:00Z"),
                rel_path: "src/lib.rs".to_string(),
                line_hash: "h2".to_string(),
                count: 15,
            },
        ];
        let mainline_removed_events = vec![
            TimedLineHashChange {
                commit_time: parse_ts("2026-03-07T09:00:00Z"),
                rel_path: "src/lib.rs".to_string(),
                line_hash: "h1".to_string(),
                count: 10,
            },
            TimedLineHashChange {
                commit_time: parse_ts("2026-03-10T09:00:00Z"),
                rel_path: "src/lib.rs".to_string(),
                line_hash: "h2".to_string(),
                count: 15,
            },
        ];

        let optimized = derive_commit_events_from_preloaded(
            &commits,
            &revert_map,
            &merge_commit_set,
            &mainline_commit_set,
            &commit_added_hashes,
            &availability,
            &mainline_added_events,
            &mainline_removed_events,
        )?;

        let commit_a = optimized.get("a").unwrap();
        assert!(commit_a.reverted_later);
        assert!(commit_a.merged_to_mainline);
        assert_eq!(commit_a.ai_added_lines_reaching_mainline, 35);
        assert_eq!(commit_a.ai_added_lines_removed_within_window, 25);

        let commit_b = optimized.get("b").unwrap();
        assert!(!commit_b.reverted_later);
        assert!(commit_b.merged_to_mainline);
        assert_eq!(commit_b.ai_added_lines_reaching_mainline, 20);
        assert_eq!(commit_b.ai_added_lines_removed_within_window, 10);
        Ok(())
    }

    #[test]
    fn bug_after_merge_detection_marks_later_fix_commit_with_file_overlap() {
        let commits = vec![
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "orig".to_string(),
                commit_time: parse_ts("2026-03-01T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 20,
                commit_total_lines: 20,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "fix".to_string(),
                commit_time: parse_ts("2026-03-10T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 0,
                commit_total_lines: 6,
            },
        ];
        let commit_messages = HashMap::from([
            ("orig".to_string(), "feat: add parser".to_string()),
            (
                "fix".to_string(),
                "fix: repair parser regression\n\nbroken edge case".to_string(),
            ),
        ]);
        let commit_removed_hashes = HashMap::from([(
            "fix".to_string(),
            HashMap::from([(
                "src/lib.rs".to_string(),
                HashMap::from([(hash_line("buggy();"), 1)]),
            )]),
        )]);
        let mut derived = HashMap::from([
            (
                "orig".to_string(),
                DerivedCommitEvent {
                    budget: HashMap::from([(
                        "src/lib.rs".to_string(),
                        HashMap::from([(hash_line("buggy();"), 1)]),
                    )]),
                    ..derived_event(true)
                },
            ),
            ("fix".to_string(), derived_event(false)),
        ]);

        annotate_bug_after_merge_signals_from_data(
            &commits,
            &commit_messages,
            &commit_removed_hashes,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(original.bug_after_merge);
        assert_eq!(original.first_bug_signal_commit_sha.as_deref(), Some("fix"));
        assert_eq!(
            original.first_bug_signal_commit_time,
            Some(parse_ts("2026-03-10T10:00:00Z"))
        );
        assert_eq!(original.bug_signal_count, 1);
    }

    #[test]
    fn bug_after_merge_detection_ignores_non_overlapping_late_and_revert_followups() {
        let commits = vec![
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "orig".to_string(),
                commit_time: parse_ts("2026-03-01T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 20,
                commit_total_lines: 20,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "late".to_string(),
                commit_time: parse_ts("2026-05-05T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 0,
                commit_total_lines: 3,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "other".to_string(),
                commit_time: parse_ts("2026-03-05T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 0,
                commit_total_lines: 3,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "revert".to_string(),
                commit_time: parse_ts("2026-03-06T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 0,
                commit_total_lines: 3,
            },
        ];
        let commit_messages = HashMap::from([
            ("orig".to_string(), "feat: add parser".to_string()),
            ("late".to_string(), "fix: late cleanup".to_string()),
            ("other".to_string(), "fix: unrelated module".to_string()),
            (
                "revert".to_string(),
                "Revert \"feat: add parser\"\n\nThis reverts commit 1234567890abcdef1234567890abcdef12345678."
                    .to_string(),
            ),
        ]);
        let commit_removed_hashes = HashMap::from([
            (
                "late".to_string(),
                HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(hash_line("buggy();"), 1)]),
                )]),
            ),
            (
                "other".to_string(),
                HashMap::from([(
                    "src/other.rs".to_string(),
                    HashMap::from([(hash_line("buggy();"), 1)]),
                )]),
            ),
            (
                "revert".to_string(),
                HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(hash_line("buggy();"), 1)]),
                )]),
            ),
        ]);
        let mut derived = HashMap::from([
            (
                "orig".to_string(),
                DerivedCommitEvent {
                    budget: HashMap::from([(
                        "src/lib.rs".to_string(),
                        HashMap::from([(hash_line("buggy();"), 1)]),
                    )]),
                    ..derived_event(true)
                },
            ),
            ("late".to_string(), derived_event(false)),
            ("other".to_string(), derived_event(false)),
            ("revert".to_string(), derived_event(false)),
        ]);

        annotate_bug_after_merge_signals_from_data(
            &commits,
            &commit_messages,
            &commit_removed_hashes,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(!original.bug_after_merge);
        assert_eq!(original.first_bug_signal_commit_sha, None);
        assert_eq!(original.bug_signal_count, 0);
    }

    #[test]
    fn bug_after_merge_detection_ignores_same_file_fix_without_exact_removed_overlap() {
        let commits = vec![
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "orig".to_string(),
                commit_time: parse_ts("2026-03-01T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 20,
                commit_total_lines: 20,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "fix".to_string(),
                commit_time: parse_ts("2026-03-03T10:00:00Z"),
                heavy_ai: false,
                matched_total_lines: 0,
                commit_total_lines: 3,
            },
        ];
        let commit_messages = HashMap::from([
            ("orig".to_string(), "feat: add parser".to_string()),
            ("fix".to_string(), "fix: unrelated cleanup".to_string()),
        ]);
        let commit_removed_hashes = HashMap::from([(
            "fix".to_string(),
            HashMap::from([(
                "src/lib.rs".to_string(),
                HashMap::from([(hash_line("other();"), 1)]),
            )]),
        )]);
        let mut derived = HashMap::from([
            (
                "orig".to_string(),
                DerivedCommitEvent {
                    budget: HashMap::from([(
                        "src/lib.rs".to_string(),
                        HashMap::from([(hash_line("buggy();"), 1)]),
                    )]),
                    ..derived_event(true)
                },
            ),
            ("fix".to_string(), derived_event(false)),
        ]);

        annotate_bug_after_merge_signals_from_data(
            &commits,
            &commit_messages,
            &commit_removed_hashes,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(!original.bug_after_merge);
        assert_eq!(original.first_bug_signal_commit_sha, None);
        assert_eq!(original.bug_signal_count, 0);
    }

    #[test]
    fn issue_linked_bug_after_merge_detection_requires_exact_hash_overlap_within_window() {
        let commits = vec![
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "orig".to_string(),
                commit_time: parse_ts("2026-03-01T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 20,
                commit_total_lines: 20,
            },
            CandidateCommit {
                repo_root: "/tmp/repo".to_string(),
                commit_sha: "other".to_string(),
                commit_time: parse_ts("2026-03-02T10:00:00Z"),
                heavy_ai: true,
                matched_total_lines: 20,
                commit_total_lines: 20,
            },
        ];
        let matching_hash = hash_line("buggy();");
        let non_matching_hash = hash_line("fixed();");
        let candidates = vec![
            IssueLinkedBugSignalCandidate {
                signal_ref: "issue#7/pr#19".to_string(),
                signal_time: parse_ts("2026-03-10T09:00:00Z"),
                window_anchor_time: parse_ts("2026-03-10T09:00:00Z"),
                removed_hashes: HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(matching_hash.clone(), 1)]),
                )]),
            },
            IssueLinkedBugSignalCandidate {
                signal_ref: "issue#8/pr#20".to_string(),
                signal_time: parse_ts("2026-05-10T09:00:00Z"),
                window_anchor_time: parse_ts("2026-05-10T09:00:00Z"),
                removed_hashes: HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(matching_hash.clone(), 1)]),
                )]),
            },
            IssueLinkedBugSignalCandidate {
                signal_ref: "issue#9/pr#21".to_string(),
                signal_time: parse_ts("2026-03-12T09:00:00Z"),
                window_anchor_time: parse_ts("2026-03-12T09:00:00Z"),
                removed_hashes: HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(non_matching_hash, 1)]),
                )]),
            },
        ];
        let mut derived = HashMap::from([
            (
                "orig".to_string(),
                DerivedCommitEvent {
                    budget: HashMap::from([(
                        "src/lib.rs".to_string(),
                        HashMap::from([(matching_hash, 1)]),
                    )]),
                    ..derived_event(true)
                },
            ),
            (
                "other".to_string(),
                DerivedCommitEvent {
                    budget: HashMap::from([(
                        "src/other.rs".to_string(),
                        HashMap::from([(hash_line("elsewhere();"), 1)]),
                    )]),
                    ..derived_event(true)
                },
            ),
        ]);

        annotate_issue_linked_bug_after_merge_signals_from_data(
            &commits,
            &candidates,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(original.bug_after_merge);
        assert_eq!(
            original.first_bug_signal_commit_sha.as_deref(),
            Some("issue#7/pr#19")
        );
        assert_eq!(
            original.first_bug_signal_commit_time,
            Some(parse_ts("2026-03-10T09:00:00Z"))
        );
        assert_eq!(original.bug_signal_count, 1);

        let other = derived.get("other").expect("other event");
        assert!(!other.bug_after_merge);
        assert_eq!(other.bug_signal_count, 0);
    }

    #[test]
    fn issue_linked_bug_after_merge_detection_uses_fix_pr_merge_time_not_issue_created_time() {
        let commits = vec![CandidateCommit {
            repo_root: "/tmp/repo".to_string(),
            commit_sha: "orig".to_string(),
            commit_time: parse_ts("2026-03-05T10:00:00Z"),
            heavy_ai: true,
            matched_total_lines: 20,
            commit_total_lines: 20,
        }];
        let matching_hash = hash_line("buggy();");
        let candidates = vec![IssueLinkedBugSignalCandidate {
            signal_ref: "issue#7/pr#19".to_string(),
            signal_time: parse_ts("2026-03-10T09:00:00Z"),
            window_anchor_time: parse_ts("2026-03-10T09:00:00Z"),
            removed_hashes: HashMap::from([(
                "src/lib.rs".to_string(),
                HashMap::from([(matching_hash.clone(), 1)]),
            )]),
        }];
        let mut derived = HashMap::from([(
            "orig".to_string(),
            DerivedCommitEvent {
                budget: HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(matching_hash, 1)]),
                )]),
                ..derived_event(true)
            },
        )]);

        annotate_issue_linked_bug_after_merge_signals_from_data(
            &commits,
            &candidates,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(original.bug_after_merge);
        assert_eq!(
            original.first_bug_signal_commit_time,
            Some(parse_ts("2026-03-10T09:00:00Z"))
        );
    }

    #[test]
    fn issue_linked_bug_after_merge_detection_ignores_fix_pr_merged_after_window() {
        let commits = vec![CandidateCommit {
            repo_root: "/tmp/repo".to_string(),
            commit_sha: "orig".to_string(),
            commit_time: parse_ts("2026-03-01T10:00:00Z"),
            heavy_ai: true,
            matched_total_lines: 20,
            commit_total_lines: 20,
        }];
        let matching_hash = hash_line("buggy();");
        let candidates = vec![IssueLinkedBugSignalCandidate {
            signal_ref: "issue#8/pr#20".to_string(),
            signal_time: parse_ts("2026-05-10T09:00:00Z"),
            window_anchor_time: parse_ts("2026-05-10T09:00:00Z"),
            removed_hashes: HashMap::from([(
                "src/lib.rs".to_string(),
                HashMap::from([(matching_hash.clone(), 1)]),
            )]),
        }];
        let mut derived = HashMap::from([(
            "orig".to_string(),
            DerivedCommitEvent {
                budget: HashMap::from([(
                    "src/lib.rs".to_string(),
                    HashMap::from([(matching_hash, 1)]),
                )]),
                ..derived_event(true)
            },
        )]);

        annotate_issue_linked_bug_after_merge_signals_from_data(
            &commits,
            &candidates,
            &mut derived,
        );

        let original = derived.get("orig").expect("original event");
        assert!(!original.bug_after_merge);
        assert_eq!(original.first_bug_signal_commit_sha, None);
        assert_eq!(original.bug_signal_count, 0);
    }

    #[test]
    fn load_issue_linked_bug_signal_candidates_uses_pr_merge_time_and_skips_incomplete_prs()
    -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO fact_github_issue (
                repo_key, issue_number, state, created_at, updated_at, closed_at, is_pull_request_flag, bug_candidate_flag
             ) VALUES
                ('git:github.com/PaceFlow/repo', 7, 'closed', '2026-02-20T09:00:00Z', NULL, NULL, 0, 1),
                ('git:github.com/PaceFlow/repo', 8, 'closed', '2026-03-10T09:00:00Z', NULL, NULL, 0, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_issue_fix_pull_request (
                repo_key, issue_number, pr_number, linked_at
             ) VALUES
                ('git:github.com/PaceFlow/repo', 7, 19, '2026-03-09T08:00:00Z'),
                ('git:github.com/PaceFlow/repo', 8, 20, '2026-03-11T08:00:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_pull_request (
                repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
                merged_at, base_ref, head_ref, html_url, removed_hashes_complete_flag
             ) VALUES
                ('git:github.com/PaceFlow/repo', 19, 'closed', 0, '2026-03-08T08:00:00Z', NULL, NULL,
                 '2026-03-10T09:00:00Z', 'main', 'fix-bug', NULL, 1),
                ('git:github.com/PaceFlow/repo', 20, 'closed', 0, '2026-03-10T08:00:00Z', NULL, NULL,
                 '2026-03-12T09:00:00Z', 'main', 'fix-bug-2', NULL, 0)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_pull_request_removed_line_hash (
                repo_key, pr_number, rel_path, line_hash, count
             ) VALUES
                ('git:github.com/PaceFlow/repo', 19, 'src/lib.rs', ?1, 1)",
            params![hash_line("buggy();")],
        )?;

        let candidates =
            load_issue_linked_bug_signal_candidates(&conn, "git:github.com/PaceFlow/repo")?;

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].signal_ref, "issue#7/pr#19");
        assert_eq!(candidates[0].signal_time, parse_ts("2026-03-10T09:00:00Z"));
        assert_eq!(
            candidates[0].window_anchor_time,
            parse_ts("2026-03-10T09:00:00Z")
        );
        Ok(())
    }

    #[test]
    fn event_stream_default_all_emits_all_base_streams_in_stable_order() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES ('codex', 's1', '/tmp/repo', 'codex/gpt-5', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 3, 1, 0, 1, '2026-03-17T09:05:00Z', 5.0, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_session_productivity (
                provider, session_id, repo_root, model_name, project_path, started_at, ended_at,
                accepted_lines_added, accepted_lines_removed, accepted_total_changed_lines, user_word_count
             ) VALUES ('codex', 's1', '/tmp/repo', 'codex/gpt-5', '/tmp/repo', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 10, 2, 12, 100)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, task_key, branch_name, provider, session_id, model_name, started_at,
                attribution_weight, commit_within_window_flag, user_turn_count, debug_loop_flag,
                mid_session_error_paste_flag, accepted_output_flag, first_accepted_change_at,
                minutes_to_first_accepted_change
             ) VALUES ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'codex', 's1', 'codex/gpt-5', '2026-03-17T09:00:00Z', 1.0, 1, 3, 1, 0, 1, '2026-03-17T09:05:00Z', 5.0)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'abc123', '2026-03-18T10:00:00Z', 1, 1, 0, 42, 52)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'abc123', 30, 5, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'abc123', 'codex', 's1', '2026-03-18T10:00:00Z', 'codex/gpt-5', 42, 0.80, 1.0)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'abc123', 0, 1.0, '2026-03-18T10:00:00Z')",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_event_stream(
            &conn,
            &EventStreamArgs {
                category: EventCategory::All,
                stream: EventStreamKind::All,
                from: None,
                to: None,
                repo: None,
                provider: None,
                task: None,
                model: None,
                limit: None,
                pretty: false,
            },
        )?;

        let stream_types = rows
            .iter()
            .map(|row| row.stream_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            stream_types,
            vec![
                "session-base",
                "task-session-base",
                "change-base",
                "commit-session-base",
                "task-commit-base",
            ]
        );
        assert_eq!(rows[0].provider.as_deref(), Some("codex"));
        assert_eq!(rows[0].model.as_deref(), Some("codex/gpt-5"));
        assert_eq!(rows[2].event_time.as_deref(), Some("2026-03-18T10:00:00Z"));
        Ok(())
    }

    #[test]
    fn session_list_rows_prefix_unknown_models_with_provider() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session(
            &conn,
            "cursor",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
        )?;
        conn.execute(
            "INSERT INTO event_session_productivity (
                provider, session_id, repo_root, project_path, started_at, ended_at,
                accepted_lines_added, accepted_lines_removed, accepted_total_changed_lines, user_word_count
             ) VALUES ('cursor', 's1', '/tmp/repo', '/tmp/repo', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 10, 2, 12, 100)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_list_rows(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: None,
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].model, "cursor/(unknown)");
        Ok(())
    }

    #[test]
    fn provider_grouped_change_and_lifecycle_reports_include_human_for_unmatched_commits()
    -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'abc123', '2026-03-18T10:00:00Z', 0, 1, 0, 0, 52)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'abc123', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'abc123', 'human', '__human__', '2026-03-18T10:00:00Z', NULL, 0.0, 1.0, 0.0)",
            [],
        )?;

        create_reporting_views(&conn)?;

        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Provider),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 10,
        };

        let change_rows = query_change_report(&conn, &args)?;
        assert_eq!(change_rows.len(), 1);
        assert_eq!(change_rows[0].group_value.as_deref(), Some("human"));
        assert_eq!(change_rows[0].commit_count, 1);

        let lifecycle_rows = query_lifecycle_report(&conn, &args)?;
        assert_eq!(lifecycle_rows.len(), 1);
        assert_eq!(lifecycle_rows[0].group_value.as_deref(), Some("human"));
        assert_eq!(lifecycle_rows[0].heavy_commit_count, 0);
        Ok(())
    }

    #[test]
    fn implicit_model_default_change_report_filters_human_and_small_models() -> Result<()> {
        let conn = open_test_db()?;
        for index in 0..4 {
            let sha = format!("g54-{index}");
            conn.execute(
                "INSERT INTO event_commit_outcome (
                    repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                    reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
                 ) VALUES ('/tmp/repo', ?1, '2026-03-18T10:00:00Z', 1, 1, 0, 20, 30)",
                params![sha],
            )?;
            conn.execute(
                "INSERT INTO event_commit_churn (
                    repo_root, commit_sha, ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window, churn_window_days
                 ) VALUES ('/tmp/repo', ?1, 10, 2, 14)",
                params![format!("g54-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_session (
                    repo_root, commit_sha, provider, session_id, commit_time, model_name,
                    matched_lines, share_of_commit, share_of_ai
                 ) VALUES ('/tmp/repo', ?1, 'codex', ?2, '2026-03-18T10:00:00Z', 'codex/gpt-5.4', 20.0, 0.67, 1.0)",
                params![format!("g54-{index}"), format!("s54-{index}")],
            )?;
        }
        for index in 0..3 {
            conn.execute(
                "INSERT INTO event_commit_outcome (
                    repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                    reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
                 ) VALUES ('/tmp/repo', ?1, '2026-03-18T11:00:00Z', 1, 0, 0, 18, 28)",
                params![format!("g53-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_churn (
                    repo_root, commit_sha, ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window, churn_window_days
                 ) VALUES ('/tmp/repo', ?1, 8, 1, 14)",
                params![format!("g53-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_session (
                    repo_root, commit_sha, provider, session_id, commit_time, model_name,
                    matched_lines, share_of_commit, share_of_ai
                 ) VALUES ('/tmp/repo', ?1, 'codex', ?2, '2026-03-18T11:00:00Z', 'codex/gpt-5.3-codex', 18.0, 0.64, 1.0)",
                params![format!("g53-{index}"), format!("s53-{index}")],
            )?;
        }
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'human-0', '2026-03-18T12:00:00Z', 0, 1, 0, 0, 12)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'human-0', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'human-0', 'human', '__human__', '2026-03-18T12:00:00Z', NULL, 0.0, 1.0, 0.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Model),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let filtered = query_change_report_with_options(
            &conn,
            &args,
            ReportQueryOptions {
                implicit_model_default: true,
            },
        )?;
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].group_value.as_deref(), Some("codex/gpt-5.4"));
        assert_eq!(
            filtered[1].group_value.as_deref(),
            Some("codex/gpt-5.3-codex")
        );

        let explicit = query_change_report(&conn, &args)?;
        assert!(
            explicit
                .iter()
                .any(|row| row.group_value.as_deref() == Some("human/(unknown)"))
        );
        Ok(())
    }

    #[test]
    fn implicit_model_default_lifecycle_report_filters_human_and_small_models() -> Result<()> {
        let conn = open_test_db()?;
        for index in 0..4 {
            let sha = format!("q54-{index}");
            conn.execute(
                "INSERT INTO event_commit_outcome (
                    repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                    reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
                 ) VALUES ('/tmp/repo', ?1, '2026-03-19T10:00:00Z', 1, 1, 0, 20, 30)",
                params![sha],
            )?;
            conn.execute(
                "INSERT INTO event_commit_churn (
                    repo_root, commit_sha, ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window, churn_window_days
                 ) VALUES ('/tmp/repo', ?1, 10, 2, 14)",
                params![format!("q54-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_bug_signal (
                    repo_root, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
                    first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
                 ) VALUES ('/tmp/repo', ?1, 1, 'fix54', '2026-03-20T10:00:00Z', 1, 60, 'git_fix_commit')",
                params![format!("q54-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_session (
                    repo_root, commit_sha, provider, session_id, commit_time, model_name,
                    matched_lines, share_of_commit, share_of_ai
                 ) VALUES ('/tmp/repo', ?1, 'codex', ?2, '2026-03-19T10:00:00Z', 'codex/gpt-5.4', 20.0, 0.67, 1.0)",
                params![format!("q54-{index}"), format!("sq54-{index}")],
            )?;
        }
        for index in 0..3 {
            conn.execute(
                "INSERT INTO event_commit_outcome (
                    repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                    reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
                 ) VALUES ('/tmp/repo', ?1, '2026-03-19T11:00:00Z', 1, 1, 0, 18, 28)",
                params![format!("q53-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_churn (
                    repo_root, commit_sha, ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window, churn_window_days
                 ) VALUES ('/tmp/repo', ?1, 8, 1, 14)",
                params![format!("q53-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_bug_signal (
                    repo_root, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
                    first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
                 ) VALUES ('/tmp/repo', ?1, 0, NULL, NULL, 0, 60, 'git_fix_commit')",
                params![format!("q53-{index}")],
            )?;
            conn.execute(
                "INSERT INTO event_commit_session (
                    repo_root, commit_sha, provider, session_id, commit_time, model_name,
                    matched_lines, share_of_commit, share_of_ai
                 ) VALUES ('/tmp/repo', ?1, 'codex', ?2, '2026-03-19T11:00:00Z', 'codex/gpt-5.3-codex', 18.0, 0.64, 1.0)",
                params![format!("q53-{index}"), format!("sq53-{index}")],
            )?;
        }
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'qhuman-0', '2026-03-19T12:00:00Z', 0, 1, 0, 0, 12)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'qhuman-0', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'qhuman-0', 'human', '__human__', '2026-03-19T12:00:00Z', NULL, 0.0, 1.0, 0.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let args = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Model),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let filtered = query_lifecycle_report_with_options(
            &conn,
            &args,
            ReportQueryOptions {
                implicit_model_default: true,
            },
        )?;
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].group_value.as_deref(), Some("codex/gpt-5.4"));
        assert_eq!(
            filtered[1].group_value.as_deref(),
            Some("codex/gpt-5.3-codex")
        );

        let explicit = query_lifecycle_report(&conn, &args)?;
        assert!(
            explicit
                .iter()
                .any(|row| row.group_value.as_deref() == Some("human/(unknown)"))
        );
        Ok(())
    }

    #[test]
    fn event_stream_can_filter_commit_session_rows_to_human_provider() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'abc123', '2026-03-18T10:00:00Z', 0, 1, 0, 0, 52)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'abc123', 0, 0, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'abc123', 'human', '__human__', '2026-03-18T10:00:00Z', NULL, 0.0, 1.0, 0.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_event_stream(
            &conn,
            &EventStreamArgs {
                category: EventCategory::All,
                stream: EventStreamKind::CommitSessionBase,
                from: None,
                to: None,
                repo: None,
                provider: Some("human".to_string()),
                task: None,
                model: None,
                limit: None,
                pretty: false,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stream_type, "commit-session-base");
        assert_eq!(rows[0].provider.as_deref(), Some("human"));
        assert_eq!(rows[0].model.as_deref(), Some("human/(unknown)"));
        Ok(())
    }

    #[test]
    fn session_report_returns_no_rows_for_human_provider_filter() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "codex",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES ('codex', 's1', '/tmp/repo', 'codex/gpt-5', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 3, 0, 0, 1, '2026-03-17T09:05:00Z', 5.0, 1)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_session_report(
            &conn,
            &ReportArgs {
                weekly: false,
                group_by: None,
                from: None,
                to: None,
                repo: None,
                all_projects: false,
                provider: Some("human".to_string()),
                task: None,
                branch: None,
                model: None,
                limit: 10,
            },
        )?;

        assert!(rows.is_empty());
        Ok(())
    }

    #[test]
    fn event_stream_filters_exclude_streams_missing_provider_or_model_dimensions() -> Result<()> {
        let conn = open_test_db()?;
        upsert_metadata_session_with_model(
            &conn,
            "cursor",
            "s1",
            Some("/tmp/repo"),
            Some("2026-03-17T09:00:00Z"),
            Some("2026-03-17T09:30:00Z"),
            None,
            Some("openai"),
            Some("gpt-5"),
        )?;
        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
             ) VALUES ('cursor', 's1', '/tmp/repo', 'cursor/gpt-5', '2026-03-17T09:00:00Z', '2026-03-17T09:30:00Z', 3, 0, 0, 0, NULL, NULL, 0)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'abc123', '2026-03-18T10:00:00Z', 1, 1, 0, 42, 52)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'abc123', 30, 5, 14)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, commit_sha, provider, session_id, commit_time, model_name,
                matched_lines, share_of_commit, share_of_ai
             ) VALUES ('/tmp/repo', 'abc123', 'cursor', 's1', '2026-03-18T10:00:00Z', 'cursor/gpt-5', 42, 0.80, 1.0)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_event_stream(
            &conn,
            &EventStreamArgs {
                category: EventCategory::All,
                stream: EventStreamKind::All,
                from: None,
                to: None,
                repo: None,
                provider: Some("cursor".to_string()),
                task: None,
                model: Some("cursor/gpt-5".to_string()),
                limit: None,
                pretty: false,
            },
        )?;

        let stream_types = rows
            .iter()
            .map(|row| row.stream_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(stream_types, vec!["session-base", "commit-session-base"]);
        Ok(())
    }

    #[test]
    fn event_stream_stream_selection_and_time_filters_work() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
             ) VALUES
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'old', 0, 1.0, '2026-03-10T10:00:00Z'),
                ('/tmp/repo', 'PAC-1', 'PAC-1-branch', 'new', 0, 1.0, '2026-03-20T10:00:00Z')",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'old', '2026-03-10T10:00:00Z', 1, 1, 0, 10, 12),
                ('/tmp/repo', 'new', '2026-03-20T10:00:00Z', 1, 1, 0, 10, 12)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES
                ('/tmp/repo', 'old', 8, 1, 14),
                ('/tmp/repo', 'new', 8, 1, 14)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_event_stream(
            &conn,
            &EventStreamArgs {
                category: EventCategory::All,
                stream: EventStreamKind::TaskCommitBase,
                from: Some("2026-03-15".to_string()),
                to: Some("2026-03-21".to_string()),
                repo: None,
                provider: None,
                task: Some("PAC-1".to_string()),
                model: None,
                limit: None,
                pretty: false,
            },
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stream_type, "task-commit-base");
        assert_eq!(rows[0].task_key.as_deref(), Some("PAC-1"));
        assert_eq!(rows[0].event_time.as_deref(), Some("2026-03-20T10:00:00Z"));
        Ok(())
    }

    #[test]
    fn event_stream_quality_category_labels_commit_rows_as_quality() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES ('/tmp/repo', 'abc123', '2026-03-18T10:00:00Z', 1, 1, 0, 42, 52)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
             ) VALUES ('/tmp/repo', 'abc123', 30, 5, 14)",
            [],
        )?;

        create_reporting_views(&conn)?;
        let rows = query_event_stream(
            &conn,
            &EventStreamArgs {
                category: EventCategory::Quality,
                stream: EventStreamKind::All,
                from: None,
                to: None,
                repo: None,
                provider: None,
                task: None,
                model: None,
                limit: None,
                pretty: false,
            },
        )?;

        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.category == "quality"));
        Ok(())
    }
}
