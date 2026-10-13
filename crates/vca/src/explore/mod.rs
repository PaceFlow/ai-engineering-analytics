//! Typed exploration queries shared by the CLI and the TUI.
//!
//! All queries read the derived insight tables. Filters are bound as SQL
//! parameters. Every money figure is an API list-price equivalent unless the
//! field name says `reported`.

pub mod bands;
pub mod compare;
pub mod delivery;
pub mod format;
pub mod practices;
pub mod rework;

use anyhow::Result;
use rusqlite::{Connection, ToSql, params_from_iter};
use serde::Serialize;
use std::collections::BTreeMap;

pub use bands::{CostBand, TokenBand};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filters {
    /// Inclusive `YYYY-MM-DD` bounds on the session/task start date.
    pub from: Option<String>,
    pub to: Option<String>,
    pub providers: Vec<String>,
    /// Substring match on any model used.
    pub model: Option<String>,
    /// Exact repo root.
    pub repo: Option<String>,
    /// Substring match on title, first request, branch or id.
    pub search: Option<String>,
    pub intent: Option<String>,
    pub outcome: Option<String>,
    pub min_cost: Option<f64>,
    pub cost_band: Option<CostBand>,
    pub token_band: Option<TokenBand>,
    /// Restrict to one root session.
    pub session: Option<(String, String)>,
    /// Restrict to explicit tasks (provider, session, task index), e.g. a
    /// practice comparison or a marked selection.
    pub task_keys: Vec<(String, String, i64)>,
    /// Label shown for `task_keys` in breadcrumbs.
    pub task_keys_label: Option<String>,
}

impl Filters {
    /// Human-readable chips for headers.
    pub fn chips(&self) -> Vec<String> {
        let mut chips = Vec::new();
        match (&self.from, &self.to) {
            (Some(from), Some(to)) => chips.push(format!("{from}..{to}")),
            (Some(from), None) => chips.push(format!("since {from}")),
            (None, Some(to)) => chips.push(format!("until {to}")),
            (None, None) => {}
        }
        if !self.providers.is_empty() {
            chips.push(format!("tool: {}", self.providers.join(",")));
        }
        if let Some(model) = &self.model {
            chips.push(format!("model~{model}"));
        }
        if let Some(repo) = &self.repo {
            chips.push(format!("project: {}", short_repo(repo)));
        }
        if let Some(search) = &self.search {
            chips.push(format!("/{search}"));
        }
        if let Some(intent) = &self.intent {
            chips.push(format!("intent: {intent}"));
        }
        if let Some(outcome) = &self.outcome {
            chips.push(format!("outcome: {outcome}"));
        }
        if let Some(band) = self.cost_band {
            chips.push(format!("cost: {}", band.label()));
        }
        if let Some(band) = self.token_band {
            chips.push(format!("tokens: {}", band.label()));
        }
        if let Some(min) = self.min_cost {
            chips.push(format!("≥${min:.2}"));
        }
        if let Some((provider, session)) = &self.session {
            chips.push(format!("session: {provider}:{}", short_id(session)));
        }
        if !self.task_keys.is_empty() {
            chips.push(
                self.task_keys_label
                    .clone()
                    .unwrap_or_else(|| format!("{} selected tasks", self.task_keys.len())),
            );
        }
        chips
    }
}

pub fn short_repo(repo: &str) -> String {
    repo.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(repo)
        .to_string()
}

pub fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

/// Strip the `provider/` prefix stored on model names.
pub fn short_model(model: &str) -> String {
    model
        .split_once('/')
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| model.to_string())
}

/// SQL predicate builder with positional parameters.
#[derive(Default)]
struct Where {
    clauses: Vec<String>,
    params: Vec<Box<dyn ToSql>>,
}

impl Where {
    fn push(&mut self, clause: &str, value: impl ToSql + 'static) {
        self.params.push(Box::new(value));
        self.clauses
            .push(clause.replace('?', &format!("?{}", self.params.len())));
    }

    fn raw(&mut self, clause: &str) {
        self.clauses.push(clause.to_string());
    }

    fn sql(&self) -> String {
        if self.clauses.is_empty() {
            "1 = 1".to_string()
        } else {
            self.clauses.join(" AND ")
        }
    }
}

// ── Tasks ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TaskRow {
    pub provider: String,
    pub session_id: String,
    pub task_index: i64,
    pub title: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub completion_source: String,
    pub segmentation_source: String,
    pub wall_minutes: Option<f64>,
    pub active_minutes_to_complete: Option<f64>,
    pub active_minutes_total: Option<f64>,
    pub agent_minutes: Option<f64>,
    pub user_minutes: Option<f64>,
    pub turns: i64,
    pub attempts: i64,
    pub validations: i64,
    pub validation_failures: i64,
    pub loop_attempts: i64,
    pub edit_calls: i64,
    pub lines_changed: i64,
    pub subagents: i64,
    pub primary_model: Option<String>,
    pub models: String,
    pub model_changes: i64,
    pub repo_root: Option<String>,
    pub git_branch: Option<String>,
    pub tokens_total: i64,
    pub fresh_tokens: i64,
    pub cost_total: Option<f64>,
    pub cost_to_complete: Option<f64>,
    pub post_completion_cost: Option<f64>,
    pub subagent_cost: Option<f64>,
    pub unpriced_tokens: i64,
    pub estimated_tokens: i64,
    pub commits: i64,
    pub intent: Option<String>,
    pub intent_source: Option<String>,
    pub intent_band: Option<String>,
    pub outcome: Option<String>,
    pub outcome_source: Option<String>,
    pub outcome_band: Option<String>,
}

impl TaskRow {
    pub fn is_completed(&self) -> bool {
        self.completed_at.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub enum TaskSort {
    #[default]
    Cost,
    CostToComplete,
    Tokens,
    ActiveTime,
    WallTime,
    Started,
    Attempts,
    Failures,
}

impl TaskSort {
    fn order_by(self) -> &'static str {
        match self {
            Self::Cost => "COALESCE(t.cost_total_usd, -1) DESC",
            Self::CostToComplete => "COALESCE(t.cost_to_complete_usd, -1) DESC",
            Self::Tokens => "t.tokens_total DESC",
            Self::ActiveTime => "COALESCE(t.active_minutes_total, -1) DESC",
            Self::WallTime => "COALESCE(t.wall_minutes_to_complete, -1) DESC",
            Self::Started => "t.started_at DESC",
            Self::Attempts => "t.attempts DESC",
            Self::Failures => "t.validation_failures DESC",
        }
    }

    pub const ALL: [TaskSort; 8] = [
        Self::Cost,
        Self::CostToComplete,
        Self::Tokens,
        Self::ActiveTime,
        Self::WallTime,
        Self::Started,
        Self::Attempts,
        Self::Failures,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::CostToComplete => "cost→done",
            Self::Tokens => "tokens",
            Self::ActiveTime => "active time",
            Self::WallTime => "wall time",
            Self::Started => "started",
            Self::Attempts => "attempts",
            Self::Failures => "failures",
        }
    }
}

fn task_where(filters: &Filters) -> Where {
    let mut w = Where::default();
    if let Some(from) = &filters.from {
        w.push("date(t.started_at) >= date(?)", from.clone());
    }
    if let Some(to) = &filters.to {
        w.push("date(t.started_at) <= date(?)", to.clone());
    }
    if !filters.providers.is_empty() {
        let placeholders: Vec<String> = filters
            .providers
            .iter()
            .map(|provider| {
                w.params.push(Box::new(provider.clone()));
                format!("?{}", w.params.len())
            })
            .collect();
        w.raw(&format!("t.provider IN ({})", placeholders.join(",")));
    }
    if let Some(model) = &filters.model {
        w.push("t.models LIKE ?", format!("%{model}%"));
    }
    if let Some(repo) = &filters.repo {
        w.push("t.repo_root = ?", repo.clone());
    }
    if let Some(search) = &filters.search {
        let pattern = format!("%{search}%");
        w.params.push(Box::new(pattern));
        let n = w.params.len();
        w.raw(&format!(
            "(t.title LIKE ?{n} OR t.session_id LIKE ?{n} OR COALESCE(t.git_branch, '') LIKE ?{n})"
        ));
    }
    if let Some(intent) = &filters.intent {
        w.push("li.label = ?", intent.clone());
    }
    if let Some(outcome) = &filters.outcome {
        w.push("lo.label = ?", outcome.clone());
    }
    if let Some(min) = filters.min_cost {
        w.push("COALESCE(t.cost_total_usd, 0) >= ?", min);
    }
    if let Some(band) = filters.cost_band {
        w.raw(&band.sql("t.cost_total_usd"));
    }
    if let Some(band) = filters.token_band {
        w.raw(&band.sql("t.fresh_tokens"));
    }
    if let Some((provider, session)) = &filters.session {
        w.push("t.provider = ?", provider.clone());
        w.push("t.session_id = ?", session.clone());
    }
    if !filters.task_keys.is_empty() {
        let placeholders: Vec<String> = filters
            .task_keys
            .iter()
            .map(|(provider, session, task)| {
                w.params
                    .push(Box::new(format!("{provider}|{session}|{task}")));
                format!("?{}", w.params.len())
            })
            .collect();
        w.raw(&format!(
            "(t.provider || '|' || t.session_id || '|' || t.task_index) IN ({})",
            placeholders.join(",")
        ));
    }
    w
}

const TASK_COLUMNS: &str = "
    t.provider, t.session_id, t.task_index, COALESCE(t.title, ''), t.started_at, t.completed_at,
    t.completion_source, t.segmentation_source, t.wall_minutes_to_complete,
    t.active_minutes_to_complete, t.active_minutes_total, t.agent_minutes, t.user_minutes,
    t.turns, t.attempts, t.validations, t.validation_failures, t.loop_attempts, t.edit_calls,
    t.lines_changed, t.subagents, t.primary_model, COALESCE(t.models, ''), t.model_changes,
    t.repo_root, t.git_branch, t.tokens_total, t.fresh_tokens, t.cost_total_usd,
    t.cost_to_complete_usd, t.post_completion_cost_usd, t.subagent_cost_usd, t.unpriced_tokens,
    t.estimated_tokens, t.commits, li.label, li.source, li.band, lo.label, lo.source, lo.band";

const TASK_JOINS: &str = "
    FROM derived_task t
    LEFT JOIN view_label_effective li
      ON li.unit_kind = 'task' AND li.question_id = 'intent'
     AND li.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index
    LEFT JOIN view_label_effective lo
      ON lo.unit_kind = 'task' AND lo.question_id = 'outcome'
     AND lo.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index";

fn task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRow> {
    Ok(TaskRow {
        provider: row.get(0)?,
        session_id: row.get(1)?,
        task_index: row.get(2)?,
        title: row.get(3)?,
        started_at: row.get(4)?,
        completed_at: row.get(5)?,
        completion_source: row.get(6)?,
        segmentation_source: row.get(7)?,
        wall_minutes: row.get(8)?,
        active_minutes_to_complete: row.get(9)?,
        active_minutes_total: row.get(10)?,
        agent_minutes: row.get(11)?,
        user_minutes: row.get(12)?,
        turns: row.get(13)?,
        attempts: row.get(14)?,
        validations: row.get(15)?,
        validation_failures: row.get(16)?,
        loop_attempts: row.get(17)?,
        edit_calls: row.get(18)?,
        lines_changed: row.get(19)?,
        subagents: row.get(20)?,
        primary_model: row.get(21)?,
        models: row.get(22)?,
        model_changes: row.get(23)?,
        repo_root: row.get(24)?,
        git_branch: row.get(25)?,
        tokens_total: row.get(26)?,
        fresh_tokens: row.get(27)?,
        cost_total: row.get(28)?,
        cost_to_complete: row.get(29)?,
        post_completion_cost: row.get(30)?,
        subagent_cost: row.get(31)?,
        unpriced_tokens: row.get(32)?,
        estimated_tokens: row.get(33)?,
        commits: row.get(34)?,
        intent: row.get(35)?,
        intent_source: row.get(36)?,
        intent_band: row.get(37)?,
        outcome: row.get(38)?,
        outcome_source: row.get(39)?,
        outcome_band: row.get(40)?,
    })
}

pub fn tasks(
    conn: &Connection,
    filters: &Filters,
    sort: TaskSort,
    limit: usize,
) -> Result<Vec<TaskRow>> {
    let w = task_where(filters);
    let sql = format!(
        "SELECT {TASK_COLUMNS} {TASK_JOINS} WHERE {} ORDER BY {}, t.started_at DESC LIMIT {}",
        w.sql(),
        sort.order_by(),
        limit.max(1)
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.params.iter()), task_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Sessions (root sessions with tree totals) ──────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SessionRow {
    pub provider: String,
    pub session_id: String,
    pub title: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub repo_root: Option<String>,
    pub git_branch: Option<String>,
    pub models: String,
    pub primary_model: Option<String>,
    pub tasks: i64,
    pub turns: i64,
    pub agents: i64,
    pub attempts: i64,
    pub validation_failures: i64,
    pub loop_attempts: i64,
    pub tokens_total: i64,
    pub fresh_tokens: i64,
    pub cache_read_tokens: i64,
    pub cost: Option<f64>,
    pub reported_cost: Option<f64>,
    pub subagent_cost: Option<f64>,
    pub unpriced_tokens: i64,
    pub estimated_tokens: i64,
    pub active_minutes: Option<f64>,
    pub completed_tasks: i64,
    pub commits: i64,
    pub source_status: String,
    pub legacy_usage: bool,
    pub intent: Option<String>,
    pub outcome: Option<String>,
    pub label_source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub enum SessionSort {
    #[default]
    Cost,
    Tokens,
    FreshTokens,
    CacheRatio,
    Started,
    Turns,
    Failures,
    Agents,
}

impl SessionSort {
    pub const ALL: [SessionSort; 8] = [
        Self::Cost,
        Self::Tokens,
        Self::FreshTokens,
        Self::CacheRatio,
        Self::Started,
        Self::Turns,
        Self::Failures,
        Self::Agents,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::Tokens => "tokens",
            Self::FreshTokens => "fresh tokens",
            Self::CacheRatio => "cache-read ratio",
            Self::Started => "started",
            Self::Turns => "turns",
            Self::Failures => "failures",
            Self::Agents => "agents",
        }
    }

    fn sort(self, rows: &mut [SessionRow]) {
        let key = |row: &SessionRow| -> f64 {
            match self {
                Self::Cost => row.cost.unwrap_or(-1.0),
                Self::Tokens => row.tokens_total as f64,
                Self::FreshTokens => row.fresh_tokens as f64,
                Self::CacheRatio => row.cache_read_tokens as f64 / row.fresh_tokens.max(1) as f64,
                Self::Started => 0.0,
                Self::Turns => row.turns as f64,
                Self::Failures => row.validation_failures as f64,
                Self::Agents => row.agents as f64,
            }
        };
        if self == Self::Started {
            rows.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        } else {
            rows.sort_by(|a, b| {
                key(b)
                    .partial_cmp(&key(a))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.started_at.cmp(&a.started_at))
            });
        }
    }
}

/// Root sessions built from their tasks, with tree (incl. subagent) totals.
pub fn sessions(
    conn: &Connection,
    filters: &Filters,
    sort: SessionSort,
    limit: usize,
) -> Result<Vec<SessionRow>> {
    let task_rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let mut by_session: BTreeMap<(String, String), Vec<TaskRow>> = BTreeMap::new();
    for task in task_rows {
        by_session
            .entry((task.provider.clone(), task.session_id.clone()))
            .or_default()
            .push(task);
    }
    if by_session.is_empty() {
        return Ok(Vec::new());
    }

    let mut extra_stmt = conn.prepare(
        "SELECT s.ended_at, COALESCE(s.title, ''), COALESCE(s.source_status, 'ok'),
                (SELECT COUNT(*) FROM metadata_sessions c
                  WHERE c.provider = s.provider AND c.root_session_id = s.session_id
                    AND c.session_id != s.session_id),
                (SELECT COALESCE(SUM(cache_read_tokens), 0) FROM derived_usage_event u
                  WHERE u.provider = s.provider AND u.root_session_id = s.session_id),
                (SELECT SUM(reported_cost_usd) FROM derived_usage_event u
                  WHERE u.provider = s.provider AND u.root_session_id = s.session_id),
                (SELECT COUNT(*) FROM fact_session_usage fu
                  JOIN metadata_sessions c ON c.provider = fu.provider AND c.session_id = fu.session_id
                  WHERE fu.provider = s.provider
                    AND COALESCE(NULLIF(c.root_session_id, ''), c.session_id) = s.session_id
                    AND fu.token_source = 'legacy')
         FROM metadata_sessions s WHERE s.provider = ?1 AND s.session_id = ?2",
    )?;

    let mut rows = Vec::new();
    for ((provider, session_id), tasks) in by_session {
        let (ended_at, title, source_status, agents, cache_read, reported, legacy): (
            Option<String>,
            String,
            String,
            i64,
            i64,
            Option<f64>,
            i64,
        ) = extra_stmt.query_row(rusqlite::params![provider, session_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })?;
        let first = &tasks[tasks.len() - 1];
        let sum_opt = |f: &dyn Fn(&TaskRow) -> Option<f64>| -> Option<f64> {
            let values: Vec<f64> = tasks.iter().filter_map(f).collect();
            (!values.is_empty()).then(|| values.iter().sum())
        };
        let mut models: Vec<String> = Vec::new();
        for task in &tasks {
            for model in task.models.split(',').filter(|model| !model.is_empty()) {
                if !models.iter().any(|existing| existing == model) {
                    models.push(model.to_string());
                }
            }
        }
        let heaviest = tasks
            .iter()
            .max_by(|a, b| {
                a.cost_total
                    .unwrap_or(0.0)
                    .partial_cmp(&b.cost_total.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.tokens_total.cmp(&b.tokens_total))
            })
            .expect("session has tasks");
        rows.push(SessionRow {
            provider: provider.clone(),
            session_id: session_id.clone(),
            title: if title.trim().is_empty() {
                first.title.clone()
            } else {
                title
            },
            started_at: tasks
                .iter()
                .filter_map(|task| task.started_at.clone())
                .min(),
            ended_at,
            repo_root: first.repo_root.clone(),
            git_branch: first.git_branch.clone(),
            models: models.join(","),
            primary_model: heaviest.primary_model.clone(),
            tasks: tasks.len() as i64,
            turns: tasks.iter().map(|task| task.turns).sum(),
            agents,
            attempts: tasks.iter().map(|task| task.attempts).sum(),
            validation_failures: tasks.iter().map(|task| task.validation_failures).sum(),
            loop_attempts: tasks.iter().map(|task| task.loop_attempts).sum(),
            tokens_total: tasks.iter().map(|task| task.tokens_total).sum(),
            fresh_tokens: tasks.iter().map(|task| task.fresh_tokens).sum(),
            cache_read_tokens: cache_read,
            cost: sum_opt(&|task| task.cost_total),
            reported_cost: reported,
            subagent_cost: sum_opt(&|task| task.subagent_cost),
            unpriced_tokens: tasks.iter().map(|task| task.unpriced_tokens).sum(),
            estimated_tokens: tasks.iter().map(|task| task.estimated_tokens).sum(),
            active_minutes: sum_opt(&|task| task.active_minutes_total),
            completed_tasks: tasks.iter().filter(|task| task.is_completed()).count() as i64,
            commits: tasks.iter().map(|task| task.commits).max().unwrap_or(0),
            source_status,
            legacy_usage: legacy > 0,
            intent: if tasks.len() == 1 {
                heaviest.intent.clone()
            } else {
                mixed_label(tasks.iter().map(|task| task.intent.clone()))
            },
            outcome: heaviest.outcome.clone(),
            label_source: heaviest.outcome_source.clone(),
        });
    }
    sort.sort(&mut rows);
    rows.truncate(limit.max(1));
    Ok(rows)
}

fn mixed_label(labels: impl Iterator<Item = Option<String>>) -> Option<String> {
    let labels: Vec<String> = labels.flatten().collect();
    let first = labels.first()?.clone();
    if labels.iter().all(|label| *label == first) {
        Some(first)
    } else {
        Some("mixed".to_string())
    }
}

// ── Groups ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GroupDim {
    Tool,
    Model,
    Project,
    CostBand,
    TokenBand,
    Intent,
    Outcome,
    Day,
    Week,
    Branch,
}

impl GroupDim {
    pub const ALL: [GroupDim; 10] = [
        Self::CostBand,
        Self::TokenBand,
        Self::Model,
        Self::Tool,
        Self::Project,
        Self::Intent,
        Self::Outcome,
        Self::Branch,
        Self::Week,
        Self::Day,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::Model => "model",
            Self::Project => "project",
            Self::CostBand => "cost band",
            Self::TokenBand => "token band",
            Self::Intent => "intent",
            Self::Outcome => "outcome",
            Self::Day => "day",
            Self::Week => "week",
            Self::Branch => "branch",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "tool" | "harness" | "provider" => Self::Tool,
            "model" => Self::Model,
            "project" | "repo" => Self::Project,
            "cost" | "cost-band" | "cost_band" => Self::CostBand,
            "tokens" | "token-band" | "token_band" => Self::TokenBand,
            "intent" => Self::Intent,
            "outcome" => Self::Outcome,
            "day" => Self::Day,
            "week" => Self::Week,
            "branch" => Self::Branch,
            _ => return None,
        })
    }

    /// Group key of a task plus the filter that reproduces the group.
    pub fn key(self, task: &TaskRow) -> (String, Filters) {
        let mut filter = Filters::default();
        let key = match self {
            Self::Tool => {
                filter.providers = vec![task.provider.clone()];
                task.provider.clone()
            }
            Self::Model => {
                let model = task
                    .primary_model
                    .clone()
                    .unwrap_or_else(|| "(unknown)".to_string());
                filter.model = Some(model.clone());
                short_model(&model)
            }
            Self::Project => {
                let repo = task.repo_root.clone().unwrap_or_default();
                if !repo.is_empty() {
                    filter.repo = Some(repo.clone());
                }
                if repo.is_empty() {
                    "(no repo)".to_string()
                } else {
                    short_repo(&repo)
                }
            }
            Self::CostBand => {
                let band = CostBand::of(task.cost_total);
                filter.cost_band = Some(band);
                band.label().to_string()
            }
            Self::TokenBand => {
                let band = TokenBand::of(task.fresh_tokens);
                filter.token_band = Some(band);
                band.label().to_string()
            }
            Self::Intent => {
                let label = task
                    .intent
                    .clone()
                    .unwrap_or_else(|| "(unclassified)".to_string());
                if task.intent.is_some() {
                    filter.intent = Some(label.clone());
                }
                label
            }
            Self::Outcome => {
                let label = task
                    .outcome
                    .clone()
                    .unwrap_or_else(|| "(unclassified)".to_string());
                if task.outcome.is_some() {
                    filter.outcome = Some(label.clone());
                }
                label
            }
            Self::Day => {
                let day = task
                    .started_at
                    .as_deref()
                    .map(|ts| ts.chars().take(10).collect::<String>())
                    .unwrap_or_default();
                filter.from = Some(day.clone());
                filter.to = Some(day.clone());
                day
            }
            Self::Week => {
                let week = week_start(task.started_at.as_deref());
                if let Some(start) = week.as_deref() {
                    filter.from = Some(start.to_string());
                    filter.to = week_end(start);
                }
                week.unwrap_or_default()
            }
            Self::Branch => {
                let branch = task
                    .git_branch
                    .clone()
                    .unwrap_or_else(|| "(none)".to_string());
                filter.search = task.git_branch.clone();
                branch
            }
        };
        (key, filter)
    }
}

fn week_start(ts: Option<&str>) -> Option<String> {
    use chrono::Datelike;
    let date = chrono::NaiveDate::parse_from_str(ts?.get(..10)?, "%Y-%m-%d").ok()?;
    let start = date - chrono::Duration::days(date.weekday().num_days_from_monday() as i64);
    Some(start.format("%Y-%m-%d").to_string())
}

fn week_end(start: &str) -> Option<String> {
    let date = chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d").ok()?;
    Some(
        (date + chrono::Duration::days(6))
            .format("%Y-%m-%d")
            .to_string(),
    )
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GroupRow {
    pub key: String,
    #[serde(skip)]
    pub filter: Filters,
    pub tasks: i64,
    pub sessions: i64,
    pub cost: Option<f64>,
    pub share: f64,
    pub fresh_tokens: i64,
    pub tokens_total: i64,
    pub median_cost_per_task: Option<f64>,
    pub completed: i64,
    pub completion_rate: Option<f64>,
    pub median_cost_to_complete: Option<f64>,
    pub median_active_to_complete: Option<f64>,
    pub stalled: i64,
    pub loops: i64,
    pub unclassified: i64,
    pub heuristic_labels: i64,
    pub unpriced_tasks: i64,
}

pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

pub fn groups(conn: &Connection, filters: &Filters, dim: GroupDim) -> Result<Vec<GroupRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    Ok(group_tasks(&rows, dim, filters))
}

pub fn group_tasks(rows: &[TaskRow], dim: GroupDim, base: &Filters) -> Vec<GroupRow> {
    let total_cost: f64 = rows.iter().filter_map(|row| row.cost_total).sum();
    let mut grouped: BTreeMap<String, (Filters, Vec<&TaskRow>)> = BTreeMap::new();
    for row in rows {
        let (key, filter) = dim.key(row);
        grouped
            .entry(key)
            .or_insert_with(|| (merge_filters(base, &filter), Vec::new()))
            .1
            .push(row);
    }
    let mut out: Vec<GroupRow> = grouped
        .into_iter()
        .map(|(key, (filter, tasks))| summarize_tasks(&tasks, key, filter, total_cost))
        .collect();
    out.sort_by(|a, b| {
        b.cost
            .unwrap_or(-1.0)
            .partial_cmp(&a.cost.unwrap_or(-1.0))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.tasks.cmp(&a.tasks))
    });
    if matches!(dim, GroupDim::Day | GroupDim::Week) {
        out.sort_by(|a, b| b.key.cmp(&a.key));
    }
    if matches!(dim, GroupDim::CostBand) {
        out.sort_by_key(|row| CostBand::order_of_label(&row.key));
    }
    if matches!(dim, GroupDim::TokenBand) {
        out.sort_by_key(|row| TokenBand::order_of_label(&row.key));
    }
    out
}

/// Summary statistics for one group of tasks.
pub fn summarize_tasks(
    tasks: &[&TaskRow],
    key: String,
    filter: Filters,
    total_cost: f64,
) -> GroupRow {
    let cost_values: Vec<f64> = tasks.iter().filter_map(|task| task.cost_total).collect();
    let cost = (!cost_values.is_empty()).then(|| cost_values.iter().sum::<f64>());
    let completed: Vec<&&TaskRow> = tasks.iter().filter(|task| task.is_completed()).collect();
    let sessions = tasks
        .iter()
        .map(|task| (&task.provider, &task.session_id))
        .collect::<std::collections::BTreeSet<_>>()
        .len() as i64;
    GroupRow {
        key,
        filter,
        tasks: tasks.len() as i64,
        sessions,
        cost,
        share: if total_cost > 0.0 {
            cost.unwrap_or(0.0) / total_cost
        } else {
            0.0
        },
        fresh_tokens: tasks.iter().map(|task| task.fresh_tokens).sum(),
        tokens_total: tasks.iter().map(|task| task.tokens_total).sum(),
        median_cost_per_task: median(&mut cost_values.clone()),
        completed: completed.len() as i64,
        completion_rate: (!tasks.is_empty()).then(|| completed.len() as f64 / tasks.len() as f64),
        median_cost_to_complete: median(
            &mut completed
                .iter()
                .filter_map(|task| task.cost_to_complete)
                .collect::<Vec<_>>(),
        ),
        median_active_to_complete: median(
            &mut completed
                .iter()
                .filter_map(|task| task.active_minutes_to_complete)
                .collect::<Vec<_>>(),
        ),
        stalled: tasks
            .iter()
            .filter(|task| task.outcome.as_deref() == Some("stalled"))
            .count() as i64,
        loops: tasks.iter().filter(|task| task.loop_attempts > 0).count() as i64,
        unclassified: tasks.iter().filter(|task| task.intent.is_none()).count() as i64,
        heuristic_labels: tasks
            .iter()
            .filter(|task| task.outcome_source.as_deref() == Some("heuristic"))
            .count() as i64,
        unpriced_tasks: tasks
            .iter()
            .filter(|task| task.cost_total.is_none())
            .count() as i64,
    }
}

/// Child filters inherit the parent filter and add the group constraint.
pub fn merge_filters(base: &Filters, extra: &Filters) -> Filters {
    let mut merged = base.clone();
    if extra.from.is_some() {
        merged.from = extra.from.clone();
    }
    if extra.to.is_some() {
        merged.to = extra.to.clone();
    }
    if !extra.providers.is_empty() {
        merged.providers = extra.providers.clone();
    }
    merged.model = extra.model.clone().or(merged.model);
    merged.repo = extra.repo.clone().or(merged.repo);
    merged.search = extra.search.clone().or(merged.search);
    merged.intent = extra.intent.clone().or(merged.intent);
    merged.outcome = extra.outcome.clone().or(merged.outcome);
    merged.cost_band = extra.cost_band.or(merged.cost_band);
    merged.token_band = extra.token_band.or(merged.token_band);
    merged.min_cost = extra.min_cost.or(merged.min_cost);
    merged.session = extra.session.clone().or(merged.session);
    if !extra.task_keys.is_empty() {
        merged.task_keys = extra.task_keys.clone();
        merged.task_keys_label = extra.task_keys_label.clone();
    }
    merged
}

// ── Usage timeline and quota ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Bucket {
    Hour,
    Day,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UsageBucket {
    pub bucket: String,
    pub provider: String,
    pub tokens: i64,
    pub fresh_tokens: i64,
    pub cost: f64,
    pub unpriced_tokens: i64,
    pub background_tokens: i64,
    pub sessions: i64,
}

fn usage_where(filters: &Filters) -> Where {
    let mut w = Where::default();
    if let Some(from) = &filters.from {
        w.push("date(u.usage_ts) >= date(?)", from.clone());
    }
    if let Some(to) = &filters.to {
        w.push("date(u.usage_ts) <= date(?)", to.clone());
    }
    if !filters.providers.is_empty() {
        let placeholders: Vec<String> = filters
            .providers
            .iter()
            .map(|provider| {
                w.params.push(Box::new(provider.clone()));
                format!("?{}", w.params.len())
            })
            .collect();
        w.raw(&format!("u.provider IN ({})", placeholders.join(",")));
    }
    if let Some(model) = &filters.model {
        w.push("u.model_name LIKE ?", format!("%{model}%"));
    }
    if let Some(repo) = &filters.repo {
        w.push(
            "EXISTS (SELECT 1 FROM derived_task t WHERE t.provider = u.provider AND t.session_id = u.root_session_id AND t.repo_root = ?)",
            repo.clone(),
        );
    }
    if let Some((provider, session)) = &filters.session {
        w.push("u.provider = ?", provider.clone());
        w.push("u.root_session_id = ?", session.clone());
    }
    w
}

pub fn usage_timeline(
    conn: &Connection,
    filters: &Filters,
    bucket: Bucket,
) -> Result<Vec<UsageBucket>> {
    let w = usage_where(filters);
    let format = match bucket {
        Bucket::Hour => "%Y-%m-%d %H:00",
        Bucket::Day => "%Y-%m-%d",
    };
    let sql = format!(
        "SELECT strftime('{format}', u.usage_ts) AS bucket, u.provider,
                SUM(u.total_tokens), SUM(u.fresh_tokens), COALESCE(SUM(u.api_equiv_usd), 0),
                SUM(CASE WHEN u.api_equiv_usd IS NULL THEN u.total_tokens ELSE 0 END),
                SUM(CASE WHEN u.background_flag = 1 THEN u.total_tokens ELSE 0 END),
                COUNT(DISTINCT u.root_session_id)
         FROM derived_usage_event u
         WHERE u.usage_ts IS NOT NULL AND {}
         GROUP BY bucket, u.provider
         ORDER BY bucket, u.provider",
        w.sql()
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            Ok(UsageBucket {
                bucket: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                provider: row.get(1)?,
                tokens: row.get(2)?,
                fresh_tokens: row.get(3)?,
                cost: row.get(4)?,
                unpriced_tokens: row.get(5)?,
                background_tokens: row.get(6)?,
                sessions: row.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UsageTotals {
    pub key: String,
    pub tokens: i64,
    pub fresh_tokens: i64,
    pub cache_read_tokens: i64,
    pub output_tokens: i64,
    pub cost: Option<f64>,
    pub reported_cost: Option<f64>,
    pub unpriced_tokens: i64,
    pub estimated_tokens: i64,
    pub sessions: i64,
    pub calls: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum UsageDim {
    Tool,
    Model,
    Project,
    Session,
    AgentKind,
    Day,
    Hour,
}

impl UsageDim {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "tool" | "provider" | "harness" => Self::Tool,
            "model" => Self::Model,
            "project" | "repo" => Self::Project,
            "session" => Self::Session,
            "agent" | "agent-kind" => Self::AgentKind,
            "day" => Self::Day,
            "hour" => Self::Hour,
            _ => return None,
        })
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Tool => "u.provider",
            Self::Model => "COALESCE(u.model_name, '(unknown)')",
            Self::Project => {
                "COALESCE((SELECT t.repo_root FROM derived_task t WHERE t.provider = u.provider AND t.session_id = u.root_session_id LIMIT 1), '(no repo)')"
            }
            Self::Session => "u.provider || ':' || u.root_session_id",
            Self::AgentKind => {
                "CASE WHEN u.session_id = u.root_session_id THEN 'main' ELSE 'subagent' END"
            }
            Self::Day => "strftime('%Y-%m-%d', u.usage_ts)",
            Self::Hour => "strftime('%Y-%m-%d %H:00', u.usage_ts)",
        }
    }
}

/// Additive usage totals: every event is counted once, under exactly one key.
pub fn usage_totals(
    conn: &Connection,
    filters: &Filters,
    dim: UsageDim,
) -> Result<Vec<UsageTotals>> {
    let w = usage_where(filters);
    let sql = format!(
        "SELECT {} AS key, SUM(u.total_tokens), SUM(u.fresh_tokens), SUM(u.cache_read_tokens),
                SUM(u.output_tokens), SUM(u.api_equiv_usd), SUM(u.reported_cost_usd),
                SUM(CASE WHEN u.api_equiv_usd IS NULL THEN u.total_tokens ELSE 0 END),
                SUM(CASE WHEN u.token_source != 'reported' THEN u.total_tokens ELSE 0 END),
                COUNT(DISTINCT u.provider || u.root_session_id), COUNT(*)
         FROM derived_usage_event u
         WHERE {}
         GROUP BY key
         ORDER BY COALESCE(SUM(u.api_equiv_usd), 0) DESC, SUM(u.total_tokens) DESC",
        dim.sql(),
        w.sql()
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            Ok(UsageTotals {
                key: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                tokens: row.get(1)?,
                fresh_tokens: row.get(2)?,
                cache_read_tokens: row.get(3)?,
                output_tokens: row.get(4)?,
                cost: row.get(5)?,
                reported_cost: row.get(6)?,
                unpriced_tokens: row.get(7)?,
                estimated_tokens: row.get(8)?,
                sessions: row.get(9)?,
                calls: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct QuotaWindowRow {
    pub window_id: i64,
    pub provider: String,
    pub name: String,
    pub observed: bool,
    pub window_start: Option<String>,
    pub resets_at: Option<String>,
    pub first_seen: String,
    pub last_seen: String,
    pub start_percent: Option<f64>,
    pub end_percent: Option<f64>,
    pub reached_limit_at: Option<String>,
    pub unobserved_minutes: i64,
    pub tokens: i64,
    pub fresh_tokens: i64,
    pub cost: Option<f64>,
    pub limit_hits_logged: i64,
    pub plan_type: Option<String>,
}

/// Observed (Codex) windows plus estimated windows for tools without quota
/// telemetry, newest first.
pub fn quota_windows(conn: &Connection, filters: &Filters) -> Result<Vec<QuotaWindowRow>> {
    let mut rows = Vec::new();
    let mut w = Where::default();
    if let Some(from) = &filters.from {
        w.push("date(q.last_seen) >= date(?)", from.clone());
    }
    if let Some(to) = &filters.to {
        w.push("date(q.first_seen) <= date(?)", to.clone());
    }
    let sql = format!(
        "SELECT q.window_id, q.provider, q.window_kind || ' ' ||
                CASE WHEN q.window_minutes >= 1440 THEN (q.window_minutes / 1440) || 'd'
                     ELSE (q.window_minutes / 60) || 'h' END,
                q.window_start, q.resets_at, q.first_seen, q.last_seen, q.start_percent,
                q.end_percent, q.reached_limit_at, q.unobserved_minutes, q.plan_type,
                COALESCE((SELECT SUM(a.tokens) FROM derived_quota_allocation a WHERE a.window_id = q.window_id), 0),
                (SELECT SUM(a.api_equiv_usd) FROM derived_quota_allocation a WHERE a.window_id = q.window_id)
         FROM derived_quota_window q WHERE {} ORDER BY q.last_seen DESC",
        w.sql()
    );
    let mut stmt = conn.prepare(&sql)?;
    let observed = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            Ok(QuotaWindowRow {
                window_id: row.get(0)?,
                provider: row.get(1)?,
                name: row.get(2)?,
                observed: true,
                window_start: row.get(3)?,
                resets_at: row.get(4)?,
                first_seen: row.get(5)?,
                last_seen: row.get(6)?,
                start_percent: row.get(7)?,
                end_percent: row.get(8)?,
                reached_limit_at: row.get(9)?,
                unobserved_minutes: row.get(10)?,
                plan_type: row.get(11)?,
                tokens: row.get(12)?,
                fresh_tokens: 0,
                cost: row.get(13)?,
                limit_hits_logged: 0,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.extend(
        observed.into_iter().filter(|row| {
            filters.providers.is_empty() || filters.providers.contains(&row.provider)
        }),
    );

    let billing = crate::insights::billing::config();
    let observed_providers: Vec<String> = rows.iter().map(|row| row.provider.clone()).collect();
    for provider in ["claude", "cursor", "opencode", "codex"] {
        if observed_providers.iter().any(|p| p == provider) {
            continue;
        }
        if !filters.providers.is_empty() && !filters.providers.iter().any(|p| p == provider) {
            continue;
        }
        for shape in billing.windows(provider) {
            rows.extend(estimated_windows(
                conn,
                filters,
                provider,
                &shape.name,
                shape.minutes,
            )?);
        }
    }
    rows.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    Ok(rows)
}

/// Token burn per window for a provider without quota telemetry. A window
/// starts at the first event after the previous window ended.
fn estimated_windows(
    conn: &Connection,
    filters: &Filters,
    provider: &str,
    name: &str,
    minutes: i64,
) -> Result<Vec<QuotaWindowRow>> {
    let mut provider_filter = filters.clone();
    provider_filter.providers = vec![provider.to_string()];
    let w = usage_where(&provider_filter);
    let sql = format!(
        "SELECT u.usage_ts, u.total_tokens, u.fresh_tokens, u.api_equiv_usd
         FROM derived_usage_event u WHERE u.usage_ts IS NOT NULL AND {} ORDER BY u.usage_ts",
        w.sql()
    );
    let mut stmt = conn.prepare(&sql)?;
    let events = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<f64>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let observations: Vec<i64> = conn
        .prepare("SELECT observed_at FROM fact_quota_observation WHERE provider = ?1 AND kind = 'limit_hit'")?
        .query_map([provider], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .filter_map(|ts| crate::insights::structure::parse_ts(&ts))
        .collect();

    let mut windows: Vec<QuotaWindowRow> = Vec::new();
    let mut current_end: Option<i64> = None;
    for (ts_text, tokens, fresh, cost) in events {
        let Some(ts) = crate::insights::structure::parse_ts(&ts_text) else {
            continue;
        };
        if current_end.is_none_or(|end| ts >= end) {
            let end = ts + minutes * 60_000;
            current_end = Some(end);
            windows.push(QuotaWindowRow {
                window_id: -(windows.len() as i64) - 1,
                provider: provider.to_string(),
                name: format!("{name} (estimated)"),
                observed: false,
                window_start: Some(crate::insights::structure::ms_to_iso(ts)),
                resets_at: Some(crate::insights::structure::ms_to_iso(end)),
                first_seen: ts_text.clone(),
                last_seen: ts_text.clone(),
                start_percent: None,
                end_percent: None,
                reached_limit_at: None,
                unobserved_minutes: 0,
                tokens: 0,
                fresh_tokens: 0,
                cost: None,
                limit_hits_logged: observations
                    .iter()
                    .filter(|hit| **hit >= ts && **hit < end)
                    .count() as i64,
                plan_type: crate::insights::billing::config()
                    .providers
                    .get(provider)
                    .and_then(|billing| billing.plan.clone()),
            });
        }
        let window = windows.last_mut().expect("window exists");
        window.tokens += tokens;
        window.fresh_tokens += fresh;
        window.last_seen = ts_text;
        if let Some(cost) = cost {
            *window.cost.get_or_insert(0.0) += cost;
        }
    }
    Ok(windows)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct QuotaContributor {
    pub provider: String,
    pub root_session_id: String,
    pub title: String,
    pub delta_percent: Option<f64>,
    pub tokens: i64,
    pub cost: Option<f64>,
    pub background_tokens: i64,
    pub subagent_tokens: i64,
    pub outcome: Option<String>,
}

/// Sessions that consumed an observed window (allocated percentages) or an
/// estimated window (time range), largest first.
pub fn window_contributors(
    conn: &Connection,
    window: &QuotaWindowRow,
) -> Result<Vec<QuotaContributor>> {
    let sql = if window.observed {
        "SELECT a.provider, a.root_session_id, SUM(a.delta_percent), SUM(a.tokens), SUM(a.api_equiv_usd),
                SUM(a.background_tokens),
                SUM(CASE WHEN a.session_id != a.root_session_id THEN a.tokens ELSE 0 END)
         FROM derived_quota_allocation a WHERE a.window_id = ?1
         GROUP BY a.provider, a.root_session_id
         ORDER BY SUM(a.delta_percent) DESC"
            .to_string()
    } else {
        "SELECT u.provider, u.root_session_id, NULL, SUM(u.total_tokens), SUM(u.api_equiv_usd),
                SUM(CASE WHEN u.background_flag = 1 THEN u.total_tokens ELSE 0 END),
                SUM(CASE WHEN u.session_id != u.root_session_id THEN u.total_tokens ELSE 0 END)
         FROM derived_usage_event u
         WHERE u.provider = ?2 AND u.usage_ts >= ?3 AND u.usage_ts < ?4 AND ?1 IS NOT NULL
         GROUP BY u.provider, u.root_session_id
         ORDER BY SUM(u.total_tokens) DESC"
            .to_string()
    };
    let mut stmt = conn.prepare(&sql)?;
    let start = window.window_start.clone().unwrap_or_default();
    let end = window.resets_at.clone().unwrap_or_default();
    let mut rows = if window.observed {
        stmt.query_map(rusqlite::params![window.window_id], contributor_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        stmt.query_map(
            rusqlite::params![window.window_id, window.provider, start, end],
            contributor_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for row in &mut rows {
        let (title, outcome): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT COALESCE(NULLIF(s.title, ''), (SELECT t.title FROM derived_task t
                          WHERE t.provider = s.provider AND t.session_id = s.session_id
                          ORDER BY t.task_index LIMIT 1)),
                        (SELECT l.label FROM view_label_effective l
                          WHERE l.unit_kind = 'task' AND l.question_id = 'outcome'
                            AND l.unit_key = s.provider || '|' || s.session_id || '|0')
                 FROM metadata_sessions s WHERE s.provider = ?1 AND s.session_id = ?2",
                rusqlite::params![row.provider, row.root_session_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap_or((None, None));
        row.title = title.unwrap_or_default();
        row.outcome = outcome;
    }
    Ok(rows)
}

fn contributor_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<QuotaContributor> {
    Ok(QuotaContributor {
        provider: row.get(0)?,
        root_session_id: row.get(1)?,
        title: String::new(),
        delta_percent: row.get(2)?,
        tokens: row.get(3)?,
        cost: row.get(4)?,
        background_tokens: row.get(5)?,
        subagent_tokens: row.get(6)?,
        outcome: None,
    })
}

/// Hours whose cost exceeds both 3x the median hourly cost and the 95th
/// percentile, per provider.
pub fn usage_spikes(buckets: &[UsageBucket]) -> Vec<UsageBucket> {
    let mut by_provider: BTreeMap<&str, Vec<&UsageBucket>> = BTreeMap::new();
    for bucket in buckets {
        by_provider
            .entry(bucket.provider.as_str())
            .or_default()
            .push(bucket);
    }
    let mut spikes = Vec::new();
    for (_, rows) in by_provider {
        let mut values: Vec<f64> = rows
            .iter()
            .map(|row| {
                if row.cost > 0.0 {
                    row.cost
                } else {
                    row.fresh_tokens as f64
                }
            })
            .collect();
        if values.len() < 6 {
            continue;
        }
        let med = median(&mut values.clone()).unwrap_or(0.0);
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p95 = values[((values.len() as f64) * 0.95) as usize - 1];
        for row in rows {
            let value = if row.cost > 0.0 {
                row.cost
            } else {
                row.fresh_tokens as f64
            };
            if value > med * 3.0 && value >= p95 {
                spikes.push((*row).clone());
            }
        }
    }
    spikes.sort_by(|a, b| b.bucket.cmp(&a.bucket));
    spikes
}

// ── Context ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ContextDim {
    Kind,
    Tool,
    Dir,
    Mcp,
    Instruction,
    Purpose,
    Target,
}

impl ContextDim {
    pub const ALL: [ContextDim; 7] = [
        Self::Kind,
        Self::Tool,
        Self::Dir,
        Self::Mcp,
        Self::Instruction,
        Self::Purpose,
        Self::Target,
    ];

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "kind" => Self::Kind,
            "tool" => Self::Tool,
            "dir" | "directory" => Self::Dir,
            "mcp" => Self::Mcp,
            "instruction" | "instructions" => Self::Instruction,
            "purpose" => Self::Purpose,
            "target" | "item" => Self::Target,
            _ => return None,
        })
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Kind => "kind",
            Self::Tool => "tool",
            Self::Dir => "directory",
            Self::Mcp => "mcp server",
            Self::Instruction => "instruction ref",
            Self::Purpose => "read purpose",
            Self::Target => "target",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Kind => "c.kind",
            Self::Tool => "COALESCE(c.tool_name, c.kind)",
            Self::Dir => "COALESCE(c.target_dir, '(' || COALESCE(c.tool_category, c.kind) || ')')",
            Self::Mcp => "COALESCE(c.mcp_server, '(not mcp)')",
            Self::Instruction => "COALESCE(c.instruction_ref, '(none)')",
            Self::Purpose => "COALESCE(c.purpose, '(n/a)')",
            Self::Target => "COALESCE(c.target, c.kind)",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ContextGroupRow {
    pub key: String,
    pub items: i64,
    pub sessions: i64,
    pub tokens: i64,
    pub write_usd: Option<f64>,
    pub carry_usd: Option<f64>,
    pub rebuild_usd: Option<f64>,
    pub total_usd: Option<f64>,
    pub per_session_usd: Option<f64>,
    pub unused_share: Option<f64>,
    pub avg_carried_calls: f64,
}

fn context_where(filters: &Filters) -> Where {
    let mut w = Where::default();
    if let Some(from) = &filters.from {
        w.push(
            "date(COALESCE(c.ts, s.started_at)) >= date(?)",
            from.clone(),
        );
    }
    if let Some(to) = &filters.to {
        w.push("date(COALESCE(c.ts, s.started_at)) <= date(?)", to.clone());
    }
    if !filters.providers.is_empty() {
        let placeholders: Vec<String> = filters
            .providers
            .iter()
            .map(|provider| {
                w.params.push(Box::new(provider.clone()));
                format!("?{}", w.params.len())
            })
            .collect();
        w.raw(&format!("c.provider IN ({})", placeholders.join(",")));
    }
    if let Some(repo) = &filters.repo {
        w.push("r.repo_root = ?", repo.clone());
    }
    if let Some((provider, session)) = &filters.session {
        w.push("c.provider = ?", provider.clone());
        w.push(
            "COALESCE(NULLIF(s.root_session_id, ''), s.session_id) = ?",
            session.clone(),
        );
    }
    if let Some(search) = &filters.search {
        w.push("COALESCE(c.target, '') LIKE ?", format!("%{search}%"));
    }
    w
}

pub fn context_groups(
    conn: &Connection,
    filters: &Filters,
    dim: ContextDim,
) -> Result<Vec<ContextGroupRow>> {
    let w = context_where(filters);
    let sql = format!(
        "SELECT {} AS key, COUNT(*), COUNT(DISTINCT c.provider || c.session_id), SUM(c.tokens),
                SUM(c.write_usd), SUM(c.carry_usd), SUM(c.rebuild_usd), SUM(c.total_usd),
                SUM(CASE WHEN c.purpose IN ('unused', 'startup_unused') THEN c.tokens ELSE 0 END) * 1.0
                    / NULLIF(SUM(CASE WHEN c.purpose IS NOT NULL THEN c.tokens ELSE 0 END), 0),
                AVG(c.carried_calls)
         FROM derived_context_item c
         JOIN metadata_sessions s ON s.provider = c.provider AND s.session_id = c.session_id
         LEFT JOIN metadata_repositories r ON r.id = s.repository_id
         WHERE {}
         GROUP BY key
         ORDER BY COALESCE(SUM(c.total_usd), 0) DESC, SUM(c.tokens) DESC
         LIMIT 500",
        dim.sql(),
        w.sql()
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            let total: Option<f64> = row.get(7)?;
            let sessions: i64 = row.get(2)?;
            Ok(ContextGroupRow {
                key: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                items: row.get(1)?,
                sessions,
                tokens: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
                write_usd: row.get(4)?,
                carry_usd: row.get(5)?,
                rebuild_usd: row.get(6)?,
                total_usd: total,
                per_session_usd: total.map(|total| total / sessions.max(1) as f64),
                unused_share: row.get(8)?,
                avg_carried_calls: row.get::<_, Option<f64>>(9)?.unwrap_or(0.0),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ContextItemRow {
    pub provider: String,
    pub session_id: String,
    pub item_key: String,
    pub ts: Option<String>,
    pub kind: String,
    pub tool_name: Option<String>,
    pub target: Option<String>,
    pub tokens: i64,
    pub entered_call: i64,
    pub carried_calls: i64,
    pub write_usd: Option<f64>,
    pub carry_usd: Option<f64>,
    pub rebuild_usd: Option<f64>,
    pub total_usd: Option<f64>,
    pub purpose: Option<String>,
    pub instruction_ref: Option<String>,
}

pub fn context_items(
    conn: &Connection,
    filters: &Filters,
    limit: usize,
) -> Result<Vec<ContextItemRow>> {
    context_items_for(conn, filters, None, limit)
}

/// Context items, optionally restricted to one group of a context dimension.
pub fn context_items_for(
    conn: &Connection,
    filters: &Filters,
    group: Option<(ContextDim, &str)>,
    limit: usize,
) -> Result<Vec<ContextItemRow>> {
    let mut w = context_where(filters);
    if let Some((dim, key)) = group {
        w.push(&format!("{} = ?", dim.sql()), key.to_string());
    }
    let sql = format!(
        "SELECT c.provider, c.session_id, c.item_key, c.ts, c.kind, c.tool_name, c.target, c.tokens,
                c.entered_call, c.carried_calls, c.write_usd, c.carry_usd, c.rebuild_usd,
                c.total_usd, c.purpose, c.instruction_ref
         FROM derived_context_item c
         JOIN metadata_sessions s ON s.provider = c.provider AND s.session_id = c.session_id
         LEFT JOIN metadata_repositories r ON r.id = s.repository_id
         WHERE {}
         ORDER BY COALESCE(c.total_usd, 0) DESC, c.tokens DESC
         LIMIT {}",
        w.sql(),
        limit.max(1)
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.params.iter()), |row| {
            Ok(ContextItemRow {
                provider: row.get(0)?,
                session_id: row.get(1)?,
                item_key: row.get(2)?,
                ts: row.get(3)?,
                kind: row.get(4)?,
                tool_name: row.get(5)?,
                target: row.get(6)?,
                tokens: row.get(7)?,
                entered_call: row.get(8)?,
                carried_calls: row.get(9)?,
                write_usd: row.get(10)?,
                carry_usd: row.get(11)?,
                rebuild_usd: row.get(12)?,
                total_usd: row.get(13)?,
                purpose: row.get(14)?,
                instruction_ref: row.get(15)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Session detail ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TurnRow {
    pub session_id: String,
    pub turn_index: i64,
    pub started_at: Option<String>,
    pub user_text: String,
    pub user_chars: i64,
    pub assistant_chars: i64,
    pub tool_calls: i64,
    pub edit_calls: i64,
    pub validation_runs: i64,
    pub validation_failures: i64,
    pub tokens: i64,
    pub cost: f64,
    pub models: String,
    pub feedback: Option<String>,
    pub feedback_source: Option<String>,
    pub task_index: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AttemptRow {
    pub task_index: i64,
    pub attempt_index: i64,
    pub turn_index: Option<i64>,
    pub edit_calls: i64,
    pub lines_changed: i64,
    pub validation_kind: Option<String>,
    pub validation_result: String,
    pub error_signature: Option<String>,
    pub ended_by: String,
    pub loop_flag: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AgentRow {
    pub session_id: String,
    pub agent_kind: String,
    pub agent_type: Option<String>,
    pub title: Option<String>,
    pub models: String,
    pub turns: i64,
    pub tokens: i64,
    pub fresh_tokens: i64,
    pub cost: Option<f64>,
    pub background_tokens: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ModelUsageRow {
    pub model: String,
    pub calls: i64,
    pub fresh_tokens: i64,
    pub cache_read_tokens: i64,
    pub output_tokens: i64,
    pub cost: Option<f64>,
    pub price_match: Option<String>,
    pub token_source: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventRow {
    pub ts: Option<String>,
    pub kind: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CommitRow {
    pub repo_root: String,
    pub sha: String,
    pub commit_time: String,
    pub subject: String,
    pub ai_share: f64,
    pub matched_lines: f64,
    pub merged_to_mainline: Option<bool>,
    pub reverted: Option<bool>,
    pub fix_signal: Option<String>,
    pub pr_number: Option<i64>,
    pub pr_merged: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CallRow {
    pub call_index: i64,
    pub ts: Option<String>,
    pub context_tokens: i64,
    pub new_tokens: i64,
    pub cache_event: String,
    pub input_cost: Option<f64>,
    pub rebuild_extra: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SessionDetail {
    pub session: SessionRow,
    pub tasks: Vec<TaskRow>,
    pub turns: Vec<TurnRow>,
    pub attempts: Vec<AttemptRow>,
    pub agents: Vec<AgentRow>,
    pub models: Vec<ModelUsageRow>,
    pub events: Vec<EventRow>,
    pub commits: Vec<CommitRow>,
    pub calls: Vec<CallRow>,
    pub context_items: Vec<ContextItemRow>,
    pub context_kinds: Vec<ContextGroupRow>,
}

pub fn session_detail(
    conn: &Connection,
    provider: &str,
    session_id: &str,
) -> Result<Option<SessionDetail>> {
    let filters = Filters {
        session: Some((provider.to_string(), session_id.to_string())),
        ..Filters::default()
    };
    let Some(session) = sessions(conn, &filters, SessionSort::Started, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let mut tasks = tasks(conn, &filters, TaskSort::Started, 10_000)?;
    tasks.sort_by_key(|task| task.task_index);

    let task_of_turn = |turn: i64| -> Option<i64> {
        tasks
            .iter()
            .find(|task| {
                task_turn_list(conn, &task.provider, &task.session_id, task.task_index)
                    .is_some_and(|turns| turns.contains(&turn))
            })
            .map(|task| task.task_index)
    };

    let mut stmt = conn.prepare(
        "SELECT d.session_id, d.turn_index, d.started_at, COALESCE(d.user_text_head, ''), d.user_chars,
                d.assistant_chars, d.tool_calls, d.edit_calls, d.validation_runs,
                d.validation_failures, d.tokens_total, d.api_equiv_usd, COALESCE(d.model_names, ''),
                l.label, l.source
         FROM derived_turn d
         LEFT JOIN view_label_effective l
           ON l.unit_kind = 'exchange' AND l.question_id = 'feedback'
          AND l.unit_key = d.provider || '|' || d.session_id || '|' || d.turn_index
         WHERE d.provider = ?1 AND d.session_id = ?2
         ORDER BY d.turn_index",
    )?;
    let mut turns = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(TurnRow {
                session_id: row.get(0)?,
                turn_index: row.get(1)?,
                started_at: row.get(2)?,
                user_text: row.get(3)?,
                user_chars: row.get(4)?,
                assistant_chars: row.get(5)?,
                tool_calls: row.get(6)?,
                edit_calls: row.get(7)?,
                validation_runs: row.get(8)?,
                validation_failures: row.get(9)?,
                tokens: row.get(10)?,
                cost: row.get(11)?,
                models: row.get(12)?,
                feedback: row.get(13)?,
                feedback_source: row.get(14)?,
                task_index: None,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for turn in &mut turns {
        turn.task_index = task_of_turn(turn.turn_index);
    }

    let mut stmt = conn.prepare(
        "SELECT task_index, attempt_index, turn_index, edit_calls, lines_changed, validation_kind,
                validation_result, error_signature, ended_by, loop_flag
         FROM derived_attempt WHERE provider = ?1 AND session_id = ?2
         ORDER BY task_index, attempt_index",
    )?;
    let attempts = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(AttemptRow {
                task_index: row.get(0)?,
                attempt_index: row.get(1)?,
                turn_index: row.get(2)?,
                edit_calls: row.get(3)?,
                lines_changed: row.get(4)?,
                validation_kind: row.get(5)?,
                validation_result: row.get(6)?,
                error_signature: row.get(7)?,
                ended_by: row.get(8)?,
                loop_flag: row.get::<_, i64>(9)? != 0,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut stmt = conn.prepare(
        "SELECT s.session_id, COALESCE(s.agent_kind, 'main'), s.agent_type, s.title,
                COALESCE((SELECT GROUP_CONCAT(DISTINCT u.model_name) FROM derived_usage_event u
                          WHERE u.provider = s.provider AND u.session_id = s.session_id), ''),
                (SELECT COUNT(*) FROM derived_turn d WHERE d.provider = s.provider AND d.session_id = s.session_id),
                COALESCE((SELECT SUM(u.total_tokens) FROM derived_usage_event u WHERE u.provider = s.provider AND u.session_id = s.session_id), 0),
                COALESCE((SELECT SUM(u.fresh_tokens) FROM derived_usage_event u WHERE u.provider = s.provider AND u.session_id = s.session_id), 0),
                (SELECT SUM(u.api_equiv_usd) FROM derived_usage_event u WHERE u.provider = s.provider AND u.session_id = s.session_id),
                COALESCE((SELECT SUM(u.total_tokens) FROM derived_usage_event u WHERE u.provider = s.provider AND u.session_id = s.session_id AND u.background_flag = 1), 0)
         FROM metadata_sessions s
         WHERE s.provider = ?1 AND COALESCE(NULLIF(s.root_session_id, ''), s.session_id) = ?2
         ORDER BY CASE WHEN s.session_id = ?2 THEN 0 ELSE 1 END, s.started_at",
    )?;
    let agents = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(AgentRow {
                session_id: row.get(0)?,
                agent_kind: row.get(1)?,
                agent_type: row.get(2)?,
                title: row.get(3)?,
                models: row.get(4)?,
                turns: row.get(5)?,
                tokens: row.get(6)?,
                fresh_tokens: row.get(7)?,
                cost: row.get(8)?,
                background_tokens: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut stmt = conn.prepare(
        "SELECT COALESCE(model_name, '(unknown)'), COUNT(*), SUM(fresh_tokens), SUM(cache_read_tokens),
                SUM(output_tokens), SUM(api_equiv_usd), MAX(price_match), MIN(token_source)
         FROM derived_usage_event WHERE provider = ?1 AND root_session_id = ?2
         GROUP BY 1 ORDER BY COALESCE(SUM(api_equiv_usd), 0) DESC",
    )?;
    let models = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(ModelUsageRow {
                model: row.get(0)?,
                calls: row.get(1)?,
                fresh_tokens: row.get(2)?,
                cache_read_tokens: row.get(3)?,
                output_tokens: row.get(4)?,
                cost: row.get(5)?,
                price_match: row.get(6)?,
                token_source: row.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut stmt = conn.prepare(
        "SELECT ts, kind, detail FROM fact_session_event
         WHERE provider = ?1 AND session_id = ?2 ORDER BY event_index",
    )?;
    let events = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(EventRow {
                ts: row.get(0)?,
                kind: row.get(1)?,
                detail: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let commits = session_commits(conn, provider, session_id)?;

    let mut stmt = conn.prepare(
        "SELECT call_index, ts, context_tokens, new_tokens, cache_event, input_cost_usd, rebuild_extra_usd
         FROM derived_api_call WHERE provider = ?1 AND session_id = ?2 ORDER BY call_index",
    )?;
    let calls = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(CallRow {
                call_index: row.get(0)?,
                ts: row.get(1)?,
                context_tokens: row.get(2)?,
                new_tokens: row.get(3)?,
                cache_event: row.get(4)?,
                input_cost: row.get(5)?,
                rebuild_extra: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let own = Filters {
        session: Some((provider.to_string(), session_id.to_string())),
        ..Filters::default()
    };
    let context_items = context_items(conn, &own, 200)?
        .into_iter()
        .filter(|item| item.session_id == session_id)
        .collect();
    let context_kinds = context_groups(conn, &own, ContextDim::Kind)?;

    Ok(Some(SessionDetail {
        session,
        tasks,
        turns,
        attempts,
        agents,
        models,
        events,
        commits,
        calls,
        context_items,
        context_kinds,
    }))
}

fn task_turn_list(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    task_index: i64,
) -> Option<Vec<i64>> {
    let list: String = conn
        .query_row(
            "SELECT turn_list FROM derived_task WHERE provider = ?1 AND session_id = ?2 AND task_index = ?3",
            rusqlite::params![provider, session_id, task_index],
            |row| row.get(0),
        )
        .ok()?;
    Some(
        list.split(',')
            .filter_map(|value| value.parse().ok())
            .collect(),
    )
}

/// Commits matched to the session or its subagents, with delivery outcomes.
pub fn session_commits(
    conn: &Connection,
    provider: &str,
    session_id: &str,
) -> Result<Vec<CommitRow>> {
    let mut stmt = conn.prepare(
        "SELECT c.repo_root, c.commit_sha, c.commit_time, c.subject, COALESCE(c.ai_share, 0),
                SUM(sm.matched_lines),
                o.merged_to_mainline_flag, o.reverted_later_flag, b.signal_source,
                pr.pr_number, pr.pr_merged_flag
         FROM fact_commit_session_match sm
         JOIN metadata_sessions s ON s.provider = sm.provider AND s.session_id = sm.session_id
         JOIN fact_commit c ON c.repo_root = sm.repo_root AND c.commit_sha = sm.commit_sha
         LEFT JOIN event_commit_outcome o ON o.repo_root = c.repo_root AND o.commit_sha = c.commit_sha
         LEFT JOIN event_commit_bug_signal b ON b.repo_root = c.repo_root AND b.commit_sha = c.commit_sha
              AND b.bug_after_merge_flag = 1
         LEFT JOIN event_commit_pr_outcome pr ON pr.repo_root = c.repo_root AND pr.commit_sha = c.commit_sha
         WHERE sm.provider = ?1 AND COALESCE(NULLIF(s.root_session_id, ''), s.session_id) = ?2
           AND sm.matched_lines > 0
         GROUP BY c.repo_root, c.commit_sha
         ORDER BY c.commit_time",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![provider, session_id], |row| {
            Ok(CommitRow {
                repo_root: row.get(0)?,
                sha: row.get(1)?,
                commit_time: row.get(2)?,
                subject: row.get(3)?,
                ai_share: row.get(4)?,
                matched_lines: row.get(5)?,
                merged_to_mainline: row.get::<_, Option<i64>>(6)?.map(|v| v != 0),
                reverted: row.get::<_, Option<i64>>(7)?.map(|v| v != 0),
                fix_signal: row.get(8)?,
                pr_number: row.get(9)?,
                pr_merged: row.get::<_, Option<i64>>(10)?.map(|v| v != 0),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(
        provider: &str,
        cost: Option<f64>,
        fresh: i64,
        completed: bool,
        model: &str,
    ) -> TaskRow {
        TaskRow {
            provider: provider.into(),
            session_id: format!("{provider}-{fresh}"),
            task_index: 0,
            title: "t".into(),
            started_at: Some("2026-10-05T10:00:00Z".into()),
            completed_at: completed.then(|| "2026-10-05T11:00:00Z".into()),
            completion_source: if completed {
                "commit".into()
            } else {
                "none".into()
            },
            segmentation_source: "whole_session".into(),
            wall_minutes: None,
            active_minutes_to_complete: completed.then_some(30.0),
            active_minutes_total: Some(40.0),
            agent_minutes: None,
            user_minutes: None,
            turns: 1,
            attempts: 1,
            validations: 0,
            validation_failures: 0,
            loop_attempts: 0,
            edit_calls: 1,
            lines_changed: 10,
            subagents: 0,
            primary_model: Some(model.into()),
            models: model.into(),
            model_changes: 0,
            repo_root: Some("/r/app".into()),
            git_branch: None,
            tokens_total: fresh * 10,
            fresh_tokens: fresh,
            cost_total: cost,
            cost_to_complete: if completed { cost } else { None },
            post_completion_cost: None,
            subagent_cost: None,
            unpriced_tokens: 0,
            estimated_tokens: 0,
            commits: 0,
            intent: None,
            intent_source: None,
            intent_band: None,
            outcome: None,
            outcome_source: None,
            outcome_band: None,
        }
    }

    #[test]
    fn group_totals_are_additive_and_bands_ordered() {
        let rows = vec![
            task(
                "claude",
                Some(25.0),
                2_000_000,
                true,
                "claude/claude-opus-5-5",
            ),
            task(
                "claude",
                Some(0.5),
                50_000,
                false,
                "claude/claude-sonnet-5-5",
            ),
            task("codex", Some(3.0), 600_000, true, "codex/gpt-5.5"),
            task("cursor", None, 10_000, false, "cursor/auto"),
        ];
        let groups = group_tasks(&rows, GroupDim::CostBand, &Filters::default());
        let keys: Vec<&str> = groups.iter().map(|g| g.key.as_str()).collect();
        assert_eq!(keys, vec![">$20", "$1–5", "$0.10–1", "unpriced"]);
        let total: f64 = groups.iter().filter_map(|g| g.cost).sum();
        assert!((total - 28.5).abs() < 1e-9);
        let by_tool = group_tasks(&rows, GroupDim::Tool, &Filters::default());
        let claude = by_tool.iter().find(|g| g.key == "claude").unwrap();
        assert_eq!(claude.tasks, 2);
        assert_eq!(claude.completion_rate, Some(0.5));
        assert_eq!(claude.filter.providers, vec!["claude".to_string()]);
    }

    #[test]
    fn child_filters_inherit_parent_filters() {
        let parent = Filters {
            from: Some("2026-10-01".into()),
            providers: vec!["claude".into()],
            ..Filters::default()
        };
        let child = Filters {
            cost_band: Some(CostBand::From5To20),
            ..Filters::default()
        };
        let merged = merge_filters(&parent, &child);
        assert_eq!(merged.from.as_deref(), Some("2026-10-01"));
        assert_eq!(merged.providers, vec!["claude".to_string()]);
        assert_eq!(merged.cost_band, Some(CostBand::From5To20));
    }
}
