//! Queries the TUI issues and the background loader that runs them.
//!
//! The UI thread never touches SQLite: it sends a [`Query`] tagged with a
//! request id, keeps rendering, and applies the [`Data`] when it arrives.
//! Results for requests that were superseded are dropped by id.

use anyhow::Result;
use rusqlite::Connection;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use crate::classify::client::OpenRouterClient;
use crate::classify::config::ClassifierConfig;
use crate::classify::overrides;
use crate::classify::run::{self as classify_run, Passes, RunOptions, RunSummary, SessionRef};
use crate::classify::state::Level;
use crate::explore::{
    self, Bucket, ContextDim, ContextGroupRow, ContextItemRow, Filters, GroupDim, GroupRow,
    QuotaContributor, QuotaWindowRow, SessionDetail, SessionRow, SessionSort, TaskRow, TaskSort,
    UsageBucket, UsageTotals, compare::CompareColumn, delivery::LadderRow, practices::PracticeRow,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Overview(Filters),
    Sessions(Filters, SessionSort),
    Tasks(Filters, TaskSort),
    Groups(Filters, GroupDim),
    Detail(String, String),
    Usage(Filters, Bucket),
    Windows(Filters),
    Contributors(QuotaWindowRow),
    ContextGroups(Filters, ContextDim),
    ContextItems(Filters, Option<(ContextDim, String)>),
    Ladder(Filters),
    Practices(Filters),
    Compare(Vec<(String, String, i64)>),
    /// Writes and classifier calls, run on the loader thread like queries.
    Action(Action),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    SetLabel {
        unit_kind: String,
        unit_key: String,
        question: String,
        label: Option<String>,
    },
    AssignTurn {
        provider: String,
        session: String,
        turn: i64,
        task: Option<i64>,
    },
    SetCompletion {
        provider: String,
        session: String,
        task: i64,
        completion: Option<Option<i64>>,
    },
    ClassifyPlan {
        sessions: Vec<(String, String)>,
        level: Level,
    },
    ClassifyPreview {
        session: (String, String),
        level: Level,
    },
    ClassifyRun {
        sessions: Vec<(String, String)>,
        level: Level,
        max_usd: f64,
    },
}

#[derive(Debug, Clone, Default)]
pub struct OverviewData {
    pub by_tool: Vec<UsageTotals>,
    pub daily: Vec<UsageBucket>,
    pub windows: Vec<QuotaWindowRow>,
    pub intents: Vec<GroupRow>,
    pub outcomes: Vec<GroupRow>,
    pub top_sessions: Vec<SessionRow>,
    pub loops_cost: f64,
    pub loop_tasks: i64,
    pub rebuild_cost: f64,
    pub corrections: i64,
    pub exchanges: i64,
    pub classified_jev: i64,
    pub classified_heuristic: i64,
    pub tasks_total: i64,
    pub subagent_count: i64,
    pub legacy_sessions: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ReworkData {
    pub split: explore::rework::SpendSplit,
    pub defects: Vec<explore::rework::DefectRow>,
    pub relations: usize,
}

#[derive(Debug, Clone)]
pub enum Data {
    Overview(Box<OverviewData>),
    Sessions(Vec<SessionRow>),
    Tasks(Vec<TaskRow>),
    Groups(Vec<GroupRow>),
    Detail(Option<Box<SessionDetail>>),
    Usage(Vec<UsageBucket>, Vec<UsageBucket>),
    Windows(Vec<QuotaWindowRow>),
    Contributors(Vec<QuotaContributor>),
    ContextGroups(Vec<ContextGroupRow>),
    ContextItems(Vec<ContextItemRow>),
    Ladder(Vec<LadderRow>, Box<ReworkData>),
    Practices(Vec<PracticeRow>),
    Compare(Vec<CompareColumn>),
    /// An action finished; the text is shown in the footer.
    Done(String),
    ClassifyPlan(Box<RunSummary>),
}

pub fn fetch(conn: &Connection, query: &Query) -> Result<Data> {
    Ok(match query {
        Query::Overview(filters) => Data::Overview(Box::new(overview(conn, filters)?)),
        Query::Sessions(filters, sort) => {
            Data::Sessions(explore::sessions(conn, filters, *sort, 500)?)
        }
        Query::Tasks(filters, sort) => Data::Tasks(explore::tasks(conn, filters, *sort, 500)?),
        Query::Groups(filters, dim) => Data::Groups(explore::groups(conn, filters, *dim)?),
        Query::Detail(provider, session) => {
            Data::Detail(explore::session_detail(conn, provider, session)?.map(Box::new))
        }
        Query::Usage(filters, bucket) => {
            let buckets = explore::usage_timeline(conn, filters, *bucket)?;
            let spikes =
                explore::usage_spikes(&explore::usage_timeline(conn, filters, Bucket::Hour)?);
            Data::Usage(buckets, spikes)
        }
        Query::Windows(filters) => Data::Windows(explore::quota_windows(conn, filters)?),
        Query::Contributors(window) => {
            Data::Contributors(explore::window_contributors(conn, window)?)
        }
        Query::ContextGroups(filters, dim) => {
            Data::ContextGroups(explore::context_groups(conn, filters, *dim)?)
        }
        Query::ContextItems(filters, constraint) => Data::ContextItems(explore::context_items_for(
            conn,
            filters,
            constraint.as_ref().map(|(dim, key)| (*dim, key.as_str())),
            300,
        )?),
        Query::Ladder(filters) => Data::Ladder(
            explore::delivery::delivery_ladder(conn, filters)?,
            Box::new(ReworkData {
                split: explore::rework::spend_split(conn, filters)?,
                defects: explore::rework::suspected_defects(conn, filters)?,
                relations: explore::rework::relations(conn, filters)?.len(),
            }),
        ),
        Query::Practices(filters) => Data::Practices(explore::practices::practices(conn, filters)?),
        Query::Compare(keys) => Data::Compare(explore::compare::compare_tasks(conn, keys)?),
        Query::Action(action) => act(conn, action)?,
    })
}

fn session_refs(conn: &Connection, sessions: &[(String, String)]) -> Result<Vec<SessionRef>> {
    sessions
        .iter()
        .map(|(provider, session_id)| {
            let repo_root: Option<String> = conn.query_row(
                "SELECT MAX(repo_root) FROM derived_task WHERE provider = ?1 AND session_id = ?2",
                rusqlite::params![provider, session_id],
                |row| row.get(0),
            )?;
            Ok(SessionRef {
                provider: provider.clone(),
                session_id: session_id.clone(),
                repo_root,
            })
        })
        .collect()
}

fn act(conn: &Connection, action: &Action) -> Result<Data> {
    Ok(match action {
        Action::SetLabel {
            unit_kind,
            unit_key,
            question,
            label,
        } => {
            overrides::set_label(conn, unit_kind, unit_key, question, label.as_deref(), None)?;
            Data::Done(match label {
                Some(label) => format!("{question} = {label} (your label)"),
                None => format!("{question}: your label removed"),
            })
        }
        Action::AssignTurn {
            provider,
            session,
            turn,
            task,
        } => {
            overrides::assign_turn(conn, provider, session, *turn, *task)?;
            Data::Done(match task {
                Some(task) => format!("Turn {turn} now starts task T{task}"),
                None => format!("Turn {turn}: task override removed"),
            })
        }
        Action::SetCompletion {
            provider,
            session,
            task,
            completion,
        } => {
            overrides::set_completion(conn, provider, session, *task, *completion)?;
            Data::Done(match completion {
                Some(Some(turn)) => format!("T{task} completed at turn {turn}"),
                Some(None) => format!("T{task} marked not completed"),
                None => format!("T{task}: completion override removed"),
            })
        }
        Action::ClassifyPlan { sessions, level } => {
            let config = ClassifierConfig::load()?;
            let refs = session_refs(conn, sessions)?;
            Data::ClassifyPlan(Box::new(classify_run::plan(conn, &config, &refs, *level)?))
        }
        Action::ClassifyPreview { session, level } => {
            let config = ClassifierConfig::load()?;
            let refs = session_refs(conn, std::slice::from_ref(session))?;
            let preview = classify_run::preview(conn, &config, &refs[0], "labels", None, *level)?;
            Data::Done(serde_json::to_string_pretty(&preview)?)
        }
        Action::ClassifyRun {
            sessions,
            level,
            max_usd,
        } => {
            let config = ClassifierConfig::load()?;
            let Some(key) = crate::classify::config::api_key()? else {
                anyhow::bail!(
                    "No OpenRouter key: set {} or run `vca classify key set`",
                    crate::classify::config::KEY_ENV
                );
            };
            let refs = session_refs(conn, sessions)?;
            let client = OpenRouterClient::new(&config.endpoint, &key)?;
            let options = RunOptions {
                level: *level,
                max_usd: *max_usd,
                passes: Passes::ALL,
                dry_run: false,
            };
            let summary = classify_run::run(conn, &client, &config, &refs, &options)?;
            let mut text = format!(
                "Classified: {} sent, {} cached, {} failed · ${:.5} · {} sessions split",
                summary.sent, summary.cached, summary.failed, summary.cost_usd, summary.tasks_split
            );
            if let Some(reason) = summary.stopped {
                text.push_str(&format!(" · stopped: {reason}"));
            }
            Data::Done(text)
        }
    })
}

fn overview(conn: &Connection, filters: &Filters) -> Result<OverviewData> {
    let tasks = explore::tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let intents = explore::group_tasks(&tasks, GroupDim::Intent, filters);
    let outcomes = explore::group_tasks(&tasks, GroupDim::Outcome, filters);
    let loop_tasks: Vec<&TaskRow> = tasks.iter().filter(|t| t.loop_attempts > 0).collect();
    let (corrections, exchanges): (i64, i64) = conn.query_row(
        "SELECT COALESCE(SUM(CASE WHEN label LIKE 'correct_%' THEN 1 ELSE 0 END), 0), COUNT(*)
         FROM view_label_effective WHERE unit_kind = 'exchange' AND question_id = 'feedback'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let (jev, heuristic): (i64, i64) = conn.query_row(
        "SELECT COALESCE(SUM(CASE WHEN source IN ('jev', 'override') THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN source = 'heuristic' THEN 1 ELSE 0 END), 0)
         FROM view_label_effective WHERE unit_kind = 'task' AND question_id = 'outcome'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let rebuild_cost: f64 = conn.query_row(
        "SELECT COALESCE(SUM(rebuild_extra_usd), 0) FROM derived_api_call",
        [],
        |row| row.get(0),
    )?;
    let subagent_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM metadata_sessions WHERE COALESCE(agent_kind, 'main') != 'main'",
        [],
        |row| row.get(0),
    )?;
    let legacy_sessions: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT provider || session_id) FROM fact_session_usage WHERE token_source = 'legacy'",
        [],
        |row| row.get(0),
    )?;
    Ok(OverviewData {
        by_tool: explore::usage_totals(conn, filters, explore::UsageDim::Tool)?,
        daily: explore::usage_timeline(conn, filters, Bucket::Day)?,
        windows: explore::quota_windows(conn, filters)?
            .into_iter()
            .take(6)
            .collect(),
        intents,
        outcomes,
        top_sessions: explore::sessions(conn, filters, SessionSort::Cost, 8)?,
        loops_cost: loop_tasks.iter().filter_map(|t| t.cost_total).sum(),
        loop_tasks: loop_tasks.len() as i64,
        rebuild_cost,
        corrections,
        exchanges,
        classified_jev: jev,
        classified_heuristic: heuristic,
        tasks_total: tasks.len() as i64,
        subagent_count,
        legacy_sessions,
    })
}

/// Executes queries; the thread loader in production, an inline one in tests.
pub trait Loader {
    fn request(&mut self, id: u64, query: Query);
    fn poll(&mut self) -> Vec<(u64, Result<Data, String>)>;
}

pub struct ThreadLoader {
    sender: Sender<(u64, Query)>,
    receiver: Receiver<(u64, Result<Data, String>)>,
}

impl ThreadLoader {
    pub fn spawn(open: impl FnOnce() -> Result<Connection> + Send + 'static) -> Self {
        let (query_tx, query_rx) = mpsc::channel::<(u64, Query)>();
        let (data_tx, data_rx) = mpsc::channel();
        thread::spawn(move || {
            let conn = match open() {
                Ok(conn) => conn,
                Err(error) => {
                    while let Ok((id, _)) = query_rx.recv() {
                        let _ = data_tx.send((id, Err(error.to_string())));
                    }
                    return;
                }
            };
            while let Ok((id, query)) = query_rx.recv() {
                let result = fetch(&conn, &query).map_err(|error| error.to_string());
                if data_tx.send((id, result)).is_err() {
                    return;
                }
            }
        });
        Self {
            sender: query_tx,
            receiver: data_rx,
        }
    }
}

impl Loader for ThreadLoader {
    fn request(&mut self, id: u64, query: Query) {
        let _ = self.sender.send((id, query));
    }

    fn poll(&mut self) -> Vec<(u64, Result<Data, String>)> {
        let mut out = Vec::new();
        while let Ok(item) = self.receiver.try_recv() {
            out.push(item);
        }
        out
    }
}

/// Runs queries immediately on the given connection (tests).
pub struct InlineLoader {
    pub conn: Connection,
    pub done: Vec<(u64, Result<Data, String>)>,
    pub requests: Vec<Query>,
}

impl InlineLoader {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            done: Vec::new(),
            requests: Vec::new(),
        }
    }
}

impl Loader for InlineLoader {
    fn request(&mut self, id: u64, query: Query) {
        self.requests.push(query.clone());
        let result = fetch(&self.conn, &query).map_err(|error| error.to_string());
        self.done.push((id, result));
    }

    fn poll(&mut self) -> Vec<(u64, Result<Data, String>)> {
        std::mem::take(&mut self.done)
    }
}
