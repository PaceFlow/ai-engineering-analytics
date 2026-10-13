//! Where work went by stage: cost and turns per stage, common stage paths of
//! tasks, and session kinds. Turn costs come from allocated usage, so stage
//! costs add up to the total for the filtered tasks.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::{Filters, TaskSort, median, tasks};
use crate::insights::stages::short_stage;

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct StageRow {
    pub stage: String,
    pub turns: i64,
    pub tasks: i64,
    pub cost: f64,
    pub share: f64,
    /// Turns labelled by the classifier or you (the rest are heuristic).
    pub classified_turns: i64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct PathRow {
    pub path: String,
    pub tasks: i64,
    pub completed: i64,
    pub median_cost: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct SessionKindRow {
    pub kind: String,
    pub sessions: i64,
    pub cost: f64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct StageSummary {
    pub stages: Vec<StageRow>,
    pub paths: Vec<PathRow>,
    pub session_kinds: Vec<SessionKindRow>,
    pub request_kinds: Vec<(String, i64)>,
}

/// turns, tasks, cost, classified turns
type StageAccumulator = (i64, HashSet<(String, String, i64)>, f64, i64);

pub fn stage_summary(conn: &Connection, filters: &Filters) -> Result<StageSummary> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let scope: HashSet<(String, String, i64)> = rows
        .iter()
        .map(|t| (t.provider.clone(), t.session_id.clone(), t.task_index))
        .collect();
    let sessions: HashSet<(String, String)> = rows
        .iter()
        .map(|t| (t.provider.clone(), t.session_id.clone()))
        .collect();

    let mut by_stage: BTreeMap<String, StageAccumulator> = BTreeMap::new();
    let mut requests: BTreeMap<String, i64> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index, stage, stage_source, request_kind, cost_usd
         FROM derived_turn_stage",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, f64>(6)?,
        ))
    })? {
        let (provider, session, task, stage, source, request, cost) = row?;
        let key = (provider, session, task);
        if !scope.contains(&key) {
            continue;
        }
        let entry = by_stage.entry(stage).or_default();
        entry.0 += 1;
        entry.1.insert(key);
        entry.2 += cost;
        entry.3 += matches!(source.as_str(), "jev" | "override") as i64;
        if let Some(request) = request {
            *requests.entry(request).or_default() += 1;
        }
    }
    let total: f64 = by_stage.values().map(|v| v.2).sum();
    let mut stages: Vec<StageRow> = by_stage
        .into_iter()
        .map(|(stage, (turns, tasks, cost, classified))| StageRow {
            stage,
            turns,
            tasks: tasks.len() as i64,
            cost,
            share: if total > 0.0 { cost / total } else { 0.0 },
            classified_turns: classified,
        })
        .collect();
    stages.sort_by(|a, b| b.cost.total_cmp(&a.cost).then(b.turns.cmp(&a.turns)));

    let mut paths: HashMap<String, (i64, i64, Vec<f64>)> = HashMap::new();
    for task in &rows {
        let path = task.stage_path.clone().unwrap_or_else(|| "—".into());
        let entry = paths.entry(path).or_default();
        entry.0 += 1;
        entry.1 += task.is_completed() as i64;
        if let Some(cost) = task.cost_to_complete.or(task.cost_total) {
            entry.2.push(cost);
        }
    }
    let mut paths: Vec<PathRow> = paths
        .into_iter()
        .map(|(path, (tasks, completed, mut costs))| PathRow {
            path,
            tasks,
            completed,
            median_cost: median(&mut costs),
        })
        .collect();
    paths.sort_by(|a, b| b.tasks.cmp(&a.tasks).then(a.path.cmp(&b.path)));

    let session_cost: HashMap<(String, String), f64> =
        rows.iter().fold(HashMap::new(), |mut acc, t| {
            *acc.entry((t.provider.clone(), t.session_id.clone()))
                .or_default() += t.cost_total.unwrap_or(0.0);
            acc
        });
    let mut kinds: BTreeMap<String, (i64, f64)> = BTreeMap::new();
    let mut stmt = conn.prepare("SELECT provider, session_id, kind FROM derived_session_kind")?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (provider, session, kind) = row?;
        let key = (provider, session);
        if !sessions.contains(&key) {
            continue;
        }
        let entry = kinds.entry(kind).or_default();
        entry.0 += 1;
        entry.1 += session_cost.get(&key).copied().unwrap_or(0.0);
    }
    let mut session_kinds: Vec<SessionKindRow> = kinds
        .into_iter()
        .map(|(kind, (sessions, cost))| SessionKindRow {
            kind,
            sessions,
            cost,
        })
        .collect();
    session_kinds.sort_by_key(|row| std::cmp::Reverse(row.sessions));
    let mut request_kinds: Vec<(String, i64)> = requests.into_iter().collect();
    request_kinds.sort_by_key(|row| std::cmp::Reverse(row.1));

    Ok(StageSummary {
        stages,
        paths,
        session_kinds,
        request_kinds,
    })
}

/// Display name of a stage.
pub fn label(stage: &str) -> &'static str {
    short_stage(stage)
}
