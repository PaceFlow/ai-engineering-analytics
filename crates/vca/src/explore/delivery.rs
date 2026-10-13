//! Delivery evidence per task, separate from conversation progress.
//!
//! The ladder counts observed facts: edited → validated → committed →
//! reached mainline → PR merged, plus later signals (fix-like commits or bug
//! issues touching the AI-written lines, reverts). Non-code intents are not
//! judged by commits. A later fix signal is evidence, not proof of a defect.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

use super::{Filters, TaskRow, TaskSort, tasks};

/// Intents that are expected to produce code.
pub const CODE_INTENTS: [&str; 6] = [
    "feature",
    "bug_fix",
    "refactor",
    "tests",
    "ops_config",
    "docs",
];

pub fn is_code_intent(intent: Option<&str>, edit_calls: i64) -> bool {
    match intent {
        Some(intent) => CODE_INTENTS.contains(&intent),
        None => edit_calls > 0,
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct SessionDelivery {
    pub commits: i64,
    pub mainline: bool,
    pub pr_merged: bool,
    pub later_fix: bool,
    pub reverted: bool,
}

/// Commit outcomes per root session (subagent matches included).
pub fn session_delivery(conn: &Connection) -> Result<HashMap<(String, String), SessionDelivery>> {
    let mut stmt = conn.prepare(
        "SELECT sm.provider, COALESCE(NULLIF(s.root_session_id, ''), s.session_id),
                COUNT(DISTINCT sm.repo_root || sm.commit_sha),
                MAX(COALESCE(o.merged_to_mainline_flag, 0)),
                MAX(COALESCE(pr.pr_merged_flag, 0)),
                MAX(COALESCE(b.bug_after_merge_flag, 0)),
                MAX(COALESCE(o.reverted_later_flag, 0))
         FROM fact_commit_session_match sm
         JOIN metadata_sessions s ON s.provider = sm.provider AND s.session_id = sm.session_id
         LEFT JOIN event_commit_outcome o ON o.repo_root = sm.repo_root AND o.commit_sha = sm.commit_sha
         LEFT JOIN event_commit_pr_outcome pr ON pr.repo_root = sm.repo_root AND pr.commit_sha = sm.commit_sha
         LEFT JOIN event_commit_bug_signal b ON b.repo_root = sm.repo_root AND b.commit_sha = sm.commit_sha
         WHERE sm.matched_lines > 0
         GROUP BY 1, 2",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            SessionDelivery {
                commits: row.get(2)?,
                mainline: row.get::<_, i64>(3)? != 0,
                pr_merged: row.get::<_, i64>(4)? != 0,
                later_fix: row.get::<_, i64>(5)? != 0,
                reverted: row.get::<_, i64>(6)? != 0,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<HashMap<_, _>>>()?)
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct LadderRow {
    pub intent: String,
    pub code_intent: bool,
    pub tasks: i64,
    pub edited: i64,
    pub validated: i64,
    pub committed: i64,
    pub mainline: i64,
    pub pr_merged: i64,
    pub later_fix: i64,
    pub reverted: i64,
    pub completed: i64,
    pub stalled: i64,
    pub cost: Option<f64>,
    pub heuristic_share: f64,
    #[serde(skip)]
    pub task_keys: Vec<(String, String, i64)>,
}

pub fn delivery_ladder(conn: &Connection, filters: &Filters) -> Result<Vec<LadderRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let delivery = session_delivery(conn)?;
    Ok(ladder_from(&rows, &delivery))
}

pub fn ladder_from(
    rows: &[TaskRow],
    delivery: &HashMap<(String, String), SessionDelivery>,
) -> Vec<LadderRow> {
    let mut by_intent: BTreeMap<String, LadderRow> = BTreeMap::new();
    for task in rows {
        let intent = task
            .intent
            .clone()
            .unwrap_or_else(|| "(unclassified)".to_string());
        let entry = by_intent
            .entry(intent.clone())
            .or_insert_with(|| LadderRow {
                intent: intent.clone(),
                code_intent: is_code_intent(task.intent.as_deref(), task.edit_calls),
                ..LadderRow::default()
            });
        let evidence = delivery
            .get(&(task.provider.clone(), task.session_id.clone()))
            .cloned()
            .unwrap_or_default();
        entry.tasks += 1;
        entry.edited += (task.edit_calls > 0) as i64;
        entry.validated += (task.validations > task.validation_failures) as i64;
        entry.committed += (evidence.commits > 0 || task.commits > 0) as i64;
        entry.mainline += evidence.mainline as i64;
        entry.pr_merged += evidence.pr_merged as i64;
        entry.later_fix += evidence.later_fix as i64;
        entry.reverted += evidence.reverted as i64;
        entry.completed += task.is_completed() as i64;
        entry.stalled += (task.outcome.as_deref() == Some("stalled")) as i64;
        if let Some(cost) = task.cost_total {
            *entry.cost.get_or_insert(0.0) += cost;
        }
        if task.intent_source.as_deref() == Some("heuristic") {
            entry.heuristic_share += 1.0;
        }
        entry.task_keys.push((
            task.provider.clone(),
            task.session_id.clone(),
            task.task_index,
        ));
    }
    let mut out: Vec<LadderRow> = by_intent
        .into_values()
        .map(|mut row| {
            row.heuristic_share /= row.tasks.max(1) as f64;
            row
        })
        .collect();
    out.sort_by(|a, b| {
        b.code_intent
            .cmp(&a.code_intent)
            .then(b.tasks.cmp(&a.tasks))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_code_intents_are_not_judged_by_commits() {
        assert!(is_code_intent(Some("bug_fix"), 0));
        assert!(!is_code_intent(Some("planning_design"), 3));
        assert!(is_code_intent(None, 2));
        assert!(!is_code_intent(None, 0));
    }
}
