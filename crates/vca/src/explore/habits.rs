//! The team's 12 habits as a scorecard: how often each is followed (where
//! it applies) and how tasks that followed it compare with tasks that did
//! not. Comparisons are correlational and show their sample sizes.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

use super::practices::MIN_N;
use super::{Filters, TaskRow, TaskSort, median, tasks};
use crate::insights::habits::HABITS;

type TaskKey = (String, String, i64);

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Outcomes {
    pub n: usize,
    pub completed: f64,
    pub corrections: f64,
    pub loops: f64,
    pub later_rework: f64,
    pub median_cost: Option<f64>,
    pub median_active_minutes: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct HabitRow {
    pub id: &'static str,
    pub name: &'static str,
    /// Only a weak signal can be seen in transcripts (habits 10 and 11).
    pub weak: bool,
    pub followed: usize,
    pub not_followed: usize,
    pub unknown: usize,
    pub not_applicable: usize,
    /// followed / (followed + not_followed)
    pub adoption: Option<f64>,
    /// Share of decided tasks that used classifier labels.
    pub classified_share: f64,
    pub with: Outcomes,
    pub without: Outcomes,
    pub low_n: bool,
    #[serde(skip)]
    pub followed_keys: Vec<TaskKey>,
    #[serde(skip)]
    pub not_followed_keys: Vec<TaskKey>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HabitTaskRow {
    pub task: TaskKey,
    pub title: String,
    pub started_at: Option<String>,
    pub value: String,
    pub source: String,
    pub evidence: String,
    pub stage_path: Option<String>,
    pub cost: Option<f64>,
}

fn key(task: &TaskRow) -> TaskKey {
    (
        task.provider.clone(),
        task.session_id.clone(),
        task.task_index,
    )
}

fn outcomes(
    tasks: &[&TaskRow],
    corrected: &HashSet<TaskKey>,
    reworked: &HashSet<TaskKey>,
) -> Outcomes {
    let n = tasks.len();
    let share = |count: usize| if n == 0 { 0.0 } else { count as f64 / n as f64 };
    let mut costs: Vec<f64> = tasks
        .iter()
        .filter_map(|t| t.cost_to_complete.or(t.cost_total))
        .collect();
    let mut minutes: Vec<f64> = tasks
        .iter()
        .filter_map(|t| t.active_minutes_to_complete.or(t.active_minutes_total))
        .collect();
    Outcomes {
        n,
        completed: share(tasks.iter().filter(|t| t.is_completed()).count()),
        corrections: share(tasks.iter().filter(|t| corrected.contains(&key(t))).count()),
        loops: share(tasks.iter().filter(|t| t.loop_attempts > 0).count()),
        later_rework: share(tasks.iter().filter(|t| reworked.contains(&key(t))).count()),
        median_cost: median(&mut costs),
        median_active_minutes: median(&mut minutes),
    }
}

/// (habit, task) → (value, source, evidence)
type HabitValues = HashMap<(String, TaskKey), (String, String, String)>;

fn habit_values(conn: &Connection) -> Result<HabitValues> {
    let mut stmt = conn.prepare(
        "SELECT habit, provider, session_id, task_index, value, source, COALESCE(evidence, '')
         FROM derived_task_habit",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                (row.get(1)?, row.get(2)?, row.get(3)?),
            ),
            (row.get(4)?, row.get(5)?, row.get(6)?),
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn habits(conn: &Connection, filters: &Filters) -> Result<Vec<HabitRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let values = habit_values(conn)?;
    let corrected: HashSet<TaskKey> = conn
        .prepare("SELECT DISTINCT provider, session_id, task_index FROM derived_turn_stage WHERE request_kind = 'correction'")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let reworked: HashSet<TaskKey> = super::rework::suspected_defects(conn, &Filters::default())?
        .into_iter()
        .filter(|d| d.level >= 1)
        .map(|d| d.origin)
        .collect();
    let mut out = Vec::new();
    for (id, name, weak) in HABITS {
        let mut row = HabitRow {
            id,
            name,
            weak,
            ..HabitRow::default()
        };
        let mut with: Vec<&TaskRow> = Vec::new();
        let mut without: Vec<&TaskRow> = Vec::new();
        let mut classified = 0usize;
        for task in &rows {
            let Some((value, source, _)) = values.get(&(id.to_string(), key(task))) else {
                row.unknown += 1;
                continue;
            };
            match value.as_str() {
                "followed" => {
                    row.followed += 1;
                    with.push(task);
                    row.followed_keys.push(key(task));
                }
                "not_followed" => {
                    row.not_followed += 1;
                    without.push(task);
                    row.not_followed_keys.push(key(task));
                }
                "na" => {
                    row.not_applicable += 1;
                    continue;
                }
                _ => {
                    row.unknown += 1;
                    continue;
                }
            }
            classified += matches!(source.as_str(), "jev" | "mixed") as usize;
        }
        let decided = row.followed + row.not_followed;
        row.adoption = (decided > 0).then(|| row.followed as f64 / decided as f64);
        row.classified_share = if decided > 0 {
            classified as f64 / decided as f64
        } else {
            0.0
        };
        row.with = outcomes(&with, &corrected, &reworked);
        row.without = outcomes(&without, &corrected, &reworked);
        row.low_n = with.len() < MIN_N || without.len() < MIN_N;
        out.push(row);
    }
    Ok(out)
}

/// Habits as Practices-screen rows (with = followed, without = not followed).
pub fn as_practice_rows(rows: Vec<HabitRow>) -> Vec<super::practices::PracticeRow> {
    let pct = |v: f64| format!("{:.0}%", v * 100.0);
    let usd = |v: Option<f64>| super::format::usd(v);
    rows.into_iter()
        .map(|row| {
            let (advice, definition) = crate::insights::habits::habit_text(row.id);
            let decided = row.followed + row.not_followed;
            let finding = match row.adoption {
                Some(adoption) => format!(
                    "followed in {} of {decided} tasks ({}); corrections {} vs {}",
                    row.followed,
                    pct(adoption),
                    pct(row.with.corrections),
                    pct(row.without.corrections)
                ),
                None => "not enough evidence yet (classify to sharpen)".into(),
            };
            super::practices::PracticeRow {
                id: row.id,
                name: row.name,
                kind: "habit",
                with_n: row.followed,
                without_n: row.not_followed,
                with_value: format!(
                    "done {} · {}",
                    pct(row.with.completed),
                    usd(row.with.median_cost)
                ),
                without_value: format!(
                    "done {} · {}",
                    pct(row.without.completed),
                    usd(row.without.median_cost)
                ),
                finding,
                confidence: if row.weak {
                    "signal"
                } else if row.low_n {
                    "low n"
                } else {
                    "n ok"
                },
                advice,
                definition,
                with_keys: row.followed_keys,
                without_keys: row.not_followed_keys,
            }
        })
        .collect()
}

/// Tasks behind one habit, for investigation.
pub fn habit_tasks(conn: &Connection, filters: &Filters, habit: &str) -> Result<Vec<HabitTaskRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let values = habit_values(conn)?;
    let mut out: Vec<HabitTaskRow> = rows
        .iter()
        .filter_map(|task| {
            let (value, source, evidence) = values.get(&(habit.to_string(), key(task)))?;
            Some(HabitTaskRow {
                task: key(task),
                title: task.title.clone(),
                started_at: task.started_at.clone(),
                value: value.clone(),
                source: source.clone(),
                evidence: evidence.clone(),
                stage_path: task.stage_path.clone(),
                cost: task.cost_total,
            })
        })
        .collect();
    out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    Ok(out)
}

/// Resolve `7`, `07` or `07_tests_can_fail` to a habit id.
pub fn resolve_habit(input: &str) -> Option<&'static str> {
    let trimmed = input.trim();
    HABITS.iter().map(|(id, _, _)| *id).find(|id| {
        *id == trimmed
            || id.split('_').next().is_some_and(|number| {
                number == trimmed
                    || number.trim_start_matches('0') == trimmed.trim_start_matches('0')
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_habit_numbers() {
        assert_eq!(resolve_habit("7"), Some("07_tests_can_fail"));
        assert_eq!(resolve_habit("07"), Some("07_tests_can_fail"));
        assert_eq!(
            resolve_habit("12_rules_from_corrections"),
            Some("12_rules_from_corrections")
        );
        assert_eq!(resolve_habit("13"), None);
    }
}
