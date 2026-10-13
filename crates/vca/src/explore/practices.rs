//! Practice rules: deterministic comparisons over tasks that point at habits
//! worth changing. Every comparison is correlational and shows its sample
//! size; groups under `MIN_N` are flagged as low n.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

use super::{Filters, TaskRow, TaskSort, median, tasks};

pub const MIN_N: usize = 15;

type TaskKey = (String, String, i64);

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PracticeRow {
    pub id: &'static str,
    pub name: &'static str,
    pub kind: &'static str,
    pub with_n: usize,
    pub without_n: usize,
    pub with_value: String,
    pub without_value: String,
    pub finding: String,
    pub confidence: &'static str,
    pub advice: &'static str,
    pub definition: &'static str,
    #[serde(skip)]
    pub with_keys: Vec<TaskKey>,
    #[serde(skip)]
    pub without_keys: Vec<TaskKey>,
}

fn key(task: &TaskRow) -> TaskKey {
    (
        task.provider.clone(),
        task.session_id.clone(),
        task.task_index,
    )
}

/// Opening request names how to verify the result.
pub fn names_verification(first_request: &str) -> bool {
    let text = first_request.to_lowercase();
    text.chars().count() >= 60
        && [
            "must ",
            "should pass",
            "acceptance",
            "verify",
            "make sure",
            "expected",
            "cargo test",
            "npm test",
            "pnpm test",
            "pytest",
            "go test",
            "tests pass",
            "test pass",
            "run the test",
            "until",
        ]
        .iter()
        .any(|needle| text.contains(needle))
}

struct Stats {
    n: usize,
    completion: Option<f64>,
    median_cost: Option<f64>,
    stalled: Option<f64>,
    corrections: Option<f64>,
}

fn stats(tasks: &[&TaskRow], corrections: &HashMap<TaskKey, i64>) -> Stats {
    let n = tasks.len();
    let rate = |count: usize| (n > 0).then(|| count as f64 / n as f64);
    Stats {
        n,
        completion: rate(tasks.iter().filter(|t| t.is_completed()).count()),
        median_cost: median(
            &mut tasks
                .iter()
                .filter_map(|t| t.cost_total)
                .collect::<Vec<_>>(),
        ),
        stalled: rate(
            tasks
                .iter()
                .filter(|t| t.outcome.as_deref() == Some("stalled") || t.loop_attempts > 0)
                .count(),
        ),
        corrections: (n > 0).then(|| {
            tasks
                .iter()
                .map(|t| corrections.get(&key(t)).copied().unwrap_or(0))
                .sum::<i64>() as f64
                / n as f64
        }),
    }
}

fn pct(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.0}%", v * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn usd(value: Option<f64>) -> String {
    value
        .map(|v| format!("~${v:.2}"))
        .unwrap_or_else(|| "—".into())
}

fn confidence(with_n: usize, without_n: usize) -> &'static str {
    if with_n >= MIN_N && without_n >= MIN_N {
        "n ok"
    } else {
        "low n"
    }
}

fn describe(with: &Stats, without: &Stats) -> String {
    format!(
        "done {} vs {} · stalled/looping {} vs {} · corrections/task {} vs {} · median {} vs {}",
        pct(with.completion),
        pct(without.completion),
        pct(with.stalled),
        pct(without.stalled),
        with.corrections
            .map(|v| format!("{v:.1}"))
            .unwrap_or_else(|| "—".into()),
        without
            .corrections
            .map(|v| format!("{v:.1}"))
            .unwrap_or_else(|| "—".into()),
        usd(with.median_cost),
        usd(without.median_cost)
    )
}

#[allow(clippy::too_many_arguments)]
fn compare_rule(
    id: &'static str,
    name: &'static str,
    advice: &'static str,
    definition: &'static str,
    rows: &[TaskRow],
    corrections: &HashMap<TaskKey, i64>,
    predicate: impl Fn(&TaskRow) -> Option<bool>,
) -> PracticeRow {
    let mut with: Vec<&TaskRow> = Vec::new();
    let mut without: Vec<&TaskRow> = Vec::new();
    for row in rows {
        match predicate(row) {
            Some(true) => with.push(row),
            Some(false) => without.push(row),
            None => {}
        }
    }
    let with_stats = stats(&with, corrections);
    let without_stats = stats(&without, corrections);
    PracticeRow {
        id,
        name,
        kind: "comparison",
        with_n: with.len(),
        without_n: without.len(),
        with_value: format!(
            "done {} · {}",
            pct(with_stats.completion),
            usd(with_stats.median_cost)
        ),
        without_value: format!(
            "done {} · {}",
            pct(without_stats.completion),
            usd(without_stats.median_cost)
        ),
        finding: describe(&with_stats, &without_stats),
        confidence: confidence(with_stats.n, without_stats.n),
        advice,
        definition,
        with_keys: with.iter().map(|t| key(t)).collect(),
        without_keys: without.iter().map(|t| key(t)).collect(),
    }
}

/// Exchange feedback labels per task: count of correction messages.
fn corrections_per_task(conn: &Connection) -> Result<HashMap<TaskKey, i64>> {
    let mut feedback: HashMap<(String, String, i64), String> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT unit_key, label FROM view_label_effective
         WHERE unit_kind = 'exchange' AND question_id = 'feedback'",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })? {
        let (unit_key, label) = row?;
        let mut parts = unit_key.rsplitn(2, '|');
        let turn = parts.next().and_then(|t| t.parse::<i64>().ok());
        let rest = parts.next().unwrap_or_default();
        if let (Some(turn), Some((provider, session)), Some(label)) =
            (turn, rest.split_once('|'), label)
        {
            feedback.insert((provider.to_string(), session.to_string(), turn), label);
        }
    }
    let mut out = HashMap::new();
    let mut stmt =
        conn.prepare("SELECT provider, session_id, task_index, turn_list FROM derived_task")?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })? {
        let (provider, session, task, list) = row?;
        let count = list
            .unwrap_or_default()
            .split(',')
            .filter_map(|turn| turn.parse::<i64>().ok())
            .filter(|turn| {
                feedback
                    .get(&(provider.clone(), session.clone(), *turn))
                    .is_some_and(|label| label.starts_with("correct_"))
            })
            .count() as i64;
        out.insert((provider, session, task), count);
    }
    Ok(out)
}

/// Tasks whose first validation happened in attempt 0 or 1.
fn early_validation(conn: &Connection) -> Result<HashMap<TaskKey, bool>> {
    let mut out = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index,
                MIN(CASE WHEN validation_kind IS NOT NULL THEN attempt_index END)
         FROM derived_attempt GROUP BY 1, 2, 3",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ),
            row.get::<_, Option<i64>>(3)?,
        ))
    })? {
        let (key, first) = row?;
        out.insert(key, first.is_some_and(|index| index <= 1));
    }
    Ok(out)
}

pub fn practices(conn: &Connection, filters: &Filters) -> Result<Vec<PracticeRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let corrections = corrections_per_task(conn)?;
    let early = early_validation(conn)?;
    let task_keys: HashSet<TaskKey> = rows.iter().map(key).collect();
    let mut out = Vec::new();

    out.push(compare_rule(
        "framing",
        "Verification named in the first request",
        "State how to check the result (test command, expected behaviour) in the first message.",
        "Tasks whose opening request names a test, check or acceptance condition vs those that do not (keyword rule; Jev framing score when classified).",
        &rows,
        &corrections,
        |t| Some(names_verification(&t.title)),
    ));
    out.push(compare_rule(
        "early_validation",
        "Validation within the first two attempts",
        "Ask the agent to run the specific test after each edit; put the command in AGENTS.md / CLAUDE.md.",
        "Code tasks whose first validation run happened in attempt 0 or 1 vs later or never.",
        &rows,
        &corrections,
        |t| (t.attempts > 0).then(|| early.get(&key(t)).copied().unwrap_or(false)),
    ));
    out.push(compare_rule(
        "delegation",
        "Subagent delegation",
        "Delegate exploration to cheaper subagents; keep implementation in the main thread.",
        "Tasks that spawned at least one subagent vs those that did not.",
        &rows,
        &corrections,
        |t| Some(t.subagents > 0),
    ));

    // Observational rules.
    let loops: Vec<&TaskRow> = rows.iter().filter(|t| t.loop_attempts > 0).collect();
    let loop_cost: f64 = loops.iter().filter_map(|t| t.cost_total).sum();
    out.push(PracticeRow {
        id: "loops",
        name: "Unproductive loops (3+ same-signature failures)",
        kind: "observation",
        with_n: loops.len(),
        without_n: rows.len() - loops.len(),
        with_value: format!("~${loop_cost:.2} in looping tasks"),
        without_value: String::new(),
        finding: format!("{} of {} tasks looped on the same failing check without new input", loops.len(), rows.len()),
        confidence: "observed",
        advice: "After two identical failures, add information, narrow scope or switch model instead of retrying.",
        definition: "Attempts whose validation failed with the same normalized error at least three times in a row, with no substantial user message in between.",
        with_keys: loops.iter().map(|t| key(t)).collect(),
        without_keys: Vec::new(),
    });

    let after: Vec<&TaskRow> = rows
        .iter()
        .filter(|t| t.post_completion_cost.unwrap_or(0.0) > 0.0)
        .collect();
    let after_cost: f64 = after.iter().filter_map(|t| t.post_completion_cost).sum();
    out.push(PracticeRow {
        id: "after_done",
        name: "Spend after the task was done",
        kind: "observation",
        with_n: after.len(),
        without_n: rows.len() - after.len(),
        with_value: format!("~${after_cost:.2} after completion"),
        without_value: String::new(),
        finding: "Work continued in the same task after completion evidence (polish, follow-ups)."
            .into(),
        confidence: "observed",
        advice: "Start a new session for the next task so context and cost stay separated.",
        definition: "API-equivalent cost of calls after the completion point of completed tasks.",
        with_keys: after.iter().map(|t| key(t)).collect(),
        without_keys: Vec::new(),
    });

    let (rebuild_count, rebuild_cost, rebuild_keys) = cache_rebuilds(conn, &task_keys)?;
    out.push(PracticeRow {
        id: "cache_rebuilds",
        name: "Cache rebuilds after idle gaps",
        kind: "observation",
        with_n: rebuild_keys.len(),
        without_n: 0,
        with_value: format!("{rebuild_count} rebuilds · ~${rebuild_cost:.2} extra"),
        without_value: String::new(),
        finding: "Calls where the cached context had expired and was written again.".into(),
        confidence: "observed",
        advice: "Resume promptly or start a fresh session instead of returning to a large context after a long pause.",
        definition: "Calls whose cache reads cover less than half of the carried context; extra cost vs a cache hit.",
        with_keys: rebuild_keys,
        without_keys: Vec::new(),
    });

    let (large_n, large_cost, unused_share, large_keys) = large_early_reads(conn, &task_keys)?;
    out.push(PracticeRow {
        id: "large_early_reads",
        name: "Large reads (>20k tokens) in the first 5 calls",
        kind: "observation",
        with_n: large_keys.len(),
        without_n: 0,
        with_value: format!("{large_n} reads · ~${large_cost:.2}"),
        without_value: format!("{} not used later", pct(unused_share)),
        finding: "Big early tool outputs are carried through every later call.".into(),
        confidence: "observed",
        advice: "Narrow instruction-driven startup reads; prefer targeted search over dumping files or directories.",
        definition: "Tool results estimated above 20k tokens entering context in the first five API calls; write + carry cost.",
        with_keys: large_keys,
        without_keys: Vec::new(),
    });

    let (bloat_n, bloat_cost, bloat_keys) = context_bloat(conn, &rows)?;
    out.push(PracticeRow {
        id: "context_bloat",
        name: "Cache reads above 15× fresh tokens",
        kind: "observation",
        with_n: bloat_n,
        without_n: rows.len().saturating_sub(bloat_n),
        with_value: format!("~${bloat_cost:.2} in these tasks"),
        without_value: String::new(),
        finding: "Long sessions keep re-reading a large context on every call.".into(),
        confidence: "observed",
        advice: "Compact or start a new session per task once the goal changes.",
        definition: "Tasks whose cache-read tokens exceed 15 times their fresh tokens.",
        with_keys: bloat_keys,
        without_keys: Vec::new(),
    });

    Ok(out)
}

/// All tasks (within `allowed`) of the root sessions returned by `sql`.
fn root_task_keys(
    conn: &Connection,
    sql: &str,
    allowed: &HashSet<TaskKey>,
) -> Result<Vec<TaskKey>> {
    let mut stmt = conn.prepare(sql)?;
    let sessions: HashSet<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    let mut keys: Vec<TaskKey> = allowed
        .iter()
        .filter(|(provider, session, _)| sessions.contains(&(provider.clone(), session.clone())))
        .cloned()
        .collect();
    keys.sort();
    Ok(keys)
}

fn cache_rebuilds(
    conn: &Connection,
    allowed: &HashSet<TaskKey>,
) -> Result<(i64, f64, Vec<TaskKey>)> {
    let (count, cost): (i64, Option<f64>) = conn.query_row(
        "SELECT COUNT(*), SUM(rebuild_extra_usd) FROM derived_api_call WHERE cache_event = 'rebuild'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let keys = root_task_keys(
        conn,
        "SELECT DISTINCT a.provider, COALESCE(NULLIF(s.root_session_id, ''), s.session_id)
         FROM derived_api_call a
         JOIN metadata_sessions s ON s.provider = a.provider AND s.session_id = a.session_id
         WHERE a.cache_event = 'rebuild'",
        allowed,
    )?;
    Ok((count, cost.unwrap_or(0.0), keys))
}

fn large_early_reads(
    conn: &Connection,
    allowed: &HashSet<TaskKey>,
) -> Result<(i64, f64, Option<f64>, Vec<TaskKey>)> {
    let (count, cost, unused): (i64, Option<f64>, Option<f64>) = conn.query_row(
        "SELECT COUNT(*), SUM(total_usd),
                SUM(CASE WHEN purpose IN ('unused', 'startup_unused') THEN 1.0 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN purpose IS NOT NULL THEN 1 ELSE 0 END), 0)
         FROM derived_context_item
         WHERE kind = 'tool_result' AND tokens > 20000 AND entered_call < 5",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let keys = root_task_keys(
        conn,
        "SELECT DISTINCT c.provider, COALESCE(NULLIF(s.root_session_id, ''), s.session_id)
         FROM derived_context_item c
         JOIN metadata_sessions s ON s.provider = c.provider AND s.session_id = c.session_id
         WHERE c.kind = 'tool_result' AND c.tokens > 20000 AND c.entered_call < 5",
        allowed,
    )?;
    Ok((count, cost.unwrap_or(0.0), unused, keys))
}

fn context_bloat(conn: &Connection, rows: &[TaskRow]) -> Result<(usize, f64, Vec<TaskKey>)> {
    let mut stmt = conn.prepare(
        "SELECT provider, root_session_id, task_index, SUM(cache_read_tokens), SUM(fresh_tokens)
         FROM derived_usage_event WHERE task_index IS NOT NULL GROUP BY 1, 2, 3",
    )?;
    let ratios: HashMap<TaskKey, (i64, i64)> = stmt
        .query_map([], |row| {
            Ok((
                (
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ),
                (row.get::<_, i64>(3)?, row.get::<_, i64>(4)?),
            ))
        })?
        .collect::<rusqlite::Result<HashMap<_, _>>>()?;
    let bloated: Vec<&TaskRow> = rows
        .iter()
        .filter(|t| {
            ratios
                .get(&key(t))
                .is_some_and(|(read, fresh)| *fresh > 0 && *read > fresh * 15)
        })
        .collect();
    let cost: f64 = bloated.iter().filter_map(|t| t.cost_total).sum();
    Ok((
        bloated.len(),
        cost,
        bloated.iter().map(|t| key(t)).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verification_keywords_need_a_real_request() {
        assert!(names_verification(
            "Add a --weekly flag to the cost report; cargo test -p vca cost must pass afterwards"
        ));
        assert!(!names_verification("make it better"));
        assert!(!names_verification(
            "Please refactor the analytics module into smaller files for readability please"
        ));
    }
}
