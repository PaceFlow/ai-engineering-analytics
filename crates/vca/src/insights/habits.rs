//! Per-task evidence for the team's 12 "not AI slop" habits.
//!
//! Each habit gets `followed`, `not_followed`, `na` (does not apply, e.g. no
//! code was written) or `unknown` (the evidence cannot tell), with the source
//! of the decision (`deterministic`, `jev`, `heuristic` labels or `mixed`) and
//! a short evidence note. Rules combine tool evidence with the effective turn
//! labels (stage, request kind, flags), so classifier answers and overrides
//! sharpen them; without classification the heuristic labels are used.

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{HashMap, HashSet};

use crate::classify::heuristic::{task_unit_key, turn_unit_key};

pub(crate) const HABITS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_task_habit (
    provider    TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    task_index  INTEGER NOT NULL,
    habit       TEXT NOT NULL,
    value       TEXT NOT NULL,
    source      TEXT NOT NULL,
    evidence    TEXT,
    PRIMARY KEY(provider, session_id, task_index, habit)
);
";

/// (id, name, weak signal only)
pub const HABITS: [(&str, &str, bool); 12] = [
    ("01_know_done", "Know what done looks like", false),
    ("02_plan_first", "Plan first, and do it yourself", false),
    ("03_make_it_ask", "Make it ask, not assume", false),
    ("04_ground_it", "Ground it in what exists", false),
    ("05_small_slices", "Work in small slices", false),
    ("06_minimal_diff", "Push for the minimal diff", false),
    ("07_tests_can_fail", "Write tests that can fail", false),
    ("08_evidence", "Ask for evidence, not reports", false),
    ("09_independent_review", "Get an independent review", false),
    (
        "10_defend_without_ai",
        "Defend it without AI (signal)",
        true,
    ),
    ("11_self_review", "Review your own PR first (signal)", true),
    (
        "12_rules_from_corrections",
        "Turn corrections into rules",
        false,
    ),
];

/// (id, cheat-sheet advice, how the habit is measured)
pub const HABIT_TEXT: [(&str, &str, &str); 12] = [
    (
        "01_know_done",
        "Understand the task and the expected output before you start; if you can't describe the result, you can't judge it.",
        "Followed when the first request states acceptance criteria or how to verify (request kind spec_criteria/plan_handoff, Jev framing ≥ 2, or a verification keyword).",
    ),
    (
        "02_plan_first",
        "Draft your own approach, then have the agent critique it; use your strongest model here.",
        "Followed when a planning stage, plan tool or your own plan (plan_handoff) comes before the first implementation turn. N/A when nothing was built.",
    ),
    (
        "03_make_it_ask",
        "Have the agent list open questions and assumptions; resolve them yourself.",
        "Followed when, before building, the agent asked (ask tool, Jev agent_asked_questions), you asked it to ask, or a clarify stage happened.",
    ),
    (
        "04_ground_it",
        "It reads the real ticket, code, data and docs first and follows local patterns.",
        "Followed when an explore/research stage, at least 3 reads/searches, or cited sources precede the first edit.",
    ),
    (
        "05_small_slices",
        "One step per run, reviewed before the next; size each iteration to what you can verify.",
        "Followed when the median edit batch is ≤ 150 lines and most batches are followed by a check.",
    ),
    (
        "06_minimal_diff",
        "Assume the first draft is overbuilt; ban unrequested changes and scope creep.",
        "Not followed when Jev flags unrequested changes; followed when changes stayed in scope or minimal changes were demanded. Unknown without classification.",
    ),
    (
        "07_tests_can_fail",
        "Write tests that can fail, see them fail, and never let the agent edit tests to pass.",
        "Followed when a test was written before the code and seen failing; not followed when tests changed after failing without a code fix (or Jev tests_changed_to_pass).",
    ),
    (
        "08_evidence",
        "Ask for evidence, not reports: read the diff and logs, run the feature.",
        "Followed when a test, build or live run follows the last edit; not followed when done is claimed without one.",
    ),
    (
        "09_independent_review",
        "Review in a fresh session against the original task, with a second model.",
        "Followed when a review stage or /review command follows implementation, a reviewer subagent ran, or a later review session touched the same branch.",
    ),
    (
        "10_defend_without_ai",
        "If you can't explain and justify the PR on your own, it isn't ready.",
        "Signal only: \"how does X work\" questions after implementation. Understanding outside the chat is invisible.",
    ),
    (
        "11_self_review",
        "Review your own PR before anyone else sees it.",
        "Signal only: a diff was shown after the last edit. Most self-review happens outside the transcript.",
    ),
    (
        "12_rules_from_corrections",
        "When the agent repeats a mistake, write the fix and the reason into the instruction file or a review skill.",
        "Applies when corrections repeat across tasks in a repo within 30 days; followed when CLAUDE.md/AGENTS.md/skills/rules changed within 14 days after.",
    ),
];

pub fn habit_text(id: &str) -> (&'static str, &'static str) {
    HABIT_TEXT
        .iter()
        .find(|(habit, _, _)| *habit == id)
        .map(|(_, advice, definition)| (*advice, *definition))
        .unwrap_or(("", ""))
}

/// Median lines per attempt at or below this counts as small slices.
pub const SMALL_SLICE_LINES: i64 = 150;
const INSTRUCTION_PATHS: [&str; 6] = [
    "claude.md",
    "agents.md",
    ".claude/",
    ".cursor/rules",
    ".github/copilot-instructions",
    ".cursorrules",
];

type TaskKey = (String, String, i64);
/// task → (title, repo root, started at)
type TaskInfo = HashMap<TaskKey, (String, Option<String>, Option<String>)>;

#[derive(Debug, Clone, Default)]
struct Turn {
    index: i64,
    stage: String,
    stage_source: String,
    request: String,
    request_source: String,
    reads: i64,
    searches: i64,
    web: i64,
    plan_tools: i64,
    ask_tools: i64,
    edits: i64,
    test_edits: i64,
    source_edits: i64,
    validations: i64,
    test_failures: i64,
    first_test_failed: Option<bool>,
    live_runs: i64,
    diff_checks: i64,
    slash: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub value: &'static str,
    pub source: &'static str,
    pub evidence: String,
}

fn verdict(value: &'static str, source: &'static str, evidence: impl Into<String>) -> Verdict {
    Verdict {
        value,
        source,
        evidence: evidence.into(),
    }
}

const IMPLEMENT_STAGES: [&str; 4] = ["implement", "write_tests", "debug_fix", "refine_polish"];

struct TaskView<'a> {
    turns: &'a [Turn],
    flags: &'a HashMap<(i64, String), bool>,
    framing: Option<i64>,
    title: &'a str,
    attempts: &'a [(i64, String)],
    reviewer_subagent: bool,
    later_review: bool,
    rule_commit_after_repeat: Option<bool>,
}

impl TaskView<'_> {
    fn flag(&self, turn: i64, name: &str) -> Option<bool> {
        self.flags.get(&(turn, name.to_string())).copied()
    }

    fn any_flag(&self, turns: &[&Turn], name: &str) -> Option<bool> {
        let values: Vec<bool> = turns
            .iter()
            .filter_map(|t| self.flag(t.index, name))
            .collect();
        if values.is_empty() {
            None
        } else {
            Some(values.iter().any(|v| *v))
        }
    }

    fn label_source(&self, turns: &[&Turn]) -> &'static str {
        if turns.iter().any(|t| {
            matches!(t.stage_source.as_str(), "jev" | "override")
                || matches!(t.request_source.as_str(), "jev" | "override")
        }) {
            "jev"
        } else {
            "heuristic"
        }
    }

    fn first_implement(&self) -> Option<usize> {
        self.turns
            .iter()
            .position(|t| IMPLEMENT_STAGES.contains(&t.stage.as_str()) || t.source_edits > 0)
    }

    fn edits(&self) -> i64 {
        self.turns.iter().map(|t| t.edits).sum()
    }
}

fn judge(habit: &str, task: &TaskView) -> Verdict {
    let first_impl = task.first_implement();
    let before: Vec<&Turn> = match first_impl {
        Some(index) => task.turns[..=index].iter().collect(),
        None => task.turns.iter().collect(),
    };
    let strictly_before: Vec<&Turn> = match first_impl {
        Some(index) => task.turns[..index].iter().collect(),
        None => Vec::new(),
    };
    let wrote_code = task.edits() > 0;
    match habit {
        "01_know_done" => {
            let first = task.turns.first();
            let spec = first
                .is_some_and(|t| matches!(t.request.as_str(), "spec_criteria" | "plan_handoff"));
            let framed = task.framing.is_some_and(|f| f >= 2);
            let named = crate::explore::practices::names_verification(task.title);
            let source = if task.framing.is_some() {
                "jev"
            } else {
                task.label_source(&before)
            };
            if spec || framed || named {
                verdict(
                    "followed",
                    source,
                    if framed {
                        "first request names constraints or acceptance criteria (framing ≥ 2)"
                    } else {
                        "first request states the expected result or how to verify it"
                    },
                )
            } else {
                verdict(
                    "not_followed",
                    source,
                    "first request gives a goal without acceptance criteria",
                )
            }
        }
        "02_plan_first" => {
            if first_impl.is_none() {
                return verdict("na", "deterministic", "no implementation in this task");
            }
            let handoff = before.iter().any(|t| t.request == "plan_handoff");
            let planned = strictly_before.iter().any(|t| t.stage == "plan_design")
                || before.iter().any(|t| t.plan_tools > 0)
                || handoff;
            let source = task.label_source(&before);
            if planned {
                verdict(
                    "followed",
                    source,
                    if handoff {
                        "user gave their own plan before implementation"
                    } else {
                        "a planning step came before implementation"
                    },
                )
            } else {
                verdict(
                    "not_followed",
                    source,
                    "implementation started without a planning step",
                )
            }
        }
        "03_make_it_ask" => {
            if first_impl.is_none() {
                return verdict("na", "deterministic", "no implementation in this task");
            }
            let asked = before.iter().any(|t| {
                t.ask_tools > 0
                    || t.stage == "clarify_requirements"
                    || t.request == "clarification_answer"
            }) || task.any_flag(&before, "agent_asked_questions") == Some(true)
                || task.any_flag(&before, "asks_agent_to_ask") == Some(true);
            let source = if task.any_flag(&before, "agent_asked_questions").is_some() {
                "jev"
            } else {
                task.label_source(&before)
            };
            if asked {
                verdict(
                    "followed",
                    source,
                    "questions or assumptions were resolved before building",
                )
            } else {
                verdict(
                    "not_followed",
                    source,
                    "the agent built without asking or being asked for open questions",
                )
            }
        }
        "04_ground_it" => {
            if first_impl.is_none() {
                return verdict("na", "deterministic", "no implementation in this task");
            }
            let reads: i64 = before.iter().map(|t| t.reads + t.searches + t.web).sum();
            let explored = strictly_before
                .iter()
                .any(|t| matches!(t.stage.as_str(), "explore_codebase" | "research_external"));
            let cited = task.any_flag(&before, "cites_sources") == Some(true);
            if explored || reads >= 3 || cited {
                verdict(
                    "followed",
                    if cited { "jev" } else { "deterministic" },
                    format!("{reads} reads/searches before the first edit"),
                )
            } else {
                verdict(
                    "not_followed",
                    "deterministic",
                    format!("only {reads} reads/searches before the first edit"),
                )
            }
        }
        "05_small_slices" => {
            if task.attempts.is_empty() {
                return verdict("na", "deterministic", "no edit attempts");
            }
            let mut lines: Vec<i64> = task.attempts.iter().map(|(lines, _)| *lines).collect();
            lines.sort_unstable();
            let median = lines[lines.len() / 2];
            let checked = task
                .attempts
                .iter()
                .filter(|(_, result)| result != "none")
                .count();
            let checked_share = checked as f64 / task.attempts.len() as f64;
            let evidence = format!(
                "{} attempts, median {median} lines, {checked}/{} followed by a check",
                task.attempts.len(),
                task.attempts.len()
            );
            if median <= SMALL_SLICE_LINES && (task.attempts.len() == 1 || checked_share >= 0.5) {
                verdict("followed", "deterministic", evidence)
            } else {
                verdict("not_followed", "deterministic", evidence)
            }
        }
        "06_minimal_diff" => {
            if !wrote_code {
                return verdict("na", "deterministic", "no code changes");
            }
            let all: Vec<&Turn> = task.turns.iter().collect();
            let demanded = task.any_flag(&all, "demands_minimal_change") == Some(true);
            match task.any_flag(&all, "unrequested_changes") {
                Some(true) => verdict(
                    "not_followed",
                    "jev",
                    "the agent made changes nobody asked for",
                ),
                Some(false) if demanded => {
                    verdict("followed", "jev", "minimal changes were requested and kept")
                }
                Some(false) => verdict("followed", "jev", "changes stayed within the request"),
                None if demanded => {
                    verdict("followed", "heuristic", "minimal changes were requested")
                }
                None => verdict(
                    "unknown",
                    "deterministic",
                    "needs classification (unrequested changes)",
                ),
            }
        }
        "07_tests_can_fail" => {
            if !wrote_code {
                return verdict("na", "deterministic", "no code changes");
            }
            let first_test = task.turns.iter().position(|t| t.test_edits > 0);
            let first_source = task.turns.iter().position(|t| t.source_edits > 0);
            let all: Vec<&Turn> = task.turns.iter().collect();
            let weakened = task.any_flag(&all, "tests_changed_to_pass") == Some(true)
                || task.turns.windows(2).any(|w| {
                    w[0].test_failures > 0 && w[1].test_edits > 0 && w[1].source_edits == 0
                });
            if weakened {
                return verdict(
                    "not_followed",
                    "mixed",
                    "tests were changed after failing, without a code fix",
                );
            }
            match (first_test, first_source) {
                (Some(test), source) if source.is_none_or(|s| test <= s) => {
                    let failed_first = task.turns[test..]
                        .iter()
                        .find_map(|t| t.first_test_failed)
                        .unwrap_or(false);
                    if failed_first {
                        verdict(
                            "followed",
                            "deterministic",
                            "test written first and seen failing before it passed",
                        )
                    } else {
                        verdict(
                            "not_followed",
                            "deterministic",
                            "test written first but never seen failing",
                        )
                    }
                }
                (Some(_), _) => verdict(
                    "not_followed",
                    "deterministic",
                    "tests were written after the code",
                ),
                (None, _) => verdict(
                    "not_followed",
                    "deterministic",
                    "no tests were written or changed",
                ),
            }
        }
        "08_evidence" => {
            if !wrote_code {
                return verdict("na", "deterministic", "no code changes");
            }
            let last_edit = task.turns.iter().rposition(|t| t.edits > 0).unwrap_or(0);
            let checked = task.turns[last_edit..]
                .iter()
                .any(|t| t.validations > 0 || t.live_runs > 0);
            let all: Vec<&Turn> = task.turns.iter().collect();
            if task.any_flag(&all, "agent_claimed_done_without_evidence") == Some(true) && !checked
            {
                return verdict(
                    "not_followed",
                    "jev",
                    "done was claimed without a check after the last edit",
                );
            }
            if checked {
                verdict(
                    "followed",
                    "deterministic",
                    "a test, build or live run followed the last edit",
                )
            } else {
                verdict(
                    "not_followed",
                    "deterministic",
                    "no check after the last edit",
                )
            }
        }
        "09_independent_review" => {
            let Some(first) = first_impl else {
                return verdict("na", "deterministic", "no implementation in this task");
            };
            let reviewed_here = task.turns[first..].iter().any(|t| {
                t.stage == "review"
                    || t.request == "review_request"
                    || t.slash.as_deref().is_some_and(|s| s.contains("review"))
            });
            if reviewed_here || task.reviewer_subagent || task.later_review {
                let why = if task.later_review {
                    "reviewed in a later session on the same branch"
                } else if task.reviewer_subagent {
                    "a reviewer subagent checked the work"
                } else {
                    "a review step followed the implementation"
                };
                verdict(
                    "followed",
                    task.label_source(&task.turns.iter().collect::<Vec<_>>()),
                    why,
                )
            } else {
                verdict(
                    "not_followed",
                    "deterministic",
                    "no review after implementation",
                )
            }
        }
        "10_defend_without_ai" => {
            let Some(first) = first_impl else {
                return verdict("na", "deterministic", "no implementation in this task");
            };
            let questions = task.turns[first..]
                .iter()
                .filter(|t| t.request == "question_explain")
                .count();
            if questions > 0 {
                verdict(
                    "followed",
                    "heuristic",
                    format!("{questions} \"how does it work\" questions after implementation"),
                )
            } else {
                verdict(
                    "unknown",
                    "heuristic",
                    "no explain questions seen (understanding may happen outside the chat)",
                )
            }
        }
        "11_self_review" => {
            if !wrote_code {
                return verdict("na", "deterministic", "no code changes");
            }
            let last_edit = task.turns.iter().rposition(|t| t.edits > 0).unwrap_or(0);
            let diffs: i64 = task.turns[last_edit..].iter().map(|t| t.diff_checks).sum();
            if diffs > 0 {
                verdict(
                    "followed",
                    "deterministic",
                    "the diff was shown after the last edit",
                )
            } else {
                verdict(
                    "unknown",
                    "deterministic",
                    "self-review happens outside the transcript",
                )
            }
        }
        "12_rules_from_corrections" => match task.rule_commit_after_repeat {
            None => verdict("na", "deterministic", "no repeated correction"),
            Some(true) => verdict(
                "followed",
                "deterministic",
                "an instruction file or skill changed after a repeated correction",
            ),
            Some(false) => verdict(
                "not_followed",
                "deterministic",
                "corrections repeated across tasks; no instruction file changed",
            ),
        },
        _ => verdict("unknown", "deterministic", ""),
    }
}

/// Rebuild `derived_task_habit` from turn stages, evidence and labels.
pub fn refresh_habits(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM derived_task_habit", [])?;
    // Turns with stage labels and evidence, grouped by task.
    let mut by_task: HashMap<(String, String, i64), Vec<Turn>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT e.provider, e.session_id, e.task_index, e.turn_index,
                COALESCE(s.stage, 'chat_other'), COALESCE(s.stage_source, ''),
                COALESCE(s.request_kind, 'other'), COALESCE(s.request_source, ''),
                e.reads, e.searches, e.web, e.plan_tools, e.ask_tools, e.edits, e.test_edits,
                e.source_edits, e.validations, e.test_failures, e.first_test_failed, e.live_runs,
                e.diff_checks, e.slash_command
         FROM derived_turn_evidence e
         LEFT JOIN derived_turn_stage s
           ON s.provider = e.provider AND s.session_id = e.session_id AND s.turn_index = e.turn_index
         ORDER BY e.provider, e.session_id, e.turn_index",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ),
            Turn {
                index: row.get(3)?,
                stage: row.get(4)?,
                stage_source: row.get(5)?,
                request: row.get(6)?,
                request_source: row.get(7)?,
                reads: row.get(8)?,
                searches: row.get(9)?,
                web: row.get(10)?,
                plan_tools: row.get(11)?,
                ask_tools: row.get(12)?,
                edits: row.get(13)?,
                test_edits: row.get(14)?,
                source_edits: row.get(15)?,
                validations: row.get(16)?,
                test_failures: row.get(17)?,
                first_test_failed: row.get::<_, Option<i64>>(18)?.map(|v| v != 0),
                live_runs: row.get(19)?,
                diff_checks: row.get(20)?,
                slash: row.get(21)?,
            },
        ))
    })?;
    for row in rows {
        let (key, turn) = row?;
        by_task.entry(key).or_default().push(turn);
    }
    drop(stmt);

    // Effective yes/no turn flags.
    let flag_names: Vec<&str> = crate::classify::questions::TURN_FLAGS
        .iter()
        .map(|(id, _, _)| *id)
        .collect();
    let mut flags: HashMap<String, Vec<(String, bool)>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT unit_key, question_id, label FROM view_label_effective
         WHERE unit_kind = 'turn' AND band <> 'uncertain' AND label IN ('true', 'false')",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (key, question, label) = row?;
        if flag_names.contains(&question.as_str()) {
            flags
                .entry(key)
                .or_default()
                .push((question, label == "true"));
        }
    }
    drop(stmt);
    let framing: HashMap<String, i64> = conn
        .prepare(
            "SELECT unit_key, label FROM view_label_effective
             WHERE unit_kind = 'task' AND question_id = 'framing' AND band <> 'uncertain'",
        )?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|row| row.ok())
        .filter_map(|(key, label)| label.parse().ok().map(|v| (key, v)))
        .collect();

    let titles: TaskInfo = conn
        .prepare("SELECT provider, session_id, task_index, COALESCE(title, ''), repo_root, started_at FROM derived_task")?
        .query_map([], |row| Ok(((row.get(0)?, row.get(1)?, row.get(2)?), (row.get(3)?, row.get(4)?, row.get(5)?))))?
        .collect::<rusqlite::Result<_>>()?;
    let mut attempts: HashMap<(String, String, i64), Vec<(i64, String)>> = HashMap::new();
    let mut stmt = conn.prepare("SELECT provider, session_id, task_index, lines_changed, validation_result FROM derived_attempt")?;
    for row in stmt.query_map([], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ),
            (row.get::<_, i64>(3)?, row.get::<_, String>(4)?),
        ))
    })? {
        let (key, attempt) = row?;
        attempts.entry(key).or_default().push(attempt);
    }
    drop(stmt);
    let reviewer_sessions: HashSet<(String, String)> = conn
        .prepare(
            "SELECT provider, COALESCE(NULLIF(root_session_id, ''), session_id) FROM metadata_sessions
             WHERE LOWER(COALESCE(agent_type, '')) LIKE '%review%'",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let later_review: HashSet<(String, String, i64)> = conn
        .prepare(
            "SELECT r.from_provider, r.from_session, r.from_task
             FROM derived_task_relation r
             JOIN derived_task_stage s
               ON s.provider = r.to_provider AND s.session_id = r.to_session AND s.task_index = r.to_task
             WHERE r.relation_kind IN ('same_branch_followup', 'touches_earlier_ai_lines')
               AND s.dominant_stage = 'review'",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let rules = corrections_into_rules(conn, &by_task, &titles)?;

    let mut insert = conn.prepare(
        "INSERT INTO derived_task_habit (provider, session_id, task_index, habit, value, source, evidence)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    for (key, turns) in &by_task {
        let mut task_flags: HashMap<(i64, String), bool> = HashMap::new();
        for turn in turns {
            if let Some(values) = flags.get(&turn_unit_key(&key.0, &key.1, turn.index)) {
                for (name, value) in values {
                    task_flags.insert((turn.index, name.clone()), *value);
                }
            }
        }
        let title = titles.get(key).map(|t| t.0.as_str()).unwrap_or("");
        let no_attempts = Vec::new();
        let view = TaskView {
            turns,
            flags: &task_flags,
            framing: framing.get(&task_unit_key(&key.0, &key.1, key.2)).copied(),
            title,
            attempts: attempts.get(key).unwrap_or(&no_attempts),
            reviewer_subagent: reviewer_sessions.contains(&(key.0.clone(), key.1.clone())),
            later_review: later_review.contains(key),
            rule_commit_after_repeat: rules.get(key).copied(),
        };
        for (habit, _, _) in HABITS {
            let v = judge(habit, &view);
            insert.execute(params![
                key.0, key.1, key.2, habit, v.value, v.source, v.evidence
            ])?;
        }
    }
    Ok(())
}

/// Tasks with a correction whose repo had a correction in an earlier task
/// within 30 days: did an instruction file or skill change within 14 days?
fn corrections_into_rules(
    conn: &Connection,
    by_task: &HashMap<(String, String, i64), Vec<Turn>>,
    titles: &TaskInfo,
) -> Result<HashMap<(String, String, i64), bool>> {
    let mut corrected: Vec<(&TaskKey, String, chrono::DateTime<chrono::Utc>)> = Vec::new();
    for (key, turns) in by_task {
        if !turns.iter().any(|t| t.request == "correction") {
            continue;
        }
        let Some((_, Some(repo), Some(started))) = titles.get(key) else {
            continue;
        };
        if let Ok(at) = chrono::DateTime::parse_from_rfc3339(started) {
            corrected.push((key, repo.clone(), at.with_timezone(&chrono::Utc)));
        }
    }
    let mut rule_commits: HashMap<String, Vec<chrono::DateTime<chrono::Utc>>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT DISTINCT f.repo_root, c.commit_time, LOWER(f.rel_path)
         FROM fact_commit_file_change f
         JOIN fact_commit c ON c.repo_root = f.repo_root AND c.commit_sha = f.commit_sha",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (repo, time, path) = row?;
        if !INSTRUCTION_PATHS
            .iter()
            .any(|p| path.ends_with(p) || path.contains(p))
        {
            continue;
        }
        if let Ok(at) = chrono::DateTime::parse_from_rfc3339(&time) {
            rule_commits
                .entry(repo)
                .or_default()
                .push(at.with_timezone(&chrono::Utc));
        }
    }
    let mut out = HashMap::new();
    for (key, repo, at) in &corrected {
        let repeated = corrected.iter().any(|(other, other_repo, other_at)| {
            other != key
                && other_repo == repo
                && *other_at < *at
                && (*at - *other_at).num_days() <= 30
        });
        if !repeated {
            continue;
        }
        let followed = rule_commits.get(repo).is_some_and(|times| {
            times
                .iter()
                .any(|t| *t >= *at && (*t - *at).num_days() <= 14)
        });
        out.insert((*key).clone(), followed);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(index: i64, stage: &str, request: &str) -> Turn {
        Turn {
            index,
            stage: stage.into(),
            stage_source: "heuristic".into(),
            request: request.into(),
            request_source: "heuristic".into(),
            ..Turn::default()
        }
    }

    fn view<'a>(
        turns: &'a [Turn],
        flags: &'a HashMap<(i64, String), bool>,
        attempts: &'a [(i64, String)],
    ) -> TaskView<'a> {
        TaskView {
            turns,
            flags,
            framing: None,
            title: "Add the weekly flag",
            attempts,
            reviewer_subagent: false,
            later_review: false,
            rule_commit_after_repeat: None,
        }
    }

    #[test]
    fn plan_ground_and_ask_before_implementation() {
        let explore = Turn {
            reads: 4,
            ..turn(0, "explore_codebase", "new_request")
        };
        let plan = Turn {
            plan_tools: 1,
            ..turn(1, "plan_design", "approve_continue")
        };
        let build = Turn {
            edits: 2,
            source_edits: 2,
            ..turn(2, "implement", "approve_continue")
        };
        let turns = vec![explore, plan, build];
        let flags = HashMap::new();
        let v = view(&turns, &flags, &[]);
        assert_eq!(judge("02_plan_first", &v).value, "followed");
        assert_eq!(judge("04_ground_it", &v).value, "followed");
        assert_eq!(judge("03_make_it_ask", &v).value, "not_followed");
        let mut asked = HashMap::new();
        asked.insert((1, "agent_asked_questions".to_string()), true);
        let v = view(&turns, &asked, &[]);
        let verdict = judge("03_make_it_ask", &v);
        assert_eq!((verdict.value, verdict.source), ("followed", "jev"));

        let straight = vec![Turn {
            edits: 3,
            source_edits: 3,
            ..turn(0, "implement", "new_request")
        }];
        let v = view(&straight, &flags, &[]);
        assert_eq!(judge("02_plan_first", &v).value, "not_followed");
        assert_eq!(judge("04_ground_it", &v).value, "not_followed");
        let research = vec![turn(0, "explore_codebase", "new_request")];
        assert_eq!(
            judge("02_plan_first", &view(&research, &flags, &[])).value,
            "na"
        );
    }

    #[test]
    fn tests_first_failing_and_weakened() {
        let flags = HashMap::new();
        let test_first = vec![
            Turn {
                edits: 1,
                test_edits: 1,
                validations: 1,
                test_failures: 1,
                first_test_failed: Some(true),
                ..turn(0, "write_tests", "new_request")
            },
            Turn {
                edits: 1,
                source_edits: 1,
                validations: 1,
                first_test_failed: Some(false),
                ..turn(1, "implement", "approve_continue")
            },
        ];
        assert_eq!(
            judge("07_tests_can_fail", &view(&test_first, &flags, &[])).value,
            "followed"
        );
        let weakened = vec![
            Turn {
                edits: 1,
                source_edits: 1,
                validations: 1,
                test_failures: 1,
                ..turn(0, "implement", "new_request")
            },
            Turn {
                edits: 1,
                test_edits: 1,
                ..turn(1, "write_tests", "other")
            },
        ];
        let verdict = judge("07_tests_can_fail", &view(&weakened, &flags, &[]));
        assert_eq!(verdict.value, "not_followed");
        assert!(verdict.evidence.contains("changed after failing"));
    }

    #[test]
    fn slices_evidence_review_and_minimal_diff() {
        let flags = HashMap::new();
        let turns = vec![
            Turn {
                edits: 2,
                source_edits: 2,
                ..turn(0, "implement", "new_request")
            },
            Turn {
                validations: 1,
                ..turn(1, "verify_run", "approve_continue")
            },
            turn(2, "review", "review_request"),
        ];
        let small = vec![
            (40, "pass".to_string()),
            (60, "fail".to_string()),
            (30, "pass".to_string()),
        ];
        let v = view(&turns, &flags, &small);
        assert_eq!(judge("05_small_slices", &v).value, "followed");
        assert_eq!(judge("08_evidence", &v).value, "followed");
        assert_eq!(judge("09_independent_review", &v).value, "followed");
        assert_eq!(judge("06_minimal_diff", &v).value, "unknown");
        let big = vec![(900, "none".to_string()), (400, "none".to_string())];
        assert_eq!(
            judge("05_small_slices", &view(&turns, &flags, &big)).value,
            "not_followed"
        );
        let mut extra = HashMap::new();
        extra.insert((0, "unrequested_changes".to_string()), true);
        assert_eq!(
            judge("06_minimal_diff", &view(&turns, &extra, &small)).value,
            "not_followed"
        );
    }

    #[test]
    fn repeated_corrections_followed_by_instruction_change_count_as_rules() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        conn.execute(
            "INSERT INTO fact_commit(repo_root, commit_sha, commit_time, subject)
             VALUES ('/repo', 'abc', '2026-10-05T12:00:00Z', 'add rule')",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_commit_file_change(repo_root, commit_sha, rel_path, change_type)
             VALUES ('/repo', 'abc', 'AGENTS.md', 'modified')",
            [],
        )?;
        let corrected = |i: i64| {
            vec![
                turn(0, "implement", "new_request"),
                turn(i, "debug_fix", "correction"),
            ]
        };
        let mut by_task = HashMap::new();
        let mut titles = HashMap::new();
        for (session, day, repo) in [
            ("a", "01", "/repo"),
            ("b", "04", "/repo"),
            ("c", "04", "/other"),
        ] {
            let key = ("claude".to_string(), session.to_string(), 0);
            by_task.insert(key.clone(), corrected(1));
            titles.insert(
                key,
                (
                    String::new(),
                    Some(repo.to_string()),
                    Some(format!("2026-10-{day}T10:00:00Z")),
                ),
            );
        }
        let rules = corrections_into_rules(&conn, &by_task, &titles)?;
        assert_eq!(rules.get(&("claude".into(), "b".into(), 0)), Some(&true));
        assert_eq!(
            rules.get(&("claude".into(), "a".into(), 0)),
            None,
            "first correction is not a repeat"
        );
        assert_eq!(
            rules.get(&("claude".into(), "c".into(), 0)),
            None,
            "other repo"
        );
        Ok(())
    }
}
