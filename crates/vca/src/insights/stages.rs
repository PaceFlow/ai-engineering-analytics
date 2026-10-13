//! Work stages: what each user turn asked for and what kind of work it was
//! (explore, plan, implement, verify, review, …), rolled up per task and
//! session.
//!
//! Deterministic per-turn evidence (tool categories, test-file edits, checks)
//! is stored first; heuristic stage and request labels are derived from it and
//! stored as `source = 'heuristic'`, so classifier (Jev) labels and your
//! overrides win through `view_label_effective`.

use anyhow::Result;
use rusqlite::{Connection, params};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};

use super::tools::{is_ask_tool, is_diff_check, is_test_path, shell_read_kind, slash_command};
use crate::classify::heuristic::{exchange_feedback, insert_heuristic, turn_unit_key};
use crate::classify::state::load_turns;

pub(crate) const STAGES_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_turn_evidence (
    provider            TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    turn_index          INTEGER NOT NULL,
    task_index          INTEGER NOT NULL DEFAULT 0,
    reads               INTEGER NOT NULL DEFAULT 0,
    searches            INTEGER NOT NULL DEFAULT 0,
    web                 INTEGER NOT NULL DEFAULT 0,
    plan_tools          INTEGER NOT NULL DEFAULT 0,
    ask_tools           INTEGER NOT NULL DEFAULT 0,
    agent_calls         INTEGER NOT NULL DEFAULT 0,
    edits               INTEGER NOT NULL DEFAULT 0,
    test_edits          INTEGER NOT NULL DEFAULT 0,
    source_edits        INTEGER NOT NULL DEFAULT 0,
    doc_edits           INTEGER NOT NULL DEFAULT 0,
    validations         INTEGER NOT NULL DEFAULT 0,
    validation_failures INTEGER NOT NULL DEFAULT 0,
    test_runs           INTEGER NOT NULL DEFAULT 0,
    test_failures       INTEGER NOT NULL DEFAULT 0,
    first_test_failed   INTEGER,
    live_runs           INTEGER NOT NULL DEFAULT 0,
    diff_checks         INTEGER NOT NULL DEFAULT 0,
    slash_command       TEXT,
    user_chars          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(provider, session_id, turn_index)
);

CREATE TABLE IF NOT EXISTS derived_turn_stage (
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    turn_index      INTEGER NOT NULL,
    task_index      INTEGER NOT NULL DEFAULT 0,
    stage           TEXT NOT NULL,
    stage_source    TEXT NOT NULL,
    stage_band      TEXT,
    request_kind    TEXT,
    request_source  TEXT,
    cost_usd        REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(provider, session_id, turn_index)
);

CREATE TABLE IF NOT EXISTS derived_task_stage (
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    task_index      INTEGER NOT NULL,
    stage_path      TEXT NOT NULL,
    dominant_stage  TEXT,
    stage_costs     TEXT,
    PRIMARY KEY(provider, session_id, task_index)
);

CREATE TABLE IF NOT EXISTS derived_session_kind (
    provider    TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    PRIMARY KEY(provider, session_id)
);
";

/// Session-kind group → (cost, turns).
type GroupTotals = BTreeMap<&'static str, (f64, i64)>;

/// Short display names used in stage paths.
pub fn short_stage(stage: &str) -> &'static str {
    match stage {
        "explore_codebase" => "explore",
        "research_external" => "research",
        "plan_design" => "plan",
        "clarify_requirements" => "clarify",
        "implement" => "implement",
        "write_tests" => "tests",
        "verify_run" => "verify",
        "debug_fix" => "debug",
        "review" => "review",
        "refine_polish" => "refine",
        "docs" => "docs",
        "ship" => "ship",
        _ => "chat",
    }
}

/// Session kind group of a stage.
fn stage_group(stage: &str) -> &'static str {
    match stage {
        "explore_codebase" | "research_external" => "exploration",
        "plan_design" | "clarify_requirements" => "planning",
        "debug_fix" => "debugging",
        "review" => "review",
        "refine_polish" => "refinement",
        "chat_other" => "other",
        _ => "implementation",
    }
}

const DOC_EXTENSIONS: [&str; 4] = [".md", ".mdx", ".rst", ".txt"];

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnEvidence {
    pub reads: i64,
    pub searches: i64,
    pub web: i64,
    pub plan_tools: i64,
    pub ask_tools: i64,
    pub agent_calls: i64,
    pub edits: i64,
    pub test_edits: i64,
    pub source_edits: i64,
    pub doc_edits: i64,
    /// Doc edits to plan/design/spec files.
    pub plan_doc_edits: i64,
    pub validations: i64,
    pub validation_failures: i64,
    pub test_runs: i64,
    pub test_failures: i64,
    pub first_test_failed: Option<bool>,
    pub live_runs: i64,
    pub diff_checks: i64,
    pub slash_command: Option<String>,
    pub user_text: String,
}

/// Heuristic stage of a turn from its evidence.
pub fn heuristic_stage(evidence: &TurnEvidence, previous_failed: bool) -> &'static str {
    let text = evidence.user_text.to_ascii_lowercase();
    let slash = evidence.slash_command.as_deref().unwrap_or("");
    let review_asked = slash.contains("review")
        || slash.contains("audit")
        || text.contains("review this")
        || text.contains("review the")
        || text.starts_with("review");
    if evidence.edits == 0 {
        if review_asked {
            return "review";
        }
        if evidence.plan_tools > 0
            || slash == "plan"
            || text.contains("make a plan")
            || text.contains("create a plan")
        {
            return "plan_design";
        }
        if evidence.ask_tools > 0 {
            return "clarify_requirements";
        }
        if evidence.validations > 0 {
            return "verify_run";
        }
        if evidence.web > 0 && evidence.web >= evidence.reads + evidence.searches {
            return "research_external";
        }
        if evidence.reads + evidence.searches > 0 {
            return "explore_codebase";
        }
        if ["commit", "push", "open a pr", "pull request"]
            .iter()
            .any(|cue| text.contains(cue))
        {
            return "ship";
        }
        return "chat_other";
    }
    if evidence.source_edits == 0 && evidence.test_edits > 0 {
        return "write_tests";
    }
    if evidence.source_edits == 0 && evidence.test_edits == 0 && evidence.plan_doc_edits > 0 {
        return "plan_design";
    }
    if evidence.source_edits == 0 && evidence.doc_edits > 0 {
        return "docs";
    }
    let correction = matches!(exchange_feedback(&evidence.user_text), "correct_defect");
    if previous_failed || correction || evidence.validation_failures > 0 && evidence.validations > 1
    {
        return "debug_fix";
    }
    if [
        "simplify", "rename", "clean up", "cleanup", "polish", "tidy", "refactor",
    ]
    .iter()
    .any(|cue| text.contains(cue))
    {
        return "refine_polish";
    }
    "implement"
}

fn has_numbered_steps(text: &str) -> bool {
    text.lines()
        .filter(|line| {
            let line = line.trim_start();
            line.starts_with("- ")
                || line.starts_with("* ")
                || line.split_once('.').is_some_and(|(n, _)| {
                    !n.is_empty() && n.len() <= 2 && n.chars().all(|c| c.is_ascii_digit())
                })
        })
        .count()
        >= 3
}

/// Heuristic request kind of a user message.
pub fn heuristic_request(text: &str, first_turn: bool) -> &'static str {
    let lower = text.to_ascii_lowercase();
    let trimmed = lower.trim();
    if let Some(slash) = slash_command(text)
        && (slash.contains("review") || slash.contains("audit"))
    {
        return "review_request";
    }
    if trimmed.starts_with("review")
        || lower.contains("review this")
        || lower.contains("review the")
    {
        return "review_request";
    }
    if (lower.contains("my plan")
        || lower.contains("here's the plan")
        || lower.contains("here is the plan")
        || lower.contains("approach:"))
        || (text.chars().count() > 300 && has_numbered_steps(text))
    {
        return "plan_handoff";
    }
    if crate::explore::practices::names_verification(text) {
        return "spec_criteria";
    }
    match exchange_feedback(text) {
        "correct_defect" | "correct_misunderstanding" => return "correction",
        "change_requirement" if !first_turn => return "requirement_change",
        _ => {}
    }
    if [
        "commit",
        "push it",
        "open a pr",
        "create a pr",
        "pull request",
    ]
    .iter()
    .any(|cue| lower.contains(cue))
        && text.chars().count() < 200
    {
        return "ship_commit";
    }
    if (trimmed.starts_with("how ") || trimmed.starts_with("why ") || trimmed.starts_with("what "))
        && trimmed.ends_with('?')
    {
        return "question_explain";
    }
    if first_turn {
        return "new_request";
    }
    if exchange_feedback(text) == "accept_continue" {
        return "approve_continue";
    }
    if ["simplify", "rename", "clean up", "polish", "tidy"]
        .iter()
        .any(|cue| lower.contains(cue))
    {
        return "refinement";
    }
    if text.chars().count() > 200 {
        return "new_request";
    }
    "other"
}

fn evidence_for(turn: &crate::classify::state::TurnFacts) -> TurnEvidence {
    let mut evidence = TurnEvidence {
        slash_command: slash_command(&turn.user),
        user_text: turn.user.clone(),
        ..TurnEvidence::default()
    };
    for tool in &turn.tools {
        let target = tool.target.as_deref().unwrap_or("");
        match tool.category.as_str() {
            "read" => evidence.reads += 1,
            "search" => evidence.searches += 1,
            "web" => evidence.web += 1,
            "plan" => evidence.plan_tools += 1,
            "agent" => evidence.agent_calls += 1,
            "edit" => {
                evidence.edits += 1;
                if is_test_path(target) {
                    evidence.test_edits += 1;
                } else if DOC_EXTENSIONS
                    .iter()
                    .any(|ext| target.to_ascii_lowercase().ends_with(ext))
                    && ["plan", "design", "spec", "rfc", "adr"].iter().any(|cue| {
                        target
                            .to_ascii_lowercase()
                            .rsplit(['/', '\\'])
                            .next()
                            .unwrap_or("")
                            .contains(cue)
                    })
                {
                    evidence.doc_edits += 1;
                    evidence.plan_doc_edits += 1;
                } else if DOC_EXTENSIONS
                    .iter()
                    .any(|ext| target.to_ascii_lowercase().ends_with(ext))
                {
                    evidence.doc_edits += 1;
                } else {
                    evidence.source_edits += 1;
                }
            }
            _ => {}
        }
        if is_ask_tool(&tool.name) || tool.category == "ask" {
            evidence.ask_tools += 1;
        }
        if let Some(kind) = tool.validation_kind.as_deref() {
            let failed = tool.exit_code.is_some_and(|code| code != 0) || tool.status == "error";
            evidence.validations += 1;
            evidence.validation_failures += failed as i64;
            if kind == "test" {
                evidence.test_runs += 1;
                evidence.test_failures += failed as i64;
                if evidence.first_test_failed.is_none() {
                    evidence.first_test_failed = Some(failed);
                }
            }
            if kind == "run" {
                evidence.live_runs += 1;
            }
        } else if matches!(tool.category.as_str(), "exec" | "validate") {
            if is_diff_check(target) {
                evidence.diff_checks += 1;
            } else {
                match shell_read_kind(target) {
                    Some("read") => evidence.reads += 1,
                    Some("search") => evidence.searches += 1,
                    _ => {}
                }
            }
        }
    }
    evidence
}

/// Rebuild turn evidence, heuristic turn labels and stage rollups. Runs after
/// structure and heuristic task labels.
pub fn refresh_stages(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "DELETE FROM derived_turn_evidence;
         DELETE FROM derived_turn_stage;
         DELETE FROM derived_task_stage;
         DELETE FROM derived_session_kind;
         DELETE FROM classification_result WHERE source = 'heuristic' AND unit_kind = 'turn';",
    )?;
    let sessions: Vec<(String, String)> = conn
        .prepare("SELECT DISTINCT provider, session_id FROM derived_task")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut task_of_turn: HashMap<(String, String, i64), i64> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index, COALESCE(turn_list, '') FROM derived_task",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })? {
        let (provider, session, task, list) = row?;
        for turn in list.split(',').filter_map(|v| v.trim().parse::<i64>().ok()) {
            task_of_turn.insert((provider.clone(), session.clone(), turn), task);
        }
    }
    let turn_cost: HashMap<(String, String, i64), f64> = conn
        .prepare(
            "SELECT provider, root_session_id, root_turn_index, SUM(COALESCE(api_equiv_usd, 0))
             FROM derived_usage_event WHERE root_turn_index IS NOT NULL
             GROUP BY provider, root_session_id, root_turn_index",
        )?
        .query_map([], |row| {
            Ok(((row.get(0)?, row.get(1)?, row.get(2)?), row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut insert_evidence = conn.prepare(
        "INSERT INTO derived_turn_evidence (
            provider, session_id, turn_index, task_index, reads, searches, web, plan_tools, ask_tools,
            agent_calls, edits, test_edits, source_edits, doc_edits, validations, validation_failures,
            test_runs, test_failures, first_test_failed, live_runs, diff_checks, slash_command, user_chars
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)",
    )?;
    for (provider, session) in &sessions {
        let turns = load_turns(conn, provider, session)?;
        let mut previous_failed = false;
        for (position, turn) in turns.iter().enumerate() {
            let evidence = evidence_for(turn);
            let task = task_of_turn
                .get(&(provider.clone(), session.clone(), turn.index))
                .copied()
                .unwrap_or(0);
            insert_evidence.execute(params![
                provider,
                session,
                turn.index,
                task,
                evidence.reads,
                evidence.searches,
                evidence.web,
                evidence.plan_tools,
                evidence.ask_tools,
                evidence.agent_calls,
                evidence.edits,
                evidence.test_edits,
                evidence.source_edits,
                evidence.doc_edits,
                evidence.validations,
                evidence.validation_failures,
                evidence.test_runs,
                evidence.test_failures,
                evidence.first_test_failed.map(i64::from),
                evidence.live_runs,
                evidence.diff_checks,
                evidence.slash_command,
                turn.user.chars().count() as i64,
            ])?;
            let key = turn_unit_key(provider, session, turn.index);
            insert_heuristic(
                conn,
                "turn",
                &key,
                "turn_stage",
                heuristic_stage(&evidence, previous_failed),
            )?;
            insert_heuristic(
                conn,
                "turn",
                &key,
                "request_kind",
                heuristic_request(&turn.user, position == 0),
            )?;
            previous_failed = evidence.validations > 0
                && turn
                    .tools
                    .iter()
                    .rev()
                    .find(|t| t.validation_kind.is_some())
                    .is_some_and(|t| {
                        t.exit_code.is_some_and(|code| code != 0) || t.status == "error"
                    });
        }
    }
    drop(insert_evidence);

    // Effective labels → per-turn stage rows.
    let labels: HashMap<(String, String), (String, String, String)> = conn
        .prepare(
            "SELECT unit_key, question_id, label, source, band FROM view_label_effective
             WHERE unit_kind = 'turn' AND question_id IN ('turn_stage', 'request_kind')",
        )?
        .query_map([], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                (row.get(2)?, row.get(3)?, row.get(4)?),
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let rows: Vec<(String, String, i64, i64)> = conn
        .prepare("SELECT provider, session_id, turn_index, task_index FROM derived_turn_evidence ORDER BY provider, session_id, turn_index")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut insert_stage = conn.prepare(
        "INSERT INTO derived_turn_stage (
            provider, session_id, turn_index, task_index, stage, stage_source, stage_band,
            request_kind, request_source, cost_usd
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    // (provider, session, task) -> ordered stages with cost; session -> group costs
    let mut task_stages: BTreeMap<(String, String, i64), Vec<(String, f64)>> = BTreeMap::new();
    let mut session_groups: BTreeMap<(String, String), GroupTotals> = BTreeMap::new();
    for (provider, session, turn, task) in rows {
        let key = turn_unit_key(&provider, &session, turn);
        let Some((stage, stage_source, stage_band)) = labels
            .get(&(key.clone(), "turn_stage".to_string()))
            .cloned()
        else {
            continue;
        };
        let request = labels.get(&(key, "request_kind".to_string())).cloned();
        let cost = turn_cost
            .get(&(provider.clone(), session.clone(), turn))
            .copied()
            .unwrap_or(0.0);
        insert_stage.execute(params![
            provider,
            session,
            turn,
            task,
            stage,
            stage_source,
            stage_band,
            request.as_ref().map(|r| r.0.clone()),
            request.as_ref().map(|r| r.1.clone()),
            cost
        ])?;
        task_stages
            .entry((provider.clone(), session.clone(), task))
            .or_default()
            .push((stage.clone(), cost));
        let group = session_groups
            .entry((provider, session))
            .or_default()
            .entry(stage_group(&stage))
            .or_default();
        group.0 += cost;
        group.1 += 1;
    }
    drop(insert_stage);

    let mut insert_task = conn.prepare(
        "INSERT INTO derived_task_stage (provider, session_id, task_index, stage_path, dominant_stage, stage_costs)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for ((provider, session, task), stages) in &task_stages {
        let mut path: Vec<&str> = Vec::new();
        let mut costs: BTreeMap<&str, f64> = BTreeMap::new();
        for (stage, cost) in stages {
            let short = short_stage(stage);
            *costs.entry(short).or_default() += cost;
            if short != "chat" && path.last() != Some(&short) {
                path.push(short);
            }
        }
        let dominant = costs
            .iter()
            .filter(|(stage, _)| **stage != "chat")
            .max_by(|a, b| a.1.total_cmp(b.1))
            .map(|(stage, _)| stage.to_string());
        insert_task.execute(params![
            provider,
            session,
            task,
            path.join("→"),
            dominant,
            json!(costs).to_string()
        ])?;
    }
    drop(insert_task);

    let mut insert_kind = conn.prepare(
        "INSERT INTO derived_session_kind (provider, session_id, kind) VALUES (?1, ?2, ?3)",
    )?;
    for ((provider, session), groups) in &session_groups {
        insert_kind.execute(params![provider, session, session_kind(groups)])?;
    }
    Ok(())
}

/// The group holding ≥60% of cost (turn count when nothing was priced), else `mixed`.
fn session_kind(groups: &GroupTotals) -> &'static str {
    let total_cost: f64 = groups
        .iter()
        .filter(|(g, _)| **g != "other")
        .map(|(_, v)| v.0)
        .sum();
    let total_turns: i64 = groups
        .iter()
        .filter(|(g, _)| **g != "other")
        .map(|(_, v)| v.1)
        .sum();
    let share = |value: &(f64, i64)| {
        if total_cost > 0.0 {
            value.0 / total_cost
        } else if total_turns > 0 {
            value.1 as f64 / total_turns as f64
        } else {
            0.0
        }
    };
    groups
        .iter()
        .filter(|(group, _)| **group != "other")
        .find(|(_, value)| share(value) >= 0.6)
        .map(|(group, _)| *group)
        .unwrap_or(if total_turns == 0 { "other" } else { "mixed" })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(text: &str) -> TurnEvidence {
        TurnEvidence {
            user_text: text.into(),
            ..TurnEvidence::default()
        }
    }

    #[test]
    fn heuristic_stage_from_tool_sequences() {
        let explore = TurnEvidence {
            reads: 4,
            searches: 2,
            ..evidence("how does the matcher work?")
        };
        assert_eq!(heuristic_stage(&explore, false), "explore_codebase");
        let plan = TurnEvidence {
            plan_tools: 1,
            reads: 3,
            ..evidence("plan the migration")
        };
        assert_eq!(heuristic_stage(&plan, false), "plan_design");
        let plan_doc = TurnEvidence {
            edits: 1,
            doc_edits: 1,
            plan_doc_edits: 1,
            ..evidence("write the plan")
        };
        assert_eq!(heuristic_stage(&plan_doc, false), "plan_design");
        let tests = TurnEvidence {
            edits: 2,
            test_edits: 2,
            ..evidence("add tests")
        };
        assert_eq!(heuristic_stage(&tests, false), "write_tests");
        let implement = TurnEvidence {
            edits: 3,
            source_edits: 3,
            ..evidence("add the flag")
        };
        assert_eq!(heuristic_stage(&implement, false), "implement");
        assert_eq!(heuristic_stage(&implement, true), "debug_fix");
        let verify = TurnEvidence {
            validations: 2,
            ..evidence("run the tests")
        };
        assert_eq!(heuristic_stage(&verify, false), "verify_run");
        let review = TurnEvidence {
            reads: 5,
            slash_command: Some("code-review".into()),
            ..evidence("/code-review")
        };
        assert_eq!(heuristic_stage(&review, false), "review");
        assert_eq!(heuristic_stage(&evidence("thanks"), false), "chat_other");
    }

    #[test]
    fn heuristic_request_kinds() {
        assert_eq!(
            heuristic_request("/review the diff", false),
            "review_request"
        );
        let plan = "My plan:\n1. parse usage per call\n2. price per model\n3. add tests\nThis keeps the sync payload unchanged and lets us compare totals before and after the migration without guessing at anything else in the pipeline.";
        assert_eq!(heuristic_request(plan, true), "plan_handoff");
        assert_eq!(
            heuristic_request("that broke the build, still failing", false),
            "correction"
        );
        assert_eq!(
            heuristic_request("how does the matcher handle renames?", false),
            "question_explain"
        );
        assert_eq!(heuristic_request("Add a weekly flag", true), "new_request");
        assert_eq!(
            heuristic_request("commit and push it", false),
            "ship_commit"
        );
    }

    #[test]
    fn session_kind_needs_a_clear_majority() {
        let mut groups = BTreeMap::new();
        groups.insert("exploration", (7.0, 3));
        groups.insert("implementation", (3.0, 2));
        assert_eq!(session_kind(&groups), "exploration");
        groups.insert("review", (5.0, 2));
        assert_eq!(session_kind(&groups), "mixed");
    }
}
