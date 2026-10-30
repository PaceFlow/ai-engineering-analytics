//! Local keyword and activity heuristics that fill the same label slots as
//! the optional classifier, marked `source = 'heuristic'`. They are the
//! baseline the classifier must beat and keep every view usable without it.

use anyhow::Result;
use rusqlite::{Connection, params};

pub const QUESTION_VERSION: &str = "heuristic-2";

fn contains_any(text: &str, words: &[&str]) -> bool {
    words.iter().any(|word| text.contains(word))
}

/// Intent from the opening request and the task's tool activity.
pub fn task_intent(first_request: &str, edit_calls: i64, reads: i64) -> &'static str {
    let request = super::request::normalize(first_request);
    let text = request.text.to_lowercase();
    if request.extraction == "uncertain" {
        return "other";
    }
    if contains_any(
        &text,
        &[
            "cache",
            "maintenance",
            "retention",
            "deploy",
            "pipeline",
            "reliability",
            "config",
        ],
    ) && !contains_any(&text, &["write documentation", "document the", "readme"])
    {
        return "ops_config";
    }
    if contains_any(&text, &["fix", "bug", "broken", "crash", "regression"]) {
        return "bug_fix";
    }
    if contains_any(
        &text,
        &[
            "review this",
            "review the",
            "code review",
            "review pr",
            "review my",
        ],
    ) {
        return "review";
    }
    if edit_calls == 0
        && contains_any(
            &text,
            &[
                "plan ",
                "plan.",
                "design",
                "proposal",
                "approach",
                "architecture",
                "spec ",
            ],
        )
    {
        return "planning_design";
    }
    if contains_any(
        &text,
        &["why ", "investigate", "debug", "root cause", "figure out"],
    ) && edit_calls <= 2
    {
        return "debug_investigate";
    }
    if edit_calls == 0 {
        return if reads > 0 || !text.trim().is_empty() {
            "research_explain"
        } else {
            "other"
        };
    }
    if contains_any(
        &text,
        &[
            "fix",
            "bug",
            "broken",
            "error",
            "crash",
            "failing",
            "regression",
        ],
    ) {
        return "bug_fix";
    }
    if contains_any(
        &text,
        &[
            "refactor", "clean up", "cleanup", "rename", "simplify", "extract ",
        ],
    ) {
        return "refactor";
    }
    if contains_any(&text, &["test", "coverage"]) {
        return "tests";
    }
    if contains_any(
        &text,
        &["readme", "docs", "documentation", "changelog", "comment"],
    ) {
        return "docs";
    }
    if contains_any(
        &text,
        &[
            "ci ",
            "pipeline",
            "deploy",
            "dependency",
            "dependencies",
            "upgrade",
            "release",
            "docker",
            "config",
        ],
    ) {
        return "ops_config";
    }
    "feature"
}

/// Outcome from deterministic completion evidence and the attempt sequence.
pub fn task_outcome(
    completion_source: &str,
    edit_calls: i64,
    validations: i64,
    loop_attempts: i64,
    last_attempt_result: Option<&str>,
    assistant_chars: i64,
) -> &'static str {
    match completion_source {
        "commit" | "validation_pass" => return "completed_verified",
        _ => {}
    }
    if loop_attempts > 0 && last_attempt_result == Some("fail") {
        return "stalled";
    }
    if edit_calls == 0 {
        return if assistant_chars > 0 {
            "answered"
        } else {
            "unclear"
        };
    }
    if last_attempt_result == Some("fail") {
        return "partial";
    }
    if validations == 0 {
        return "completed_unverified";
    }
    "unclear"
}

/// How a user message reacts to the previous assistant output.
pub fn exchange_feedback(user_text: &str) -> &'static str {
    let text = super::request::normalize(user_text)
        .text
        .trim()
        .to_lowercase();
    if text.is_empty() {
        return "other";
    }
    if contains_any(
        &text,
        &[
            "still failing",
            "still fails",
            "same error",
            "that broke",
            "doesn't work",
            "does not work",
            "didn't work",
            "not working",
            "error:",
            "error[",
            "panic",
            "traceback",
            "broken",
        ],
    ) {
        return "correct_defect";
    }
    if contains_any(
        &text,
        &[
            "no, i meant",
            "not what i",
            "i meant",
            "that's not what",
            "misunderstood",
            "wrong file",
            "you misread",
        ],
    ) {
        return "correct_misunderstanding";
    }
    if contains_any(
        &text,
        &[
            "actually",
            "instead",
            "change it to",
            "also make",
            "let's make it",
            "rather than",
        ],
    ) {
        return "change_requirement";
    }
    if contains_any(
        &text,
        &[
            "separate thing",
            "unrelated",
            "new task",
            "different topic",
            "switching to",
            "next task",
        ],
    ) {
        return "redirect_new_goal";
    }
    if text.ends_with('?')
        || text.starts_with("why ")
        || text.starts_with("what ")
        || text.starts_with("how ")
    {
        return "question";
    }
    if contains_any(
        &text,
        &[
            "thanks",
            "great",
            "looks good",
            "lgtm",
            "perfect",
            "ok ",
            "okay",
            "go ahead",
            "yes",
            "continue",
            "proceed",
            "now ",
        ],
    ) {
        return "accept_continue";
    }
    "other"
}

/// Store one heuristic label (used by other insight modules too).
pub(crate) fn insert_heuristic(
    conn: &Connection,
    unit_kind: &str,
    unit_key: &str,
    question: &str,
    label: &str,
) -> Result<()> {
    insert(conn, unit_kind, unit_key, question, label)
}

fn insert(
    conn: &Connection,
    unit_kind: &str,
    unit_key: &str,
    question: &str,
    label: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO classification_result (
            unit_kind, unit_key, question_id, question_version, source, label, band, cache_key
         ) VALUES (?1, ?2, ?3, ?4, 'heuristic', ?5, 'heuristic', 'heuristic')",
        params![unit_kind, unit_key, question, QUESTION_VERSION, label],
    )?;
    Ok(())
}

pub fn task_unit_key(provider: &str, session_id: &str, task_index: i64) -> String {
    format!("{provider}|{session_id}|{task_index}")
}

pub fn turn_unit_key(provider: &str, session_id: &str, turn_index: i64) -> String {
    format!("{provider}|{session_id}|{turn_index}")
}

/// Rebuild heuristic labels for all tasks and exchanges.
pub fn refresh_heuristic_labels(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM classification_result WHERE source = 'heuristic'",
        [],
    )?;

    let mut stmt = conn.prepare(
        "SELECT t.provider, t.session_id, t.task_index, COALESCE(t.title, ''), t.edit_calls,
                t.validations, t.loop_attempts, t.completion_source,
                (SELECT a.validation_result FROM derived_attempt a
                  WHERE a.provider = t.provider AND a.session_id = t.session_id
                    AND a.task_index = t.task_index
                  ORDER BY a.attempt_index DESC LIMIT 1),
                (SELECT COALESCE(SUM(dt.assistant_chars), 0) FROM derived_turn dt
                  WHERE dt.provider = t.provider AND dt.session_id = t.session_id
                    AND (',' || t.turn_list || ',') LIKE ('%,' || dt.turn_index || ',%')),
                (SELECT COUNT(*) FROM fact_session_tool_call c
                  WHERE c.provider = t.provider AND c.session_id = t.session_id
                    AND c.tool_category IN ('read', 'search'))
         FROM derived_task t",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (
        provider,
        session_id,
        task,
        title,
        edits,
        validations,
        loops,
        completion,
        last,
        assistant,
        reads,
    ) in rows
    {
        let key = task_unit_key(&provider, &session_id, task);
        let (purpose, subtype) = corrective_purpose(&title);
        insert(conn, "task", &key, "purpose", purpose)?;
        insert(conn, "task", &key, "corrective_subtype", subtype)?;
        insert(
            conn,
            "task",
            &key,
            "intent",
            task_intent(&title, edits, reads),
        )?;
        insert(
            conn,
            "task",
            &key,
            "outcome",
            task_outcome(
                &completion,
                edits,
                validations,
                loops,
                last.as_deref(),
                assistant,
            ),
        )?;
        if loops > 0 {
            insert(conn, "task", &key, "p_repeated_failure", "true")?;
        }
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, turn_index, COALESCE(user_text_head, '')
         FROM derived_turn
         WHERE turn_index > 0 AND root_session_id = session_id",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (provider, session_id, turn, text) in rows {
        insert(
            conn,
            "exchange",
            &turn_unit_key(&provider, &session_id, turn),
            "feedback",
            exchange_feedback(&text),
        )?;
    }

    // Why later work changed earlier AI lines: only a fix-like ask is evidence.
    let mut stmt = conn.prepare(
        "SELECT r.from_provider, r.from_session, r.from_task, r.to_provider, r.to_session, r.to_task,
                COALESCE(t.title, '')
         FROM derived_task_relation r
         LEFT JOIN derived_task t
           ON t.provider = r.to_provider AND t.session_id = r.to_session AND t.task_index = r.to_task
         WHERE r.relation_kind = 'touches_earlier_ai_lines'",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                (
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ),
                (
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ),
                row.get::<_, String>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (from, to, later_ask) in rows {
        insert(
            conn,
            "relation",
            &crate::insights::relations::relation_unit_key(&from, &to),
            "rework_kind",
            rework_kind(&later_ask),
        )?;
    }
    Ok(())
}

/// Heuristic rework kind from the later task's first request.
pub fn rework_kind(later_ask: &str) -> &'static str {
    if crate::analytics::is_fix_like_commit_message(later_ask)
        || contains_any(
            &later_ask.to_lowercase(),
            &["error", "failing", "crash", "wrong", "doesn't work"],
        )
    {
        "defect_correction"
    } else {
        "unclear"
    }
}

pub fn corrective_purpose(text: &str) -> (&'static str, &'static str) {
    let request = super::request::normalize(text);
    let text = request.text.to_lowercase();
    if request.extraction == "uncertain" || text.contains("substantive goal unavailable") {
        return ("unknown", "not_applicable");
    }
    if contains_any(
        &text,
        &["fix", "broken", "crash", "bug", "regression", "repair"],
    ) {
        return ("corrective", "defect_repair");
    }
    if contains_any(
        &text,
        &[
            "cache",
            "retention",
            "performance",
            "memory",
            "latency",
            "optimize",
            "tune",
        ],
    ) {
        return ("corrective", "performance_resource");
    }
    if contains_any(
        &text,
        &[
            "reliability",
            "deployment failure",
            "configure",
            "maintenance",
            "pin ",
        ],
    ) {
        return ("corrective", "configuration_reliability");
    }
    if contains_any(&text, &["usability", "polish", "simplify", "refine"]) {
        return ("corrective", "usability");
    }
    if contains_any(&text, &["improve", "adjust"]) {
        return ("corrective", "unspecified");
    }
    if contains_any(&text, &["add ", "implement ", "create ", "build "]) {
        return ("new_capability", "not_applicable");
    }
    ("other", "not_applicable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heuristic_intent_examples() {
        assert_eq!(
            task_intent("Ingest crashes on empty Cursor DB, fix it", 3, 2),
            "bug_fix"
        );
        assert_eq!(
            task_intent("How does the matcher handle renames?", 0, 5),
            "research_explain"
        );
        assert_eq!(
            task_intent("Plan the schema v2 migration", 0, 3),
            "planning_design"
        );
        assert_eq!(
            task_intent("Split analytics.rs into modules", 8, 2),
            "feature"
        );
        assert_eq!(
            task_intent("Refactor analytics.rs into modules", 8, 2),
            "refactor"
        );
        assert_eq!(
            task_intent("Review this PR for correctness", 0, 4),
            "review"
        );
    }

    #[test]
    fn heuristic_outcome_examples() {
        assert_eq!(
            task_outcome("commit", 4, 2, 0, Some("pass"), 10),
            "completed_verified"
        );
        assert_eq!(task_outcome("none", 6, 5, 3, Some("fail"), 10), "stalled");
        assert_eq!(task_outcome("none", 0, 0, 0, None, 400), "answered");
        assert_eq!(
            task_outcome("none", 3, 0, 0, None, 400),
            "completed_unverified"
        );
    }

    #[test]
    fn heuristic_feedback_examples() {
        assert_eq!(
            exchange_feedback("still failing: E0308 in sync.rs"),
            "correct_defect"
        );
        assert_eq!(
            exchange_feedback("No, I meant per-session, not per-model"),
            "correct_misunderstanding"
        );
        assert_eq!(
            exchange_feedback("Actually make it weekly instead"),
            "change_requirement"
        );
        assert_eq!(exchange_feedback("Great, now add tests"), "accept_continue");
        assert_eq!(
            exchange_feedback("Why did you use a HashMap here?"),
            "question"
        );
    }
}
