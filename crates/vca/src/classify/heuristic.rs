//! Local keyword and activity heuristics that fill the same label slots as
//! the optional classifier, marked `source = 'heuristic'`. They are the
//! baseline the classifier must beat and keep every view usable without it.

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{BTreeSet, HashMap};

pub const QUESTION_VERSION: &str = "heuristic-3";

fn contains_any(text: &str, words: &[&str]) -> bool {
    words.iter().any(|word| text.contains(word))
}

/// Edits a task made, by kind of file (see `stages::edit_kind`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EditMix {
    /// All edit calls, as counted by task structure.
    pub edits: i64,
    pub docs: i64,
    /// Source and test files.
    pub code: i64,
    /// Temporary files and reply drafts; not work on the project.
    pub scratch: i64,
}

impl EditMix {
    pub fn only(edits: i64) -> Self {
        Self {
            edits,
            ..Self::default()
        }
    }
}

/// The request itself asks for documentation.
fn asks_for_docs(text: &str) -> bool {
    contains_any(
        text,
        &[
            "readme",
            "documentation",
            "document the",
            "document how",
            "write docs",
            "write the docs",
            "update docs",
            "update the docs",
            "add docs",
            "changelog",
            "docstring",
            "doc comment",
        ],
    )
}

/// Intent from the opening request and the task's tool activity.
///
/// Delivery wrappers are stripped by `request::normalize` first. `docs` needs
/// documentation edits: doc files only, or an explicit docs ask whose edits
/// are not mostly code. Generic words (config, deploy, pipeline) only decide
/// when nothing more specific matched.
pub fn task_intent(first_request: &str, mix: EditMix, reads: i64) -> &'static str {
    let request = super::request::normalize(first_request);
    if request.goal_unavailable() {
        return "other";
    }
    let text = request.text.to_lowercase();
    let edit_calls = (mix.edits - mix.scratch).max(0);
    let docs_asked = asks_for_docs(&text);
    if contains_any(
        &text,
        &[
            "cache",
            "maintenance",
            "retention",
            "reliability",
            "outage",
            "incident",
            "kubectl",
            "restart",
            "rollback",
            "redeploy",
        ],
    ) && !docs_asked
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
    if contains_any(&text, &["error", "failing", "repair"]) {
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
    if (mix.docs > 0 && mix.code == 0) || (docs_asked && mix.docs >= mix.code) {
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
            "namespace",
            "cluster",
            "helm",
            "terraform",
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
    let mixes = task_edit_mixes(conn)?;
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
        let mix = EditMix {
            edits,
            ..mixes
                .get(&(provider.clone(), session_id.clone(), task))
                .copied()
                .unwrap_or_default()
        };
        let key = task_unit_key(&provider, &session_id, task);
        let (purpose, subtype) = corrective_purpose(&title);
        insert(conn, "task", &key, "purpose", purpose)?;
        insert(conn, "task", &key, "corrective_subtype", subtype)?;
        insert(
            conn,
            "task",
            &key,
            "intent",
            task_intent(&title, mix, reads),
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

/// Doc, code and scratch edits per task, from each edit's target file.
fn task_edit_mixes(conn: &Connection) -> Result<HashMap<(String, String, i64), EditMix>> {
    use crate::insights::stages::{EditKind, edit_kind};
    let mut task_of_turn = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT t.provider, t.session_id, t.task_index, COALESCE(t.turn_list, '') FROM derived_task t
         WHERE EXISTS(SELECT 1 FROM fact_session_tool_call c
                      WHERE c.provider = t.provider AND c.session_id = t.session_id
                        AND c.tool_category = 'edit')",
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
    for (provider, session, task, list) in rows {
        for turn in list.split(',').filter_map(|v| v.trim().parse::<i64>().ok()) {
            task_of_turn.insert((provider.clone(), session.clone(), turn), task);
        }
    }
    let sessions: BTreeSet<(String, String)> = task_of_turn
        .keys()
        .map(|(provider, session, _)| (provider.clone(), session.clone()))
        .collect();
    let mut mixes: HashMap<(String, String, i64), EditMix> = HashMap::new();
    for (provider, session) in sessions {
        for turn in crate::classify::state::load_turns(conn, &provider, &session)? {
            let key = (provider.clone(), session.clone(), turn.index);
            let Some(&task) = task_of_turn.get(&key) else {
                continue;
            };
            let mix = mixes
                .entry((provider.clone(), session.clone(), task))
                .or_default();
            for tool in turn.tools.iter().filter(|tool| tool.category == "edit") {
                match edit_kind(tool.target.as_deref().unwrap_or("")) {
                    EditKind::Doc | EditKind::PlanDoc => mix.docs += 1,
                    EditKind::Scratch => mix.scratch += 1,
                    EditKind::Source | EditKind::Test => mix.code += 1,
                }
            }
        }
    }
    Ok(mixes)
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
    if request.goal_unavailable() {
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
            task_intent(
                "Ingest crashes on empty Cursor DB, fix it",
                EditMix::only(3),
                2
            ),
            "bug_fix"
        );
        assert_eq!(
            task_intent("How does the matcher handle renames?", EditMix::only(0), 5),
            "research_explain"
        );
        assert_eq!(
            task_intent("Plan the schema v2 migration", EditMix::only(0), 3),
            "planning_design"
        );
        assert_eq!(
            task_intent("Split analytics.rs into modules", EditMix::only(8), 2),
            "feature"
        );
        assert_eq!(
            task_intent("Refactor analytics.rs into modules", EditMix::only(8), 2),
            "refactor"
        );
        assert_eq!(
            task_intent("Review this PR for correctness", EditMix::only(0), 4),
            "review"
        );
    }

    #[test]
    fn reply_as_comment_ops_repair_is_not_docs() {
        let reply_draft = EditMix {
            edits: 1,
            scratch: 1,
            ..EditMix::default()
        };
        for ask in [
            "The nightly sync job stopped; restart it and confirm it runs. Reply as a comment.",
            "You are running as a local coding agent for a Multica workspace.
[NEW COMMENT] Another agent (Ops) just left a new comment.
> The sync worker stopped overnight, restart it.

Reply as a comment.",
        ] {
            assert_eq!(task_intent(ask, reply_draft, 3), "ops_config", "{ask}");
            assert_eq!(task_intent(ask, EditMix::only(0), 3), "ops_config");
        }
        // Commenting on an issue is delivery even when the job needs a code edit.
        let code = EditMix {
            edits: 2,
            code: 1,
            scratch: 1,
            ..EditMix::default()
        };
        assert_eq!(
            task_intent("Add CSV export. Post the summary as a comment.", code, 1),
            "feature"
        );
        // A docs path in the request is context, not a docs goal.
        assert_eq!(
            task_intent(
                "@docs/postmortems/POSTMORTEM_DB.md:104-108 implement these suggestions",
                EditMix {
                    edits: 4,
                    code: 4,
                    ..EditMix::default()
                },
                2
            ),
            "feature"
        );
    }

    #[test]
    fn real_docs_edits_are_still_docs() {
        let docs = EditMix {
            edits: 2,
            docs: 2,
            ..EditMix::default()
        };
        assert_eq!(
            task_intent("Add a VPN client section to README_VPN.md", docs, 1),
            "docs"
        );
        assert_eq!(
            task_intent("Explain the onboarding flow for new hires", docs, 1),
            "docs"
        );
        // An explicit docs ask with a small code touch stays docs.
        let mostly_docs = EditMix {
            edits: 3,
            docs: 2,
            code: 1,
            ..EditMix::default()
        };
        assert_eq!(
            task_intent("Update the docs for the sync command", mostly_docs, 1),
            "docs"
        );
        // Without doc edits, mentioning a readme is not enough.
        assert_eq!(
            task_intent(
                "Read README first, then add the export button",
                EditMix {
                    edits: 3,
                    code: 3,
                    ..EditMix::default()
                },
                1
            ),
            "feature"
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
