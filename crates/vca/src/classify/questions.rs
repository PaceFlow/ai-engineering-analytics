//! Jev questions. Jev answers typed questions (`noul`, `choice`, `score`)
//! about a JSON state; it does not generate text. Every question in one
//! request is answered independently, so dependent logic lives in our code.

use serde_json::{Map, Value, json};

/// Bump when instructions or criteria change; cached results are keyed by it.
pub const QUESTION_SET_VERSION: &str = "2026-10-q2";

pub const INTENTS: [(&str, &str); 12] = [
    ("feature", "Adding new user-visible behavior or capability."),
    (
        "bug_fix",
        "Making existing behavior correct after a reported or observed defect.",
    ),
    (
        "debug_investigate",
        "Finding the cause of a problem without (yet) a known fix.",
    ),
    (
        "refactor",
        "Restructuring code without intended behavior change.",
    ),
    ("tests", "Writing or fixing tests as the main goal."),
    (
        "research_explain",
        "Understanding code, a library or an API; the goal is answers, not edits.",
    ),
    (
        "planning_design",
        "Producing a plan, design or spec, or deciding an approach.",
    ),
    (
        "review",
        "Reviewing a diff, pull request or code for problems.",
    ),
    (
        "ops_config",
        "Build, CI, dependencies, environment, release or deployment work.",
    ),
    (
        "docs",
        "Writing documentation, README or comments as the goal.",
    ),
    (
        "data_scripting",
        "One-off data analysis, queries or scripts that are not part of the product.",
    ),
    ("other", "None of the above clearly fits."),
];

pub const OUTCOMES: [(&str, &str); 8] = [
    (
        "completed_verified",
        "The goal appears met AND a validation passed or the user explicitly confirmed (for example 'works', 'lgtm', then commits).",
    ),
    (
        "completed_unverified",
        "The assistant reported completion; no validation or user confirmation is visible.",
    ),
    (
        "answered",
        "An information, planning or review goal was answered or decided (non-code goals).",
    ),
    (
        "partial",
        "Clear progress, but the stated goal is not fully met at the end.",
    ),
    (
        "stalled",
        "Repeated attempts without measurable progress; the same failure persists.",
    ),
    (
        "abandoned_redirected",
        "The user dropped the goal or switched to a different goal without resolution.",
    ),
    (
        "blocked_external",
        "Progress depends on something outside the conversation: access, missing information, a broken environment.",
    ),
    (
        "unclear",
        "The evidence is insufficient to judge, for example a truncated transcript.",
    ),
];

pub const FEEDBACK: [(&str, &str); 8] = [
    (
        "accept_continue",
        "Approves the output, or asks for the next step building on it.",
    ),
    (
        "correct_defect",
        "Says the output is wrong or broken, or reports an error it caused.",
    ),
    (
        "correct_misunderstanding",
        "Says the assistant misunderstood the request and restates it.",
    ),
    (
        "change_requirement",
        "Changes or adds requirements; the output was not wrong for the original ask.",
    ),
    ("redirect_new_goal", "Starts a different goal."),
    (
        "clarify_answer",
        "Answers the assistant's question or supplies requested information.",
    ),
    ("question", "Asks a question about the output."),
    ("other", "Anything else (thanks, small talk, commands)."),
];

pub const REWORK_KINDS: [(&str, &str); 7] = [
    (
        "defect_correction",
        "The earlier output was wrong: a bug, failing behavior or broken build is being fixed.",
    ),
    (
        "requirement_change",
        "The earlier output met the earlier ask; the ask changed.",
    ),
    (
        "refinement",
        "Polish, refactor, naming or performance work on output that worked.",
    ),
    (
        "continuation",
        "Extends the earlier work as planned; the next step.",
    ),
    (
        "revert_abandon",
        "Removes the earlier output because the approach was dropped.",
    ),
    (
        "incidental",
        "The overlap is incidental: formatting, moved code, generated files.",
    ),
    ("unclear", "Cannot tell from the evidence."),
];

/// What the user's message asks for (Q17).
pub const REQUEST_KINDS: [(&str, &str); 12] = [
    (
        "new_request",
        "Starts a new piece of work or a new question.",
    ),
    (
        "plan_handoff",
        "Gives the user's own plan, design or step-by-step approach for the agent to follow or critique.",
    ),
    (
        "spec_criteria",
        "States acceptance criteria, expected behaviour, constraints or how to verify the result.",
    ),
    (
        "clarification_answer",
        "Answers a question the agent asked or supplies information it requested.",
    ),
    (
        "refinement",
        "Asks to polish, simplify or adjust output that already works.",
    ),
    (
        "correction",
        "Says the output is wrong, broken or misunderstood and asks for a fix.",
    ),
    (
        "requirement_change",
        "Changes or adds requirements; the earlier output was not wrong for the earlier ask.",
    ),
    (
        "question_explain",
        "Asks how or why something works, to understand the code or the change.",
    ),
    (
        "review_request",
        "Asks the agent to review, audit or check code, a diff, a plan or a pull request.",
    ),
    (
        "approve_continue",
        "Approves and asks to continue with the next step.",
    ),
    (
        "ship_commit",
        "Asks to commit, push, open a pull request or release.",
    ),
    (
        "other",
        "None of the above (thanks, small talk, a bare command).",
    ),
];

/// What kind of work happened in this turn (Q18).
pub const TURN_STAGES: [(&str, &str); 13] = [
    (
        "explore_codebase",
        "Reading and searching the existing code, data or docs to understand them; no edits.",
    ),
    (
        "research_external",
        "Looking up external documentation, APIs, libraries or the web.",
    ),
    (
        "plan_design",
        "Producing or discussing a plan, design, approach or task list before building.",
    ),
    (
        "clarify_requirements",
        "Asking or answering questions about what is wanted; resolving assumptions.",
    ),
    ("implement", "Writing or changing product code."),
    (
        "write_tests",
        "Writing or changing tests as the main activity.",
    ),
    (
        "verify_run",
        "Running tests, builds, the app or checks to verify behaviour, without changing code.",
    ),
    (
        "debug_fix",
        "Diagnosing and fixing a failure, error or bug that showed up.",
    ),
    (
        "review",
        "Reviewing code, a diff, a plan or a pull request for problems.",
    ),
    (
        "refine_polish",
        "Simplifying, renaming, cleaning up or adjusting already-working code.",
    ),
    (
        "docs",
        "Writing documentation, comments, changelog or instruction files.",
    ),
    (
        "ship",
        "Committing, pushing, opening a pull request or releasing.",
    ),
    ("chat_other", "Conversation without substantial work."),
];

/// Yes/no observations about one turn (Q19): (id, true when…, false when…).
pub const TURN_FLAGS: [(&str, &str, &str); 7] = [
    (
        "asks_agent_to_ask",
        "The user tells the agent to ask questions, list open questions or state its assumptions before acting.",
        "The user does not ask for questions or assumptions.",
    ),
    (
        "demands_minimal_change",
        "The user asks for minimal, targeted changes or forbids unrequested changes or rewrites.",
        "No request to keep changes minimal.",
    ),
    (
        "cites_sources",
        "The user points the agent to concrete sources: a ticket, docs, specific files, data or existing implementations.",
        "No concrete sources are referenced.",
    ),
    (
        "agent_asked_questions",
        "The agent asked the user clarifying questions or for a decision before acting.",
        "The agent acted without asking.",
    ),
    (
        "agent_claimed_done_without_evidence",
        "The agent says the work is done or working without running or showing any check (tests, run output, diff).",
        "Completion claims are backed by visible checks, or no completion is claimed.",
    ),
    (
        "unrequested_changes",
        "The agent changed things the user did not ask for: extra features, rewrites, unrelated files, scope creep.",
        "Changes stay within what was asked.",
    ),
    (
        "tests_changed_to_pass",
        "The agent weakened, deleted or rewrote tests so that they pass instead of fixing the code.",
        "Tests were not weakened to make them pass.",
    ),
];

pub const PROBLEMS: [(&str, &str, &str); 7] = [
    (
        "p_repeated_failure",
        "The same error or failing check recurs across two or more attempts without being resolved.",
        "Failures change, get fixed, or none occur.",
    ),
    (
        "p_misunderstood",
        "The assistant pursued something other than what the user asked, and the user had to restate the request.",
        "The assistant's actions match the request.",
    ),
    (
        "p_user_corrected_output",
        "The user pointed out a defect or error in output the assistant produced earlier in this task.",
        "The user reports no assistant mistakes.",
    ),
    (
        "p_requirements_changed",
        "The user changed or added requirements after seeing output, not because the output was wrong.",
        "Requirements stayed stable.",
    ),
    (
        "p_ignored_constraint",
        "The assistant violated an explicit user instruction or constraint (files not to touch, style, scope).",
        "No explicit instruction was violated.",
    ),
    (
        "p_env_tooling",
        "Time was lost to environment, tooling, permissions, dependency or network problems rather than the code itself.",
        "No significant environment or tooling problems.",
    ),
    (
        "p_hallucination",
        "The assistant referenced APIs, files, flags or facts that turned out not to exist.",
        "No references to things that do not exist.",
    ),
];

/// Valid labels for a fixed question (dynamic questions return `None`).
pub fn labels_for(question: &str) -> Option<Vec<&'static str>> {
    let names =
        |options: &[(&'static str, &'static str)]| options.iter().map(|(name, _)| *name).collect();
    Some(match question {
        "intent" => names(&INTENTS),
        "outcome" => names(&OUTCOMES),
        "feedback" => names(&FEEDBACK),
        "rework_kind" => names(&REWORK_KINDS),
        "framing" => vec!["0", "1", "2", "3"],
        "progress" => vec!["0", "1", "2", "3", "4"],
        "frustration" => vec!["0", "1", "2"],
        "request_kind" => names(&REQUEST_KINDS),
        "turn_stage" => names(&TURN_STAGES),
        id if TURN_FLAGS.iter().any(|(name, _, _)| *name == id) => vec!["true", "false"],
        id if PROBLEMS.iter().any(|(name, _, _)| *name == id) => vec!["true", "false"],
        _ => return None,
    })
}

/// Unit kind a fixed question is asked about.
pub fn unit_kind_for(question: &str) -> &'static str {
    match question {
        "feedback" | "frustration" => "exchange",
        "request_kind" | "turn_stage" => "turn",
        id if TURN_FLAGS.iter().any(|(name, _, _)| *name == id) => "turn",
        "rework_kind" => "relation",
        _ => "task",
    }
}

fn choice(instructions: &str, options: &[(&str, &str)]) -> Value {
    let criteria: Map<String, Value> = options
        .iter()
        .map(|(name, description)| (name.to_string(), Value::String(description.to_string())))
        .collect();
    json!({"type": "choice", "instructions": instructions, "criteria": criteria})
}

fn noul(instructions: &str, yes: &str, no: &str) -> Value {
    json!({"type": "noul", "instructions": instructions, "criteria": {"true": yes, "false": no}})
}

fn score(instructions: &str, levels: &[&str]) -> Value {
    json!({"type": "score", "instructions": instructions, "criteria": levels})
}

/// Q1–Q11, asked together about one task (privacy level decides which).
pub fn task_label_questions(level: crate::classify::state::Level) -> Map<String, Value> {
    use crate::classify::state::Level;
    let mut questions = Map::new();
    if level != Level::EvidenceOnly {
        questions.insert(
            "intent".into(),
            choice(
                "What kind of software work is the user pursuing in this task? Judge by the user's goal, not by what the assistant happened to do.",
                &INTENTS,
            ),
        );
        questions.insert(
            "framing".into(),
            score(
                "How well-specified was the initial request (the first user message of the task)?",
                &[
                    "Vague goal, no constraints (for example 'make it better').",
                    "Goal stated, no acceptance criteria.",
                    "Goal plus constraints or relevant files.",
                    "Goal plus acceptance criteria or how to verify (a test command, expected behavior).",
                ],
            ),
        );
    }
    questions.insert(
        "outcome".into(),
        choice(
            "By the end of the evidence shown, what is the state of the user's goal in this task? Use validation results and the user's last messages; do not assume success from the assistant's claims.",
            &OUTCOMES,
        ),
    );
    questions.insert(
        "progress".into(),
        score(
            "How far did the task get toward its goal?",
            &[
                "No movement toward the goal.",
                "Exploration only.",
                "Partial implementation, not validated.",
                "Mostly done, minor issues remaining.",
                "Goal reached.",
            ],
        ),
    );
    for (id, yes, no) in PROBLEMS {
        let needs_text = !matches!(id, "p_repeated_failure" | "p_env_tooling");
        if level == Level::EvidenceOnly && needs_text {
            continue;
        }
        if level == Level::UserText && id == "p_hallucination" {
            continue;
        }
        questions.insert(id.into(), noul("Does this hold for the task?", yes, no));
    }
    questions
}

/// Q12–Q13 about one user reply.
pub fn exchange_questions() -> Map<String, Value> {
    let mut questions = Map::new();
    questions.insert(
        "feedback".into(),
        choice(
            "How does the user's message respond to the assistant's previous output?",
            &FEEDBACK,
        ),
    );
    questions.insert(
        "frustration".into(),
        score(
            "How frustrated does the user's message read?",
            &["Neutral.", "Mild impatience.", "Clear frustration."],
        ),
    );
    questions
}

/// Q17–Q19 about one turn: request kind, stage and flags. Text-dependent
/// questions are skipped at `evidence_only`.
pub fn turn_questions(level: crate::classify::state::Level) -> Map<String, Value> {
    use crate::classify::state::Level;
    let mut questions = Map::new();
    questions.insert(
        "turn_stage".into(),
        choice(
            "What kind of work happens in this turn: the user's new message and everything the agent does until the next user message (see current_turn)? Judge by the actions, not by what the agent says it did.",
            &TURN_STAGES,
        ),
    );
    if level == Level::EvidenceOnly {
        return questions;
    }
    questions.insert(
        "request_kind".into(),
        choice("What does the user's new message ask for?", &REQUEST_KINDS),
    );
    for (id, yes, no) in TURN_FLAGS {
        if level < Level::Conversation
            && matches!(
                id,
                "agent_asked_questions" | "agent_claimed_done_without_evidence"
            )
        {
            continue;
        }
        questions.insert(id.into(), noul("Does this hold for this turn?", yes, no));
    }
    questions
}

/// Q14: which task a user turn belongs to, with options built from the
/// session's open tasks.
pub fn task_assignment_question(
    current: Option<(&str, &str)>,
    open: &[(i64, String)],
) -> Map<String, Value> {
    let mut options: Vec<(String, String)> = Vec::new();
    if let Some((label, request)) = current {
        options.push((
            "continue".into(),
            format!("Continues the current task {label}: '{request}'. Refinements, corrections and follow-up fixes of the same goal count as continuing."),
        ));
    }
    for (task, request) in open.iter().rev().take(20) {
        options.push((
            format!("resume_T{task}"),
            format!("Returns to earlier open task T{task}: '{request}'."),
        ));
    }
    options.push((
        "new_task".into(),
        "Starts a goal not covered by any task above.".into(),
    ));
    options.push((
        "not_task_work".into(),
        "Small talk, thanks, meta questions about the assistant, or a slash command without a goal.".into(),
    ));
    let refs: Vec<(&str, &str)> = options
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let mut questions = Map::new();
    questions.insert(
        "task_assignment".into(),
        choice(
            "Which task does the user's new message belong to? A task is one goal the user wants done.",
            &refs,
        ),
    );
    questions
}

/// Q16: the turn where a task was actually completed.
pub fn completion_question(turn_options: &[(i64, String)]) -> Map<String, Value> {
    let mut options: Vec<(String, String)> = turn_options
        .iter()
        .map(|(turn, description)| (format!("turn_{turn}"), description.clone()))
        .collect();
    options.push((
        "not_completed".into(),
        "The goal was never reached in this task.".into(),
    ));
    options.push(("unclear".into(), "Cannot tell from the evidence.".into()));
    let refs: Vec<(&str, &str)> = options
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let mut questions = Map::new();
    questions.insert(
        "completion_turn".into(),
        choice(
            "At which turn was this task's goal actually achieved? Achieved means the requested change works or the question was answered. Prefer turns where a validation passed or the user confirmed.",
            &refs,
        ),
    );
    questions
}

/// Q15: why later work changed lines earlier AI work wrote.
pub fn rework_question() -> Map<String, Value> {
    let mut questions = Map::new();
    questions.insert(
        "rework_kind".into(),
        choice(
            "The later work modifies lines produced by the earlier AI work. Why?",
            &REWORK_KINDS,
        ),
    );
    questions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::state::Level;

    #[test]
    fn evidence_only_level_drops_text_questions() {
        let full = task_label_questions(Level::Conversation);
        let evidence = task_label_questions(Level::EvidenceOnly);
        assert!(full.contains_key("intent") && full.contains_key("p_hallucination"));
        assert!(!evidence.contains_key("intent"));
        assert!(evidence.contains_key("outcome") && evidence.contains_key("p_repeated_failure"));
    }

    #[test]
    fn assignment_options_list_only_open_tasks_and_cap_choices() {
        let open: Vec<(i64, String)> = (0..30).map(|i| (i, format!("task {i}"))).collect();
        let question = task_assignment_question(Some(("T31", "current")), &open);
        let criteria = question["task_assignment"]["criteria"].as_object().unwrap();
        assert_eq!(criteria.len(), 1 + 20 + 2);
        assert!(criteria.contains_key("resume_T29"));
        assert!(!criteria.contains_key("resume_T0"));
    }
}
