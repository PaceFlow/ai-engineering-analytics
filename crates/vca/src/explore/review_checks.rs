//! The short checklist a person can verify by reading a session, and a plain
//! explanation for every number vca records.
//!
//! The review page shows ~10 statements ("You sent 34 messages", "These 21
//! commits came out of this session"), each with what it means, how to check
//! it and a button that highlights the evidence. Everything else (token types,
//! the context ledger, habit verdicts) is not something a person can verify by
//! eye; the corpus records it as an unreviewed regression lock instead.

use serde::Serialize;
use std::collections::HashMap;

use super::review::{FileTouch, Stat};
use super::{SessionDetail, short_id};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Check {
    pub id: String,
    pub title: String,
    /// What vca concluded, in a sentence.
    pub statement: String,
    /// What the number means.
    pub explain: String,
    /// How to verify it.
    pub how: String,
    /// Numbers this check approves or rejects.
    pub stats: Vec<String>,
    /// Timeline entries that back it.
    pub entries: Vec<usize>,
    /// Facts to look over: file paths, commit subjects, models.
    pub items: Vec<String>,
}

pub fn checks(detail: &SessionDetail, stats: &[Stat], files: &[FileTouch]) -> Vec<Check> {
    let by_id: HashMap<&str, &Stat> = stats.iter().map(|s| (s.id.as_str(), s)).collect();
    let int = |id: &str| by_id.get(id).and_then(|s| s.value.as_i64()).unwrap_or(0);
    let num = |id: &str| by_id.get(id).and_then(|s| s.value.as_f64());
    let entries = |id: &str| by_id.get(id).map(|s| s.entries.clone()).unwrap_or_default();
    let present = |ids: &[&str]| -> Vec<String> {
        ids.iter()
            .filter(|id| by_id.contains_key(**id))
            .map(|id| id.to_string())
            .collect()
    };
    let s = &detail.session;
    let mut out = Vec::new();

    let messages = int("shape.user_messages");
    out.push(Check {
        id: "messages".into(),
        title: "Your messages".into(),
        statement: format!("You sent {messages} message{} to the agent.", plural(messages)),
        explain: "Every message you typed to the main agent. Each one starts a new turn. Tool output and subagent prompts are not counted.".into(),
        how: "Press “Show me” and count the highlighted USER entries. Pasted text and slash commands count too.".into(),
        stats: present(&["shape.user_messages", "shape.turns"]),
        entries: entries("shape.user_messages"),
        items: vec![],
    });

    let wall = num("time.wall_minutes");
    let active = num("time.active_minutes");
    out.push(Check {
        id: "time".into(),
        title: "When and how long".into(),
        statement: format!(
            "From {} to {} ({} on the clock), about {} of it active.",
            when(s.started_at.as_deref()),
            when(s.ended_at.as_deref()),
            duration(wall),
            duration(active)
        ),
        explain: "Clock time runs from the first to the last event. Active time leaves out pauses longer than 30 minutes, so overnight breaks do not count.".into(),
        how: "Compare with when you remember working on it.".into(),
        stats: present(&["time.wall_minutes", "time.active_minutes"]),
        entries: vec![],
        items: vec![],
    });

    let models: Vec<&Stat> = stats
        .iter()
        .filter(|s| s.id.starts_with("model."))
        .collect();
    out.push(Check {
        id: "models".into(),
        title: "Models".into(),
        statement: if models.is_empty() {
            "No model was recorded.".into()
        } else {
            format!(
                "Model{} used: {}.",
                plural(models.len() as i64),
                models
                    .iter()
                    .map(|m| format!("{} ({})", m.label, m.display))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        },
        explain: "Which model answered each request, as the tool logged it, with the number of model calls and their list-price cost.".into(),
        how: "Is this the model you had selected? Several models appear when you switched, or when the tool used a cheaper one for small jobs.".into(),
        stats: models.iter().map(|m| m.id.clone()).collect(),
        entries: models.first().map(|m| m.entries.clone()).unwrap_or_default(),
        items: vec![],
    });

    let edited = int("tools.files_edited");
    let edited_files: Vec<&FileTouch> = files.iter().filter(|f| f.edits > 0).collect();
    let matching_only = files.iter().filter(|f| f.edits == 0).count();
    out.push(Check {
        id: "files".into(),
        title: "Files changed".into(),
        statement: {
            let mut text = if edited == 1 {
                "The agent edited 1 file.".to_string()
            } else {
                format!("The agent edited {edited} files.")
            };
            if edited_files.len() as i64 != edited && !edited_files.is_empty() {
                text.push_str(&format!(
                    " ⚠ vca's own counts disagree: {edited} by edit tool calls, {} in its edit records (listed). If the list is right, mark ✗ and say so.",
                    edited_files.len()
                ));
            }
            text
        },
        explain: "Distinct files the agent (or its subagents) wrote or edited with its edit tools, listed with the lines it added and removed.".into(),
        how: "Do these match the files you remember the agent changing? Press “Show me” to see the edits.".into(),
        stats: present(&["tools.files_edited", "tools.edit"]),
        entries: entries("tools.edit"),
        items: edited_files
            .iter()
            .take(20)
            .map(|f| {
                format!(
                    "{}  +{} −{}",
                    f.rel_path.as_deref().unwrap_or(&f.path),
                    f.session_added,
                    f.session_removed
                )
            })
            .chain((edited_files.len() > 20).then(|| format!("… and {} more", edited_files.len() - 20)))
            .chain((matching_only > 0).then(|| {
                format!(
                    "(+ {matching_only} file{} seen only when matching commits; see All numbers → Files changed)",
                    plural(matching_only as i64)
                )
            }))
            .collect(),
    });

    let runs = int("checks.runs");
    let failures = int("checks.failures");
    out.push(Check {
        id: "tests".into(),
        title: "Tests and builds".into(),
        statement: format!(
            "{runs} test, build or lint command{} ran; {failures} failed.",
            plural(runs)
        ),
        explain: "Commands vca recognises as checks (cargo test, pytest, npm test, tsc, eslint, go build …). A failure is a non-zero exit code.".into(),
        how: "Press “Show me”: are these really tests or builds, and did the failed ones really fail?".into(),
        stats: present(&["checks.runs", "checks.failures"]),
        entries: entries("checks.runs"),
        items: vec![],
    });

    let failed = int("tools.failed");
    out.push(Check {
        id: "tool_errors".into(),
        title: "Failed tool calls".into(),
        statement: format!("{failed} tool call{} failed.", plural(failed)),
        explain: "Tool calls that returned an error, were rejected, or ended with a non-zero exit code (any command, not only tests).".into(),
        how: "Press “Show me” and look over a few: are they real errors?".into(),
        stats: present(&["tools.failed"]),
        entries: entries("tools.failed"),
        items: vec![],
    });

    let subagents = int("shape.subagents");
    out.push(Check {
        id: "subagents".into(),
        title: "Subagents".into(),
        statement: format!("The agent started {subagents} subagent{}.", plural(subagents)),
        explain: "Helper agents the main agent launched for side jobs (exploring, reviewing). Their cost is included in this session.".into(),
        how: "Press “Show me”: subagent entries are indented and tagged “sub”.".into(),
        stats: present(&["shape.subagents", "cost.subagent_usd"]),
        entries: entries("shape.subagents"),
        items: vec![],
    });

    let compactions = int("shape.compactions");
    out.push(Check {
        id: "compactions".into(),
        title: "Compactions".into(),
        statement: format!(
            "The conversation was compacted {compactions} time{}.",
            plural(compactions)
        ),
        explain: "When the context fills up, the tool replaces the history with a summary to make room. It shows as “compacting conversation” or similar.".into(),
        how: "Press “Show me” to see where each one happened.".into(),
        stats: present(&["shape.compactions"]),
        entries: entries("shape.compactions"),
        items: vec![],
    });

    let commits = int("delivery.commits");
    out.push(Check {
        id: "commits".into(),
        title: "Commits".into(),
        statement: format!(
            "{commits} commit{} contain lines written in this session.",
            plural(commits)
        ),
        explain: "vca matches the lines the agent wrote against the lines in this repository's commits. A commit counts when some of its lines match, and only if it was made after the session started.".into(),
        how: "Do you recognise these commits as coming out of this session? Missing ones, or ones from other work, are worth a ✗.".into(),
        stats: present(&["delivery.commits", "delivery.ai_lines"]),
        entries: vec![],
        items: detail
            .commits
            .iter()
            .take(25)
            .map(|c| {
                format!(
                    "{} {}  {}",
                    short_id(&c.sha),
                    c.commit_time.get(..10).unwrap_or(&c.commit_time),
                    c.subject
                )
            })
            .chain((detail.commits.len() > 25).then(|| format!("… and {} more", detail.commits.len() - 25)))
            .collect(),
    });

    let mut task_stats = present(&["tasks.count", "tasks.completed"]);
    let mut task_items = Vec::new();
    for task in &detail.tasks {
        let prefix = format!("task.{}.", task.task_index);
        for suffix in ["intent", "completed", "completion_source", "outcome"] {
            let id = format!("{prefix}{suffix}");
            if by_id.contains_key(id.as_str()) {
                task_stats.push(id);
            }
        }
        let value = |suffix: &str| {
            by_id
                .get(format!("{prefix}{suffix}").as_str())
                .map(|s| s.display.clone())
                .unwrap_or_default()
        };
        let finished = match value("completed").as_str() {
            "true" => format!(
                "Finished: yes, {}.",
                finished_by(&value("completion_source"))
            ),
            "false" => "Finished: not that vca could tell.".to_string(),
            _ => String::new(),
        };
        task_items.push(format!(
            "T{}: Kind of work: {}. {} Outcome: {}.",
            task.task_index,
            plain(&value("intent")),
            finished,
            plain(&value("outcome"))
        ));
    }
    out.push(Check {
        id: "outcome".into(),
        title: "Kind of work and outcome".into(),
        statement: match task_items.as_slice() {
            [only] => only.split_once(": ").map_or(only.clone(), |(_, rest)| rest.to_string()),
            _ => format!("{} tasks, {} finished.", int("tasks.count"), int("tasks.completed")),
        },
        explain: "A guess from the conversation and git: the kind of work (bug fix, feature, review …) and whether it finished. Finished means a commit or a passing check came after the work.".into(),
        how: "Does the kind of work fit? Did it actually get done?".into(),
        stats: task_stats,
        entries: vec![],
        items: if task_items.len() > 1 { task_items } else { vec![] },
    });

    let cost = num("cost.total_usd");
    let tokens = int("tokens.total");
    let cache = int("tokens.cache_read");
    out.push(Check {
        id: "cost".into(),
        title: "Cost".into(),
        statement: format!(
            "At API list prices this is about {} for {} tokens over {} model calls ({}% read from cache).",
            cost.map_or("—".to_string(), |c| format!("${c:.2}")),
            compact(tokens),
            int("calls.model_calls"),
            if tokens > 0 { cache * 100 / tokens } else { 0 }
        ),
        explain: "Tokens come from the tool's own logs; the price is what the API would charge for them (LiteLLM's public price list), not what your subscription costs.".into(),
        how: "Only if you can compare with the tool's usage page. Otherwise skip it; it is recorded either way.".into(),
        stats: present(&["cost.total_usd", "tokens.total", "calls.model_calls"]),
        entries: vec![],
        items: vec![],
    });

    // Drop checks for numbers this session does not have (e.g. no tasks table).
    out.retain(|check| !check.stats.is_empty());
    out
}

/// What a number means, in plain words.
pub fn help(id: &str) -> String {
    if let Some(model) = id.strip_prefix("model.") {
        return format!("Model calls answered by {model}, and their list-price cost.");
    }
    if let Some(kind) = id.strip_prefix("context.") {
        return format!(
            "{} — how much of what the model re-read on every call was this kind of content, in tokens and what re-reading it cost.",
            context_kind(kind)
        );
    }
    if let Some(category) = id.strip_prefix("tools.")
        && !matches!(category, "calls" | "failed" | "files_edited")
    {
        return format!("Tool calls in the “{category}” category (all agents).");
    }
    if let Some(rest) = id.strip_prefix("task.")
        && let Some((_, field)) = rest.split_once('.')
    {
        if let Some(habit) = field.strip_prefix("habit.") {
            return crate::insights::habits::HABIT_TEXT
                .iter()
                .find(|(h, _, _)| *h == habit)
                .map(|(_, advice, measured)| format!("Habit: {advice} Measured as: {measured}"))
                .unwrap_or_else(|| format!("Habit {habit}."));
        }
        return task_field(field).to_string();
    }
    match id {
        "cost.total_usd" => "What the session's tokens would cost at API list prices (subagents included).",
        "cost.subagent_usd" => "The part of the cost spent by subagents.",
        "tokens.total" => "All tokens: input, cache reads and writes, output (and reasoning when billed separately).",
        "tokens.input_uncached" => "Input tokens the model read fresh, not from the prompt cache.",
        "tokens.fresh" => "Everything not served from cache: uncached input + cache writes + output.",
        "tokens.cache_read" => "Input tokens served from the prompt cache (cheap re-reads of earlier context).",
        "tokens.cache_write" => "Input tokens written into the prompt cache for later calls.",
        "tokens.output" => "Tokens the model generated.",
        "tokens.reasoning" => "Tokens the model spent thinking (shown by some tools; usually part of output).",
        "tokens.unpriced" => "Tokens whose model has no known price, so they add nothing to the cost.",
        "calls.model_calls" => "How many times the model was called (one per response, subagents included).",
        "time.wall_minutes" => "Minutes from the first to the last event.",
        "time.active_minutes" => "Minutes of activity, leaving out pauses longer than 30 minutes.",
        "shape.turns" => "Turns: each message you send starts one.",
        "shape.user_messages" => "Messages you typed to the main agent.",
        "shape.subagents" => "Helper agents the main agent launched.",
        "shape.compactions" => "Times the tool replaced the history with a summary to free context.",
        "tools.calls" => "All tool calls (reads, edits, commands …), subagents included.",
        "tools.failed" => "Tool calls that errored, were rejected or exited non-zero.",
        "tools.files_edited" => "Distinct files edited.",
        "checks.runs" => "Test, build and lint commands that ran.",
        "checks.failures" => "Of those, how many failed.",
        "delivery.commits" => "Commits containing lines written in this session (made after it started).",
        "delivery.ai_lines" => "Lines in those commits that match lines the agent wrote.",
        "delivery.rework_relations" => "Times later AI work changed lines from this session, or this session changed lines from earlier AI work.",
        "tasks.count" => "Goals vca split the session into (one without classification).",
        "tasks.completed" => "Tasks judged finished (a commit or passing check followed the work).",
        _ => "",
    }
    .to_string()
}

fn task_field(field: &str) -> &'static str {
    match field {
        "turns" => "Turns in this task.",
        "cost_total_usd" => "List-price cost of the whole task.",
        "cost_to_complete_usd" => "Cost until the task was judged finished.",
        "active_minutes" => "Active minutes until the task was judged finished.",
        "completed" => "Whether the task was judged finished.",
        "completion_source" => {
            "What decided it was finished: a commit, a passing check, a classifier or you."
        }
        "intent" => "Kind of work: bug fix, feature, review, research …",
        "outcome" => "How it ended: verified, unverified, answered, stalled, abandoned …",
        "stage_path" => "The sequence of work stages (explore → plan → implement → verify …).",
        "edit_calls" => "Edit tool calls in this task.",
        "lines_changed" => "Lines added plus removed in this task.",
        "validations" => "Test/build runs in this task, and how many failed.",
        _ => "",
    }
}

fn context_kind(kind: &str) -> &'static str {
    match kind {
        "tool_result" => "Tool output (file contents, command output)",
        "assistant_output" => "The model's own earlier replies",
        "baseline" => "The fixed start of every call (system prompt and tool list)",
        "unexplained" => "Context vca could not attribute to anything it saw",
        "tool_definitions" => "Tool definitions",
        "environment" => "Environment details the tool injects (cwd, date, git status)",
        "skill_listing" => "The list of available skills",
        "reminder" => "System reminders the tool injects",
        "system_prompt" => "The system prompt",
        "instructions" => "Instruction files (CLAUDE.md, AGENTS.md)",
        "user_message" => "Your messages",
        "attachment_other" => "Other attachments",
        "mcp_instructions" => "MCP server instructions",
        "agent_listing" => "The list of available subagents",
        "subagent_result" => "Results returned by subagents",
        _ => "Context",
    }
}

/// How vca decided a task was finished, in words.
fn finished_by(source: &str) -> &'static str {
    match source {
        "commit" => "a commit followed the work",
        "validation_pass" => "a passing test or build followed the work",
        "classifier" | "jev" => "the classifier judged it done",
        "override" | "manual" => "you marked it done",
        _ => "vca saw it finish",
    }
}

fn plural(n: i64) -> &'static str {
    if n == 1 { "" } else { "s" }
}

fn plain(label: &str) -> String {
    label.replace('_', " ")
}

fn compact(n: i64) -> String {
    match n {
        n if n >= 1_000_000 => format!("{:.1}M", n as f64 / 1_000_000.0),
        n if n >= 1_000 => format!("{:.0}k", n as f64 / 1_000.0),
        n => n.to_string(),
    }
}

fn duration(minutes: Option<f64>) -> String {
    match minutes {
        None => "—".into(),
        Some(m) if m < 60.0 => format!("{m:.0} min"),
        Some(m) if m < 48.0 * 60.0 => format!("{:.1} h", m / 60.0),
        Some(m) => format!("{:.1} days", m / 1440.0),
    }
}

fn when(ts: Option<&str>) -> String {
    ts.and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%-d %b %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "—".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_known_number_has_help() {
        for id in [
            "cost.total_usd",
            "tokens.cache_read",
            "time.active_minutes",
            "shape.compactions",
            "tools.exec",
            "tools.failed",
            "checks.failures",
            "context.tool_result",
            "delivery.ai_lines",
            "task.0.intent",
            "task.3.habit.07_tests_can_fail",
            "model.gpt-6.1-sol",
        ] {
            assert!(!help(id).is_empty(), "{id}");
        }
        assert!(help("task.0.habit.07_tests_can_fail").contains("tests"));
    }

    #[test]
    fn durations_read_naturally() {
        assert_eq!(duration(Some(42.0)), "42 min");
        assert_eq!(duration(Some(438.0)), "7.3 h");
        assert_eq!(duration(Some(16240.0)), "11.3 days");
        assert_eq!(compact(143_876_584), "143.9M");
    }
}
