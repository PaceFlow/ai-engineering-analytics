//! What is sent to the classifier: compact JSON states built from local
//! facts, at a user-chosen privacy level, redacted and size-bounded.

use anyhow::Result;
use regex::Regex;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::sync::OnceLock;

use crate::insights::structure::parse_ts;

/// Target size for one request; the provider's prompt limit is 32k tokens.
pub const TARGET_TOKENS: usize = 6_000;
pub const HARD_CAP_TOKENS: usize = 28_000;
const CHARS_PER_TOKEN: f64 = 3.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    EvidenceOnly,
    UserText,
    Conversation,
    Full,
}

impl Level {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "evidence_only" | "evidence" => Self::EvidenceOnly,
            "user_text" | "user" => Self::UserText,
            "conversation" => Self::Conversation,
            "full" => Self::Full,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceOnly => "evidence_only",
            Self::UserText => "user_text",
            Self::Conversation => "conversation",
            Self::Full => "full",
        }
    }

    pub fn lower(self) -> Option<Self> {
        match self {
            Self::Full => Some(Self::Conversation),
            Self::Conversation => Some(Self::UserText),
            Self::UserText => Some(Self::EvidenceOnly),
            Self::EvidenceOnly => None,
        }
    }
}

pub fn estimate_tokens(value: &Value) -> usize {
    (value.to_string().chars().count() as f64 / CHARS_PER_TOKEN).ceil() as usize
}

// ── Redaction ────────────────────────────────────────────────────────────

fn redaction_patterns() -> &'static [(Regex, &'static str)] {
    static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            (r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----", "[secret]"),
            (r"\bAKIA[0-9A-Z]{16}\b", "[secret]"),
            (r"\b(sk|rk|pk)-[A-Za-z0-9_\-]{20,}", "[secret]"),
            (r"\bsk-ant-[A-Za-z0-9_\-]{20,}", "[secret]"),
            (r"\bsk-or-[A-Za-z0-9_\-]{20,}", "[secret]"),
            (r"\bgh[pousr]_[A-Za-z0-9]{30,}\b", "[secret]"),
            (r"\bgithub_pat_[A-Za-z0-9_]{30,}\b", "[secret]"),
            (r"\bxox[baprs]-[A-Za-z0-9\-]{10,}", "[secret]"),
            (r"\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}", "[secret]"),
            (
                r#"(?i)\b(password|passwd|secret|api[_\-]?key|access[_\-]?token|auth[_\-]?token|private[_\-]?key)\b\s*[:=]\s*["']?[^\s"']{6,}"#,
                "$1=[secret]",
            ),
            (r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", "[email]"),
            (r"/(home|Users)/[^/\s]+", "~"),
            (r"(?i)C:\\Users\\[^\\\s]+", "~"),
        ]
        .into_iter()
        .map(|(pattern, replacement)| (Regex::new(pattern).expect("valid redaction regex"), replacement))
        .collect()
    })
}

/// Redact secrets, emails and home paths; returns the text and the number
/// of replacements made.
pub fn redact(text: &str) -> (String, usize) {
    let mut out = text.to_string();
    let mut count = 0;
    for (regex, replacement) in redaction_patterns() {
        let matches = regex.find_iter(&out).count();
        if matches > 0 {
            count += matches;
            out = regex.replace_all(&out, *replacement).into_owned();
        }
    }
    (out, count)
}

/// Replace fenced code blocks with a short marker.
pub fn strip_code_blocks(text: &str) -> String {
    static FENCE: OnceLock<Regex> = OnceLock::new();
    let fence = FENCE.get_or_init(|| {
        Regex::new(r"(?s)```([A-Za-z0-9_+\-]*)\n(.*?)```").expect("valid fence regex")
    });
    fence
        .replace_all(text, |caps: &regex::Captures<'_>| {
            let lang = caps
                .get(1)
                .map(|m| m.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("code");
            let lines = caps.get(2).map(|m| m.as_str().lines().count()).unwrap_or(0);
            format!("[code: {lines} lines {lang}]")
        })
        .into_owned()
}

/// Keep the head and tail of long text.
pub fn head_tail(text: &str, max: usize) -> String {
    let count = text.chars().count();
    if count <= max {
        return text.to_string();
    }
    let head = max * 2 / 3;
    let tail = max - head;
    let start: String = text.chars().take(head).collect();
    let end: String = text.chars().skip(count - tail).collect();
    format!("{start} […{} chars omitted…] {end}", count - max)
}

// ── Facts for one session ────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct TurnFacts {
    pub index: i64,
    pub ts: Option<i64>,
    pub user: String,
    pub assistant: String,
    pub tools: Vec<ToolFact>,
    pub idle_minutes_before: Option<f64>,
    pub events_before: Vec<String>,
    pub runtime_outcomes: Vec<String>,
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ToolFact {
    pub name: String,
    pub category: String,
    pub validation_kind: Option<String>,
    pub status: String,
    pub exit_code: Option<i64>,
    pub signature: Option<String>,
    pub target: Option<String>,
}

impl TurnFacts {
    pub fn tool_summary(&self, level: Level) -> String {
        let mut counts: Vec<(String, usize)> = Vec::new();
        let mut validations: Vec<String> = Vec::new();
        for tool in &self.tools {
            if let Some(kind) = tool.validation_kind.as_deref() {
                let failed = tool.exit_code.is_some_and(|code| code != 0) || tool.status == "error";
                let mut text = format!("{kind}{}", if failed { "✗" } else { "✓" });
                if failed
                    && level >= Level::Full
                    && let Some(signature) = &tool.signature
                {
                    text.push_str(&format!("({})", head_tail(signature, 80)));
                }
                validations.push(text);
                continue;
            }
            match counts.iter_mut().find(|(name, _)| *name == tool.category) {
                Some((_, count)) => *count += 1,
                None => counts.push((tool.category.clone(), 1)),
            }
        }
        let mut parts: Vec<String> = counts
            .iter()
            .map(|(name, count)| format!("{name}×{count}"))
            .collect();
        parts.extend(validations);
        parts.join(" ")
    }
}

/// Load turns of a session in the same shape the structure pass uses.
pub fn load_turns(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<TurnFacts>> {
    let mut turns: Vec<TurnFacts> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT message_ts, role, content, is_compaction_summary FROM fact_session_message
         WHERE provider = ?1 AND session_id = ?2 ORDER BY message_index",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)? != 0,
        ))
    })?;
    for row in rows {
        let (ts, role, content, compaction) = row?;
        if role == "user" && !compaction {
            turns.push(TurnFacts {
                index: turns.len() as i64,
                ts: ts.as_deref().and_then(parse_ts),
                user: content,
                ..TurnFacts::default()
            });
        } else if role == "assistant"
            && !compaction
            && let Some(turn) = turns.last_mut()
        {
            if !turn.assistant.is_empty() {
                turn.assistant.push('\n');
            }
            turn.assistant.push_str(&content);
        }
    }
    if turns.is_empty() {
        return Ok(turns);
    }
    let turn_for = |turns: &[TurnFacts], ts: Option<i64>| -> usize {
        let Some(ts) = ts else {
            return turns.len() - 1;
        };
        let mut current = 0;
        for (index, turn) in turns.iter().enumerate() {
            if turn.ts.is_some_and(|start| start <= ts) {
                current = index;
            }
        }
        current
    };

    let mut stmt = conn.prepare(
        "SELECT ts, tool_name, tool_category, validation_kind, status, exit_code, error_signature, target
         FROM fact_session_tool_call WHERE provider = ?1 AND session_id = ?2 ORDER BY call_index",
    )?;
    let tools = stmt
        .query_map(params![provider, session_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                ToolFact {
                    name: row.get(1)?,
                    category: row.get(2)?,
                    validation_kind: row.get(3)?,
                    status: row.get(4)?,
                    exit_code: row.get(5)?,
                    signature: row.get(6)?,
                    target: row.get(7)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (ts, tool) in tools {
        let index = turn_for(&turns, ts.as_deref().and_then(parse_ts));
        if tool.category == "edit"
            && let Some(target) = tool.target.clone()
            && !turns[index].files.contains(&target)
        {
            turns[index].files.push(target);
        }
        turns[index].tools.push(tool);
    }

    let mut stmt = conn.prepare(
        "SELECT ts, kind, detail FROM fact_session_event WHERE provider = ?1 AND session_id = ?2 ORDER BY event_index",
    )?;
    let events = stmt
        .query_map(params![provider, session_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (ts, kind, detail) in events {
        let Some(ts) = ts.as_deref().and_then(parse_ts) else {
            continue;
        };
        if kind == "runtime_outcome" {
            if let Some(value) = detail.filter(|s| {
                ["succeeded", "failed", "cancelled", "interrupted"].contains(&s.as_str())
            }) && let Some(turn) = turns
                .iter_mut()
                .rev()
                .find(|t| t.ts.is_some_and(|start| start <= ts))
            {
                turn.runtime_outcomes.push(value);
            }
            continue;
        }
        if let Some(next) = turns
            .iter_mut()
            .find(|turn| turn.ts.is_some_and(|start| start >= ts))
        {
            next.events_before.push(kind);
        }
    }

    let mut commits_stmt = conn.prepare(
        "SELECT c.commit_time FROM fact_commit_session_match sm
         JOIN fact_commit c ON c.repo_root = sm.repo_root AND c.commit_sha = sm.commit_sha
         WHERE sm.provider = ?1 AND sm.session_id = ?2",
    )?;
    let commit_times: Vec<i64> = commits_stmt
        .query_map(params![provider, session_id], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .filter_map(|ts| parse_ts(&ts))
        .collect();

    for index in 1..turns.len() {
        let previous_end = turns[index - 1].ts;
        if let (Some(previous), Some(current)) = (previous_end, turns[index].ts) {
            turns[index].idle_minutes_before = Some((current - previous) as f64 / 60_000.0);
            if commit_times
                .iter()
                .any(|commit| *commit > previous && *commit <= current)
            {
                turns[index].events_before.push("commit".into());
            }
        }
    }
    Ok(turns)
}

/// Redacted, level-filtered text of one turn's user message.
fn user_text(turn: &TurnFacts, max: usize, redactions: &mut usize) -> String {
    let (text, count) = redact(&head_tail(&turn.user, max));
    *redactions += count;
    text
}

fn assistant_text(
    turn: &TurnFacts,
    level: Level,
    max: usize,
    redactions: &mut usize,
) -> Option<String> {
    if level < Level::Conversation || turn.assistant.trim().is_empty() {
        return None;
    }
    let stripped = strip_code_blocks(&turn.assistant);
    let (text, count) = redact(&head_tail(&stripped, max));
    *redactions += count;
    Some(text)
}

pub struct BuiltState {
    pub state: Value,
    pub level: Level,
    pub redactions: usize,
    pub est_tokens: usize,
}

/// State for the task label and completion questions.
pub fn task_state(
    tool: &str,
    turns: &[TurnFacts],
    task_turns: &[i64],
    commits_after: i64,
    subagents: i64,
    level: Level,
) -> BuiltState {
    let selected: Vec<&TurnFacts> = turns
        .iter()
        .filter(|turn| task_turns.contains(&turn.index))
        .collect();
    let mut redactions = 0usize;
    let mut validations: Vec<Value> = Vec::new();
    let mut files: Vec<String> = Vec::new();
    let mut compactions = 0;
    for turn in &selected {
        for tool in &turn.tools {
            if let Some(kind) = &tool.validation_kind {
                let failed = tool.exit_code.is_some_and(|code| code != 0) || tool.status == "error";
                let mut entry =
                    json!({"kind": kind, "result": if failed { "fail" } else { "pass" }});
                if failed && let Some(signature) = &tool.signature {
                    entry["sig"] = Value::String(redact(&head_tail(signature, 80)).0);
                }
                validations.push(entry);
            }
        }
        for file in &turn.files {
            if !files.contains(file) {
                files.push(file.clone());
            }
        }
        compactions += turn
            .events_before
            .iter()
            .filter(|event| *event == "compaction")
            .count();
    }
    let duration = match (
        selected.first().and_then(|t| t.ts),
        selected.last().and_then(|t| t.ts),
    ) {
        (Some(start), Some(end)) => (end - start) / 60_000,
        _ => 0,
    };
    let evidence = json!({
        "files_edited": files.len(),
        "validations": validations,
        "commits_after_task": commits_after,
        "subagents": subagents,
        "compactions": compactions,
        "runtime_outcomes": selected.iter().flat_map(|t|t.runtime_outcomes.iter()).collect::<Vec<_>>(),
        "runtime_outcome_is_goal_completion": false,
    });

    let build = |level: Level,
                 keep_head: usize,
                 keep_tail: usize,
                 user_max: usize,
                 assistant_max: usize,
                 redactions: &mut usize|
     -> Value {
        let mut transcript: Vec<Value> = Vec::new();
        if level > Level::EvidenceOnly {
            let count = selected.len();
            let mut omitted: Vec<&TurnFacts> = Vec::new();
            for (position, turn) in selected.iter().enumerate() {
                let keep = position < keep_head || position + keep_tail >= count;
                if !keep {
                    omitted.push(turn);
                    continue;
                }
                if !omitted.is_empty() {
                    transcript.push(json!({"omitted": omitted_summary(&omitted)}));
                    omitted.clear();
                }
                let mut entry =
                    json!({"turn": turn.index, "user": user_text(turn, user_max, redactions)});
                if let Some(text) = assistant_text(turn, level, assistant_max, redactions) {
                    entry["assistant"] = Value::String(text);
                }
                let tools = turn.tool_summary(level);
                if !tools.is_empty() {
                    entry["tools"] = Value::String(tools);
                }
                transcript.push(entry);
            }
        }
        json!({
            "unit": "task",
            "tool": tool,
            "duration_minutes": duration,
            "turns": selected.len(),
            "evidence": evidence,
            "transcript": transcript,
        })
    };

    let mut level = level;
    let attempts = [
        (usize::MAX, 0, 1500, 600),
        (3, 5, 1500, 600),
        (3, 5, 800, 300),
        (2, 3, 500, 200),
    ];
    loop {
        for (head, tail, user_max, assistant_max) in attempts {
            let mut count = 0;
            let state = build(level, head, tail, user_max, assistant_max, &mut count);
            let est = estimate_tokens(&state);
            if est <= TARGET_TOKENS || (head, tail) == (2, 3) && est <= HARD_CAP_TOKENS {
                redactions += count;
                return BuiltState {
                    state,
                    level,
                    redactions,
                    est_tokens: est,
                };
            }
        }
        match level.lower() {
            Some(lower) => level = lower,
            None => {
                let state = build(Level::EvidenceOnly, 0, 0, 0, 0, &mut redactions);
                let est = estimate_tokens(&state);
                return BuiltState {
                    state,
                    level: Level::EvidenceOnly,
                    redactions,
                    est_tokens: est,
                };
            }
        }
    }
}

fn omitted_summary(turns: &[&TurnFacts]) -> String {
    let edits: usize = turns
        .iter()
        .map(|t| t.tools.iter().filter(|x| x.category == "edit").count())
        .sum();
    let validations: Vec<&ToolFact> = turns
        .iter()
        .flat_map(|t| t.tools.iter().filter(|x| x.validation_kind.is_some()))
        .collect();
    let failed = validations
        .iter()
        .filter(|t| t.exit_code.is_some_and(|code| code != 0) || t.status == "error")
        .count();
    format!(
        "turns {}-{}: {} edits, {} validation runs ({} failed)",
        turns.first().map(|t| t.index).unwrap_or(0),
        turns.last().map(|t| t.index).unwrap_or(0),
        edits,
        validations.len(),
        failed
    )
}

/// State for one exchange: the previous assistant output and the reply.
pub fn exchange_state(turns: &[TurnFacts], turn_index: usize, level: Level) -> BuiltState {
    let mut redactions = 0;
    let previous = &turns[turn_index - 1];
    let current = &turns[turn_index];
    let mut state = json!({
        "unit": "exchange",
        "previous_tools": previous.tool_summary(level),
        "user_message": if level > Level::EvidenceOnly { user_text(current, 1500, &mut redactions) } else { String::new() },
    });
    if let Some(text) = assistant_text(previous, level, 800, &mut redactions) {
        state["assistant_previous"] = Value::String(text);
    }
    let est = estimate_tokens(&state);
    BuiltState {
        state,
        level,
        redactions,
        est_tokens: est,
    }
}

/// State for the task assignment of one user turn.
pub fn assignment_state(turns: &[TurnFacts], turn_index: usize, level: Level) -> BuiltState {
    let mut redactions = 0;
    let current = &turns[turn_index];
    let recent: Vec<Value> = turns[turn_index.saturating_sub(3)..turn_index]
        .iter()
        .map(|turn| {
            let mut entry = json!({
                "turn": turn.index,
                "tools": turn.tool_summary(level),
                "files": turn.files.iter().take(8).map(|f| redact(f).0).collect::<Vec<_>>(),
            });
            if level > Level::EvidenceOnly {
                entry["user"] = Value::String(user_text(turn, 400, &mut redactions));
            }
            if let Some(text) = assistant_text(turn, level, 400, &mut redactions) {
                entry["assistant"] = Value::String(text);
            }
            entry
        })
        .collect();
    let state = json!({
        "unit": "user_turn",
        "new_message": if level > Level::EvidenceOnly { user_text(current, 1500, &mut redactions) } else { String::new() },
        "idle_minutes_before": current.idle_minutes_before.map(|m| m.round()),
        "events_before": current.events_before,
        "recent_turns": recent,
        "current_turn": current_turn_block(current, level, &mut redactions),
    });
    let est = estimate_tokens(&state);
    BuiltState {
        state,
        level,
        redactions,
        est_tokens: est,
    }
}

/// What the agent did in a turn: tool summary, edits, checks and (at
/// `conversation` and above) its reply.
fn current_turn_block(turn: &TurnFacts, level: Level, redactions: &mut usize) -> Value {
    let edits = turn.tools.iter().filter(|t| t.category == "edit").count();
    let checks: Vec<Value> = turn
        .tools
        .iter()
        .filter(|t| t.validation_kind.is_some())
        .map(|t| {
            let failed = t.exit_code.is_some_and(|code| code != 0) || t.status == "error";
            json!({"kind": t.validation_kind, "result": if failed { "fail" } else { "pass" }})
        })
        .collect();
    let mut block = json!({
        "tools": turn.tool_summary(level),
        "files_edited": turn.files.iter().take(8).map(|f| redact(f).0).collect::<Vec<_>>(),
        "edit_calls": edits,
        "checks": checks,
    });
    if let Some(text) = assistant_text(turn, level, 600, redactions) {
        block["assistant"] = Value::String(text);
    }
    block
}

/// One-line description of a turn for the completion choice options.
pub fn turn_option(turn: &TurnFacts, level: Level, redactions: &mut usize) -> String {
    let mut text = format!("Turn {}", turn.index);
    if level > Level::EvidenceOnly {
        text.push_str(&format!(": user '{}'", user_text(turn, 150, redactions)));
    }
    if let Some(assistant) = assistant_text(turn, level, 150, redactions) {
        text.push_str(&format!("; assistant '{assistant}'"));
    }
    let tools = turn.tool_summary(level);
    if !tools.is_empty() {
        text.push_str(&format!("; tools: {tools}"));
    }
    if turn.events_before.iter().any(|event| event == "commit") {
        text.push_str("; a commit was made before this turn");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_secrets_emails_and_home_paths() {
        let (text, count) = redact(
            "key sk-ant-REDACTED-FIXTURE-KEY api_key=hunter22222 mail me@example.com at /home/tadas/repo",
        );
        assert!(!text.contains("sk-ant-REDACTED-FIXTURE-KEY"));
        assert!(!text.contains("hunter22222"));
        assert!(text.contains("[email]"));
        assert!(text.contains("~/repo"));
        assert!(count >= 4);
    }

    #[test]
    fn strips_code_blocks() {
        let text = "Here:\n```rust\nfn a() {}\nfn b() {}\n```\nDone";
        assert_eq!(strip_code_blocks(text), "Here:\n[code: 2 lines rust]\nDone");
    }

    #[test]
    fn long_tasks_are_truncated_under_budget() {
        let turns: Vec<TurnFacts> = (0..60)
            .map(|index| TurnFacts {
                index,
                ts: Some(index * 60_000),
                user: "please fix the failing parser test again ".repeat(80),
                assistant: "I changed the parser and reran the tests ".repeat(80),
                ..TurnFacts::default()
            })
            .collect();
        let all: Vec<i64> = (0..60).collect();
        let built = task_state("claude", &turns, &all, 0, 0, Level::Conversation);
        assert!(built.est_tokens <= HARD_CAP_TOKENS, "{}", built.est_tokens);
        let transcript = built.state["transcript"].as_array().unwrap();
        assert!(
            transcript
                .iter()
                .any(|entry| entry.get("omitted").is_some())
        );
    }

    #[test]
    fn evidence_only_sends_no_text() {
        let turns = vec![TurnFacts {
            index: 0,
            user: "secret plan".into(),
            assistant: "reply".into(),
            ..TurnFacts::default()
        }];
        let built = task_state("codex", &turns, &[0], 0, 0, Level::EvidenceOnly);
        assert!(!built.state.to_string().contains("secret plan"));
    }
}
