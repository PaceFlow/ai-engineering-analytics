//! Extract requests from known transport envelopes without modifying source messages.
//!
//! Routing instructions ("reply as a comment"), agent role preambles, platform
//! notifications and issue-template boilerplate say how to deliver work, not
//! what the work is. They are stripped so they cannot steer classification;
//! `stripped` records what was removed and `text` is the extracted goal.
use serde::Serialize;
pub const VERSION: &str = "request-2";

/// Stand-in text when an assignment names a task but not its goal.
const ASSIGNED_WITHOUT_GOAL: &str =
    "Assigned external task; substantive goal unavailable in this request.";
/// Stand-in text for platform notifications that carry no request.
const PLATFORM_EVENT: &str = "Platform notification; substantive goal unavailable in this request.";

#[derive(Debug, Serialize)]
pub struct Request {
    pub text: String,
    pub speaker: &'static str,
    pub extraction: &'static str,
    /// Wrappers removed to reach `text`.
    pub stripped: Vec<&'static str>,
}

impl Request {
    /// The message names no substantive goal (bare assignment or notification).
    pub fn goal_unavailable(&self) -> bool {
        matches!(self.extraction, "uncertain" | "platform_event")
    }
}

pub fn normalize(raw: &str) -> Request {
    let trimmed = raw.trim();
    // Already-normalized stand-ins (task titles store normalized text).
    if trimmed == ASSIGNED_WITHOUT_GOAL {
        return unavailable("unknown", "uncertain", vec![]);
    }
    if trimmed == PLATFORM_EVENT {
        return unavailable("platform", "platform_event", vec![]);
    }
    let mut stripped = Vec::new();
    let mut text = strip_blocks(trimmed, &mut stripped);
    if let Some(rest) = strip_leading_tag(&text, "timestamp") {
        stripped.push("timestamp");
        text = rest;
    }
    for tag in [
        "task-notification",
        "system_notification",
        "goal_continuation",
        "goal_command_control",
    ] {
        if text.starts_with(&format!("<{tag}>")) {
            stripped.push("platform_notification");
            return unavailable("platform", "platform_event", stripped);
        }
    }
    if text.starts_with("<heartbeat>") {
        stripped.push("heartbeat");
        let Some(inner) = tag_body(&text, "instructions") else {
            return unavailable("platform", "platform_event", stripped);
        };
        return finish(inner, "platform", "recognized_heartbeat", stripped);
    }
    if text.starts_with("You are running as a local coding agent for a Multica workspace.") {
        stripped.push("multica_preamble");
        return multica(&text, stripped);
    }
    if let Some((_, request)) = text.split_once("## My request:") {
        stripped.push("pasted_files");
        return finish(request, "human", "recognized_paste", stripped);
    }
    let (body, speaker) = match strip_role_preamble(&text) {
        Some(body) => {
            stripped.push("agent_preamble");
            (body, "agent")
        }
        None => (text.as_str(), "unknown"),
    };
    finish(body, speaker, "unchanged", stripped)
}

fn unavailable(
    speaker: &'static str,
    extraction: &'static str,
    stripped: Vec<&'static str>,
) -> Request {
    let text = if extraction == "platform_event" {
        PLATFORM_EVENT
    } else {
        ASSIGNED_WITHOUT_GOAL
    };
    Request {
        text: text.into(),
        speaker,
        extraction,
        stripped,
    }
}

/// Remove routing instructions and template boilerplate from an extracted body.
fn finish(
    body: &str,
    speaker: &'static str,
    extraction: &'static str,
    mut stripped: Vec<&'static str>,
) -> Request {
    let mut lines = Vec::new();
    let mut routed = false;
    for line in body.lines() {
        let kept = strip_routing(line);
        routed |= kept.len() != line.trim_end().len();
        if !(kept.trim().is_empty() && !line.trim().is_empty()) {
            lines.push(kept);
        }
    }
    if routed {
        stripped.push("routing_instruction");
    }
    let text = drop_empty_sections(&lines.join("\n"), &mut stripped);
    let text = text.trim();
    if text.is_empty() {
        return unavailable(speaker, "uncertain", stripped);
    }
    let extraction = if extraction == "unchanged" && !stripped.is_empty() {
        "stripped_wrapper"
    } else {
        extraction
    };
    Request {
        text: text.to_owned(),
        speaker,
        extraction,
        stripped,
    }
}

fn multica(raw: &str, stripped: Vec<&'static str>) -> Request {
    let speaker = if raw.contains("Another agent (") {
        "agent"
    } else if raw.contains("A user just left") {
        "human"
    } else if raw.contains("The platform just left") || raw.contains("[WAKEUP]") {
        "platform"
    } else {
        "unknown"
    };
    let quoted: Vec<_> = raw
        .lines()
        .filter_map(|line| line.strip_prefix("> ").or((line == ">").then_some("")))
        .collect();
    if !quoted.is_empty() {
        return finish(&quoted.join("\n"), speaker, "recognized_comment", stripped);
    }
    if let Some((_, tail)) = raw.split_once("Instruction:") {
        let text = tail.split("## ").next().unwrap_or(tail);
        return finish(text, speaker, "recognized_wakeup", stripped);
    }
    // An assignment containing only an issue id supplies no goal. Do not classify
    // its comment-posting instructions as documentation or invent the issue body.
    unavailable(speaker, "uncertain", stripped)
}

/// Injected context blocks that are never part of the request.
fn strip_blocks(text: &str, stripped: &mut Vec<&'static str>) -> String {
    let mut text = text.to_owned();
    for (open, close, name) in [
        ("<system-reminder>", "</system-reminder>", "system_reminder"),
        (
            "<environment_context>",
            "</environment_context>",
            "environment_context",
        ),
        (
            "<user_instructions>",
            "</user_instructions>",
            "user_instructions",
        ),
        ("<!--", "-->", "template_comment"),
    ] {
        while let Some(start) = text.find(open) {
            let Some(end) = text[start..].find(close) else {
                break;
            };
            text.replace_range(start..start + end + close.len(), "");
            if !stripped.contains(&name) {
                stripped.push(name);
            }
        }
    }
    text.trim().to_owned()
}

fn strip_leading_tag(text: &str, tag: &str) -> Option<String> {
    let rest = text.strip_prefix(&format!("<{tag}>"))?;
    let (_, after) = rest.split_once(&format!("</{tag}>"))?;
    Some(after.trim().to_owned())
}

fn tag_body<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let (_, rest) = text.split_once(&format!("<{tag}>"))?;
    Some(rest.split(&format!("</{tag}>")).next().unwrap_or(rest))
}

/// "You are a … agent." opening paragraph, when a request follows it.
fn strip_role_preamble(text: &str) -> Option<&str> {
    if !text.starts_with("You are ") {
        return None;
    }
    let (first, rest) = text.split_once("\n\n")?;
    let lower = first.to_ascii_lowercase();
    let role = ["agent", "assistant", "worker", "coordinator", "subagent"]
        .iter()
        .any(|word| lower.contains(word));
    (role && first.chars().count() <= 600 && !rest.trim().is_empty()).then_some(rest)
}

/// Where a reply should be delivered: an issue comment or thread.
const DELIVERY_CUES: [&str; 13] = [
    " as a comment",
    " as an issue comment",
    " in a comment",
    " via a comment",
    " with a comment",
    "comment on the issue",
    " in the issue",
    " on the issue",
    " to the issue",
    " in this issue",
    " in the thread",
    " on the ticket",
    " in the ticket",
];
const DELIVERY_VERBS: [&str; 9] = [
    "reply", "respond", "answer", "post", "report", "publish", "share", "send", "leave",
];
const DELIVERY_LEADS: [&str; 9] = [
    "please ",
    "when done, ",
    "when done ",
    "when finished, ",
    "once done, ",
    "once finished, ",
    "then ",
    "finally, ",
    "and ",
];

/// One sentence that only says how to deliver the answer.
fn is_routing_sentence(sentence: &str) -> bool {
    let mut lower = sentence
        .trim()
        .trim_start_matches(['-', '*', ' '])
        .trim_end_matches(['.', '!', ' '])
        .to_ascii_lowercase();
    if lower.is_empty() || lower.len() > 200 {
        return false;
    }
    if lower.contains("multica issue comment") || lower.contains("multica comment") {
        return true;
    }
    while let Some(lead) = DELIVERY_LEADS.iter().find(|lead| lower.starts_with(**lead)) {
        lower.drain(..lead.len());
    }
    DELIVERY_VERBS.iter().any(|verb| lower.starts_with(verb))
        && DELIVERY_CUES.iter().any(|cue| lower.contains(cue))
}

/// Drop routing sentences from one line; other sentences stay verbatim.
fn strip_routing(line: &str) -> String {
    let mut kept = String::new();
    let mut rest = line;
    while !rest.is_empty() {
        let end = rest
            .find(". ")
            .or_else(|| rest.find("! "))
            .map_or(rest.len(), |i| i + 2);
        let (sentence, tail) = rest.split_at(end);
        if !is_routing_sentence(sentence) {
            kept.push_str(sentence);
        }
        rest = tail;
    }
    kept.trim_end().to_owned()
}

/// Issue-template headings left without content ("_No response_", "N/A").
fn drop_empty_sections(text: &str, stripped: &mut Vec<&'static str>) -> String {
    let placeholder = |line: &str| {
        let line = line.trim().to_ascii_lowercase();
        line.is_empty() || matches!(line.as_str(), "_no response_" | "n/a" | "none" | "-")
    };
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        if line.trim_start().starts_with("##") {
            let next = lines[i + 1..]
                .iter()
                .position(|l| l.trim_start().starts_with('#'))
                .map_or(lines.len(), |p| i + 1 + p);
            if lines[i + 1..next].iter().all(|l| placeholder(l)) {
                if !stripped.contains(&"empty_template_section") {
                    stripped.push("empty_template_section");
                }
                i = next;
                continue;
            }
        }
        out.push(line);
        i += 1;
    }
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_comment_and_preserves_speaker() {
        let raw = "You are running as a local coding agent for a Multica workspace.\n[NEW COMMENT] Another agent (Ops) just left a new comment.\n> Tune the cache limit and verify service health.\n\nReply as a comment.";
        let r = normalize(raw);
        assert_eq!(r.text, "Tune the cache limit and verify service health.");
        assert_eq!(r.speaker, "agent");
        assert_eq!(
            normalize("Fix the bug; leave this comment intact").text,
            "Fix the bug; leave this comment intact"
        );
    }
    #[test]
    fn assignment_does_not_invent_goal() {
        let r = normalize(
            "You are running as a local coding agent for a Multica workspace.\nYour assigned issue ID is: abc\nReply as a comment.",
        );
        assert_eq!(r.extraction, "uncertain");
        assert!(r.goal_unavailable());
        assert!(!r.text.contains("comment"));
        // Normalized titles are normalized again by later passes.
        assert!(normalize(&r.text).goal_unavailable());
    }
    #[test]
    fn strips_routing_sentences_but_keeps_code_comment_asks() {
        let r = normalize(
            "The nightly backup job fails with OOM; restart it and raise the memory limit. Reply as a comment when done.",
        );
        assert_eq!(
            r.text,
            "The nightly backup job fails with OOM; restart it and raise the memory limit."
        );
        assert_eq!(r.stripped, vec!["routing_instruction"]);
        assert_eq!(r.extraction, "stripped_wrapper");
        let r = normalize("Please post your findings on the issue.\nInvestigate the 502s.");
        assert_eq!(r.text, "Investigate the 502s.");
        for ask in [
            "Leave a comment explaining the regex",
            "Add a comment above the parser",
            "Write docs and post them as a comment",
        ] {
            assert_eq!(normalize(ask).text, ask);
        }
        assert!(normalize("Reply as a comment.").goal_unavailable());
    }
    #[test]
    fn strips_agent_and_platform_wrappers() {
        let r = normalize(
            "You are a recovery WORKER in a fleet. Read AGENTS.md first.\n\nRecover the packet parser at 0x0043305C.",
        );
        assert_eq!(r.text, "Recover the packet parser at 0x0043305C.");
        assert_eq!(r.speaker, "agent");
        let r = normalize(
            "<heartbeat>\n<automation_id>x</automation_id>\n<instructions>\nKeep the harvest jobs healthy.\n</instructions>\n</heartbeat>",
        );
        assert_eq!(r.text, "Keep the harvest jobs healthy.");
        assert_eq!(r.extraction, "recognized_heartbeat");
        let r = normalize(
            "<timestamp>Sunday</timestamp>\n<system_notification>\nThe following task has finished.\n</system_notification>",
        );
        assert!(r.goal_unavailable());
        assert_eq!(r.speaker, "platform");
        let r = normalize(
            "# Files pasted by the user:\n\n## \"comments\": /tmp/p.txt\n\n## My request:\nResearch small models",
        );
        assert_eq!(r.text, "Research small models");
        // A plain "You are" sentence without a role is not a preamble.
        let plain = "You are right.\n\nNow add the export button.";
        assert_eq!(normalize(plain).text, plain);
    }
    #[test]
    fn strips_issue_template_boilerplate() {
        let r = normalize(
            "## Summary\nDeploy fails after the config change\n<!-- Describe docs or comments here -->\n## Screenshots\n_No response_\n## Steps\n1. run deploy",
        );
        assert_eq!(
            r.text,
            "## Summary\nDeploy fails after the config change\n\n## Steps\n1. run deploy"
        );
        assert_eq!(
            r.stripped,
            vec!["template_comment", "empty_template_section"]
        );
    }
}
