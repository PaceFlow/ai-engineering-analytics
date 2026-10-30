//! Extract requests from known transport envelopes without modifying source messages.
use serde::Serialize;
pub const VERSION: &str = "request-1";
#[derive(Debug, Serialize)]
pub struct Request {
    pub text: String,
    pub speaker: &'static str,
    pub extraction: &'static str,
}
pub fn normalize(raw: &str) -> Request {
    let wrapped =
        raw.starts_with("You are running as a local coding agent for a Multica workspace.");
    if !wrapped {
        return Request {
            text: raw.to_owned(),
            speaker: "unknown",
            extraction: "unchanged",
        };
    }
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
        .filter_map(|line| line.strip_prefix("> "))
        .collect();
    if !quoted.is_empty() {
        return Request {
            text: quoted.join("\n"),
            speaker,
            extraction: "recognized_comment",
        };
    }
    if let Some((_, tail)) = raw.split_once("Instruction:") {
        let text = tail.split("## ").next().unwrap_or(tail).trim();
        return Request {
            text: text.to_owned(),
            speaker,
            extraction: "recognized_wakeup",
        };
    }
    // An assignment containing only an issue id supplies no goal. Do not classify
    // its comment-posting instructions as documentation or invent the issue body.
    Request {
        text: "Assigned external task; substantive goal unavailable in this request.".into(),
        speaker,
        extraction: "uncertain",
    }
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
        assert!(!r.text.contains("comment"));
    }
}
