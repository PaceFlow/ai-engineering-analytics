//! The assigned issue as the agent read it.
//!
//! An agent platform assignment can name an issue without stating its goal;
//! the agent then reads the issue itself (`multica issue get <id>`). The
//! title and the head of the description it read are kept, redacted, on that
//! tool call and attached to the bare assignment, so the goal comes from the
//! conversation's own transcript rather than from the platform.
use std::collections::HashMap;

use anyhow::Result;
use rusqlite::{Connection, params};
use serde_json::{Value, json};

/// Description characters kept from the issue read.
const DESCRIPTION_CHARS: usize = 600;
/// Block appended to a bare assignment; `request::normalize` extracts it.
pub const OPEN: &str = "<assigned_issue>";
pub const CLOSE: &str = "</assigned_issue>";

#[derive(Debug, Clone, PartialEq)]
pub struct IssueRead {
    pub id: String,
    pub title: String,
    pub description: String,
}

/// The issue an agent read with `multica issue get`, as stored JSON. Only a
/// whole JSON issue counts: filtered or reformatted output is ignored.
pub fn parse(command: &str, output: &str) -> Option<String> {
    let mut words = command.split_whitespace();
    if (words.next(), words.next(), words.next()) != (Some("multica"), Some("issue"), Some("get")) {
        return None;
    }
    let issue: Value = serde_json::from_str(output.trim()).ok()?;
    let text = |key: &str| {
        issue
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
    };
    let id = text("id")?;
    let title = text("title")?;
    let description: String = text("description")
        .unwrap_or_default()
        .chars()
        .take(DESCRIPTION_CHARS)
        .collect();
    let redact = |text: &str| super::state::redact(text).0;
    Some(
        json!({
            "id": id,
            "identifier": text("identifier"),
            "title": redact(title),
            "description": redact(&description),
        })
        .to_string(),
    )
}

fn decode(stored: &str) -> Option<IssueRead> {
    let value: Value = serde_json::from_str(stored).ok()?;
    let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
    Some(IssueRead {
        id: text("id")?,
        title: text("title")?,
        description: text("description").unwrap_or_default(),
    })
}

/// Issues read in each conversation, in call order.
pub fn load_all(conn: &Connection) -> Result<HashMap<(String, String), Vec<IssueRead>>> {
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, issue_read FROM fact_session_tool_call
         WHERE issue_read IS NOT NULL ORDER BY provider, session_id, call_index",
    )?;
    let mut out: HashMap<(String, String), Vec<IssueRead>> = HashMap::new();
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (provider, session_id, stored) = row?;
        if let Some(issue) = decode(&stored) {
            out.entry((provider, session_id)).or_default().push(issue);
        }
    }
    Ok(out)
}

/// Issues read in one conversation, in call order.
pub fn load(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<IssueRead>> {
    let mut stmt = conn.prepare(
        "SELECT issue_read FROM fact_session_tool_call
         WHERE provider = ?1 AND session_id = ?2 AND issue_read IS NOT NULL ORDER BY call_index",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.extend(decode(&row?));
    }
    Ok(out)
}

/// A bare assignment with the issue it names attached, when the conversation
/// read that issue. Any other request is returned unchanged.
pub fn attach(raw: &str, issues: &[IssueRead]) -> Option<String> {
    if issues.is_empty() || !super::request::normalize(raw).goal_unavailable() {
        return None;
    }
    let references = super::request::references(raw);
    let issue = issues
        .iter()
        .find(|issue| references.contains(&format!("issue:{}", issue.id)))?;
    let body = if issue.description.is_empty() {
        issue.title.clone()
    } else {
        format!("{}\n\n{}", issue.title, issue.description)
    };
    Some(format!("{raw}\n\n{OPEN}\n{body}\n{CLOSE}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ASSIGNMENT: &str = "You are running as a local coding agent for a Multica workspace.\n\nYour assigned issue ID is: 01a0d42d-99af-7d42-9f54-65863fbc704c\n\nReply as a comment when done.";

    fn issue_json() -> String {
        json!({
            "id": "01a0d42d-99af-7d42-9f54-65863fbc704c",
            "identifier": "KLIK-15",
            "title": "Migrate email sending to Cloudflare",
            "description": "## User request\n\nMigrate email sending to Cloudflare.\n\nToken: sk-abcdefghijklmnopqrstuvwxyz123456",
            "status": "in_progress",
        })
        .to_string()
    }

    #[test]
    fn keeps_a_redacted_brief_of_a_whole_issue_read() {
        let stored = parse(
            "multica issue get 01a0d42d-99af-7d42-9f54-65863fbc704c --output json",
            &issue_json(),
        )
        .expect("issue read");
        let issue = decode(&stored).expect("decodes");
        assert_eq!(issue.title, "Migrate email sending to Cloudflare");
        assert!(issue.description.starts_with("## User request"));
        assert!(!stored.contains("sk-abcdefghijklmnopqrstuvwxyz123456"));
        assert!(stored.contains("KLIK-15"));
    }

    #[test]
    fn ignores_other_commands_and_filtered_output() {
        assert!(parse("multica issue comment list x --output json", &issue_json()).is_none());
        assert!(parse("cat issue.json", &issue_json()).is_none());
        assert!(
            parse(
                "multica issue get x --output json | grep status",
                "  \"status\": \"blocked\","
            )
            .is_none()
        );
        assert!(parse("multica issue get x", "{\"id\":\"x\"}").is_none());
    }

    #[test]
    fn a_bare_assignment_takes_the_goal_of_the_issue_it_read() {
        let issue = decode(&parse("multica issue get 01a0d42d", &issue_json()).unwrap()).unwrap();
        let attached = attach(ASSIGNMENT, std::slice::from_ref(&issue)).expect("attached");
        let request = super::super::request::normalize(&attached);
        assert!(!request.goal_unavailable());
        assert_eq!(request.extraction, "agent_issue_read");
        assert!(
            request
                .text
                .starts_with("Migrate email sending to Cloudflare\n\n## User request")
        );
        assert!(!request.text.contains("01a0d42d"));
        // Normalized titles stay stated when normalized again.
        assert!(!super::super::request::normalize(&request.text).goal_unavailable());
        // Another issue, or a request that states its goal, is left alone.
        let other = IssueRead {
            id: "01a0ffff-0000-0000-0000-000000000000".into(),
            ..issue.clone()
        };
        assert!(attach(ASSIGNMENT, &[other]).is_none());
        assert!(attach("Fix the cache limit", &[issue]).is_none());
    }
}
