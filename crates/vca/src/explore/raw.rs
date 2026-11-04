//! The raw transcript of one session, read from the provider's own files when the
//! review page asks for it. Nothing here is stored: prompts, replies, reasoning and
//! tool output stay in the source and are only shown on screen.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::Path;

/// Longest text shown per block; the rest is cut and the cut size reported.
const MAX_BLOCK_CHARS: usize = 40_000;

/// Messages in source order, each `{id, role, ts, blocks}`. `id` matches the usage
/// key of the model call that produced it, so the page can show a call's raw step.
pub fn raw_transcript(conn: &Connection, provider: &str, session_id: &str) -> Result<Value> {
    let source: Option<String> = conn
        .query_row(
            "SELECT source_path FROM metadata_sessions WHERE provider = ?1 AND session_id = ?2",
            params![provider, session_id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(source) = source else {
        return Ok(json!({ "error": "no source file is recorded for this session" }));
    };
    let path = Path::new(&source);
    if !path.exists() {
        return Ok(json!({ "error": format!("the source is no longer there: {source}") }));
    }
    let messages = match provider {
        "opencode" => opencode(path, session_id)?,
        "claude" => claude(path)?,
        _ => {
            return Ok(
                json!({ "error": format!("the raw view does not read {provider} sessions yet") }),
            );
        }
    };
    Ok(json!({ "source": source, "messages": messages }))
}

fn clip(text: &str) -> (String, usize) {
    let total = text.chars().count();
    if total <= MAX_BLOCK_CHARS {
        return (text.to_string(), 0);
    }
    (
        text.chars().take(MAX_BLOCK_CHARS).collect(),
        total - MAX_BLOCK_CHARS,
    )
}

fn text_block(kind: &str, text: &str) -> Option<Value> {
    if text.trim().is_empty() {
        return None;
    }
    let (text, cut) = clip(text);
    Some(json!({ "kind": kind, "text": text, "cut": cut }))
}

fn tool_block(
    call_id: Option<&str>,
    name: &str,
    status: Option<&str>,
    input: &Value,
    output: &str,
) -> Value {
    let input = match input {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => serde_json::to_string_pretty(other).unwrap_or_default(),
    };
    let (input, input_cut) = clip(&input);
    let (text, cut) = clip(output);
    json!({
        "kind": "tool", "call_id": call_id, "tool": name, "status": status, "input": input,
        "input_cut": input_cut, "text": text, "cut": cut,
    })
}

fn ms_ts(ms: Option<i64>) -> Option<String> {
    ms.and_then(crate::providers::opencode::ms_to_iso)
}

fn opencode(path: &Path, session_id: &str) -> Result<Vec<Value>> {
    let source = crate::providers::opencode::open_readonly_db(path)?;
    let has_table = |name: &str| -> Result<bool> {
        Ok(source
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    };
    if has_table("session_message")? {
        let v2 = opencode_v2(&source, session_id)?;
        if !v2.is_empty() {
            return Ok(v2);
        }
    }
    if has_table("message")? {
        return opencode_v1(&source, session_id);
    }
    Ok(Vec::new())
}

fn opencode_v1(source: &Connection, session_id: &str) -> Result<Vec<Value>> {
    let mut blocks: HashMap<String, Vec<Value>> = HashMap::new();
    let mut stmt = source.prepare(
        "SELECT message_id, data FROM part WHERE session_id = ?1 ORDER BY time_created, id",
    )?;
    let rows = stmt.query_map([session_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (message_id, data) = row?;
        let part: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        let text = |key: &str| part.get(key).and_then(Value::as_str).unwrap_or_default();
        let block = match part.get("type").and_then(Value::as_str) {
            Some("text") => text_block("text", text("text")),
            Some("reasoning") => text_block("reasoning", text("text")),
            Some("tool") => Some(tool_block(
                part.get("callID").and_then(Value::as_str),
                text("tool"),
                part.pointer("/state/status").and_then(Value::as_str),
                part.pointer("/state/input").unwrap_or(&Value::Null),
                part.pointer("/state/output")
                    .or_else(|| part.pointer("/state/error"))
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )),
            _ => None,
        };
        if let Some(block) = block {
            blocks.entry(message_id).or_default().push(block);
        }
    }
    let mut stmt = source.prepare(
        "SELECT id, time_created, data FROM message WHERE session_id = ?1 ORDER BY time_created, id",
    )?;
    let rows = stmt.query_map([session_id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, created, data) = row?;
        let data: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        out.push(json!({
            "role": data.get("role").and_then(Value::as_str).unwrap_or("unknown"),
            "ts": ms_ts(data.pointer("/time/created").and_then(Value::as_i64).or(Some(created))),
            "blocks": blocks.remove(&id).unwrap_or_default(),
            "id": id,
        }));
    }
    Ok(out)
}

fn opencode_v2(source: &Connection, session_id: &str) -> Result<Vec<Value>> {
    let mut stmt = source.prepare(
        "SELECT id, type, time_created, data FROM session_message WHERE session_id = ?1 ORDER BY seq, id",
    )?;
    let rows = stmt.query_map([session_id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, kind, created, data) = row?;
        let data: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        let text = |value: &Value, key: &str| {
            value
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let mut blocks = Vec::new();
        for part in data
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let block = match part.get("type").and_then(Value::as_str) {
                Some("text") => text_block("text", &text(part, "text")),
                Some("reasoning") => text_block("reasoning", &text(part, "text")),
                Some("tool") => {
                    let output = part
                        .pointer("/state/content")
                        .and_then(Value::as_array)
                        .map(|content| {
                            content
                                .iter()
                                .filter_map(|p| p.get("text").and_then(Value::as_str))
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .unwrap_or_default();
                    Some(tool_block(
                        part.get("id").and_then(Value::as_str),
                        &text(part, "name"),
                        part.pointer("/state/status").and_then(Value::as_str),
                        part.pointer("/state/input").unwrap_or(&Value::Null),
                        &output,
                    ))
                }
                _ => None,
            };
            blocks.extend(block);
        }
        // User, system and summary messages carry their text at the top level.
        blocks.extend(text_block("text", &text(&data, "text")));
        blocks.extend(text_block("text", &text(&data, "summary")));
        out.push(json!({
            "role": kind,
            "ts": ms_ts(data.pointer("/time/created").and_then(Value::as_i64).or(Some(created))),
            "blocks": blocks,
            "id": id,
        }));
    }
    Ok(out)
}

/// Claude Code writes one JSONL line per content block; assistant lines of one API
/// message share `message.id`, which is also how its usage is keyed (`msg:<id>`).
/// Tool results arrive as user lines and are put on the tool call that asked for them.
fn claude(path: &Path) -> Result<Vec<Value>> {
    let reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let mut out: Vec<Value> = Vec::new();
    let mut by_id: HashMap<String, usize> = HashMap::new();
    // tool_use id -> (message, block) of the call
    let mut calls: HashMap<String, (usize, usize)> = HashMap::new();
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let role = record.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(role, "user" | "assistant" | "system") {
            continue;
        }
        let message = record.get("message").unwrap_or(&Value::Null);
        let ts = record.get("timestamp").and_then(Value::as_str);
        let at = match message.get("id").and_then(Value::as_str) {
            Some(message_id) if role == "assistant" => {
                let id = format!("msg:{message_id}");
                *by_id.entry(id.clone()).or_insert_with(|| {
                    out.push(json!({ "id": id, "role": role, "ts": ts, "blocks": [] }));
                    out.len() - 1
                })
            }
            _ => {
                let id = record
                    .get("uuid")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("line:{line_no}"));
                out.push(json!({ "id": id, "role": role, "ts": ts, "blocks": [] }));
                out.len() - 1
            }
        };
        let parts = match message.get("content") {
            Some(Value::Array(parts)) => parts.clone(),
            Some(Value::String(text)) => vec![json!({ "type": "text", "text": text })],
            _ => vec![json!({ "type": "text", "text": record.get("content") })],
        };
        for part in &parts {
            let text = |key: &str| part.get(key).and_then(Value::as_str).unwrap_or("");
            let block = match part.get("type").and_then(Value::as_str) {
                Some("text") => text_block("text", text("text")),
                Some("thinking") => text_block("reasoning", text("thinking")),
                Some("tool_use") => {
                    let blocks = out[at]["blocks"].as_array().map_or(0, Vec::len);
                    calls.insert(text("id").to_string(), (at, blocks));
                    Some(tool_block(
                        part.get("id").and_then(Value::as_str),
                        text("name"),
                        None,
                        part.get("input").unwrap_or(&Value::Null),
                        "",
                    ))
                }
                Some("tool_result") => {
                    let output = match part.get("content") {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Array(items)) => items
                            .iter()
                            .filter_map(|p| p.get("text").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join(
                                "
",
                            ),
                        _ => String::new(),
                    };
                    let status = if part.get("is_error").and_then(Value::as_bool) == Some(true) {
                        "error"
                    } else {
                        "completed"
                    };
                    match calls.get(text("tool_use_id")) {
                        Some(&(message, block)) => {
                            let (output, cut) = clip(&output);
                            let call = &mut out[message]["blocks"][block];
                            call["text"] = json!(output);
                            call["cut"] = json!(cut);
                            call["status"] = json!(status);
                            None
                        }
                        None => text_block("tool_output", &output),
                    }
                }
                _ => None,
            };
            if let (Some(block), Some(blocks)) = (block, out[at]["blocks"].as_array_mut()) {
                blocks.push(block);
            }
        }
        // A line whose only content went onto an earlier tool call leaves nothing to show.
        if out[at]["blocks"].as_array().is_some_and(Vec::is_empty) && role == "user" {
            out.pop();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_blocks_are_cut_and_the_cut_is_reported() {
        let text = "x".repeat(MAX_BLOCK_CHARS + 5);
        let block = text_block("text", &text).unwrap();
        assert_eq!(block["cut"], 5);
        assert_eq!(block["text"].as_str().unwrap().len(), MAX_BLOCK_CHARS);
    }

    #[test]
    fn claude_assistant_lines_of_one_message_are_joined_under_its_usage_key() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("s.jsonl");
        let lines = [
            json!({"type":"user","uuid":"u1","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"fix it"}}),
            json!({"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{"id":"m1","content":[{"type":"thinking","thinking":"look first"}]}}),
            json!({"type":"assistant","timestamp":"2026-10-01T10:00:02Z","message":{"id":"m1","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}),
            json!({"type":"user","uuid":"u2","timestamp":"2026-10-01T10:00:03Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"a.rs"}]}}),
        ];
        std::fs::write(
            &path,
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        let messages = claude(&path)?;
        // The tool result went onto the call, so its user line is not listed.
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1]["id"], "msg:m1");
        let kinds: Vec<&str> = messages[1]["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["reasoning", "tool"]);
        assert_eq!(messages[1]["blocks"][1]["text"], "a.rs");
        assert_eq!(messages[1]["blocks"][1]["status"], "completed");
        Ok(())
    }

    #[test]
    fn opencode_steps_show_reasoning_text_and_tools_under_the_message_id() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("opencode.db");
        let db = Connection::open(&path)?;
        db.execute_batch(
            "CREATE TABLE message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
             CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);",
        )?;
        db.execute(
            "INSERT INTO message VALUES ('msg1', 's', 1790848860000, ?1)",
            [json!({"role":"assistant","time":{"created":1790848860000_i64}}).to_string()],
        )?;
        for (id, data) in [
            ("p1", json!({"type":"reasoning","text":"check the issue"})),
            (
                "p2",
                json!({"type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"multica issue get 1"},"output":"{\"id\":1}"}}),
            ),
            ("p3", json!({"type":"step-finish","tokens":{"input":0}})),
        ] {
            db.execute(
                "INSERT INTO part VALUES (?1, 'msg1', 's', 1790848860000, ?2)",
                params![id, data.to_string()],
            )?;
        }
        drop(db);
        let messages = opencode(&path, "s")?;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["id"], "msg1");
        let blocks = messages[0]["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["text"], "check the issue");
        assert_eq!(blocks[1]["tool"], "bash");
        assert!(
            blocks[1]["input"]
                .as_str()
                .unwrap()
                .contains("multica issue get 1")
        );
        assert_eq!(blocks[1]["text"], "{\"id\":1}");
        Ok(())
    }
}
