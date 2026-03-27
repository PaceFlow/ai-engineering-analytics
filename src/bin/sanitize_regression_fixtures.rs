use anyhow::{Context, Result};
use regex::Regex;
use rusqlite::{Connection, params};
use serde_json::{Map, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

static EMAIL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").expect("valid email regex")
});
static DOC_TITLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(How Developers Fight Vibe Coding Problems|Vibe Coding KPIs [^"]+?ROI)\.pdf"#)
        .expect("valid doc title regex")
});
static AUTHOR_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^Author:\s+.*$").expect("valid author regex"));
static ERROR_COUNT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d+\s+errors?\b").expect("valid error-count regex"));

const USER_TEMPLATES: &[&str] = &[
    "Please review the implementation and keep the behavior stable.",
    "Can you tighten this logic without changing the user-visible output?",
    "Please simplify the code path and preserve the current behavior.",
    "Can you investigate the current implementation and summarize the safest fix?",
];

const ASSISTANT_TEMPLATES: &[&str] = &[
    "Reviewed the request, preserved the behavior, and summarized the implementation tradeoffs.",
    "Inspected the code path, kept behavior stable, and outlined the safest update.",
    "Checked the implementation, preserved the current behavior, and noted the key follow-ups.",
];

const CONTINUATION_TEMPLATES: &[&str] = &["same error", "still failing", "doesnt work, same error"];

const ERROR_TEMPLATES: &[&str] = &[
    "TypeError: synthetic regression failure at line 12",
    "RuntimeError: synthetic fixture build failed",
    "test failed: synthetic regression case",
];

const EMPTY_RICH_TEXT: &str = r#"{"root":{"children":[{"children":[],"format":"","indent":0,"type":"paragraph","version":1}],"format":"","indent":0,"type":"root","version":1}}"#;
const SANITIZED_BASE_INSTRUCTIONS: &str =
    "Synthetic regression fixture instructions. Original fixture content was anonymized.";
const SANITIZED_REASONING: &str = "Summarizing the safest implementation approach.";
const SANITIZED_COMMIT_MATCHING: &str =
    "Please adjust the commit matching logic without changing the surrounding behavior.";
const SANITIZED_FIXTURE_OUTPUT: &str = "Sanitized fixture output.\n";
const SANITIZED_EXTERNAL_OUTPUT: &str = "Sanitized external reference output.\n";
const SANITIZED_GIT_OUTPUT: &str = "Sanitized git inspection output.\n";

fn main() -> Result<()> {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("regression")
        .join("home_template");
    let codex_root = fixture_root.join(".codex").join("sessions");
    let cursor_db = fixture_root.join("cursor").join("state.vscdb");

    let mut session_files = Vec::new();
    collect_jsonl_files(&codex_root, &mut session_files)?;
    session_files.sort();
    for path in session_files {
        sanitize_codex_session(&path)?;
    }
    sanitize_cursor_db(&cursor_db)?;
    Ok(())
}

fn contains_error_signal(text: &str) -> bool {
    let lower = text.to_lowercase();
    let markers = [
        "traceback",
        "typeerror",
        "referenceerror",
        "syntaxerror",
        "runtimeerror",
        "panic:",
        "exception:",
        "stack trace",
        "error:",
        "build failed",
        "test failed",
        "tests failed",
        "compilation failed",
        "cannot find module",
        "module not found",
    ];
    markers.iter().any(|marker| lower.contains(marker)) || ERROR_COUNT_RE.is_match(&lower)
}

fn is_error_continuation(text: &str) -> bool {
    let lower = text.to_lowercase();
    let phrases = [
        "same error",
        "same issue",
        "still failing",
        "still fails",
        "still broken",
        "not fixed",
        "didnt work",
        "doesnt work",
        "still not working",
    ];
    phrases.iter().any(|phrase| lower.contains(phrase))
}

fn sanitize_tool_output(text: &str) -> String {
    let cleaned = EMAIL_RE.replace_all(text, "redacted@example.com");
    let cleaned =
        AUTHOR_LINE_RE.replace_all(&cleaned, "Contributor: Sanitized <redacted@example.com>");
    let cleaned = cleaned.into_owned();
    if cleaned.contains("|codex|user|") || cleaned.contains("|codex|assistant|") {
        return SANITIZED_FIXTURE_OUTPUT.to_string();
    }
    if cleaned.contains("__HOME__/Downloads/") && cleaned.contains(".pdf") {
        return SANITIZED_EXTERNAL_OUTPUT.to_string();
    }
    if cleaned.contains("%PDF-") {
        return SANITIZED_EXTERNAL_OUTPUT.to_string();
    }
    if cleaned.starts_with("commit ") && cleaned.contains("diff --git") {
        return SANITIZED_GIT_OUTPUT.to_string();
    }
    if cleaned.contains("Vibe Coding KPIs") || cleaned.contains("=== PAGE 1 ===") {
        return SANITIZED_EXTERNAL_OUTPUT.to_string();
    }
    cleaned
}

fn sanitize_command_arguments(arguments: &str) -> String {
    if !arguments.contains("Vibe Coding KPIs")
        && !arguments.contains("How Developers Fight Vibe Coding Problems")
    {
        return arguments.to_string();
    }
    DOC_TITLE_RE
        .replace_all(arguments, "sanitized-reference.pdf")
        .into_owned()
}

#[derive(Default)]
struct MessageSanitizer {
    user_index: usize,
    assistant_index: usize,
}

impl MessageSanitizer {
    fn user_text(&mut self, text: &str) -> String {
        self.user_index += 1;
        let index = self.user_index - 1;
        if is_error_continuation(text) {
            return CONTINUATION_TEMPLATES[index % CONTINUATION_TEMPLATES.len()].to_string();
        }
        if contains_error_signal(text) {
            return ERROR_TEMPLATES[index % ERROR_TEMPLATES.len()].to_string();
        }
        if EMAIL_RE.is_match(text) || text.to_lowercase().contains("author:") {
            return SANITIZED_COMMIT_MATCHING.to_string();
        }
        USER_TEMPLATES[index % USER_TEMPLATES.len()].to_string()
    }

    fn assistant_text(&mut self, _text: &str) -> String {
        self.assistant_index += 1;
        ASSISTANT_TEMPLATES[(self.assistant_index - 1) % ASSISTANT_TEMPLATES.len()].to_string()
    }

    fn by_role(&mut self, role: &str, text: &str) -> String {
        match role {
            "user" => self.user_text(text),
            "assistant" => self.assistant_text(text),
            _ => text.to_string(),
        }
    }
}

fn sanitize_content_items(items: &mut [Value], role: &str, sanitizer: &mut MessageSanitizer) {
    for item in items {
        let Some(item_type) = item.get("type").and_then(Value::as_str) else {
            continue;
        };
        if !matches!(item_type, "input_text" | "output_text" | "summary_text") {
            continue;
        }
        if let Some(text) = item.get("text").and_then(Value::as_str).map(str::to_string) {
            item["text"] = Value::String(sanitizer.by_role(role, &text));
        }
    }
}

fn sanitize_compacted_history(payload: &mut Map<String, Value>, sanitizer: &mut MessageSanitizer) {
    let Some(history) = payload
        .get_mut("replacement_history")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for entry in history {
        let Some(role) = entry
            .get("role")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        if !matches!(role.as_str(), "user" | "assistant") {
            continue;
        }
        if let Some(items) = entry.get_mut("content").and_then(Value::as_array_mut) {
            sanitize_content_items(items, &role, sanitizer);
        }
    }
}

fn sanitize_codex_session(path: &Path) -> Result<()> {
    let content =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut sanitizer = MessageSanitizer::default();
    let mut new_lines = Vec::new();

    for raw_line in content.lines() {
        if raw_line.trim().is_empty() {
            new_lines.push(raw_line.to_string());
            continue;
        }

        let mut record: Value = serde_json::from_str(raw_line)
            .with_context(|| format!("parsing JSONL record in {}", path.display()))?;
        let record_type = record
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if let Some(payload) = record.get_mut("payload").and_then(Value::as_object_mut) {
            match record_type.as_str() {
                "session_meta" => {
                    if let Some(base_instructions) = payload
                        .get_mut("base_instructions")
                        .and_then(Value::as_object_mut)
                        && base_instructions
                            .get("text")
                            .and_then(Value::as_str)
                            .is_some()
                    {
                        base_instructions.insert(
                            "text".to_string(),
                            Value::String(SANITIZED_BASE_INSTRUCTIONS.to_string()),
                        );
                    }
                }
                "event_msg" => {
                    let payload_type = payload
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    match payload_type {
                        "user_message" => {
                            if let Some(message) = payload
                                .get("message")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                            {
                                payload.insert(
                                    "message".to_string(),
                                    Value::String(sanitizer.user_text(&message)),
                                );
                            }
                        }
                        "agent_message" => {
                            if let Some(message) = payload
                                .get("message")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                            {
                                payload.insert(
                                    "message".to_string(),
                                    Value::String(sanitizer.assistant_text(&message)),
                                );
                            }
                        }
                        "agent_reasoning" => {
                            if payload.get("text").and_then(Value::as_str).is_some() {
                                payload.insert(
                                    "text".to_string(),
                                    Value::String(SANITIZED_REASONING.to_string()),
                                );
                            }
                        }
                        "task_complete" => {
                            if let Some(message) = payload
                                .get("last_agent_message")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                            {
                                payload.insert(
                                    "last_agent_message".to_string(),
                                    Value::String(sanitizer.assistant_text(&message)),
                                );
                            }
                        }
                        _ => {}
                    }
                }
                "response_item" => {
                    let payload_type = payload
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let role = payload
                        .get("role")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    match payload_type {
                        "message" if matches!(role.as_str(), "user" | "assistant") => {
                            if let Some(items) =
                                payload.get_mut("content").and_then(Value::as_array_mut)
                            {
                                sanitize_content_items(items, &role, &mut sanitizer);
                            }
                        }
                        "reasoning" => {
                            if let Some(items) =
                                payload.get_mut("summary").and_then(Value::as_array_mut)
                            {
                                sanitize_content_items(items, "assistant", &mut sanitizer);
                            }
                            payload.insert("encrypted_content".to_string(), Value::Null);
                        }
                        "function_call" => {
                            if let Some(arguments) = payload
                                .get("arguments")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                            {
                                payload.insert(
                                    "arguments".to_string(),
                                    Value::String(sanitize_command_arguments(&arguments)),
                                );
                            }
                        }
                        "function_call_output" => {
                            if let Some(output) = payload
                                .get("output")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                            {
                                payload.insert(
                                    "output".to_string(),
                                    Value::String(sanitize_tool_output(&output)),
                                );
                            }
                        }
                        _ => {}
                    }
                }
                "compacted" => sanitize_compacted_history(payload, &mut sanitizer),
                _ => {}
            }
        }

        new_lines.push(serde_json::to_string(&record)?);
    }

    fs::write(path, format!("{}\n", new_lines.join("\n")))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn sanitize_cursor_row(key: &str, value: &str, row_index: usize) -> Result<String> {
    let mut parsed: Value =
        serde_json::from_str(value).with_context(|| format!("parsing cursor row for key {key}"))?;

    if key.starts_with("bubbleId:") {
        if let Some(text) = parsed.get_mut("text")
            && text.is_string()
        {
            *text = Value::String(USER_TEMPLATES[row_index % USER_TEMPLATES.len()].to_string());
        }
        return Ok(serde_json::to_string(&parsed)?);
    }

    if key.starts_with("composerData:") {
        if let Some(text) = parsed.get_mut("text")
            && text.is_string()
        {
            *text = Value::String(USER_TEMPLATES[row_index % USER_TEMPLATES.len()].to_string());
        }
        if let Some(rich_text) = parsed.get_mut("richText")
            && rich_text.is_string()
        {
            *rich_text = Value::String(EMPTY_RICH_TEXT.to_string());
        }
        if let Some(conversation) = parsed.get_mut("conversation").and_then(Value::as_array_mut) {
            for (idx, bubble) in conversation.iter_mut().enumerate() {
                let Some(bubble_obj) = bubble.as_object_mut() else {
                    continue;
                };
                if !bubble_obj.get("text").is_some_and(Value::is_string) {
                    continue;
                }
                let replacement = if bubble_obj.get("bubbleType").and_then(Value::as_i64) == Some(1)
                {
                    USER_TEMPLATES[idx % USER_TEMPLATES.len()]
                } else {
                    ASSISTANT_TEMPLATES[idx % ASSISTANT_TEMPLATES.len()]
                };
                bubble_obj.insert("text".to_string(), Value::String(replacement.to_string()));
            }
        }
        return Ok(serde_json::to_string(&parsed)?);
    }

    Ok(value.to_string())
}

fn sanitize_cursor_db(path: &Path) -> Result<()> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    let rows = {
        let mut stmt = conn.prepare("SELECT key, value FROM cursorDiskKV ORDER BY key")?;
        let mapped = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut rows = Vec::new();
        for row in mapped {
            rows.push(row?);
        }
        rows
    };

    for (idx, (key, value)) in rows.into_iter().enumerate() {
        let updated = sanitize_cursor_row(&key, &value, idx)?;
        if updated != value {
            conn.execute(
                "UPDATE cursorDiskKV SET value = ?1 WHERE key = ?2",
                params![updated, key],
            )?;
        }
    }

    Ok(())
}

fn collect_jsonl_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "jsonl") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_error_signals_from_markers_and_counts() {
        assert!(contains_error_signal("RuntimeError: boom"));
        assert!(contains_error_signal("build failed with 12 errors"));
        assert!(!contains_error_signal("all good here"));
    }

    #[test]
    fn sanitizes_tool_output_for_git_and_external_content() {
        assert_eq!(
            sanitize_tool_output("Author: Alice <alice@example.com>\nrest"),
            "Contributor: Sanitized <redacted@example.com>\nrest"
        );
        assert_eq!(
            sanitize_tool_output("commit abc123\n...\ndiff --git a/x b/x"),
            SANITIZED_GIT_OUTPUT
        );
        assert_eq!(
            sanitize_tool_output("%PDF-1.7 something"),
            SANITIZED_EXTERNAL_OUTPUT
        );
    }

    #[test]
    fn sanitizes_command_arguments_doc_titles() {
        let input = r#"{"cmd":"open __HOME__/Downloads/Vibe Coding KPIs Q4 ROI.pdf"}"#;
        assert!(sanitize_command_arguments(input).contains("sanitized-reference.pdf"));
    }

    #[test]
    fn sanitizes_cursor_rows() -> Result<()> {
        let updated = sanitize_cursor_row(
            "composerData:1",
            r#"{"text":"hello","richText":"{}","conversation":[{"bubbleType":1,"text":"u"},{"bubbleType":2,"text":"a"}]}"#,
            0,
        )?;
        let parsed: Value = serde_json::from_str(&updated)?;
        assert_eq!(parsed["text"], USER_TEMPLATES[0]);
        assert_eq!(parsed["richText"], EMPTY_RICH_TEXT);
        assert_eq!(parsed["conversation"][0]["text"], USER_TEMPLATES[0]);
        assert_eq!(parsed["conversation"][1]["text"], ASSISTANT_TEMPLATES[1]);
        Ok(())
    }
}
