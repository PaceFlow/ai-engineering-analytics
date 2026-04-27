use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::change_intel::line_hash::{diff_with_hashes, hash_line, hashes_for_text, line_count};
use crate::change_intel::types::{LineHashCount, LineSide, WriteMode};
use crate::path_utils::{
    normalize_filesystem_path, path_to_string, resolve_path, strip_file_scheme,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeMessageRole {
    User,
    Assistant,
}

impl ClaudeMessageRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeVisibleMessage {
    pub role: ClaudeMessageRole,
    pub text: String,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaudeStructuredWrite {
    pub session_id: String,
    pub source_file: String,
    pub session_cwd: Option<String>,
    pub timestamp: Option<String>,
    pub call_id: String,
    pub abs_path: String,
    pub write_mode: WriteMode,
    pub before_known: bool,
    pub added_lines: i64,
    pub removed_lines: i64,
    pub line_hashes: Vec<LineHashCount>,
    pub parser_name: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedClaudeSession {
    pub session_id: String,
    pub source_file: String,
    pub session_cwd: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub model_name: Option<String>,
    pub visible_messages: Vec<ClaudeVisibleMessage>,
    pub structured_writes: Vec<ClaudeStructuredWrite>,
    pub tool_call_count: usize,
}

#[derive(Debug, Clone)]
struct PendingToolUse {
    timestamp: Option<String>,
    call_id: String,
    tool_name: String,
    input: Value,
    result_payload: Option<Value>,
    result_error: bool,
}

const CLAUDE_EDIT_PARSER_NAME: &str = "claude_edit_v1";
const CLAUDE_WRITE_PARSER_NAME: &str = "claude_write_v1";

pub(crate) fn plan_session_files() -> Result<Vec<PathBuf>> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    let root = home.join(".claude").join("projects");
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = discover_top_level_jsonl_files(&root);
    files.sort();
    Ok(files)
}

pub(crate) fn parse_session_file(path: &Path) -> Result<ParsedClaudeSession> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let source_file = path.to_string_lossy().to_string();
    let fallback_session_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string();

    let mut session_id = fallback_session_id.clone();
    let mut session_cwd: Option<String> = None;
    let mut started_at: Option<String> = None;
    let mut ended_at: Option<String> = None;
    let mut model_name: Option<String> = None;
    let mut visible_messages = Vec::new();
    let mut pending = Vec::new();
    let mut pending_by_id = HashMap::new();
    let mut tool_call_count = 0usize;

    for line_result in reader.lines() {
        let raw = line_result?;
        if raw.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if let Some(found_session_id) = parsed.get("sessionId").and_then(Value::as_str)
            && !found_session_id.trim().is_empty()
        {
            session_id = found_session_id.to_string();
        }

        if let Some(found_cwd) = parsed.get("cwd").and_then(Value::as_str)
            && !found_cwd.trim().is_empty()
        {
            session_cwd = Some(found_cwd.to_string());
        }

        let outer_timestamp = parsed
            .get("timestamp")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        update_time_bounds(&mut started_at, &mut ended_at, outer_timestamp.as_deref());

        process_top_level_record(
            &parsed,
            &session_id,
            &source_file,
            &mut model_name,
            &mut visible_messages,
            &mut pending,
            &mut pending_by_id,
            &mut tool_call_count,
        );

        if parsed.get("type").and_then(Value::as_str) == Some("progress")
            && parsed
                .get("data")
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                == Some("agent_progress")
            && let Some(nested) = parsed.get("data").and_then(|value| value.get("message"))
        {
            process_nested_progress_message(
                nested,
                &parsed,
                &session_id,
                &source_file,
                &mut pending,
                &mut pending_by_id,
                &mut tool_call_count,
            );
        }
    }

    if session_id.is_empty() {
        session_id = fallback_session_id;
    }

    let structured_writes = pending
        .into_iter()
        .filter_map(|tool| {
            build_structured_write(tool, &session_id, &source_file, session_cwd.as_deref())
        })
        .collect();

    Ok(ParsedClaudeSession {
        session_id,
        source_file,
        session_cwd,
        started_at,
        ended_at,
        model_name,
        visible_messages,
        structured_writes,
        tool_call_count,
    })
}

fn discover_top_level_jsonl_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return files;
    };

    for entry in entries.flatten() {
        let project_dir = entry.path();
        if !project_dir.is_dir() {
            continue;
        }

        let Ok(project_entries) = std::fs::read_dir(&project_dir) else {
            continue;
        };
        for project_entry in project_entries.flatten() {
            let candidate = project_entry.path();
            if candidate.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                files.push(candidate);
            }
        }
    }

    files
}

fn process_top_level_record(
    parsed: &Value,
    session_id: &str,
    source_file: &str,
    model_name: &mut Option<String>,
    visible_messages: &mut Vec<ClaudeVisibleMessage>,
    pending: &mut Vec<PendingToolUse>,
    pending_by_id: &mut HashMap<String, usize>,
    tool_call_count: &mut usize,
) {
    let Some(kind) = parsed.get("type").and_then(Value::as_str) else {
        return;
    };
    let Some(message) = parsed.get("message") else {
        return;
    };
    let timestamp = parsed
        .get("timestamp")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let is_meta = parsed
        .get("isMeta")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    match kind {
        "assistant" => {
            if let Some(found_model) = extract_real_model_name(message) {
                *model_name = Some(found_model);
            }
            if let Some(text) = extract_assistant_visible_text(message)
                && !text.trim().is_empty()
            {
                visible_messages.push(ClaudeVisibleMessage {
                    role: ClaudeMessageRole::Assistant,
                    text,
                    timestamp: timestamp.clone(),
                });
            }
            register_tool_uses(
                message,
                timestamp,
                session_id,
                source_file,
                pending,
                pending_by_id,
                tool_call_count,
            );
        }
        "user" => {
            attach_tool_results(parsed, message, pending, pending_by_id);
            if !is_meta
                && let Some(text) = extract_user_visible_text(message)
                && !should_ignore_user_text(&text)
            {
                visible_messages.push(ClaudeVisibleMessage {
                    role: ClaudeMessageRole::User,
                    text,
                    timestamp,
                });
            }
        }
        _ => {}
    }
}

fn process_nested_progress_message(
    nested: &Value,
    outer: &Value,
    session_id: &str,
    source_file: &str,
    pending: &mut Vec<PendingToolUse>,
    pending_by_id: &mut HashMap<String, usize>,
    tool_call_count: &mut usize,
) {
    let Some(kind) = nested.get("type").and_then(Value::as_str) else {
        return;
    };
    let Some(message) = nested.get("message") else {
        return;
    };
    let timestamp = nested
        .get("timestamp")
        .and_then(Value::as_str)
        .or_else(|| outer.get("timestamp").and_then(Value::as_str))
        .map(ToOwned::to_owned);

    match kind {
        "assistant" => register_tool_uses(
            message,
            timestamp,
            session_id,
            source_file,
            pending,
            pending_by_id,
            tool_call_count,
        ),
        "user" => attach_tool_results(outer, message, pending, pending_by_id),
        _ => {}
    }
}

fn register_tool_uses(
    message: &Value,
    timestamp: Option<String>,
    session_id: &str,
    source_file: &str,
    pending: &mut Vec<PendingToolUse>,
    pending_by_id: &mut HashMap<String, usize>,
    tool_call_count: &mut usize,
) {
    let Some(items) = message.get("content").and_then(Value::as_array) else {
        return;
    };

    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("tool_use") {
            continue;
        }

        let Some(call_id) = item.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(tool_name) = item.get("name").and_then(Value::as_str) else {
            continue;
        };

        *tool_call_count += 1;
        let pending_index = pending.len();
        pending.push(PendingToolUse {
            timestamp: timestamp.clone(),
            call_id: call_id.to_string(),
            tool_name: tool_name.to_string(),
            input: item.get("input").cloned().unwrap_or(Value::Null),
            result_payload: None,
            result_error: false,
        });
        pending_by_id.insert(call_id.to_string(), pending_index);
        let _ = (session_id, source_file);
    }
}

fn attach_tool_results(
    outer: &Value,
    message: &Value,
    pending: &mut [PendingToolUse],
    pending_by_id: &HashMap<String, usize>,
) {
    let Some(items) = message.get("content").and_then(Value::as_array) else {
        return;
    };

    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("tool_result") {
            continue;
        }

        let Some(tool_use_id) = item
            .get("tool_use_id")
            .and_then(Value::as_str)
            .or_else(|| item.get("toolUseId").and_then(Value::as_str))
        else {
            continue;
        };
        let Some(index) = pending_by_id.get(tool_use_id).copied() else {
            continue;
        };
        let Some(tool) = pending.get_mut(index) else {
            continue;
        };

        tool.result_error = item
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        tool.result_payload = outer.get("toolUseResult").cloned();
        if tool.result_error
            && tool.result_payload.is_none()
            && let Some(text) = item.get("content").and_then(Value::as_str)
        {
            tool.result_payload = Some(Value::String(text.to_string()));
        }
    }
}

fn extract_real_model_name(message: &Value) -> Option<String> {
    let model = message.get("model").and_then(Value::as_str)?;
    let trimmed = model.trim();
    if trimmed.is_empty() || trimmed == "<synthetic>" {
        return None;
    }
    Some(trimmed.to_string())
}

fn extract_assistant_visible_text(message: &Value) -> Option<String> {
    let items = message.get("content").and_then(Value::as_array)?;
    let text = items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn extract_user_visible_text(message: &Value) -> Option<String> {
    match message.get("content") {
        Some(Value::String(text)) => Some(text.to_string()),
        Some(Value::Array(items)) => {
            if items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"))
            {
                return None;
            }

            let text = items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            (!text.trim().is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn should_ignore_user_text(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.is_empty()
        || trimmed == "[Request interrupted by user for tool use]"
        || trimmed.contains("<local-command-caveat>")
        || trimmed.contains("<command-name>/")
        || trimmed.starts_with("<command-")
}

fn build_structured_write(
    tool: PendingToolUse,
    session_id: &str,
    source_file: &str,
    session_cwd: Option<&str>,
) -> Option<ClaudeStructuredWrite> {
    if tool.result_error {
        return None;
    }

    match tool.tool_name.as_str() {
        "Edit" => build_edit_write(tool, session_id, source_file, session_cwd),
        "Write" => build_file_write(tool, session_id, source_file, session_cwd),
        _ => None,
    }
}

fn build_edit_write(
    tool: PendingToolUse,
    session_id: &str,
    source_file: &str,
    session_cwd: Option<&str>,
) -> Option<ClaudeStructuredWrite> {
    let payload = tool.result_payload.as_ref()?.as_object()?;
    let raw_path = payload
        .get("filePath")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("file_path").and_then(Value::as_str))?;
    let abs_path = resolve_claude_path(raw_path, session_cwd)?;
    if should_ignore_write_path(&abs_path) {
        return None;
    }

    let old_string = payload
        .get("oldString")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("old_string").and_then(Value::as_str))?;
    let new_string = payload
        .get("newString")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("new_string").and_then(Value::as_str))?;

    let diff = diff_with_hashes(old_string, new_string);
    if diff.added_lines == 0 && diff.removed_lines == 0 {
        return None;
    }

    Some(ClaudeStructuredWrite {
        session_id: session_id.to_string(),
        source_file: source_file.to_string(),
        session_cwd: session_cwd.map(ToOwned::to_owned),
        timestamp: tool.timestamp,
        call_id: tool.call_id,
        abs_path,
        write_mode: WriteMode::Patch,
        before_known: true,
        added_lines: diff.added_lines,
        removed_lines: diff.removed_lines,
        line_hashes: diff.line_hashes,
        parser_name: CLAUDE_EDIT_PARSER_NAME,
    })
}

fn build_file_write(
    tool: PendingToolUse,
    session_id: &str,
    source_file: &str,
    session_cwd: Option<&str>,
) -> Option<ClaudeStructuredWrite> {
    let payload = tool.result_payload.as_ref()?.as_object()?;
    let raw_path = payload
        .get("filePath")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("file_path").and_then(Value::as_str))?;
    let abs_path = resolve_claude_path(raw_path, session_cwd)?;
    if should_ignore_write_path(&abs_path) {
        return None;
    }

    let write_type = payload
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("type").and_then(Value::as_str));
    let after = payload
        .get("content")
        .and_then(Value::as_str)
        .or_else(|| tool.input.get("content").and_then(Value::as_str))?;

    let (write_mode, before_known, added_lines, removed_lines, line_hashes) =
        if write_type == Some("create") {
            (
                WriteMode::Overwrite,
                false,
                line_count(after),
                0,
                hashes_for_text(after, LineSide::Added),
            )
        } else if let Some(summary) = structured_patch_summary(payload.get("structuredPatch")) {
            (
                WriteMode::Patch,
                true,
                summary.added_lines,
                summary.removed_lines,
                summary.line_hashes,
            )
        } else if let Some(original) = payload.get("originalFile").and_then(Value::as_str) {
            let diff = diff_with_hashes(original, after);
            (
                WriteMode::Overwrite,
                true,
                diff.added_lines,
                diff.removed_lines,
                diff.line_hashes,
            )
        } else {
            (
                WriteMode::Overwrite,
                false,
                line_count(after),
                0,
                hashes_for_text(after, LineSide::Added),
            )
        };

    if added_lines == 0 && removed_lines == 0 {
        return None;
    }

    Some(ClaudeStructuredWrite {
        session_id: session_id.to_string(),
        source_file: source_file.to_string(),
        session_cwd: session_cwd.map(ToOwned::to_owned),
        timestamp: tool.timestamp,
        call_id: tool.call_id,
        abs_path,
        write_mode,
        before_known,
        added_lines,
        removed_lines,
        line_hashes,
        parser_name: CLAUDE_WRITE_PARSER_NAME,
    })
}

#[derive(Debug)]
struct StructuredPatchSummary {
    added_lines: i64,
    removed_lines: i64,
    line_hashes: Vec<LineHashCount>,
}

fn structured_patch_summary(value: Option<&Value>) -> Option<StructuredPatchSummary> {
    let hunks = value?.as_array()?;
    let mut counts: HashMap<(LineSide, String), i64> = HashMap::new();
    let mut added_lines = 0i64;
    let mut removed_lines = 0i64;

    for hunk in hunks {
        let Some(lines) = hunk.get("lines").and_then(Value::as_array) else {
            continue;
        };

        for line in lines.iter().filter_map(Value::as_str) {
            if let Some(rest) = line.strip_prefix('+') {
                added_lines += 1;
                *counts
                    .entry((LineSide::Added, hash_line(rest)))
                    .or_insert(0) += 1;
            } else if let Some(rest) = line.strip_prefix('-') {
                removed_lines += 1;
                *counts
                    .entry((LineSide::Removed, hash_line(rest)))
                    .or_insert(0) += 1;
            }
        }
    }

    if added_lines == 0 && removed_lines == 0 {
        return None;
    }

    let line_hashes = counts
        .into_iter()
        .map(|((side, line_hash), count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect();

    Some(StructuredPatchSummary {
        added_lines,
        removed_lines,
        line_hashes,
    })
}

fn resolve_claude_path(raw_path: &str, session_cwd: Option<&str>) -> Option<String> {
    let stripped = strip_file_scheme(raw_path);
    let normalized = normalize_filesystem_path(&stripped);
    let resolved = resolve_path(&normalized, None, session_cwd);
    Some(path_to_string(&resolved))
}

fn should_ignore_write_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    normalized.contains("/.claude/plans/")
}

fn update_time_bounds(
    started_at: &mut Option<String>,
    ended_at: &mut Option<String>,
    timestamp: Option<&str>,
) {
    let Some(timestamp) = timestamp else {
        return;
    };
    if started_at
        .as_ref()
        .map(|current| timestamp < current.as_str())
        .unwrap_or(true)
    {
        *started_at = Some(timestamp.to_string());
    }
    if ended_at
        .as_ref()
        .map(|current| timestamp > current.as_str())
        .unwrap_or(true)
    {
        *ended_at = Some(timestamp.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClaudeMessageRole, discover_top_level_jsonl_files, parse_session_file,
        should_ignore_user_text, structured_patch_summary,
    };
    use anyhow::Result;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn structured_patch_summary_counts_and_hashes_changes() {
        let summary = structured_patch_summary(Some(&json!([
            {"lines": [" line 1", "-old value", "+new value", "+second line"]}
        ])))
        .expect("summary should be produced");

        assert_eq!(summary.added_lines, 2);
        assert_eq!(summary.removed_lines, 1);
        assert_eq!(summary.line_hashes.len(), 3);
    }

    #[test]
    fn ignore_user_text_filters_local_command_noise() {
        assert!(should_ignore_user_text(
            "<local-command-caveat>ignore me</local-command-caveat>"
        ));
        assert!(should_ignore_user_text(
            "[Request interrupted by user for tool use]"
        ));
        assert!(!should_ignore_user_text("please implement this"));
    }

    #[test]
    fn parser_extracts_visible_messages_and_structured_writes() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("session.jsonl");
        fs::write(
            &path,
            [
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:00Z",
                    "sessionId": "claude-visible",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": "please update the matcher"}
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-visible",
                    "cwd": "/tmp/repo",
                    "message": {
                        "model": "claude-sonnet-4-5-20250929",
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "I will update the matcher."},
                            {"type": "tool_use", "id": "toolu_edit", "name": "Edit", "input": {
                                "file_path": "/tmp/repo/src/lib.rs",
                                "old_string": "old",
                                "new_string": "new"
                            }}
                        ]
                    }
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-visible",
                    "cwd": "/tmp/repo",
                    "message": {
                        "role": "user",
                        "content": [{"type": "tool_result", "tool_use_id": "toolu_edit", "content": "updated", "is_error": false}]
                    },
                    "toolUseResult": {
                        "filePath": "/tmp/repo/src/lib.rs",
                        "oldString": "old",
                        "newString": "new",
                        "originalFile": "old\n"
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )?;

        let parsed = parse_session_file(&path)?;
        assert_eq!(parsed.session_id, "claude-visible");
        assert_eq!(
            parsed.visible_messages.len(),
            2,
            "top-level user + assistant text should be visible"
        );
        assert_eq!(parsed.visible_messages[0].role, ClaudeMessageRole::User);
        assert_eq!(
            parsed.visible_messages[1].role,
            ClaudeMessageRole::Assistant
        );
        assert_eq!(
            parsed.model_name.as_deref(),
            Some("claude-sonnet-4-5-20250929")
        );
        assert_eq!(parsed.structured_writes.len(), 1);
        assert_eq!(parsed.structured_writes[0].abs_path, "/tmp/repo/src/lib.rs");
        Ok(())
    }

    #[test]
    fn discover_only_top_level_project_sessions() -> Result<()> {
        let tempdir = tempdir()?;
        let project_root = tempdir.path().join("project-a");
        let subagents = project_root.join("subagents");
        fs::create_dir_all(&subagents)?;
        fs::write(project_root.join("top.jsonl"), "")?;
        fs::write(subagents.join("agent.jsonl"), "")?;

        let planned = discover_top_level_jsonl_files(tempdir.path());

        assert_eq!(planned.len(), 1);
        assert!(planned[0].ends_with("top.jsonl"));
        Ok(())
    }
}
