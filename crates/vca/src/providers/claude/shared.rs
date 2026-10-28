use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::change_intel::line_hash::{diff_with_hashes, hash_line, hashes_for_text, line_count};
use crate::change_intel::types::{LineHashCount, LineSide, WriteMode};
use crate::db::session_store::{
    ContextInjectionRecord, MessageRecord, SessionEventRecord, ToolCallRecord, UsageRecord,
};
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
    pub key: Option<String>,
    pub is_compaction_summary: bool,
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
    /// One record per API response (`message.id`), de-duplicated.
    pub usage: Vec<UsageRecord>,
    pub structured_writes: Vec<ClaudeStructuredWrite>,
    pub tool_call_count: usize,
    pub tool_calls: Vec<ToolCallRecord>,
    pub events: Vec<SessionEventRecord>,
    pub context_injections: Vec<ContextInjectionRecord>,
    pub git_branch: Option<String>,
    pub client_version: Option<String>,
    pub entrypoint: Option<String>,
    pub title: Option<String>,
    pub agent_id: Option<String>,
    /// The session this one resumed or forked; its copied history was skipped.
    pub continued_from: Option<String>,
}

impl ParsedClaudeSession {
    pub(crate) fn message_records(&self) -> Vec<MessageRecord> {
        self.visible_messages
            .iter()
            .enumerate()
            .map(|(index, message)| MessageRecord {
                key: message
                    .key
                    .clone()
                    .unwrap_or_else(|| format!("msg:{index}")),
                ts: message.timestamp.clone(),
                role: message.role.as_str().to_string(),
                text: message.text.clone(),
                is_compaction_summary: message.is_compaction_summary,
            })
            .collect()
    }
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

#[derive(Debug, Default)]
struct ParseState {
    model_name: Option<String>,
    visible_messages: Vec<ClaudeVisibleMessage>,
    pending: Vec<PendingToolUse>,
    pending_by_id: HashMap<String, usize>,
    tool_call_count: usize,
    usage: Vec<UsageRecord>,
    tool_results: HashMap<String, ToolResultInfo>,
    events: Vec<SessionEventRecord>,
    context_injections: Vec<ContextInjectionRecord>,
    git_branch: Option<String>,
    client_version: Option<String>,
    entrypoint: Option<String>,
    title: Option<String>,
    agent_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ToolResultInfo {
    ts: Option<String>,
    output_chars: i64,
    is_error: bool,
    interrupted: bool,
    exit_code: Option<i64>,
    error_head: Option<String>,
    child_agent_id: Option<String>,
    child_agent_type: Option<String>,
}

const CLAUDE_EDIT_PARSER_NAME: &str = "claude_edit_v1";
const CLAUDE_WRITE_PARSER_NAME: &str = "claude_write_v1";

pub(crate) fn plan_session_files() -> Result<Vec<PathBuf>> {
    let home = crate::path_utils::require_agent_home()?;
    let root = home.join(".claude").join("projects");
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = discover_top_level_jsonl_files(&root);
    files.sort();
    Ok(files)
}

pub(crate) fn parse_session_file(path: &Path) -> Result<ParsedClaudeSession> {
    let text = std::fs::read_to_string(path)?;
    let source_file = path.to_string_lossy().to_string();
    let fallback_session_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string();
    // A resumed or forked session starts its file with a copy of the earlier
    // conversation, whose lines keep the earlier `sessionId`. Those calls are
    // already counted in that session, so they are skipped here. Only applies
    // when the file has lines of its own (subagent transcripts carry the parent's
    // id throughout and are named after the agent).
    let owns_lines = !fallback_session_id.is_empty()
        && text.contains(&format!("\"sessionId\":\"{fallback_session_id}\""));

    let mut session_id = fallback_session_id.clone();
    let mut continued_from: Option<String> = None;
    let mut session_cwd: Option<String> = None;
    let mut started_at: Option<String> = None;
    let mut ended_at: Option<String> = None;
    let mut state = ParseState::default();

    for raw in text.lines() {
        if raw.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if owns_lines
            && let Some(other) = parsed.get("sessionId").and_then(Value::as_str)
            && !other.trim().is_empty()
            && other != fallback_session_id
            && parsed.get("agentId").is_none()
        {
            continued_from.get_or_insert_with(|| other.to_string());
            continue;
        }

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

        capture_session_attributes(&parsed, &mut state);
        process_auxiliary_record(&parsed, outer_timestamp.as_deref(), &mut state);
        process_top_level_record(&parsed, &mut state);

        if parsed.get("type").and_then(Value::as_str) == Some("progress")
            && parsed
                .get("data")
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                == Some("agent_progress")
            && let Some(nested) = parsed.get("data").and_then(|value| value.get("message"))
        {
            process_nested_progress_message(nested, &parsed, &mut state);
        }
    }

    if session_id.is_empty() {
        session_id = fallback_session_id;
    }

    let tool_calls = build_tool_call_records(&state);
    let structured_writes = state
        .pending
        .into_iter()
        .filter_map(|tool| {
            build_structured_write(tool, &session_id, &source_file, session_cwd.as_deref())
        })
        .collect();

    let mut usage = state.usage;
    dedupe_usage_keep_last(&mut usage);

    Ok(ParsedClaudeSession {
        session_id,
        source_file,
        session_cwd,
        started_at,
        ended_at,
        model_name: state.model_name,
        visible_messages: state.visible_messages,
        usage,
        structured_writes,
        tool_call_count: state.tool_call_count,
        tool_calls,
        events: state.events,
        context_injections: state.context_injections,
        git_branch: state.git_branch,
        client_version: state.client_version,
        entrypoint: state.entrypoint,
        title: state.title,
        agent_id: state.agent_id,
        continued_from,
    })
}

/// Subagent transcripts stored next to a main session file
/// (`<project>/<session>/subagents/agent-*.jsonl`).
pub(crate) fn discover_subagent_files(main_session_file: &Path) -> Vec<PathBuf> {
    let Some(stem) = main_session_file.file_stem() else {
        return Vec::new();
    };
    let Some(parent) = main_session_file.parent() else {
        return Vec::new();
    };
    let dir = parent.join(stem).join("subagents");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .collect();
    files.sort();
    files
}

/// Agent metadata written next to a subagent transcript (`agent-<id>.meta.json`).
pub(crate) fn read_subagent_meta(subagent_file: &Path) -> Option<Value> {
    let stem = subagent_file.file_stem()?.to_str()?;
    let meta = subagent_file.with_file_name(format!("{stem}.meta.json"));
    let text = std::fs::read_to_string(meta).ok()?;
    serde_json::from_str(&text).ok()
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

fn process_top_level_record(parsed: &Value, state: &mut ParseState) {
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
            let model = extract_real_model_name(message);
            if let Some(found_model) = model.clone() {
                state.model_name = Some(found_model);
            }
            if let Some(mut usage) = extract_claude_usage(parsed, message, timestamp.clone(), model)
            {
                if usage.key.is_empty() {
                    usage.key = format!("anon:{}", state.usage.len());
                }
                state.usage.push(usage);
            }
            if parsed
                .get("isApiErrorMessage")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                state.events.push(SessionEventRecord {
                    ts: timestamp.clone(),
                    kind: "api_error".to_string(),
                    detail: extract_assistant_visible_text(message)
                        .map(|text| text.chars().take(200).collect()),
                });
            }
            if let Some(text) = extract_assistant_visible_text(message)
                && !text.trim().is_empty()
            {
                state.visible_messages.push(ClaudeVisibleMessage {
                    role: ClaudeMessageRole::Assistant,
                    text,
                    timestamp: timestamp.clone(),
                    key: record_key(parsed),
                    is_compaction_summary: false,
                });
            }
            register_tool_uses(message, timestamp, state);
        }
        "user" => {
            attach_tool_results(parsed, message, state);
            let is_compaction_summary = parsed
                .get("isCompactSummary")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(text) = extract_user_visible_text(message)
                && let Some(command) = slash_command_name(&text)
                && (command == "clear" || command == "compact")
            {
                state.events.push(SessionEventRecord {
                    ts: timestamp.clone(),
                    kind: if command == "clear" {
                        "clear".to_string()
                    } else {
                        "compaction".to_string()
                    },
                    detail: Some(format!("/{command}")),
                });
            }
            if !is_meta
                && let Some(text) = extract_user_visible_text(message)
                && !should_ignore_user_text(&text)
            {
                state.visible_messages.push(ClaudeVisibleMessage {
                    role: ClaudeMessageRole::User,
                    text,
                    timestamp,
                    key: record_key(parsed),
                    is_compaction_summary,
                });
            }
        }
        _ => {}
    }
}

fn extract_claude_usage(
    record: &Value,
    message: &Value,
    timestamp: Option<String>,
    model: Option<String>,
) -> Option<UsageRecord> {
    let usage = message.get("usage")?;
    let int = |key: &str| usage.get(key).and_then(Value::as_i64).unwrap_or(0);
    let key = message
        .get("id")
        .and_then(Value::as_str)
        .map(|id| format!("msg:{id}"))
        .or_else(|| {
            record
                .get("requestId")
                .and_then(Value::as_str)
                .map(|id| format!("req:{id}"))
        })
        .or_else(|| record_key(record).map(|uuid| format!("uuid:{uuid}")))
        .unwrap_or_default();
    let mut out = UsageRecord::new(key);
    out.ts = timestamp;
    out.model = model;
    out.input_tokens = int("input_tokens");
    out.cache_write_tokens = int("cache_creation_input_tokens");
    out.cache_read_tokens = int("cache_read_input_tokens");
    out.cache_write_1h_tokens = usage
        .get("cache_creation")
        .and_then(|creation| creation.get("ephemeral_1h_input_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    out.output_tokens = int("output_tokens");
    out.reasoning_tokens = usage
        .get("output_tokens_details")
        .and_then(|details| details.get("thinking_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    // Anthropic bills thinking as part of output_tokens.
    out.reasoning_in_output = true;
    out.service_tier = usage
        .get("service_tier")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    out.stop_reason = message
        .get("stop_reason")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some(out)
}

fn dedupe_usage_keep_last(usage: &mut Vec<UsageRecord>) {
    let mut seen = std::collections::HashSet::new();
    let mut kept: Vec<UsageRecord> = Vec::with_capacity(usage.len());
    for record in usage.drain(..).rev() {
        if seen.insert(record.key.clone()) {
            kept.push(record);
        }
    }
    kept.reverse();
    *usage = kept;
}

fn record_key(record: &Value) -> Option<String> {
    record
        .get("uuid")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn slash_command_name(text: &str) -> Option<String> {
    let start = text.find("<command-name>/")? + "<command-name>/".len();
    let rest = &text[start..];
    let end = rest.find('<').unwrap_or(rest.len());
    Some(rest[..end].trim().to_string())
}

fn capture_session_attributes(record: &Value, state: &mut ParseState) {
    let text = |key: &str| {
        record
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
    };
    if let Some(branch) = text("gitBranch") {
        state.git_branch = Some(branch);
    }
    if let Some(version) = text("version") {
        state.client_version = Some(version);
    }
    if state.entrypoint.is_none() {
        state.entrypoint = text("entrypoint");
    }
    if state.agent_id.is_none() {
        state.agent_id = text("agentId");
    }
}

/// Records without a chat message: attachments (injected context), system
/// markers (compaction) and titles.
fn process_auxiliary_record(record: &Value, timestamp: Option<&str>, state: &mut ParseState) {
    let Some(kind) = record.get("type").and_then(Value::as_str) else {
        return;
    };
    match kind {
        "attachment" => {
            let Some(attachment) = record.get("attachment") else {
                return;
            };
            let Some(attachment_type) = attachment.get("type").and_then(Value::as_str) else {
                return;
            };
            if attachment_type == "skill_listing" {
                let names = skill_listing_names(attachment);
                if !names.is_empty() {
                    let total = attachment.to_string().chars().count() as i64;
                    let count = names.len() as i64;
                    let share = total / count;
                    let key = record_key(record)
                        .unwrap_or_else(|| format!("att:{}", state.context_injections.len()));
                    for (index, name) in names.into_iter().enumerate() {
                        state.context_injections.push(ContextInjectionRecord {
                            key: format!("{key}:skill:{index}"),
                            ts: timestamp.map(ToOwned::to_owned),
                            kind: "skill_listing".to_string(),
                            source: Some(name),
                            chars: share + if index == 0 { total % count } else { 0 },
                        });
                    }
                    return;
                }
            }
            let (category, source, chars) = classify_attachment(attachment_type, attachment);
            let key = record_key(record)
                .unwrap_or_else(|| format!("att:{}", state.context_injections.len()));
            state.context_injections.push(ContextInjectionRecord {
                key,
                ts: timestamp.map(ToOwned::to_owned),
                kind: category.to_string(),
                source,
                chars,
            });
        }
        "system" => {
            let subtype = record.get("subtype").and_then(Value::as_str).unwrap_or("");
            if subtype == "compact_boundary" || subtype.contains("compact") {
                state.events.push(SessionEventRecord {
                    ts: timestamp.map(ToOwned::to_owned),
                    kind: "compaction".to_string(),
                    detail: record
                        .get("compactMetadata")
                        .and_then(|meta| meta.get("trigger"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                });
            }
        }
        "summary" | "custom-title" | "ai-title" => {
            if let Some(title) = record
                .get("summary")
                .or_else(|| record.get("customTitle"))
                .or_else(|| record.get("title"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                state.title = Some(title.chars().take(200).collect());
            }
        }
        _ => {}
    }
}

fn skill_listing_names(attachment: &Value) -> Vec<String> {
    ["skills", "items", "addedSkills", "addedNames"]
        .iter()
        .find_map(|key| attachment.get(key).and_then(Value::as_array))
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let name = item.as_str().or_else(|| {
                        item.get("name")
                            .or_else(|| item.get("id"))
                            .and_then(Value::as_str)
                    })?;
                    let name = name.trim();
                    (!name.is_empty()
                        && name.len() <= 96
                        && name
                            .chars()
                            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
                    .then(|| name.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn classify_attachment(
    attachment_type: &str,
    attachment: &Value,
) -> (&'static str, Option<String>, i64) {
    let json_chars = || attachment.to_string().chars().count() as i64;
    match attachment_type {
        "instructions" | "nested_memory" | "memory" => {
            let files = attachment.get("files").and_then(Value::as_array);
            let (paths, chars) = files
                .map(|files| {
                    let paths: Vec<String> = files
                        .iter()
                        .filter_map(|file| file.get("path").and_then(Value::as_str))
                        .map(ToOwned::to_owned)
                        .collect();
                    let chars = files
                        .iter()
                        .filter_map(|file| file.get("content").and_then(Value::as_str))
                        .map(|content| content.chars().count() as i64)
                        .sum::<i64>();
                    (paths, chars)
                })
                .unwrap_or_default();
            let source = (!paths.is_empty()).then(|| paths.join("\n"));
            let chars = if chars > 0 { chars } else { json_chars() };
            ("instructions", source, chars)
        }
        "prompt_snapshot" => {
            let chars = attachment
                .get("systemPrompt")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|part| part.chars().count() as i64)
                        .sum()
                })
                .unwrap_or_else(json_chars);
            ("system_prompt", None, chars)
        }
        "deferred_tools_delta" | "deferred_tools_record" | "tools_delta" => {
            ("tool_definitions", None, json_chars())
        }
        "skill_listing" => ("skill_listing", None, json_chars()),
        "mcp_instructions_delta" | "mcp_instructions" => {
            let source = attachment
                .get("addedNames")
                .and_then(Value::as_array)
                .map(|names| {
                    names
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(",")
                });
            ("mcp_instructions", source, json_chars())
        }
        "agent_listing_delta" => ("agent_listing", None, json_chars()),
        "environment"
        | "date"
        | "model"
        | "session_context"
        | "credential_org"
        | "remote_session_change" => ("environment", None, json_chars()),
        kind if kind.ends_with("_reminder") || kind.contains("mode") => {
            ("reminder", Some(kind.to_string()), json_chars())
        }
        other => ("attachment_other", Some(other.to_string()), json_chars()),
    }
}

fn tool_target(tool_name: &str, input: &Value) -> Option<String> {
    let text = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
    };
    match tool_name {
        "Skill" | "skill" => text("name").or_else(|| text("skill")),
        "Task" | "Agent" => text("description").or_else(|| text("subagent_type")),
        "Grep" | "Glob" => match (text("pattern"), text("path")) {
            (Some(pattern), Some(path)) => Some(format!("{path} :: {pattern}")),
            (pattern, path) => pattern.or(path),
        },
        _ => text("file_path")
            .or_else(|| text("notebook_path"))
            .or_else(|| text("path"))
            .or_else(|| text("command"))
            .or_else(|| text("url"))
            .or_else(|| text("query"))
            .or_else(|| text("pattern"))
            .or_else(|| text("cmd")),
    }
}

fn tool_result_content_text(item: &Value) -> String {
    match item.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn parse_exit_code(text: &str) -> Option<i64> {
    let trimmed = text.trim_start();
    let rest = trimmed.strip_prefix("Exit code ")?;
    rest.split_whitespace().next()?.parse().ok()
}

fn build_tool_call_records(state: &ParseState) -> Vec<ToolCallRecord> {
    state
        .pending
        .iter()
        .map(|tool| {
            let result = state.tool_results.get(&tool.call_id);
            let input_chars = tool.input.to_string().chars().count() as i64;
            let status = match result {
                None => "unknown",
                Some(info) if info.interrupted => "interrupted",
                Some(info) if info.is_error => "error",
                Some(_) => "ok",
            };
            let is_shell = tool.tool_name == "Bash";
            let exit_code = result.and_then(|info| {
                info.exit_code
                    .or_else(|| (is_shell && !info.is_error && !info.interrupted).then_some(0))
            });
            ToolCallRecord {
                key: tool.call_id.clone(),
                ts: tool.timestamp.clone(),
                ended_ts: result.and_then(|info| info.ts.clone()),
                tool_name: tool.tool_name.clone(),
                target: tool_target(&tool.tool_name, &tool.input),
                input_chars,
                output_chars: result.map(|info| info.output_chars).unwrap_or(0),
                output_in_context: true,
                status: status.to_string(),
                exit_code,
                error_head: result.and_then(|info| info.error_head.clone()),
                child_session_id: result.and_then(|info| info.child_agent_id.clone()),
                agent_type: result
                    .and_then(|info| info.child_agent_type.clone())
                    .or_else(|| {
                        tool.input
                            .get("subagent_type")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                    }),
            }
        })
        .collect()
}

fn process_nested_progress_message(nested: &Value, outer: &Value, state: &mut ParseState) {
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
        "assistant" => register_tool_uses(message, timestamp, state),
        "user" => attach_tool_results(outer, message, state),
        _ => {}
    }
}

fn register_tool_uses(message: &Value, timestamp: Option<String>, state: &mut ParseState) {
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

        state.tool_call_count += 1;
        let pending_index = state.pending.len();
        state.pending.push(PendingToolUse {
            timestamp: timestamp.clone(),
            call_id: call_id.to_string(),
            tool_name: tool_name.to_string(),
            input: item.get("input").cloned().unwrap_or(Value::Null),
            result_payload: None,
            result_error: false,
        });
        state
            .pending_by_id
            .insert(call_id.to_string(), pending_index);
    }
}

fn attach_tool_results(outer: &Value, message: &Value, state: &mut ParseState) {
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
        let Some(index) = state.pending_by_id.get(tool_use_id).copied() else {
            continue;
        };
        let Some(tool) = state.pending.get_mut(index) else {
            continue;
        };

        tool.result_error = item
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        tool.result_payload = outer.get("toolUseResult").cloned();

        let content_text = tool_result_content_text(item);
        let payload = outer.get("toolUseResult");
        let interrupted = payload
            .and_then(|value| value.get("interrupted"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let exit_code = parse_exit_code(&content_text).or_else(|| {
            payload
                .and_then(|value| value.get("code"))
                .and_then(Value::as_i64)
        });
        let is_error = tool.result_error;
        let error_head = (is_error || exit_code.is_some_and(|code| code != 0))
            .then(|| content_text.chars().take(600).collect::<String>());
        let info = ToolResultInfo {
            ts: outer
                .get("timestamp")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            output_chars: content_text.chars().count() as i64,
            is_error,
            interrupted,
            exit_code,
            error_head,
            child_agent_id: payload
                .and_then(|value| value.get("agentId"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            child_agent_type: payload
                .and_then(|value| value.get("agentType"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        };
        let call_id = tool.call_id.clone();
        state.tool_results.insert(call_id, info);
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
        should_ignore_user_text, skill_listing_names, structured_patch_summary,
    };
    use anyhow::Result;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn listing_names_are_bounded_and_explicit() {
        assert_eq!(
            skill_listing_names(&json!({"skills": ["review", {"name": "test-run"}, "../unsafe"]})),
            vec!["review", "test-run"]
        );
    }

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

    /// `claude --resume` writes a new file that starts with a copy of the earlier
    /// session's lines (still carrying its `sessionId`).
    #[test]
    fn resumed_session_skips_copied_history() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("new-session.jsonl");
        let assistant = |session: &str, id: &str, ts: &str, output: i64| {
            json!({
                "type": "assistant", "timestamp": ts, "sessionId": session, "cwd": "/tmp/repo",
                "uuid": format!("u-{id}"),
                "message": {"id": id, "model": "claude-opus-5-5", "role": "assistant",
                    "usage": {"input_tokens": 10, "output_tokens": output},
                    "content": [{"type": "text", "text": format!("reply {id}")}]}
            })
            .to_string()
        };
        let user = |session: &str, ts: &str, text: &str| {
            json!({"type": "user", "timestamp": ts, "sessionId": session, "cwd": "/tmp/repo",
                "message": {"role": "user", "content": text}})
            .to_string()
        };
        fs::write(
            &path,
            [
                user("old-session", "2026-09-02T16:33:06Z", "first ask"),
                assistant("old-session", "m1", "2026-09-02T16:33:10Z", 100),
                user("new-session", "2026-09-03T08:00:00Z", "continue"),
                assistant("new-session", "m2", "2026-09-03T08:00:05Z", 7),
            ]
            .join("\n"),
        )?;

        let parsed = parse_session_file(&path)?;
        assert_eq!(parsed.session_id, "new-session");
        assert_eq!(parsed.continued_from.as_deref(), Some("old-session"));
        assert_eq!(
            parsed.usage.len(),
            1,
            "copied call is counted in old-session"
        );
        assert_eq!(parsed.usage[0].output_tokens, 7);
        assert_eq!(parsed.visible_messages.len(), 2);
        assert_eq!(parsed.started_at.as_deref(), Some("2026-09-03T08:00:00Z"));
        Ok(())
    }

    #[test]
    fn parser_extracts_usage_events() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("usage.jsonl");
        fs::write(
            &path,
            json!({
                "type": "assistant",
                "timestamp": "2026-04-22T09:00:01Z",
                "sessionId": "claude-usage",
                "cwd": "/tmp/repo",
                "message": {
                    "model": "claude-opus-4-6",
                    "role": "assistant",
                    "usage": {
                        "input_tokens": 3,
                        "cache_creation_input_tokens": 1390,
                        "cache_read_input_tokens": 14316,
                        "output_tokens": 2
                    },
                    "content": [{"type": "text", "text": "done"}]
                }
            })
            .to_string(),
        )?;

        let parsed = parse_session_file(&path)?;

        assert_eq!(parsed.usage.len(), 1);
        let usage = &parsed.usage[0];
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.cache_write_tokens, 1390);
        assert_eq!(usage.cache_read_tokens, 14316);
        assert_eq!(usage.output_tokens, 2);
        assert_eq!(usage.total_tokens(), 15711);
        assert_eq!(usage.ts.as_deref(), Some("2026-04-22T09:00:01Z"));
        assert_eq!(usage.model.as_deref(), Some("claude-opus-4-6"));
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
