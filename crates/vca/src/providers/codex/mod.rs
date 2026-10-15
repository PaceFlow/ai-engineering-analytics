use anyhow::Result;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use super::utils::diff_line_counts;
use crate::db::session_store::{
    self, AcceptedChangeRecord, AgentKind, ContextInjectionRecord, MessageRecord, RateLimitRecord,
    SessionEventRecord, SessionRecord, ToolCallRecord, UsageRecord,
};
use crate::ingest_progress::IngestProgressObserver;

pub fn plan_session_files() -> Result<Vec<PathBuf>> {
    let sessions_root = codex_sessions_dir()?;
    if !sessions_root.exists() {
        return Ok(Vec::new());
    }

    let mut files = find_jsonl_files(&sessions_root);
    files.sort();
    Ok(files)
}

pub fn ingest_planned_sessions(
    db: &Connection,
    session_files: &[PathBuf],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    let mut total_rows = 0;
    for session_file in session_files {
        if verbose {
            eprint!("  {:?} ... ", session_file);
        }
        match ingest_session(session_file, db) {
            Ok(0) => {
                if verbose {
                    eprintln!("skipped (already ingested or empty)");
                }
            }
            Ok(n) => {
                if verbose {
                    eprintln!("wrote {} rows", n);
                }
                total_rows += n;
            }
            Err(e) => {
                eprintln!("Warning: skipping {:?}: {}", session_file, e);
            }
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(&session_file.to_string_lossy());
        }
    }

    Ok(total_rows)
}

fn codex_sessions_dir() -> Result<PathBuf> {
    Ok(crate::path_utils::codex_home()?.join("sessions"))
}

/// Recursively collect all .jsonl files under `root`.
fn find_jsonl_files(root: &PathBuf) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(find_jsonl_files(&path));
            } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                files.push(path);
            }
        }
    }
    files
}

// ── Serde types for Codex JSONL lines ──────────────────────────────────────

/// Outer wrapper — every JSONL line has this shape.
#[derive(Deserialize)]
struct Line {
    #[serde(rename = "type")]
    kind: String,
    timestamp: Option<String>,
    payload: Value,
}

/// Payload fields when kind == "session_meta".
#[derive(Deserialize)]
struct SessionMetaPayload {
    id: String,
    timestamp: Option<String>,
    cwd: Option<String>,
    model_provider: Option<String>,
}

// ── Session ingestion ───────────────────────────────────────────────────────

fn ingest_session(path: &PathBuf, db: &Connection) -> Result<usize> {
    let source_key = path.to_string_lossy().to_string();
    let fingerprint = session_store::file_fingerprint(path).unwrap_or_default();
    if !session_store::source_needs_ingest(db, "codex", &source_key, &fingerprint)? {
        return Ok(0);
    }
    let Some(record) = parse_session(path)? else {
        session_store::mark_source_ingested(db, "codex", &source_key, &fingerprint, &[])?;
        return Ok(0);
    };
    let written = if record.is_empty() {
        0
    } else {
        session_store::store_session_record(db, &record)?
    };
    session_store::mark_source_ingested(
        db,
        "codex",
        &source_key,
        &fingerprint,
        &[record.session_id.as_str()],
    )?;
    Ok(written)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CodexTotals {
    input: i64,
    cached: i64,
    output: i64,
    reasoning: i64,
}

impl CodexTotals {
    fn from_value(value: &Value) -> Option<Self> {
        let int = |key: &str| value.get(key).and_then(Value::as_i64).unwrap_or(0);
        Some(Self {
            input: int("input_tokens"),
            cached: int("cached_input_tokens"),
            output: int("output_tokens"),
            reasoning: int("reasoning_output_tokens"),
        })
    }

    fn minus(self, other: Self) -> Self {
        Self {
            input: self.input - other.input,
            cached: self.cached - other.cached,
            output: self.output - other.output,
            reasoning: self.reasoning - other.reasoning,
        }
    }

    fn is_positive(self) -> bool {
        self.input > 0 || self.output > 0 || self.cached > 0
    }

    fn is_valid_delta(self) -> bool {
        self.input >= 0 && self.cached >= 0 && self.output >= 0 && self.reasoning >= 0
    }
}

struct PendingCall {
    index: usize,
}

/// A code-mode `exec` call (Codex CLI 0.160+): a script that calls inner tools.
/// The inner calls arrive as `item_completed` events; the script's input and output
/// are what enter the model context, so they are attributed to its first inner call.
struct OpenWrapper {
    call_id: String,
    ts: Option<String>,
    input_chars: i64,
    first_inner: Option<usize>,
}

/// Parse one Codex rollout file into a normalized session record.
fn parse_session(path: &PathBuf) -> Result<Option<SessionRecord>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let first_line = match lines.next() {
        Some(Ok(line)) if !line.trim().is_empty() => line,
        _ => return Ok(None),
    };
    let first: Line = match serde_json::from_str(&first_line) {
        Ok(line) => line,
        Err(_) => return Ok(None),
    };
    if first.kind != "session_meta" {
        return Ok(None);
    }
    let first_outer_ts = first.timestamp.clone();
    let meta_value = first.payload.clone();
    let meta: SessionMetaPayload = match serde_json::from_value(first.payload) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };

    let session_start_ts = meta.timestamp.clone().or(first_outer_ts);
    let mut record = SessionRecord::new("codex", meta.id.clone());
    record.source_path = Some(path.to_string_lossy().to_string());
    record.project_path = meta.cwd.clone();
    record.started_at = session_start_ts.clone();
    record.ended_at = session_start_ts.clone();
    record.git_branch = meta_value
        .get("git")
        .and_then(|git| git.get("branch"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    record.client_version = meta_value
        .get("cli_version")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    record.entrypoint = meta_value
        .get("originator")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    apply_session_source(&meta_value, &mut record);
    if let Some(instructions) = meta_value.get("base_instructions") {
        let chars = match instructions {
            Value::String(text) => text.chars().count() as i64,
            Value::Object(map) => map
                .get("text")
                .and_then(Value::as_str)
                .map(|text| text.chars().count() as i64)
                .unwrap_or_else(|| instructions.to_string().chars().count() as i64),
            other => other.to_string().chars().count() as i64,
        };
        record.context_injections.push(ContextInjectionRecord {
            key: "base_instructions".to_string(),
            ts: session_start_ts.clone(),
            kind: "system_prompt".to_string(),
            source: None,
            chars,
        });
    }

    let mut pending_reads: HashMap<String, String> = HashMap::new();
    let mut file_cache: HashMap<String, String> = HashMap::new();
    let mut pending_calls: HashMap<String, PendingCall> = HashMap::new();
    let mut current_model: Option<String> = None;
    let mut previous_totals = CodexTotals::default();
    let mut context_window: Option<i64> = None;
    // Newer rollouts write exact per-response `token_usage_record` lines next to the
    // cumulative `token_count` events; the records win when present (they also cover
    // compaction calls), and token_count deltas are only the fallback.
    let mut count_usage: Vec<UsageRecord> = Vec::new();
    let mut saw_usage_records = false;
    let mut wrapper: Option<OpenWrapper> = None;
    let mut line_no = 0usize;

    for line_result in lines {
        line_no += 1;
        let raw = line_result?;
        if raw.trim().is_empty() {
            continue;
        }
        let line: Line = match serde_json::from_str(&raw) {
            Ok(line) => line,
            Err(_) => continue,
        };
        let ts = line.timestamp.clone().or_else(|| session_start_ts.clone());
        if let Some(ts) = ts.as_deref()
            && record.ended_at.as_deref().is_none_or(|end| ts > end)
        {
            record.ended_at = Some(ts.to_string());
        }
        let payload_type = line
            .payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");

        match line.kind.as_str() {
            "turn_context" => {
                if let Some(model) = line.payload.get("model").and_then(Value::as_str) {
                    if current_model
                        .as_deref()
                        .is_some_and(|current| current != model)
                    {
                        record.events.push(SessionEventRecord {
                            ts: ts.clone(),
                            kind: "model_change".to_string(),
                            detail: Some(format!(
                                "{} -> {model}",
                                current_model.as_deref().unwrap_or("?")
                            )),
                        });
                    }
                    current_model = Some(model.to_string());
                }
            }
            "compacted" => {
                record.events.push(SessionEventRecord {
                    ts: ts.clone(),
                    kind: "compaction".to_string(),
                    detail: None,
                });
            }
            "event_msg" => match payload_type {
                "token_count" => {
                    let info = line.payload.get("info");
                    if let Some(window) = info
                        .and_then(|info| info.get("model_context_window"))
                        .and_then(Value::as_i64)
                    {
                        context_window = Some(window);
                    }
                    if let Some(totals) = info
                        .and_then(|info| info.get("total_token_usage"))
                        .and_then(CodexTotals::from_value)
                    {
                        let delta = totals.minus(previous_totals);
                        let delta = if delta.is_valid_delta() {
                            delta
                        } else {
                            // Totals reset (e.g. after compaction): fall back to the last call.
                            info.and_then(|info| info.get("last_token_usage"))
                                .and_then(CodexTotals::from_value)
                                .unwrap_or_default()
                        };
                        previous_totals = totals;
                        if delta.is_positive() {
                            let mut usage = UsageRecord::new(format!("tc:{line_no}"));
                            usage.ts = ts.clone();
                            usage.model = current_model.clone();
                            usage.input_tokens = (delta.input - delta.cached).max(0);
                            usage.cache_read_tokens = delta.cached;
                            usage.output_tokens = delta.output;
                            usage.reasoning_tokens = delta.reasoning;
                            usage.reasoning_in_output = true;
                            usage.context_window = context_window;
                            usage.provider_route = meta.model_provider.clone();
                            count_usage.push(usage);
                        }
                    }
                    if let (Some(ts), Some(limits)) =
                        (ts.as_deref(), line.payload.get("rate_limits"))
                    {
                        push_rate_limits(&mut record, ts, limits);
                    }
                }
                "item_completed" => {
                    if let Some(item) = line.payload.get("item") {
                        push_completed_item(
                            &mut record,
                            &mut wrapper,
                            item,
                            &line.payload,
                            ts.clone(),
                            line_no,
                        );
                    }
                }
                "user_message" => {
                    if let Some(message) = line.payload.get("message").and_then(Value::as_str) {
                        record.messages.push(MessageRecord {
                            key: format!("line:{line_no}"),
                            ts: ts.clone(),
                            role: "user".to_string(),
                            text: message.to_string(),
                            is_compaction_summary: false,
                        });
                    }
                }
                "turn_aborted" => record.events.push(SessionEventRecord {
                    ts: ts.clone(),
                    kind: "turn_aborted".to_string(),
                    detail: line
                        .payload
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                }),
                "context_compacted" => {}
                "task_complete" => record.events.push(SessionEventRecord {
                    ts: ts.clone(),
                    kind: "task_complete".to_string(),
                    detail: None,
                }),
                _ => {}
            },
            "response_item" => {
                let role = line
                    .payload
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                match (payload_type, role) {
                    ("message", "assistant") => {
                        let text = content_text(&line.payload, "output_text");
                        if !text.trim().is_empty() {
                            record.messages.push(MessageRecord {
                                key: format!("line:{line_no}"),
                                ts: ts.clone(),
                                role: "assistant".to_string(),
                                text,
                                is_compaction_summary: false,
                            });
                        }
                    }
                    ("message", "user") | ("message", "developer") => {
                        let text = content_text(&line.payload, "input_text");
                        if let Some(kind) = injected_context_kind(&text) {
                            record.context_injections.push(ContextInjectionRecord {
                                key: format!("line:{line_no}"),
                                ts: ts.clone(),
                                kind: kind.to_string(),
                                source: (kind == "instructions").then(|| "AGENTS.md".to_string()),
                                chars: text.chars().count() as i64,
                            });
                        } else if role == "developer" && !text.is_empty() {
                            record.context_injections.push(ContextInjectionRecord {
                                key: format!("line:{line_no}"),
                                ts: ts.clone(),
                                kind: "system_prompt".to_string(),
                                source: Some("developer".to_string()),
                                chars: text.chars().count() as i64,
                            });
                        }
                    }
                    ("custom_tool_call", _)
                        if line.payload.get("name").and_then(Value::as_str) == Some("exec") =>
                    {
                        wrapper = Some(OpenWrapper {
                            call_id: line
                                .payload
                                .get("call_id")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                                .unwrap_or_else(|| format!("line:{line_no}")),
                            ts: ts.clone(),
                            input_chars: tool_arguments(&line.payload).chars().count() as i64,
                            first_inner: None,
                        });
                    }
                    ("custom_tool_call_output", _)
                        if wrapper.as_ref().is_some_and(|open| {
                            line.payload.get("call_id").and_then(Value::as_str)
                                == Some(open.call_id.as_str())
                        }) =>
                    {
                        let open = wrapper.take().expect("checked above");
                        let output_chars = output_text(&line.payload).chars().count() as i64;
                        match open
                            .first_inner
                            .and_then(|index| record.tool_calls.get_mut(index))
                        {
                            Some(call) => call.output_chars = output_chars,
                            // A script that called no tools (pure computation, waits).
                            None => record.tool_calls.push(ToolCallRecord {
                                key: open.call_id,
                                ts: open.ts,
                                ended_ts: ts.clone(),
                                tool_name: "exec".to_string(),
                                input_chars: open.input_chars,
                                output_chars,
                                output_in_context: true,
                                status: "ok".to_string(),
                                ..ToolCallRecord::default()
                            }),
                        }
                    }
                    ("function_call", _) | ("custom_tool_call", _) | ("local_shell_call", _) => {
                        let name = line
                            .payload
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or(if payload_type == "local_shell_call" {
                                "local_shell"
                            } else {
                                "unknown"
                            })
                            .to_string();
                        let call_id = line
                            .payload
                            .get("call_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| format!("line:{line_no}"));
                        let arguments = tool_arguments(&line.payload);
                        let command = command_from_arguments(&arguments);
                        if name == "exec_command"
                            && let Some(cmd) = command.as_deref()
                        {
                            let trimmed = cmd.trim();
                            if trimmed.starts_with("cat ") && !trimmed.contains('>') {
                                let file_path = trimmed[4..].trim().to_string();
                                if !file_path.is_empty() {
                                    pending_reads.insert(call_id.clone(), file_path);
                                }
                            }
                        }
                        for change in accepted_changes_for_call(&name, &arguments, &file_cache) {
                            record.accepted_changes.push(AcceptedChangeRecord {
                                abs_path: change.0,
                                lines_added: change.1,
                                lines_removed: change.2,
                                ts: ts.clone(),
                            });
                        }
                        let target = command.clone().or_else(|| patch_target(&arguments));
                        pending_calls.insert(
                            call_id.clone(),
                            PendingCall {
                                index: record.tool_calls.len(),
                            },
                        );
                        record.tool_calls.push(ToolCallRecord {
                            key: call_id,
                            ts: ts.clone(),
                            tool_name: name,
                            target,
                            input_chars: arguments.chars().count() as i64,
                            output_in_context: true,
                            status: "unknown".to_string(),
                            ..ToolCallRecord::default()
                        });
                    }
                    ("function_call_output", _) | ("custom_tool_call_output", _) => {
                        let call_id = line
                            .payload
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let output = output_text(&line.payload);
                        if let Some(file_path) = pending_reads.remove(call_id) {
                            let content = match output.find("\nOutput:\n") {
                                Some(pos) => output[pos + "\nOutput:\n".len()..].to_string(),
                                None => output.clone(),
                            };
                            file_cache.insert(file_path, content);
                        }
                        if let Some(pending) = pending_calls.remove(call_id)
                            && let Some(call) = record.tool_calls.get_mut(pending.index)
                        {
                            let exit_code = output_exit_code(&line.payload, &output);
                            call.ended_ts = ts.clone();
                            call.output_chars = output.chars().count() as i64;
                            call.exit_code = exit_code;
                            call.status = match exit_code {
                                Some(0) => "ok",
                                Some(_) => "error",
                                None => "ok",
                            }
                            .to_string();
                            if exit_code.is_some_and(|code| code != 0) {
                                let body = match output.find("\nOutput:\n") {
                                    Some(pos) => &output[pos + "\nOutput:\n".len()..],
                                    None => output.as_str(),
                                };
                                call.error_head = Some(body.chars().take(600).collect());
                            }
                        }
                    }
                    ("web_search_call", _) => {
                        record.tool_calls.push(ToolCallRecord {
                            key: format!("line:{line_no}"),
                            ts: ts.clone(),
                            tool_name: "web_search_call".to_string(),
                            target: line
                                .payload
                                .get("action")
                                .and_then(|action| action.get("query"))
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned),
                            output_in_context: true,
                            status: "ok".to_string(),
                            ..ToolCallRecord::default()
                        });
                    }
                    _ => {}
                }
            }
            "token_usage_record" => {
                if let Some(per_call) = line.payload.get("usage").filter(|usage| usage.is_object())
                {
                    saw_usage_records = true;
                    let int = |key: &str| per_call.get(key).and_then(Value::as_i64).unwrap_or(0);
                    let (input, cached) = (int("input_tokens"), int("cached_input_tokens"));
                    let cache_write = int("cache_write_input_tokens");
                    let key = line
                        .payload
                        .get("response_id")
                        .and_then(Value::as_str)
                        .map(|id| format!("resp:{id}"))
                        .unwrap_or_else(|| format!("tur:{line_no}"));
                    let mut usage = UsageRecord::new(key);
                    usage.ts = ts.clone();
                    usage.model = current_model.clone();
                    usage.input_tokens = (input - cached - cache_write).max(0);
                    usage.cache_read_tokens = cached;
                    usage.cache_write_tokens = cache_write;
                    usage.output_tokens = int("output_tokens");
                    usage.reasoning_tokens = int("reasoning_output_tokens");
                    usage.reasoning_in_output = true;
                    usage.context_window = context_window;
                    usage.provider_route = meta.model_provider.clone();
                    record.usage.push(usage);
                }
            }
            _ => {}
        }
    }

    if !saw_usage_records {
        record.usage.extend(count_usage);
    }
    // Rollouts from Codex CLI 0.160+ record prompts as `UserMessage` items; drop the
    // legacy `user_message` events if a file somehow carries both, so turns are not doubled.
    if record
        .messages
        .iter()
        .any(|message| message.key.starts_with("item:"))
    {
        record
            .messages
            .retain(|message| message.role != "user" || message.key.starts_with("item:"));
    }
    record.model = record.primary_model_from_usage().or(current_model);
    Ok(Some(record))
}

/// One `item_completed` event (Codex CLI 0.160+): user prompts and the inner tool calls
/// of code-mode `exec` scripts. Agent messages and reasoning are already covered by
/// `response_item` lines, and `Extension` items mirror direct function calls.
fn push_completed_item(
    record: &mut SessionRecord,
    wrapper: &mut Option<OpenWrapper>,
    item: &Value,
    payload: &Value,
    ts: Option<String>,
    line_no: usize,
) {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    let ms_ts = |key: &str| {
        payload
            .get(key)
            .and_then(Value::as_i64)
            .and_then(chrono::DateTime::from_timestamp_millis)
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    };
    let started = ms_ts("started_at_ms").or_else(|| ts.clone());
    let ended = ms_ts("completed_at_ms").or_else(|| ts.clone());
    let key = item
        .get("id")
        .and_then(Value::as_str)
        .map(|id| format!("item:{id}"))
        .unwrap_or_else(|| format!("item:line:{line_no}"));

    let mut call = match item_type {
        "UserMessage" => {
            let text = item
                .get("content")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(|part| part.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_default();
            if !text.trim().is_empty() {
                record.messages.push(MessageRecord {
                    key,
                    ts: started,
                    role: "user".to_string(),
                    text,
                    is_compaction_summary: false,
                });
            }
            return;
        }
        "CommandExecution" => {
            let target = item
                .get("parsed_cmd")
                .and_then(Value::as_array)
                .and_then(|parsed| parsed.first())
                .and_then(|first| first.get("cmd"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    // ["pwsh.exe", "-Command", "<script>"]: the script is the last part.
                    item.get("command")
                        .and_then(Value::as_array)
                        .and_then(|parts| parts.last())
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                });
            let exit_code = item.get("exit_code").and_then(Value::as_i64);
            let failed = exit_code.is_some_and(|code| code != 0);
            let error_head = failed.then(|| {
                let stderr = item.get("stderr").and_then(Value::as_str).unwrap_or("");
                let text = if stderr.trim().is_empty() {
                    item.get("aggregated_output")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                } else {
                    stderr
                };
                text.chars().take(600).collect()
            });
            ToolCallRecord {
                key,
                ts: started,
                ended_ts: ended,
                tool_name: "exec_command".to_string(),
                target,
                output_in_context: true,
                status: if failed { "error" } else { "ok" }.to_string(),
                exit_code,
                error_head,
                ..ToolCallRecord::default()
            }
        }
        "FileChange" => {
            let completed = item.get("status").and_then(Value::as_str) == Some("completed");
            let changes = item.get("changes").and_then(Value::as_object);
            if completed {
                for (path, change) in changes.into_iter().flatten() {
                    let (added, removed) = file_change_counts(change);
                    if added > 0 || removed > 0 {
                        record.accepted_changes.push(AcceptedChangeRecord {
                            abs_path: path.clone(),
                            lines_added: added,
                            lines_removed: removed,
                            ts: ended.clone(),
                        });
                    }
                }
            }
            ToolCallRecord {
                key,
                ts: started,
                ended_ts: ended,
                tool_name: "apply_patch".to_string(),
                target: changes.and_then(|changes| changes.keys().next().cloned()),
                output_in_context: true,
                status: if completed { "ok" } else { "error" }.to_string(),
                ..ToolCallRecord::default()
            }
        }
        "ImageView" => ToolCallRecord {
            key,
            ts: started,
            ended_ts: ended,
            tool_name: "view_image".to_string(),
            target: item
                .get("path")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            output_in_context: true,
            status: "ok".to_string(),
            ..ToolCallRecord::default()
        },
        _ => return,
    };

    let index = record.tool_calls.len();
    // Long-running commands can complete after their script returned; they stand alone.
    if let Some(open) = wrapper.as_mut()
        && open.first_inner.is_none()
    {
        open.first_inner = Some(index);
        call.input_chars = open.input_chars;
    }
    record.tool_calls.push(call);
}

/// Lines added and removed by one `FileChange` entry (`add`, `update` or `delete`).
fn file_change_counts(change: &Value) -> (i64, i64) {
    let content_lines = || {
        change
            .get("content")
            .and_then(Value::as_str)
            .map(|text| text.lines().count() as i64)
            .unwrap_or(0)
    };
    match change.get("type").and_then(Value::as_str) {
        Some("add") => (content_lines(), 0),
        Some("delete") => (0, content_lines()),
        _ => {
            let diff = change
                .get("unified_diff")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mut counts = (0, 0);
            for line in diff.lines() {
                if line.starts_with('+') && !line.starts_with("+++") {
                    counts.0 += 1;
                } else if line.starts_with('-') && !line.starts_with("---") {
                    counts.1 += 1;
                }
            }
            counts
        }
    }
}

/// Codex may record that a rollout was spawned by another session (subagent/fork).
fn apply_session_source(meta: &Value, record: &mut SessionRecord) {
    let Some(source) = meta.get("source") else {
        return;
    };
    let Some(object) = source.as_object() else {
        return;
    };
    for (kind, value) in object {
        let parent = value
            .get("parent_thread_id")
            .or_else(|| value.get("parent_session_id"))
            .or_else(|| value.get("parent_id"))
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .as_object()
                    .and_then(|inner| inner.values().find_map(|v| v.get("parent_thread_id")))
                    .and_then(Value::as_str)
            });
        if let Some(parent) = parent {
            record.parent_session_id = Some(parent.to_string());
            record.agent_kind = if kind.contains("fork") {
                AgentKind::Fork
            } else {
                AgentKind::Subagent
            };
            record.agent_type = value
                .get("agent_type")
                .or_else(|| value.get("role"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| Some(kind.clone()));
            return;
        }
    }
}

fn push_rate_limits(record: &mut SessionRecord, ts: &str, limits: &Value) {
    if limits.is_null() {
        return;
    }
    let limit_id = limits
        .get("limit_id")
        .and_then(Value::as_str)
        .unwrap_or("codex")
        .to_string();
    let plan_type = limits
        .get("plan_type")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let credits_balance = limits
        .get("credits")
        .and_then(|credits| credits.get("balance"))
        .and_then(|balance| {
            balance
                .as_f64()
                .or_else(|| balance.as_str().and_then(|text| text.parse().ok()))
        });
    for window_kind in ["primary", "secondary"] {
        let Some(window) = limits.get(window_kind) else {
            continue;
        };
        let Some(used_percent) = window.get("used_percent").and_then(Value::as_f64) else {
            continue;
        };
        let resets_at = window.get("resets_at").and_then(|value| {
            value
                .as_i64()
                .and_then(|epoch| chrono::DateTime::from_timestamp(epoch, 0))
                .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .or_else(|| value.as_str().map(ToOwned::to_owned))
        });
        record.rate_limits.push(RateLimitRecord {
            ts: ts.to_string(),
            limit_id: limit_id.clone(),
            window_kind: window_kind.to_string(),
            window_minutes: window.get("window_minutes").and_then(Value::as_i64),
            used_percent,
            resets_at,
            plan_type: plan_type.clone(),
            credits_balance,
        });
    }
}

fn content_text(payload: &Value, item_type: &str) -> String {
    payload
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some(item_type))
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn injected_context_kind(text: &str) -> Option<&'static str> {
    let trimmed = text.trim_start();
    if trimmed.starts_with("<user_instructions>")
        || trimmed.starts_with("# AGENTS.md")
        || trimmed.contains("AGENTS.md instructions")
    {
        Some("instructions")
    } else if trimmed.starts_with("<environment_context>") {
        Some("environment")
    } else if trimmed.starts_with("<permissions") || trimmed.starts_with("<collaboration_mode") {
        Some("system_prompt")
    } else {
        None
    }
}

fn tool_arguments(payload: &Value) -> String {
    payload
        .get("arguments")
        .or_else(|| payload.get("input"))
        .or_else(|| payload.get("action"))
        .map(|value| match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn command_from_arguments(arguments: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(arguments).ok()?;
    match parsed.get("cmd").or_else(|| parsed.get("command"))? {
        Value::String(cmd) => Some(cmd.clone()),
        Value::Array(parts) => Some(
            parts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
        ),
        _ => None,
    }
}

fn patch_target(arguments: &str) -> Option<String> {
    arguments.lines().find_map(|line| {
        [
            "*** Update File: ",
            "*** Add File: ",
            "*** Delete File: ",
            "+++ ",
        ]
        .iter()
        .find_map(|prefix| line.strip_prefix(prefix))
        .map(|path| path.trim().trim_start_matches("b/").to_string())
    })
}

fn output_text(payload: &Value) -> String {
    match payload.get("output") {
        Some(Value::String(text)) => {
            // custom_tool_call_output wraps {"output": "...", "metadata": {...}} as a string.
            match serde_json::from_str::<Value>(text) {
                Ok(Value::Object(map)) => map
                    .get("output")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| text.clone()),
                _ => text.clone(),
            }
        }
        Some(Value::Object(map)) => map
            .get("content")
            .or_else(|| map.get("output"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_default(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn output_exit_code(payload: &Value, output: &str) -> Option<i64> {
    if let Some(Value::String(text)) = payload.get("output")
        && let Ok(parsed) = serde_json::from_str::<Value>(text)
        && let Some(code) = parsed
            .get("metadata")
            .and_then(|meta| meta.get("exit_code"))
            .and_then(Value::as_i64)
    {
        return Some(code);
    }
    for line in output.lines().take(6) {
        for prefix in ["Process exited with code ", "Exit code: ", "exit code: "] {
            if let Some(rest) = line.trim().strip_prefix(prefix)
                && let Some(code) = rest.split_whitespace().next().and_then(|v| v.parse().ok())
            {
                return Some(code);
            }
        }
    }
    None
}

/// Accepted code changes implied by a successful write-like tool call.
fn accepted_changes_for_call(
    name: &str,
    arguments: &str,
    file_cache: &HashMap<String, String>,
) -> Vec<(String, i64, i64)> {
    if name == "exec_command" {
        return command_from_arguments(arguments)
            .and_then(|cmd| parse_exec_cmd_write(&cmd, file_cache))
            .into_iter()
            .collect();
    }
    if name != "apply_patch" {
        return Vec::new();
    }
    let patch = match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(map)) => map
            .get("patch")
            .or_else(|| map.get("input"))
            .and_then(Value::as_str)
            .unwrap_or(arguments)
            .to_string(),
        Ok(Value::String(text)) => text,
        _ => arguments.to_string(),
    };
    if patch.trim_start().starts_with("*** Begin Patch") {
        parse_codex_patch(&patch)
    } else {
        parse_unified_diff(&patch)
    }
}

/// Parse the Codex `*** Begin Patch` format into per-file line counts.
fn parse_codex_patch(patch: &str) -> Vec<(String, i64, i64)> {
    let mut results: Vec<(String, i64, i64)> = Vec::new();
    for line in patch.lines() {
        let header = ["*** Update File: ", "*** Add File: ", "*** Delete File: "]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix));
        if let Some(path) = header {
            results.push((path.trim().to_string(), 0, 0));
            continue;
        }
        if line.starts_with("*** ") {
            continue;
        }
        let Some(current) = results.last_mut() else {
            continue;
        };
        if line.starts_with('+') {
            current.1 += 1;
        } else if line.starts_with('-') {
            current.2 += 1;
        }
    }
    results.retain(|(_, added, removed)| *added > 0 || *removed > 0);
    results
}

/// Detect a `cat > file <<'EOF'` or `cat >> file <<'EOF'` write in a shell
/// command string. Returns `(file_path, lines_added, lines_removed)` on success.
fn parse_exec_cmd_write(
    cmd: &str,
    file_cache: &HashMap<String, String>,
) -> Option<(String, i64, i64)> {
    let cmd = cmd.trim();
    if !cmd.starts_with("cat ") {
        return None;
    }
    let after_cat = cmd[4..].trim_start();

    // Locate the heredoc marker ("<<")
    let heredoc_pos = after_cat.find("<<")?;
    let redirect_and_path = after_cat[..heredoc_pos].trim();

    // Extract file path from `> /path` or `>> /path`
    let file_path = if let Some(path) = redirect_and_path.strip_prefix(">>") {
        path.trim()
    } else {
        redirect_and_path.strip_prefix('>')?.trim()
    };
    if file_path.is_empty() {
        return None;
    }

    // Determine heredoc end-marker (strip quotes: 'EOF' or "EOF" → EOF)
    let raw_marker = after_cat[heredoc_pos + 2..].lines().next().unwrap_or("EOF");
    let marker = raw_marker.trim().trim_matches('\'').trim_matches('"');

    // Content is everything after the first newline, up to the closing marker line
    let newline_pos = cmd.find('\n')?;
    let body = &cmd[newline_pos + 1..];
    let end_pat = format!("\n{}", marker);
    let content = if let Some(end) = body.rfind(end_pat.as_str()) {
        &body[..end]
    } else {
        body
    };

    let (lines_added, lines_removed) = if let Some(before) = file_cache.get(file_path) {
        diff_line_counts(before, content)
    } else {
        (content.lines().count() as i64, 0)
    };
    Some((file_path.to_string(), lines_added, lines_removed))
}

/// Parse a unified diff and return `(file_path, lines_added, lines_removed)` per file.
fn parse_unified_diff(diff: &str) -> Vec<(String, i64, i64)> {
    let mut results = Vec::new();
    let mut current_file: Option<String> = None;
    let mut added: i64 = 0;
    let mut removed: i64 = 0;

    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("+++ ") {
            // Flush previous file
            if let Some(f) = current_file.take() {
                results.push((f, added, removed));
            }
            // Strip "b/" prefix that unified diff adds
            let path = rest.strip_prefix("b/").unwrap_or(rest).trim().to_string();
            current_file = Some(path);
            added = 0;
            removed = 0;
        } else if let Some(rest) = line.strip_prefix('+') {
            if !rest.starts_with("++") {
                added += 1;
            }
        } else if let Some(rest) = line.strip_prefix('-')
            && !rest.starts_with("--")
        {
            removed += 1;
        }
    }

    // Flush last file
    if let Some(f) = current_file {
        results.push((f, added, removed));
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_codex_patch_counts() {
        let patch = "*** Begin Patch\n*** Update File: src/a.rs\n@@\n-old\n+new\n+more\n*** Add File: src/b.rs\n+x\n*** End Patch";
        assert_eq!(
            parse_codex_patch(patch),
            vec![
                ("src/a.rs".to_string(), 2, 1),
                ("src/b.rs".to_string(), 1, 0)
            ]
        );
    }

    #[test]
    fn codex_usage_events_sum_to_final_total_without_double_reasoning() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("rollout.jsonl");
        let lines = [
            json!({"timestamp":"2026-03-10T06:41:40Z","type":"session_meta","payload":{"id":"s1","cwd":"/tmp/repo","git":{"branch":"feat/x"}}}),
            json!({"timestamp":"2026-03-10T06:41:41Z","type":"turn_context","payload":{"model":"gpt-5.5"}}),
            json!({"timestamp":"2026-03-10T06:41:42Z","type":"event_msg","payload":{"type":"user_message","message":"run the tests"}}),
            json!({"timestamp":"2026-03-10T06:41:50Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":11042,"cached_input_tokens":6528,"output_tokens":557,"reasoning_output_tokens":404,"total_tokens":11599},"model_context_window":258400},"rate_limits":{"limit_id":"codex","primary":{"used_percent":12.0,"window_minutes":300,"resets_at":1772548900},"secondary":{"used_percent":40.0,"window_minutes":10080,"resets_at":1773135700},"plan_type":"plus"}}}),
            json!({"timestamp":"2026-03-10T06:41:51Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"c1","arguments":"{\"cmd\":\"cargo test\"}"}}),
            json!({"timestamp":"2026-03-10T06:41:53Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"Process exited with code 101\nOutput:\nerror[E0308]: mismatched types"}}),
            json!({"timestamp":"2026-03-10T06:41:55Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":22830,"cached_input_tokens":13056,"output_tokens":772,"reasoning_output_tokens":432,"total_tokens":23602}},"rate_limits":null}}),
            json!({"timestamp":"2026-03-10T06:41:56Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":22830,"cached_input_tokens":13056,"output_tokens":772,"reasoning_output_tokens":432,"total_tokens":23602}},"rate_limits":null}}),
        ];
        std::fs::write(
            &path,
            lines
                .iter()
                .map(|l| l.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        let record = parse_session(&path)?.expect("record");
        assert_eq!(record.usage.len(), 2, "duplicate totals produce no event");
        let total: i64 = record.usage.iter().map(|u| u.total_tokens()).sum();
        assert_eq!(total, 23602);
        assert!(record.usage.iter().all(|u| u.reasoning_in_output));
        assert_eq!(record.usage[0].model.as_deref(), Some("gpt-5.5"));
        assert_eq!(record.usage[0].ts.as_deref(), Some("2026-03-10T06:41:50Z"));
        assert_eq!(record.rate_limits.len(), 2);
        assert_eq!(record.git_branch.as_deref(), Some("feat/x"));
        let call = &record.tool_calls[0];
        assert_eq!(call.exit_code, Some(101));
        assert_eq!(call.status, "error");
        assert_eq!(call.target.as_deref(), Some("cargo test"));
        Ok(())
    }

    fn write_rollout(dir: &std::path::Path, lines: &[Value]) -> anyhow::Result<PathBuf> {
        let path = dir.join("rollout.jsonl");
        let body: Vec<String> = lines.iter().map(Value::to_string).collect();
        std::fs::write(&path, body.join("\n"))?;
        Ok(path)
    }

    /// Codex CLI 0.160+ rollouts: prompts as `UserMessage` items, per-response
    /// `token_usage_record` lines, and tools run inside code-mode `exec` scripts whose
    /// inner calls arrive as `item_completed` events. Shapes follow a 0.161 rollout.
    #[test]
    fn parses_code_mode_rollout_items() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let item = |ts: &str, started_ms: i64, item: Value| {
            json!({"timestamp": ts, "type": "event_msg", "payload": {"type": "item_completed",
                "thread_id": "s2", "turn_id": "t1", "item": item,
                "started_at_ms": started_ms, "completed_at_ms": started_ms + 500}})
        };
        let usage = |ts: &str, id: &str, input: i64, cached: i64, write: i64, output: i64| {
            json!({"timestamp": ts, "type": "token_usage_record", "payload": {"thread_id": "s2",
                "turn_id": "t1", "response_id": id, "usage": {"input_tokens": input,
                "cached_input_tokens": cached, "cache_write_input_tokens": write,
                "output_tokens": output, "reasoning_output_tokens": 7,
                "total_tokens": input + output}}})
        };
        let path = write_rollout(
            dir.path(),
            &[
                json!({"timestamp":"2026-10-11T21:17:40Z","type":"session_meta","payload":{"id":"s2","cwd":"/tmp/game","cli_version":"0.161.0","originator":"codex-tui","model_provider":"openai","base_instructions":{"text":"You are Codex."}}}),
                json!({"timestamp":"2026-10-11T21:17:40.100Z","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}),
                json!({"timestamp":"2026-10-11T21:17:40.400Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>\n  <cwd>/tmp/game</cwd>\n</environment_context>"}]}}),
                json!({"timestamp":"2026-10-11T21:17:40.500Z","type":"turn_context","payload":{"turn_id":"t1","model":"gpt-6.1-sol"}}),
                item(
                    "2026-10-11T21:17:40.555Z",
                    1_791_753_460_555,
                    json!({"type":"UserMessage","id":"u1","content":[{"type":"text","text":"plan the first mission"}]}),
                ),
                json!({"timestamp":"2026-10-11T21:17:42.954Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Looking at the code."}]}}),
                json!({"timestamp":"2026-10-11T21:17:44.482Z","type":"response_item","payload":{"type":"custom_tool_call","call_id":"w1","name":"exec","input":"text(await tools.exec_command({cmd:\"cargo test\"}));\nawait tools.apply_patch(\"...\");"}}),
                usage("2026-10-11T21:17:44.536Z", "r1", 15_574, 0, 0, 145),
                json!({"timestamp":"2026-10-11T21:17:44.540Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":15574,"cached_input_tokens":0,"output_tokens":145,"reasoning_output_tokens":7,"total_tokens":15719}},"rate_limits":null}}),
                item(
                    "2026-10-11T21:17:45.808Z",
                    1_791_753_465_100,
                    json!({"type":"CommandExecution","id":"exec-1","command":["pwsh.exe","-Command","cargo test"],"parsed_cmd":[{"type":"unknown","cmd":"cargo test"}],"source":"unified_exec_startup","status":"completed","stdout":"ok","stderr":"","exit_code":0}),
                ),
                item(
                    "2026-10-11T21:17:46.000Z",
                    1_791_753_465_900,
                    json!({"type":"FileChange","id":"exec-2","status":"completed","changes":{
                        "/tmp/game/src/mission.rs":{"type":"update","unified_diff":"@@ -1,2 +1,3 @@\n fn a() {}\n-fn b() {}\n+fn b() { c() }\n+fn c() {}\n","move_path":null},
                        "/tmp/game/PLAN.md":{"type":"add","content":"# Plan\n\nStep 1\n"}}}),
                ),
                json!({"timestamp":"2026-10-11T21:17:46.200Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"w1","output":[{"type":"input_text","text":"Script completed\nOutput:\n"},{"type":"input_text","text":"{\"exit_code\":0,\"output\":\"ok\"}"}]}}),
                // A long-running command that finishes after its script returned.
                item(
                    "2026-10-11T21:18:30.000Z",
                    1_791_753_470_000,
                    json!({"type":"CommandExecution","id":"exec-3","command":["pwsh.exe","-Command","./run-game.ps1"],"source":"unified_exec_startup","status":"completed","stdout":"","stderr":"crash: access violation","exit_code":1}),
                ),
                item(
                    "2026-10-11T21:18:31.000Z",
                    1_791_753_511_000,
                    json!({"type":"ContextCompaction","id":"cc1"}),
                ),
                json!({"timestamp":"2026-10-11T21:18:40Z","type":"response_item","payload":{"type":"custom_tool_call","call_id":"w2","name":"exec","input":"await tools.wait(1000);"}}),
                usage("2026-10-11T21:18:41Z", "r2", 16_000, 15_000, 300, 60),
                json!({"timestamp":"2026-10-11T21:18:42Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"w2","output":"done"}}),
                item(
                    "2026-10-11T21:19:00.000Z",
                    1_791_753_540_000,
                    json!({"type":"UserMessage","id":"u2","content":[{"type":"text","text":"now run it"}]}),
                ),
                item(
                    "2026-10-11T21:19:05.000Z",
                    1_791_753_545_000,
                    json!({"type":"ImageView","id":"exec-4","path":"file:///tmp/game/shot.png"}),
                ),
            ],
        )?;
        let record = parse_session(&path)?.expect("record");

        let users: Vec<&str> = record
            .messages
            .iter()
            .filter(|m| m.role == "user")
            .map(|m| m.text.as_str())
            .collect();
        assert_eq!(users, ["plan the first mission", "now run it"]);
        assert_eq!(
            record.messages[0].ts.as_deref(),
            Some("2026-10-11T21:17:40.555Z")
        );

        // Per-response records only: the token_count line must not add a second event.
        assert_eq!(record.usage.len(), 2);
        let second = &record.usage[1];
        assert_eq!(
            second.input_tokens, 700,
            "input excludes cached and written"
        );
        assert_eq!(second.cache_read_tokens, 15_000);
        assert_eq!(second.cache_write_tokens, 300);
        assert_eq!(second.output_tokens, 60);
        assert_eq!(second.model.as_deref(), Some("gpt-6.1-sol"));
        let total: i64 = record.usage.iter().map(UsageRecord::total_tokens).sum();
        assert_eq!(total, 15_574 + 145 + 16_000 + 60);

        let calls: Vec<(&str, &str, Option<i64>)> = record
            .tool_calls
            .iter()
            .map(|c| (c.tool_name.as_str(), c.status.as_str(), c.exit_code))
            .collect();
        assert_eq!(
            calls,
            [
                ("exec_command", "ok", Some(0)),
                ("apply_patch", "ok", None),
                ("exec_command", "error", Some(1)),
                ("exec", "ok", None),
                ("view_image", "ok", None),
            ]
        );
        let first = &record.tool_calls[0];
        assert_eq!(first.target.as_deref(), Some("cargo test"));
        assert!(first.input_chars > 0, "script input goes to its first call");
        assert!(
            first.output_chars > 0,
            "script output goes to its first call"
        );
        assert_eq!(first.ts.as_deref(), Some("2026-10-11T21:17:45.100Z"));
        assert_eq!(record.tool_calls[1].input_chars, 0);
        assert_eq!(
            record.tool_calls[2].error_head.as_deref(),
            Some("crash: access violation")
        );
        assert_eq!(record.tool_calls[3].output_chars, 4);

        let mut changes: Vec<(&str, i64, i64)> = record
            .accepted_changes
            .iter()
            .map(|c| (c.abs_path.as_str(), c.lines_added, c.lines_removed))
            .collect();
        changes.sort();
        assert_eq!(
            changes,
            [
                ("/tmp/game/PLAN.md", 3, 0),
                ("/tmp/game/src/mission.rs", 2, 1)
            ]
        );
        Ok(())
    }

    #[test]
    fn legacy_user_message_events_are_dropped_when_items_exist() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = write_rollout(
            dir.path(),
            &[
                json!({"timestamp":"2026-10-11T10:00:00Z","type":"session_meta","payload":{"id":"s3","cwd":"/tmp/x"}}),
                json!({"timestamp":"2026-10-11T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"hello"}}),
                json!({"timestamp":"2026-10-11T10:00:01Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","id":"u1","content":[{"type":"text","text":"hello"}]}}}),
            ],
        )?;
        let record = parse_session(&path)?.expect("record");
        assert_eq!(
            record.messages.iter().filter(|m| m.role == "user").count(),
            1
        );
        Ok(())
    }
}
