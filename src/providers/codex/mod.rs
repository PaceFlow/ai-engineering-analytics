use anyhow::Result;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use crate::db;
use crate::events::Event;
use super::Provider;
use super::utils::diff_line_counts;

pub struct CodexProvider;

impl Provider for CodexProvider {
    fn name(&self) -> &str {
        "codex"
    }

    fn ingest(&self, db: &Connection, verbose: bool) -> Result<usize> {
        let sessions_root = codex_sessions_dir()?;
        if !sessions_root.exists() {
            if verbose {
                eprintln!("[codex] sessions dir not found: {:?}", sessions_root);
            }
            return Ok(0);
        }

        let files = find_jsonl_files(&sessions_root);
        println!("  found {} session file(s)", files.len());

        let mut total_events = 0;
        for session_file in &files {
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
                        eprintln!("wrote {} events", n);
                    }
                    total_events += n;
                }
                Err(e) => {
                    eprintln!("Warning: skipping {:?}: {}", session_file, e);
                }
            }
        }
        Ok(total_events)
    }
}

fn codex_sessions_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    Ok(home.join(".codex").join("sessions"))
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
}

// ── Session ingestion ───────────────────────────────────────────────────────

fn ingest_session(path: &PathBuf, db: &Connection) -> Result<usize> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // First line must be the session_meta envelope
    let first_line = match lines.next() {
        Some(Ok(l)) if !l.trim().is_empty() => l,
        _ => return Ok(0),
    };

    let first: Line = match serde_json::from_str(&first_line) {
        Ok(l) => l,
        Err(_) => return Ok(0), // not a Codex session file
    };

    if first.kind != "session_meta" {
        return Ok(0);
    }

    // Capture outer timestamp before payload is consumed
    let first_outer_ts = first.timestamp;

    let meta: SessionMetaPayload = match serde_json::from_value(first.payload) {
        Ok(m) => m,
        Err(_) => return Ok(0),
    };

    let session_id = &meta.id;

    // Skip if already ingested
    if db::session_exists(db, session_id)? {
        return Ok(0);
    }

    // Use meta.timestamp (payload) if present, otherwise the outer envelope timestamp
    let session_start_ts = meta.timestamp.clone().or(first_outer_ts);

    // call_id -> file_path for pending read operations
    let mut pending_reads: HashMap<String, String> = HashMap::new();
    // file_path -> last known content (before-state for next write)
    let mut file_cache: HashMap<String, String> = HashMap::new();

    let mut events: Vec<Event> = Vec::new();

    events.push(Event::ChatStart {
        session_id: session_id.clone(),
        provider: "codex".into(),
        project_path: meta.cwd.clone(),
        timestamp: session_start_ts.clone(),
        last_updated: None,
    });

    // Parse remaining lines
    for line_result in lines {
        let raw = line_result?;
        if raw.trim().is_empty() {
            continue;
        }

        let line: Line = match serde_json::from_str(&raw) {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Use the outer envelope timestamp for each event (most precise per line)
        let ts = line.timestamp.clone().or_else(|| session_start_ts.clone());

        match line.kind.as_str() {
            "event_msg" => {
                // Only handle user_message subtype
                if line.payload.get("type").and_then(|v| v.as_str()) == Some("user_message") {
                    if let Some(msg) = line.payload.get("message").and_then(|v| v.as_str()) {
                        let content = msg.to_string();
                        let words = content.split_whitespace().count();
                        if words > 0 {
                            events.push(Event::Message {
                                session_id: session_id.clone(),
                                provider: "codex".into(),
                                role: "user".into(),
                                content,
                                content_words: words,
                                timestamp: ts,
                            });
                        }
                    }
                }
            }
            "response_item" => {
                let role = line.payload.get("role").and_then(|v| v.as_str()).unwrap_or("");
                let item_type = line.payload.get("type").and_then(|v| v.as_str()).unwrap_or("");

                if role == "assistant" {
                    // Extract text from the content array (output_text items)
                    if let Some(content_arr) = line.payload.get("content").and_then(|v| v.as_array()) {
                        let text: String = content_arr
                            .iter()
                            .filter(|item| {
                                item.get("type").and_then(|t| t.as_str()) == Some("output_text")
                            })
                            .filter_map(|item| item.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("");
                        let words = text.split_whitespace().count();
                        if words > 0 {
                            events.push(Event::Message {
                                session_id: session_id.clone(),
                                provider: "codex".into(),
                                role: "assistant".into(),
                                content: text,
                                content_words: words,
                                timestamp: ts.clone(),
                            });
                        }
                    }
                }

                if item_type == "function_call_output" {
                    let call_id = line.payload
                        .get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                    if let Some(file_path) = pending_reads.remove(call_id) {
                        // Extract file content: everything after "\nOutput:\n"
                        let raw_output = line.payload
                            .get("output").and_then(|v| v.as_str()).unwrap_or("");
                        let content = if let Some(pos) = raw_output.find("\nOutput:\n") {
                            raw_output[pos + "\nOutput:\n".len()..].to_string()
                        } else {
                            raw_output.to_string()
                        };
                        file_cache.insert(file_path, content);
                    }
                }

                if item_type == "function_call" {
                    // Detect plain reads: `cat <path>` with no redirect, record for before-state
                    let name = line.payload.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    if name == "exec_command" {
                        let args_str: Option<String> = line.payload
                            .get("arguments")
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            });
                        let parsed_cmd: Option<String> = args_str.as_ref().and_then(|args| {
                            serde_json::from_str::<Value>(args)
                                .ok()
                                .and_then(|obj| obj.get("cmd").and_then(|v| v.as_str()).map(String::from))
                        });
                        if let Some(cmd) = &parsed_cmd {
                            let trimmed = cmd.trim();
                            if trimmed.starts_with("cat ") && !trimmed.contains('>') {
                                let path = trimmed[4..].trim().to_string();
                                if !path.is_empty() {
                                    let call_id = line.payload
                                        .get("call_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    if !call_id.is_empty() {
                                        pending_reads.insert(call_id, path);
                                    }
                                }
                            }
                        }
                    }

                    parse_tool_call(&line.payload, session_id, ts.as_deref().unwrap_or(""), &mut events, &file_cache);
                }
            }
            _ => {} // skip unknown line types
        }
    }

    let count = events.len();
    for event in &events {
        db::insert_event(db, event)?;
    }
    Ok(count)
}

/// Try to parse an `apply_patch` tool call from a JSON value and push
/// `ChangesAccepted` events for each modified file found in the diff.
fn parse_tool_call(value: &Value, session_id: &str, timestamp: &str, events: &mut Vec<Event>, file_cache: &HashMap<String, String>) {
    let name = value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if name == "exec_command" {
        let args_str: Option<String> = value
            .get("arguments")
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            });

        if let Some(args) = args_str {
            let cmd: Option<String> = serde_json::from_str::<Value>(&args)
                .ok()
                .and_then(|obj| obj.get("cmd").and_then(|v| v.as_str()).map(String::from));

            if let Some(cmd) = cmd {
                if let Some((file_path, lines_added, lines_removed)) = parse_exec_cmd_write(&cmd, file_cache) {
                    events.push(Event::ChangesAccepted {
                        session_id: session_id.to_string(),
                        provider: "codex".into(),
                        file_path,
                        lines_added,
                        lines_removed,
                        timestamp: Some(timestamp.to_string()),
                    });
                }
            }
        }
        return;
    }

    if name != "apply_patch" {
        return;
    }

    // Arguments is a JSON string: {"patch": "...diff..."}
    let args_str: Option<String> = value
        .get("arguments")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });

    let patch = match args_str {
        Some(s) => {
            if let Ok(obj) = serde_json::from_str::<Value>(&s) {
                obj.get("patch")
                    .and_then(|p| p.as_str())
                    .unwrap_or(&s)
                    .to_string()
            } else {
                s
            }
        }
        None => return,
    };

    for (file_path, added, removed) in parse_unified_diff(&patch) {
        events.push(Event::ChangesAccepted {
            session_id: session_id.to_string(),
            provider: "codex".into(),
            file_path,
            lines_added: added,
            lines_removed: removed,
            timestamp: Some(timestamp.to_string()),
        });
    }
}

/// Detect a `cat > file <<'EOF'` or `cat >> file <<'EOF'` write in a shell
/// command string. Returns `(file_path, lines_added, lines_removed)` on success.
fn parse_exec_cmd_write(cmd: &str, file_cache: &HashMap<String, String>) -> Option<(String, i64, i64)> {
    let cmd = cmd.trim();
    if !cmd.starts_with("cat ") {
        return None;
    }
    let after_cat = cmd[4..].trim_start();

    // Locate the heredoc marker ("<<")
    let heredoc_pos = after_cat.find("<<")?;
    let redirect_and_path = after_cat[..heredoc_pos].trim();

    // Extract file path from `> /path` or `>> /path`
    let file_path = if redirect_and_path.starts_with(">>") {
        redirect_and_path[2..].trim()
    } else if redirect_and_path.starts_with('>') {
        redirect_and_path[1..].trim()
    } else {
        return None;
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
        } else if let Some(rest) = line.strip_prefix('-') {
            if !rest.starts_with("--") {
                removed += 1;
            }
        }
    }

    // Flush last file
    if let Some(f) = current_file {
        results.push((f, added, removed));
    }

    results
}
