use anyhow::Result;
use rusqlite::{Connection, TransactionBehavior};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::change_intel::providers::claude;
use crate::change_intel::providers::codex::parsers::apply_patch::ApplyPatchParser;
use crate::change_intel::providers::codex::parsers::exec_heredoc::ExecHeredocWriteParser;
use crate::change_intel::providers::cursor;
use crate::change_intel::session_context::SessionContext;
use crate::change_intel::storage;
use crate::change_intel::types::{PatternParser, SessionInfo, ToolCallEvent};
use crate::ingest_progress::IngestProgressObserver;

const CODEX_CURSOR_NAMESPACE: &str = "codex_core_v1";

#[derive(Debug, Clone, Default)]
pub struct ProviderCodeChangeSummary {
    pub provider: String,
    pub sources_discovered: usize,
    pub sources_skipped: usize,
    pub tool_calls_inspected: usize,
    pub ops_upserted: usize,
    pub parse_errors: usize,
    pub legacy_sessions_considered: usize,
    pub legacy_entries_inspected: usize,
    pub legacy_diff_rows_found: usize,
    pub legacy_ops_upserted: usize,
    pub legacy_parse_errors: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ProviderCodeChangePlan {
    pub provider: String,
    pub sources: Vec<PathBuf>,
}

impl ProviderCodeChangePlan {
    pub fn item_count(&self) -> usize {
        self.sources.len()
    }

    pub fn progress_unit_count(&self) -> usize {
        if self.provider == "cursor" {
            // Cursor ingestion has 9 real sub-phases per vscdb source
            // (open → graphs → candidates → tool writes → inline undo →
            //  partial fates old → partial fates new → legacy blocks → commit).
            // Reserve a tick for each so the progress bar actually moves
            // during the long warmup-heavy stages instead of going silent.
            return self.sources.len().saturating_mul(9);
        }
        self.item_count()
    }
}

#[derive(Debug, Clone, Default)]
struct ParsedSource {
    session: Option<SessionInfo>,
    events: Vec<ToolCallEvent>,
}

pub fn plan_provider_code_changes(provider_name: &str) -> Result<ProviderCodeChangePlan> {
    let provider = provider_name.to_ascii_lowercase();
    match provider.as_str() {
        "codex" => Ok(ProviderCodeChangePlan {
            provider,
            sources: discover_codex_sources()?,
        }),
        "claude" => Ok(ProviderCodeChangePlan {
            provider,
            sources: crate::providers::claude::plan_session_files()?,
        }),
        "cursor" => Ok(ProviderCodeChangePlan {
            provider,
            sources: cursor::cursor_vscdb_path()?.into_iter().collect(),
        }),
        _ => Ok(ProviderCodeChangePlan {
            provider,
            ..ProviderCodeChangePlan::default()
        }),
    }
}

pub fn ingest_provider_code_changes(
    conn: &mut Connection,
    plan: &ProviderCodeChangePlan,
    verbose: bool,
    progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<ProviderCodeChangeSummary> {
    match plan.provider.as_str() {
        "claude" => {
            claude::ingest_claude_code_changes_from_sources(conn, &plan.sources, verbose, progress)
        }
        "codex" => ingest_codex_sources(conn, &plan.sources, verbose, progress),
        "cursor" => {
            cursor::ingest_cursor_code_changes_from_sources(conn, &plan.sources, verbose, progress)
        }
        _ => Ok(ProviderCodeChangeSummary {
            provider: plan.provider.clone(),
            ..ProviderCodeChangeSummary::default()
        }),
    }
}

fn ingest_codex_sources(
    conn: &mut Connection,
    sources: &[PathBuf],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<ProviderCodeChangeSummary> {
    let parsers: Vec<Box<dyn PatternParser>> =
        vec![Box::new(ExecHeredocWriteParser), Box::new(ApplyPatchParser)];

    let mut summary = ProviderCodeChangeSummary {
        provider: "codex".to_string(),
        ..ProviderCodeChangeSummary::default()
    };

    for source in sources {
        summary.sources_discovered += 1;
        let source_file = source.to_string_lossy().to_string();

        let result = (|| -> Result<()> {
            let sig = file_signature(source)?;
            if let Some((mtime, size)) = sig {
                let cursor =
                    storage::get_ingest_cursor(conn, CODEX_CURSOR_NAMESPACE, &source_file)?;
                if let Some(cursor) = cursor
                    && cursor.file_mtime == mtime
                    && cursor.file_size == size
                {
                    summary.sources_skipped += 1;
                    return Ok(());
                }
            }

            let parsed = parse_codex_source(source)?;
            summary.tool_calls_inspected += parsed.events.len();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            if let Some(session) = parsed.session {
                storage::upsert_change_session(&tx, &session)?;
                let mut ctx = SessionContext::new(session.session_cwd.clone());
                let mut ops = Vec::new();

                for event in parsed.events {
                    if !tool_call_succeeded(&event) {
                        continue;
                    }

                    for parser in &parsers {
                        let outcome = parser.parse(&event, &mut ctx);

                        for err in outcome.errors {
                            if verbose {
                                eprintln!(
                                    "[change-intel] {} {} {}",
                                    parser.name(),
                                    event.call_id,
                                    err.error
                                );
                            }
                            storage::insert_parse_error(&tx, &err)?;
                            summary.parse_errors += 1;
                        }

                        ops.extend(outcome.ops);
                    }
                }

                let reconcile = storage::reconcile_source_tool_writes(
                    &tx,
                    &session.provider,
                    &session.source_file,
                    &ops,
                )?;
                summary.ops_upserted += reconcile.ops_upserted;
            }

            if let Some((mtime, size)) = sig {
                storage::upsert_ingest_cursor(
                    &tx,
                    CODEX_CURSOR_NAMESPACE,
                    &source_file,
                    mtime,
                    size,
                )?;
            }

            tx.commit()?;
            Ok(())
        })();

        if let Some(observer) = progress.as_mut() {
            observer.advance(&source_file);
        }
        result?;
    }

    Ok(summary)
}

fn discover_codex_sources() -> Result<Vec<PathBuf>> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    let root = home.join(".codex").join("sessions");

    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = discover_jsonl_files(&root);
    files.sort();
    Ok(files)
}

fn discover_jsonl_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return files;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(discover_jsonl_files(&path));
        } else if path.extension().and_then(|x| x.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }

    files
}

fn parse_codex_source(path: &Path) -> Result<ParsedSource> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let source_file = path.to_string_lossy().to_string();

    let mut session_id: Option<String> = None;
    let mut session_cwd: Option<String> = None;
    let mut last_seen_at: Option<String> = None;

    let mut events: Vec<ToolCallEvent> = Vec::new();
    let mut outputs_by_call: HashMap<String, String> = HashMap::new();

    for line_result in reader.lines() {
        let raw = line_result?;
        if raw.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let timestamp = parsed
            .get("timestamp")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);
        if let Some(ts) = &timestamp
            && last_seen_at.as_ref().map(|s| ts > s).unwrap_or(true)
        {
            last_seen_at = Some(ts.clone());
        }

        match parsed.get("type").and_then(|v| v.as_str()) {
            Some("session_meta") => {
                if let Some(payload) = parsed.get("payload") {
                    session_id = payload
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned);
                    session_cwd = payload
                        .get("cwd")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned);
                }
            }
            Some("response_item") => {
                let Some(payload) = parsed.get("payload") else {
                    continue;
                };

                match payload.get("type").and_then(|v| v.as_str()) {
                    Some("function_call") => {
                        let Some(name) = payload.get("name").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        if name != "exec_command" {
                            continue;
                        }

                        let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        let Some(sid) = session_id.clone() else {
                            continue;
                        };

                        let input_json = payload
                            .get("arguments")
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "null".to_string());

                        events.push(ToolCallEvent {
                            provider: "codex".to_string(),
                            session_id: sid,
                            source_file: source_file.clone(),
                            timestamp: timestamp.clone(),
                            call_id: call_id.to_string(),
                            tool_name: "exec_command".to_string(),
                            input_json,
                            output_json: None,
                        });
                    }
                    Some("custom_tool_call") => {
                        let Some(name) = payload.get("name").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        if name != "apply_patch" {
                            continue;
                        }

                        let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        let Some(sid) = session_id.clone() else {
                            continue;
                        };

                        let input_json = payload
                            .get("input")
                            .and_then(|v| v.as_str())
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| {
                                payload
                                    .get("input")
                                    .map(|v| v.to_string())
                                    .unwrap_or_default()
                            });

                        events.push(ToolCallEvent {
                            provider: "codex".to_string(),
                            session_id: sid,
                            source_file: source_file.clone(),
                            timestamp: timestamp.clone(),
                            call_id: call_id.to_string(),
                            tool_name: "apply_patch".to_string(),
                            input_json,
                            output_json: None,
                        });
                    }
                    Some("function_call_output") | Some("custom_tool_call_output") => {
                        let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        outputs_by_call.insert(call_id.to_string(), payload.to_string());
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    if let Some(sid) = session_id {
        for event in &mut events {
            if let Some(output) = outputs_by_call.get(&event.call_id) {
                event.output_json = Some(output.clone());
            }
        }

        return Ok(ParsedSource {
            session: Some(SessionInfo {
                provider: "codex".to_string(),
                session_id: sid,
                source_file,
                session_cwd,
                last_seen_at,
            }),
            events,
        });
    }

    Ok(ParsedSource::default())
}

fn file_signature(path: &Path) -> Result<Option<(i64, i64)>> {
    if !path.is_file() {
        return Ok(None);
    }

    let md = std::fs::metadata(path)?;
    let size = md.len() as i64;
    let mtime = md
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    Ok(Some((mtime, size)))
}

fn tool_call_succeeded(event: &ToolCallEvent) -> bool {
    match event.tool_name.as_str() {
        "exec_command" => exec_command_succeeded(event.output_json.as_deref()),
        "apply_patch" => apply_patch_succeeded(event.output_json.as_deref()),
        _ => false,
    }
}

fn exec_command_succeeded(output_json: Option<&str>) -> bool {
    let Some(output_json) = output_json else {
        return false;
    };

    let parsed: Value = match serde_json::from_str(output_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let output = parsed.get("output").and_then(|v| v.as_str()).unwrap_or("");
    output.contains("Process exited with code 0")
}

fn apply_patch_succeeded(output_json: Option<&str>) -> bool {
    let Some(output_json) = output_json else {
        return false;
    };

    let parsed: Value = match serde_json::from_str(output_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let raw_inner = parsed.get("output").and_then(|v| v.as_str()).unwrap_or("");
    let inner: Value = match serde_json::from_str(raw_inner) {
        Ok(v) => v,
        Err(_) => return false,
    };

    inner
        .get("metadata")
        .and_then(|v| v.get("exit_code"))
        .and_then(|v| v.as_i64())
        == Some(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::change_intel::schema::init_change_intel_schema;

    fn write_jsonl(lines: Vec<Value>) -> Result<PathBuf> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("paceflow_codex_change_{}.jsonl", now));

        let mut body = String::new();
        for line in lines {
            body.push_str(&line.to_string());
            body.push('\n');
        }

        fs::write(&path, body)?;
        Ok(path)
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
    }

    fn tool_write_count(conn: &Connection) -> Result<i64> {
        conn.query_row(
            "SELECT COUNT(*) FROM fact_session_code_change WHERE source_kind = 'tool_write'",
            [],
            |r| r.get(0),
        )
        .map_err(Into::into)
    }

    fn load_tool_write_modes(conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT write_mode
             FROM fact_session_code_change
             WHERE source_kind = 'tool_write'
             ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    #[test]
    fn ingest_is_idempotent_with_cursor_skip() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_change_intel_schema(&conn)?;

        let path = write_jsonl(vec![
            json!({"timestamp":"2026-03-04T10:00:00Z","type":"session_meta","payload":{"id":"s1","cwd":"/tmp/repo"}}),
            json!({"timestamp":"2026-03-04T10:00:01Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"c1","arguments":"{\"cmd\":\"cat > src/a.txt <<'EOF'\\nhello\\nEOF\",\"workdir\":\"/tmp/repo\"}"}}),
            json!({"timestamp":"2026-03-04T10:00:02Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"Chunk ID: 1\nProcess exited with code 0\nOutput:\n"}}),
        ])?;

        let summary1 = ingest_codex_sources(&mut conn, std::slice::from_ref(&path), false, None)?;
        assert_eq!(summary1.ops_upserted, 1);

        let summary2 = ingest_codex_sources(&mut conn, std::slice::from_ref(&path), false, None)?;
        assert_eq!(summary2.sources_skipped, 1);

        assert_eq!(tool_write_count(&conn)?, 1);

        cleanup(&path);
        Ok(())
    }

    #[test]
    fn failed_exec_call_is_skipped() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_change_intel_schema(&conn)?;

        let path = write_jsonl(vec![
            json!({"timestamp":"2026-03-04T10:00:00Z","type":"session_meta","payload":{"id":"s2","cwd":"/tmp/repo"}}),
            json!({"timestamp":"2026-03-04T10:00:01Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"c1","arguments":"{\"cmd\":\"cat > src/a.txt <<'EOF'\\nhello\\nEOF\",\"workdir\":\"/tmp/repo\"}"}}),
            json!({"timestamp":"2026-03-04T10:00:02Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"Chunk ID: 1\nProcess exited with code 1\nOutput:\n"}}),
        ])?;

        let summary = ingest_codex_sources(&mut conn, std::slice::from_ref(&path), false, None)?;
        assert_eq!(summary.ops_upserted, 0);

        assert_eq!(tool_write_count(&conn)?, 0);

        cleanup(&path);
        Ok(())
    }

    #[test]
    fn apply_patch_success_and_failure_filtering() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_change_intel_schema(&conn)?;

        let success_output = json!({
            "output": "Success. Updated the following files:\nM src/a.txt\n",
            "metadata": {"exit_code": 0, "duration_seconds": 0.0}
        })
        .to_string();

        let fail_output = json!({
            "output": "Failed.",
            "metadata": {"exit_code": 1, "duration_seconds": 0.0}
        })
        .to_string();

        let path = write_jsonl(vec![
            json!({"timestamp":"2026-03-04T10:00:00Z","type":"session_meta","payload":{"id":"s3","cwd":"/tmp/repo"}}),
            json!({"timestamp":"2026-03-04T10:00:01Z","type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","call_id":"p_ok","input":"*** Begin Patch\n*** Update File: src/a.txt\n@@\n-old\n+new\n*** End Patch\n"}}),
            json!({"timestamp":"2026-03-04T10:00:02Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"p_ok","output": success_output}}),
            json!({"timestamp":"2026-03-04T10:00:03Z","type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","call_id":"p_fail","input":"*** Begin Patch\n*** Update File: src/b.txt\n@@\n-old\n+new\n*** End Patch\n"}}),
            json!({"timestamp":"2026-03-04T10:00:04Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"p_fail","output": fail_output}}),
        ])?;

        let summary = ingest_codex_sources(&mut conn, std::slice::from_ref(&path), false, None)?;
        assert_eq!(summary.ops_upserted, 1);

        let modes = load_tool_write_modes(&conn)?;
        assert_eq!(modes, vec!["patch".to_string()]);

        cleanup(&path);
        Ok(())
    }

    #[test]
    fn unknown_provider_returns_noop_summary() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_change_intel_schema(&conn)?;

        let summary = ingest_provider_code_changes(
            &mut conn,
            &ProviderCodeChangePlan {
                provider: "unknown".to_string(),
                sources: Vec::new(),
            },
            false,
            None,
        )?;
        assert_eq!(summary.provider, "unknown");
        assert_eq!(summary.sources_discovered, 0);
        assert_eq!(summary.ops_upserted, 0);
        Ok(())
    }
}
