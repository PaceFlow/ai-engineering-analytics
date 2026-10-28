//! OpenCode 2 stores typed messages, content and tools in session_message.
//! It retains V1 rows on migration; those are read only to preserve message anchors.
use super::*;
use rusqlite::OptionalExtension;
use serde_json::json;

pub(super) fn parse(
    source: &Connection,
    header: &SessionHeader,
    out: &mut OpenCodeSession,
) -> Result<()> {
    let legacy_parts = !table_columns(source, "part")?.is_empty();
    let mut q = source.prepare(
        "SELECT id,type,time_created,data FROM session_message WHERE session_id=?1 ORDER BY seq,id",
    )?;
    let mut rows = q.query([&header.id])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let created: i64 = row.get(2)?;
        let data: String = row.get(3)?;
        let data: Value = serde_json::from_str(&data)
            .with_context(|| format!("Invalid OpenCode V2 message {id}"))?;
        let ts = data
            .pointer("/time/created")
            .and_then(Value::as_i64)
            .and_then(ms_to_iso)
            .or_else(|| ms_to_iso(created));
        let finished = data
            .pointer("/time/completed")
            .and_then(Value::as_i64)
            .and_then(ms_to_iso)
            .or_else(|| ts.clone());
        let text = data.get("text").and_then(Value::as_str).unwrap_or_default();
        match kind.as_str() {
            "user" => {
                let key = legacy_anchor(source, legacy_parts, &id, text)?;
                out.messages.push(OpenCodeMessage {
                    key,
                    role: "user".into(),
                    text: text.into(),
                    timestamp: ts,
                    compaction: false,
                });
            }
            "assistant" => {
                usage(&id, &data, finished.clone(), out);
                let content = data.get("content").and_then(Value::as_array);
                let mut text = Vec::new();
                for (index, part) in content.into_iter().flatten().enumerate() {
                    match part.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(s) = part.get("text").and_then(Value::as_str) {
                                text.push(s);
                            }
                        }
                        Some("tool") => {
                            let normalized = normalize_tool(part);
                            let fallback = format!("{id}:tool:{index}");
                            let mut call = tool_call_from_part(&fallback, &normalized);
                            if call.ts.is_none() {
                                call.ts = ts.clone();
                            }
                            if call.ended_ts.is_none()
                                && matches!(call.status.as_str(), "ok" | "error" | "interrupted")
                            {
                                call.ended_ts = finished.clone();
                            }
                            if call.status == "ok" {
                                out.tool_edits.extend(edits_from_tool_part(
                                    &normalized,
                                    out.directory.as_deref(),
                                    &call.ts,
                                ));
                            }
                            out.tool_calls.push(call);
                        }
                        _ => {}
                    }
                }
                let text = text.join("\n");
                if !text.trim().is_empty() {
                    out.messages.push(OpenCodeMessage {
                        key: legacy_anchor(source, legacy_parts, &id, &text)?,
                        role: "assistant".into(),
                        text,
                        timestamp: finished,
                        compaction: false,
                    });
                }
                if data.get("finish").and_then(Value::as_str) == Some("error") {
                    out.events.push(SessionEventRecord {
                        ts,
                        kind: "api_error".into(),
                        detail: Some("OpenCode assistant finished with an error".into()),
                    });
                }
            }
            "compaction" => {
                usage(&id, &data, finished, out);
                out.events.push(SessionEventRecord {
                    ts: ts.clone(),
                    kind: "compaction".into(),
                    detail: data
                        .get("status")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
                if let Some(summary) = data
                    .get("summary")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    out.messages.push(OpenCodeMessage {
                        key: id,
                        role: "assistant".into(),
                        text: summary.into(),
                        timestamp: ts,
                        compaction: true,
                    });
                }
            }
            "system" | "synthetic" => {
                let sources = data
                    .pointer("/metadata/instructionSources")
                    .and_then(Value::as_array)
                    .map(|v| v.iter().filter_map(Value::as_str).collect::<Vec<_>>());
                out.context.push(ContextInjectionRecord {
                    key: id,
                    ts,
                    kind: if sources.is_some() {
                        "instructions"
                    } else {
                        "reminder"
                    }
                    .into(),
                    source: sources.map(|s| s.join("\n")),
                    chars: text.chars().count() as i64,
                });
            }
            "model-switched" => out.events.push(SessionEventRecord {
                ts,
                kind: "model_change".into(),
                detail: data
                    .pointer("/model/id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }),
            "idle" => {
                // Execution outcome is evidence for assessment, never a goal-completion mark.
                out.events.push(SessionEventRecord {
                    ts,
                    kind: "runtime_outcome".into(),
                    detail: data
                        .get("outcome")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            "agent-switched" | "location-switched" => out.events.push(SessionEventRecord {
                ts,
                kind: kind.replace('-', "_"),
                detail: None,
            }),
            _ => bail!("Unsupported OpenCode V2 message type {kind} in {id}"),
        }
    }
    if let Some(parent) = &header.fork_id {
        out.events.push(SessionEventRecord {
            ts: out.started_at.clone(),
            kind: "forked_from".into(),
            detail: Some(parent.clone()),
        });
    }
    Ok(())
}

fn usage(id: &str, data: &Value, ts: Option<String>, out: &mut OpenCodeSession) {
    let mut normalized = data.clone();
    normalized["role"] = json!("assistant");
    if let Some(mut usage) = parse_opencode_usage(&normalized) {
        usage.key = id.into();
        usage.timestamp = ts;
        usage.model_name = data
            .pointer("/model/id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        usage.provider_id = data
            .pointer("/model/providerID")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if usage.model_name.is_some() {
            out.model_name = usage.model_name.clone();
        }
        out.usage_events.push(usage);
    }
}

fn legacy_anchor(source: &Connection, has_parts: bool, id: &str, text: &str) -> Result<String> {
    if !has_parts {
        return Ok(id.into());
    }
    // Match exact text, so a changed message does not inherit an unrelated saved decision.
    let anchor:Option<String>=source.query_row("SELECT id FROM part WHERE message_id=?1 AND json_valid(data) AND json_extract(data,'$.type')='text' AND json_extract(data,'$.text')=?2 ORDER BY time_created,id LIMIT 1",params![id,text],|r|r.get(0)).optional()?;
    Ok(anchor.unwrap_or_else(|| id.into()))
}

fn normalize_tool(part: &Value) -> Value {
    let mut normalized = part.clone();
    normalized["tool"] = part.get("name").cloned().unwrap_or(Value::Null);
    normalized["callID"] = part.get("id").cloned().unwrap_or(Value::Null);
    let state = part.get("state").unwrap_or(&Value::Null);
    let output = state
        .get("content")
        .and_then(Value::as_array)
        .map(|v| {
            v.iter()
                .filter_map(|p| p.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    normalized["state"]["output"] = json!(output);
    normalized["state"]["time"] = json!({"start":part.pointer("/time/ran").or_else(||part.pointer("/time/created")),"end":part.pointer("/time/completed")});
    normalized
}
