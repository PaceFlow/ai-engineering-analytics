//! What vca stored about one session, as rows rather than conclusions: every
//! classifier and heuristic label per unit, the tasks and the goal each one
//! belongs to (with the links that joined it), and the context ledger.
//! Served next to the review timeline so a person can check the data model
//! against the conversation it came from.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::HashMap;

/// Unit kinds whose keys start with `provider|session|`.
const UNIT_KINDS: [&str; 7] = [
    "turn",
    "exchange",
    "task",
    "task_completion",
    "work_start",
    "skill",
    "relation",
];

pub fn inspect(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    agents: &[String],
) -> Result<Value> {
    Ok(json!({
        "labels": labels(conn, provider, session_id)?,
        "tasks": tasks(conn, provider, session_id)?,
        "context": context(conn, provider, agents)?,
    }))
}

/// Every stored answer for this session's units, newest first per question,
/// with the one reports use marked `effective`: an override, else the newest
/// confident or tentative classifier answer, else a heuristic, else an
/// uncertain classifier answer (the order of `view_label_effective`).
fn labels(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<Value>> {
    let prefix = format!("{provider}|{session_id}|");
    let end = format!("{provider}|{session_id}}}");
    let mut out: Vec<(String, String, String, i64, Value)> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, unit_key, question_id, source, label, probability, confidence, band,
                probabilities_json, model_version, created_at, superseded_by IS NOT NULL
         FROM classification_result
         WHERE unit_kind = ?1 AND unit_key >= ?2 AND unit_key < ?3",
    )?;
    for kind in UNIT_KINDS {
        let rows = stmt.query_map(params![kind, prefix, end], |r| {
            let unit_key: String = r.get(1)?;
            let probabilities: Option<String> = r.get(8)?;
            let source: String = r.get(3)?;
            let band: String = r.get(7)?;
            let superseded: bool = r.get(11)?;
            let rank = if superseded {
                9
            } else if source == "jev" && matches!(band.as_str(), "confident" | "tentative") {
                1
            } else if source == "jev" {
                3
            } else {
                2
            };
            Ok((
                unit_key.clone(),
                r.get::<_, String>(2)?,
                rank,
                r.get::<_, i64>(0)?,
                json!({
                    "unit_kind": kind,
                    "unit": unit_key[prefix.len()..],
                    "question": r.get::<_, String>(2)?,
                    "source": source,
                    "label": r.get::<_, Option<String>>(4)?,
                    "probability": r.get::<_, Option<f64>>(5)?,
                    "confidence": r.get::<_, Option<f64>>(6)?,
                    "band": band,
                    "probabilities": probabilities
                        .and_then(|p| serde_json::from_str::<Value>(&p).ok()),
                    "model": r.get::<_, Option<String>>(9)?,
                    "created_at": r.get::<_, String>(10)?,
                    "superseded": superseded,
                }),
            ))
        })?;
        for row in rows {
            let (key, question, rank, id, value) = row?;
            out.push((
                format!("{kind}\u{0}{key}"),
                question,
                format!("{rank}"),
                -id,
                value,
            ));
        }
    }
    let mut stmt = conn.prepare(
        "SELECT unit_kind, unit_key, question_id, label, note, created_at
         FROM classification_override WHERE unit_key >= ?1 AND unit_key < ?2",
    )?;
    let rows = stmt.query_map(params![prefix, end], |r| {
        let kind: String = r.get(0)?;
        let key: String = r.get(1)?;
        Ok((
            format!("{kind}\u{0}{key}"),
            r.get::<_, String>(2)?,
            "0".to_string(),
            0,
            json!({
                "unit_kind": kind,
                "unit": key[prefix.len()..],
                "question": r.get::<_, String>(2)?,
                "source": "override",
                "label": r.get::<_, String>(3)?,
                "band": "override",
                "note": r.get::<_, Option<String>>(4)?,
                "created_at": r.get::<_, String>(5)?,
                "superseded": false,
            }),
        ))
    })?;
    for row in rows {
        out.push(row?);
    }
    out.sort_by(|a, b| (&a.0, &a.1, &a.2, a.3).cmp(&(&b.0, &b.1, &b.2, b.3)));
    let mut seen: Option<(String, String)> = None;
    Ok(out
        .into_iter()
        .map(|(unit, question, _, _, mut value)| {
            let first = seen.as_ref() != Some(&(unit.clone(), question.clone()));
            if first {
                seen = Some((unit, question));
            }
            value["effective"] =
                Value::Bool(first && !value["superseded"].as_bool().unwrap_or(false));
            value
        })
        .collect())
}

/// Tasks of the session with the goal each belongs to: the goal's state, its
/// conversations, and the links into and out of this task.
fn tasks(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT t.task_index, t.first_turn, t.last_turn, t.completion_turn, t.title, t.started_at,
                t.completed_at, t.completion_source, t.segmentation_source, m.identity_id,
                i.work_id, t.cost_total_usd, t.commits
         FROM derived_task t
         LEFT JOIN derived_work_member m
           ON m.provider = t.provider AND m.session_id = t.session_id AND m.task_index = t.task_index
         LEFT JOIN derived_work_index_member i
           ON i.provider = t.provider AND i.session_id = t.session_id AND i.task_index = t.task_index
         WHERE t.provider = ?1 AND t.session_id = ?2 ORDER BY t.task_index",
    )?;
    let rows = stmt
        .query_map(params![provider, session_id], |r| {
            Ok((
                r.get::<_, Option<String>>(9)?,
                r.get::<_, Option<String>>(10)?,
                json!({
                    "task": r.get::<_, i64>(0)?,
                    "first_turn": r.get::<_, Option<i64>>(1)?,
                    "last_turn": r.get::<_, Option<i64>>(2)?,
                    "completion_turn": r.get::<_, Option<i64>>(3)?,
                    "title": r.get::<_, Option<String>>(4)?,
                    "started_at": r.get::<_, Option<String>>(5)?,
                    "completed_at": r.get::<_, Option<String>>(6)?,
                    "completion_source": r.get::<_, Option<String>>(7)?,
                    "segmentation": r.get::<_, Option<String>>(8)?,
                    "cost": r.get::<_, Option<f64>>(11)?,
                    "commits": r.get::<_, Option<i64>>(12)?,
                }),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for (identity, work_id, mut task) in rows {
        task["identity"] = json!(identity);
        if let Some(identity) = &identity {
            task["relations"] = Value::Array(relations(conn, identity)?);
        }
        if let Some(work_id) = &work_id {
            task["goal"] = goal(conn, work_id, provider, session_id)?;
        }
        out.push(task);
    }
    Ok(out)
}

fn goal(conn: &Connection, work_id: &str, provider: &str, session_id: &str) -> Result<Value> {
    let index = conn
        .query_row(
            "SELECT intent, intent_source, state, verified, conversations, attempts, corrections,
                    validation_failures, started_ms, completed_ms, last_ms
             FROM derived_work_index WHERE work_id = ?1",
            [work_id],
            |r| {
                Ok(json!({
                    "intent": r.get::<_, Option<String>>(0)?,
                    "intent_source": r.get::<_, Option<String>>(1)?,
                    "state": r.get::<_, Option<String>>(2)?,
                    "verified": r.get::<_, Option<i64>>(3)?,
                    "conversations": r.get::<_, Option<i64>>(4)?,
                    "attempts": r.get::<_, Option<i64>>(5)?,
                    "corrections": r.get::<_, Option<i64>>(6)?,
                    "validation_failures": r.get::<_, Option<i64>>(7)?,
                    "started_ms": r.get::<_, Option<i64>>(8)?,
                    "completed_ms": r.get::<_, Option<i64>>(9)?,
                    "last_ms": r.get::<_, Option<i64>>(10)?,
                }))
            },
        )
        .optional()?;
    let mut stmt = conn.prepare(
        "SELECT t.provider, t.session_id, t.task_index, t.title, t.started_at, t.completed_at
         FROM derived_work_index_member i
         JOIN derived_task t
           ON t.provider = i.provider AND t.session_id = i.session_id AND t.task_index = i.task_index
         WHERE i.work_id = ?1 ORDER BY t.started_at, t.task_index",
    )?;
    let members = stmt
        .query_map([work_id], |r| {
            let p: String = r.get(0)?;
            let s: String = r.get(1)?;
            Ok(json!({
                "this": p == provider && s == session_id,
                "provider": p,
                "session_id": s,
                "task": r.get::<_, i64>(2)?,
                "title": r.get::<_, Option<String>>(3)?,
                "started_at": r.get::<_, Option<String>>(4)?,
                "completed_at": r.get::<_, Option<String>>(5)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut goal = index.unwrap_or_else(|| json!({}));
    goal["id"] = json!(work_id);
    goal["members"] = Value::Array(members);
    Ok(goal)
}

fn relations(conn: &Connection, identity: &str) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT CASE WHEN r.to_id = ?1 THEN 'from' ELSE 'to' END, r.kind, r.decision, r.source,
                r.confidence, r.evidence, r.created_at, m.provider, m.session_id, m.task_index,
                t.title
         FROM work_relation r
         LEFT JOIN derived_work_member m
           ON m.identity_id = CASE WHEN r.to_id = ?1 THEN r.from_id ELSE r.to_id END
         LEFT JOIN derived_task t
           ON t.provider = m.provider AND t.session_id = m.session_id AND t.task_index = m.task_index
         WHERE r.to_id = ?1 OR r.from_id = ?1
         ORDER BY r.created_at",
    )?;
    let rows = stmt
        .query_map([identity], |r| {
            Ok(json!({
                "direction": r.get::<_, String>(0)?,
                "kind": r.get::<_, String>(1)?,
                "decision": r.get::<_, String>(2)?,
                "source": r.get::<_, String>(3)?,
                "confidence": r.get::<_, Option<f64>>(4)?,
                "evidence": r.get::<_, String>(5)?,
                "created_at": r.get::<_, String>(6)?,
                "provider": r.get::<_, Option<String>>(7)?,
                "session_id": r.get::<_, Option<String>>(8)?,
                "task": r.get::<_, Option<i64>>(9)?,
                "title": r.get::<_, Option<String>>(10)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// The context ledger: each API call's context and every item that entered
/// it, with how many later calls carried the item and what that cost.
fn context(conn: &Connection, provider: &str, agents: &[String]) -> Result<Value> {
    let mut calls = Vec::new();
    let mut items = Vec::new();
    let mut call_stmt = conn.prepare(
        "SELECT call_index, usage_id, ts, model_name, context_tokens, new_tokens, carried_tokens,
                cache_event, input_cost_usd, rebuild_extra_usd, residual_tokens, context_window
         FROM derived_api_call WHERE provider = ?1 AND session_id = ?2 ORDER BY call_index",
    )?;
    let mut item_stmt = conn.prepare(
        "SELECT item_key, ts, kind, tool_name, tool_category, mcp_server, target, source,
                entered_call, evicted_call, carried_calls, chars, est_tokens, tokens, write_usd,
                carry_usd, rebuild_usd, total_usd, peak_share, purpose
         FROM derived_context_item WHERE provider = ?1 AND session_id = ?2
         ORDER BY entered_call, ts, item_key",
    )?;
    for agent in agents {
        let rows = call_stmt.query_map(params![provider, agent], |r| {
            Ok(json!({
                "agent": agent,
                "call": r.get::<_, i64>(0)?,
                "usage_id": r.get::<_, i64>(1)?,
                "ts": r.get::<_, Option<String>>(2)?,
                "model": r.get::<_, Option<String>>(3)?,
                "context": r.get::<_, i64>(4)?,
                "new": r.get::<_, i64>(5)?,
                "carried": r.get::<_, i64>(6)?,
                "cache_event": r.get::<_, String>(7)?,
                "input_cost": r.get::<_, Option<f64>>(8)?,
                "rebuild_extra": r.get::<_, Option<f64>>(9)?,
                "residual": r.get::<_, i64>(10)?,
                "window": r.get::<_, Option<i64>>(11)?,
            }))
        })?;
        for row in rows {
            calls.push(row?);
        }
        let rows = item_stmt.query_map(params![provider, agent], |r| {
            Ok(json!({
                "agent": agent,
                "key": r.get::<_, String>(0)?,
                "ts": r.get::<_, Option<String>>(1)?,
                "kind": r.get::<_, String>(2)?,
                "tool": r.get::<_, Option<String>>(3)?,
                "category": r.get::<_, Option<String>>(4)?,
                "mcp": r.get::<_, Option<String>>(5)?,
                "target": r.get::<_, Option<String>>(6)?,
                "source": r.get::<_, Option<String>>(7)?,
                "entered": r.get::<_, i64>(8)?,
                "evicted": r.get::<_, Option<i64>>(9)?,
                "carried": r.get::<_, i64>(10)?,
                "chars": r.get::<_, i64>(11)?,
                "est_tokens": r.get::<_, i64>(12)?,
                "tokens": r.get::<_, i64>(13)?,
                "write_usd": r.get::<_, Option<f64>>(14)?,
                "carry_usd": r.get::<_, Option<f64>>(15)?,
                "rebuild_usd": r.get::<_, Option<f64>>(16)?,
                "total_usd": r.get::<_, Option<f64>>(17)?,
                "peak_share": r.get::<_, Option<f64>>(18)?,
                "purpose": r.get::<_, Option<String>>(19)?,
            }))
        })?;
        for row in rows {
            items.push(row?);
        }
    }
    Ok(json!({ "calls": calls, "items": items }))
}

/// Context footprint per timeline source, keyed `agent item_key`, for
/// annotating messages and tool calls with how often they were carried.
pub fn footprints(
    conn: &Connection,
    provider: &str,
    agents: &[String],
) -> Result<HashMap<String, Value>> {
    let mut out = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT item_key, tokens, carried_calls, total_usd, entered_call, evicted_call
         FROM derived_context_item WHERE provider = ?1 AND session_id = ?2",
    )?;
    let mut calls = conn.prepare(
        "SELECT usage_id, call_index, context_tokens, new_tokens, carried_tokens, cache_event
         FROM derived_api_call WHERE provider = ?1 AND session_id = ?2",
    )?;
    for agent in agents {
        let rows = stmt.query_map(params![provider, agent], |r| {
            Ok((
                r.get::<_, String>(0)?,
                json!({
                    "tokens": r.get::<_, i64>(1)?,
                    "carried": r.get::<_, i64>(2)?,
                    "usd": r.get::<_, Option<f64>>(3)?,
                    "entered": r.get::<_, i64>(4)?,
                    "evicted": r.get::<_, Option<i64>>(5)?,
                }),
            ))
        })?;
        for row in rows {
            let (key, value) = row?;
            out.insert(format!("{agent} {key}"), value);
        }
        let rows = calls.query_map(params![provider, agent], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                json!({
                    "call": r.get::<_, i64>(1)?,
                    "context": r.get::<_, i64>(2)?,
                    "new": r.get::<_, i64>(3)?,
                    "carried": r.get::<_, i64>(4)?,
                    "cache_event": r.get::<_, String>(5)?,
                }),
            ))
        })?;
        for row in rows {
            let (usage, value) = row?;
            out.insert(format!("{agent} usage:{usage}"), value);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;

    fn answer(conn: &Connection, key: &str, source: &str, label: &str, band: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO classification_result
               (unit_kind, unit_key, question_id, question_version, source, label, band, cache_key)
             VALUES ('turn', ?1, 'request_kind', 'v1', ?2, ?3, ?4, ?1 || ?2 || ?3)",
            params![key, source, label, band],
        )?;
        Ok(())
    }

    #[test]
    fn marks_the_label_reports_use_per_question() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        crate::insights::init_schema(&conn)?;
        answer(
            &conn,
            "opencode|s1|0",
            "heuristic",
            "new_request",
            "confident",
        )?;
        answer(&conn, "opencode|s1|0", "jev", "follow_up", "tentative")?;
        answer(
            &conn,
            "opencode|s1|1",
            "heuristic",
            "new_request",
            "confident",
        )?;
        answer(&conn, "opencode|s1|1", "jev", "correction", "uncertain")?;
        answer(&conn, "opencode|s10|0", "jev", "other_session", "confident")?;

        let labels = labels(&conn, "opencode", "s1")?;
        assert_eq!(labels.len(), 4, "another session's labels stay out");
        let chosen: Vec<(&str, &str)> = labels
            .iter()
            .filter(|l| l["effective"] == true)
            .map(|l| (l["unit"].as_str().unwrap(), l["label"].as_str().unwrap()))
            .collect();
        assert_eq!(chosen, [("0", "follow_up"), ("1", "new_request")]);
        // Every query of the page runs against the current schema.
        let all = inspect(&conn, "opencode", "s1", &["s1".to_string()])?;
        assert_eq!(all["labels"].as_array().map(Vec::len), Some(4));
        Ok(())
    }
}
