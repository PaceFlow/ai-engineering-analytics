//! One session laid out for human review: the full timeline (messages, tool
//! calls, model calls, events) next to the key numbers vca derives from it.
//! A reviewer approves or rejects each number; approved values become an
//! expectations file that `check` asserts on later runs.

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::{SessionDetail, session_detail};

/// Characters of message or tool text kept per timeline entry.
const BODY_LIMIT: usize = 6_000;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Entry {
    pub id: usize,
    pub ts: Option<String>,
    /// `main` or the subagent's session id.
    pub agent: String,
    /// user, assistant, compaction, tool, call, event
    pub kind: String,
    pub turn: Option<i64>,
    pub task: Option<i64>,
    pub title: String,
    pub body: String,
    pub meta: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Stat {
    pub id: String,
    pub group: String,
    pub label: String,
    pub value: Value,
    pub display: String,
    /// Timeline entries that back the number.
    pub entries: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReviewData {
    pub key: String,
    pub pricing: String,
    pub detail: SessionDetail,
    pub turns: Vec<TurnMark>,
    pub timeline: Vec<Entry>,
    pub stats: Vec<Stat>,
    /// Files the session (subagents included) changed, one row per path.
    pub files: Vec<FileTouch>,
}

/// Lines one session changed in one file. `session` counts come from the
/// provider's edit records (what `lines_changed` uses); `delivery` counts from
/// the change extraction that commit attribution matches against.
#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct FileTouch {
    pub path: String,
    pub rel_path: Option<String>,
    pub repo_root: Option<String>,
    pub edits: i64,
    pub session_added: i64,
    pub session_removed: i64,
    pub delivery_added: i64,
    pub delivery_removed: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TurnMark {
    pub turn: i64,
    pub started_at: Option<String>,
    pub task: Option<i64>,
    pub stage: Option<String>,
    pub request_kind: Option<String>,
    pub cost: f64,
}

/// What a reviewer approved for one session.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Expectations {
    /// `provider:session_id`
    pub session: String,
    #[serde(default)]
    pub approved: BTreeMap<String, Value>,
    #[serde(default)]
    pub rejected: BTreeMap<String, Rejected>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Rejected {
    pub value: Value,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Mismatch {
    pub id: String,
    pub expected: Value,
    /// `None` when vca no longer produces the number.
    pub actual: Option<Value>,
}

pub fn review(conn: &Connection, provider: &str, session_id: &str) -> Result<Option<ReviewData>> {
    let Some(detail) = session_detail(conn, provider, session_id)? else {
        return Ok(None);
    };
    let turns = turn_marks(&detail);
    let agents = agent_ids(conn, provider, session_id)?;
    let timeline = timeline(conn, provider, session_id, &agents, &turns)?;
    let stats = stats(conn, &detail, &timeline)?;
    let files = file_touches(conn, provider, &agents)?;
    Ok(Some(ReviewData {
        key: format!("{provider}:{session_id}"),
        pricing: crate::cost::pricing::pricing_version(),
        detail,
        turns,
        timeline,
        stats,
        files,
    }))
}

fn file_touches(conn: &Connection, provider: &str, sessions: &[String]) -> Result<Vec<FileTouch>> {
    let mut stmt = conn.prepare(
        "SELECT COALESCE(abs_path, rel_path, '(unknown)'), rel_path, repo_root, source_kind,
                lines_added, lines_removed, change_ts
         FROM fact_session_code_change
         WHERE provider = ?1 AND session_id = ?2",
    )?;
    let mut by_path: BTreeMap<String, FileTouch> = BTreeMap::new();
    for session in sessions {
        let rows = stmt.query_map(rusqlite::params![provider, session], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;
        for row in rows {
            let (path, rel_path, repo_root, kind, added, removed, ts) = row?;
            let touch = by_path.entry(path.clone()).or_insert_with(|| FileTouch {
                path,
                ..FileTouch::default()
            });
            touch.rel_path = touch.rel_path.take().or(rel_path);
            touch.repo_root = touch.repo_root.take().or(repo_root);
            if kind == "accepted_change" {
                touch.edits += 1;
                touch.session_added += added;
                touch.session_removed += removed;
            } else {
                touch.delivery_added += added;
                touch.delivery_removed += removed;
            }
            if let Some(ts) = ts {
                if touch
                    .first_ts
                    .as_deref()
                    .is_none_or(|first| ts.as_str() < first)
                {
                    touch.first_ts = Some(ts.clone());
                }
                if touch
                    .last_ts
                    .as_deref()
                    .is_none_or(|last| ts.as_str() > last)
                {
                    touch.last_ts = Some(ts);
                }
            }
        }
    }
    let mut files: Vec<FileTouch> = by_path.into_values().collect();
    files.sort_by(|a, b| {
        (b.session_added + b.session_removed + b.delivery_added + b.delivery_removed)
            .cmp(&(a.session_added + a.session_removed + a.delivery_added + a.delivery_removed))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(files)
}

/// Compares approved values with what vca derives now.
pub fn check(conn: &Connection, expected: &Expectations) -> Result<Vec<Mismatch>> {
    let (provider, session_id) = expected
        .session
        .split_once(':')
        .context("expectations `session` must be provider:session_id")?;
    let data = review(conn, provider, session_id)?
        .with_context(|| format!("session {} not found", expected.session))?;
    let actual: HashMap<&str, &Value> = data
        .stats
        .iter()
        .map(|stat| (stat.id.as_str(), &stat.value))
        .collect();
    Ok(expected
        .approved
        .iter()
        .filter_map(|(id, value)| match actual.get(id.as_str()) {
            Some(now) if same(value, now) => None,
            now => Some(Mismatch {
                id: id.clone(),
                expected: value.clone(),
                actual: now.map(|v| (*v).clone()),
            }),
        })
        .collect())
}

/// Equal, with numbers compared to six decimals.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let (x, y) = (
                x.as_f64().unwrap_or(f64::NAN),
                y.as_f64().unwrap_or(f64::NAN),
            );
            (x - y).abs() < 5e-7
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn turn_marks(detail: &SessionDetail) -> Vec<TurnMark> {
    detail
        .turns
        .iter()
        .map(|turn| TurnMark {
            turn: turn.turn_index,
            started_at: turn.started_at.clone(),
            task: turn.task_index,
            stage: turn.stage.clone(),
            request_kind: turn.request_kind.clone(),
            cost: turn.cost,
        })
        .collect()
}

fn agent_ids(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT session_id FROM metadata_sessions
         WHERE provider = ?1 AND COALESCE(NULLIF(root_session_id, ''), session_id) = ?2
         ORDER BY CASE WHEN session_id = ?2 THEN 0 ELSE 1 END, started_at",
    )?;
    let mut ids = stmt
        .query_map(rusqlite::params![provider, session_id], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    if ids.is_empty() {
        ids.push(session_id.to_string());
    }
    Ok(ids)
}

/// The turn an entry belongs to: the last turn that started at or before it.
fn turn_at(turns: &[TurnMark], ts: Option<&str>) -> Option<i64> {
    let ts = ts?;
    turns
        .iter()
        .filter(|turn| turn.started_at.as_deref().is_some_and(|start| start <= ts))
        .map(|turn| turn.turn)
        .next_back()
        .or_else(|| turns.first().map(|turn| turn.turn))
}

fn clip(text: &str) -> String {
    if text.chars().count() <= BODY_LIMIT {
        return text.to_string();
    }
    let kept: String = text.chars().take(BODY_LIMIT).collect();
    format!(
        "{kept}\n… ({} more characters)",
        text.chars().count() - BODY_LIMIT
    )
}

fn first_line(text: &str, max: usize) -> String {
    let line = text
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim();
    if line.chars().count() <= max {
        line.to_string()
    } else {
        format!("{}…", line.chars().take(max).collect::<String>())
    }
}

/// (ts, source order, index, entry without id)
type Pending = (Option<String>, u8, i64, Entry);

fn timeline(
    conn: &Connection,
    provider: &str,
    root: &str,
    agents: &[String],
    turns: &[TurnMark],
) -> Result<Vec<Entry>> {
    let mut pending: Vec<Pending> = Vec::new();
    let agent_label = |id: &str| {
        if id == root {
            "main".to_string()
        } else {
            id.to_string()
        }
    };
    let entry =
        |ts: Option<String>, agent: &str, kind: &str, title: String, body: String, meta| Entry {
            id: 0,
            ts,
            agent: agent_label(agent),
            kind: kind.to_string(),
            turn: None,
            task: None,
            title,
            body,
            meta,
        };

    for agent in agents {
        let mut stmt = conn.prepare(
            "SELECT message_index, message_ts, role, content, COALESCE(is_compaction_summary, 0)
             FROM fact_session_message WHERE provider = ?1 AND session_id = ?2
             ORDER BY message_index",
        )?;
        let rows = stmt.query_map(rusqlite::params![provider, agent], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)? != 0,
            ))
        })?;
        for row in rows {
            let (index, ts, role, content, compaction) = row?;
            let kind = if compaction {
                "compaction"
            } else if role == "user" {
                "user"
            } else {
                "assistant"
            };
            let title = first_line(&content, 120);
            let meta = json!({ "chars": content.chars().count() });
            pending.push((
                ts.clone(),
                0,
                index,
                entry(ts, agent, kind, title, clip(&content), meta),
            ));
        }

        let mut stmt = conn.prepare(
            "SELECT call_index, ts, ended_ts, tool_name, tool_category, validation_kind, target,
                    input_chars, output_chars, output_in_context, status, exit_code,
                    error_signature, child_session_id, agent_type
             FROM fact_session_tool_call WHERE provider = ?1 AND session_id = ?2
             ORDER BY call_index",
        )?;
        let rows = stmt.query_map(rusqlite::params![provider, agent], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                json!({
                    "ended": row.get::<_, Option<String>>(2)?,
                    "tool": row.get::<_, String>(3)?,
                    "category": row.get::<_, String>(4)?,
                    "validation": row.get::<_, Option<String>>(5)?,
                    "target": row.get::<_, Option<String>>(6)?,
                    "input_chars": row.get::<_, i64>(7)?,
                    "output_chars": row.get::<_, i64>(8)?,
                    "in_context": row.get::<_, i64>(9)? != 0,
                    "status": row.get::<_, String>(10)?,
                    "exit_code": row.get::<_, Option<i64>>(11)?,
                    "error": row.get::<_, Option<String>>(12)?,
                    "child_session": row.get::<_, Option<String>>(13)?,
                    "agent_type": row.get::<_, Option<String>>(14)?,
                }),
            ))
        })?;
        for row in rows {
            let (index, ts, meta) = row?;
            let tool = meta["tool"].as_str().unwrap_or("").to_string();
            let target = meta["target"].as_str().unwrap_or("");
            let title = if target.is_empty() {
                tool
            } else {
                format!("{tool} · {}", first_line(target, 160))
            };
            let body = meta["error"].as_str().unwrap_or("").to_string();
            pending.push((
                ts.clone(),
                1,
                index,
                entry(ts, agent, "tool", title, body, meta),
            ));
        }

        let mut stmt = conn.prepare(
            "SELECT u.usage_id, u.usage_ts, COALESCE(u.model_name, '(unknown)'), u.root_turn_index,
                    u.fresh_tokens, u.cache_read_tokens, COALESCE(f.cache_creation_tokens, 0),
                    COALESCE(f.cache_creation_1h_tokens, 0), u.output_tokens,
                    COALESCE(f.reasoning_tokens, 0), u.total_tokens, u.context_tokens,
                    u.api_equiv_usd, u.price_match, u.token_source, u.background_flag,
                    COALESCE(f.input_tokens, u.fresh_tokens - u.output_tokens)
             FROM derived_usage_event u
             LEFT JOIN fact_session_usage f ON f.id = u.usage_id
             WHERE u.provider = ?1 AND u.session_id = ?2
             ORDER BY u.usage_ts, u.usage_id",
        )?;
        let rows = stmt.query_map(rusqlite::params![provider, agent], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                json!({
                    "model": row.get::<_, String>(2)?,
                    "input": row.get::<_, i64>(16)?,
                    "fresh": row.get::<_, i64>(4)?,
                    "cache_read": row.get::<_, i64>(5)?,
                    "cache_write": row.get::<_, i64>(6)?,
                    "cache_write_1h": row.get::<_, i64>(7)?,
                    "output": row.get::<_, i64>(8)?,
                    "reasoning": row.get::<_, i64>(9)?,
                    "total": row.get::<_, i64>(10)?,
                    "context": row.get::<_, i64>(11)?,
                    "cost": row.get::<_, Option<f64>>(12)?,
                    "price_match": row.get::<_, Option<String>>(13)?,
                    "token_source": row.get::<_, String>(14)?,
                    "background": row.get::<_, i64>(15)? != 0,
                }),
            ))
        })?;
        for row in rows {
            let (id, ts, model, root_turn, meta) = row?;
            let title = crate::explore::short_model(&model);
            let mut item = entry(ts.clone(), agent, "call", title, String::new(), meta);
            item.turn = root_turn;
            pending.push((ts, 2, id, item));
        }

        let mut stmt = conn.prepare(
            "SELECT event_index, ts, kind, detail FROM fact_session_event
             WHERE provider = ?1 AND session_id = ?2 ORDER BY event_index",
        )?;
        let rows = stmt.query_map(rusqlite::params![provider, agent], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (index, ts, kind, detail) = row?;
            let body = detail.unwrap_or_default();
            pending.push((
                ts.clone(),
                3,
                index,
                entry(
                    ts,
                    agent,
                    "event",
                    kind.clone(),
                    clip(&body),
                    json!({ "event": kind }),
                ),
            ));
        }
    }

    // Stable: entries without a timestamp keep their place after the previous one.
    let mut last_ts: Option<String> = None;
    for item in pending.iter_mut() {
        if item.0.is_none() {
            item.0 = last_ts.clone();
        } else {
            last_ts = item.0.clone();
        }
    }
    pending.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let task_of: HashMap<i64, Option<i64>> = turns.iter().map(|t| (t.turn, t.task)).collect();
    Ok(pending
        .into_iter()
        .enumerate()
        .map(|(id, (_, _, _, mut item))| {
            item.id = id;
            if item.turn.is_none() {
                item.turn = turn_at(turns, item.ts.as_deref());
            }
            item.task = item
                .turn
                .and_then(|turn| task_of.get(&turn).copied().flatten());
            item
        })
        .collect())
}

fn round(value: f64, places: i32) -> f64 {
    let factor = 10f64.powi(places);
    (value * factor).round() / factor
}

fn usd(value: Option<f64>) -> Value {
    value.map_or(Value::Null, |v| json!(round(v, 6)))
}

fn minutes(value: Option<f64>) -> Value {
    value.map_or(Value::Null, |v| json!(round(v, 1)))
}

fn grouped(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

fn show(value: &Value) -> String {
    match value {
        Value::Null => "—".into(),
        Value::Number(n) if n.is_i64() => grouped(n.as_i64().unwrap_or(0)),
        Value::String(text) => text.clone(),
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| format!("{k} {}", show(v)))
            .collect::<Vec<_>>()
            .join(" · "),
        Value::Array(items) => items.iter().map(show).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

struct Stats<'a> {
    out: Vec<Stat>,
    timeline: &'a [Entry],
}

impl Stats<'_> {
    fn add(&mut self, id: &str, group: &str, label: &str, value: Value, entries: Vec<usize>) {
        let display = match value.as_f64() {
            Some(cost) if id.ends_with("_usd") => format!("${cost:.4}"),
            _ => show(&value),
        };
        self.add_with(id, group, label, value, display, entries);
    }

    fn add_with(
        &mut self,
        id: &str,
        group: &str,
        label: &str,
        value: Value,
        display: String,
        entries: Vec<usize>,
    ) {
        self.out.push(Stat {
            id: id.to_string(),
            group: group.to_string(),
            label: label.to_string(),
            value,
            display,
            entries,
        });
    }

    fn ids(&self, keep: impl Fn(&Entry) -> bool) -> Vec<usize> {
        self.timeline
            .iter()
            .filter(|e| keep(e))
            .map(|e| e.id)
            .collect()
    }
}

fn wall_minutes(start: Option<&str>, end: Option<&str>) -> Option<f64> {
    let parse = |ts: &str| chrono::DateTime::parse_from_rfc3339(ts).ok();
    let (start, end) = (parse(start?)?, parse(end?)?);
    Some((end - start).num_seconds() as f64 / 60.0)
}

fn stats(conn: &Connection, detail: &SessionDetail, timeline: &[Entry]) -> Result<Vec<Stat>> {
    let s = &detail.session;
    let mut st = Stats {
        out: Vec::new(),
        timeline,
    };
    let calls = |e: &Entry| e.kind == "call";
    let sum = |field: &str| -> i64 {
        timeline
            .iter()
            .filter(|e| calls(e))
            .map(|e| e.meta[field].as_i64().unwrap_or(0))
            .sum()
    };

    // Cost and tokens.
    let all_calls = st.ids(calls);
    st.add(
        "cost.total_usd",
        "Cost & tokens",
        "API-equivalent cost",
        usd(s.cost),
        all_calls.clone(),
    );
    let sub_calls = st.ids(|e| calls(e) && e.agent != "main");
    st.add(
        "cost.subagent_usd",
        "Cost & tokens",
        "of which subagents",
        usd(s.subagent_cost),
        sub_calls,
    );
    st.add(
        "tokens.total",
        "Cost & tokens",
        "Tokens, total",
        json!(s.tokens_total),
        all_calls.clone(),
    );
    st.add(
        "tokens.input_uncached",
        "Cost & tokens",
        "Input tokens, not from cache",
        json!(sum("input")),
        all_calls.clone(),
    );
    st.add(
        "tokens.fresh",
        "Cost & tokens",
        "Fresh tokens (uncached input + cache write + output)",
        json!(sum("fresh")),
        all_calls.clone(),
    );
    st.add(
        "tokens.cache_read",
        "Cost & tokens",
        "Cache-read tokens",
        json!(sum("cache_read")),
        all_calls.clone(),
    );
    st.add(
        "tokens.cache_write",
        "Cost & tokens",
        "Cache-write tokens",
        json!(sum("cache_write")),
        all_calls.clone(),
    );
    st.add(
        "tokens.output",
        "Cost & tokens",
        "Output tokens",
        json!(sum("output")),
        all_calls.clone(),
    );
    st.add(
        "tokens.reasoning",
        "Cost & tokens",
        "Reasoning tokens",
        json!(sum("reasoning")),
        all_calls.clone(),
    );
    st.add(
        "calls.model_calls",
        "Cost & tokens",
        "Model calls",
        json!(all_calls.len()),
        all_calls,
    );
    st.add(
        "tokens.unpriced",
        "Cost & tokens",
        "Unpriced tokens (no price match)",
        json!(s.unpriced_tokens),
        st.ids(|e| calls(e) && e.meta["cost"].is_null()),
    );
    let mut by_model: BTreeMap<String, (i64, f64)> = BTreeMap::new();
    for e in timeline.iter().filter(|e| calls(e)) {
        let model = e.meta["model"].as_str().unwrap_or("(unknown)").to_string();
        let slot = by_model.entry(model).or_default();
        slot.0 += 1;
        slot.1 += e.meta["cost"].as_f64().unwrap_or(0.0);
    }
    for (model, (count, cost)) in &by_model {
        let id_model = model.clone();
        st.add_with(
            &format!("model.{model}"),
            "Models",
            model,
            json!({ "calls": count, "cost_usd": round(*cost, 6) }),
            format!("{} calls · ${cost:.4}", grouped(*count)),
            st.ids(|e| calls(e) && e.meta["model"].as_str() == Some(id_model.as_str())),
        );
    }

    // Time and shape.
    let users = st.ids(|e| e.kind == "user" && e.agent == "main");
    st.add(
        "time.wall_minutes",
        "Time & shape",
        "Wall time, first to last event (min)",
        minutes(wall_minutes(s.started_at.as_deref(), s.ended_at.as_deref())),
        vec![],
    );
    st.add(
        "time.active_minutes",
        "Time & shape",
        "Active time (min)",
        minutes(s.active_minutes),
        vec![],
    );
    st.add(
        "shape.turns",
        "Time & shape",
        "Turns (user requests)",
        json!(s.turns),
        users.clone(),
    );
    st.add(
        "shape.user_messages",
        "Time & shape",
        "User messages, main agent",
        json!(users.len()),
        users,
    );
    st.add(
        "shape.subagents",
        "Time & shape",
        "Subagent sessions",
        json!(
            detail
                .agents
                .iter()
                .filter(|a| a.agent_kind != "main")
                .count()
        ),
        st.ids(|e| e.agent != "main"),
    );
    let compactions =
        st.ids(|e| e.kind == "compaction" || (e.kind == "event" && e.title == "compaction"));
    st.add(
        "shape.compactions",
        "Time & shape",
        "Compactions",
        json!(
            detail
                .events
                .iter()
                .filter(|e| e.kind == "compaction")
                .count()
        ),
        compactions,
    );

    // Tools.
    let tools = |e: &Entry| e.kind == "tool";
    st.add(
        "tools.calls",
        "Tools",
        "Tool calls (all agents)",
        json!(st.ids(tools).len()),
        st.ids(tools),
    );
    let mut by_category: BTreeMap<String, i64> = BTreeMap::new();
    for e in timeline.iter().filter(|e| tools(e)) {
        *by_category
            .entry(e.meta["category"].as_str().unwrap_or("other").to_string())
            .or_default() += 1;
    }
    for (category, count) in &by_category {
        let cat = category.clone();
        st.add(
            &format!("tools.{category}"),
            "Tools",
            &format!("{category} calls"),
            json!(count),
            st.ids(|e| tools(e) && e.meta["category"].as_str() == Some(cat.as_str())),
        );
    }
    let failed =
        |e: &Entry| tools(e) && matches!(e.meta["status"].as_str(), Some("error" | "failed"));
    st.add(
        "tools.failed",
        "Tools",
        "Tool calls that failed",
        json!(st.ids(failed).len()),
        st.ids(failed),
    );
    let edits = |e: &Entry| tools(e) && e.meta["category"].as_str() == Some("edit");
    let files: BTreeSet<String> = timeline
        .iter()
        .filter(|e| edits(e))
        .filter_map(|e| e.meta["target"].as_str())
        .map(|t| {
            t.split(" :: ")
                .next()
                .unwrap_or(t)
                .trim()
                .replace('\\', "/")
        })
        .filter(|t| !t.is_empty())
        .collect();
    st.add(
        "tools.files_edited",
        "Tools",
        "Distinct files edited",
        json!(files.len()),
        st.ids(edits),
    );
    let checks = |e: &Entry| tools(e) && !e.meta["validation"].is_null();
    let check_fail = |e: &Entry| checks(e) && failed(e);
    st.add(
        "checks.runs",
        "Tools",
        "Validation runs (tests, builds, lint)",
        json!(st.ids(checks).len()),
        st.ids(checks),
    );
    st.add(
        "checks.failures",
        "Tools",
        "Validation failures",
        json!(st.ids(check_fail).len()),
        st.ids(check_fail),
    );

    // Context.
    for kind in &detail.context_kinds {
        st.add_with(
            &format!("context.{}", kind.key),
            "Context",
            &format!("{} in context", kind.key),
            json!({ "items": kind.items, "tokens": kind.tokens, "total_usd": round(kind.total_usd.unwrap_or(0.0), 6) }),
            format!(
                "{} items · {} tokens · ${:.4}",
                grouped(kind.items),
                grouped(kind.tokens),
                kind.total_usd.unwrap_or(0.0)
            ),
            vec![],
        );
    }

    // Delivery.
    st.add(
        "delivery.commits",
        "Delivery",
        "Commits associated",
        json!(detail.commits.len()),
        vec![],
    );
    st.add(
        "delivery.ai_lines",
        "Delivery",
        "AI lines matched in commits",
        json!(
            round(
                detail.commits.iter().map(|c| c.matched_lines).sum::<f64>(),
                1
            ) + 0.0
        ),
        vec![],
    );
    let relations: i64 = conn.query_row(
        "SELECT COUNT(*) FROM derived_task_relation
         WHERE (from_provider = ?1 AND from_session = ?2) OR (to_provider = ?1 AND to_session = ?2)",
        rusqlite::params![s.provider, s.session_id],
        |row| row.get(0),
    )?;
    st.add(
        "delivery.rework_relations",
        "Delivery",
        "Rework relations touching this session",
        json!(relations),
        vec![],
    );

    // Tasks.
    st.add(
        "tasks.count",
        "Tasks",
        "Tasks",
        json!(detail.tasks.len()),
        vec![],
    );
    st.add(
        "tasks.completed",
        "Tasks",
        "Tasks completed",
        json!(s.completed_tasks),
        vec![],
    );
    let habits = habit_values(conn, &s.provider, &s.session_id)?;
    for task in &detail.tasks {
        let n = task.task_index;
        let group = format!("Task T{n}: {}", super::format::truncate(&task.title, 60));
        let in_task = st.ids(|e| e.task == Some(n));
        st.add(
            &format!("task.{n}.turns"),
            &group,
            "Turns",
            json!(task.turns),
            in_task.clone(),
        );
        st.add(
            &format!("task.{n}.cost_total_usd"),
            &group,
            "Cost, whole task",
            usd(task.cost_total),
            in_task.clone(),
        );
        st.add(
            &format!("task.{n}.cost_to_complete_usd"),
            &group,
            "Cost to completion",
            usd(task.cost_to_complete),
            in_task.clone(),
        );
        st.add(
            &format!("task.{n}.active_minutes"),
            &group,
            "Active minutes to completion",
            minutes(
                task.active_minutes_to_complete
                    .or(task.active_minutes_total),
            ),
            vec![],
        );
        st.add(
            &format!("task.{n}.completed"),
            &group,
            "Completed",
            json!(task.is_completed()),
            vec![],
        );
        st.add(
            &format!("task.{n}.completion_source"),
            &group,
            "Completion decided by",
            json!(task.completion_source),
            vec![],
        );
        st.add(
            &format!("task.{n}.intent"),
            &group,
            "Intent",
            json!(task.intent),
            vec![],
        );
        st.add(
            &format!("task.{n}.outcome"),
            &group,
            "Outcome",
            json!(task.outcome),
            vec![],
        );
        st.add(
            &format!("task.{n}.stage_path"),
            &group,
            "Stage path",
            json!(task.stage_path),
            in_task.clone(),
        );
        st.add(
            &format!("task.{n}.edit_calls"),
            &group,
            "Edit calls",
            json!(task.edit_calls),
            vec![],
        );
        st.add(
            &format!("task.{n}.lines_changed"),
            &group,
            "Lines changed",
            json!(task.lines_changed),
            vec![],
        );
        st.add(
            &format!("task.{n}.validations"),
            &group,
            "Validations (failures)",
            json!({ "runs": task.validations, "failures": task.validation_failures }),
            vec![],
        );
        if let Some(values) = habits.get(&n) {
            for (habit, value) in values {
                st.add(
                    &format!("task.{n}.habit.{habit}"),
                    &group,
                    &format!("Habit {habit}"),
                    json!(value),
                    vec![],
                );
            }
        }
    }
    Ok(st.out)
}

fn habit_values(
    conn: &Connection,
    provider: &str,
    session_id: &str,
) -> Result<HashMap<i64, BTreeMap<String, String>>> {
    let mut out: HashMap<i64, BTreeMap<String, String>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT task_index, habit, value FROM derived_task_habit
         WHERE provider = ?1 AND session_id = ?2",
    )?;
    for row in stmt.query_map(rusqlite::params![provider, session_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (task, habit, value) = row?;
        out.entry(task).or_default().insert(habit, value);
    }
    Ok(out)
}

const TEMPLATE: &str = include_str!("review.html");

/// The self-contained review page. `<` is escaped so the JSON cannot end the script tag.
pub fn render_html(data: &ReviewData) -> Result<String> {
    Ok(embed(&serde_json::to_string(data)?))
}

/// The page served by the corpus review app: `corpus` carries the save URL
/// (and when the session was last saved), which turns on "Save as reference".
pub fn render_html_for_corpus(data: &ReviewData, corpus: &Value) -> Result<String> {
    Ok(embed(&serde_json::to_string(data)?).replace(
        "/*__CORPUS__*/null",
        &serde_json::to_string(corpus)?.replace('<', "\\u003c"),
    ))
}

fn embed(json: &str) -> String {
    TEMPLATE.replace("/*__REVIEW_DATA__*/null", &json.replace('<', "\\u003c"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_compares_numbers_to_six_decimals() {
        assert!(same(&json!(1.2345671), &json!(1.2345672)));
        assert!(!same(&json!(1.23), &json!(1.24)));
        assert!(same(
            &json!({"calls": 2, "cost_usd": 0.1}),
            &json!({"cost_usd": 0.1000001, "calls": 2})
        ));
        assert!(!same(&json!("a"), &json!("b")));
    }

    #[test]
    fn grouped_inserts_thousands_separators() {
        assert_eq!(grouped(22_005_335), "22,005,335");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(-1_000), "-1,000");
    }

    #[test]
    fn turn_at_picks_the_last_turn_started_before() {
        let mark = |turn, ts: &str| TurnMark {
            turn,
            started_at: Some(ts.into()),
            task: None,
            stage: None,
            request_kind: None,
            cost: 0.0,
        };
        let turns = vec![
            mark(0, "2026-01-01T10:00:00Z"),
            mark(1, "2026-01-01T11:00:00Z"),
        ];
        assert_eq!(turn_at(&turns, Some("2026-01-01T10:30:00Z")), Some(0));
        assert_eq!(turn_at(&turns, Some("2026-01-01T11:00:00Z")), Some(1));
        assert_eq!(turn_at(&turns, Some("2026-01-01T09:00:00Z")), Some(0));
        assert_eq!(turn_at(&turns, None), None);
    }

    #[test]
    fn embed_escapes_script_breakers() {
        let html = embed(r#"{"key":"claude:</script>"}"#);
        assert!(!html.contains("claude:</script>"));
        assert!(html.contains(r"claude:\u003c/script>"));
        assert!(!html.contains("__REVIEW_DATA__"));
    }
}
