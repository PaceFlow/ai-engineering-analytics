//! Deterministic session structure: turns, per-call usage allocation, tasks,
//! attempts and validation loops.
//!
//! A session is a container; a task is one goal inside it. Without classifier
//! assignments every root session is exactly one task. Task boundaries and
//! completion turns can come from Jev or manual overrides
//! (`task_turn_assignment`, `task_completion_mark`); everything measured about
//! a task (time, tokens, cost) is computed here from facts.

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::cost::pricing::{self, UsageForPricing};

/// Gaps longer than this are idle time, not work.
pub const IDLE_GAP_MINUTES: f64 = 30.0;
/// Usage this long after the user's last message counts as background work.
pub const BACKGROUND_AFTER_MINUTES: f64 = 10.0;
/// Consecutive failing validations with one signature that make a loop.
pub const LOOP_MIN_FAILURES: usize = 3;
/// A user message at least this long counts as new information in a loop.
const LOOP_NEW_INFO_WORDS_CHARS: i64 = 120;

pub fn parse_ts(ts: &str) -> Option<i64> {
    let ts = ts.trim();
    if ts.is_empty() {
        return None;
    }
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(ts) {
        return Some(parsed.timestamp_millis());
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(ts, format) {
            return Some(parsed.and_utc().timestamp_millis());
        }
    }
    None
}

fn minutes(ms: i64) -> f64 {
    ms as f64 / 60_000.0
}

#[derive(Debug, Clone)]
struct SessionRow {
    provider: String,
    session_id: String,
    root_session_id: String,
    started_at: Option<String>,
    ended_at: Option<String>,
    repo_root: Option<String>,
    git_branch: Option<String>,
    title: Option<String>,
}

#[derive(Debug, Clone)]
struct MessageRow {
    ts: Option<i64>,
    ts_text: Option<String>,
    role: String,
    chars: i64,
    head: String,
    is_compaction_summary: bool,
}

#[derive(Debug, Clone)]
struct ToolRow {
    ts: Option<i64>,
    ended_ts: Option<i64>,
    category: String,
    validation_kind: Option<String>,
    status: String,
    exit_code: Option<i64>,
    error_signature: Option<String>,
    child_session_id: Option<String>,
}

#[derive(Debug, Clone)]
struct UsageRow {
    id: i64,
    ts: Option<i64>,
    ts_text: Option<String>,
    model: Option<String>,
    input: i64,
    cache_read: i64,
    cache_write: i64,
    cache_write_1h: i64,
    output: i64,
    reasoning: i64,
    reasoning_in_output: bool,
    total: i64,
    reported_cost: Option<f64>,
    token_source: String,
}

#[derive(Debug, Clone)]
struct ChangeRow {
    ts: Option<i64>,
    lines: i64,
    path: String,
}

#[derive(Debug, Clone)]
struct CommitRow {
    time: i64,
}

type Key = (String, String);

#[derive(Default)]
struct Facts {
    messages: HashMap<Key, Vec<MessageRow>>,
    tools: HashMap<Key, Vec<ToolRow>>,
    usage: HashMap<Key, Vec<UsageRow>>,
    changes: HashMap<Key, Vec<ChangeRow>>,
    commits: HashMap<Key, Vec<CommitRow>>,
    assignments: HashMap<Key, BTreeMap<i64, i64>>,
    completion_marks: HashMap<(String, String, i64), (Option<i64>, String)>,
}

#[derive(Debug, Clone)]
struct Turn {
    index: i64,
    start: Option<i64>,
    start_text: Option<String>,
    end_text: Option<String>,
    user_head: String,
    user_chars: i64,
    assistant_chars: i64,
    last_activity: Option<i64>,
}

/// Rebuild all derived structure tables from facts.
pub fn refresh_session_structure(conn: &Connection) -> Result<()> {
    let sessions = load_sessions(conn)?;
    let facts = load_facts(conn)?;
    let table = pricing::table();

    conn.execute_batch(
        "DELETE FROM derived_work_activity;
         DELETE FROM derived_turn;
         DELETE FROM derived_usage_event;
         DELETE FROM derived_task;
         DELETE FROM derived_attempt;",
    )?;

    // Turns for every session.
    let mut turns_by_session: HashMap<Key, Vec<Turn>> = HashMap::new();
    for session in &sessions {
        let key = (session.provider.clone(), session.session_id.clone());
        let messages = facts.messages.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let mut turns = build_turns(session, messages);
        extend_turn_activity(&mut turns, &facts, &key);
        turns_by_session.insert(key, turns);
    }

    let by_key: HashMap<Key, &SessionRow> = sessions
        .iter()
        .map(|session| {
            (
                (session.provider.clone(), session.session_id.clone()),
                session,
            )
        })
        .collect();

    // Child session -> root turn it belongs to (spawning call, else by time).
    let mut spawn_turn: HashMap<Key, i64> = HashMap::new();
    for session in &sessions {
        let key = (session.provider.clone(), session.session_id.clone());
        let Some(tools) = facts.tools.get(&key) else {
            continue;
        };
        if session.root_session_id != session.session_id {
            continue;
        }
        let turns = &turns_by_session[&key];
        for tool in tools {
            if let Some(child) = tool.child_session_id.as_deref() {
                spawn_turn.insert(
                    (session.provider.clone(), child.to_string()),
                    turn_for_ts(turns, tool.ts),
                );
            }
        }
    }

    let mut usage_stmt = conn.prepare(
        "INSERT INTO derived_usage_event (
            usage_id, provider, session_id, root_session_id, usage_ts, model_name, turn_index,
            root_turn_index, task_index, fresh_tokens, cache_read_tokens, output_tokens,
            total_tokens, context_tokens, api_equiv_usd, reported_cost_usd, price_id,
            price_match, token_source, background_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                   ?18, ?19, ?20)",
    )?;
    let mut turn_stmt = conn.prepare(
        "INSERT INTO derived_turn (
            provider, session_id, turn_index, root_session_id, started_at, ended_at,
            user_text_head, user_chars, assistant_chars, tool_calls, edit_calls,
            validation_runs, validation_failures, usage_events, tokens_total, api_equiv_usd,
            model_names
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
    )?;

    // Per root session task accumulation.
    let mut task_usage: HashMap<Key, Vec<PricedEvent>> = HashMap::new();

    for session in &sessions {
        let key = (session.provider.clone(), session.session_id.clone());
        let turns = &turns_by_session[&key];
        let root_key = (session.provider.clone(), session.root_session_id.clone());
        let is_root = session.root_session_id == session.session_id;
        let root_turns = turns_by_session.get(&root_key);
        let assignment = facts.assignments.get(&root_key);
        let spawned_in = spawn_turn.get(&key).copied();

        let mut per_turn: BTreeMap<i64, TurnTotals> = BTreeMap::new();
        for usage in facts.usage.get(&key).map(Vec::as_slice).unwrap_or(&[]) {
            let turn_index = turn_for_ts(turns, usage.ts);
            let root_turn_index = if is_root {
                Some(turn_index)
            } else {
                spawned_in.or_else(|| root_turns.map(|turns| turn_for_ts(turns, usage.ts)))
            };
            let task_index = root_turn_index.map(|turn| task_for_turn(assignment, turn));
            let priced = (usage.total > 0)
                .then_some(usage.model.as_deref())
                .flatten()
                .and_then(|model| {
                    table.estimate(
                        model,
                        UsageForPricing {
                            input_tokens: usage.input,
                            cache_read_tokens: usage.cache_read,
                            cache_write_tokens: usage.cache_write,
                            cache_write_1h_tokens: usage.cache_write_1h,
                            output_tokens: usage.output,
                            reasoning_tokens: usage.reasoning,
                            reasoning_in_output: usage.reasoning_in_output,
                            per_call: usage.token_source != "legacy",
                        },
                    )
                });
            let fresh = usage.input + usage.cache_write + usage.output;
            let background = match (
                usage.ts,
                root_turn_index
                    .and_then(|turn| root_turns.and_then(|turns| turns.get(turn as usize)))
                    .and_then(|turn| turn.start),
            ) {
                (Some(ts), Some(start)) => minutes(ts - start) > BACKGROUND_AFTER_MINUTES,
                _ => false,
            };
            usage_stmt.execute(params![
                usage.id,
                session.provider,
                session.session_id,
                session.root_session_id,
                usage.ts_text,
                usage.model,
                turn_index,
                root_turn_index,
                task_index,
                fresh,
                usage.cache_read,
                usage.output,
                usage.total,
                usage.input + usage.cache_read + usage.cache_write,
                priced.as_ref().map(|p| p.cost_usd),
                usage.reported_cost,
                priced.as_ref().map(|p| p.price_id.clone()),
                priced.as_ref().map(|p| p.matched.as_str()),
                usage.token_source,
                background as i64
            ])?;

            let totals = per_turn.entry(turn_index).or_default();
            totals.usage_events += 1;
            totals.tokens += usage.total;
            totals.cost += priced.as_ref().map(|p| p.cost_usd).unwrap_or(0.0);
            if let Some(model) = usage.model.as_deref() {
                totals.models.insert(model.to_string());
            }

            if let Some(task_index) = task_index {
                task_usage
                    .entry(root_key.clone())
                    .or_default()
                    .push(PricedEvent {
                        ts: usage.ts,
                        task_index,
                        tokens: usage.total,
                        fresh,
                        cost: priced.as_ref().map(|p| p.cost_usd),
                        model: usage.model.clone(),
                        is_child: !is_root,
                        estimated: usage.token_source == "estimated_text",
                    });
            }
        }

        for tool in facts.tools.get(&key).map(Vec::as_slice).unwrap_or(&[]) {
            let totals = per_turn.entry(turn_for_ts(turns, tool.ts)).or_default();
            totals.tool_calls += 1;
            if tool.category == "edit" {
                totals.edit_calls += 1;
            }
            if tool.validation_kind.is_some() {
                totals.validation_runs += 1;
                if validation_failed(tool) {
                    totals.validation_failures += 1;
                }
            }
        }

        for turn in turns {
            let totals = per_turn.remove(&turn.index).unwrap_or_default();
            turn_stmt.execute(params![
                session.provider,
                session.session_id,
                turn.index,
                session.root_session_id,
                turn.start_text,
                turn.end_text,
                turn.user_head,
                turn.user_chars,
                turn.assistant_chars,
                totals.tool_calls,
                totals.edit_calls,
                totals.validation_runs,
                totals.validation_failures,
                totals.usage_events,
                totals.tokens,
                totals.cost,
                totals.models.into_iter().collect::<Vec<_>>().join(",")
            ])?;
        }
    }
    drop(usage_stmt);
    drop(turn_stmt);

    // Tasks for root sessions.
    let children_by_root: HashMap<Key, Vec<&SessionRow>> =
        sessions.iter().fold(HashMap::new(), |mut acc, session| {
            if session.root_session_id != session.session_id {
                acc.entry((session.provider.clone(), session.root_session_id.clone()))
                    .or_default()
                    .push(session);
            }
            acc
        });

    for session in sessions
        .iter()
        .filter(|session| session.root_session_id == session.session_id)
    {
        let key = (session.provider.clone(), session.session_id.clone());
        let turns = &turns_by_session[&key];
        if turns.is_empty() {
            continue;
        }
        let children = children_by_root.get(&key).cloned().unwrap_or_default();
        let events = task_usage.remove(&key).unwrap_or_default();
        write_tasks(
            conn,
            session,
            turns,
            &children,
            &events,
            &facts,
            &by_key,
            &spawn_turn,
        )?;
    }

    Ok(())
}

#[derive(Default)]
struct TurnTotals {
    tool_calls: i64,
    edit_calls: i64,
    validation_runs: i64,
    validation_failures: i64,
    usage_events: i64,
    tokens: i64,
    cost: f64,
    models: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct PricedEvent {
    ts: Option<i64>,
    task_index: i64,
    tokens: i64,
    fresh: i64,
    cost: Option<f64>,
    model: Option<String>,
    is_child: bool,
    estimated: bool,
}

fn validation_failed(tool: &ToolRow) -> bool {
    tool.exit_code.is_some_and(|code| code != 0) || tool.status == "error"
}

fn validation_passed(tool: &ToolRow) -> bool {
    tool.exit_code == Some(0) || (tool.exit_code.is_none() && tool.status == "ok")
}

fn task_for_turn(assignment: Option<&BTreeMap<i64, i64>>, turn: i64) -> i64 {
    match assignment {
        None => 0,
        Some(map) => map
            .range(..=turn)
            .next_back()
            .map(|(_, task)| *task)
            .unwrap_or(0),
    }
}

/// Index of the turn whose span contains `ts`; events before the first user
/// message (or without a timestamp) belong to the first turn.
fn turn_for_ts(turns: &[Turn], ts: Option<i64>) -> i64 {
    let Some(ts) = ts else {
        return turns.last().map(|turn| turn.index).unwrap_or(0);
    };
    let mut current = 0;
    for turn in turns {
        match turn.start {
            Some(start) if start <= ts => current = turn.index,
            Some(_) => break,
            None => {}
        }
    }
    current
}

fn build_turns(session: &SessionRow, messages: &[MessageRow]) -> Vec<Turn> {
    let mut turns: Vec<Turn> = Vec::new();
    for message in messages {
        if message.role == "user" && !message.is_compaction_summary {
            turns.push(Turn {
                index: turns.len() as i64,
                start: message.ts,
                start_text: message.ts_text.clone(),
                end_text: None,
                user_head: message.head.clone(),
                user_chars: message.chars,
                assistant_chars: 0,
                last_activity: message.ts,
            });
        } else if message.role == "assistant"
            && let Some(turn) = turns.last_mut()
        {
            turn.assistant_chars += message.chars;
        }
    }
    if turns.is_empty() {
        turns.push(Turn {
            index: 0,
            start: session.started_at.as_deref().and_then(parse_ts),
            start_text: session.started_at.clone(),
            end_text: None,
            user_head: String::new(),
            user_chars: 0,
            assistant_chars: messages
                .iter()
                .filter(|message| message.role == "assistant")
                .map(|message| message.chars)
                .sum(),
            last_activity: session.started_at.as_deref().and_then(parse_ts),
        });
    }
    let count = turns.len();
    for index in 0..count {
        let end = if index + 1 < count {
            turns[index + 1].start_text.clone()
        } else {
            session.ended_at.clone()
        };
        turns[index].end_text = end;
    }
    turns
}

fn extend_turn_activity(turns: &mut [Turn], facts: &Facts, key: &Key) {
    let mut activity: Vec<i64> = Vec::new();
    if let Some(messages) = facts.messages.get(key) {
        activity.extend(messages.iter().filter_map(|message| message.ts));
    }
    if let Some(tools) = facts.tools.get(key) {
        activity.extend(tools.iter().filter_map(|tool| tool.ended_ts.or(tool.ts)));
    }
    if let Some(usage) = facts.usage.get(key) {
        activity.extend(usage.iter().filter_map(|usage| usage.ts));
    }
    for ts in activity {
        let index = turn_for_ts(turns, Some(ts)) as usize;
        if let Some(turn) = turns.get_mut(index)
            && turn.last_activity.is_none_or(|last| ts > last)
        {
            turn.last_activity = Some(ts);
        }
    }
}

#[derive(Debug, Clone)]
struct Attempt {
    turn_index: i64,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    edit_calls: i64,
    validation_kind: Option<String>,
    validation_result: &'static str,
    error_signature: Option<String>,
    ended_by: &'static str,
    loop_flag: bool,
}

/// Group edits into attempts closed by validation, a user message, or the end.
fn build_attempts(
    turns: &[Turn],
    tools: &[(i64, ToolRow)],
    turn_set: &BTreeSet<i64>,
) -> Vec<Attempt> {
    let mut attempts: Vec<Attempt> = Vec::new();
    let mut open: Option<Attempt> = None;
    let mut last_turn: Option<i64> = None;
    for (turn_index, tool) in tools {
        if !turn_set.contains(turn_index) {
            continue;
        }
        if let (Some(previous), Some(attempt)) = (last_turn, open.as_mut())
            && previous != *turn_index
        {
            attempt.ended_by = "user_feedback";
            attempts.push(open.take().expect("open attempt"));
        }
        last_turn = Some(*turn_index);
        if tool.category == "edit" {
            let attempt = open.get_or_insert(Attempt {
                turn_index: *turn_index,
                started_at: tool.ts,
                ended_at: tool.ts,
                edit_calls: 0,
                validation_kind: None,
                validation_result: "none",
                error_signature: None,
                ended_by: "session_end",
                loop_flag: false,
            });
            attempt.edit_calls += 1;
            attempt.ended_at = tool.ended_ts.or(tool.ts).or(attempt.ended_at);
        } else if let Some(kind) = tool.validation_kind.as_deref()
            && let Some(mut attempt) = open.take()
        {
            attempt.validation_kind = Some(kind.to_string());
            attempt.validation_result = if validation_failed(tool) {
                "fail"
            } else if validation_passed(tool) {
                "pass"
            } else {
                "unknown"
            };
            attempt.error_signature = tool.error_signature.clone();
            attempt.ended_at = tool.ended_ts.or(tool.ts).or(attempt.ended_at);
            attempt.ended_by = "validation";
            attempts.push(attempt);
        }
    }
    if let Some(attempt) = open {
        attempts.push(attempt);
    }
    mark_loops(&mut attempts, turns);
    attempts
}

/// Mark unproductive loops: at least `LOOP_MIN_FAILURES` consecutive failing
/// validations with one error signature and no substantial new user input.
fn mark_loops(attempts: &mut [Attempt], turns: &[Turn]) {
    let mut start = 0usize;
    while start < attempts.len() {
        let Some(signature) = attempts[start]
            .error_signature
            .clone()
            .filter(|_| attempts[start].validation_result == "fail")
        else {
            start += 1;
            continue;
        };
        let mut end = start + 1;
        while end < attempts.len()
            && attempts[end].validation_result == "fail"
            && attempts[end].error_signature.as_deref() == Some(signature.as_str())
            && !new_user_information(
                turns,
                attempts[end - 1].turn_index,
                attempts[end].turn_index,
            )
        {
            end += 1;
        }
        if end - start >= LOOP_MIN_FAILURES {
            for attempt in &mut attempts[start..end] {
                attempt.loop_flag = true;
            }
        }
        start = end;
    }
}

fn new_user_information(turns: &[Turn], from_turn: i64, to_turn: i64) -> bool {
    turns
        .iter()
        .filter(|turn| turn.index > from_turn && turn.index <= to_turn)
        .any(|turn| turn.user_chars >= LOOP_NEW_INFO_WORDS_CHARS)
}

#[allow(clippy::too_many_arguments)]
fn write_tasks(
    conn: &Connection,
    session: &SessionRow,
    turns: &[Turn],
    children: &[&SessionRow],
    events: &[PricedEvent],
    facts: &Facts,
    _by_key: &HashMap<Key, &SessionRow>,
    spawn_turn: &HashMap<Key, i64>,
) -> Result<()> {
    let key = (session.provider.clone(), session.session_id.clone());
    let assignment = facts.assignments.get(&key);
    let segmentation_source = if assignment.is_some() {
        "classified"
    } else {
        "whole_session"
    };

    // Tool calls of the root and its children, mapped onto root turns.
    let mut tools: Vec<(i64, ToolRow, bool)> = Vec::new();
    for tool in facts.tools.get(&key).map(Vec::as_slice).unwrap_or(&[]) {
        tools.push((turn_for_ts(turns, tool.ts), tool.clone(), false));
    }
    let mut changes: Vec<(i64, ChangeRow)> = Vec::new();
    for change in facts.changes.get(&key).map(Vec::as_slice).unwrap_or(&[]) {
        changes.push((turn_for_ts(turns, change.ts), change.clone()));
    }
    let mut commits: Vec<CommitRow> = facts.commits.get(&key).cloned().unwrap_or_default();
    for child in children {
        let child_key = (child.provider.clone(), child.session_id.clone());
        let spawned = spawn_turn.get(&child_key).copied();
        for tool in facts
            .tools
            .get(&child_key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            let turn = spawned.unwrap_or_else(|| turn_for_ts(turns, tool.ts));
            tools.push((turn, tool.clone(), true));
        }
        for change in facts
            .changes
            .get(&child_key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            let turn = spawned.unwrap_or_else(|| turn_for_ts(turns, change.ts));
            changes.push((turn, change.clone()));
        }
        commits.extend(facts.commits.get(&child_key).cloned().unwrap_or_default());
    }
    tools.sort_by_key(|(turn, tool, _)| (tool.ts.unwrap_or(i64::MAX), *turn));
    commits.sort_by_key(|commit| commit.time);
    commits.dedup_by_key(|commit| commit.time);

    let mut task_turns: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    for turn in turns {
        task_turns
            .entry(task_for_turn(assignment, turn.index))
            .or_default()
            .insert(turn.index);
    }

    let mut task_stmt = conn.prepare_cached(
        "INSERT INTO derived_task (
            provider, session_id, task_index, segmentation_source, first_turn, last_turn,
            completion_turn, turn_list, title, started_at, completed_at, last_activity_at,
            completion_source, wall_minutes_to_complete, active_minutes_to_complete,
            active_minutes_total, agent_minutes, user_minutes, turns, attempts, validations,
            validation_failures, loop_attempts, edit_calls, lines_changed, files_touched,
            subagents, primary_model, models, model_changes, harness, repo_root, git_branch,
            tokens_total, fresh_tokens, cost_total_usd, cost_to_complete_usd,
            post_completion_cost_usd, subagent_cost_usd, unpriced_tokens, estimated_tokens,
            commits, content_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                   ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32,
                   ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, ?41, ?42, ?43)",
    )?;
    let mut attempt_stmt = conn.prepare_cached(
        "INSERT INTO derived_attempt (
            provider, session_id, task_index, attempt_index, turn_index, started_at, ended_at,
            edit_calls, lines_changed, validation_kind, validation_result, error_signature,
            ended_by, loop_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?;

    for (task_index, turn_set) in &task_turns {
        let task_turn_list: Vec<&Turn> = turns
            .iter()
            .filter(|turn| turn_set.contains(&turn.index))
            .collect();
        let first = task_turn_list.first().expect("task has turns");
        let last = task_turn_list.last().expect("task has turns");
        let started = first.start;
        let task_tools: Vec<(i64, ToolRow)> = tools
            .iter()
            .filter(|(turn, _, _)| turn_set.contains(turn))
            .map(|(turn, tool, _)| (*turn, tool.clone()))
            .collect();
        let task_changes: Vec<&ChangeRow> = changes
            .iter()
            .filter(|(turn, _)| turn_set.contains(turn))
            .map(|(_, change)| change)
            .collect();
        let task_events: Vec<&PricedEvent> = events
            .iter()
            .filter(|event| event.task_index == *task_index)
            .collect();

        // Activity timeline for durations.
        let mut activity: Vec<i64> = Vec::new();
        for turn in &task_turn_list {
            activity.extend(turn.start);
            activity.extend(turn.last_activity);
        }
        activity.extend(
            task_tools
                .iter()
                .filter_map(|(_, tool)| tool.ended_ts.or(tool.ts)),
        );
        activity.extend(task_events.iter().filter_map(|event| event.ts));
        activity.sort_unstable();
        activity.dedup();
        for pair in activity.windows(2) {
            if pair[1] > pair[0] && pair[1] - pair[0] <= (IDLE_GAP_MINUTES * 60_000.0) as i64 {
                conn.execute(
                    "INSERT INTO derived_work_activity VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        session.provider,
                        session.session_id,
                        task_index,
                        turn_for_ts(turns, Some(pair[0])),
                        pair[0],
                        pair[1]
                    ],
                )?;
            }
        }
        let last_activity = activity.last().copied();

        let attempts = build_attempts(turns, &task_tools, turn_set);
        let last_edit_ts = task_tools
            .iter()
            .filter(|(_, tool)| tool.category == "edit")
            .filter_map(|(_, tool)| tool.ended_ts.or(tool.ts))
            .max();

        // Completion: override/classifier mark, then commit, then validation.
        let mark = facts.completion_marks.get(&(
            session.provider.clone(),
            session.session_id.clone(),
            *task_index,
        ));
        let (completion_turn, completed_at, completion_source): (Option<i64>, Option<i64>, String) =
            match mark {
                Some((Some(turn), source)) => {
                    let ts = turns
                        .iter()
                        .find(|candidate| candidate.index == *turn)
                        .and_then(|turn| turn.last_activity.or(turn.start));
                    (Some(*turn), ts, source.clone())
                }
                Some((None, source)) => (None, None, format!("{source}:not_completed")),
                None => {
                    let window_end = last_activity.map(|ts| ts + 4 * 3_600_000);
                    let commit = last_edit_ts.and_then(|edit| {
                        commits
                            .iter()
                            .find(|commit| {
                                commit.time >= edit
                                    && window_end.is_none_or(|end| commit.time <= end)
                            })
                            .map(|commit| commit.time)
                    });
                    let validation = last_edit_ts.and_then(|edit| {
                        task_tools
                            .iter()
                            .filter(|(_, tool)| {
                                tool.validation_kind.is_some()
                                    && validation_passed(tool)
                                    && tool.ts.is_some_and(|ts| ts >= edit)
                            })
                            .filter_map(|(_, tool)| tool.ended_ts.or(tool.ts))
                            .next_back()
                    });
                    match (commit, validation) {
                        (Some(ts), _) => (
                            Some(turn_for_ts(turns, Some(ts)).min(last.index)),
                            Some(ts),
                            "commit".to_string(),
                        ),
                        (None, Some(ts)) => (
                            Some(turn_for_ts(turns, Some(ts))),
                            Some(ts),
                            "validation_pass".to_string(),
                        ),
                        (None, None) => (None, None, "none".to_string()),
                    }
                }
            };

        let completed_cutoff = completed_at;
        let wall = match (started, completed_at) {
            (Some(start), Some(done)) if done >= start => Some(minutes(done - start)),
            _ => None,
        };
        let active_to_complete = completed_at.map(|done| active_minutes(&activity, Some(done)));
        let active_total = active_minutes(&activity, None);
        let (agent_minutes, user_minutes) = agent_and_user_minutes(&task_turn_list);

        let cost_total: Option<f64> = sum_cost(task_events.iter().copied());
        let cost_to_complete = completed_cutoff.and_then(|cutoff| {
            sum_cost(
                task_events
                    .iter()
                    .copied()
                    .filter(|event| event.ts.is_none_or(|ts| ts <= cutoff)),
            )
        });
        let post_completion = completed_cutoff.and_then(|cutoff| {
            sum_cost(
                task_events
                    .iter()
                    .copied()
                    .filter(|event| event.ts.is_some_and(|ts| ts > cutoff)),
            )
        });
        let subagent_cost = sum_cost(task_events.iter().copied().filter(|event| event.is_child));
        let tokens_total: i64 = task_events.iter().map(|event| event.tokens).sum();
        let fresh_tokens: i64 = task_events.iter().map(|event| event.fresh).sum();
        let unpriced_tokens: i64 = task_events
            .iter()
            .filter(|event| event.cost.is_none())
            .map(|event| event.tokens)
            .sum();
        let estimated_tokens: i64 = task_events
            .iter()
            .filter(|event| event.estimated)
            .map(|event| event.tokens)
            .sum();

        let mut model_tokens: BTreeMap<String, i64> = BTreeMap::new();
        let mut model_sequence: Vec<String> = Vec::new();
        for event in &task_events {
            if let Some(model) = event.model.as_deref() {
                *model_tokens.entry(model.to_string()).or_default() += event.tokens;
                if !event.is_child && model_sequence.last().map(String::as_str) != Some(model) {
                    model_sequence.push(model.to_string());
                }
            }
        }
        let primary_model = model_tokens
            .iter()
            .max_by_key(|(_, tokens)| **tokens)
            .map(|(model, _)| model.clone());
        let model_changes = model_sequence.len().saturating_sub(1) as i64;
        let subagents = children
            .iter()
            .filter(|child| {
                spawn_turn
                    .get(&(child.provider.clone(), child.session_id.clone()))
                    .is_some_and(|turn| turn_set.contains(turn))
                    || (assignment.is_none())
            })
            .count() as i64;
        let validations = task_tools
            .iter()
            .filter(|(_, tool)| tool.validation_kind.is_some())
            .count() as i64;
        let validation_failures = task_tools
            .iter()
            .filter(|(_, tool)| tool.validation_kind.is_some() && validation_failed(tool))
            .count() as i64;
        let edit_calls = task_tools
            .iter()
            .filter(|(_, tool)| tool.category == "edit")
            .count() as i64;
        let lines_changed: i64 = task_changes.iter().map(|change| change.lines).sum();
        let files_touched = task_changes
            .iter()
            .map(|change| change.path.as_str())
            .collect::<BTreeSet<_>>()
            .len() as i64;
        let task_commits = commits
            .iter()
            .filter(|commit| {
                started.is_some_and(|start| commit.time >= start)
                    && last_activity.is_none_or(|end| commit.time <= end + 4 * 3_600_000)
            })
            .count() as i64;
        let title = if first.user_head.trim().is_empty() {
            session.title.clone()
        } else {
            Some(crate::classify::request::normalize(&first.user_head).text)
        };
        let turn_list = turn_set
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let digest = format!(
            "{:x}",
            md5::compute(format!(
                "{turn_list}|{}|{}|{}|{}",
                task_tools.len(),
                task_events.len(),
                last.user_chars,
                last.last_activity.unwrap_or(0)
            ))
        );

        task_stmt.execute(params![
            session.provider,
            session.session_id,
            task_index,
            segmentation_source,
            first.index,
            last.index,
            completion_turn,
            turn_list,
            title,
            first.start_text,
            completed_at.map(ms_to_iso),
            last_activity.map(ms_to_iso),
            completion_source,
            wall,
            active_to_complete,
            active_total,
            agent_minutes,
            user_minutes,
            task_turn_list.len() as i64,
            attempts.len() as i64,
            validations,
            validation_failures,
            attempts.iter().filter(|attempt| attempt.loop_flag).count() as i64,
            edit_calls,
            lines_changed,
            files_touched,
            subagents,
            primary_model,
            model_tokens.keys().cloned().collect::<Vec<_>>().join(","),
            model_changes,
            session.provider,
            session.repo_root,
            session.git_branch,
            tokens_total,
            fresh_tokens,
            cost_total,
            cost_to_complete,
            post_completion,
            subagent_cost,
            unpriced_tokens,
            estimated_tokens,
            task_commits,
            digest
        ])?;

        for (attempt_index, attempt) in attempts.iter().enumerate() {
            let lines: i64 = task_changes
                .iter()
                .filter(|change| {
                    change.ts.is_some_and(|ts| {
                        attempt.started_at.is_some_and(|start| ts >= start)
                            && attempt.ended_at.is_some_and(|end| ts <= end)
                    })
                })
                .map(|change| change.lines)
                .sum();
            attempt_stmt.execute(params![
                session.provider,
                session.session_id,
                task_index,
                attempt_index as i64,
                attempt.turn_index,
                attempt.started_at.map(ms_to_iso),
                attempt.ended_at.map(ms_to_iso),
                attempt.edit_calls,
                lines,
                attempt.validation_kind,
                attempt.validation_result,
                attempt.error_signature,
                attempt.ended_by,
                attempt.loop_flag as i64
            ])?;
        }
    }
    Ok(())
}

fn sum_cost<'a>(events: impl Iterator<Item = &'a PricedEvent>) -> Option<f64> {
    let mut total: Option<f64> = None;
    for event in events {
        if let Some(cost) = event.cost {
            *total.get_or_insert(0.0) += cost;
        }
    }
    total
}

/// Active minutes between the first activity and `until` (or the last
/// activity), excluding gaps longer than `IDLE_GAP_MINUTES`.
fn active_minutes(sorted_activity: &[i64], until: Option<i64>) -> f64 {
    let mut total = 0.0;
    let mut previous: Option<i64> = None;
    for ts in sorted_activity {
        if until.is_some_and(|until| *ts > until) {
            break;
        }
        if let Some(prev) = previous {
            let gap = minutes(ts - prev);
            if gap <= IDLE_GAP_MINUTES {
                total += gap;
            }
        }
        previous = Some(*ts);
    }
    if let (Some(until), Some(prev)) = (until, previous) {
        let gap = minutes(until - prev);
        if gap > 0.0 && gap <= IDLE_GAP_MINUTES {
            total += gap;
        }
    }
    total
}

/// Time the agent worked (user message to last activity in the turn) and time
/// the agent waited for the user (last activity to the next user message,
/// capped at the idle threshold).
fn agent_and_user_minutes(turns: &[&Turn]) -> (Option<f64>, Option<f64>) {
    let mut agent = 0.0;
    let mut user = 0.0;
    let mut any = false;
    for (index, turn) in turns.iter().enumerate() {
        if let (Some(start), Some(last)) = (turn.start, turn.last_activity)
            && last >= start
        {
            agent += minutes(last - start);
            any = true;
        }
        if let (Some(last), Some(next)) = (
            turn.last_activity,
            turns.get(index + 1).and_then(|next| next.start),
        ) && next >= last
        {
            user += minutes(next - last).min(IDLE_GAP_MINUTES);
        }
    }
    if any {
        (Some(agent), Some(user))
    } else {
        (None, None)
    }
}

pub fn ms_to_iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

fn load_sessions(conn: &Connection) -> Result<Vec<SessionRow>> {
    let mut stmt = conn.prepare(
        "SELECT s.provider, s.session_id,
                COALESCE(NULLIF(s.root_session_id, ''), s.session_id),
                s.started_at, s.ended_at, r.repo_root, s.git_branch, s.title
         FROM metadata_sessions s
         LEFT JOIN metadata_repositories r ON r.id = s.repository_id
         ORDER BY s.provider, s.session_id",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(SessionRow {
                provider: row.get(0)?,
                session_id: row.get(1)?,
                root_session_id: row.get(2)?,
                started_at: row.get(3)?,
                ended_at: row.get(4)?,
                repo_root: row.get(5)?,
                git_branch: row.get(6)?,
                title: row.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_facts(conn: &Connection) -> Result<Facts> {
    let mut facts = Facts::default();

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, message_ts, role, COALESCE(chars, LENGTH(content)),
                SUBSTR(content, 1, 400), is_compaction_summary
         FROM fact_session_message
         ORDER BY provider, session_id, message_index",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts_text: Option<String> = row.get(2)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            MessageRow {
                ts: ts_text.as_deref().and_then(parse_ts),
                ts_text,
                role: row.get(3)?,
                chars: row.get(4)?,
                head: row.get(5)?,
                is_compaction_summary: row.get::<_, i64>(6)? != 0,
            },
        ))
    })?;
    for row in rows {
        let (key, message) = row?;
        facts.messages.entry(key).or_default().push(message);
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, ts, ended_ts, tool_category, validation_kind, status,
                exit_code, error_signature, child_session_id
         FROM fact_session_tool_call
         ORDER BY provider, session_id, call_index",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts: Option<String> = row.get(2)?;
        let ended: Option<String> = row.get(3)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            ToolRow {
                ts: ts.as_deref().and_then(parse_ts),
                ended_ts: ended.as_deref().and_then(parse_ts),
                category: row.get(4)?,
                validation_kind: row.get(5)?,
                status: row.get(6)?,
                exit_code: row.get(7)?,
                error_signature: row.get(8)?,
                child_session_id: row.get(9)?,
            },
        ))
    })?;
    for row in rows {
        let (key, tool) = row?;
        facts.tools.entry(key).or_default().push(tool);
    }
    for tools in facts.tools.values_mut() {
        tools.sort_by_key(|tool| tool.ts.unwrap_or(i64::MAX));
    }

    let mut stmt = conn.prepare(
        "SELECT id, provider, session_id, usage_ts, model_name, input_tokens, cached_input_tokens,
                cache_creation_tokens, cache_creation_1h_tokens, output_tokens, reasoning_tokens,
                reasoning_in_output, total_tokens, actual_cost_usd, token_source
         FROM fact_session_usage
         ORDER BY provider, session_id, usage_index",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts_text: Option<String> = row.get(3)?;
        Ok((
            (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
            UsageRow {
                id: row.get(0)?,
                ts: ts_text.as_deref().and_then(parse_ts),
                ts_text,
                model: row.get(4)?,
                input: row.get(5)?,
                cache_read: row.get(6)?,
                cache_write: row.get(7)?,
                cache_write_1h: row.get(8)?,
                output: row.get(9)?,
                reasoning: row.get(10)?,
                reasoning_in_output: row.get::<_, i64>(11)? != 0,
                total: row.get(12)?,
                reported_cost: row.get(13)?,
                token_source: row.get(14)?,
            },
        ))
    })?;
    for row in rows {
        let (key, usage) = row?;
        facts.usage.entry(key).or_default().push(usage);
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, change_ts, lines_added + lines_removed,
                COALESCE(rel_path, abs_path, '')
         FROM fact_session_code_change
         WHERE source_kind = 'accepted_change'",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts: Option<String> = row.get(2)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            ChangeRow {
                ts: ts.as_deref().and_then(parse_ts),
                lines: row.get(3)?,
                path: row.get(4)?,
            },
        ))
    })?;
    for row in rows {
        let (key, change) = row?;
        facts.changes.entry(key).or_default().push(change);
    }

    let mut stmt = conn.prepare(
        "SELECT sm.provider, sm.session_id, c.commit_time
         FROM fact_commit_session_match sm
         JOIN fact_commit c ON c.repo_root = sm.repo_root AND c.commit_sha = sm.commit_sha
         WHERE sm.matched_lines > 0",
    )?;
    let rows = stmt.query_map([], |row| {
        let time: String = row.get(2)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            parse_ts(&time),
        ))
    })?;
    for row in rows {
        let (key, time) = row?;
        if let Some(time) = time {
            facts
                .commits
                .entry(key)
                .or_default()
                .push(CommitRow { time });
        }
    }

    // Overrides win over classifier assignments.
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, turn_index, task_index, source
         FROM task_turn_assignment
         ORDER BY CASE source WHEN 'override' THEN 0 ELSE 1 END",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    for row in rows {
        let (key, turn, task) = row?;
        facts
            .assignments
            .entry(key)
            .or_default()
            .entry(turn)
            .or_insert(task);
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index, completion_turn, source
         FROM task_completion_mark
         ORDER BY CASE source WHEN 'override' THEN 0 ELSE 1 END",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ),
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    for row in rows {
        let (key, turn, source) = row?;
        facts.completion_marks.entry(key).or_insert((turn, source));
    }

    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;
    use crate::db::session_store::{
        MessageRecord, SessionRecord, ToolCallRecord, UsageRecord, store_session_record,
    };

    fn message(key: &str, ts: &str, role: &str, text: &str) -> MessageRecord {
        MessageRecord {
            key: key.into(),
            ts: Some(ts.into()),
            role: role.into(),
            text: text.into(),
            is_compaction_summary: false,
        }
    }

    fn usage(key: &str, ts: &str, output: i64) -> UsageRecord {
        let mut usage = UsageRecord::new(key);
        usage.ts = Some(ts.into());
        usage.model = Some("claude-sonnet-4-5".into());
        usage.input_tokens = 1000;
        usage.output_tokens = output;
        usage
    }

    fn tool(
        key: &str,
        ts: &str,
        name: &str,
        target: &str,
        exit: Option<i64>,
        err: Option<&str>,
    ) -> ToolCallRecord {
        ToolCallRecord {
            key: key.into(),
            ts: Some(ts.into()),
            ended_ts: Some(ts.into()),
            tool_name: name.into(),
            target: Some(target.into()),
            status: if exit.is_some_and(|c| c != 0) {
                "error".into()
            } else {
                "ok".into()
            },
            exit_code: exit,
            error_head: err.map(ToOwned::to_owned),
            output_in_context: true,
            ..ToolCallRecord::default()
        }
    }

    fn sample_session(conn: &Connection) -> Result<()> {
        let mut record = SessionRecord::new("claude", "s1");
        record.started_at = Some("2026-10-01T10:00:00Z".into());
        record.ended_at = Some("2026-10-01T12:00:00Z".into());
        record.messages = vec![
            message(
                "u1",
                "2026-10-01T10:00:00Z",
                "user",
                "Fix the token parser so the tests pass",
            ),
            message("a1", "2026-10-01T10:01:00Z", "assistant", "Working on it"),
            message("u2", "2026-10-01T10:20:00Z", "user", "still failing"),
            message("u3", "2026-10-01T11:30:00Z", "user", "ok thanks"),
        ];
        record.usage = vec![
            usage("m1", "2026-10-01T10:01:00Z", 100),
            usage("m2", "2026-10-01T10:21:00Z", 100),
            usage("m3", "2026-10-01T11:31:00Z", 100),
        ];
        let err = Some("error[E0308]: mismatched types");
        record.tool_calls = vec![
            tool("t1", "2026-10-01T10:02:00Z", "Edit", "src/a.rs", None, None),
            tool(
                "t2",
                "2026-10-01T10:03:00Z",
                "Bash",
                "cargo test",
                Some(101),
                err,
            ),
            tool("t3", "2026-10-01T10:04:00Z", "Edit", "src/a.rs", None, None),
            tool(
                "t4",
                "2026-10-01T10:05:00Z",
                "Bash",
                "cargo test",
                Some(101),
                err,
            ),
            tool("t5", "2026-10-01T10:06:00Z", "Edit", "src/a.rs", None, None),
            tool(
                "t6",
                "2026-10-01T10:07:00Z",
                "Bash",
                "cargo test",
                Some(101),
                err,
            ),
            tool("t7", "2026-10-01T10:22:00Z", "Edit", "src/a.rs", None, None),
            tool(
                "t8",
                "2026-10-01T10:23:00Z",
                "Bash",
                "cargo test",
                Some(0),
                None,
            ),
        ];
        store_session_record(conn, &record)?;

        let mut child = SessionRecord::new("claude", "s1/agent-x");
        child.parent_session_id = Some("s1".into());
        child.agent_kind = crate::db::session_store::AgentKind::Subagent;
        child.messages = vec![message("c1", "2026-10-01T10:01:30Z", "user", "explore")];
        child.usage = vec![usage("cm1", "2026-10-01T10:01:40Z", 50)];
        store_session_record(conn, &child)?;
        crate::db::session_store::refresh_session_roots(conn)?;
        Ok(())
    }

    #[test]
    fn whole_session_is_one_task_without_jev() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        sample_session(&conn)?;
        refresh_session_structure(&conn)?;

        let tasks: i64 = conn.query_row("SELECT COUNT(*) FROM derived_task", [], |r| r.get(0))?;
        assert_eq!(tasks, 1);
        let (source, completion_source, attempts, loops, subagents): (
            String,
            String,
            i64,
            i64,
            i64,
        ) = conn.query_row(
            "SELECT segmentation_source, completion_source, attempts, loop_attempts, subagents
                 FROM derived_task",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )?;
        assert_eq!(source, "whole_session");
        assert_eq!(completion_source, "validation_pass");
        assert_eq!(attempts, 4);
        assert_eq!(loops, 3, "three same-signature failures form a loop");
        assert_eq!(subagents, 1);
        Ok(())
    }

    #[test]
    fn task_spend_sums_to_session_spend() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        sample_session(&conn)?;
        refresh_session_structure(&conn)?;
        let event_total: f64 = conn.query_row(
            "SELECT SUM(api_equiv_usd) FROM derived_usage_event WHERE root_session_id = 's1'",
            [],
            |r| r.get(0),
        )?;
        let task_total: f64 =
            conn.query_row("SELECT SUM(cost_total_usd) FROM derived_task", [], |r| {
                r.get(0)
            })?;
        assert!((event_total - task_total).abs() < 1e-9);
        let (to_complete, after): (f64, f64) = conn.query_row(
            "SELECT cost_to_complete_usd, post_completion_cost_usd FROM derived_task",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert!((to_complete + after - task_total).abs() < 1e-9);
        assert!(after > 0.0, "the 11:31 call happened after completion");
        Ok(())
    }

    #[test]
    fn zero_native_usage_leaves_cost_unknown() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let mut record = SessionRecord::new("opencode", "zero-usage");
        record.messages = vec![message("m1", "2026-10-01T10:00:00Z", "user", "Review this")];
        let mut empty = UsageRecord::new("u1");
        empty.ts = Some("2026-10-01T10:01:00Z".into());
        empty.model = Some("claude-sonnet-4-5".into());
        record.usage.push(empty);
        store_session_record(&conn, &record)?;
        crate::db::session_store::refresh_session_roots(&conn)?;
        refresh_session_structure(&conn)?;
        let cost: Option<f64> = conn.query_row(
            "SELECT cost_total_usd FROM derived_task WHERE session_id = 'zero-usage'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(cost, None);
        Ok(())
    }

    #[test]
    fn classified_assignments_split_tasks_and_resume_collects_turns() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        sample_session(&conn)?;
        // Turn 1 starts task 1; turn 2 resumes task 0.
        for (turn, task) in [(0, 0), (1, 1), (2, 0)] {
            conn.execute(
                "INSERT INTO task_turn_assignment(provider, session_id, turn_index, task_index, source)
                 VALUES ('claude', 's1', ?1, ?2, 'jev')",
                params![turn, task],
            )?;
        }
        refresh_session_structure(&conn)?;
        let lists: Vec<(i64, String)> = conn
            .prepare("SELECT task_index, turn_list FROM derived_task ORDER BY task_index")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        assert_eq!(lists, vec![(0, "0,2".to_string()), (1, "1".to_string())]);
        Ok(())
    }

    #[test]
    fn active_time_excludes_idle_gaps() {
        let base = 1_000_000_000i64;
        let activity = vec![
            base,
            base + 5 * 60_000,
            base + 120 * 60_000,
            base + 125 * 60_000,
        ];
        assert!((active_minutes(&activity, None) - 10.0).abs() < 1e-9);
    }
}
