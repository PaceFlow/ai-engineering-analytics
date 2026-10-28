//! Skill evidence and downstream usage. Windows overlap and must never be summed.

use anyhow::Result;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::insights::structure::parse_ts;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SkillFrequencyRow {
    pub skill_id: String,
    pub calls: i64,
    pub successful: i64,
    pub failed: i64,
    pub unknown_calls: i64,
    pub sessions: i64,
    pub calls_per_session: f64,
    pub listed: i64,
    pub injected: i64,
    pub loaded: i64,
    pub distinct_tasks: i64,
    pub verified_tasks: i64,
    pub validation_failures: i64,
    pub cost_per_verified_usd: Option<f64>,
    pub eligible_tasks: Option<i64>,
    pub eligible_invoked_verified_tasks: Option<i64>,
    pub eligible_invoked_cost_per_verified_usd: Option<f64>,
    pub uninvoked_verified_tasks: Option<i64>,
    pub uninvoked_cost_per_verified_usd: Option<f64>,
}

pub fn skill_frequency(conn: &Connection) -> Result<Vec<SkillFrequencyRow>> {
    let mut stmt = conn.prepare(
        "WITH events AS (
           SELECT COALESCE(NULLIF(target, ''), '(unknown skill)') AS skill_id,
                  provider || ':' || session_id AS session_key,
                  CASE WHEN status = 'ok' THEN 1 ELSE 0 END AS successful,
                  CASE WHEN status IN ('error', 'rejected', 'interrupted') THEN 1 ELSE 0 END AS failed,
                  1 AS called, 0 AS listed, 0 AS injected
           FROM fact_session_tool_call
           WHERE lower(tool_name) = 'skill' OR substr(lower(tool_name), -6) IN ('.skill', '_skill', ':skill')
           UNION ALL
           SELECT COALESCE(NULLIF(source, ''), '(unresolved listing)'),
                  provider || ':' || session_id, 0, 0, 0,
                  CASE WHEN kind = 'skill_listing' THEN 1 ELSE 0 END,
                  CASE WHEN kind IN ('skill_body', 'skill_injection') THEN 1 ELSE 0 END
           FROM fact_session_context_injection
           WHERE kind IN ('skill_listing', 'skill_body', 'skill_injection')
         )
         SELECT skill_id, SUM(called), SUM(successful), SUM(failed),
                COUNT(DISTINCT CASE WHEN called = 1 THEN session_key END),
                SUM(listed), SUM(injected)
         FROM events GROUP BY skill_id ORDER BY SUM(called) DESC, skill_id",
    )?;
    let rows = stmt.query_map([], |row| {
        let calls: i64 = row.get(1)?;
        let sessions: i64 = row.get(4)?;
        Ok(SkillFrequencyRow {
            skill_id: row.get(0)?,
            calls,
            successful: row.get(2)?,
            failed: row.get(3)?,
            unknown_calls: calls - row.get::<_, i64>(2)? - row.get::<_, i64>(3)?,
            sessions,
            calls_per_session: calls as f64 / sessions.max(1) as f64,
            listed: row.get(5)?,
            injected: row.get(6)?,
            loaded: 0,
            distinct_tasks: 0,
            verified_tasks: 0,
            validation_failures: 0,
            cost_per_verified_usd: None,
            eligible_tasks: None,
            eligible_invoked_verified_tasks: None,
            eligible_invoked_cost_per_verified_usd: None,
            uninvoked_verified_tasks: None,
            uninvoked_cost_per_verified_usd: None,
        })
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut by_id: BTreeMap<String, SkillFrequencyRow> = BTreeMap::new();
    for mut row in raw {
        row.skill_id = if row.skill_id == "(unresolved listing)" {
            row.skill_id
        } else {
            normalize_skill_id(&row.skill_id).unwrap_or_else(|| "(unknown skill)".into())
        };
        if let Some(existing) = by_id.get_mut(&row.skill_id) {
            existing.calls += row.calls;
            existing.successful += row.successful;
            existing.failed += row.failed;
            existing.unknown_calls += row.unknown_calls;
            existing.sessions += row.sessions;
            existing.listed += row.listed;
            existing.injected += row.injected;
            existing.calls_per_session = existing.calls as f64 / existing.sessions.max(1) as f64;
        } else {
            by_id.insert(row.skill_id.clone(), row);
        }
    }
    let mut frequencies: Vec<SkillFrequencyRow> = by_id.into_values().collect();
    let mut task_stmt = conn.prepare(
        "WITH matched AS (
           SELECT COALESCE(NULLIF(sc.target, ''), '(unknown skill)') AS skill_id,
                  t.provider || '|' || t.session_id || '|' || t.task_index AS task_key,
                  t.completion_source, t.validation_failures, t.cost_total_usd
           FROM fact_session_tool_call sc JOIN derived_task t
             ON t.provider = sc.provider AND t.session_id = sc.session_id
            AND t.task_index = (
              SELECT u.task_index FROM derived_usage_event u
              WHERE u.provider = sc.provider AND u.session_id = sc.session_id
                AND u.usage_ts >= COALESCE(sc.ended_ts, sc.ts)
                AND u.task_index IS NOT NULL ORDER BY u.usage_ts, u.usage_id LIMIT 1)
           WHERE lower(sc.tool_name) = 'skill' OR substr(lower(sc.tool_name), -6) IN ('.skill', '_skill', ':skill')
           GROUP BY skill_id, task_key
         )
         SELECT skill_id, COUNT(*),
                SUM(CASE WHEN completion_source IN ('commit', 'validation_pass') THEN 1 ELSE 0 END),
                SUM(validation_failures),
                SUM(CASE WHEN completion_source IN ('commit', 'validation_pass')
                    THEN cost_total_usd ELSE 0 END)
         FROM matched GROUP BY skill_id",
    )?;
    let rows = task_stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;
    for row in rows {
        let (id, tasks, verified, failures, cost) = row?;
        if let Some(entry) = frequencies.iter_mut().find(|entry| entry.skill_id == id) {
            entry.distinct_tasks = tasks;
            entry.verified_tasks = verified;
            entry.validation_failures = failures;
            entry.cost_per_verified_usd = cost
                .filter(|_| verified > 0)
                .map(|cost| cost / verified as f64);
        }
    }
    let mut injected_stmt = conn.prepare(
        "SELECT content FROM fact_session_message WHERE provider = 'opencode' AND role = 'user'
         AND (content LIKE '# Skill: %' OR content LIKE '<skill name=%')",
    )?;
    let injected = injected_stmt.query_map([], |row| row.get::<_, String>(0))?;
    for content in injected {
        if let Some(id) = explicit_injected_skill_id(&content?) {
            if let Some(entry) = frequencies.iter_mut().find(|entry| entry.skill_id == id) {
                entry.injected += 1;
            } else {
                frequencies.push(SkillFrequencyRow {
                    skill_id: id,
                    calls: 0,
                    successful: 0,
                    failed: 0,
                    unknown_calls: 0,
                    sessions: 0,
                    calls_per_session: 0.0,
                    listed: 0,
                    injected: 1,
                    loaded: 0,
                    distinct_tasks: 0,
                    verified_tasks: 0,
                    validation_failures: 0,
                    cost_per_verified_usd: None,
                    eligible_tasks: None,
                    eligible_invoked_verified_tasks: None,
                    eligible_invoked_cost_per_verified_usd: None,
                    uninvoked_verified_tasks: None,
                    uninvoked_cost_per_verified_usd: None,
                });
            }
        }
    }
    let mut read_stmt = conn.prepare(
        "SELECT target FROM fact_session_tool_call WHERE target IS NOT NULL
         AND status = 'ok' AND output_chars > 0
         AND (target LIKE '%/skills/%/SKILL.md%' OR target LIKE '%\\skills\\%\\SKILL.md%')",
    )?;
    let reads = read_stmt.query_map([], |row| row.get::<_, String>(0))?;
    for target in reads {
        if let Some(id) = skill_id_from_file_target(&target?) {
            if let Some(entry) = frequencies.iter_mut().find(|entry| entry.skill_id == id) {
                entry.loaded += 1;
            } else {
                frequencies.push(SkillFrequencyRow {
                    skill_id: id,
                    calls: 0,
                    successful: 0,
                    failed: 0,
                    unknown_calls: 0,
                    sessions: 0,
                    calls_per_session: 0.0,
                    listed: 0,
                    injected: 0,
                    loaded: 1,
                    distinct_tasks: 0,
                    verified_tasks: 0,
                    validation_failures: 0,
                    cost_per_verified_usd: None,
                    eligible_tasks: None,
                    eligible_invoked_verified_tasks: None,
                    eligible_invoked_cost_per_verified_usd: None,
                    uninvoked_verified_tasks: None,
                    uninvoked_cost_per_verified_usd: None,
                });
            }
        }
    }
    frequencies.sort_by(|a, b| b.calls.cmp(&a.calls).then(a.skill_id.cmp(&b.skill_id)));
    for row in &mut frequencies {
        enrich_opportunities(conn, row, None)?;
    }
    Ok(frequencies)
}

fn enrich_opportunities(
    conn: &Connection,
    row: &mut SkillFrequencyRow,
    selected: Option<&str>,
) -> Result<()> {
    let resolved_listings: i64 = conn.query_row(
        "SELECT COUNT(*) FROM fact_session_context_injection
         WHERE kind = 'skill_listing' AND source = ?1",
        [&row.skill_id],
        |record| record.get(0),
    )?;
    if resolved_listings == 0 || row.distinct_tasks == 0 {
        return Ok(());
    }
    let (eligible, invoked_verified, invoked_cost, uninvoked_verified, uninvoked_cost):
        (i64, i64, Option<f64>, i64, Option<f64>) = conn.query_row(
        "WITH selected AS (SELECT * FROM derived_task WHERE ?2 IS NULL OR provider || '|' || session_id || '|' || task_index IN (SELECT value FROM json_each(?2))), invoked AS (
           SELECT DISTINCT t.provider, t.session_id, t.task_index,
                  li.label AS intent, s.agent_kind AS role, st.dominant_stage AS stage
           FROM fact_session_tool_call sc JOIN selected t
             ON t.provider = sc.provider AND t.session_id = sc.session_id
            AND t.task_index = (
              SELECT u.task_index FROM derived_usage_event u
              WHERE u.provider = sc.provider AND u.session_id = sc.session_id
                AND u.usage_ts >= COALESCE(sc.ended_ts, sc.ts)
                AND u.task_index IS NOT NULL ORDER BY u.usage_ts, u.usage_id LIMIT 1)
           LEFT JOIN view_label_effective li ON li.unit_kind = 'task'
             AND li.question_id = 'intent'
             AND li.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index
           LEFT JOIN metadata_sessions s ON s.provider = t.provider AND s.session_id = t.session_id
           LEFT JOIN derived_task_stage st ON st.provider = t.provider
             AND st.session_id = t.session_id AND st.task_index = t.task_index
           WHERE sc.target = ?1 AND (lower(sc.tool_name) = 'skill'
             OR substr(lower(sc.tool_name), -6) IN ('.skill', '_skill', ':skill'))
         ), eligible AS (
           SELECT DISTINCT t.provider, t.session_id, t.task_index,
                  t.completion_source, t.cost_total_usd,
                  EXISTS(SELECT 1 FROM invoked i WHERE i.provider = t.provider
                    AND i.session_id = t.session_id AND i.task_index = t.task_index) AS used
           FROM fact_session_context_injection listing
           JOIN selected t ON t.provider = listing.provider AND t.session_id = listing.session_id
           LEFT JOIN view_label_effective li ON li.unit_kind = 'task'
             AND li.question_id = 'intent'
             AND li.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index
           LEFT JOIN metadata_sessions s ON s.provider = t.provider AND s.session_id = t.session_id
           LEFT JOIN derived_task_stage st ON st.provider = t.provider
             AND st.session_id = t.session_id AND st.task_index = t.task_index
           WHERE listing.kind = 'skill_listing' AND listing.source = ?1
             AND (listing.ts IS NULL OR t.started_at IS NULL OR listing.ts <= t.started_at)
             AND EXISTS(SELECT 1 FROM invoked i WHERE i.intent IS NOT NULL
               AND i.intent = li.label AND i.role = s.agent_kind
               AND (i.stage = st.dominant_stage OR i.stage IS NULL OR st.dominant_stage IS NULL))
         )
         SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN used = 1 AND completion_source IN ('commit', 'validation_pass') THEN 1 ELSE 0 END), 0),
                SUM(CASE WHEN used = 1 AND completion_source IN ('commit', 'validation_pass')
                    THEN cost_total_usd END),
                COALESCE(SUM(CASE WHEN used = 0 AND completion_source IN ('commit', 'validation_pass') THEN 1 ELSE 0 END), 0),
                SUM(CASE WHEN used = 0 AND completion_source IN ('commit', 'validation_pass')
                    THEN cost_total_usd END)
         FROM eligible",
        rusqlite::params![row.skill_id, selected], |record| Ok((record.get(0)?, record.get(1)?, record.get(2)?,
            record.get(3)?, record.get(4)?)),
    )?;
    row.eligible_tasks = Some(eligible);
    row.eligible_invoked_verified_tasks = Some(invoked_verified);
    row.eligible_invoked_cost_per_verified_usd = invoked_cost
        .filter(|_| invoked_verified > 0)
        .map(|cost| cost / invoked_verified as f64);
    row.uninvoked_verified_tasks = Some(uninvoked_verified);
    row.uninvoked_cost_per_verified_usd = uninvoked_cost
        .filter(|_| uninvoked_verified > 0)
        .map(|cost| cost / uninvoked_verified as f64);
    Ok(())
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SkillEventRow {
    pub key: String,
    pub skill_id: String,
    pub current_file_fingerprint: Option<String>,
    pub version_status: String,
    pub kind: String,
    pub status: String,
    pub ts: Option<String>,
    pub task_index: Option<i64>,
    pub task_intent: Option<String>,
    pub agent_role: Option<String>,
    pub work_stage: Option<String>,
    pub body_chars: i64,
    pub body_est_tokens: i64,
    pub carried_calls: i64,
    pub task: UsageWindow,
    pub session: UsageWindow,
    pub next_skill: UsageWindow,
    pub outcome: String,
    pub follow_through: String,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct UsageWindow {
    pub calls: i64,
    pub input_tokens: i64,
    pub cached_tokens: i64,
    pub output_tokens: i64,
    pub reported_cost_usd: Option<f64>,
    pub api_equiv_usd: Option<f64>,
    pub incomplete: bool,
}

#[derive(Debug, Clone)]
struct NativeCall {
    ts: i64,
    task_index: Option<i64>,
    fresh: i64,
    cached: i64,
    output: i64,
    reported_cost: Option<f64>,
    api_equiv: Option<f64>,
    reported_tokens: bool,
}

struct TaskInfo {
    index: i64,
    end: Option<i64>,
    completion_source: String,
    outcome: String,
    intent: Option<String>,
    role: Option<String>,
    stage: Option<String>,
}

/// Counts only explicit calls as invocations. A listing is an opportunity, not use.
pub fn session_skill_events(
    conn: &Connection,
    provider: &str,
    session_id: &str,
) -> Result<Vec<SkillEventRow>> {
    let mut events = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT t.call_key, t.tool_name, t.target, t.status, COALESCE(t.ended_ts, t.ts),
                t.output_chars, COALESCE(c.est_tokens, 0), COALESCE(c.carried_calls, 0)
         FROM fact_session_tool_call t
         LEFT JOIN derived_context_item c ON c.provider = t.provider
           AND c.session_id = t.session_id AND c.item_key = 'tool:' || t.call_key
         WHERE t.provider = ?1 AND t.session_id = ?2 ORDER BY t.call_index",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    for row in rows {
        let (key, tool, target, status, ts, chars, est_tokens, carried) = row?;
        let kind = if is_skill_tool(&tool) {
            Some("call")
        } else if status == "ok"
            && chars > 0
            && target
                .as_deref()
                .and_then(skill_id_from_file_target)
                .is_some()
        {
            Some("loaded")
        } else {
            None
        };
        let Some(kind) = kind else {
            continue;
        };
        let skill_id = if kind == "loaded" {
            target
                .as_deref()
                .and_then(skill_id_from_file_target)
                .unwrap_or_else(|| "(unknown skill)".into())
        } else {
            target
                .as_deref()
                .and_then(normalize_skill_id)
                .unwrap_or_else(|| "(unknown skill)".into())
        };
        events.push(SkillEventRow {
            key,
            skill_id,
            current_file_fingerprint: None,
            version_status: "unknown".into(),
            kind: kind.into(),
            status,
            ts,
            task_index: None,
            task_intent: None,
            agent_role: None,
            work_stage: None,
            body_chars: chars,
            body_est_tokens: est_tokens,
            carried_calls: carried,
            task: UsageWindow::default(),
            session: UsageWindow::default(),
            next_skill: UsageWindow::default(),
            outcome: "unknown".into(),
            follow_through: "unknown".into(),
        });
    }

    if provider == "opencode" {
        let mut stmt = conn.prepare(
            "SELECT message_index, message_ts, content FROM fact_session_message
             WHERE provider = ?1 AND session_id = ?2 AND role = 'user' ORDER BY message_index",
        )?;
        let rows = stmt.query_map(params![provider, session_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (index, ts, content) = row?;
            let Some(skill_id) = explicit_injected_skill_id(&content) else {
                continue;
            };
            let (tokens, carried): (i64, i64) = conn
                .query_row(
                    "SELECT est_tokens, carried_calls FROM derived_context_item
                 WHERE provider = ?1 AND session_id = ?2 AND item_key = ?3",
                    params![provider, session_id, format!("user:{index}")],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap_or((0, 0));
            events.push(SkillEventRow {
                key: format!("user:{index}"),
                skill_id,
                current_file_fingerprint: None,
                version_status: "unknown".into(),
                kind: "injected".into(),
                status: "observed".into(),
                ts,
                task_index: None,
                task_intent: None,
                agent_role: None,
                work_stage: None,
                body_chars: content.chars().count() as i64,
                body_est_tokens: tokens,
                carried_calls: carried,
                task: UsageWindow::default(),
                session: UsageWindow::default(),
                next_skill: UsageWindow::default(),
                outcome: "unknown".into(),
                follow_through: "unknown".into(),
            });
        }
    }

    let mut stmt = conn.prepare(
        "SELECT item_key, ts, kind, source, chars FROM fact_session_context_injection
         WHERE provider = ?1 AND session_id = ?2
           AND kind IN ('skill_listing', 'skill_body', 'skill_injection') ORDER BY ts, item_key",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    for row in rows {
        let (key, ts, kind, source, chars) = row?;
        let (est_tokens, carried): (i64, i64) = conn
            .query_row(
                "SELECT est_tokens, carried_calls FROM derived_context_item
             WHERE provider = ?1 AND session_id = ?2 AND item_key = ?3",
                params![provider, session_id, format!("inj:{key}")],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, 0));
        events.push(SkillEventRow {
            key: format!("inj:{key}"),
            skill_id: source
                .as_deref()
                .and_then(normalize_skill_id)
                .unwrap_or_else(|| "(unresolved listing)".into()),
            current_file_fingerprint: None,
            version_status: "unknown".into(),
            kind: if kind == "skill_listing" {
                "listed"
            } else {
                "injected"
            }
            .into(),
            status: "observed".into(),
            ts,
            task_index: None,
            task_intent: None,
            agent_role: None,
            work_stage: None,
            body_chars: chars,
            body_est_tokens: est_tokens,
            carried_calls: carried,
            task: UsageWindow::default(),
            session: UsageWindow::default(),
            next_skill: UsageWindow::default(),
            outcome: "unknown".into(),
            follow_through: "unknown".into(),
        });
    }

    events.sort_by(|a, b| a.ts.cmp(&b.ts).then(a.key.cmp(&b.key)));
    let repo_root: Option<String> = conn.query_row(
        "SELECT MAX(repo_root) FROM derived_task WHERE provider = ?1 AND session_id = ?2",
        params![provider, session_id],
        |row| row.get(0),
    )?;
    if let Some(repo_root) = repo_root {
        for event in &mut events {
            if let Some(body) = local_skill_instructions(&repo_root, &event.skill_id) {
                event.current_file_fingerprint =
                    Some(format!("{:x}", md5::compute(body.as_bytes())));
                event.version_status = "current file; historical version unverified".into();
            }
        }
    }
    let calls = load_calls(conn, provider, session_id)?;
    let tasks = load_tasks(conn, provider, session_id)?;
    for index in 0..events.len() {
        if events[index].kind == "listed" {
            continue;
        }
        let Some(start) = events[index].ts.as_deref().and_then(parse_ts) else {
            continue;
        };
        let task_index = calls
            .iter()
            .find(|call| call.ts >= start)
            .and_then(|call| call.task_index);
        events[index].task_index = task_index;
        let task = task_index.and_then(|id| tasks.iter().find(|task| task.index == id));
        let task_end = task
            .and_then(|task| task.end)
            .map(|end| end.saturating_add(1));
        let next = events
            .iter()
            .skip(index + 1)
            .find(|event| event.kind != "listed")
            .and_then(|event| event.ts.as_deref().and_then(parse_ts));
        events[index].task = if task_index.is_some() {
            window(&calls, start, task_end, task_index)
        } else {
            UsageWindow {
                incomplete: true,
                ..UsageWindow::default()
            }
        };
        events[index].session = window(&calls, start, None, None);
        events[index].next_skill = window(&calls, start, next, None);
        if let Some(task) = task {
            events[index].outcome = format!("{} ({})", task.outcome, task.completion_source);
            events[index].task_intent = task.intent.clone();
            events[index].agent_role = task.role.clone();
            events[index].work_stage = task.stage.clone();
        }
        let unit_key = format!("{provider}|{session_id}|{}", events[index].key);
        let label: rusqlite::Result<String> = conn.query_row(
            "SELECT COALESCE(label, 'unknown') FROM view_label_effective
             WHERE unit_kind = 'skill' AND unit_key = ?1
               AND question_id = 'skill_follow_through' AND band IN ('confident', 'override')
             LIMIT 1",
            [unit_key],
            |row| row.get(0),
        );
        events[index].follow_through = label
            .map(|value| {
                if value == "unknown" {
                    value
                } else {
                    format!("{value} (current skill file)")
                }
            })
            .unwrap_or_else(|_| "unknown".into());
    }
    Ok(events)
}

fn is_skill_tool(name: &str) -> bool {
    name.rsplit(['.', '_', ':'])
        .next()
        .is_some_and(|last| last.eq_ignore_ascii_case("skill"))
}

fn normalize_skill_id(value: &str) -> Option<String> {
    let id = value.trim();
    (!id.is_empty()
        && id.len() <= 96
        && id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.')))
    .then(|| id.to_string())
}

fn skill_id_from_file_target(target: &str) -> Option<String> {
    let normalized = target.replace('\\', "/");
    let after = normalized.split("/skills/").nth(1)?;
    let (id, remainder) = after.split_once('/')?;
    (remainder.starts_with("SKILL.md")
        && !id.is_empty()
        && id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
    .then(|| id.to_string())
}

fn explicit_injected_skill_id(text: &str) -> Option<String> {
    let trimmed = text.trim_start();
    let candidate = trimmed
        .strip_prefix("# Skill: ")
        .map(|rest| rest.lines().next().unwrap_or(""))
        .or_else(|| trimmed.strip_prefix("<skill name=\"")?.split('"').next())?;
    let id = candidate.trim();
    (!id.is_empty()
        && id.len() <= 96
        && id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
    .then(|| id.to_string())
}

/// Read a current repository skill only; a historical session may have used another version.
pub(crate) fn local_skill_instructions(repo_root: &str, skill_id: &str) -> Option<String> {
    if skill_id.is_empty()
        || !skill_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return None;
    }
    let root = std::path::Path::new(repo_root);
    let canonical_root = root.canonicalize().ok()?;
    [".agents/skills", ".codex/skills", "skills"]
        .iter()
        .find_map(|dir| {
            let path = root.join(dir).join(skill_id).join("SKILL.md");
            if !path.canonicalize().ok()?.starts_with(&canonical_root) {
                return None;
            }
            let metadata = std::fs::metadata(&path).ok()?;
            if !metadata.is_file() || metadata.len() > 128_000 {
                return None;
            }
            std::fs::read_to_string(path).ok()
        })
}

fn window(calls: &[NativeCall], start: i64, end: Option<i64>, task: Option<i64>) -> UsageWindow {
    let mut result = UsageWindow::default();
    for call in calls.iter().filter(|call| {
        call.ts >= start
            && end.is_none_or(|end| call.ts < end)
            && task.is_none_or(|task| call.task_index == Some(task))
    }) {
        result.calls += 1;
        if call.reported_tokens {
            result.input_tokens += call.fresh;
            result.cached_tokens += call.cached;
            result.output_tokens += call.output;
        } else {
            result.incomplete = true;
        }
        if let Some(cost) = call.reported_cost {
            *result.reported_cost_usd.get_or_insert(0.0) += cost;
        } else if let Some(cost) = call.api_equiv {
            *result.api_equiv_usd.get_or_insert(0.0) += cost;
        }
    }
    result
}

fn load_calls(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<NativeCall>> {
    let mut stmt = conn.prepare(
        "SELECT d.usage_ts, d.task_index, u.input_tokens + u.cache_creation_tokens,
                u.cached_input_tokens, u.output_tokens,
                d.reported_cost_usd, d.api_equiv_usd, d.token_source
         FROM derived_usage_event d JOIN fact_session_usage u ON u.id = d.usage_id
         WHERE d.provider = ?1 AND d.session_id = ?2 ORDER BY d.usage_ts, d.usage_id",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| {
        let ts: Option<String> = row.get(0)?;
        let source: String = row.get(7)?;
        let fresh: i64 = row.get(2)?;
        let cached: i64 = row.get(3)?;
        let output: i64 = row.get(4)?;
        Ok((
            ts,
            NativeCall {
                ts: 0,
                task_index: row.get(1)?,
                fresh,
                cached,
                output,
                reported_cost: row.get(5)?,
                api_equiv: row.get(6)?,
                reported_tokens: source == "reported" && fresh + cached + output > 0,
            },
        ))
    })?;
    let mut calls = Vec::new();
    for row in rows {
        let (ts, mut call) = row?;
        if let Some(parsed) = ts.as_deref().and_then(parse_ts) {
            call.ts = parsed;
            calls.push(call);
        }
    }
    Ok(calls)
}

fn load_tasks(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<TaskInfo>> {
    let mut stmt = conn.prepare(
        "SELECT t.task_index, COALESCE(t.completed_at, t.last_activity_at), t.completion_source,
                (SELECT l.label FROM view_label_effective l WHERE l.unit_kind = 'task'
                   AND l.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index
                   AND l.question_id = 'outcome' LIMIT 1),
                (SELECT l.label FROM view_label_effective l WHERE l.unit_kind = 'task'
                   AND l.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index
                   AND l.question_id = 'intent' LIMIT 1),
                s.agent_kind, st.dominant_stage
         FROM derived_task t LEFT JOIN metadata_sessions s
           ON s.provider = t.provider AND s.session_id = t.session_id
         LEFT JOIN derived_task_stage st ON st.provider = t.provider
           AND st.session_id = t.session_id AND st.task_index = t.task_index
         WHERE t.provider = ?1 AND t.session_id = ?2 ORDER BY t.task_index",
    )?;
    let rows = stmt.query_map(params![provider, session_id], |row| {
        let end: Option<String> = row.get(1)?;
        let source: String = row.get(2)?;
        let outcome: Option<String> = row.get(3)?;
        Ok(TaskInfo {
            index: row.get(0)?,
            end: end.as_deref().and_then(parse_ts),
            completion_source: if source == "none" {
                "unknown".into()
            } else {
                source
            },
            outcome: outcome.unwrap_or_else(|| "unknown".into()),
            intent: row.get(4)?,
            role: row.get(5)?,
            stage: row.get(6)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Scoped descriptive counts. Listing/intent matches are an explicitly selected cohort,
/// never a claim that every task was an opportunity or that a skill caused its outcome.
pub fn skill_frequency_for(
    conn: &Connection,
    filters: &crate::explore::Filters,
) -> Result<Vec<SkillFrequencyRow>> {
    let tasks = crate::explore::tasks(conn, filters, crate::explore::TaskSort::Started, 1_000_000)?;
    let sessions: std::collections::BTreeSet<_> =
        tasks.iter().map(|t| (&t.provider, &t.session_id)).collect();
    let mut rows: BTreeMap<String, SkillFrequencyRow> = BTreeMap::new();
    let mut used: BTreeMap<String, std::collections::BTreeSet<(String, String, i64)>> =
        BTreeMap::new();
    let mut called_sessions: BTreeMap<String, std::collections::BTreeSet<(String, String)>> =
        BTreeMap::new();
    for (provider, session) in sessions {
        for event in session_skill_events(conn, provider, session)? {
            if event.ts.as_deref().is_some_and(|ts| {
                filters
                    .from
                    .as_deref()
                    .is_some_and(|from| ts.get(..10).is_some_and(|d| d < from))
                    || filters
                        .to
                        .as_deref()
                        .is_some_and(|to| ts.get(..10).is_some_and(|d| d > to))
            }) {
                continue;
            }
            let row = rows
                .entry(event.skill_id.clone())
                .or_insert_with(|| SkillFrequencyRow {
                    skill_id: event.skill_id.clone(),
                    calls: 0,
                    successful: 0,
                    failed: 0,
                    unknown_calls: 0,
                    sessions: 0,
                    calls_per_session: 0.0,
                    listed: 0,
                    injected: 0,
                    loaded: 0,
                    distinct_tasks: 0,
                    verified_tasks: 0,
                    validation_failures: 0,
                    cost_per_verified_usd: None,
                    eligible_tasks: None,
                    eligible_invoked_verified_tasks: None,
                    eligible_invoked_cost_per_verified_usd: None,
                    uninvoked_verified_tasks: None,
                    uninvoked_cost_per_verified_usd: None,
                });
            match event.kind.as_str() {
                "call" => {
                    row.calls += 1;
                    match event.status.as_str() {
                        "ok" => row.successful += 1,
                        "error" | "rejected" | "interrupted" => row.failed += 1,
                        _ => row.unknown_calls += 1,
                    };
                    called_sessions
                        .entry(event.skill_id.clone())
                        .or_default()
                        .insert((provider.clone(), session.clone()));
                    if let Some(task) = event.task_index {
                        used.entry(event.skill_id.clone()).or_default().insert((
                            provider.clone(),
                            session.clone(),
                            task,
                        ));
                    }
                }
                "listed" => row.listed += 1,
                "injected" => row.injected += 1,
                "loaded" => row.loaded += 1,
                _ => {}
            }
        }
    }
    for (id, row) in &mut rows {
        row.sessions = called_sessions.get(id).map_or(0, |s| s.len()) as i64;
        row.calls_per_session = row.calls as f64 / row.sessions.max(1) as f64;
        let mut costs = Vec::new();
        if let Some(keys) = used.get(id) {
            for task in &tasks {
                if keys.contains(&(
                    task.provider.clone(),
                    task.session_id.clone(),
                    task.task_index,
                )) {
                    row.distinct_tasks += 1;
                    row.validation_failures += task.validation_failures;
                    if ["commit", "validation_pass"].contains(&task.completion_source.as_str()) {
                        row.verified_tasks += 1;
                        if let Some(cost) = task.cost_total {
                            costs.push(cost);
                        }
                    }
                }
            }
        }
        row.cost_per_verified_usd = (!costs.is_empty()
            && costs.len() == row.verified_tasks as usize)
            .then(|| costs.iter().sum::<f64>() / costs.len() as f64);
    }
    let selected = serde_json::to_string(
        &tasks
            .iter()
            .map(|t| format!("{}|{}|{}", t.provider, t.session_id, t.task_index))
            .collect::<Vec<_>>(),
    )?;
    for row in rows.values_mut() {
        enrich_opportunities(conn, row, Some(&selected))?;
    }
    let mut result: Vec<_> = rows.into_values().collect();
    result.sort_by(|a, b| b.calls.cmp(&a.calls).then(a.skill_id.cmp(&b.skill_id)));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;
    use crate::db::session_store::{
        ContextInjectionRecord, MessageRecord, SessionRecord, ToolCallRecord, UsageRecord,
        store_session_record,
    };

    #[test]
    fn skill_tool_detection_is_specific() {
        assert!(is_skill_tool("skill"));
        assert!(is_skill_tool("functions.skill"));
        assert!(!is_skill_tool("skill_listing"));
    }

    #[test]
    fn current_skill_file_lookup_stays_inside_repository() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let skill = dir.path().join(".agents/skills/review");
        std::fs::create_dir_all(&skill)?;
        std::fs::write(skill.join("SKILL.md"), "# Review\nCheck tests.")?;
        assert!(local_skill_instructions(dir.path().to_str().unwrap(), "review").is_some());
        assert!(local_skill_instructions(dir.path().to_str().unwrap(), "../review").is_none());
        Ok(())
    }

    #[test]
    fn downstream_windows_can_overlap() {
        let calls = vec![NativeCall {
            ts: 2,
            task_index: Some(1),
            fresh: 5,
            cached: 7,
            output: 3,
            reported_cost: Some(0.2),
            api_equiv: None,
            reported_tokens: true,
        }];
        assert_eq!(window(&calls, 1, None, None).input_tokens, 5);
        assert_eq!(window(&calls, 2, None, None).input_tokens, 5);
        assert_eq!(window(&calls, 1, Some(2), None).calls, 0);
    }

    #[test]
    fn distinguishes_listings_calls_and_downstream_usage() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        crate::insights::init_schema(&conn)?;
        let mut record = SessionRecord::new("opencode", "s1");
        record.started_at = Some("2026-10-01T10:00:00Z".into());
        record.ended_at = Some("2026-10-01T10:00:10Z".into());
        record.messages.push(MessageRecord {
            key: "u1".into(),
            ts: Some("2026-10-01T10:00:00Z".into()),
            role: "user".into(),
            text: "do the task".into(),
            is_compaction_summary: false,
        });
        record.context_injections.push(ContextInjectionRecord {
            key: "list".into(),
            ts: Some("2026-10-01T10:00:00Z".into()),
            kind: "skill_listing".into(),
            source: Some("review".into()),
            chars: 40,
        });
        for (key, second, status) in [("a", "01", "ok"), ("b", "05", "error")] {
            record.tool_calls.push(ToolCallRecord {
                key: key.into(),
                ts: Some(format!("2026-10-01T10:00:{second}Z")),
                ended_ts: Some(format!("2026-10-01T10:00:{second}Z")),
                tool_name: "skill".into(),
                target: Some("review".into()),
                output_chars: 100,
                status: status.into(),
                ..ToolCallRecord::default()
            });
        }
        for (key, second) in [("m1", "03"), ("m2", "07")] {
            let mut usage = UsageRecord::new(key);
            usage.ts = Some(format!("2026-10-01T10:00:{second}Z"));
            usage.model = Some("claude-opus-5-5".into());
            usage.input_tokens = 10;
            usage.cache_read_tokens = 20;
            usage.output_tokens = 5;
            usage.reported_cost_usd = Some(0.01);
            record.usage.push(usage);
        }
        store_session_record(&conn, &record)?;
        crate::insights::refresh(&conn)?;
        let events = session_skill_events(&conn, "opencode", "s1")?;
        assert_eq!(events.len(), 3);
        assert_eq!(
            events.iter().filter(|event| event.kind == "call").count(),
            2
        );
        assert_eq!(events[0].kind, "listed");
        assert_eq!(events[1].next_skill.calls, 1);
        assert_eq!(events[1].session.calls, 2);
        assert_eq!(events[1].session.input_tokens, 20);
        assert_eq!(events[1].session.cached_tokens, 40);
        assert_eq!(events[1].session.output_tokens, 10);
        assert_eq!(events[2].status, "error");
        let frequencies = skill_frequency(&conn)?;
        assert_eq!(frequencies[0].calls, 2);
        assert_eq!(frequencies[0].sessions, 1);
        assert_eq!(frequencies[0].listed, 1);
        assert_eq!(frequencies[0].distinct_tasks, 1);
        assert_eq!(frequencies[0].eligible_tasks, Some(1));
        Ok(())
    }
}
