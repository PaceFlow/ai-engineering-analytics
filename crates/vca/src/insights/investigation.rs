//! Read-only, deterministic investigation queries shared by both CLIs and TUI.
use super::{audit, structure::parse_ts, work::WorkItem};
use anyhow::{Result, ensure};
use rusqlite::{Connection, params};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EvidenceRow {
    pub reference: audit::Reference,
    pub occurred_at: Option<String>,
    pub kind: String,
    pub excerpt: String,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Interval {
    pub kind: String,
    pub key: String,
    pub opened_by: String,
    pub closed_by: Option<String>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub minutes: Option<f64>,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Hotspot {
    pub kind: String,
    pub target: String,
    pub occurrences: i64,
    pub interpretation: String,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Explanation {
    pub schema_version: u32,
    pub work_id: String,
    pub data_cutoff: Option<i64>,
    pub policy_version: String,
    pub native_tokens: super::work::Tokens,
    pub estimated_tokens: super::work::Tokens,
    pub missing_usage_records: usize,
    pub captured_usage_events: usize,
    pub reported_cost_usd: Option<f64>,
    pub reported_cost_records: usize,
    pub activity_minutes: f64,
    pub observed_span_minutes: Option<f64>,
    pub stages: Vec<(String, Option<String>)>,
    pub state: String,
    pub completion_at: Option<i64>,
    pub verified: bool,
    pub corrective_purpose: String,
    pub statements: Vec<String>,
    pub gaps: Vec<String>,
    pub lifecycle: Vec<audit::Event>,
    pub intervals: Vec<Interval>,
    pub hotspots: Vec<Hotspot>,
    pub pull_requests: Vec<super::delivery::PrEvidence>,
    pub members: Vec<String>,
    pub evidence: Vec<EvidenceRow>,
    pub next_cursor: Option<String>,
}
pub fn purpose(
    conn: &Connection,
    provider: &str,
    session: &str,
    task: i64,
) -> Result<(String, String, String)> {
    let key = crate::classify::heuristic::task_unit_key(provider, session, task);
    let get = |q: &str| -> Result<(String, String)> {
        Ok(conn.query_row("SELECT label,source FROM view_label_effective WHERE unit_kind='task' AND unit_key=?1 AND question_id=?2",params![key,q],|r|Ok((r.get(0)?,r.get(1)?))).unwrap_or(("unknown".into(),"missing".into())))
    };
    let (p, s) = get("purpose")?;
    let (sub, _) = get("corrective_subtype")?;
    Ok((p, sub, s))
}
pub fn explain(
    conn: &Connection,
    work: &WorkItem,
    include_evidence: bool,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Explanation> {
    ensure!((1..=100).contains(&limit), "Evidence limit must be 1..100");
    let mut history = vec![];
    let mut excerpts = vec![];
    let mut hotspots = vec![];
    let mut statements = vec![];
    let mut gaps = vec![];
    let mut purposes = BTreeSet::new();
    for member in &work.members {
        let t = &member.task;
        history.extend(audit::events(conn, "task", &member.id)?);
        let (p, _, _) = purpose(conn, &t.provider, &t.session_id, t.task_index)?;
        purposes.insert(p);
        if t.completed_at.is_some() && work.completed_at.is_none() {
            statements.push(format!("Task {} has a historical completion mark; the current goal is not supported complete.",member.id));
        }
        if t.outcome_source.as_deref() == Some("heuristic") {
            gaps.push(format!(
                "Task {} outcome uses heuristic evidence.",
                member.id
            ));
        }
        let mut stmt=conn.prepare("SELECT COALESCE(target,tool_name),COUNT(*) FROM fact_session_tool_call WHERE provider=?1 AND session_id=?2 AND (target LIKE '%SKILL.md%' OR target LIKE '%gh pr checks%' OR target LIKE '%gh run list%') GROUP BY COALESCE(target,tool_name) HAVING COUNT(*)>1 ORDER BY COUNT(*) DESC LIMIT 10")?;
        for row in stmt.query_map(params![t.provider, t.session_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (target, occurrences) = row?;
            hotspots.push(Hotspot{kind:if target.contains("SKILL.md"){"instruction_load"}else{"status_check"}.into(),target:crate::classify::state::head_tail(&crate::classify::state::redact(&target).0,240),occurrences,interpretation:"Repeated recorded operations are investigation candidates; necessity and avoidable cost are unknown. Session-level context may include other tasks.".into()});
        }
        if include_evidence {
            let turns = crate::classify::state::load_turns(conn, &t.provider, &t.session_id)?;
            let allowed: BTreeSet<_>=conn.query_row("SELECT COALESCE(turn_list,'') FROM derived_task WHERE provider=?1 AND session_id=?2 AND task_index=?3",params![t.provider,t.session_id,t.task_index],|r|r.get::<_,String>(0))?.split(',').filter_map(|x|x.parse::<i64>().ok()).collect();
            let refs: BTreeSet<_> = turns
                .iter()
                .filter(|turn| allowed.contains(&turn.index))
                .flat_map(|t| t.source_refs.iter())
                .filter_map(|v| v["message_key"].as_str().map(str::to_owned))
                .collect();
            let tool_refs: BTreeSet<_> = turns
                .iter()
                .filter(|turn| allowed.contains(&turn.index))
                .flat_map(|t| t.source_refs.iter())
                .filter_map(|v| v["call_key"].as_str().map(str::to_owned))
                .collect();
            let mut tools=conn.prepare("SELECT call_key,ts,tool_name,target,status,exit_code,error_signature FROM fact_session_tool_call WHERE provider=?1 AND session_id=?2 ORDER BY call_index")?;
            for row in tools.query_map(params![t.provider, t.session_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                ))
            })? {
                let (key, at, name, target, status, exit, error) = row?;
                if !tool_refs.contains(&key) {
                    continue;
                }
                let text = format!(
                    "{name}: {} · status {status} · exit {exit:?} · {}",
                    target.unwrap_or_default(),
                    error.unwrap_or_default()
                );
                excerpts.push(EvidenceRow {
                    reference: audit::Reference::Tool {
                        provider: t.provider.clone(),
                        session_id: t.session_id.clone(),
                        call_key: key,
                    },
                    occurred_at: at,
                    kind: "tool".into(),
                    excerpt: crate::classify::state::head_tail(
                        &crate::classify::state::redact(&text).0,
                        1200,
                    ),
                });
            }
            let mut stmt=conn.prepare("SELECT COALESCE(message_key,'index:' || message_index),message_ts,role,content FROM fact_session_message WHERE provider=?1 AND session_id=?2 ORDER BY message_index")?;
            for row in stmt.query_map(params![t.provider, t.session_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })? {
                let (key, ts, role, text) = row?;
                if !refs.contains(&key) {
                    continue;
                }
                let text = if role == "user" {
                    crate::classify::request::normalize(&text).text
                } else {
                    text
                };
                let text = crate::classify::state::redact(&text).0;
                excerpts.push(EvidenceRow {
                    reference: audit::Reference::Message {
                        provider: t.provider.clone(),
                        session_id: t.session_id.clone(),
                        message_key: key,
                    },
                    occurred_at: ts,
                    kind: role,
                    excerpt: crate::classify::state::head_tail(&text, 1200),
                });
            }
        }
    }
    for pr in &work.pull_requests {
        history.extend(audit::events(conn, "pr", &pr.pr.id)?);
    }
    history.sort_by_key(|e| {
        (
            e.occurred_at.clone().unwrap_or(e.observed_at.clone()),
            e.observed_at.clone(),
            e.id.clone(),
        )
    });
    let replaced: BTreeSet<_> = history
        .iter()
        .filter_map(|e| e.supersedes_id.as_deref())
        .collect();
    let mut waiting: BTreeMap<(String, String), &audit::Event> = BTreeMap::new();
    let mut intervals = vec![];
    for e in history.iter().filter(|e| !replaced.contains(e.id.as_str())) {
        let pair = match &e.payload {
            audit::Payload::Blocked { block_id, .. } => Some(("blocked", block_id, true)),
            audit::Payload::Unblocked { block_id, .. } => Some(("blocked", block_id, false)),
            audit::Payload::ApprovalRequested { approval_id, .. } => {
                Some(("approval", approval_id, true))
            }
            audit::Payload::ApprovalGranted { approval_id, .. }
            | audit::Payload::ApprovalDeclined { approval_id, .. } => {
                Some(("approval", approval_id, false))
            }
            _ => None,
        };
        if let Some((kind, key, opening)) = pair {
            let identity = (
                format!("{}:{}:{kind}", e.subject_kind, e.subject_key),
                key.clone(),
            );
            if opening {
                waiting.entry(identity).or_insert(e);
            } else if let Some(start) = waiting.remove(&identity) {
                let a = start.occurred_at.as_deref().and_then(parse_ts);
                let b = e.occurred_at.as_deref().and_then(parse_ts);
                intervals.push(Interval {
                    kind: kind.into(),
                    key: key.clone(),
                    opened_by: start.id.clone(),
                    closed_by: Some(e.id.clone()),
                    start_ms: a,
                    end_ms: b,
                    minutes: a
                        .zip(b)
                        .filter(|(a, b)| b >= a)
                        .map(|(a, b)| (b - a) as f64 / 60000.),
                });
            } else {
                gaps.push(format!(
                    "Lifecycle event {} has no matching opening event.",
                    e.id
                ));
            }
        }
        match &e.payload {
            audit::Payload::AcceptanceFailed{criterion_id,..}|audit::Payload::AcceptanceWithdrawn{criterion_id,..}=>statements.push(format!("Acceptance criterion {criterion_id} failed or was withdrawn; deployment is assessed separately (event {}).",e.id)),
            audit::Payload::CompletionWithdrawn{reason,..}=>statements.push(format!("Completion was withdrawn: {} (event {}).",crate::classify::state::redact(reason).0,e.id)),
            audit::Payload::CorrectionDecision{origin_member_id,attribution,correction_type,..}=>statements.push(format!("Correction {correction_type}: attribution {attribution}; origin {} (event {}).",origin_member_id.as_deref().unwrap_or("unknown"),e.id)),_=>{}
        }
    }
    for ((_, key), e) in waiting {
        intervals.push(Interval {
            kind: if matches!(e.payload, audit::Payload::Blocked { .. }) {
                "blocked"
            } else {
                "approval"
            }
            .into(),
            key,
            opened_by: e.id.clone(),
            closed_by: None,
            start_ms: e.occurred_at.as_deref().and_then(parse_ts),
            end_ms: None,
            minutes: None,
        });
    }
    if history.is_empty() {
        gaps.push("No structured lifecycle history; elapsed gaps cannot be attributed to approval or blocking.".into());
    }
    if work.pull_requests.is_empty() {
        gaps.push("No linked PR/deployment evidence; delivery remains unknown.".into());
    }
    gaps.push("Corrective workload is not a measure of AI-caused defects. Unattributed corrections remain origin unknown.".into());
    if work.members_missing_usage > 0 {
        gaps.push("Some contributing work has unavailable usage; zero must not be interpreted as savings.".into());
    }
    statements.insert(0,format!("Current supported goal state: {}. {} root conversation(s), {} recorded attempt(s), {} correction feedback event(s).",work.state,work.conversations,work.attempts,work.corrections));
    excerpts.sort_by_key(|e| {
        (
            e.occurred_at.clone(),
            serde_json::to_string(&e.reference).unwrap_or_default(),
        )
    });
    excerpts.dedup_by(|a, b| a.reference == b.reference);
    let digest = format!(
        "{:x}",
        md5::compute(serde_json::to_vec(&(&excerpts, &history))?)
    );
    let offset = if let Some(c) = cursor {
        let (d, n) = c
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("Invalid evidence cursor"))?;
        ensure!(d == digest, "Evidence changed; restart pagination");
        n.parse::<usize>()?
    } else {
        0
    };
    ensure!(offset <= excerpts.len(), "Evidence cursor out of range");
    let next_cursor =
        (offset + limit < excerpts.len()).then(|| format!("{digest}:{}", offset + limit));
    let evidence = excerpts.into_iter().skip(offset).take(limit).collect();
    // Redact imported free text as well as source excerpts.
    let mut native_tokens = super::work::Tokens::default();
    let mut estimated_tokens = super::work::Tokens::default();
    let mut reported = None;
    let mut reported_records = 0;
    for usage in &work.usage {
        if usage.available {
            if usage.native {
                native_tokens.add(&usage.tokens)
            } else {
                estimated_tokens.add(&usage.tokens)
            }
        }
        if let Some(cost) = usage.reported_cost {
            reported = Some(reported.unwrap_or(0.0) + cost);
            reported_records += 1;
        }
    }
    let mut result = Explanation {
        native_tokens,
        estimated_tokens,
        missing_usage_records: work.usage.iter().filter(|u| !u.available).count(),
        captured_usage_events: work.usage.len(),
        reported_cost_usd: reported,
        reported_cost_records: reported_records,
        activity_minutes: work.active_minutes(),
        observed_span_minutes: work
            .started_at
            .zip(work.last_activity_at)
            .map(|(a, b)| (b - a).max(0) as f64 / 60000.),
        stages: work
            .members
            .iter()
            .map(|m| (m.id.clone(), m.task.stage_path.clone()))
            .collect(),
        schema_version: 1,
        work_id: work.id.clone(),
        data_cutoff: work.last_activity_at,
        policy_version: crate::classify::questions::QUESTION_SET_VERSION.into(),
        state: work.state.clone(),
        completion_at: work.completed_at,
        verified: work.verified,
        corrective_purpose: if purposes.len() == 1 {
            purposes.into_iter().next().unwrap()
        } else {
            "mixed".into()
        },
        statements,
        gaps,
        lifecycle: history,
        intervals,
        hotspots,
        pull_requests: work.pull_requests.clone(),
        members: work.members.iter().map(|m| m.id.clone()).collect(),
        evidence,
        next_cursor,
    };
    fn sanitize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::String(s) => *s = crate::classify::state::redact(s).0,
            serde_json::Value::Array(items) => {
                for item in items {
                    sanitize(item)
                }
            }
            serde_json::Value::Object(items) => {
                for item in items.values_mut() {
                    sanitize(item)
                }
            }
            _ => {}
        }
    }
    for event in &mut result.lifecycle {
        let mut payload = serde_json::to_value(&event.payload)?;
        sanitize(&mut payload);
        event.payload = serde_json::from_value(payload)?;
    }
    for pr in &mut result.pull_requests {
        pr.pr.title = crate::classify::state::redact(&pr.pr.title).0;
        pr.pr.description = crate::classify::state::redact(&pr.pr.description).0;
    }

    Ok(result)
}

pub fn text(report: &Explanation) -> String {
    let mut out = format!(
        "Work {} · {} · corrective purpose {}\n",
        report.work_id, report.state, report.corrective_purpose
    );
    out.push_str(&format!("  {} native tokens · {} estimated tokens · {} unavailable usage records · {:.1} estimated activity minutes\n",report.native_tokens.total,report.estimated_tokens.total,report.missing_usage_records,report.activity_minutes));
    for statement in &report.statements {
        out.push_str(&format!("  {statement}\n"));
    }
    for interval in &report.intervals {
        out.push_str(&format!(
            "  {} {}: {}\n",
            interval.kind,
            interval.key,
            interval
                .minutes
                .map(|m| format!("{m:.1} observed minutes"))
                .unwrap_or("open or incomplete interval".into())
        ));
    }
    for hotspot in &report.hotspots {
        out.push_str(&format!(
            "  {}: {} recorded operations · {}\n",
            hotspot.kind, hotspot.occurrences, hotspot.target
        ));
    }
    for gap in &report.gaps {
        out.push_str(&format!("  Gap: {gap}\n"));
    }
    for row in &report.evidence {
        out.push_str(&format!(
            "  Evidence {}: {}\n",
            serde_json::to_string(&row.reference).unwrap_or_default(),
            row.excerpt
        ));
    }
    if let Some(cursor) = &report.next_cursor {
        out.push_str(&format!("  Next evidence cursor: {cursor}\n"));
    }
    out
}
