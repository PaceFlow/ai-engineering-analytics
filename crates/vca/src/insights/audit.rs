//! Persistent lifecycle assertions, distinct from captured provider records.
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS audit_work_event (
 id TEXT PRIMARY KEY, subject_kind TEXT NOT NULL, subject_key TEXT NOT NULL,
 kind TEXT NOT NULL, occurred_at TEXT, observed_at TEXT NOT NULL, source TEXT NOT NULL,
 source_key TEXT NOT NULL, schema_version INTEGER NOT NULL, payload_json TEXT NOT NULL,
 supersedes_id TEXT, UNIQUE(source,source_key));
 CREATE INDEX IF NOT EXISTS idx_audit_subject ON audit_work_event(subject_kind,subject_key,occurred_at);";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reference {
    Message {
        provider: String,
        session_id: String,
        message_key: String,
    },
    Tool {
        provider: String,
        session_id: String,
        call_key: String,
    },
    Task {
        member_id: String,
    },
    PullRequest {
        id: String,
        fingerprint: Option<String>,
    },
    Deployment {
        id: String,
    },
    Classification {
        id: i64,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    CompletionSupported {
        verified: bool,
        evidence: Vec<Reference>,
    },
    CompletionWithdrawn {
        reason: String,
        evidence: Vec<Reference>,
    },
    Reopened {
        reason: String,
        evidence: Vec<Reference>,
    },
    Blocked {
        block_id: String,
        reason: String,
        evidence: Vec<Reference>,
    },
    Unblocked {
        block_id: String,
        evidence: Vec<Reference>,
    },
    ApprovalRequested {
        approval_id: String,
        evidence: Vec<Reference>,
    },
    ApprovalGranted {
        approval_id: String,
        evidence: Vec<Reference>,
    },
    ApprovalDeclined {
        approval_id: String,
        evidence: Vec<Reference>,
    },
    AcceptanceRequired {
        criterion_id: String,
        evidence: Vec<Reference>,
    },
    AcceptancePassed {
        criterion_id: String,
        evidence: Vec<Reference>,
    },
    AcceptanceFailed {
        criterion_id: String,
        evidence: Vec<Reference>,
    },
    AcceptanceWithdrawn {
        criterion_id: String,
        evidence: Vec<Reference>,
    },
    CorrectionDecision {
        origin_member_id: Option<String>,
        origin_turn: Option<Box<Reference>>,
        correction_turn: Option<Box<Reference>>,
        correction_type: String,
        attribution: String,
        reason: String,
        evidence: Vec<Reference>,
    },
}
impl Payload {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::CompletionSupported { .. } => "completion_supported",
            Self::CompletionWithdrawn { .. } => "completion_withdrawn",
            Self::Reopened { .. } => "reopened",
            Self::Blocked { .. } => "blocked",
            Self::Unblocked { .. } => "unblocked",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ApprovalGranted { .. } => "approval_granted",
            Self::ApprovalDeclined { .. } => "approval_declined",
            Self::AcceptanceRequired { .. } => "acceptance_required",
            Self::AcceptancePassed { .. } => "acceptance_passed",
            Self::AcceptanceFailed { .. } => "acceptance_failed",
            Self::AcceptanceWithdrawn { .. } => "acceptance_withdrawn",
            Self::CorrectionDecision { .. } => "correction_decision",
        }
    }
    pub fn evidence(&self) -> &[Reference] {
        match self {
            Self::CompletionSupported { evidence, .. }
            | Self::CompletionWithdrawn { evidence, .. }
            | Self::Reopened { evidence, .. }
            | Self::Blocked { evidence, .. }
            | Self::Unblocked { evidence, .. }
            | Self::ApprovalRequested { evidence, .. }
            | Self::ApprovalGranted { evidence, .. }
            | Self::ApprovalDeclined { evidence, .. }
            | Self::AcceptanceRequired { evidence, .. }
            | Self::AcceptancePassed { evidence, .. }
            | Self::AcceptanceFailed { evidence, .. }
            | Self::AcceptanceWithdrawn { evidence, .. }
            | Self::CorrectionDecision { evidence, .. } => evidence,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub id: String,
    pub subject_kind: String,
    pub subject_key: String,
    pub occurred_at: Option<String>,
    pub observed_at: String,
    pub source: String,
    pub source_key: String,
    pub schema_version: u32,
    pub payload: Payload,
    pub supersedes_id: Option<String>,
}
pub fn init(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA)?;
    for (table, column) in [
        ("classifier_request", "input_manifest_json"),
        ("classification_result", "evidence_json"),
        ("derived_work_index", "purpose_json"),
        ("published_work_usage", "activity_purpose"),
        ("published_work_activity", "activity_purpose"),
    ] {
        let found = conn
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|x| x == column);
        if !found {
            conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"))?;
        }
    }
    Ok(())
}
pub fn save(conn: &Connection, event: &Event) -> Result<bool> {
    ensure!(
        event.schema_version == 1,
        "Unsupported work-event payload version"
    );
    ensure!(
        ["task", "pr"].contains(&event.subject_kind.as_str()),
        "Work event subject must be task or pr"
    );
    ensure!(
        !event.id.is_empty() && !event.subject_key.is_empty() && !event.source_key.is_empty(),
        "Work event identities must not be empty"
    );
    ensure!(
        ["classifier", "manual", "import", "legacy"].contains(&event.source.as_str()),
        "Unknown work event source"
    );
    DateTime::parse_from_rfc3339(&event.observed_at)?;
    if let Some(ts) = &event.occurred_at {
        DateTime::parse_from_rfc3339(ts)?;
    }
    match &event.payload {
        Payload::Blocked { block_id, .. } | Payload::Unblocked { block_id, .. } => {
            ensure!(!block_id.is_empty(), "Block id required")
        }
        Payload::ApprovalRequested { approval_id, .. }
        | Payload::ApprovalGranted { approval_id, .. }
        | Payload::ApprovalDeclined { approval_id, .. } => {
            ensure!(!approval_id.is_empty(), "Approval id required")
        }
        Payload::AcceptanceRequired { criterion_id, .. }
        | Payload::AcceptancePassed { criterion_id, .. }
        | Payload::AcceptanceFailed { criterion_id, .. }
        | Payload::AcceptanceWithdrawn { criterion_id, .. } => {
            ensure!(!criterion_id.is_empty(), "Criterion id required")
        }
        Payload::CorrectionDecision {
            attribution,
            correction_type,
            evidence,
            ..
        } => {
            ensure!(
                [
                    "unknown",
                    "suspected",
                    "corroborated",
                    "confirmed",
                    "rejected"
                ]
                .contains(&attribution.as_str()),
                "Invalid attribution"
            );
            ensure!(
                [
                    "defect_correction",
                    "refinement",
                    "requirement_change",
                    "continuation",
                    "incidental",
                    "unknown"
                ]
                .contains(&correction_type.as_str()),
                "Invalid correction type"
            );
            ensure!(
                attribution != "confirmed" || event.source == "manual",
                "Confirmation requires a manual decision"
            );
            ensure!(
                attribution != "corroborated"
                    || (event.source != "classifier" && !evidence.is_empty()),
                "Corroboration needs non-classifier evidence"
            );
        }
        _ => {}
    }
    let payload = serde_json::to_string(&event.payload)?;
    type StoredEvent = (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        i64,
    );
    let prior:Option<StoredEvent>=conn.query_row("SELECT id,subject_kind,subject_key,payload_json,occurred_at,supersedes_id,schema_version FROM audit_work_event WHERE source=?1 AND source_key=?2",params![event.source,event.source_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    if let Some(p) = prior {
        ensure!(
            p == (
                event.id.clone(),
                event.subject_kind.clone(),
                event.subject_key.clone(),
                payload,
                event.occurred_at.clone(),
                event.supersedes_id.clone(),
                event.schema_version as i64
            ),
            "Conflicting event; append a new source key and explicit supersession"
        );
        return Ok(false);
    }
    if let Some(id) = &event.supersedes_id {
        let subject: Option<(String, String)> = conn
            .query_row(
                "SELECT subject_kind,subject_key FROM audit_work_event WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        ensure!(
            subject == Some((event.subject_kind.clone(), event.subject_key.clone())),
            "Supersession must reference an existing event for the same subject"
        );
        let replaced: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM audit_work_event WHERE supersedes_id=?1)",
            [id],
            |r| r.get(0),
        )?;
        ensure!(
            !replaced,
            "Event already superseded; supersede the latest revision"
        );
    }
    conn.execute(
        "INSERT INTO audit_work_event VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            event.id,
            event.subject_kind,
            event.subject_key,
            event.payload.kind(),
            event.occurred_at,
            event.observed_at,
            event.source,
            event.source_key,
            event.schema_version,
            payload,
            event.supersedes_id
        ],
    )?;
    Ok(true)
}
pub fn events(conn: &Connection, kind: &str, key: &str) -> Result<Vec<Event>> {
    let mut stmt=conn.prepare("SELECT id,occurred_at,observed_at,source,source_key,schema_version,payload_json,supersedes_id FROM audit_work_event WHERE subject_kind=?1 AND subject_key=?2 ORDER BY COALESCE(occurred_at,observed_at),observed_at,id")?;
    let rows = stmt
        .query_map(params![kind, key], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, u32>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, Option<String>>(7)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(
            |(
                id,
                occurred_at,
                observed_at,
                source,
                source_key,
                schema_version,
                payload,
                supersedes_id,
            )| {
                Ok(Event {
                    id,
                    subject_kind: kind.into(),
                    subject_key: key.into(),
                    occurred_at,
                    observed_at,
                    source,
                    source_key,
                    schema_version,
                    payload: serde_json::from_str(&payload)?,
                    supersedes_id,
                })
            },
        )
        .collect()
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct Resolution {
    pub completed_at: Option<i64>,
    pub verified: bool,
    pub acceptance: String,
    pub history: Vec<Event>,
    pub open_criteria: Vec<String>,
    pub conflicts: Vec<String>,
}
/// One resolver for work aggregation and conversation drill-down. Provider claims
/// and generic passing commands never establish supported goal completion.
pub fn resolve(
    conn: &Connection,
    member: &str,
    candidate: Option<i64>,
    verified: bool,
) -> Result<Resolution> {
    let history = events(conn, "task", member)?;
    let superseded: std::collections::BTreeSet<_> = history
        .iter()
        .filter_map(|e| e.supersedes_id.as_deref())
        .collect();
    let mut completion = candidate;
    let mut verified = verified;
    let mut criteria = std::collections::BTreeSet::new();
    let mut conflicts = vec![];
    for event in history
        .iter()
        .filter(|e| e.source != "legacy" && !superseded.contains(e.id.as_str()))
    {
        let ts = event
            .occurred_at
            .as_deref()
            .and_then(super::structure::parse_ts);
        if event.source == "manual"
            && matches!(&event.payload,Payload::CompletionWithdrawn{reason,..} if reason=="Manual completion override cleared")
        {
            continue;
        }
        let missing = event
            .payload
            .evidence()
            .iter()
            .map(|r| reference_available(conn, r))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .any(|available| !*available);
        if missing {
            conflicts.push(format!(
                "Event {} has unresolved evidence references",
                event.id
            ));
        }
        if matches!(event.payload, Payload::CompletionSupported { .. })
            && (missing || (event.source == "import" && event.payload.evidence().is_empty()))
        {
            conflicts.push(format!(
                "Completion event {} lacks available supporting evidence",
                event.id
            ));
            continue;
        }
        match &event.payload {
            Payload::CompletionSupported { verified: v, .. } => {
                if let Some(at) = ts
                    && completion.is_none_or(|c| at >= c)
                {
                    completion = Some(at);
                    verified = *v;
                }
            }
            Payload::CompletionWithdrawn { .. } | Payload::Reopened { .. } => {
                if ts.is_none() || completion.is_none_or(|c| ts.unwrap() >= c) {
                    completion = None;
                    verified = false;
                }
            }
            Payload::AcceptanceRequired { criterion_id, .. }
            | Payload::AcceptanceFailed { criterion_id, .. }
            | Payload::AcceptanceWithdrawn { criterion_id, .. } => {
                criteria.insert(criterion_id.clone());
            }
            Payload::AcceptancePassed { criterion_id, .. } => {
                criteria.remove(criterion_id);
            }
            _ => {}
        }
    }
    if !criteria.is_empty() && completion.is_some() {
        conflicts.push("Completion evidence exists but required acceptance is outstanding".into());
        completion = None;
        verified = false;
    }
    let acceptance = if !criteria.is_empty() {
        "pending"
    } else if history
        .iter()
        .any(|e| matches!(e.payload, Payload::AcceptancePassed { .. }))
    {
        "passed"
    } else {
        "unknown"
    };
    Ok(Resolution {
        completed_at: completion,
        verified,
        acceptance: acceptance.into(),
        history: history.clone(),
        open_criteria: criteria.into_iter().collect(),
        conflicts,
    })
}
/// Classifier and legacy decisions use content-derived keys: a repeated refresh
/// cannot duplicate history or change the observation time of an earlier record.
pub fn record_completion(
    conn: &Connection,
    member: &str,
    source: &str,
    at: Option<i64>,
    verified: bool,
    evidence: Vec<Reference>,
    reason: &str,
) -> Result<()> {
    let payload = if at.is_some() {
        Payload::CompletionSupported { verified, evidence }
    } else {
        Payload::CompletionWithdrawn {
            reason: reason.into(),
            evidence,
        }
    };
    let previous: Option<(String,String)> = conn.query_row("SELECT id,payload_json FROM audit_work_event WHERE subject_kind='task' AND subject_key=?1 AND source=?2 AND kind IN ('completion_supported','completion_withdrawn') AND NOT EXISTS(SELECT 1 FROM audit_work_event n WHERE n.supersedes_id=audit_work_event.id) ORDER BY rowid DESC LIMIT 1",params![member,source],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let serialized = serde_json::to_string(&payload)?;
    if previous.as_ref().is_some_and(|(_, p)| p == &serialized) {
        return Ok(());
    }
    let supersedes_id = previous.map(|(id, _)| id);
    let key = format!(
        "{:x}",
        md5::compute(format!(
            "{member}|{source}|{at:?}|{serialized}|{supersedes_id:?}"
        ))
    );
    save(
        conn,
        &Event {
            id: format!("completion-{key}"),
            subject_kind: "task".into(),
            subject_key: member.into(),
            occurred_at: at
                .and_then(DateTime::from_timestamp_millis)
                .map(|t| t.to_rfc3339()),
            observed_at: Utc::now().to_rfc3339(),
            source: source.into(),
            source_key: key,
            schema_version: 1,
            payload,
            supersedes_id,
        },
    )?;
    Ok(())
}

pub fn task_resolution(conn: &Connection, task: &crate::explore::TaskRow) -> Result<Resolution> {
    let member:Option<String>=conn.query_row("SELECT identity_id FROM derived_work_member WHERE provider=?1 AND session_id=?2 AND task_index=?3",params![task.provider,task.session_id,task.task_index],|r|r.get(0)).optional()?;
    let supported = task.completion_source == "override"
        || task.completion_source == "jev"
            && matches!(
                task.outcome.as_deref(),
                Some("completed_verified" | "completed_unverified" | "answered")
            );
    let candidate = supported
        .then(|| {
            task.completed_at
                .as_deref()
                .and_then(super::structure::parse_ts)
        })
        .flatten();
    let Some(member) = member else {
        return Ok(Resolution {
            completed_at: candidate,
            verified: task.outcome.as_deref() == Some("completed_verified"),
            acceptance: "unknown".into(),
            ..Default::default()
        });
    };
    let mut resolution = resolve(
        conn,
        &member,
        candidate,
        task.outcome.as_deref() == Some("completed_verified"),
    )?;
    if let Some(at) = resolution.completed_at {
        let later:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM derived_turn t JOIN view_label_effective l ON l.unit_kind='exchange' AND l.question_id='feedback' AND l.unit_key=t.provider || '|' || t.session_id || '|' || t.turn_index JOIN derived_task d ON d.provider=t.provider AND d.session_id=t.session_id WHERE d.provider=?1 AND d.session_id=?2 AND d.task_index=?3 AND (',' || d.turn_list || ',') LIKE ('%,' || t.turn_index || ',%') AND l.label='correct_defect' AND l.source IN ('jev','override') AND julianday(t.started_at)>julianday(?4))",params![task.provider,task.session_id,task.task_index,DateTime::from_timestamp_millis(at).map(|t|t.to_rfc3339())],|r|r.get(0))?;
        if later {
            resolution.completed_at = None;
            resolution.verified = false;
            resolution
                .conflicts
                .push("A later classified correction withdraws the earlier completion".into());
        }
    }
    Ok(resolution)
}
/// Persist retained pre-journal marks as legacy decisions, never as historical
/// observations. New manual/classifier decisions already have their own history.
pub fn preserve_legacy(conn: &Connection) -> Result<()> {
    let rows=conn.prepare("SELECT m.identity_id,t.completed_at,t.completion_source FROM derived_work_member m JOIN derived_task t USING(provider,session_id,task_index) WHERE (t.completion_source IN ('jev','override') OR t.completion_source LIKE '%:not_completed') AND NOT EXISTS(SELECT 1 FROM audit_work_event a WHERE a.subject_kind='task' AND a.subject_key=m.identity_id)")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,String>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for (member, at, source) in rows {
        record_completion(
            conn,
            &member,
            "legacy",
            at.as_deref().and_then(super::structure::parse_ts),
            false,
            vec![],
            &format!("Retained {source} decision; original observation/evidence unavailable"),
        )?;
    }
    Ok(())
}

/// Persist explicit correction feedback even when both turns are in one task
/// and no local code change was captured. Origin attribution remains suspected.
pub fn capture_corrections(conn: &Connection) -> Result<()> {
    let rows=conn.prepare("SELECT m.identity_id,t.provider,t.session_id,t.turn_index,t.started_at,l.source,l.unit_key FROM derived_turn t JOIN derived_work_member m ON m.provider=t.provider AND m.session_id=t.session_id JOIN derived_task d ON d.provider=m.provider AND d.session_id=m.session_id AND d.task_index=m.task_index JOIN view_label_effective l ON l.unit_kind='exchange' AND l.question_id='feedback' AND l.unit_key=t.provider || '|' || t.session_id || '|' || t.turn_index WHERE (',' || d.turn_list || ',') LIKE ('%,' || t.turn_index || ',%') AND l.label='correct_defect' AND l.source IN ('jev','override')")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,String>(5)?,r.get::<_,String>(6)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for (member, provider, session, turn, at, source, key) in rows {
        let turns = crate::classify::state::load_turns(conn, &provider, &session)?;
        let reference = |index: i64| -> Option<Reference> {
            turns
                .iter()
                .find(|t| t.index == index)?
                .source_refs
                .first()
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        };
        let correction_turn = reference(turn);
        let origin_turn: Option<Reference> = turns
            .iter()
            .find(|t| t.index == turn - 1 && !t.assistant.is_empty())
            .and_then(|t| t.source_refs.iter().rev().find(|v| v["type"] == "message"))
            .and_then(|v| serde_json::from_value(v.clone()).ok());
        let id = format!(
            "feedback-{:x}",
            md5::compute(format!("{key}|{source}|{member}"))
        );
        save(conn,&Event{id:id.clone(),subject_kind:"task".into(),subject_key:member,occurred_at:at,observed_at:Utc::now().to_rfc3339(),source:if source=="override"{"manual"}else{"classifier"}.into(),source_key:id,schema_version:1,payload:Payload::CorrectionDecision{origin_member_id:None,origin_turn:origin_turn.map(Box::new),correction_turn:correction_turn.clone().map(Box::new),correction_type:"defect_correction".into(),attribution:"suspected".into(),reason:"Feedback identifies a correction; introducing change and AI causation are not established".into(),evidence:correction_turn.into_iter().collect()},supersedes_id:None})?;
    }
    Ok(())
}

pub fn reference_available(conn: &Connection, r: &Reference) -> Result<bool> {
    Ok(match r {
        Reference::Message{provider,session_id,message_key}=>conn.query_row("SELECT EXISTS(SELECT 1 FROM fact_session_message WHERE provider=?1 AND session_id=?2 AND COALESCE(message_key,'index:' || message_index)=?3)",params![provider,session_id,message_key],|r|r.get(0))?,
        Reference::Tool{provider,session_id,call_key}=>conn.query_row("SELECT EXISTS(SELECT 1 FROM fact_session_tool_call WHERE provider=?1 AND session_id=?2 AND call_key=?3)",params![provider,session_id,call_key],|r|r.get(0))?,
        Reference::Task{member_id}=>conn.query_row("SELECT EXISTS(SELECT 1 FROM derived_work_member m JOIN work_identity i ON i.id=m.identity_id WHERE m.identity_id=?1 AND i.review_reason IS NULL)",[member_id],|r|r.get(0))?,
        Reference::PullRequest{id,fingerprint}=>if let Some(fp)=fingerprint{conn.query_row("SELECT EXISTS(SELECT 1 FROM pull_request_revision WHERE pr_id=?1 AND fingerprint=?2)",params![id,fp],|r|r.get(0))?}else{conn.query_row("SELECT EXISTS(SELECT 1 FROM pull_request_record WHERE id=?1)",[id],|r|r.get(0))?},
        Reference::Deployment{id}=>conn.query_row("SELECT EXISTS(SELECT 1 FROM deployment_record WHERE id=?1)",[id],|r|r.get(0))?,
        Reference::Classification{id}=>conn.query_row("SELECT EXISTS(SELECT 1 FROM classification_result WHERE id=?1)",[id],|r|r.get(0))?,
    })
}
