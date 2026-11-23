use anyhow::Result;
use rusqlite::Connection;
use vca::{
    db::{
        self,
        session_store::{self, MessageRecord, SessionRecord, UsageRecord},
    },
    insights::{
        self,
        audit::{self, Event, Payload},
        daily, delivery, investigation, work,
    },
};
fn setup(text: &str) -> Result<(Connection, String)> {
    let c = Connection::open_in_memory()?;
    db::init_app_schema(&c)?;
    let mut s = SessionRecord::new("opencode", "s");
    s.started_at = Some("2026-10-01T10:00:00Z".into());
    s.ended_at = Some("2026-10-01T10:10:00Z".into());
    s.messages=vec![MessageRecord{key:"m1".into(),ts:s.started_at.clone(),role:"user".into(),text:text.into(),is_compaction_summary:false},MessageRecord{key:"m2".into(),ts:s.ended_at.clone(),role:"assistant".into(),text:"Partial progress; awaiting acceptance. sk-or-v1-this-is-a-test-secret-not-a-real-key".into(),is_compaction_summary:false}];
    let mut u = UsageRecord::new("u");
    u.ts = s.ended_at.clone();
    u.input_tokens = 10;
    u.cache_read_tokens = 100;
    u.output_tokens = 2;
    u.model = Some("model".into());
    s.usage = vec![u];
    session_store::store_session_record(&c, &s)?;
    session_store::refresh_session_roots(&c)?;
    insights::refresh(&c)?;
    let id = work::resolve_member(&c, "opencode:s#0")?;
    Ok((c, id))
}
fn event(member: &str, id: &str, at: &str, payload: Payload) -> Event {
    Event {
        id: id.into(),
        subject_kind: "task".into(),
        subject_key: member.into(),
        occurred_at: Some(at.into()),
        observed_at: "2026-10-02T00:00:00Z".into(),
        source: "import".into(),
        source_key: id.into(),
        schema_version: 1,
        payload: match payload {
            Payload::CompletionSupported { verified, evidence } if evidence.is_empty() => {
                Payload::CompletionSupported {
                    verified,
                    evidence: vec![audit::Reference::Message {
                        provider: "opencode".into(),
                        session_id: "s".into(),
                        message_key: "m2".into(),
                    }],
                }
            }
            payload => payload,
        },
        supersedes_id: None,
    }
}
#[test]
fn fp163_wrapper_is_operations_and_corrective_not_docs() -> Result<()> {
    let (c, _) = setup(
        "You are running as a local coding agent for a Multica workspace.\n[NEW COMMENT] Another agent (Operations Lead) just left a new comment.\n> Authorized maintenance: tune cacheSizeGB and run bounded retention batches.\nReply as a comment.",
    )?;
    let w = work::items(&c)?.remove(0);
    assert_eq!(w.intent, "ops_config");
    assert_eq!(
        investigation::purpose(&c, "opencode", "s", 0)?.0,
        "corrective"
    );
    let report = daily::report(
        &c,
        &daily::Options {
            from: Some("2026-10-01".into()),
            to: Some("2026-10-01".into()),
            timezone: Some("UTC".into()),
            ..Default::default()
        },
    )?;
    assert_eq!(report.days[0].corrective.corrective_goal_work, 1);
    let before = report.days[0].native_tokens.total;
    assert_eq!(
        report.days[0]
            .corrective
            .native_tokens_by_activity_purpose
            .values()
            .sum::<i64>(),
        before
    );
    Ok(())
}
#[test]
fn klik35_withdrawal_and_missing_acceptance_are_consistent() -> Result<()> {
    let (c, id) = setup("Fix the authentication defect")?;
    let done = event(
        &id,
        "done",
        "2026-10-01T10:10:00Z",
        Payload::CompletionSupported {
            verified: true,
            evidence: vec![],
        },
    );
    audit::save(&c, &done)?;
    assert!(work::items(&c)?[0].completed_at.is_some());
    audit::save(
        &c,
        &event(
            &id,
            "acceptance",
            "2026-10-01T10:11:00Z",
            Payload::AcceptanceRequired {
                criterion_id: "signed-in-flow".into(),
                evidence: vec![],
            },
        ),
    )?;
    audit::save(
        &c,
        &event(
            &id,
            "withdraw",
            "2026-10-01T10:12:00Z",
            Payload::CompletionWithdrawn {
                reason: "Authenticated acceptance missing".into(),
                evidence: vec![],
            },
        ),
    )?;
    insights::observation::publish(&c)?;
    let w = work::items(&c)?.remove(0);
    assert_eq!(w.state, "open");
    assert!(w.completed_at.is_none());
    let tasks = vca::explore::tasks(&c, &Default::default(), vca::explore::TaskSort::Started, 10)?;
    assert!(tasks[0].supported_completed_at.is_none());
    let report = investigation::explain(&c, &w, false, 10, None)?;
    assert!(report.statements.iter().any(|s| s.contains("withdrawn")));
    assert_eq!(report.lifecycle.len(), 3);
    // Passing acceptance alone does not establish a subsequent goal completion.
    audit::save(
        &c,
        &event(
            &id,
            "accepted",
            "2026-10-01T10:13:00Z",
            Payload::AcceptancePassed {
                criterion_id: "signed-in-flow".into(),
                evidence: vec![],
            },
        ),
    )?;
    assert!(work::items(&c)?[0].completed_at.is_none());
    audit::save(
        &c,
        &event(
            &id,
            "redone",
            "2026-10-01T10:14:00Z",
            Payload::CompletionSupported {
                verified: true,
                evidence: vec![],
            },
        ),
    )?;
    assert_eq!(work::items(&c)?[0].state, "completed");
    Ok(())
}
#[test]
fn imports_are_atomic_idempotent_and_preserve_supersession() -> Result<()> {
    let (c, id) = setup("Investigate")?;
    let e = event(
        &id,
        "e",
        "2026-10-01T10:11:00Z",
        Payload::Blocked {
            block_id: "access".into(),
            reason: "No test identity".into(),
            evidence: vec![],
        },
    );
    let bytes = serde_json::to_vec(
        &serde_json::json!({"schema_version":2,"records":[{"kind":"work_event","record":e}]}),
    )?;
    delivery::import(&c, &bytes)?;
    delivery::import(&c, &bytes)?;
    assert_eq!(audit::events(&c, "task", &id)?.len(), 1);
    let mut conflicting = e.clone();
    conflicting.payload = Payload::Unblocked {
        block_id: "access".into(),
        evidence: vec![],
    };
    assert!(audit::save(&c, &conflicting).is_err());
    conflicting.id = "revision".into();
    conflicting.source_key = "revision".into();
    conflicting.supersedes_id = Some("e".into());
    audit::save(&c, &conflicting)?;
    assert_eq!(audit::events(&c, "task", &id)?.len(), 2);
    let mut bad = e.clone();
    bad.id = "bad".into();
    bad.source_key = "bad".into();
    bad.subject_kind = "random".into();
    let bytes = serde_json::to_vec(
        &serde_json::json!({"schema_version":2,"records":[{"kind":"work_event","record":event(&id,"rollback","2026-10-01T10:20:00Z",Payload::Reopened{reason:"x".into(),evidence:vec![]})},{"kind":"work_event","record":bad}]}),
    )?;
    assert!(delivery::import(&c, &bytes).is_err());
    assert!(
        !audit::events(&c, "task", &id)?
            .iter()
            .any(|e| e.id == "rollback")
    );
    Ok(())
}
#[test]
fn evidence_is_redacted_paginated_and_cursor_detects_changes() -> Result<()> {
    let (c, _) = setup("Fix behavior")?;
    let w = work::items(&c)?.remove(0);
    let first = investigation::explain(&c, &w, true, 1, None)?;
    let cursor = first.next_cursor.as_deref().unwrap();
    let second = investigation::explain(&c, &w, true, 1, Some(cursor))?;
    assert_eq!(second.evidence.len(), 1);
    assert!(!second.evidence[0].excerpt.contains("sk-or-v1"));
    assert!(second.next_cursor.is_none());
    c.execute(
        "UPDATE fact_session_message SET content='changed' WHERE message_key='m2'",
        [],
    )?;
    assert!(investigation::explain(&c, &w, true, 1, Some(cursor)).is_err());
    Ok(())
}
#[test]
fn explicit_pairs_only_explain_waiting() -> Result<()> {
    let (c, id) = setup("Plan")?;
    audit::save(
        &c,
        &event(
            &id,
            "ask",
            "2026-10-01T10:00:00Z",
            Payload::ApprovalRequested {
                approval_id: "merge".into(),
                evidence: vec![],
            },
        ),
    )?;
    audit::save(
        &c,
        &event(
            &id,
            "approve",
            "2026-10-01T11:00:00Z",
            Payload::ApprovalGranted {
                approval_id: "merge".into(),
                evidence: vec![],
            },
        ),
    )?;
    let w = work::items(&c)?.remove(0);
    let e = investigation::explain(&c, &w, false, 20, None)?;
    assert_eq!(e.intervals[0].minutes, Some(60.));
    Ok(())
}
#[test]
fn attribution_cannot_be_confirmed_by_classifier() -> Result<()> {
    let (c, id) = setup("Fix")?;
    let mut e = event(
        &id,
        "correction",
        "2026-10-01T10:05:00Z",
        Payload::CorrectionDecision {
            origin_member_id: None,
            origin_turn: None,
            correction_turn: None,
            correction_type: "defect_correction".into(),
            attribution: "confirmed".into(),
            reason: "same file".into(),
            evidence: vec![],
        },
    );
    e.source = "classifier".into();
    assert!(audit::save(&c, &e).is_err());
    if let Payload::CorrectionDecision { attribution, .. } = &mut e.payload {
        *attribution = "suspected".into();
    }
    audit::save(&c, &e)?;
    assert_eq!(work::items(&c)?.len(), 1);
    Ok(())
}
#[test]
fn schema_upgrade_adds_only_one_table_and_two_provenance_columns() -> Result<()> {
    let c = Connection::open_in_memory()?;
    db::init_app_schema(&c)?;
    c.execute_batch("DROP TABLE audit_work_event; ALTER TABLE classifier_request DROP COLUMN input_manifest_json; ALTER TABLE classification_result DROP COLUMN evidence_json; PRAGMA user_version=4;")?;
    let before: i64 = c.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
        [],
        |r| r.get(0),
    )?;
    db::init_app_schema(&c)?;
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        before + 1
    );
    assert!(
        c.prepare("SELECT input_manifest_json FROM classifier_request")?
            .column_count()
            == 1
    );
    assert!(
        c.prepare("SELECT evidence_json FROM classification_result")?
            .column_count()
            == 1
    );
    Ok(())
}

#[test]
fn hotspot_redaction_precedes_truncation() -> Result<()> {
    let (c, _) = setup("Review CI checks")?;
    let secret = format!("sk-or-v1-{}", "a".repeat(400));
    let command = format!("gh pr checks --note {secret}");
    for index in 0..2 {
        c.execute("INSERT INTO fact_session_tool_call(provider,session_id,call_key,call_index,tool_name,tool_category,target) VALUES('opencode','s',?1,?2,'bash','exec',?3)",rusqlite::params![format!("call-{index}"),index,command])?;
    }
    let w = work::items(&c)?.remove(0);
    let report = investigation::explain(&c, &w, false, 10, None)?;
    assert_eq!(report.hotspots.len(), 1);
    assert!(!report.hotspots[0].target.contains(&"a".repeat(20)));
    Ok(())
}
