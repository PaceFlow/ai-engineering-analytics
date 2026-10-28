use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::{Connection, params};
use vca::{
    db::{
        self,
        session_store::{self, MessageRecord, SessionRecord, UsageRecord},
    },
    insights::{
        self, daily,
        delivery::{self, Deployment, PullRequest},
        observation::{self, SourceCoverage},
        trends, work,
    },
};

fn db() -> Result<Connection> {
    let c = Connection::open_in_memory()?;
    db::init_app_schema(&c)?;
    Ok(c)
}
fn session(c: &Connection, id: &str, repo: &str, start: &str, end: &str) -> Result<String> {
    let mut s = SessionRecord::new("codex", id);
    s.started_at = Some(start.into());
    s.ended_at = Some(end.into());
    s.messages = vec![
        MessageRecord {
            key: format!("{id}-request"),
            ts: s.started_at.clone(),
            role: "user".into(),
            text: format!("Implement requested behavior {id}"),
            is_compaction_summary: false,
        },
        MessageRecord {
            key: format!("{id}-answer"),
            ts: s.ended_at.clone(),
            role: "assistant".into(),
            text: "Implementation ready".into(),
            is_compaction_summary: false,
        },
    ];
    let mut u = UsageRecord::new("usage");
    u.ts = s.ended_at.clone();
    u.model = Some("model-a".into());
    u.input_tokens = 100;
    u.cache_read_tokens = 200;
    u.output_tokens = 20;
    u.reasoning_tokens = 5;
    u.reasoning_in_output = false;
    s.usage = vec![u];
    session_store::store_session_record(c, &s)?;
    let rid = db::upsert_metadata_repository(c, repo)?;
    c.execute(
        "UPDATE metadata_sessions SET repository_id=?1 WHERE session_id=?2",
        params![rid, id],
    )?;
    session_store::refresh_session_roots(c)?;
    insights::refresh(c)?;
    work::resolve_member(c, &format!("codex:{id}#0"))
}
fn finish(c: &Connection, id: &str) -> Result<()> {
    c.execute("INSERT OR REPLACE INTO classification_override(unit_kind,unit_key,question_id,label) VALUES('task',?1,'intent','bug_fix')",[format!("codex|{id}|0")])?;
    c.execute("INSERT OR REPLACE INTO classification_override(unit_kind,unit_key,question_id,label) VALUES('task',?1,'outcome','completed_verified')",[format!("codex|{id}|0")])?;
    c.execute(
        "INSERT OR REPLACE INTO task_completion_mark VALUES('codex',?1,0,0,'override',1.0)",
        [id],
    )?;
    insights::refresh(c)
}
fn pr(number: i64) -> PullRequest {
    PullRequest {
        provider: "github".into(),
        repo_key: "git:github.com/org/repo".into(),
        number,
        url: format!("https://github.com/org/repo/pull/{number}"),
        state: "merged".into(),
        created_at: Some("2026-10-01T10:05:00Z".into()),
        merged_at: Some("2026-10-01T11:00:00Z".into()),
        merge_sha: Some("squashed".into()),
        ..Default::default()
    }
}
fn deployment(id: &str, sha: &str, at: &str) -> Deployment {
    Deployment {
        id: id.into(),
        provider: "github".into(),
        repo_key: "git:github.com/org/repo".into(),
        artifact_sha: sha.into(),
        environment: "production".into(),
        live: true,
        status: "success".into(),
        occurred_at: at.into(),
        evidence_url: format!("https://example.test/deploy/{id}"),
        ..Default::default()
    }
}
fn opts(from: &str, to: &str) -> daily::Options {
    daily::Options {
        from: Some(from.into()),
        to: Some(to.into()),
        timezone: Some("UTC".into()),
        ..Default::default()
    }
}
fn at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 12, 12, 0, 0).unwrap()
}
fn coverage(c: &Connection, from: &str) -> Result<()> {
    observation::record(
        c,
        &SourceCoverage {
            source_id: "releases".into(),
            provider: "github".into(),
            repo_root: Some("git:github.com/org/repo".into()),
            observed_from: Some(from.into()),
            observed_through: Some("2026-10-12T12:00:00Z".into()),
            ingested_at: "2026-10-12T12:00:00Z".into(),
            complete: true,
            kind: "pull_requests_and_deployments".into(),
            error: None,
        },
    )
}

#[test]
fn delivery_supplements_completion_and_preserves_manual_links_and_unknowns() -> Result<()> {
    let c = db()?;
    observation::set_alias(&c, "/repo", "git:github.com/org/repo", "manual")?;
    let member = session(
        &c,
        "feature",
        "/repo",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:30:00Z",
    )?;
    finish(&c, "feature")?;
    let before = work::items(&c)?;
    let pid = delivery::save_pr(&c, &pr(1))?;
    delivery::link(
        &c,
        &member,
        &pid,
        "implementation",
        "accepted",
        "manual",
        "explicit artifact",
    )?;
    let mut green = deployment("ci", "squashed", "2026-10-01T12:00:00Z");
    green.live = false;
    delivery::save_deployment(&c, &green)?;
    assert!(delivery::evidence(&c)?[0].deployed_at.is_none());
    let mut failed = deployment("failed", "squashed", "2026-10-01T12:05:00Z");
    failed.status = "failed".into();
    delivery::save_deployment(&c, &failed)?;
    let mut a = deployment("web", "web-sha", "2026-10-02T12:00:00Z");
    a.component = "web".into();
    a.required_components = vec!["web".into(), "worker".into()];
    a.release_id = Some("release-1".into());
    a.included_commits = vec!["squashed".into()];
    delivery::save_deployment(&c, &a)?;
    assert!(delivery::evidence(&c)?[0].deployed_at.is_none());
    let mut b = a.clone();
    b.id = "worker".into();
    b.artifact_sha = "worker-sha".into();
    b.component = "worker".into();
    b.occurred_at = "2026-10-02T12:10:00Z".into();
    delivery::save_deployment(&c, &b)?;
    let e = delivery::evidence(&c)?;
    assert_eq!(e[0].deployed_at.as_deref(), Some("2026-10-02T12:10:00Z"));
    let mut rollback = deployment("rollback", "prior-release", "2026-10-03T12:00:00Z");
    rollback.status = "rollback".into();
    rollback.rollback_of = Some("web".into());
    delivery::save_deployment(&c, &rollback)?;
    assert!(delivery::evidence(&c)?[0].rolled_back);
    delivery::link(
        &c,
        &member,
        &pid,
        "review",
        "suggested",
        "similarity",
        "same branch",
    )?;
    insights::refresh(&c)?;
    let after = work::items(&c)?;
    assert_eq!(before[0].completed_at, after[0].completed_at);
    assert_eq!(before[0].native_tokens(), after[0].native_tokens());
    assert_eq!(after[0].pull_requests[0].roles, vec!["implementation"]);
    let mut o = opts("2026-10-01", "2026-10-03");
    o.scope.repo = Some("/repo".into());
    let r = daily::report_at(&c, &o, at())?;
    assert_eq!(r.days[0].completed, 1);
    assert_eq!(r.days[0].pull_requests.deployed, 0);
    assert_eq!(r.days[1].pull_requests.deployed, 1);
    assert_eq!(r.days[2].pull_requests.rollback_events, 1);
    assert_eq!(
        r.days[1].pull_requests.engineering_delivery_minutes.median,
        Some(1570.0)
    );
    assert_eq!(r.days[0].native_tokens.total, 325);
    assert_eq!(r.days[0].native_tokens.reasoning_extra, 5);
    Ok(())
}
#[test]
fn unmatched_prs_shared_effort_and_cohort_history_are_explicit() -> Result<()> {
    let c = db()?;
    let member = session(
        &c,
        "shared",
        "/repo",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:30:00Z",
    )?;
    finish(&c, "shared")?;
    for number in 1..=3 {
        let id = delivery::save_pr(&c, &pr(number))?;
        if number != 3 {
            delivery::link(
                &c,
                &member,
                &id,
                "implementation",
                "accepted",
                "manual",
                "split release",
            )?;
        }
    }
    let mut d = deployment("failure", "squashed", "2026-10-01T12:00:00Z");
    d.status = "failed".into();
    delivery::save_deployment(&c, &d)?;
    delivery::save_deployment(
        &c,
        &deployment("success", "squashed", "2026-10-02T12:00:00Z"),
    )?;
    coverage(&c, "2026-10-02T00:00:00Z")?;
    let o = opts("2026-10-01", "2026-10-02");
    let r = daily::report_at(&c, &o, at())?;
    assert_eq!(r.days[0].pull_requests.failed_deployment_events, 1);
    assert_eq!(r.days[0].coverage.prs_without_work, 1);
    assert_eq!(r.days[1].pull_requests.deployed, 3);
    assert_eq!(r.days[1].pull_requests.tokens_per_delivered_pr.samples, 0);
    assert_eq!(r.days[1].pull_requests.tokens_per_delivered_pr.missing, 3);
    assert_eq!(r.cohorts[0].pending_followup, 3);
    coverage(&c, "2026-09-01T00:00:00Z")?;
    let r = daily::report_at(&c, &o, at())?;
    assert_eq!(r.cohorts[0].mature, 3);
    assert_eq!(r.cohorts[0].deployed_within_window, 3);
    let mut scoped = o.clone();
    scoped.scope.repo = Some("/different".into());
    assert_eq!(
        daily::contributors(&c, &scoped, "2026-10-02", "deployed", 0, 50)?["total"],
        0
    );
    Ok(())
}
#[test]
fn revision_and_import_are_idempotent_and_atomic() -> Result<()> {
    let c = db()?;
    let mut p = pr(1);
    delivery::save_pr(&c, &p)?;
    p.observed_at = "2026-10-28T12:00:00Z".into();
    delivery::save_pr(&c, &p)?;
    assert_eq!(
        c.query_row("SELECT count(*) FROM pull_request_revision", [], |r| r
            .get::<_, i64>(0))?,
        1
    );
    let mut invalid = deployment("bad", "", "2026-10-03T12:00:00Z");
    invalid.evidence_url.clear();
    let input = delivery::Import {
        schema_version: 1,
        records: vec![
            delivery::ImportRecord::PullRequest(pr(2)),
            delivery::ImportRecord::Deployment(invalid),
        ],
    };
    assert!(delivery::import(&c, &serde_json::to_vec(&input)?).is_err());
    assert_eq!(delivery::prs(&c)?.len(), 1);
    Ok(())
}
#[test]
fn daily_dst_and_event_allocation_match_trends_and_paginated_evidence() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "cross",
        "/repo",
        "2026-10-24T21:50:00Z",
        "2026-10-24T22:10:00Z",
    )?;
    finish(&c, "cross")?;
    let mut o = opts("2026-10-24", "2026-10-25");
    o.timezone = Some("Europe/Berlin".into());
    let r = daily::report_at(&c, &o, at() + chrono::Duration::days(20))?;
    assert_eq!(r.days[0].native_tokens.total, 0);
    assert_eq!(r.days[1].native_tokens.total, 325);
    assert_eq!(r.days[0].work_activity_minutes, 10.0);
    assert_eq!(r.days[1].work_activity_minutes, 10.0);
    assert_eq!(r.days[1].end_ms - r.days[1].start_ms, 25 * 3600 * 1000);
    let t = trends::report_at(
        &c,
        &trends::Options {
            from: o.from.clone(),
            to: o.to.clone(),
            timezone: o.timezone.clone(),
            daily: true,
            ..Default::default()
        },
        at() + chrono::Duration::days(20),
    )?;
    assert_eq!(
        t.periods[1].native_tokens.total,
        r.days[1].native_tokens.total
    );
    let ids = daily::contributors(&c, &o, "2026-10-25", "completed", 0, 1)?;
    assert_eq!(ids["total"], 1);
    assert_eq!(ids["ids"].as_array().unwrap().len(), 1);
    assert!(ids["next_offset"].is_null());
    assert!(daily::contributors(&c, &o, "2026-10-25", "completed", 0, 501).is_err());
    Ok(())
}

#[test]
fn in_progress_reclassification_cannot_publish_half_a_daily_revision() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "stable",
        "/repo",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:30:00Z",
    )?;
    finish(&c, "stable")?;
    let o = opts("2026-10-01", "2026-10-01");
    let before = daily::report_at(&c, &o, at())?;
    c.execute("DELETE FROM derived_usage_event", [])?;
    c.execute("DELETE FROM derived_work_activity", [])?;
    c.execute("UPDATE derived_task SET models='new-model'", [])?;
    let during = daily::report_at(&c, &o, at())?;
    assert_eq!(before.days, during.days);
    assert_eq!(before.published_revision, during.published_revision);
    Ok(())
}

#[test]
fn full_task_reference_does_not_confuse_pr_one_with_pr_ten() -> Result<()> {
    let c = db()?;
    let member = session(
        &c,
        "review",
        "/repo",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:30:00Z",
    )?;
    c.execute(
        "UPDATE fact_session_message SET content=?1 WHERE role='user'",
        [format!(
            "Review {} https://github.com/org/repo/pull/10",
            "context ".repeat(100)
        )],
    )?;
    delivery::save_pr(&c, &pr(1))?;
    delivery::save_pr(&c, &pr(10))?;
    insights::refresh(&c)?;
    let evidence = delivery::evidence(&c)?;
    let one = evidence.iter().find(|e| e.pr.number == 1).unwrap();
    let ten = evidence.iter().find(|e| e.pr.number == 10).unwrap();
    assert!(one.work_ids.is_empty());
    assert_eq!(ten.member_ids, vec![member]);
    assert_eq!(ten.roles, vec!["review"]);
    assert_eq!(ten.contributions[0].source, "explicit_reference");
    Ok(())
}

#[test]
fn missing_suspect_and_explicit_zero_do_not_share_an_efficiency_denominator() -> Result<()> {
    let c = db()?;
    for (id, available) in [
        ("missing", Some(false)),
        ("suspect", None),
        ("zero", Some(true)),
    ] {
        let mut s = SessionRecord::new("codex", id);
        s.started_at = Some("2026-10-01T12:00:00Z".into());
        s.ended_at = Some("2026-10-01T12:10:00Z".into());
        s.messages = vec![
            MessageRecord {
                key: format!("{id}-request"),
                ts: s.started_at.clone(),
                role: "user".into(),
                text: format!("Explain {id}"),
                is_compaction_summary: false,
            },
            MessageRecord {
                key: format!("{id}-answer"),
                ts: s.ended_at.clone(),
                role: "assistant".into(),
                text: "Here is the answer".into(),
                is_compaction_summary: false,
            },
        ];
        let mut u = UsageRecord::new("usage");
        u.ts = s.ended_at.clone();
        u.usage_available = available;
        s.usage = vec![u];
        session_store::store_session_record(&c, &s)?;
    }
    session_store::refresh_session_roots(&c)?;
    insights::refresh(&c)?;
    for id in ["missing", "suspect", "zero"] {
        finish(&c, id)?;
    }
    let r = daily::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    let d = &r.days[0];
    assert_eq!(d.completed, 3);
    assert_eq!(d.bug_work_completed, 3);
    assert_eq!(d.coverage.explicit_zero_events, 1);
    assert_eq!(d.coverage.suspect_zero_events, 1);
    assert_eq!(d.coverage.missing_events, 1);
    assert_eq!(d.tokens_per_completed.samples, 1);
    assert_eq!(d.tokens_per_completed.median, Some(0.0));
    assert_eq!(d.tokens_per_completed.missing, 2);
    Ok(())
}

#[test]
fn stale_source_marks_trends_partial_without_discarding_later_retained_usage() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "retained",
        "/repo",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:30:00Z",
    )?;
    observation::record(
        &c,
        &SourceCoverage {
            source_id: "snapshot".into(),
            provider: "codex".into(),
            repo_root: None,
            observed_from: None,
            observed_through: Some("2026-09-30T12:00:00Z".into()),
            ingested_at: "2026-10-12T12:00:00Z".into(),
            complete: false,
            kind: "snapshot".into(),
            error: None,
        },
    )?;
    let r = trends::report_at(
        &c,
        &trends::Options {
            from: Some("2026-10-01".into()),
            to: Some("2026-10-01".into()),
            timezone: Some("UTC".into()),
            daily: true,
            ..Default::default()
        },
        at(),
    )?;
    assert_eq!(r.periods[0].native_tokens.total, 325);
    assert!(r.periods[0].provisional);
    assert!(r.periods[0].coverage.source_incomplete);
    Ok(())
}

#[test]
fn release_components_order_by_instant_across_different_utc_offsets() -> Result<()> {
    let c = db()?;
    delivery::save_pr(&c, &pr(1))?;
    for (id, at) in [
        ("web", "2026-10-02T10:00:00-02:00"),
        ("worker", "2026-10-02T13:00:00+02:00"),
    ] {
        let mut d = deployment(id, "squashed", at);
        d.component = id.into();
        d.required_components = vec!["web".into(), "worker".into()];
        delivery::save_deployment(&c, &d)?;
    }
    let rows = delivery::evidence(&c)?;
    assert_eq!(
        rows[0]
            .deployed_at
            .as_deref()
            .and_then(insights::structure::parse_ts),
        insights::structure::parse_ts("2026-10-02T12:00:00Z")
    );
    Ok(())
}
