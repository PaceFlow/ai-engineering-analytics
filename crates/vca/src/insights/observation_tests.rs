use super::{setup, trends, work};
use crate::db::{
    init_app_schema,
    session_store::{
        AgentKind, MessageRecord, SessionRecord, TokenSource, UsageRecord, refresh_session_roots,
        store_session_record,
    },
};
use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::{Connection, params};
fn db() -> Result<Connection> {
    let c = Connection::open_in_memory()?;
    init_app_schema(&c)?;
    c.execute(
        "INSERT INTO analytics_setting VALUES('observed_provider:codex','2026-10-27T12:00:00Z')",
        [],
    )?;
    c.execute(
        "INSERT INTO metadata_repositories(id,repo_root,repo_name) VALUES(1,'/project','project')",
        [],
    )?;
    Ok(c)
}
fn session(c: &Connection, id: &str, start: &str, end: &str, tokens: i64) -> Result<()> {
    let mut record = SessionRecord::new("codex", id);
    record.started_at = Some(start.into());
    record.ended_at = Some(end.into());
    record.git_branch = Some("topic".into());
    record.messages = vec![
        MessageRecord {
            key: format!("{id}-request"),
            ts: Some(start.into()),
            role: "user".into(),
            text: format!("Implement {id}"),
            is_compaction_summary: false,
        },
        MessageRecord {
            key: format!("{id}-answer"),
            ts: Some(end.into()),
            role: "assistant".into(),
            text: "The requested behavior is implemented".into(),
            is_compaction_summary: false,
        },
    ];
    let mut usage = UsageRecord::new(format!("{id}-usage"));
    usage.ts = Some(end.into());
    usage.model = Some("gpt-5".into());
    usage.input_tokens = tokens;
    record.usage = vec![usage];
    store_session_record(c, &record)?;
    c.execute(
        "UPDATE metadata_sessions SET repository_id=1 WHERE session_id=?1",
        [id],
    )?;
    Ok(())
}
fn refresh(c: &Connection) -> Result<()> {
    refresh_session_roots(c)?;
    super::refresh(c)
}
fn finish(c: &Connection, id: &str, intent: &str) -> Result<()> {
    c.execute("INSERT OR REPLACE INTO classification_override(unit_kind,unit_key,question_id,label) VALUES('task',?1,'intent',?2)",params![format!("codex|{id}|0"),intent])?;
    c.execute("INSERT OR REPLACE INTO classification_override(unit_kind,unit_key,question_id,label) VALUES('task',?1,'outcome','completed_verified')",[format!("codex|{id}|0")])?;
    c.execute(
        "INSERT OR REPLACE INTO task_completion_mark VALUES('codex',?1,0,0,'override',1.0)",
        [id],
    )?;
    refresh(c)
}
fn at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 27, 12, 0, 0).unwrap()
}
fn opts(from: &str, to: &str) -> trends::Options {
    trends::Options {
        from: Some(from.into()),
        to: Some(to.into()),
        timezone: Some("UTC".into()),
        daily: true,
        ..Default::default()
    }
}
#[test]
fn continued_work_unions_overlaps_and_counts_subagent_usage_once() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "first",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:20:00Z",
        100,
    )?;
    session(
        &c,
        "second",
        "2026-10-01T10:10:00Z",
        "2026-10-01T10:30:00Z",
        200,
    )?;
    session(
        &c,
        "third",
        "2026-10-01T10:25:00Z",
        "2026-10-01T10:40:00Z",
        300,
    )?;
    let mut child = SessionRecord::new("codex", "child");
    child.parent_session_id = Some("first".into());
    child.agent_kind = AgentKind::Subagent;
    child.messages = vec![MessageRecord {
        key: "child-request".into(),
        ts: Some("2026-10-01T10:01:00Z".into()),
        role: "user".into(),
        text: "Investigate".into(),
        is_compaction_summary: false,
    }];
    let mut u = UsageRecord::new("child-u");
    u.ts = Some("2026-10-01T10:02:00Z".into());
    u.input_tokens = 50;
    child.usage.push(u);
    store_session_record(&c, &child)?;
    refresh(&c)?;
    let a = work::resolve_member(&c, "codex:first#0")?;
    let b = work::resolve_member(&c, "codex:second#0")?;
    let d = work::resolve_member(&c, "codex:third#0")?;
    work::insert_relation(
        &c,
        &a,
        &b,
        "continuation",
        "accepted",
        "manual",
        "same goal",
        Some(1.0),
    )?;
    work::insert_relation(
        &c,
        &b,
        &d,
        "continuation",
        "accepted",
        "manual",
        "same goal",
        Some(1.0),
    )?;
    finish(&c, "third", "feature")?;
    let rows = work::items(&c)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].conversations, 3);
    assert_eq!(rows[0].native_tokens(), Some(650));
    assert_eq!(rows[0].cycle_minutes(), Some(40.0));
    assert_eq!(rows[0].active_minutes(), 40.0);
    refresh(&c)?;
    assert_eq!(work::items(&c)?[0].id, rows[0].id);
    Ok(())
}
#[test]
fn task_usage_and_activity_are_split_at_midnight_and_week_boundary() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "week",
        "2026-10-04T23:50:00Z",
        "2026-10-05T00:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "week", "feature")?;
    let r = trends::report_at(&c, &opts("2026-10-04", "2026-10-05"), at())?;
    assert_eq!(r.periods[0].native_tokens.total, 0);
    assert_eq!(r.periods[1].native_tokens.total, 100);
    assert_eq!(r.periods[0].work_activity_minutes, 10.0);
    assert_eq!(r.periods[1].work_activity_minutes, 10.0);
    assert_eq!(r.periods[0].started, 1);
    assert_eq!(r.periods[1].completed, 1);
    Ok(())
}
#[test]
fn berlin_dst_days_have_real_calendar_boundaries() -> Result<()> {
    let tz: chrono_tz::Tz = "Europe/Berlin".parse()?;
    let d = chrono::NaiveDate::from_ymd_opt(2026, 10, 25).unwrap();
    assert_eq!(
        trends::midnight(tz, d + chrono::Duration::days(1))? - trends::midnight(tz, d)?,
        25 * 3_600_000
    );
    let d = chrono::NaiveDate::from_ymd_opt(2026, 3, 29).unwrap();
    assert_eq!(
        trends::midnight(tz, d + chrono::Duration::days(1))? - trends::midnight(tz, d)?,
        23 * 3_600_000
    );
    Ok(())
}
#[test]
fn lower_bug_share_exposes_unchanged_absolute_bug_work() -> Result<()> {
    let c = db()?;
    for (id, date, tokens, intent) in [
        ("bug-before", "2026-10-01", 100, "bug_fix"),
        ("feature-before", "2026-10-01", 100, "feature"),
        ("bug-after", "2026-10-08", 100, "bug_fix"),
        ("feature-after", "2026-10-08", 900, "feature"),
    ] {
        session(
            &c,
            id,
            &format!("{date}T10:00:00Z"),
            &format!("{date}T10:10:00Z"),
            tokens,
        )?;
        refresh(&c)?;
        finish(&c, id, intent)?;
    }
    let mut o = opts("2026-10-08", "2026-10-08");
    o.baseline_from = Some("2026-10-01".into());
    o.baseline_to = Some("2026-10-01".into());
    let r = trends::report_at(&c, &o, at())?;
    let comparison = r.comparison.unwrap();
    let before = comparison
        .before
        .goal_mix
        .iter()
        .find(|m| m.label == "bug_fix")
        .unwrap();
    let after = comparison
        .after
        .goal_mix
        .iter()
        .find(|m| m.label == "bug_fix")
        .unwrap();
    assert_eq!(before.native_tokens, after.native_tokens);
    assert_eq!(before.token_share, Some(0.5));
    assert_eq!(after.token_share, Some(0.1));
    assert_eq!(before.work_items, after.work_items);
    Ok(())
}
#[test]
fn debugging_inside_feature_and_unknown_labels_remain_visible() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "feature",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "feature", "feature")?;
    crate::classify::overrides::set_label(
        &c,
        "turn",
        "codex|feature|0",
        "turn_stage",
        Some("debug_fix"),
        None,
    )?;
    let r = trends::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    let p = &r.periods[0];
    assert_eq!(p.goal_mix[0].label, "feature");
    let stage = p
        .activity_mix
        .iter()
        .find(|m| m.label == "debug_fix")
        .unwrap();
    assert_eq!(stage.native_tokens, 100);
    assert_eq!(stage.activity_minutes, 10.0);
    Ok(())
}
#[test]
fn missing_and_estimated_usage_do_not_become_native_zero() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "missing",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        0,
    )?;
    session(
        &c,
        "estimate",
        "2026-10-01T11:00:00Z",
        "2026-10-01T11:10:00Z",
        250,
    )?;
    c.execute(
        "UPDATE fact_session_usage SET token_source=?1 WHERE session_id='estimate'",
        [TokenSource::EstimatedText.as_str()],
    )?;
    refresh(&c)?;
    finish(&c, "missing", "feature")?;
    finish(&c, "estimate", "feature")?;
    let r = trends::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    let p = &r.periods[0];
    assert_eq!(p.tokens_per_completed.samples, 0);
    assert_eq!(p.tokens_per_completed.missing, 2);
    assert_eq!(p.estimated_tokens.total, 250);
    assert_eq!(p.coverage.missing_usage, 1);
    let mut o = opts("2026-10-01", "2026-10-01");
    o.scope.include_estimated = true;
    let r = trends::report_at(&c, &o, at())?;
    assert_eq!(r.periods[0].tokens_per_completed.median, Some(250.0));
    Ok(())
}
#[test]
fn premature_completion_is_reopened_and_correction_goal_stays_separate() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "first",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "first", "feature")?;
    session(
        &c,
        "reopened",
        "2026-10-02T10:00:00Z",
        "2026-10-02T10:20:00Z",
        200,
    )?;
    session(
        &c,
        "fix",
        "2026-10-03T10:00:00Z",
        "2026-10-03T10:10:00Z",
        50,
    )?;
    refresh(&c)?;
    let a = work::resolve_member(&c, "codex:first#0")?;
    let b = work::resolve_member(&c, "codex:reopened#0")?;
    let d = work::resolve_member(&c, "codex:fix#0")?;
    work::insert_relation(
        &c,
        &a,
        &b,
        "reopens",
        "accepted",
        "manual",
        "still broken",
        Some(1.0),
    )?;
    work::insert_relation(
        &c,
        &a,
        &d,
        "corrects_previous_work",
        "accepted",
        "manual",
        "separate correction",
        Some(1.0),
    )?;
    let rows = work::items(&c)?;
    assert_eq!(rows.len(), 2);
    let w = rows.iter().find(|w| w.members.len() == 2).unwrap();
    assert_eq!(w.state, "open");
    assert!(w.initial_completed_at.is_some());
    assert!(w.completed_at.is_none());
    finish(&c, "reopened", "feature")?;
    let r = trends::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    assert_eq!(r.periods[0].reopen_rate, Some(1.0));
    assert_eq!(r.periods[0].correction_rate, Some(1.0));
    Ok(())
}
#[test]
fn raw_validation_does_not_close_work_without_goal_evidence() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "check",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    c.execute("UPDATE derived_task SET completed_at='2026-10-01T10:10:00Z',completion_source='validation_pass'",[])?;
    assert_eq!(work::items(&c)?[0].state, "open");
    Ok(())
}
#[test]
fn milestone_excludes_spanning_lifecycle_but_keeps_spend() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "span",
        "2026-10-01T10:00:00Z",
        "2026-10-10T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "span", "feature")?;
    let id = setup::save_milestone(
        &c,
        None,
        "Installed skill",
        "2026-10-05T12:00:00Z",
        Some("/project"),
        "skill",
        "",
        None,
    )?;
    let o = trends::Options {
        around: Some(id),
        timezone: Some("UTC".into()),
        ..Default::default()
    };
    let r = trends::report_at(&c, &o, at())?;
    let after = r.comparison.unwrap().after;
    assert_eq!(after.native_tokens.total, 100);
    assert_eq!(after.cycle_minutes.excluded, 1);
    assert_eq!(after.spanning_change_ids.len(), 1);
    assert!(after.provisional);
    Ok(())
}
#[test]
fn setup_scans_never_invent_install_time_or_historical_version() -> Result<()> {
    let c = db()?;
    let dir = tempfile::tempdir()?;
    let skill = dir.path().join("review");
    std::fs::create_dir(&skill)?;
    std::fs::write(skill.join("SKILL.md"), "Review carefully")?;
    setup::capture(&c, Some("/nonexistent"), &[dir.path().to_path_buf()])?;
    let first = setup::observations(&c, Some("/nonexistent"))?;
    let row = first
        .iter()
        .find(|o| o.identity.ends_with("review/SKILL.md"))
        .unwrap();
    assert!(row.effective_at.is_none());
    assert_eq!(row.state, "first_observed");
    assert!(row.fingerprint.is_some());
    std::fs::write(skill.join("SKILL.md"), "Review changes carefully")?;
    setup::capture(&c, Some("/nonexistent"), &[dir.path().to_path_buf()])?;
    std::fs::remove_file(skill.join("SKILL.md"))?;
    setup::capture(&c, Some("/nonexistent"), &[dir.path().to_path_buf()])?;
    let rows: Vec<_> = setup::observations(&c, Some("/nonexistent"))?
        .into_iter()
        .filter(|o| o.identity.ends_with("review/SKILL.md"))
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].state, "removed");
    assert!(rows[2].previous_observed_at.is_some());
    Ok(())
}
#[test]
fn auto_classification_inherits_global_enable_and_keeps_project_limits() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "old",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    let mut config = crate::classify::config::ClassifierConfig::default();
    let before = crate::classify::automatic::status(&c, None, &config)?;
    assert_eq!(before.len(), 1);
    assert!(!before[0].enabled);
    assert_eq!(before[0].pending, 1);
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM classifier_auto_project", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    crate::classify::automatic::configure_limits(&c, "/project", 2.0, 0.5, 10000)?;
    assert!(!crate::classify::automatic::status(&c, None, &config)?[0].enabled);
    config.enabled = true;
    let status = crate::classify::automatic::status(&c, Some("/project"), &config)?;
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].pending, 1);
    assert_eq!(status[0].spent_usd, 0.0);
    assert!(status[0].enabled);
    config.enabled = false;
    assert!(!crate::classify::automatic::status(&c, None, &config)?[0].enabled);
    Ok(())
}
#[test]
fn report_drilldowns_reconcile_with_tui_query() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "one",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "one", "feature")?;
    let filter = crate::explore::Filters {
        from: Some("2026-10-01".into()),
        to: Some("2026-10-01".into()),
        repo: Some("/project".into()),
        ..Default::default()
    };
    c.execute("INSERT INTO analytics_setting VALUES('timezone','UTC')", [])?;
    let data = crate::tui::data::fetch(&c, &crate::tui::data::Query::Trends(filter, None))?;
    let crate::tui::data::Data::Trends(report) = data else {
        panic!("wrong response")
    };
    assert_eq!(report.periods[0].native_tokens.total, 100);
    let ids = crate::tui::trend_ui::contributors(&report, 0, 0);
    assert_eq!(ids, report.periods[0].contributing_work_ids);
    assert_eq!(ids.len(), 1);
    Ok(())
}

#[test]
fn corpus_persistent_inputs_roundtrip_without_classification_opt_in() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "saved",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "saved", "research")?;
    c.execute(
        "INSERT INTO analytics_setting VALUES('timezone','Europe/Berlin')",
        [],
    )?;
    crate::classify::automatic::configure_limits(&c, "/project", 2.0, 0.5, 56)?;
    let temp = tempfile::tempdir()?;
    let file = temp.path().join("inputs.json");
    crate::corpus::observations::save(&c, &file, "codex", "saved", Some("/project"))?;
    let restored = db()?;
    session(
        &restored,
        "saved",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    crate::corpus::observations::restore(&restored, &file, &serde_json::Map::new())?;
    refresh(&restored)?;
    assert_eq!(work::items(&c)?, work::items(&restored)?);
    let n: i64 = restored.query_row("SELECT COUNT(*) FROM classifier_auto_project", [], |r| {
        r.get(0)
    })?;
    assert_eq!(n, 0);
    assert_eq!(trends::timezone(&restored, None)?.0.name(), "Europe/Berlin");
    Ok(())
}

#[test]
fn resegmentation_suspends_manual_decisions_instead_of_reusing_task_number() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "edit",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    session(
        &c,
        "other",
        "2026-10-02T10:00:00Z",
        "2026-10-02T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "edit", "feature")?;
    let old = work::resolve_member(&c, "codex:edit#0")?;
    let other = work::resolve_member(&c, "codex:other#0")?;
    work::insert_relation(
        &c,
        &old,
        &other,
        "continuation",
        "accepted",
        "manual",
        "same goal",
        Some(1.0),
    )?;
    // A different source request now occupies index zero.
    c.execute("UPDATE fact_session_message SET message_key='replacement' WHERE session_id='edit' AND role='user'",[])?;
    refresh(&c)?;
    let n: i64 = c.query_row(
        "SELECT COUNT(*) FROM classification_override WHERE unit_key='codex|edit|0'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(n, 0);
    let pending: i64 = c.query_row(
        "SELECT COUNT(*) FROM work_saved_decision WHERE review_reason IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    assert!(pending >= 2);
    assert_eq!(work::items(&c)?.len(), 2);
    assert!(
        work::relations(&c)?
            .iter()
            .any(|r| r.source == "manual" && r.review_reason.is_some())
    );
    refresh(&c)?;
    assert_eq!(work::items(&c)?.len(), 2);
    Ok(())
}

#[test]
fn native_components_and_reasoning_are_not_counted_twice() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "components",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    c.execute("UPDATE fact_session_usage SET cached_input_tokens=50,cache_creation_tokens=20,output_tokens=30,reasoning_tokens=10,reasoning_in_output=1,total_tokens=200",[])?;
    refresh(&c)?;
    let items = work::items(&c)?;
    let t = &items[0].usage[0].tokens;
    assert_eq!(
        (
            t.uncached_input,
            t.cache_read,
            t.cache_write,
            t.output,
            t.reasoning_extra,
            t.total
        ),
        (100, 50, 20, 30, 0, 200)
    );
    c.execute(
        "UPDATE fact_session_usage SET reasoning_in_output=0,total_tokens=210",
        [],
    )?;
    refresh(&c)?;
    assert_eq!(work::items(&c)?[0].usage[0].tokens.total, 210);
    Ok(())
}

#[test]
fn explicit_continuation_and_manual_rejection_survive_refresh() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "previous-goal",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    session(
        &c,
        "continuation",
        "2026-10-02T10:00:00Z",
        "2026-10-02T10:10:00Z",
        100,
    )?;
    c.execute("UPDATE fact_session_message SET content='Continue previous-goal' WHERE session_id='continuation' AND role='user'",[])?;
    refresh(&c)?;
    assert_eq!(work::items(&c)?.len(), 1);
    let relation = work::relations(&c)?
        .into_iter()
        .find(|r| r.source == "explicit_reference")
        .unwrap();
    work::decide(&c, &relation.id, false)?;
    refresh(&c)?;
    assert_eq!(work::items(&c)?.len(), 2);
    assert!(
        work::relations(&c)?
            .iter()
            .any(|r| r.decision == "rejected" && r.source == "manual")
    );
    Ok(())
}

#[test]
fn automatic_status_reads_real_newline_source_ids_and_defers_active_work() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "old",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    let now = Utc::now().to_rfc3339();
    session(&c, "active", &now, &now, 100)?;
    crate::db::session_store::mark_source_ingested(
        &c,
        "codex",
        "source",
        "fp",
        &["old", "active"],
    )?;
    refresh(&c)?;
    crate::classify::automatic::configure_limits(&c, "/project", 2.0, 0.5, 3650)?;
    let rows = crate::classify::automatic::status(&c, Some("/project"), &Default::default())?;
    assert_eq!(rows[0].pending, 1);
    assert_eq!(rows[0].deferred, 1);
    Ok(())
}

#[test]
fn followup_requires_observed_history_not_just_elapsed_wall_clock() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "done",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    finish(&c, "done", "feature")?;
    c.execute(
        "DELETE FROM analytics_setting WHERE key LIKE 'observed_provider:%'",
        [],
    )?;
    let r = trends::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    assert_eq!(r.periods[0].mature_followups, 0);
    assert_eq!(r.periods[0].pending_followups, 1);
    assert_eq!(r.periods[0].reopen_rate, None);
    Ok(())
}

#[test]
fn empty_startup_conversations_do_not_become_open_engineering_goals() -> Result<()> {
    let c = db()?;
    let mut s = SessionRecord::new("codex", "empty");
    s.started_at = Some("2026-10-01T10:00:00Z".into());
    s.ended_at = Some("2026-10-01T10:00:03Z".into());
    store_session_record(&c, &s)?;
    refresh(&c)?;
    let items = work::items(&c)?;
    assert_eq!(items.len(), 1);
    assert!(!items[0].observed_activity);
    assert_eq!(items[0].state, "unobserved");
    let report = trends::report_at(&c, &opts("2026-10-01", "2026-10-01"), at())?;
    let p = &report.periods[0];
    assert_eq!((p.started, p.open, p.completed), (0, 0, 0));
    assert_eq!(p.coverage.empty_conversations_excluded, 1);
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM metadata_sessions", [], |r| r
            .get::<_, i64>(0))?,
        1
    );
    Ok(())
}

#[test]
fn incomplete_sources_and_unknown_goal_completion_are_visible_and_scoped() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "work",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    c.execute("INSERT INTO analytics_setting VALUES('source_error:opencode','Unsupported future source layout')",[])?;
    let mut o = opts("2026-10-01", "2026-10-01");
    let r = trends::report_at(&c, &o, at())?;
    assert!(r.source_errors.contains_key("opencode"));
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains("Missing activity must not be interpreted as zero"))
    );
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains("No supported goal completions"))
    );
    assert_eq!(r.periods[0].cycle_minutes.median, None);
    o.scope.providers = vec!["codex".into()];
    assert!(trends::report_at(&c, &o, at())?.source_errors.is_empty());
    Ok(())
}

#[test]
fn comparisons_warn_when_the_baseline_predates_recorded_history() -> Result<()> {
    let c = db()?;
    session(
        &c,
        "first",
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:10:00Z",
        100,
    )?;
    refresh(&c)?;
    let mut o = opts("2026-10-01", "2026-10-07");
    o.compare = true;
    let r = trends::report_at(&c, &o, at())?;
    let comparison = r.comparison.unwrap();
    assert!(comparison.before.coverage.before_first_observation);
    assert!(!comparison.after.coverage.before_first_observation);
    assert!(r.warnings.iter().any(|s| s.contains("unverified coverage")));
    Ok(())
}
