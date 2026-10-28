use anyhow::Result;
use assert_cmd::cargo::CommandCargoExt;
use serde_json::Value;
use std::process::Command;
use tempfile::TempDir;

fn command(home: &TempDir, args: &[&str]) -> Result<std::process::Output> {
    let output = Command::cargo_bin("vca")?
        .args(args)
        .current_dir(home.path())
        .env("VCA_HOME", home.path())
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .env("CODEX_HOME", home.path().join(".codex"))
        .env("VCA_PRICING_OFFLINE", "1")
        .env_remove("OPENROUTER_API_KEY")
        .output()?;
    Ok(output)
}
fn run(home: &TempDir, args: &[&str]) -> Result<Value> {
    let output = command(home, args)?;
    anyhow::ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn upgrade_and_reports_keep_missing_sources_and_export_contract() -> Result<()> {
    let home = TempDir::new()?;
    std::fs::create_dir_all(home.path().join(".vca"))?;
    let id;
    {
        let conn = rusqlite::Connection::open(home.path().join(".vca/vca.db"))?;
        vca::db::init_app_schema(&conn)?;
        let mut s = vca::db::session_store::SessionRecord::new("codex", "retained");
        s.started_at = Some("2026-10-01T10:00:00Z".into());
        s.source_path = Some("/unavailable/transcript.jsonl".into());
        s.messages.push(vca::db::session_store::MessageRecord {
            key: "request".into(),
            ts: s.started_at.clone(),
            role: "user".into(),
            text: "Explain the design".into(),
            is_compaction_summary: false,
        });
        vca::db::session_store::store_session_record(&conn, &s)?;
        vca::db::session_store::refresh_session_roots(&conn)?;
        vca::insights::refresh(&conn)?;
        id = vca::insights::work::items(&conn)?[0].id.clone();
        conn.pragma_update(None, "user_version", 2)?;
    }
    let items = run(&home, &["work", "list", "--json"])?;
    assert_eq!(items["data"][0]["id"], id);
    let report = run(
        &home,
        &[
            "trends",
            "--from",
            "2026-10-01",
            "--to",
            "2026-10-01",
            "--timezone",
            "UTC",
            "--json",
        ],
    )?;
    assert_eq!(report["periods"][0]["started"], 1);
    assert_eq!(report["periods"][0]["open"], 1);
    assert_eq!(
        report["periods"][0]["tokens_per_completed"]["median"],
        Value::Null
    );
    let before = run(&home, &["changes", "list", "--json"])?;
    assert!(before["data"]["milestones"].as_array().unwrap().is_empty());
    let status = run(&home, &["classify", "auto", "status", "--json"])?;
    assert_eq!(status["automatic_classification"]["enabled"], false);
    assert!(
        status["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["enabled"] == false)
    );
    let conn = rusqlite::Connection::open(home.path().join(".vca/vca.db"))?;
    vca::db::init_app_schema(&conn)?;
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))?,
        4
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM metadata_sessions", [], |r| r
            .get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM classifier_run", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    let direct = vca::insights::trends::report(
        &conn,
        &vca::insights::trends::Options {
            from: Some("2026-10-01".into()),
            to: Some("2026-10-01".into()),
            timezone: Some("UTC".into()),
            ..Default::default()
        },
    )?;
    assert_eq!(report["periods"], serde_json::to_value(direct.periods)?);
    Ok(())
}

#[test]
fn global_enable_preserves_settings_covers_future_projects_and_disable_stops_every_project()
-> Result<()> {
    let home = TempDir::new()?;
    std::fs::create_dir_all(home.path().join(".vca"))?;
    let path = home.path().join(".vca/classifier.json");
    std::fs::write(
        &path,
        serde_json::to_vec(
            &serde_json::json!({"enabled":false,"monthly_budget_usd":1.25,"project_monthly_budget_usd":0.75,"max_run_usd":0.1,"backfill_days":21,"allow_projects":["/allowed/*"],"deny_projects":["/allowed/private"]}),
        )?,
    )?;
    let enabled = command(&home, &["classify", "auto", "enable"])?;
    assert!(
        enabled.status.success(),
        "{}",
        String::from_utf8_lossy(&enabled.stderr)
    );
    assert!(String::from_utf8_lossy(&enabled.stdout).contains("globally"));
    let saved: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    assert_eq!(saved["enabled"], true);
    assert_eq!(saved["monthly_budget_usd"], 1.25);
    assert_eq!(saved["project_monthly_budget_usd"], 0.75);
    assert_eq!(saved["max_run_usd"], 0.1);
    assert_eq!(saved["backfill_days"], 21);
    assert_eq!(saved["deny_projects"][0], "/allowed/private");
    {
        let c = rusqlite::Connection::open(home.path().join(".vca/vca.db"))?;
        let mut s = vca::db::session_store::SessionRecord::new("codex", "future");
        s.started_at = Some((chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339());
        s.messages.push(vca::db::session_store::MessageRecord {
            key: "future-request".into(),
            ts: s.started_at.clone(),
            role: "user".into(),
            text: "Explain the new behavior".into(),
            is_compaction_summary: false,
        });
        s.repository_hint = Some(vca::db::session_store::RepositoryHint {
            project_id: "future".into(),
            root: "/allowed/future".into(),
            source: "fixture".into(),
        });
        vca::db::session_store::store_session_record(&c, &s)?;
        vca::db::session_store::refresh_session_roots(&c)?;
        vca::insights::refresh(&c)?;
    }
    let status = run(&home, &["classify", "auto", "status", "--json"])?;
    assert_eq!(status["automatic_classification"]["enabled"], true);
    assert_eq!(
        status["automatic_classification"]["scope"],
        "all_ingested_projects"
    );
    assert_eq!(status["data"][0]["enabled"], true);
    assert_eq!(status["data"][0]["repo_root"], "/allowed/future");
    assert_eq!(status["data"][0]["monthly_budget"], 0.75);
    assert_eq!(
        status["data"][0]["blocked_reason"],
        "No classifier API key configured"
    );
    let disabled = command(&home, &["classify", "auto", "disable"])?;
    assert!(disabled.status.success());
    let limits = command(
        &home,
        &[
            "classify",
            "auto",
            "limits",
            "--repo",
            "/allowed/future",
            "--monthly-budget",
            "0.4",
        ],
    )?;
    assert!(limits.status.success());
    let status = run(&home, &["classify", "auto", "status", "--json"])?;
    assert_eq!(status["automatic_classification"]["enabled"], false);
    assert_eq!(status["data"][0]["enabled"], false);
    assert_eq!(status["data"][0]["monthly_budget"], 0.4);
    let scoped_enable = command(
        &home,
        &["classify", "auto", "enable", "--repo", "/allowed/future"],
    )?;
    assert!(!scoped_enable.status.success());
    let c = rusqlite::Connection::open(home.path().join(".vca/vca.db"))?;
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM classifier_run", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    Ok(())
}

#[test]
fn daily_json_csv_and_paginated_drill_reconcile_without_external_requests() -> Result<()> {
    let home = TempDir::new()?;
    std::fs::create_dir_all(home.path().join(".vca"))?;
    let conn = rusqlite::Connection::open(home.path().join(".vca/vca.db"))?;
    vca::db::init_app_schema(&conn)?;
    let mut s = vca::db::session_store::SessionRecord::new("codex", "daily-cli");
    s.started_at = Some("2026-10-01T12:00:00Z".into());
    s.messages.push(vca::db::session_store::MessageRecord {
        key: "request".into(),
        ts: s.started_at.clone(),
        role: "user".into(),
        text: "Explain the daily design".into(),
        is_compaction_summary: false,
    });
    let mut u = vca::db::session_store::UsageRecord::new("u");
    u.ts = s.started_at.clone();
    u.input_tokens = 120;
    s.usage.push(u);
    vca::db::session_store::store_session_record(&conn, &s)?;
    vca::db::session_store::refresh_session_roots(&conn)?;
    vca::insights::refresh(&conn)?;
    let args = [
        "daily",
        "--all-projects",
        "--from",
        "2026-10-01",
        "--to",
        "2026-10-01",
        "--timezone",
        "UTC",
    ];
    let mut json = args.to_vec();
    json.push("--json");
    let report = run(&home, &json)?;
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["days"][0]["native_tokens"]["total"], 120);
    assert_eq!(report["days"][0]["started"], 1);
    assert_eq!(report["days"][0]["reported_cost_usd"], Value::Null);
    let mut csv = args.to_vec();
    csv.push("--csv");
    let output = command(&home, &csv)?;
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout)?;
    let mut lines = text.lines();
    let header: Vec<_> = lines.next().unwrap().split(',').collect();
    let values: Vec<_> = lines.next().unwrap().split(',').collect();
    let value = |name| values[header.iter().position(|h| *h == name).unwrap()];
    assert_eq!(value("native_tokens"), "120");
    assert_eq!(value("started"), "1");
    assert_eq!(value("reported_cost_usd"), "");
    let detail = run(
        &home,
        &[
            "daily",
            "--all-projects",
            "--details",
            "2026-10-01",
            "--metric",
            "active",
            "--timezone",
            "UTC",
            "--limit",
            "1",
        ],
    )?;
    assert_eq!(detail["total"], 1);
    assert_eq!(detail["ids"].as_array().unwrap().len(), 1);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM classifier_run", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    Ok(())
}
