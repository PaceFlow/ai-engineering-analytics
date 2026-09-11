//! Ingest real provider-shaped SQLite/JSONL inputs through the CLI, then
//! assert independently specified metrics. No analytics tables are seeded.
use anyhow::Result;
use assert_cmd::cargo::CommandCargoExt;
use clap::Parser;
use paceflow::{
    analytics,
    cli::{Cli, Commands},
};
use rusqlite::{Connection, params};
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Result<Self> {
        let fixture = Self {
            dir: tempfile::tempdir()?,
        };
        for child in [
            "opencode/storage/session_diff",
            "cursor/History",
            "codex",
            "repo",
        ] {
            fs::create_dir_all(fixture.path(child))?;
        }
        Ok(fixture)
    }

    fn path(&self, child: &str) -> PathBuf {
        self.dir.path().join(child)
    }

    fn git(&self, args: &[&str], date: &str) -> Result<()> {
        let output = Command::new("git")
            .args(args)
            .current_dir(self.path("repo"))
            .env("GIT_AUTHOR_NAME", "Fixture")
            .env("GIT_COMMITTER_NAME", "Fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.test")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.test")
            .env("GIT_AUTHOR_DATE", date)
            .env("GIT_COMMITTER_DATE", date)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", self.path("empty-git-config"))
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn run(&self, args: &[&str]) -> Result<Output> {
        Ok(Command::cargo_bin("vba")?
            .args(args)
            .current_dir(self.dir.path())
            .env("PACEFLOW_HOME", self.dir.path())
            .env(
                "PACEFLOW_OPENCODE_DB_PATH",
                self.path("opencode/opencode.db"),
            )
            .env(
                "PACEFLOW_CURSOR_STATE_PATH",
                self.path("cursor/state.vscdb"),
            )
            .env("PACEFLOW_CURSOR_HISTORY_PATH", self.path("cursor/History"))
            .env("PACEFLOW_CODEX_SESSIONS_PATH", self.path("codex"))
            .env_remove("PACEFLOW_GITHUB_TOKEN")
            .output()?)
    }

    fn ingest(&self, provider: &str) -> Result<()> {
        let output = self.run(&["ingest", "--provider", provider])?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("Warning:"));
        Ok(())
    }

    fn analytics(&self) -> Result<Connection> {
        Ok(Connection::open(self.path(".paceflow/paceflow.db"))?)
    }

    fn opencode(&self) -> Result<()> {
        let db = Connection::open(self.path("opencode/opencode.db"))?;
        db.execute_batch("CREATE TABLE project(id TEXT PRIMARY KEY, worktree TEXT);
            CREATE TABLE session(id TEXT PRIMARY KEY, project_id TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);
            CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
            CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);")?;
        let repo = self.path("repo").to_string_lossy().to_string();
        db.execute("INSERT INTO project VALUES ('p', ?1)", [&repo])?;
        for (id, model, tokens, cost) in [
            ("modern", "gpt-4o", 1200, 0.12),
            ("legacy", "gpt-4o", 0, 0.01),
            ("unpriced", "unknown-e2e-model", 50, 0.0),
        ] {
            let start = 1_767_225_600_000i64;
            db.execute(
                "INSERT INTO session VALUES (?1,'p',?2,?3,?4)",
                params![id, repo, start, start + 60_000],
            )?;
            for (role, time) in [("user", start), ("assistant", start + 60_000)] {
                let mid = format!("{id}-{role}");
                let data = if role == "assistant" {
                    json!({"role":role,"modelID":model,"tokens":{"input":tokens,"output":0,"total":tokens},"cost":cost})
                } else {
                    json!({"role":role})
                };
                db.execute(
                    "INSERT INTO message VALUES (?1,?2,?3,?3,?4)",
                    params![mid, id, time, data.to_string()],
                )?;
                db.execute(
                    "INSERT INTO part VALUES (?1,?1,?2,?3,?3,?4)",
                    params![
                        mid,
                        id,
                        time,
                        json!({"type":"text","text":"update the example"}).to_string()
                    ],
                )?;
            }
        }
        fs::write(self.path("opencode/storage/session_diff/modern.json"), json!([{"file":"example.rs","before":"old\nkeep\n","after":"new\nkeep\nextra\n","additions":999,"deletions":999,"status":"modified"}]).to_string())?;
        fs::write(
            self.path("opencode/storage/session_diff/legacy.json"),
            json!([{"file":"legacy.rs","patch":"@@ -0,0 +1,1 @@\n+legacy\n"}]).to_string(),
        )?;
        Ok(())
    }

    fn cost(&self, provider: &str) -> Result<analytics::CostReportRow> {
        let cli = Cli::parse_from([
            "vba",
            "cost",
            "--all-projects",
            "--overall",
            "--provider",
            provider,
        ]);
        let Commands::Cost(mut args) = cli.command else {
            unreachable!()
        };
        args.report.group_by = None;
        Ok(analytics::query_cost_report(&self.analytics()?, &args.report)?.remove(0))
    }
}

#[test]
fn ingest_db_then_delivery_and_churn_follow_commits_even_after_cache_warmup() -> Result<()> {
    let fixture = Fixture::new()?;
    fixture.opencode()?;
    fixture.git(&["init", "-b", "main"], "2025-12-31T23:59:00Z")?;
    fixture.git(
        &["config", "commit.gpgsign", "false"],
        "2025-12-31T23:59:00Z",
    )?;
    fs::write(fixture.path("repo/example.rs"), "old\nkeep\n")?;
    fixture.git(&["add", "."], "2025-12-31T23:59:00Z")?;
    fixture.git(&["commit", "-m", "initial example"], "2025-12-31T23:59:00Z")?;
    fs::write(fixture.path("repo/example.rs"), "new\nkeep\nextra\n")?;
    fixture.git(&["add", "."], "2026-01-01T00:02:00Z")?;
    fixture.git(&["commit", "-m", "update example"], "2026-01-01T00:02:00Z")?;
    for _ in 0..2 {
        fixture.ingest("opencode")?;
        let db = fixture.analytics()?;
        let (commits, heavy, mainline): (i64,i64,i64) = db.query_row("SELECT count(*),sum(heavy_ai_flag),sum(CASE WHEN heavy_ai_flag=1 THEN merged_to_mainline_flag ELSE 0 END) FROM event_commit_outcome", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        assert_eq!((commits, heavy, mainline), (2, 1, 1));
        let churn: (i64,i64) = db.query_row("SELECT sum(ai_added_lines_reaching_mainline),sum(ai_added_lines_removed_within_window) FROM event_commit_churn", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
        assert_eq!(churn, (2, 0));
    }
    // Moving main must refresh quality, even though the source DB is unchanged.
    fs::write(fixture.path("repo/example.rs"), "fixed\nkeep\nextra\n")?;
    fixture.git(&["add", "."], "2026-01-01T00:03:00Z")?;
    fixture.git(&["commit", "-m", "fix example"], "2026-01-01T00:03:00Z")?;
    fixture.ingest("opencode")?;
    let churn: (i64,i64) = fixture.analytics()?.query_row("SELECT sum(ai_added_lines_reaching_mainline),sum(ai_added_lines_removed_within_window) FROM event_commit_churn", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(churn, (2, 1));
    Ok(())
}

#[test]
fn ingest_opencode_db_then_changes_tokens_cost_and_coverage_have_known_values() -> Result<()> {
    let fixture = Fixture::new()?;
    fixture.opencode()?;
    // Two changed sessions: 2 additions + 1 removal, and 1 legacy addition.
    // Only one of the two sessions with nonzero usage has a known price.
    for _ in 0..2 {
        fixture.ingest("opencode")?;
        let cost = fixture.cost("opencode")?;
        assert_eq!((cost.session_count, cost.accepted_session_count), (3, 2));
        assert_eq!(cost.accepted_total_changed_lines, 4);
        assert_eq!(cost.total_tokens, 1250);
        assert!((cost.total_cost_usd.unwrap() - 0.13).abs() < 1e-9);
        assert_eq!(
            (cost.priced_session_count, cost.usage_session_count),
            (1, 2)
        );
        let output = fixture.run(&[
            "cost",
            "--provider",
            "opencode",
            "--all-projects",
            "--overall",
        ])?;
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("50.0%"));
        let cli = Cli::parse_from([
            "vba",
            "session",
            "--provider",
            "opencode",
            "--all-projects",
            "--overall",
        ]);
        let Commands::Session(mut args) = cli.command else {
            unreachable!()
        };
        args.report.group_by = None;
        let rows = analytics::query_session_report(&fixture.analytics()?, &args.report)?;
        assert_eq!(rows[0].session_count, 3);
        assert_eq!(rows[0].s2_avg, Some(1.0));
        assert_eq!(
            (
                rows[0].no_output_session_rate.numerator,
                rows[0].no_output_session_rate.denominator
            ),
            (1, 3)
        );
        assert!((rows[0].avg_minutes_to_first_accepted_change.unwrap() - 1.0).abs() < 1e-6);
        let counts: (i64, i64) = fixture.analytics()?.query_row("SELECT sum(lines_added), sum(lines_removed) FROM fact_session_code_change WHERE source_kind='tool_write'", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        assert_eq!(counts, (3, 1));
    }
    // A changed diff must refresh both metrics and attribution without duplicates.
    fs::write(
        fixture.path("opencode/storage/session_diff/modern.json"),
        json!([{"file":"example.rs","before":"old\nkeep\n","after":"new\nkeep\nextra\nanother\n"}])
            .to_string(),
    )?;
    fixture.ingest("opencode")?;
    assert_eq!(fixture.cost("opencode")?.accepted_total_changed_lines, 5);
    assert_eq!(fixture.cost("opencode")?.total_tokens, 1250);
    let counts: (i64, i64) = fixture.analytics()?.query_row("SELECT sum(lines_added),sum(lines_removed) FROM fact_session_code_change WHERE source_kind='tool_write'", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
    assert_eq!(counts, (4, 1));
    Ok(())
}

/// Private copies are deliberately outside git. See DEV.md for the layout.
#[test]
#[ignore = "requires VBA_LOCAL_FIXTURES pointing at the 2026-09-10 local copies"]
fn ingest_copied_local_databases_then_known_metrics_match() -> Result<()> {
    let sources = PathBuf::from(std::env::var("VBA_LOCAL_FIXTURES")?);
    let fixture = Fixture::new()?;
    for (provider, sessions, tokens, changed) in [
        ("opencode", 9, Some(51_866_246), Some(232)),
        ("codex", 141, Some(1_758_526_143), None),
        ("cursor", 482, None, None),
    ] {
        let mut first_metrics = None;
        for _ in 0..2 {
            let output = Command::cargo_bin("vba")?
                .args(["ingest", "--provider", provider])
                .current_dir(fixture.dir.path())
                .env("PACEFLOW_HOME", fixture.dir.path())
                .env(
                    "PACEFLOW_OPENCODE_DB_PATH",
                    sources.join("sources/opencode/opencode.db"),
                )
                .env(
                    "PACEFLOW_CURSOR_STATE_PATH",
                    sources.join("sources/cursor/state.vscdb"),
                )
                .env(
                    "PACEFLOW_CURSOR_HISTORY_PATH",
                    sources.join("sources/cursor/History"),
                )
                .env(
                    "PACEFLOW_CODEX_SESSIONS_PATH",
                    sources.join("home/.codex/sessions"),
                )
                .env_remove("PACEFLOW_GITHUB_TOKEN")
                .output()?;
            assert!(
                output.status.success(),
                "{provider}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let cost = fixture.cost(provider)?;
            assert_eq!(cost.session_count, sessions, "{provider} sessions");
            if let Some(tokens) = tokens {
                assert_eq!(cost.total_tokens, tokens, "{provider} tokens");
            }
            if let Some(changed) = changed {
                assert_eq!(
                    cost.accepted_total_changed_lines, changed,
                    "{provider} changed lines"
                );
            }
            assert!(cost.priced_session_count <= cost.usage_session_count);
            let metrics = (
                cost.session_count,
                cost.accepted_session_count,
                cost.accepted_total_changed_lines,
                cost.total_tokens,
                cost.priced_session_count,
                cost.usage_session_count,
            );
            if let Some(first) = first_metrics {
                assert_eq!(
                    metrics, first,
                    "{provider} metrics changed on repeat ingestion"
                );
            } else {
                first_metrics = Some(metrics);
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_opencode_diff_fails_without_partial_metrics_and_retry_recovers() -> Result<()> {
    let fixture = Fixture::new()?;
    fixture.opencode()?;
    let path = fixture.path("opencode/storage/session_diff/modern.json");
    let valid = fs::read(&path)?;
    fs::write(&path, r#"[{"file":"example.rs"}]"#)?;
    let output = fixture.run(&["ingest", "--provider", "opencode"])?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires patch or both before and after")
    );
    assert_eq!(
        fixture
            .analytics()?
            .query_row("SELECT count(*) FROM metadata_sessions", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    fs::write(&path, valid)?;
    fixture.ingest("opencode")?;
    assert_eq!(fixture.cost("opencode")?.accepted_total_changed_lines, 4);
    Ok(())
}

#[test]
fn ingest_cursor_db_then_one_edit_means_three_changed_lines_and_one_session() -> Result<()> {
    let fixture = Fixture::new()?;
    let db = Connection::open(fixture.path("cursor/state.vscdb"))?;
    db.execute_batch("CREATE TABLE cursorDiskKV(key TEXT PRIMARY KEY, value TEXT);")?;
    let file = fixture
        .path("repo/example.rs")
        .to_string_lossy()
        .to_string();
    for (key, value) in [
        (
            "composerData:cursor-e2e",
            json!({"composerId":"cursor-e2e","createdAt":1767225600000i64,"lastUpdatedAt":1767225660000i64,"conversation":[{"type":1,"text":"update example"}],"originalFileStates":{format!("file://{file}"):{"content":"old\n","firstEditBubbleId":"edit"}}}),
        ),
        (
            "bubbleId:cursor-e2e:edit",
            json!({"type":2,"text":"","createdAt":1767225660000i64,"toolFormerData":{"status":"completed","name":"edit_file_v2","toolCallId":"edit","params":json!({"relativeWorkspacePath":file,"streamingContent":"@@\n-old\n+new\n+extra\n"}).to_string()}}),
        ),
    ] {
        db.execute(
            "INSERT INTO cursorDiskKV VALUES (?1,?2)",
            params![key, value.to_string()],
        )?;
    }
    drop(db);
    for _ in 0..2 {
        fixture.ingest("cursor")?;
        let cost = fixture.cost("cursor")?;
        assert_eq!(
            (
                cost.session_count,
                cost.accepted_session_count,
                cost.accepted_total_changed_lines
            ),
            (1, 1, 3)
        );
        assert_eq!(
            fixture.analytics()?.query_row(
                "SELECT count(*) FROM fact_session_code_change WHERE source_kind='tool_write'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
    }
    Ok(())
}

#[test]
fn ingest_codex_jsonl_then_cumulative_usage_is_counted_once() -> Result<()> {
    let fixture = Fixture::new()?;
    let events = [
        json!({"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"id":"codex-e2e","cwd":fixture.path("repo"),"model_provider":"openai"}}),
        json!({"type":"turn_context","timestamp":"2026-01-01T00:00:00Z","payload":{"model":"gpt-5.4"}}),
        json!({"type":"event_msg","timestamp":"2026-01-01T00:00:00Z","payload":{"type":"user_message","message":"update example"}}),
        json!({"type":"event_msg","timestamp":"2026-01-01T00:01:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"output_tokens":20,"total_tokens":120}}}}),
        json!({"type":"event_msg","timestamp":"2026-01-01T00:02:00Z","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":200,"output_tokens":40,"total_tokens":240}}}}),
    ];
    fs::write(
        fixture.path("codex/session.jsonl"),
        events.iter().map(|e| format!("{e}\n")).collect::<String>(),
    )?;
    for _ in 0..2 {
        fixture.ingest("codex")?;
        let cost = fixture.cost("codex")?;
        assert_eq!(
            (
                cost.session_count,
                cost.total_tokens,
                cost.accepted_total_changed_lines
            ),
            (1, 240, 0)
        );
        assert_eq!(
            (cost.priced_session_count, cost.usage_session_count),
            (1, 1)
        );
    }
    Ok(())
}
