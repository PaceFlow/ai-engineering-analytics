//! Live tests against OpenRouter's Decisions API (Jev). Opt-in: they are
//! `#[ignore]`d and need `OPENROUTER_API_KEY` (CI maps the `OPENROUTER_KEY`
//! secret to it). They print response shapes, never the key.
//!
//! ```bash
//! OPENROUTER_API_KEY=… cargo test -p vibe-coding-analytics --test jev_live -- --ignored --nocapture --test-threads=1
//! ```

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Value, json};
use vca::classify::client::{
    DecisionClient, DecisionRequest, OpenRouterClient, answer_valid, band, parse_response,
};
use vca::classify::config::{ClassifierConfig, KEY_ENV};
use vca::classify::questions::{task_label_questions, turn_questions};
use vca::classify::run::{Passes, RunOptions, SessionRef, run};
use vca::classify::state::Level;
use vca::db::session_store::{MessageRecord, SessionRecord, store_session_record};

fn key() -> Option<String> {
    std::env::var(KEY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn sample_request() -> DecisionRequest {
    let mut questions = task_label_questions(Level::Conversation);
    questions.extend(turn_questions(Level::Conversation));
    DecisionRequest {
        model: ClassifierConfig::default().model,
        state: json!({
            "unit": "task",
            "tool": "claude_code",
            "duration_minutes": 25,
            "turns": 3,
            "evidence": {
                "files_edited": 2,
                "validations": [
                    {"kind": "test", "result": "fail", "sig": "assertion failed: totals match"},
                    {"kind": "test", "result": "pass"}
                ],
                "commits_after_task": 1,
                "subagents": 0,
                "compactions": 0
            },
            "transcript": [
                {"turn": 0, "user": "Add a --weekly flag to the cost report. cargo test cost_report must pass.",
                 "assistant": "I'll read the report module first, then add the flag.",
                 "tools": "read×3 edit×2 test✗"},
                {"turn": 1, "user": "still failing: totals don't match",
                 "assistant": "Fixed the week boundary; tests pass now.", "tools": "edit×1 test✓"},
                {"turn": 2, "user": "looks good, commit it", "tools": "exec×1"}
            ],
            "current_turn": {"tools": "edit×1 test✓", "files_edited": ["src/report.rs"], "edit_calls": 1,
                             "checks": [{"kind": "test", "result": "pass"}]}
        }),
        questions,
        provider: ClassifierConfig::default().provider,
    }
}

#[test]
#[ignore = "needs OPENROUTER_API_KEY; run with --ignored"]
fn decisions_api_answers_every_question_type() -> Result<()> {
    let Some(key) = key() else {
        eprintln!("skipping: {KEY_ENV} not set");
        return Ok(());
    };
    let config = ClassifierConfig::default();
    let request = sample_request();

    // Raw call first, so the response shape is visible in the log.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (status, body) = runtime.block_on(async {
        let response = reqwest::Client::new()
            .post(&config.endpoint)
            .bearer_auth(&key)
            .json(&request.body())
            .send()
            .await?;
        let status = response.status().as_u16();
        Ok::<_, anyhow::Error>((status, response.text().await?))
    })?;
    let value: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "HTTP {status}, non-JSON body: {}",
            body.chars().take(500).collect::<String>()
        )
    })?;
    let mut shape = value.clone();
    if let Some(answers) = shape.get_mut("answers").and_then(Value::as_object_mut) {
        answers
            .retain(|name, _| matches!(name.as_str(), "intent" | "progress" | "p_misunderstood"));
    }
    eprintln!("HTTP {status}; response sample: {shape:#}");
    assert_eq!(
        status,
        200,
        "Decisions API error: {}",
        body.chars().take(800).collect::<String>()
    );

    let parsed = parse_response(&value, status, 1);
    let mut problems = Vec::new();
    for (name, question) in &request.questions {
        match parsed.answers.get(name) {
            Some(answer) if answer_valid(question, answer) => eprintln!(
                "{name:<36} {:<24} conf {:?} p {:?} band {}",
                answer.label.as_deref().unwrap_or("-"),
                answer.confidence,
                answer.probability,
                band(question, answer)
            ),
            Some(answer) => problems.push(format!("{name}: invalid answer {answer:?}")),
            None => problems.push(format!("{name}: missing")),
        }
    }
    assert!(
        problems.is_empty(),
        "answers our parser cannot use:\n{}",
        problems.join("\n")
    );
    assert!(
        parsed.input_tokens.unwrap_or(0) > 0,
        "usage.input_tokens missing"
    );
    assert!(parsed.cost_usd.is_some(), "usage.cost missing");

    // The production client gives the same result.
    let client = OpenRouterClient::new(&config.endpoint, &key)?;
    let response = client
        .decide(&request)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(response.answers.len(), request.questions.len());
    Ok(())
}

fn message(key: &str, ts: &str, role: &str, text: &str) -> MessageRecord {
    MessageRecord {
        key: key.into(),
        ts: Some(ts.into()),
        role: role.into(),
        text: text.into(),
        is_compaction_summary: false,
    }
}

#[test]
#[ignore = "needs OPENROUTER_API_KEY; run with --ignored"]
fn classify_run_labels_a_session_end_to_end() -> Result<()> {
    let Some(key) = key() else {
        eprintln!("skipping: {KEY_ENV} not set");
        return Ok(());
    };
    let conn = Connection::open_in_memory()?;
    vca::db::init_app_schema(&conn)?;
    let mut record = SessionRecord::new("claude", "jev-live");
    record.started_at = Some("2026-10-01T10:00:00Z".into());
    record.ended_at = Some("2026-10-01T11:00:00Z".into());
    record.messages = vec![
        message(
            "u0",
            "2026-10-01T10:00:00Z",
            "user",
            "Add a --weekly flag to the cost report; cargo test cost_report must pass.",
        ),
        message(
            "a0",
            "2026-10-01T10:01:00Z",
            "assistant",
            "Added the flag and a test.",
        ),
        message(
            "u1",
            "2026-10-01T10:10:00Z",
            "user",
            "Separate thing: how does the matcher handle renamed files?",
        ),
        message(
            "a1",
            "2026-10-01T10:11:00Z",
            "assistant",
            "It follows git rename detection and compares line hashes.",
        ),
        message(
            "u2",
            "2026-10-01T10:20:00Z",
            "user",
            "Back to the weekly flag: totals are wrong on empty weeks.",
        ),
        message(
            "a2",
            "2026-10-01T10:21:00Z",
            "assistant",
            "Fixed empty weeks; tests pass.",
        ),
    ];
    store_session_record(&conn, &record)?;
    vca::db::session_store::refresh_session_roots(&conn)?;
    vca::insights::refresh(&conn)?;

    let config = ClassifierConfig {
        monthly_budget_usd: 1.0,
        ..ClassifierConfig::default()
    };
    let client = OpenRouterClient::new(&config.endpoint, &key)?;
    let options = RunOptions {
        level: Level::Conversation,
        max_usd: 0.02,
        passes: Passes::ALL,
        dry_run: false,
    };
    let session = SessionRef {
        provider: "claude".into(),
        session_id: "jev-live".into(),
        repo_root: None,
    };
    let summary = run(&conn, &client, &config, &[session], &options)?;
    eprintln!("{summary:#?}");
    assert!(
        summary.stopped.is_none(),
        "run stopped: {:?}",
        summary.stopped
    );
    assert_eq!(summary.failed, 0);
    assert!(summary.sent > 0);
    assert!(summary.cost_usd > 0.0 && summary.cost_usd < 0.02);

    let jev_stages: i64 = conn.query_row(
        "SELECT COUNT(*) FROM derived_turn_stage WHERE stage_source = 'jev'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(jev_stages, 3, "every turn gets a Jev stage");
    let (tasks, segmentation): (i64, String) = conn.query_row(
        "SELECT COUNT(*), MAX(segmentation_source) FROM derived_task WHERE session_id = 'jev-live'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    eprintln!("tasks after segmentation: {tasks} ({segmentation})");
    let rows: Vec<(i64, String, String)> = conn
        .prepare("SELECT turn_index, stage, COALESCE(request_kind, '') FROM derived_turn_stage ORDER BY turn_index")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    eprintln!("turn stages: {rows:?}");
    Ok(())
}
