use anyhow::Result;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rusqlite::Connection;

use super::app::{App, DetailTab, Screen, Unit, UsageTab};
use super::data::InlineLoader;
use super::ui;
use crate::db::init_app_schema;
use crate::db::session_store::{
    AgentKind, MessageRecord, RateLimitRecord, SessionRecord, ToolCallRecord, UsageRecord,
    refresh_session_roots, store_session_record,
};
use crate::explore::Filters;

fn message(key: &str, ts: &str, role: &str, text: &str) -> MessageRecord {
    MessageRecord {
        key: key.into(),
        ts: Some(ts.into()),
        role: role.into(),
        text: text.into(),
        is_compaction_summary: false,
    }
}

fn usage(key: &str, ts: &str, model: &str, input: i64, output: i64) -> UsageRecord {
    let mut usage = UsageRecord::new(key);
    usage.ts = Some(ts.into());
    usage.model = Some(model.into());
    usage.input_tokens = input;
    usage.output_tokens = output;
    usage
}

fn seeded() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    init_app_schema(&conn)?;
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let ts = |time: &str| format!("{today}T{time}Z");

    let mut a = SessionRecord::new("claude", "sess-alpha");
    a.started_at = Some(ts("09:00:00"));
    a.ended_at = Some(ts("10:00:00"));
    a.title = Some("Fix the token parser double count".into());
    a.messages = vec![
        message(
            "u1",
            &ts("09:00:00"),
            "user",
            "Fix the token parser double count; cargo test must pass",
        ),
        message("a1", &ts("09:01:00"), "assistant", "Looking at the parser"),
        message(
            "u2",
            &ts("09:20:00"),
            "user",
            "still failing with the same error",
        ),
    ];
    a.usage = vec![
        usage("m1", &ts("09:01:00"), "claude-opus-5-5", 50_000, 2_000),
        usage("m2", &ts("09:21:00"), "claude-opus-5-5", 80_000, 3_000),
    ];
    a.tool_calls = vec![
        ToolCallRecord {
            key: "t1".into(),
            ts: Some(ts("09:02:00")),
            tool_name: "Edit".into(),
            target: Some("src/parser.rs".into()),
            status: "ok".into(),
            output_in_context: true,
            ..ToolCallRecord::default()
        },
        ToolCallRecord {
            key: "t2".into(),
            ts: Some(ts("09:03:00")),
            tool_name: "Bash".into(),
            target: Some("cargo test".into()),
            status: "ok".into(),
            exit_code: Some(0),
            output_chars: 4000,
            output_in_context: true,
            ..ToolCallRecord::default()
        },
        ToolCallRecord {
            key: "t3".into(),
            ts: Some(ts("09:04:00")),
            tool_name: "Task".into(),
            target: Some("explore".into()),
            status: "ok".into(),
            child_session_id: Some("sess-alpha/agent-1".into()),
            output_in_context: true,
            ..ToolCallRecord::default()
        },
    ];
    store_session_record(&conn, &a)?;

    let mut child = SessionRecord::new("claude", "sess-alpha/agent-1");
    child.parent_session_id = Some("sess-alpha".into());
    child.agent_kind = AgentKind::Subagent;
    child.agent_type = Some("Explore".into());
    child.messages = vec![message("c1", &ts("09:04:10"), "user", "explore the parser")];
    child.usage = vec![usage(
        "cm1",
        &ts("09:05:00"),
        "claude-sonnet-5-5",
        20_000,
        1_000,
    )];
    store_session_record(&conn, &child)?;

    let mut b = SessionRecord::new("codex", "sess-beta");
    b.started_at = Some(ts("11:00:00"));
    b.ended_at = Some(ts("11:30:00"));
    b.messages = vec![message(
        "u1",
        &ts("11:00:00"),
        "user",
        "How does the matcher handle renames?",
    )];
    b.usage = vec![usage("tc1", &ts("11:01:00"), "gpt-5.5", 10_000, 500)];
    b.rate_limits = vec![
        RateLimitRecord {
            ts: ts("11:00:30"),
            limit_id: "codex".into(),
            window_kind: "primary".into(),
            window_minutes: Some(300),
            used_percent: 1.0,
            resets_at: Some(ts("15:00:00")),
            plan_type: None,
            credits_balance: None,
        },
        RateLimitRecord {
            ts: ts("11:01:00"),
            limit_id: "codex".into(),
            window_kind: "primary".into(),
            window_minutes: Some(300),
            used_percent: 4.0,
            resets_at: Some(ts("15:00:00")),
            plan_type: None,
            credits_balance: None,
        },
    ];
    store_session_record(&conn, &b)?;
    refresh_session_roots(&conn)?;
    crate::insights::refresh(&conn)?;
    Ok(conn)
}

fn app() -> Result<App> {
    let loader = InlineLoader::new(seeded()?);
    Ok(App::new(
        Box::new(loader),
        Filters::default(),
        "Test Analytics",
    ))
}

fn key(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

fn render(app: &App, width: u16, height: u16) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("terminal");
    terminal.draw(|frame| ui::draw(frame, app)).expect("draw");
    let buffer = terminal.backend().buffer().clone();
    let mut out = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}

#[test]
fn overview_renders_spend_quota_and_top_sessions() -> Result<()> {
    let app = app()?;
    let screen = render(&app, 140, 44);
    assert!(screen.contains("Spend & tokens"), "{screen}");
    assert!(screen.contains("Quota windows"));
    assert!(screen.contains("Top sessions"));
    assert!(screen.contains("Fix the token parser"));
    assert!(screen.contains("1%→4%"), "observed codex window: {screen}");
    Ok(())
}

#[test]
fn drill_into_session_and_back_restores_list_state() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    assert!(matches!(
        app.frame().screen,
        Screen::List {
            unit: Unit::Sessions,
            ..
        }
    ));
    key(&mut app, KeyCode::Char('s'));
    key(&mut app, KeyCode::Char('j'));
    let selected = app.frame().selected;
    let sort = app.sort_label();
    key(&mut app, KeyCode::Enter);
    assert!(matches!(app.frame().screen, Screen::Detail { .. }));
    let screen = render(&app, 140, 40);
    assert!(screen.contains("Conversation"));
    key(&mut app, KeyCode::Tab);
    key(&mut app, KeyCode::Tab);
    assert!(matches!(
        app.frame().screen,
        Screen::Detail {
            tab: DetailTab::Agents,
            ..
        }
    ));
    let agents = render(&app, 140, 40);
    assert!(
        agents.contains("● main") || agents.contains("agent-1"),
        "{agents}"
    );
    key(&mut app, KeyCode::Esc);
    assert!(matches!(app.frame().screen, Screen::List { .. }));
    assert_eq!(app.frame().selected, selected);
    assert_eq!(app.sort_label(), sort);
    Ok(())
}

#[test]
fn groups_drill_inherits_filters_and_tasks_unit() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('3'));
    key(&mut app, KeyCode::Char('g'));
    key(&mut app, KeyCode::Char('g'));
    key(&mut app, KeyCode::Char('g')); // tool
    let screen = render(&app, 140, 40);
    assert!(
        screen.contains("claude") && screen.contains("codex"),
        "{screen}"
    );
    key(&mut app, KeyCode::Enter);
    assert!(matches!(
        app.frame().screen,
        Screen::List {
            unit: Unit::Tasks,
            ..
        }
    ));
    assert_eq!(app.frame().filters.providers.len(), 1);
    assert!(app.frame().filters.from.is_some(), "time filter inherited");
    key(&mut app, KeyCode::Esc);
    assert!(matches!(app.frame().screen, Screen::Groups { .. }));
    Ok(())
}

#[test]
fn usage_windows_and_contributors() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('4'));
    key(&mut app, KeyCode::Tab);
    assert!(matches!(
        app.frame().screen,
        Screen::Usage {
            tab: UsageTab::Windows,
            ..
        }
    ));
    let windows = render(&app, 140, 40);
    assert!(windows.contains("primary 5h"), "{windows}");
    assert!(
        windows.contains("estimated"),
        "claude window is estimated: {windows}"
    );
    // Select the observed codex window.
    for _ in 0..5 {
        if let Some(super::data::Data::Windows(rows)) = app.frame().data.as_ref()
            && rows.get(app.frame().selected).is_some_and(|w| w.observed)
        {
            break;
        }
        key(&mut app, KeyCode::Char('j'));
    }
    key(&mut app, KeyCode::Enter);
    assert!(matches!(app.frame().screen, Screen::Window { .. }));
    let detail = render(&app, 140, 40);
    assert!(detail.contains("≈3.0pp"), "{detail}");
    key(&mut app, KeyCode::Enter);
    assert!(matches!(app.frame().screen, Screen::Detail { .. }));
    Ok(())
}

#[test]
fn context_tab_and_explain_popup() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('4'));
    key(&mut app, KeyCode::Tab);
    key(&mut app, KeyCode::Tab);
    assert!(matches!(
        app.frame().screen,
        Screen::Usage {
            tab: UsageTab::Context,
            ..
        }
    ));
    let screen = render(&app, 150, 40);
    assert!(
        screen.contains("Bash") || screen.contains("baseline"),
        "{screen}"
    );
    key(&mut app, KeyCode::Char('x'));
    assert!(app.popup.is_some());
    let popup = render(&app, 150, 40);
    assert!(popup.contains("Write"), "{popup}");
    key(&mut app, KeyCode::Esc);
    assert!(app.popup.is_none());
    Ok(())
}

#[test]
fn mark_and_compare_tasks() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Char('u'));
    key(&mut app, KeyCode::Char('m'));
    key(&mut app, KeyCode::Char('j'));
    key(&mut app, KeyCode::Char('m'));
    assert_eq!(app.marked.len(), 2);
    key(&mut app, KeyCode::Char('c'));
    assert!(matches!(app.frame().screen, Screen::Compare { .. }));
    let screen = render(&app, 150, 40);
    assert!(screen.contains("Verification in first ask"), "{screen}");
    assert!(screen.contains("Cache-read ratio"));
    Ok(())
}

#[test]
fn filter_editor_and_search_apply_to_current_view() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Char('/'));
    for c in "parser".chars() {
        key(&mut app, KeyCode::Char(c));
    }
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.frame().filters.search.as_deref(), Some("parser"));
    let rows = match app.frame().data.as_ref() {
        Some(super::data::Data::Sessions(rows)) => rows.len(),
        _ => 0,
    };
    assert_eq!(rows, 1);
    key(&mut app, KeyCode::Char('f'));
    key(&mut app, KeyCode::Char('j')); // Tools
    key(&mut app, KeyCode::Enter); // claude
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.frame().filters.providers, vec!["claude".to_string()]);
    Ok(())
}

#[test]
fn practices_and_delivery_screens_render() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('6'));
    let practices = render(&app, 150, 40);
    assert!(
        practices.contains("Verification named in the first request"),
        "{practices}"
    );
    key(&mut app, KeyCode::Char('5'));
    let delivery = render(&app, 150, 40);
    assert!(delivery.contains("Mainline"), "{delivery}");
    assert!(
        delivery.contains("Rework") && delivery.contains("corrections"),
        "{delivery}"
    );
    Ok(())
}

#[test]
fn narrow_terminal_still_renders() -> Result<()> {
    let mut app = app()?;
    let _ = render(&app, 80, 24);
    key(&mut app, KeyCode::Char('2'));
    let screen = render(&app, 80, 24);
    assert!(screen.contains("Sessions"));
    Ok(())
}

#[test]
fn edit_popup_saves_task_label_override() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Tab);
    assert!(matches!(
        app.frame().screen,
        Screen::Detail {
            tab: DetailTab::Tasks,
            ..
        }
    ));
    key(&mut app, KeyCode::Char('e'));
    let popup = render(&app, 140, 44);
    assert!(
        popup.contains("intent") && popup.contains("completed at"),
        "{popup}"
    );
    // outcome field → "stalled", save.
    key(&mut app, KeyCode::Char('j'));
    while !matches!(
        &app.popup,
        Some(super::app::Popup::Edit { target, field, choice })
            if target.fields()[*field].1[*choice] == "stalled"
    ) {
        key(&mut app, KeyCode::Right);
    }
    key(&mut app, KeyCode::Enter);
    assert!(app.popup.is_none());
    let message = app.message.clone().unwrap_or_default();
    assert_eq!(message, "outcome = stalled (your label)");
    let screen = render(&app, 140, 44);
    assert!(screen.contains('✎'), "override glyph: {message} {screen}");
    Ok(())
}

#[test]
fn edit_popup_on_turn_starts_a_new_task() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Enter);
    let before = match app.frame().data.as_ref() {
        Some(super::data::Data::Detail(Some(detail))) => detail.tasks.len(),
        _ => 0,
    };
    key(&mut app, KeyCode::Char('j'));
    key(&mut app, KeyCode::Char('e'));
    key(&mut app, KeyCode::Char('j'));
    // "starts task" options: T1.., new task, clear → pick "new task".
    let Some(super::app::Popup::Edit { target, .. }) = app.popup.clone() else {
        panic!("edit popup expected");
    };
    let options = target.fields()[1].1.clone();
    let new_task = options.iter().position(|o| o == "new task").unwrap();
    for _ in 0..new_task {
        key(&mut app, KeyCode::Right);
    }
    key(&mut app, KeyCode::Enter);
    let after = match app.frame().data.as_ref() {
        Some(super::data::Data::Detail(Some(detail))) => detail.tasks.len(),
        _ => 0,
    };
    assert_eq!(after, before + 1, "{:?}", app.message);
    Ok(())
}

#[test]
fn classify_popup_estimates_then_reports_missing_key() -> Result<()> {
    let _lock = crate::test_support::lock_env();
    let home = tempfile::tempdir()?;
    let _home = crate::test_support::ScopedEnvVar::set("VCA_HOME", home.path());
    let _key = crate::test_support::ScopedEnvVar::unset("OPENROUTER_API_KEY");
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Char('K'));
    let Some(super::app::Popup::Classify { plan, .. }) = app.popup.clone() else {
        panic!("classify popup expected");
    };
    let plan = plan.expect("plan computed");
    assert!(plan.est_tokens > 0);
    let screen = render(&app, 140, 44);
    assert!(
        screen.contains("Classify with Jev") && screen.contains("conversation"),
        "{screen}"
    );
    key(&mut app, KeyCode::Enter);
    let Some(super::app::Popup::Classify { error, .. }) = app.popup.clone() else {
        panic!("popup stays open on error");
    };
    assert!(error.unwrap_or_default().contains("No OpenRouter key"));
    key(&mut app, KeyCode::Esc);
    assert!(app.popup.is_none());
    Ok(())
}

#[test]
fn conversation_shows_stages_and_practices_show_habits() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Enter);
    let conversation = render(&app, 160, 40);
    assert!(
        conversation.contains("Stage") && conversation.contains("Asked for"),
        "{conversation}"
    );
    assert!(
        conversation.contains(" h"),
        "heuristic stage marker: {conversation}"
    );
    key(&mut app, KeyCode::Char('6'));
    let practices = render(&app, 160, 50);
    assert!(
        practices.contains("Plan first, and do it yourself"),
        "{practices}"
    );
    assert!(
        practices.contains("Write tests that can fail"),
        "{practices}"
    );
    Ok(())
}

#[test]
fn edit_popup_sets_turn_stage() -> Result<()> {
    let mut app = app()?;
    key(&mut app, KeyCode::Char('2'));
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Char('e'));
    for _ in 0..2 {
        key(&mut app, KeyCode::Char('j'));
    }
    while !matches!(
        &app.popup,
        Some(super::app::Popup::Edit { target, field, choice })
            if target.fields()[*field].1[*choice] == "review"
    ) {
        key(&mut app, KeyCode::Right);
    }
    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.message.as_deref(),
        Some("turn_stage = review (your label)")
    );
    let screen = render(&app, 160, 40);
    assert!(screen.contains("review✎"), "{screen}");
    Ok(())
}
