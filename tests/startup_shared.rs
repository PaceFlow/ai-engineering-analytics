// Executed separately by both packages against their own installed command.
use assert_cmd::Command;
use rusqlite::{Connection, params};
use std::path::Path;
use tempfile::TempDir;

fn binary_name() -> &'static str {
    if env!("CARGO_PKG_NAME") == "vibe-coding-analytics" { "vca" } else { "paceflow" }
}

fn command(home: &Path) -> Command {
    let mut command = Command::cargo_bin(binary_name()).unwrap();
    command.current_dir(home)
        .env("VCA_HOME", home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env_remove("PACEFLOW_GITHUB_TOKEN")
        .env_remove("VCA_GITHUB_TOKEN")
        .env_remove("PACEFLOW_CURSOR_STATE_PATH")
        .env_remove("PACEFLOW_CURSOR_HISTORY_PATH")
        .env_remove("VCA_CURSOR_STATE_PATH")
        .env_remove("VCA_CURSOR_HISTORY_PATH")
        .env("VCA_OPENCODE_DB_PATH", home.join("missing-opencode.db"))
        .env("PACEFLOW_OPENCODE_DB_PATH", home.join("missing-opencode.db"));
    command
}

fn legacy(home: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(home.join(".paceflow"))?;
    let conn = Connection::open(home.join(".paceflow/paceflow.db"))?;
    vca::db::init_app_schema(&conn)?;
    conn.execute("INSERT INTO metadata_sessions(provider, session_id, started_at) VALUES ('codex', 'previous-session', '2026-09-30')", [])?;
    conn.execute("INSERT INTO fact_sync_event_state VALUES ('org', 'session', 'event', 'hash', '2026-09-30', 'checkpoint')", [])?;
    Ok(())
}

#[test]
fn help_and_version_have_no_storage_side_effects() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    legacy(home.path())?;
    let output = command(home.path()).arg("--help").assert().success().get_output().stdout.clone();
    let help = String::from_utf8(output)?;
    assert!(help.contains(&format!("Usage: {}", binary_name())));
    command(home.path()).arg("--version").assert().success();
    for subcommand in ["ingest", "session", "delivery", "quality", "cost", "github", "event-stream", "tui"] {
        let output = command(home.path()).args([subcommand, "--help"]).assert().success().get_output().stdout.clone();
        let help = String::from_utf8(output)?;
        if binary_name() == "vca" { assert!(!help.to_lowercase().contains("paceflow")); }
        if binary_name() == "paceflow" { assert!(!help.contains("vca ")); }
    }
    assert!(!home.path().join(".vca").exists());
    if binary_name() == "vca" {
        assert!(!help.to_lowercase().contains("paceflow"));
        command(home.path()).arg("sync").assert().failure();
        command(home.path()).arg("hooks").assert().failure();
        assert!(!home.path().join(".vca").exists());
    }
    Ok(())
}

#[test]
fn first_report_imports_paceflow_history_without_ingestion_or_backend_access() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    legacy(home.path())?;
    let original = std::fs::read(home.path().join(".paceflow/paceflow.db"))?;
    command(home.path()).args(["session", "--all-projects"])
        .env("PACEFLOW_SYNC_BASE_URL", "http://127.0.0.1:1")
        .env("PACEFLOW_SYNC_TOKEN", "must-not-be-used")
        .env("PACEFLOW_SYNC_ORGANIZATION_ID", "org")
        .assert().success();
    let conn = Connection::open(home.path().join(".vca/vca.db"))?;
    let session: String = conn.query_row("SELECT session_id FROM metadata_sessions", [], |row| row.get(0))?;
    assert_eq!(session, "previous-session");
    let cursor: String = conn.query_row("SELECT last_server_checkpoint FROM fact_sync_event_state", [], |row| row.get(0))?;
    assert_eq!(cursor, "checkpoint");
    assert_eq!(std::fs::read(home.path().join(".paceflow/paceflow.db"))?, original);
    command(home.path()).args(["quality", "--all-projects"]).assert().success();
    assert!(!home.path().join(".vca/sync_config.json").exists());
    Ok(())
}

#[test]
fn fresh_rebuild_clears_shared_history_and_cursors_without_reimporting_legacy() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    legacy(home.path())?;
    std::fs::write(home.path().join(".paceflow/github_token"), "saved-token")?;
    command(home.path()).args(["session", "--all-projects"]).assert().success();
    // No repositories or provider history exist here, so no GitHub requests are needed.
    std::fs::write(home.path().join(".paceflow/sync_config.json"), "preserved-backend-config")?;
    command(home.path()).args(["ingest", "--fresh"]).assert().success();
    command(home.path()).args(["session", "--all-projects"]).assert().success();
    let conn = Connection::open(home.path().join(".vca/vca.db"))?;
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM metadata_sessions WHERE session_id = ?1", params!["previous-session"], |r| r.get::<_, i64>(0))?, 0);
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM fact_sync_event_state", [], |r| r.get::<_, i64>(0))?, 0);
    assert!(home.path().join(".paceflow/paceflow.db").exists());
    assert_eq!(std::fs::read_to_string(home.path().join(".vca/github_token"))?, "saved-token");
    assert_eq!(std::fs::read_to_string(home.path().join(".paceflow/sync_config.json"))?, "preserved-backend-config");
    assert_eq!(std::fs::read_to_string(home.path().join(".paceflow/github_token"))?, "saved-token");
    Ok(())
}

#[test]
fn startup_honors_legacy_home_and_isolates_an_explicit_shared_home() -> anyhow::Result<()> {
    let old = TempDir::new()?;
    let isolated = TempDir::new()?;
    legacy(old.path())?;
    command(isolated.path()).args(["session", "--all-projects"])
        .env("PACEFLOW_HOME", old.path()).assert().success();
    assert!(!old.path().join(".vca").exists());
    let conn = Connection::open(isolated.path().join(".vca/vca.db"))?;
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM metadata_sessions", [], |r| r.get::<_, i64>(0))?, 0);
    command(old.path()).args(["session", "--all-projects"])
        .env_remove("VCA_HOME").env("PACEFLOW_HOME", old.path()).assert().success();
    assert!(old.path().join(".vca/vca.db").exists());
    Ok(())
}
