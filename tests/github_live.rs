use ai_engineering_analytics::analytics;
use ai_engineering_analytics::cli::ReportArgs;
use ai_engineering_analytics::db;
use ai_engineering_analytics::github;
use anyhow::Result;
use rusqlite::{Connection, params};
use std::ffi::{OsStr, OsString};
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

const LIVE_REPO_ROOT: &str = "/tmp/paceflow-github-live";
const LIVE_REPO_KEY: &str = "git:github.com/PaceFlow/ai-engineering-analytics";
const LIVE_COMMIT_SHA: &str = "0c42afacd5ce7f20e666f77682e6de10e09f1508";
const LIVE_EXPECTED_PR: i64 = 3;
const LIVE_EXPECTED_MERGED: bool = true;
const LIVE_NO_PR_COMMIT_SHA: Option<&str> = Some("17d0dd8df07dcd0e1ae8c8c285e344e857761ee5");

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env() -> MutexGuard<'static, ()> {
    env_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ScopedEnvVar {
    key: &'static str,
    original: Option<OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

#[derive(Debug)]
struct LiveGitHubConfig {
    token: String,
    repo_key: String,
    commit_sha: String,
    expected_pr: i64,
    expected_merged: bool,
    no_pr_commit_sha: Option<String>,
}

impl LiveGitHubConfig {
    fn from_env() -> Result<Option<Self>> {
        let Some(token) = required_env("PACEFLOW_GITHUB_TOKEN") else {
            return Ok(None);
        };

        Ok(Some(Self {
            token,
            repo_key: LIVE_REPO_KEY.to_string(),
            commit_sha: LIVE_COMMIT_SHA.to_string(),
            expected_pr: LIVE_EXPECTED_PR,
            expected_merged: LIVE_EXPECTED_MERGED,
            no_pr_commit_sha: LIVE_NO_PR_COMMIT_SHA.map(str::to_string),
        }))
    }
}

struct LiveGitHubFixture {
    _env_guard: MutexGuard<'static, ()>,
    _tempdir: TempDir,
    _paceflow_home: ScopedEnvVar,
    _github_token: ScopedEnvVar,
    conn: Connection,
    config: LiveGitHubConfig,
    summary: github::types::GitHubSyncSummary,
}

impl LiveGitHubFixture {
    fn load() -> Result<Option<Self>> {
        let env_guard = lock_env();
        let Some(config) = LiveGitHubConfig::from_env()? else {
            eprintln!("{}", missing_live_test_env_message());
            return Ok(None);
        };

        let tempdir = TempDir::new()?;
        let paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let github_token = ScopedEnvVar::set("PACEFLOW_GITHUB_TOKEN", &config.token);

        let mut conn = db::open()?;
        seed_github_candidate_commit(&conn, &config.repo_key, &config.commit_sha, 0)?;
        if let Some(no_pr_commit_sha) = config.no_pr_commit_sha.as_deref() {
            seed_github_candidate_commit(&conn, &config.repo_key, no_pr_commit_sha, 1)?;
        }

        let summary = github::sync::sync_github_pull_requests(&mut conn, false, None)?;

        Ok(Some(Self {
            _env_guard: env_guard,
            _tempdir: tempdir,
            _paceflow_home: paceflow_home,
            _github_token: github_token,
            conn,
            config,
            summary,
        }))
    }
}

#[test]
#[ignore = "requires live GitHub token"]
fn github_live_commit_lookup_and_pr_state_match_expected_fixture() -> Result<()> {
    let Some(fixture) = LiveGitHubFixture::load()? else {
        return Ok(());
    };

    let conn = &fixture.conn;
    let config = &fixture.config;

    assert_eq!(fixture.summary.repos_considered, 1);
    assert!(fixture.summary.commit_lookups_completed >= 1);

    let lookup: (String, Option<i64>) = fixture.conn.query_row(
        "SELECT status, owning_pr_number
         FROM fact_github_commit_pr_lookup
         WHERE repo_key = ?1 AND commit_sha = ?2",
        params![config.repo_key, config.commit_sha],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(lookup.0, "resolved");
    assert_eq!(lookup.1, Some(config.expected_pr));

    let pr_outcome: (Option<i64>, i64, i64) = conn.query_row(
        "SELECT pr_number, pr_opened_flag, pr_merged_flag
         FROM event_commit_pr_outcome
         WHERE repo_root = ?1 AND commit_sha = ?2",
        params![LIVE_REPO_ROOT, config.commit_sha],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(pr_outcome.0, Some(config.expected_pr));
    assert_eq!(pr_outcome.1, 1);
    assert_eq!(pr_outcome.2, i64::from(config.expected_merged));

    let merged_at: Option<String> = conn.query_row(
        "SELECT merged_at
         FROM fact_github_pull_request
         WHERE repo_key = ?1 AND pr_number = ?2",
        params![config.repo_key, config.expected_pr],
        |row| row.get(0),
    )?;
    assert_eq!(merged_at.is_some(), config.expected_merged);

    if let Some(no_pr_commit_sha) = config.no_pr_commit_sha.as_deref() {
        let no_pr_lookup: (String, Option<i64>) = fixture.conn.query_row(
            "SELECT status, owning_pr_number
             FROM fact_github_commit_pr_lookup
             WHERE repo_key = ?1 AND commit_sha = ?2",
            params![config.repo_key, no_pr_commit_sha],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(no_pr_lookup.0, "no_pr");
        assert_eq!(no_pr_lookup.1, None);

        let no_pr_outcome: (String, i64, i64) = conn.query_row(
            "SELECT lookup_status, pr_opened_flag, pr_merged_flag
             FROM event_commit_pr_outcome
             WHERE repo_root = ?1 AND commit_sha = ?2",
            params![LIVE_REPO_ROOT, no_pr_commit_sha],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(no_pr_outcome.0, "no_pr");
        assert_eq!(no_pr_outcome.1, 0);
        assert_eq!(no_pr_outcome.2, 0);
    }

    Ok(())
}

#[test]
#[ignore = "requires live GitHub token"]
fn github_live_change_report_exposes_c1_and_c3_for_synced_commits() -> Result<()> {
    let Some(fixture) = LiveGitHubFixture::load()? else {
        return Ok(());
    };

    let rows = query_live_change_report(&fixture.conn)?;

    assert_eq!(rows.len(), 1);
    assert!(rows[0].github_pr_metrics_available);
    assert_eq!(rows[0].pr_reach_rate.numerator, 1);
    assert_eq!(
        rows[0].pr_reach_rate.denominator,
        if fixture.config.no_pr_commit_sha.is_some() {
            2
        } else {
            1
        }
    );
    assert_eq!(
        rows[0].pr_merge_rate.numerator,
        i64::from(fixture.config.expected_merged)
    );
    assert_eq!(rows[0].pr_merge_rate.denominator, 1);

    Ok(())
}

fn query_live_change_report(conn: &Connection) -> Result<Vec<analytics::ChangeReportRow>> {
    analytics::create_reporting_views(conn)?;
    analytics::query_change_report(
        conn,
        &ReportArgs {
            weekly: false,
            group_by: None,
            from: None,
            to: None,
            repo: Some(LIVE_REPO_ROOT.to_string()),
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 10,
        },
    )
}

fn missing_live_test_env_message() -> &'static str {
    "skipping live GitHub test: set PACEFLOW_GITHUB_TOKEN"
}

fn required_env(key: &'static str) -> Option<String> {
    optional_env(key)
}

fn optional_env(key: &'static str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn seed_github_candidate_commit(
    conn: &Connection,
    repo_key: &str,
    commit_sha: &str,
    offset_minutes: i64,
) -> Result<()> {
    let commit_time = format!("2026-03-17T09:{offset_minutes:02}:00Z");
    conn.execute(
        "INSERT INTO event_commit_outcome (
            repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
            reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
         ) VALUES (?1, ?2, ?3, ?4, 1, 0, 0, 42, 60)",
        params![LIVE_REPO_ROOT, repo_key, commit_sha, commit_time],
    )?;
    conn.execute(
        "INSERT INTO event_commit_churn (
            repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
            ai_added_lines_removed_within_window, churn_window_days
         ) VALUES (?1, ?2, ?3, 0, 0, 14)",
        params![LIVE_REPO_ROOT, repo_key, commit_sha],
    )?;
    Ok(())
}
