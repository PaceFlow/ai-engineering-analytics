use assert_cmd::cargo::CommandCargoExt;
use insta::{assert_snapshot, assert_yaml_snapshot};
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/regression");
const HOME_TEMPLATE_CURSOR_DB: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/regression/home_template/cursor/state.vscdb"
);
const PACEFLOW_BUNDLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/regression/repos/paceflow.bundle"
);

struct TestEnv {
    _tempdir: TempDir,
    home: PathBuf,
    paceflow_repo: PathBuf,
    cursor_dir: PathBuf,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct SessionSummarySnapshot {
    sessions: String,
    average_user_prompts: String,
    avg_time_to_first_accepted_change_minutes: String,
    debug_loop_rate: String,
    s6_rate: String,
    s9_rate: String,
    no_output_session_rate: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ChangeSummarySnapshot {
    commits: String,
    heavy_commits: String,
    merge_rate: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct LifecycleSummarySnapshot {
    heavy_commits: String,
    code_churn_rate: String,
    revert_rate: String,
}

impl TestEnv {
    fn new_live_fixture() -> anyhow::Result<Self> {
        let tempdir = TempDir::new()?;
        let home = tempdir.path().to_path_buf();
        let work_dir = home.join("work");
        let paceflow_repo = work_dir.join("fixture-paceflow");
        let cursor_dir = work_dir.join("fixture-cursor");

        fs::create_dir_all(&work_dir)?;
        fs::create_dir_all(&cursor_dir)?;

        materialize_paceflow_repo(&paceflow_repo)?;
        copy_codex_sessions(
            &home,
            &["__REPO_PACEFLOW__", "__REPO_CURSOR__", "__HOME__"],
            &[
                &paceflow_repo.to_string_lossy(),
                &cursor_dir.to_string_lossy(),
                &home.to_string_lossy(),
            ],
        )?;
        install_cursor_fixture(&home, &paceflow_repo, &cursor_dir)?;

        Ok(Self {
            _tempdir: tempdir,
            home,
            paceflow_repo,
            cursor_dir,
        })
    }

    fn new_seeded_reporting() -> anyhow::Result<Self> {
        let tempdir = TempDir::new()?;
        let home = tempdir.path().to_path_buf();
        let work_dir = home.join("work");
        let paceflow_repo = work_dir.join("fixture-paceflow");
        let cursor_dir = work_dir.join("fixture-cursor");

        fs::create_dir_all(&paceflow_repo)?;
        fs::create_dir_all(&cursor_dir)?;

        let env = Self {
            _tempdir: tempdir,
            home,
            paceflow_repo,
            cursor_dir,
        };
        env.initialize_db_schema()?;
        env.seed_reporting_fixture()?;
        Ok(env)
    }

    fn run_paceflow(&self, args: &[&str]) -> anyhow::Result<String> {
        let output = Command::cargo_bin("paceflow")?
            .args(args)
            .current_dir(&self.home)
            .env("PACEFLOW_HOME", &self.home)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env_remove("HOMEDRIVE")
            .env_remove("HOMEPATH")
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("PACEFLOW_GITHUB_TOKEN")
            .env_remove("PACEFLOW_GITHUB_TEST_REPO_KEY")
            .env_remove("PACEFLOW_GITHUB_TEST_COMMIT_SHA")
            .env_remove("PACEFLOW_GITHUB_TEST_EXPECTED_PR")
            .env_remove("PACEFLOW_GITHUB_TEST_EXPECTED_MERGED")
            .env_remove("PACEFLOW_GITHUB_TEST_NO_PR_COMMIT_SHA")
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "paceflow {:?} failed\nstdout:\n{}\nstderr:\n{}",
                args,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8(output.stdout)?)
    }

    fn ingest(&self) -> anyhow::Result<()> {
        self.run_paceflow(&["ingest"])?;
        Ok(())
    }

    fn initialize_db_schema(&self) -> anyhow::Result<()> {
        let _ = self.run_paceflow(&["session"])?;
        Ok(())
    }

    fn db_path(&self) -> PathBuf {
        self.home.join(".paceflow").join("paceflow.db")
    }

    fn seed_reporting_fixture(&self) -> anyhow::Result<()> {
        fs::create_dir_all(self.home.join(".paceflow"))?;
        let conn = Connection::open(self.db_path())?;
        let repo_root = "__REPO_PACEFLOW__";

        let sessions = [
            (
                "codex",
                "019cb311-b93e-7423-aeb4-81249c578638",
                Some(repo_root),
                "codex/gpt-5.3-codex",
                "2026-03-03T09:40:09.156Z",
                "2026-03-03T16:10:00.000Z",
                117,
                0,
                1,
                1,
                Some("2026-03-03T15:22:37.117Z"),
                Some(342.46601678431034),
                1,
            ),
            (
                "cursor",
                "b052dae4-a1d2-43f0-a3ba-6faf6a40489e",
                None,
                "cursor/default",
                "2026-03-05T07:02:58+00:00",
                "2026-03-05T07:05:00+00:00",
                1,
                0,
                0,
                1,
                Some("2026-03-05T07:02:58+00:00"),
                Some(0.0),
                0,
            ),
            (
                "codex",
                "019cd678-8f72-7b43-a07f-d49cdbbb81c9",
                Some(repo_root),
                "codex/gpt-5.3-codex",
                "2026-03-10T06:39:11.222Z",
                "2026-03-10T12:00:00.000Z",
                15,
                0,
                1,
                1,
                Some("2026-03-10T11:37:27.402Z"),
                Some(298.26966628432274),
                1,
            ),
        ];

        for (
            provider,
            session_id,
            repo_root,
            model_name,
            started_at,
            ended_at,
            user_turn_count,
            debug_loop_flag,
            mid_session_error_paste_flag,
            accepted_output_flag,
            first_accepted_change_at,
            minutes_to_first_accepted_change,
            session_commit_within_4h_flag,
        ) in sessions
        {
            conn.execute(
                "INSERT INTO event_session_quality (
                    provider, session_id, repo_root, model_name, started_at, ended_at, user_turn_count,
                    debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                    first_accepted_change_at, minutes_to_first_accepted_change, session_commit_within_4h_flag
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                (
                    provider,
                    session_id,
                    repo_root,
                    model_name,
                    started_at,
                    ended_at,
                    user_turn_count,
                    debug_loop_flag,
                    mid_session_error_paste_flag,
                    accepted_output_flag,
                    first_accepted_change_at,
                    minutes_to_first_accepted_change,
                    session_commit_within_4h_flag,
                ),
            )?;
        }

        let commits = [
            (
                "de8ab41b5cec40dd3dc63fc8bd0ca743cea1d46c",
                "2026-03-03T09:37:41.000Z",
                0,
                1,
                0,
                135,
                2283,
                135,
                37,
                "staging",
                "staging",
                1,
                0.5,
            ),
            (
                "17a3ee9fd5e70b92092dd9ec160a63cd5fb01b1d",
                "2026-03-05T08:40:50.000Z",
                0,
                1,
                0,
                1096,
                3003,
                1091,
                42,
                "staging",
                "staging",
                1,
                0.5,
            ),
            (
                "2dcb057965747739cea8bb891a260c347be8cb39",
                "2026-03-06T08:24:52.000Z",
                1,
                1,
                0,
                1515,
                1573,
                1505,
                24,
                "staging",
                "staging",
                1,
                0.5,
            ),
            (
                "f3d68adc96fbfb533c2e76cda51ca174f6d7954f",
                "2026-03-10T08:33:33.000Z",
                1,
                1,
                0,
                1306,
                1374,
                1225,
                21,
                "staging",
                "staging",
                1,
                0.5,
            ),
            (
                "75c5d97151a6a681a26737f88ec4cc1e3ad7fe23",
                "2026-03-10T11:54:22.000Z",
                0,
                0,
                0,
                0,
                2,
                0,
                0,
                "PAC-999",
                "codex/PAC-999-task-stats-demo",
                0,
                1.0,
            ),
            (
                "9ffe1bf647c827de2b2def4ccfb32ce7cee1b207",
                "2026-03-11T13:44:10.000Z",
                1,
                1,
                0,
                1336,
                1466,
                1276,
                14,
                "main",
                "main",
                1,
                0.5,
            ),
            (
                "47d03452796ad3cda5c12edeb61a28965e76b4bd",
                "2026-03-12T06:20:19.000Z",
                0,
                1,
                0,
                5,
                49,
                1,
                0,
                "main",
                "main",
                1,
                0.5,
            ),
            (
                "77abc1af8a5af3614fbd7d6639bfc9ef557b8f29",
                "2026-03-13T16:06:41.000Z",
                0,
                1,
                0,
                0,
                1055,
                0,
                0,
                "main",
                "main",
                1,
                0.5,
            ),
            (
                "32dfa74d2e2bf9c06c815b31c9de01037db5ffbf",
                "2026-03-13T16:17:59.000Z",
                0,
                1,
                0,
                28,
                107,
                28,
                0,
                "main",
                "main",
                1,
                0.5,
            ),
        ];

        for (
            commit_sha,
            commit_time,
            heavy_ai_flag,
            merged_to_mainline_flag,
            reverted_later_flag,
            total_matched_ai_lines,
            commit_total_changed_lines,
            ai_added_lines_reaching_mainline,
            ai_added_lines_removed_within_window,
            task_key,
            branch_name,
            fallback_flag,
            confidence,
        ) in commits
        {
            conn.execute(
                "INSERT INTO event_commit_outcome (
                    repo_root, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                    reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                (
                    repo_root,
                    commit_sha,
                    commit_time,
                    heavy_ai_flag,
                    merged_to_mainline_flag,
                    reverted_later_flag,
                    total_matched_ai_lines,
                    commit_total_changed_lines,
                ),
            )?;
            conn.execute(
                "INSERT INTO event_commit_churn (
                    repo_root, commit_sha, ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window, churn_window_days
                 ) VALUES (?1, ?2, ?3, ?4, 14)",
                (
                    repo_root,
                    commit_sha,
                    ai_added_lines_reaching_mainline,
                    ai_added_lines_removed_within_window,
                ),
            )?;
            conn.execute(
                "INSERT INTO event_task_commit (
                    repo_root, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    repo_root,
                    task_key,
                    branch_name,
                    commit_sha,
                    fallback_flag,
                    confidence,
                    commit_time,
                ),
            )?;
        }

        let commit_sessions = [
            (
                "de8ab41b5cec40dd3dc63fc8bd0ca743cea1d46c",
                "codex",
                "019cb311-b93e-7423-aeb4-81249c578638",
                "codex/gpt-5.3-codex",
                133.66666666666666,
                0.05854869323988903,
                0.9901234567901234,
            ),
            (
                "de8ab41b5cec40dd3dc63fc8bd0ca743cea1d46c",
                "codex",
                "019cd678-8f72-7b43-a07f-d49cdbbb81c9",
                "codex/gpt-5.3-codex",
                1.3333333333333333,
                0.0005840268652358008,
                0.009876543209876543,
            ),
        ];

        for (
            commit_sha,
            provider,
            session_id,
            model_name,
            matched_lines,
            share_of_commit,
            share_of_ai,
        ) in commit_sessions
        {
            conn.execute(
                "INSERT INTO event_commit_session (
                    repo_root, commit_sha, provider, session_id, commit_time, model_name,
                    matched_lines, share_of_commit, share_of_ai
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                (
                    repo_root,
                    commit_sha,
                    provider,
                    session_id,
                    "2026-03-03T09:37:41.000Z",
                    model_name,
                    matched_lines,
                    share_of_commit,
                    share_of_ai,
                ),
            )?;
        }

        Ok(())
    }

    fn normalize_output(&self, output: String) -> String {
        let mut normalized = output;
        let mut replacements = Vec::new();
        for (path, placeholder) in [
            (&self.paceflow_repo, "__REPO_PACEFLOW__"),
            (&self.cursor_dir, "__REPO_CURSOR__"),
            (&self.home, "__HOME__"),
        ] {
            replacements.extend(
                path_variants(path)
                    .into_iter()
                    .map(|variant| (variant, placeholder)),
            );
        }
        replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
        for (variant, placeholder) in replacements {
            normalized = normalized.replace(&variant, placeholder);
        }
        normalized
    }
}

#[test]
fn category_reports_match_snapshots() -> anyhow::Result<()> {
    let env = TestEnv::new_seeded_reporting()?;

    let session = env.normalize_output(env.run_paceflow(&["session"])?);
    let change = env.normalize_output(env.run_paceflow(&["delivery"])?);
    let lifecycle = env.normalize_output(env.run_paceflow(&["quality"])?);

    assert!(session.contains("Session Metrics"));
    assert!(change.contains("Delivery Metrics"));
    assert!(lifecycle.contains("Quality Metrics"));

    let structured_session = parse_session_summary(&session)?;
    let structured_change = parse_change_summary(&change)?;
    let structured_lifecycle = parse_lifecycle_summary(&lifecycle)?;

    assert_snapshot!("fixture_corpus_session_report_text", session);
    assert_snapshot!("fixture_corpus_change_report_text", change);
    assert_snapshot!("fixture_corpus_lifecycle_report_text", lifecycle);
    assert_yaml_snapshot!(
        "fixture_corpus_session_report_structured",
        structured_session
    );
    assert_yaml_snapshot!("fixture_corpus_change_report_structured", structured_change);
    assert_yaml_snapshot!(
        "fixture_corpus_lifecycle_report_structured",
        structured_lifecycle
    );

    Ok(())
}

#[test]
fn grouped_and_weekly_reports_match_snapshots() -> anyhow::Result<()> {
    let env = TestEnv::new_seeded_reporting()?;

    let session_grouped =
        env.normalize_output(env.run_paceflow(&["session", "--group-by", "provider"])?);
    let change_grouped =
        env.normalize_output(env.run_paceflow(&["delivery", "--group-by", "repo"])?);
    let lifecycle_grouped =
        env.normalize_output(env.run_paceflow(&["quality", "--group-by", "repo"])?);
    let session_weekly = env.normalize_output(env.run_paceflow(&[
        "session",
        "--weekly",
        "--group-by",
        "provider",
    ])?);

    assert_snapshot!(
        "fixture_corpus_session_grouped_by_provider_text",
        session_grouped
    );
    assert_snapshot!("fixture_corpus_change_grouped_by_repo_text", change_grouped);
    assert_snapshot!(
        "fixture_corpus_lifecycle_grouped_by_repo_text",
        lifecycle_grouped
    );
    assert_snapshot!(
        "fixture_corpus_session_weekly_by_provider_text",
        session_weekly
    );

    Ok(())
}

#[test]
fn event_stream_matches_snapshots() -> anyhow::Result<()> {
    let env = TestEnv::new_seeded_reporting()?;

    let session_stream =
        env.normalize_output(env.run_paceflow(&["event-stream", "--stream", "session-base"])?);
    let task_commit_stream = env.normalize_output(env.run_paceflow(&[
        "event-stream",
        "--stream",
        "task-commit-base",
    ])?);
    let all_streams_smoke =
        env.normalize_output(env.run_paceflow(&["event-stream", "--limit", "5"])?);

    assert_snapshot!("fixture_corpus_event_stream_session_base", session_stream);
    assert_snapshot!(
        "fixture_corpus_event_stream_task_commit_base",
        task_commit_stream
    );
    assert_snapshot!("fixture_corpus_event_stream_all_smoke", all_streams_smoke);

    Ok(())
}

fn path_variants(path: &Path) -> Vec<String> {
    let mut variants = Vec::new();
    push_path_variant(&mut variants, &path.to_string_lossy());
    if let Ok(canonical) = fs::canonicalize(path) {
        push_path_variant(&mut variants, &canonical.to_string_lossy());
    }
    variants
}

fn push_path_variant(variants: &mut Vec<String>, path: &str) {
    for candidate in [
        path.to_string(),
        path.replace('\\', "/"),
        path.replace('/', "\\"),
    ] {
        if !candidate.is_empty() && !variants.contains(&candidate) {
            variants.push(candidate);
        }
    }
}

#[test]
fn ingest_is_idempotent_for_fixture_corpus() -> anyhow::Result<()> {
    let env = TestEnv::new_live_fixture()?;
    env.ingest()?;

    let session_before = env.run_paceflow(&["session"])?;
    let change_before = env.run_paceflow(&["delivery"])?;
    let lifecycle_before = env.run_paceflow(&["quality"])?;

    let second_ingest = env.run_paceflow(&["ingest"])?;
    assert!(second_ingest.contains("Ingest progress: 100%"));

    let session_after = env.run_paceflow(&["session"])?;
    let change_after = env.run_paceflow(&["delivery"])?;
    let lifecycle_after = env.run_paceflow(&["quality"])?;

    assert_eq!(
        session_before, session_after,
        "session output drifted after rerun"
    );
    assert_eq!(
        change_before, change_after,
        "change output drifted after rerun"
    );
    assert_eq!(
        lifecycle_before, lifecycle_after,
        "lifecycle output drifted after rerun"
    );

    Ok(())
}

#[test]
fn ingest_reports_commit_event_progress() -> anyhow::Result<()> {
    let env = TestEnv::new_live_fixture()?;
    let ingest_output = env.run_paceflow(&["ingest"])?;

    assert!(ingest_output.contains("Planning ingest..."));
    assert!(ingest_output.contains("Stage: codex sessions"));
    assert!(ingest_output.contains("Stage: commit association"));
    assert!(ingest_output.contains("Ingest progress: 100%"));
    assert!(ingest_output.contains("Materializing commit events ..."));
    assert!(ingest_output.contains("Commit events materialized: repos="));

    Ok(())
}

#[test]
fn fixture_corpus_ingest_smoke_is_cross_platform_friendly() -> anyhow::Result<()> {
    let env = TestEnv::new_live_fixture()?;
    let ingest_output = env.normalize_output(env.run_paceflow(&["ingest"])?);

    assert!(ingest_output.contains("Association summary:"));
    assert!(ingest_output.contains("Commit events materialized: repos="));
    assert!(ingest_output.contains("commits_scanned="));

    let session = env.normalize_output(env.run_paceflow(&["session"])?);
    let change = env.normalize_output(env.run_paceflow(&["delivery"])?);
    let lifecycle = env.normalize_output(env.run_paceflow(&["quality"])?);
    let change_grouped =
        env.normalize_output(env.run_paceflow(&["delivery", "--group-by", "repo"])?);
    let event_stream = env.normalize_output(env.run_paceflow(&["event-stream", "--limit", "5"])?);

    assert!(session.contains("Session Metrics"));
    assert!(change.contains("Delivery Metrics"));
    assert!(lifecycle.contains("Quality Metrics"));
    assert!(change_grouped.contains("Delivery Metrics"));
    assert!(change_grouped.contains("Group") || change_grouped.contains("No delivery rows found."));
    assert!(event_stream.trim().is_empty() || event_stream.contains("\"stream_type\""));

    let structured_change = parse_change_summary(&change)?;
    let commits: i64 = structured_change.commits.parse()?;
    let heavy_commits: i64 = structured_change.heavy_commits.parse()?;
    assert!((0..=commits).contains(&heavy_commits));

    Ok(())
}

fn materialize_paceflow_repo(repo_path: &Path) -> anyhow::Result<()> {
    run_command(
        Command::new("git")
            .arg("clone")
            .arg(PACEFLOW_BUNDLE)
            .arg(repo_path),
    )?;
    run_command(
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .arg("branch")
            .arg("staging")
            .arg("origin/staging"),
    )?;
    run_command(
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .arg("branch")
            .arg("codex/PAC-999-task-stats-demo")
            .arg("origin/codex/PAC-999-task-stats-demo"),
    )?;
    run_command(
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .arg("checkout")
            .arg("main"),
    )?;
    Ok(())
}

fn copy_codex_sessions(home: &Path, from: &[&str], to: &[&str]) -> anyhow::Result<()> {
    let src_root = Path::new(FIXTURE_ROOT)
        .join("home_template")
        .join(".codex")
        .join("sessions");
    let dst_root = home.join(".codex").join("sessions");
    copy_dir_recursive(&src_root, &dst_root)?;

    for path in collect_files(&dst_root)? {
        let mut content = fs::read_to_string(&path)?;
        for (from, to) in from.iter().zip(to.iter()) {
            content = content.replace(from, to);
        }
        fs::write(path, content)?;
    }

    Ok(())
}

fn install_cursor_fixture(
    home: &Path,
    paceflow_repo: &Path,
    cursor_dir: &Path,
) -> anyhow::Result<()> {
    for rel in [
        Path::new("Library")
            .join("Application Support")
            .join("Cursor")
            .join("User"),
        Path::new(".config").join("Cursor").join("User"),
    ] {
        let user_dir = home.join(rel);
        let global_storage = user_dir.join("globalStorage");
        let history = user_dir.join("History");
        fs::create_dir_all(&global_storage)?;
        fs::create_dir_all(&history)?;

        let db_path = global_storage.join("state.vscdb");
        fs::copy(HOME_TEMPLATE_CURSOR_DB, &db_path)?;
        rewrite_cursor_db(&db_path, home, paceflow_repo, cursor_dir)?;
    }
    Ok(())
}

fn rewrite_cursor_db(
    db_path: &Path,
    home: &Path,
    paceflow_repo: &Path,
    cursor_dir: &Path,
) -> anyhow::Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute(
        "UPDATE cursorDiskKV
         SET value = replace(
             replace(
                 replace(value, '__REPO_PACEFLOW__', ?1),
                 '__REPO_CURSOR__', ?2
             ),
             '__HOME__', ?3
         )",
        (
            paceflow_repo.to_string_lossy().to_string(),
            cursor_dir.to_string_lossy().to_string(),
            home.to_string_lossy().to_string(),
        ),
    )?;
    Ok(())
}

fn collect_files(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            out.extend(collect_files(&path)?);
        } else {
            out.push(path);
        }
    }
    Ok(out)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn run_command(cmd: &mut Command) -> anyhow::Result<()> {
    let output = cmd.output()?;
    if output.status.success() {
        return Ok(());
    }

    anyhow::bail!(
        "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
        cmd,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_session_summary(output: &str) -> anyhow::Result<SessionSummarySnapshot> {
    Ok(SessionSummarySnapshot {
        sessions: line_value(output, "Sessions: ")?,
        average_user_prompts: line_value(output, "Average User Prompts: ")?,
        avg_time_to_first_accepted_change_minutes: line_value(
            output,
            "Avg Time to First Accepted Change (min): ",
        )?,
        debug_loop_rate: line_value(output, "Debug Loop Rate: ")?,
        s6_rate: line_value(output, "Error Paste Rate: ")?,
        s9_rate: line_value(output, "Session-to-Commit Rate: ")?,
        no_output_session_rate: line_value(output, "No-Output Session Rate: ")?,
    })
}

fn parse_change_summary(output: &str) -> anyhow::Result<ChangeSummarySnapshot> {
    Ok(ChangeSummarySnapshot {
        commits: line_value(output, "Commits: ")?,
        heavy_commits: line_value(output, "Heavy commits: ")?,
        merge_rate: line_value(output, "C2 Merge Rate: ")?,
    })
}

fn parse_lifecycle_summary(output: &str) -> anyhow::Result<LifecycleSummarySnapshot> {
    Ok(LifecycleSummarySnapshot {
        heavy_commits: line_value(output, "Heavy commits: ")?,
        code_churn_rate: line_value(output, "L1 Code Churn Rate: ")?,
        revert_rate: line_value(output, "L4 Revert Rate: ")?,
    })
}

fn line_value(output: &str, prefix: &str) -> anyhow::Result<String> {
    output
        .lines()
        .find_map(|line| {
            line.strip_prefix(prefix)
                .map(|value| value.trim().to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("missing line for prefix {}", prefix))
}
