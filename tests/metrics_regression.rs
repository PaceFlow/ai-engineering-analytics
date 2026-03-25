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
const VCA_BUNDLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/regression/repos/vca.bundle"
);

struct TestEnv {
    _tempdir: TempDir,
    home: PathBuf,
    vca_repo: PathBuf,
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
    fn new() -> anyhow::Result<Self> {
        let tempdir = TempDir::new()?;
        let home = tempdir.path().to_path_buf();
        let work_dir = home.join("work");
        let vca_repo = work_dir.join("fixture-vca");
        let cursor_dir = work_dir.join("fixture-cursor");

        fs::create_dir_all(&work_dir)?;
        fs::create_dir_all(&cursor_dir)?;

        materialize_vca_repo(&vca_repo)?;
        copy_codex_sessions(
            &home,
            &["__REPO_VCA__", "__REPO_CURSOR__", "__HOME__"],
            &[
                &vca_repo.to_string_lossy(),
                &cursor_dir.to_string_lossy(),
                &home.to_string_lossy(),
            ],
        )?;
        install_cursor_fixture(&home, &vca_repo, &cursor_dir)?;

        Ok(Self {
            _tempdir: tempdir,
            home,
            vca_repo,
            cursor_dir,
        })
    }

    fn run_vca(&self, args: &[&str]) -> anyhow::Result<String> {
        let output = Command::cargo_bin("vca")?
            .args(args)
            .current_dir(&self.home)
            .env("HOME", &self.home)
            .env_remove("XDG_CONFIG_HOME")
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "vca {:?} failed\nstdout:\n{}\nstderr:\n{}",
                args,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8(output.stdout)?)
    }

    fn ingest(&self) -> anyhow::Result<()> {
        self.run_vca(&["ingest"])?;
        Ok(())
    }

    fn normalize_stream_output(&self, output: String) -> String {
        let mut normalized = output;
        for (path, placeholder) in [
            (&self.vca_repo, "__REPO_VCA__"),
            (&self.cursor_dir, "__REPO_CURSOR__"),
            (&self.home, "__HOME__"),
        ] {
            for variant in path_variants(path) {
                normalized = normalized.replace(&variant, placeholder);
            }
        }
        normalized
    }
}

#[test]
fn category_reports_match_snapshots() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    env.ingest()?;

    let session = env.run_vca(&["session"])?;
    let change = env.run_vca(&["change"])?;
    let lifecycle = env.run_vca(&["lifecycle"])?;

    assert!(session.contains("Session Metrics"));
    assert!(change.contains("Change Metrics"));
    assert!(lifecycle.contains("Lifecycle Metrics"));

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
    let env = TestEnv::new()?;
    env.ingest()?;

    let session_grouped = env.run_vca(&["session", "--group-by", "provider"])?;
    let change_grouped = env.run_vca(&["change", "--group-by", "repo"])?;
    let lifecycle_grouped = env.run_vca(&["lifecycle", "--group-by", "repo"])?;
    let session_weekly = env.run_vca(&["session", "--weekly", "--group-by", "provider"])?;

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
    let env = TestEnv::new()?;
    env.ingest()?;

    let session_stream =
        env.normalize_stream_output(env.run_vca(&["event-stream", "--stream", "session-base"])?);
    let task_commit_stream = env.normalize_stream_output(env.run_vca(&[
        "event-stream",
        "--stream",
        "task-commit-base",
    ])?);
    let all_streams_smoke =
        env.normalize_stream_output(env.run_vca(&["event-stream", "--limit", "5"])?);

    assert_snapshot!("fixture_corpus_event_stream_session_base", session_stream);
    assert_snapshot!(
        "fixture_corpus_event_stream_task_commit_base",
        task_commit_stream
    );
    assert_snapshot!("fixture_corpus_event_stream_all_smoke", all_streams_smoke);

    Ok(())
}

fn path_variants(path: &Path) -> Vec<String> {
    let mut variants = vec![path.to_string_lossy().into_owned()];
    if let Ok(canonical) = fs::canonicalize(path) {
        let canonical = canonical.to_string_lossy().into_owned();
        if !variants.contains(&canonical) {
            variants.push(canonical);
        }
    }
    variants
}

#[test]
fn ingest_is_idempotent_for_fixture_corpus() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    env.ingest()?;

    let session_before = env.run_vca(&["session"])?;
    let change_before = env.run_vca(&["change"])?;
    let lifecycle_before = env.run_vca(&["lifecycle"])?;

    let second_ingest = env.run_vca(&["ingest"])?;
    assert!(second_ingest.contains("Ingest progress: 100%"));

    let session_after = env.run_vca(&["session"])?;
    let change_after = env.run_vca(&["change"])?;
    let lifecycle_after = env.run_vca(&["lifecycle"])?;

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
    let env = TestEnv::new()?;
    let ingest_output = env.run_vca(&["ingest"])?;

    assert!(ingest_output.contains("Planning ingest..."));
    assert!(ingest_output.contains("Stage: codex sessions"));
    assert!(ingest_output.contains("Stage: commit association"));
    assert!(ingest_output.contains("Ingest progress: 100%"));
    assert!(ingest_output.contains("Materializing commit events ..."));
    assert!(ingest_output.contains("Commit events materialized: repos="));

    Ok(())
}

fn materialize_vca_repo(repo_path: &Path) -> anyhow::Result<()> {
    run_command(
        Command::new("git")
            .arg("clone")
            .arg(VCA_BUNDLE)
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

fn install_cursor_fixture(home: &Path, vca_repo: &Path, cursor_dir: &Path) -> anyhow::Result<()> {
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
        rewrite_cursor_db(&db_path, home, vca_repo, cursor_dir)?;
    }
    Ok(())
}

fn rewrite_cursor_db(
    db_path: &Path,
    home: &Path,
    vca_repo: &Path,
    cursor_dir: &Path,
) -> anyhow::Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute(
        "UPDATE cursorDiskKV
         SET value = replace(
             replace(
                 replace(value, '__REPO_VCA__', ?1),
                 '__REPO_CURSOR__', ?2
             ),
             '__HOME__', ?3
         )",
        (
            vca_repo.to_string_lossy().to_string(),
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
