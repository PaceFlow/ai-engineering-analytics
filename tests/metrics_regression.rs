use assert_cmd::cargo::CommandCargoExt;
use insta::{assert_snapshot, assert_yaml_snapshot};
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/regression");
const HOME_TEMPLATE_CURSOR_DB: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/regression/home_template/cursor/state.vscdb");
const VCA_BUNDLE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/regression/repos/vca.bundle");

struct TestEnv {
    _tempdir: TempDir,
    home: PathBuf,
    vca_repo: PathBuf,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct MetricSnapshot {
    name: String,
    percent: String,
    numerator: String,
    denominator: String,
    status: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StatsRowSnapshot {
    provider: String,
    session: String,
    project: String,
    last_active: String,
    loc: String,
    added: String,
    removed: String,
    words_per_loc: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct RepoMetricSnapshot {
    percent: String,
    numerator: String,
    denominator: String,
    status: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct RepoBreakdownSnapshot {
    repo: String,
    heavy_commits: String,
    l4: RepoMetricSnapshot,
    c2: RepoMetricSnapshot,
    l1: RepoMetricSnapshot,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct TaskRowSnapshot {
    task: String,
    branch: String,
    commits: String,
    sessions_with_signals: String,
    sessions_total: String,
    s2_avg: String,
    s4_loop: String,
    s6_mid_error: String,
    s9_4h: String,
    vs_staging: String,
    grade: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StatsReportSnapshot {
    metrics: Vec<MetricSnapshot>,
    repo_rows: Vec<RepoBreakdownSnapshot>,
    session_rows: Vec<StatsRowSnapshot>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct TaskStatsReportSnapshot {
    task_rows: Vec<TaskRowSnapshot>,
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
            &[
                "__REPO_VCA__",
                "__REPO_CURSOR__",
                "__HOME__",
            ],
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
        })
    }

    fn run_vca(&self, args: &[&str]) -> anyhow::Result<String> {
        let output = Command::cargo_bin("vca")?
            .args(args)
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

    fn ingest_and_associate(&self) -> anyhow::Result<()> {
        self.run_vca(&["ingest"])?;
        self.associate_current_head()?;
        self.checkout_repo_branch("codex/PAC-999-task-stats-demo")?;
        self.associate_current_head()?;
        self.checkout_repo_branch("main")?;
        Ok(())
    }

    fn associate_current_head(&self) -> anyhow::Result<()> {
        self.run_vca(&[
            "associate-commits",
            "--repo",
            &self.vca_repo.to_string_lossy(),
        ])?;
        Ok(())
    }

    fn checkout_repo_branch(&self, branch: &str) -> anyhow::Result<()> {
        run_command(
            Command::new("git")
                .arg("-C")
                .arg(&self.vca_repo)
                .arg("checkout")
                .arg(branch),
        )
    }
}

#[test]
fn stats_and_task_stats_match_snapshots() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    env.ingest_and_associate()?;

    let stats = env.run_vca(&["stats"])?;
    let task_stats = env.run_vca(&["task-stats", "--limit", "20"])?;
    let structured_stats = parse_stats_report(&stats)?;
    let structured_task_stats = parse_task_stats_report(&task_stats)?;

    assert!(stats.contains("AI Quality Metrics (heavy commits only)"));
    assert!(stats.contains("codex"));
    assert!(stats.contains("cursor"));
    assert!(task_stats.contains("Task Stats (Execution Quality)"));
    assert!(task_stats.contains("PAC-999"));
    assert!(!task_stats.contains("No task rows found"));
    assert_eq!(structured_stats.metrics[0].percent, "0.00%");
    assert_eq!(structured_stats.metrics[1].percent, "80.00%");
    assert_eq!(structured_stats.metrics[2].percent, "1.98%");
    assert_eq!(structured_stats.metrics[3].percent, "0.00%");
    assert_eq!(structured_task_stats.task_rows.len(), 1);
    assert_eq!(structured_task_stats.task_rows[0].task, "PAC-999");
    assert_eq!(structured_task_stats.task_rows[0].commits, "1");
    assert_eq!(structured_task_stats.task_rows[0].s2_avg, "15.00");
    assert_eq!(structured_task_stats.task_rows[0].s9_4h, "100.0%");

    assert_snapshot!("fixture_corpus_stats_report_text", stats);
    assert_snapshot!("fixture_corpus_task_stats_report_text", task_stats);
    assert_yaml_snapshot!(
        "fixture_corpus_stats_report_structured",
        structured_stats
    );
    assert_yaml_snapshot!(
        "fixture_corpus_task_stats_report_structured",
        structured_task_stats
    );

    Ok(())
}

#[test]
fn ingest_and_associate_are_idempotent_for_fixture_corpus() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    env.ingest_and_associate()?;

    let stats_before = env.run_vca(&["stats"])?;
    let task_stats_before = env.run_vca(&["task-stats", "--limit", "20"])?;

    env.ingest_and_associate()?;

    let stats_after = env.run_vca(&["stats"])?;
    let task_stats_after = env.run_vca(&["task-stats", "--limit", "20"])?;

    assert_eq!(stats_before, stats_after, "stats output drifted after rerun");
    assert_eq!(
        task_stats_before, task_stats_after,
        "task-stats output drifted after rerun"
    );

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
    let src_root = Path::new(FIXTURE_ROOT).join("home_template").join(".codex").join("sessions");
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

fn parse_stats_report(output: &str) -> anyhow::Result<StatsReportSnapshot> {
    let lines: Vec<&str> = output.lines().collect();
    let mut metrics = Vec::new();
    for prefix in [
        "L4 Revert Rate",
        "C2 Merge Rate (git proxy, squash-aware)",
        "L1 Code Churn Rate (14d, mainline-only)",
        "S4 Debug Loop Rate",
    ] {
        let line = lines
            .iter()
            .find(|line| line.starts_with(prefix))
            .ok_or_else(|| anyhow::anyhow!("missing metric line for {}", prefix))?;
        metrics.push(parse_metric_line(line)?);
    }

    let repo_start = lines
        .iter()
        .position(|line| *line == "Per-repo breakdown")
        .ok_or_else(|| anyhow::anyhow!("missing repo section"))?
        + 3;
    let session_start = lines
        .iter()
        .position(|line| line.starts_with("Provider  Session"))
        .ok_or_else(|| anyhow::anyhow!("missing session table"))?;

    let mut repo_rows = Vec::new();
    for line in &lines[repo_start..session_start - 1] {
        if !line.trim().is_empty() {
            let columns = split_columns(line);
            if columns.len() != 5 {
                anyhow::bail!("unexpected repo row format: {}", line);
            }

            repo_rows.push(RepoBreakdownSnapshot {
                repo: columns[0].to_string(),
                heavy_commits: columns[1].to_string(),
                l4: parse_repo_metric(columns[2])?,
                c2: parse_repo_metric(columns[3])?,
                l1: parse_repo_metric(columns[4])?,
            });
        }
    }

    let mut session_rows = Vec::new();
    for line in lines.iter().skip(session_start + 2) {
        if line.trim().is_empty() {
            continue;
        }
        let columns = split_columns(line);
        if columns.len() != 8 {
            anyhow::bail!("unexpected stats row format: {}", line);
        }
        session_rows.push(StatsRowSnapshot {
            provider: columns[0].to_string(),
            session: columns[1].to_string(),
            project: columns[2].to_string(),
            last_active: columns[3].to_string(),
            loc: columns[4].to_string(),
            added: columns[5].to_string(),
            removed: columns[6].to_string(),
            words_per_loc: columns[7].to_string(),
        });
    }

    Ok(StatsReportSnapshot {
        metrics,
        repo_rows,
        session_rows,
    })
}

fn parse_task_stats_report(output: &str) -> anyhow::Result<TaskStatsReportSnapshot> {
    let lines: Vec<&str> = output.lines().collect();
    let header_start = lines
        .iter()
        .position(|line| line.starts_with("Task                          Branch"))
        .ok_or_else(|| anyhow::anyhow!("missing task table"))?;

    let mut task_rows = Vec::new();
    for line in lines.iter().skip(header_start + 2) {
        if line.trim().is_empty() || line.starts_with('`') {
            break;
        }
        let columns = split_columns(line);
        if columns.len() != 10 {
            anyhow::bail!("unexpected task row format: {}", line);
        }

        let mut sessions = columns[3].splitn(2, '/');
        let sessions_with_signals = sessions.next().unwrap_or_default().to_string();
        let sessions_total = sessions.next().unwrap_or_default().to_string();

        task_rows.push(TaskRowSnapshot {
            task: columns[0].to_string(),
            branch: columns[1].to_string(),
            commits: columns[2].to_string(),
            sessions_with_signals,
            sessions_total,
            s2_avg: columns[4].to_string(),
            s4_loop: columns[5].to_string(),
            s6_mid_error: columns[6].to_string(),
            s9_4h: columns[7].to_string(),
            vs_staging: columns[8].to_string(),
            grade: columns[9].to_string(),
        });
    }

    Ok(TaskStatsReportSnapshot { task_rows })
}

fn parse_metric_line(line: &str) -> anyhow::Result<MetricSnapshot> {
    let percent_end = line
        .find('%')
        .ok_or_else(|| anyhow::anyhow!("missing percent in metric line: {}", line))?;
    let percent_start = line[..percent_end]
        .rfind(char::is_whitespace)
        .map(|idx| idx + 1)
        .ok_or_else(|| anyhow::anyhow!("missing metric percent start: {}", line))?;

    let name = line[..percent_start].trim().to_string();
    let percent = line[percent_start..=percent_end].trim().to_string();

    let ratio_start = line[percent_end + 1..]
        .find('(')
        .map(|idx| idx + percent_end + 1)
        .ok_or_else(|| anyhow::anyhow!("missing ratio in metric line: {}", line))?;
    let ratio_end = line[ratio_start..]
        .find(')')
        .map(|idx| idx + ratio_start)
        .ok_or_else(|| anyhow::anyhow!("missing ratio end in metric line: {}", line))?;
    let ratio = &line[ratio_start + 1..ratio_end];
    let mut ratio_parts = ratio.splitn(2, '/');

    let status_start = line[ratio_end + 1..]
        .find('[')
        .map(|idx| idx + ratio_end + 1)
        .ok_or_else(|| anyhow::anyhow!("missing status in metric line: {}", line))?;
    let status_end = line[status_start..]
        .find(']')
        .map(|idx| idx + status_start)
        .ok_or_else(|| anyhow::anyhow!("missing status end in metric line: {}", line))?;

    Ok(MetricSnapshot {
        name,
        percent,
        numerator: ratio_parts.next().unwrap_or_default().to_string(),
        denominator: ratio_parts.next().unwrap_or_default().to_string(),
        status: line[status_start + 1..status_end].to_string(),
    })
}

fn parse_repo_metric(cell: &str) -> anyhow::Result<RepoMetricSnapshot> {
    let percent_end = cell
        .find('%')
        .ok_or_else(|| anyhow::anyhow!("missing percent in repo metric: {}", cell))?;
    let percent = cell[..=percent_end].trim().to_string();

    let ratio_start = cell[percent_end + 1..]
        .find('(')
        .map(|idx| idx + percent_end + 1)
        .ok_or_else(|| anyhow::anyhow!("missing ratio in repo metric: {}", cell))?;
    let ratio_end = cell[ratio_start..]
        .find(')')
        .map(|idx| idx + ratio_start)
        .ok_or_else(|| anyhow::anyhow!("missing ratio end in repo metric: {}", cell))?;
    let ratio = &cell[ratio_start + 1..ratio_end];
    let mut ratio_parts = ratio.splitn(2, '/');

    Ok(RepoMetricSnapshot {
        percent,
        numerator: ratio_parts.next().unwrap_or_default().to_string(),
        denominator: ratio_parts.next().unwrap_or_default().to_string(),
        status: cell[ratio_end + 1..].trim().to_string(),
    })
}

fn split_columns(line: &str) -> Vec<&str> {
    let mut columns = Vec::new();
    let mut start = 0usize;
    let chars: Vec<(usize, char)> = line.char_indices().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        if chars[idx].1 != ' ' {
            idx += 1;
            continue;
        }

        let run_start = chars[idx].0;
        let mut run_end = run_start;
        let mut run_len = 0usize;
        while idx < chars.len() && chars[idx].1 == ' ' {
            run_end = chars[idx].0 + chars[idx].1.len_utf8();
            run_len += 1;
            idx += 1;
        }

        if run_len >= 2 {
            let cell = line[start..run_start].trim();
            if !cell.is_empty() {
                columns.push(cell);
            }
            start = run_end;
        }
    }

    let tail = line[start..].trim();
    if !tail.is_empty() {
        columns.push(tail);
    }

    columns
}
