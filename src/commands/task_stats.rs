use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::Command;

use crate::change_intel::commit_assoc::matcher::ALGO_VERSION as COMMIT_ASSOC_ALGO_VERSION;
use crate::change_intel::commit_assoc::storage::TASK_ALGO_VERSION;
use crate::cli::TaskStatsArgs;
use crate::db;
use crate::metrics::quality::compute_session_quality_signals;

#[derive(Debug, Clone)]
struct SessionSignal {
    user_turns: i64,
    debug_loop: bool,
    mid_session_error_paste: bool,
}

#[derive(Debug, Default)]
struct TaskAggregate {
    branch_counts: BTreeMap<String, i64>,
    repo_counts: BTreeMap<String, i64>,
    commit_keys: BTreeSet<(String, String)>,
    session_weights: BTreeMap<(String, String), f64>,
    session_commit_times: BTreeMap<(String, String), BTreeSet<String>>,
}

#[derive(Debug, Clone)]
struct TaskRow {
    task_key: String,
    branch_name: String,
    commits: usize,
    sessions: usize,
    sessions_with_signals: usize,
    avg_reprompt: Option<f64>,
    debug_loop_rate: Option<f64>,
    mid_session_error_rate: Option<f64>,
    session_to_commit_rate: Option<f64>,
    staging_diff: Option<DiffStat>,
    grade: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct DiffStat {
    added: i64,
    removed: i64,
}

#[derive(Debug, Clone, Copy)]
struct SessionWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

pub fn run(args: TaskStatsArgs) -> Result<()> {
    let db = db::open()?;
    let task_rows = build_task_rows(&db, args.task.as_deref())?;
    let rendered = render_task_rows(&task_rows, args.limit, args.task.as_deref());
    print!("{}", rendered);
    Ok(())
}

fn build_task_rows(conn: &Connection, task_filter: Option<&str>) -> Result<Vec<TaskRow>> {
    let session_signals = load_session_signals(conn)?;
    let session_windows = load_session_windows(conn)?;
    let aggregates = load_task_aggregates(conn, task_filter)?;
    let mut rows = Vec::with_capacity(aggregates.len());
    let mut diff_cache: HashMap<(String, String), Option<DiffStat>> = HashMap::new();

    for (task_key, agg) in aggregates {
        let mut weighted_reprompts = 0.0f64;
        let mut weighted_debug_loops = 0.0f64;
        let mut total_weight = 0.0f64;
        let mut sessions_with_signals = 0usize;
        let mut s6_num = 0usize;
        let mut s6_den = 0usize;
        let mut s9_num = 0usize;
        let mut s9_den = 0usize;
        let branch_name = dominant_branch(&agg.branch_counts);
        let repo_root = dominant_repo(&agg.repo_counts);

        for ((provider, session_id), raw_weight) in &agg.session_weights {
            let session_key = (provider.clone(), session_id.clone());

            if let Some(signal) = session_signals.get(&session_key) {
                sessions_with_signals += 1;
                let weight = if *raw_weight > 0.0 { *raw_weight } else { 1.0 };
                total_weight += weight;
                weighted_reprompts += weight * signal.user_turns as f64;
                if signal.debug_loop {
                    weighted_debug_loops += weight;
                }

                s6_den += 1;
                if signal.mid_session_error_paste {
                    s6_num += 1;
                }
            }

            let Some(window) = session_windows.get(&session_key) else {
                continue;
            };
            let Some(commit_times) = agg.session_commit_times.get(&session_key) else {
                continue;
            };
            s9_den += 1;
            if has_commit_within_four_hours(window, commit_times) {
                s9_num += 1;
            }
        }

        let avg_reprompt = if total_weight > 0.0 {
            Some(weighted_reprompts / total_weight)
        } else {
            None
        };
        let debug_loop_rate = if total_weight > 0.0 {
            Some(weighted_debug_loops / total_weight)
        } else {
            None
        };
        let mid_session_error_rate = if s6_den > 0 {
            Some(s6_num as f64 / s6_den as f64)
        } else {
            None
        };
        let session_to_commit_rate = if s9_den > 0 {
            Some(s9_num as f64 / s9_den as f64)
        } else {
            None
        };
        let staging_diff = diff_vs_staging_for_branch(&repo_root, &branch_name, &mut diff_cache);

        rows.push(TaskRow {
            task_key,
            branch_name,
            commits: agg.commit_keys.len(),
            sessions: agg.session_weights.len(),
            sessions_with_signals,
            avg_reprompt,
            debug_loop_rate,
            mid_session_error_rate,
            session_to_commit_rate,
            staging_diff,
            grade: grade_task_quality(avg_reprompt, debug_loop_rate),
        });
    }

    rows.sort_by(|a, b| b.commits.cmp(&a.commits).then_with(|| a.task_key.cmp(&b.task_key)));
    Ok(rows)
}

fn load_session_signals(conn: &Connection) -> Result<HashMap<(String, String), SessionSignal>> {
    let signals = compute_session_quality_signals(conn)?;
    let mut out = HashMap::with_capacity(signals.len());
    for signal in signals {
        out.insert(
            (signal.provider, signal.session_id),
            SessionSignal {
                user_turns: signal.user_turns,
                debug_loop: signal.debug_loop,
                mid_session_error_paste: signal.mid_session_error_paste,
            },
        );
    }
    Ok(out)
}

fn load_session_windows(conn: &Connection) -> Result<HashMap<(String, String), SessionWindow>> {
    let mut stmt = conn.prepare(
        "SELECT provider,
                session_id,
                MIN(COALESCE(event_ts, session_started_at)) AS started_at,
                MAX(COALESCE(event_ts, session_last_updated, session_started_at)) AS ended_at
         FROM events
         GROUP BY provider, session_id",
    )?;

    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })?;

    let mut out = HashMap::new();
    for row in rows {
        let (provider, session_id, start_raw, end_raw) = row?;
        let Some(start_raw) = start_raw else {
            continue;
        };
        let Some(end_raw) = end_raw else {
            continue;
        };
        let Some(start) = parse_timestamp_utc(&start_raw) else {
            continue;
        };
        let Some(end) = parse_timestamp_utc(&end_raw) else {
            continue;
        };
        out.insert((provider, session_id), SessionWindow { start, end });
    }
    Ok(out)
}

fn load_task_aggregates(
    conn: &Connection,
    task_filter: Option<&str>,
) -> Result<BTreeMap<String, TaskAggregate>> {
    let mut stmt = conn.prepare(
        "SELECT
            t.task_key,
            t.branch_name,
            g.repo_root,
            g.commit_sha,
            g.commit_time,
            s.provider,
            s.session_id,
            s.matched_lines
         FROM commit_task_attributions t
         JOIN git_commits g ON g.id = t.commit_id
         LEFT JOIN commit_ai_session_attributions s
           ON s.commit_id = t.commit_id
          AND s.algo_version = ?1
         WHERE t.algo_version = ?2
           AND t.is_fallback = 0
           AND (?3 IS NULL OR t.task_key = ?3)
         ORDER BY g.commit_time DESC, g.commit_sha",
    )?;

    let rows = stmt.query_map(
        params![COMMIT_ASSOC_ALGO_VERSION, TASK_ALGO_VERSION, task_filter],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, Option<f64>>(7)?,
            ))
        },
    )?;

    let mut tasks: BTreeMap<String, TaskAggregate> = BTreeMap::new();
    for row in rows {
        let (
            task_key,
            branch_name,
            repo_root,
            commit_sha,
            commit_time,
            provider,
            session_id,
            matched_lines,
        ) = row?;
        if !looks_like_task_id(&task_key) {
            continue;
        }
        let aggregate = tasks.entry(task_key).or_default();

        if aggregate
            .commit_keys
            .insert((repo_root.clone(), commit_sha.clone()))
        {
            *aggregate.branch_counts.entry(branch_name).or_insert(0) += 1;
            *aggregate.repo_counts.entry(repo_root).or_insert(0) += 1;
        }

        let (Some(provider), Some(session_id)) = (provider, session_id) else {
            continue;
        };
        let weight = matched_lines.unwrap_or(0.0).max(0.0);
        aggregate
            .session_commit_times
            .entry((provider.clone(), session_id.clone()))
            .or_default()
            .insert(commit_time);
        *aggregate
            .session_weights
            .entry((provider, session_id))
            .or_insert(0.0) += weight;
    }

    Ok(tasks)
}

fn dominant_branch(branch_counts: &BTreeMap<String, i64>) -> String {
    branch_counts
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| "(unknown)".to_string())
}

fn dominant_repo(repo_counts: &BTreeMap<String, i64>) -> String {
    repo_counts
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| "(unknown)".to_string())
}

fn grade_task_quality(avg_reprompt: Option<f64>, debug_loop_rate: Option<f64>) -> &'static str {
    let (Some(s2), Some(s4)) = (avg_reprompt, debug_loop_rate) else {
        return "N/A";
    };

    if s2 <= 4.0 && s4 < 0.20 {
        "Healthy"
    } else if s2 <= 8.0 && s4 < 0.40 {
        "Watch"
    } else {
        "Warning"
    }
}

fn render_task_rows(rows: &[TaskRow], limit: usize, task_filter: Option<&str>) -> String {
    let mut out = String::new();
    out.push_str("Task Stats (Execution Quality)\n");
    out.push_str("S2 = average user prompts across sessions linked to task (lower is better)\n");
    out.push_str("S4 = debug-loop session rate (5+ repeated error turns, lower is better)\n");
    out.push_str("S6 = sessions with mid-session error paste (lower is better)\n");
    out.push_str(
        "S9 = sessions with task commit in [session start, session end + 4h] (higher is better)\n\n",
    );

    if rows.is_empty() {
        if let Some(task) = task_filter {
            out.push_str(&format!("No task rows found for '{}'.\n", task));
        } else {
            out.push_str("No task rows found. Run `vca associate-commits` first.\n");
            out.push_str("Only ticket task branches (e.g. ABC-123) are shown.\n");
        }
        return out;
    }

    let shown = rows.len().min(limit.max(1));
    out.push_str(&format!(
        "{:<28}  {:<26}  {:>7}  {:>9}  {:>11}  {:>9}  {:>11}  {:>9}  {:>11}  {:<8}\n",
        "Task",
        "Branch",
        "Commits",
        "Sessions",
        "S2(avg)",
        "S4(loop)",
        "S6(mid-err)",
        "S9(4h)",
        "vs Staging",
        "Grade"
    ));
    out.push_str(&format!(
        "{0:─<28}  {0:─<26}  {0:─>7}  {0:─>9}  {0:─>11}  {0:─>9}  {0:─>11}  {0:─>9}  {0:─>11}  {0:─<8}\n",
        ""
    ));

    for row in rows.iter().take(shown) {
        out.push_str(&format!(
            "{:<28}  {:<26}  {:>7}  {:>3}/{:<5}  {:>11}  {:>9}  {:>11}  {:>9}  {:>11}  {:<8}\n",
            truncate(&row.task_key, 28),
            truncate(&row.branch_name, 26),
            row.commits,
            row.sessions_with_signals,
            row.sessions,
            fmt_opt_decimal(row.avg_reprompt, 2),
            fmt_opt_percent(row.debug_loop_rate, 1),
            fmt_opt_percent(row.mid_session_error_rate, 1),
            fmt_opt_percent(row.session_to_commit_rate, 1),
            fmt_diff(row.staging_diff),
            row.grade
        ));
    }

    if rows.len() > shown {
        out.push_str(&format!(
            "\nShowing {} of {} task rows. Use `--limit` to expand.\n",
            shown,
            rows.len()
        ));
    }

    out.push_str(
        "\n`vs Staging` shows +added/-removed lines from `git diff staging...<branch>` for non-integration branches.\n",
    );
    out.push_str("Task rows exclude integration/fallback branches and non-ticket task keys.\n");
    out.push_str("S9 uses task-linked sessions with known timestamps.\n");
    out.push_str(
        "Grade thresholds: Healthy (S2<=4 and S4<20%), Watch (S2<=8 and S4<40%), else Warning.\n",
    );
    out
}

fn fmt_opt_decimal(value: Option<f64>, precision: usize) -> String {
    match value {
        Some(v) => format!("{:.*}", precision, v),
        None => "N/A".to_string(),
    }
}

fn fmt_opt_percent(value: Option<f64>, precision: usize) -> String {
    match value {
        Some(v) => format!("{:.*}%", precision, v * 100.0),
        None => "N/A".to_string(),
    }
}

fn fmt_diff(value: Option<DiffStat>) -> String {
    match value {
        Some(v) => format!("+{}/-{}", v.added, v.removed),
        None => "N/A".to_string(),
    }
}

fn truncate(input: &str, max_len: usize) -> String {
    if input.chars().count() <= max_len {
        return input.to_string();
    }
    if max_len <= 3 {
        return "...".to_string();
    }
    let mut out = String::new();
    for ch in input.chars().take(max_len - 3) {
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn diff_vs_staging_for_branch(
    repo_root: &str,
    branch_name: &str,
    cache: &mut HashMap<(String, String), Option<DiffStat>>,
) -> Option<DiffStat> {
    if is_integration_branch(branch_name) || repo_root == "(unknown)" {
        return None;
    }

    let key = (repo_root.to_string(), branch_name.to_string());
    if let Some(cached) = cache.get(&key) {
        return *cached;
    }

    let staging_ref = resolve_ref(repo_root, &["refs/heads/staging", "refs/remotes/origin/staging"]);
    let branch_ref = resolve_ref(
        repo_root,
        &[
            &format!("refs/heads/{branch_name}"),
            &format!("refs/remotes/origin/{branch_name}"),
        ],
    );

    let Some(staging_ref) = staging_ref else {
        cache.insert(key, None);
        return None;
    };
    let Some(branch_ref) = branch_ref else {
        cache.insert(key, None);
        return None;
    };

    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("diff")
        .arg("--numstat")
        .arg("--no-renames")
        .arg(format!("{staging_ref}...{branch_ref}"))
        .output();

    let Some(output) = output.ok() else {
        cache.insert(key, None);
        return None;
    };
    if !output.status.success() {
        cache.insert(key, None);
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut added = 0i64;
    let mut removed = 0i64;
    for line in stdout.lines() {
        let mut parts = line.splitn(3, '\t');
        let Some(a_raw) = parts.next() else {
            continue;
        };
        let Some(r_raw) = parts.next() else {
            continue;
        };
        if let Ok(v) = a_raw.parse::<i64>() {
            added += v;
        }
        if let Ok(v) = r_raw.parse::<i64>() {
            removed += v;
        }
    }

    let stat = DiffStat { added, removed };
    cache.insert(key, Some(stat));
    Some(stat)
}

fn resolve_ref(repo_root: &str, refs: &[&str]) -> Option<String> {
    for reference in refs {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .arg("show-ref")
            .arg("--verify")
            .arg("--quiet")
            .arg(reference)
            .output()
            .ok()?;
        if output.status.success() {
            return Some((*reference).to_string());
        }
    }
    None
}

fn is_integration_branch(branch_name: &str) -> bool {
    matches!(branch_name, "staging" | "main" | "master" | "develop" | "(unknown)")
}

fn looks_like_task_id(task_key: &str) -> bool {
    let bytes = task_key.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_uppercase() {
        i += 1;
    }
    if i == 0 || i >= bytes.len() || bytes[i] != b'-' {
        return false;
    }

    let mut j = i + 1;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }

    j > i + 1 && j == bytes.len()
}

fn has_commit_within_four_hours(window: &SessionWindow, commit_times: &BTreeSet<String>) -> bool {
    let upper_bound = window.end + Duration::hours(4);
    commit_times.iter().any(|raw| {
        parse_timestamp_utc(raw)
            .map(|commit_ts| commit_ts >= window.start && commit_ts <= upper_bound)
            .unwrap_or(false)
    })
}

fn parse_timestamp_utc(raw: &str) -> Option<DateTime<Utc>> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }

    if let Ok(ts) = DateTime::parse_from_rfc3339(value) {
        return Some(ts.with_timezone(&Utc));
    }

    if let Ok(ts) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc));
    }

    if let Ok(ts) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc));
    }

    None
}
