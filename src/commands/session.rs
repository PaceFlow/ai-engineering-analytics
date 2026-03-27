use anyhow::Result;
use std::io::{IsTerminal, Write};
use std::process::{Command, Stdio};

use crate::analytics;
use crate::cli::SessionReportArgs;
use crate::commands::report_scope;
use crate::db;

pub fn run(args: SessionReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let report = report_scope::resolve_report_args(&args.report);

    let output = if args.list_sessions {
        let rows = analytics::query_session_list_rows(&db, &report)?;
        render_session_list(&rows)
    } else {
        let rows = analytics::query_session_report(&db, &report)?;
        render_session_report(&rows, &args)
    };

    if std::io::stdout().is_terminal() && output.lines().count() > terminal_height() {
        pipe_to_pager(&output)
    } else {
        print!("{output}");
        Ok(())
    }
}

fn render_session_report(rows: &[analytics::SessionReportRow], args: &SessionReportArgs) -> String {
    let mut out = String::new();
    out.push_str("Session Metrics\n");
    out.push_str("Average user prompts = average number of user prompts per session\n");
    out.push_str(
        "Avg time to first accepted change = minutes from session start to the first accepted code change\n",
    );
    out.push_str("Debug loop rate = share of sessions that look like repeated fix-retry loops\n");
    out.push_str(
        "Error paste rate = share of sessions where an error message was pasted mid-session\n",
    );
    out.push_str(
        "Session-to-commit rate = share of sessions followed by a commit within 4 hours\n",
    );
    out.push_str("No-output session rate = share of sessions with no accepted code changes\n\n");

    if rows.is_empty() {
        out.push_str("No session rows found. Run `aieng ingest` first.\n");
        return out;
    }

    if !args.report.weekly && args.report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Sessions: {}\n", row.session_count));
        out.push_str(&format!(
            "Average User Prompts: {}\n",
            fmt_opt_decimal(row.s2_avg, 2)
        ));
        out.push_str(&format!(
            "Avg Time to First Accepted Change (min): {}\n",
            fmt_opt_decimal(row.avg_minutes_to_first_accepted_change, 2)
        ));
        out.push_str(&format!(
            "Debug Loop Rate: {}\n",
            fmt_ratio(&row.debug_loop_rate, 2)
        ));
        out.push_str(&format!(
            "Error Paste Rate: {}\n",
            fmt_ratio(&row.s6_rate, 2)
        ));
        out.push_str(&format!(
            "Session-to-Commit Rate: {}\n",
            fmt_ratio(&row.s9_rate, 2)
        ));
        out.push_str(&format!(
            "No-Output Session Rate: {}\n",
            fmt_ratio(&row.no_output_session_rate, 2)
        ));
        return out;
    }

    let show_week = args.report.weekly;
    let show_group = args.report.group_by.is_some();
    let show_branch = matches!(args.report.group_by, Some(crate::cli::GroupBy::Task));
    let mut headers = vec![];
    if show_week {
        headers.push(format!("{:<10}", "Week"));
    }
    if show_group {
        headers.push(format!("{:<28}", "Group"));
    }
    if show_branch {
        headers.push(format!("{:<26}", "Branch"));
    }
    headers.push(format!("{:>8}", "Sessions"));
    headers.push(format!("{:>10}", "Prompts"));
    headers.push(format!("{:>11}", "First Chg"));
    headers.push(format!("{:>14}", "Loop"));
    headers.push(format!("{:>14}", "Error"));
    headers.push(format!("{:>14}", "To Commit"));
    headers.push(format!("{:>14}", "No Output"));
    out.push_str(&format!("{}\n", headers.join("  ")));

    for row in rows {
        let mut cols = vec![];
        if show_week {
            cols.push(format!("{:<10}", row.week_start.as_deref().unwrap_or("-")));
        }
        if show_group {
            cols.push(format!(
                "{:<28}",
                truncate(row.group_value.as_deref().unwrap_or("(all)"), 28)
            ));
        }
        if show_branch {
            cols.push(format!(
                "{:<26}",
                truncate(row.branch_name.as_deref().unwrap_or("-"), 26)
            ));
        }
        cols.push(format!("{:>8}", row.session_count));
        cols.push(format!("{:>10}", fmt_opt_decimal(row.s2_avg, 2)));
        cols.push(format!(
            "{:>11}",
            fmt_opt_decimal(row.avg_minutes_to_first_accepted_change, 1)
        ));
        cols.push(format!(
            "{:>14}",
            fmt_ratio_percent(&row.debug_loop_rate, 1)
        ));
        cols.push(format!("{:>14}", fmt_ratio_percent(&row.s6_rate, 1)));
        cols.push(format!("{:>14}", fmt_ratio_percent(&row.s9_rate, 1)));
        cols.push(format!(
            "{:>14}",
            fmt_ratio_percent(&row.no_output_session_rate, 1)
        ));
        out.push_str(&format!("{}\n", cols.join("  ")));
    }

    out
}

fn render_session_list(rows: &[analytics::SessionListRow]) -> String {
    let mut out = String::new();
    out.push_str("Session List\n\n");
    if rows.is_empty() {
        out.push_str("No session rows found. Run `aieng ingest` first.\n");
        return out;
    }

    let display_paths = rows
        .iter()
        .map(|row| display_project_path(&row.project_path))
        .collect::<Vec<_>>();
    let project_width = rows
        .iter()
        .zip(display_paths.iter())
        .map(|(_, path)| path.chars().count())
        .max()
        .unwrap_or(7)
        .max(7);

    out.push_str(&format!(
        "{:<8}  {:<14}  {:<8}  {:<width$}  {:<16}  {:>6}  {:>7}  {:>7}  {:>9}\n",
        "Provider",
        "Model",
        "Session",
        "Project",
        "Last active",
        "LOC",
        "+Lines",
        "-Lines",
        "Words/LOC",
        width = project_width
    ));

    for (row, project_path) in rows.iter().zip(display_paths.iter()) {
        let words_per_loc = if row.total_loc > 0 {
            format!("{:.1}", row.total_words as f64 / row.total_loc as f64)
        } else {
            "N/A".to_string()
        };
        let last_active = row.last_active.as_deref().unwrap_or("?");
        let last_active = &last_active[..last_active.len().min(16)];
        let session_short = &row.session_id[..row.session_id.len().min(8)];
        out.push_str(&format!(
            "{:<8}  {:<14}  {:<8}  {:<width$}  {:<16}  {:>6}  {:>7}  {:>7}  {:>9}\n",
            row.provider,
            truncate(&row.model, 14),
            session_short,
            project_path,
            last_active,
            row.total_loc,
            row.total_added,
            row.total_removed,
            words_per_loc,
            width = project_width
        ));
    }

    out
}

fn fmt_ratio(metric: &analytics::RatioMetric, precision: usize) -> String {
    match metric.percent() {
        Some(value) => format!(
            "{:.*}% ({}/{})",
            precision, value, metric.numerator, metric.denominator
        ),
        None => "N/A".to_string(),
    }
}

fn fmt_ratio_percent(metric: &analytics::RatioMetric, precision: usize) -> String {
    match metric.percent() {
        Some(value) => format!("{:.*}%", precision, value),
        None => "N/A".to_string(),
    }
}

fn fmt_opt_decimal(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format!("{:.*}", precision, value))
        .unwrap_or_else(|| "N/A".to_string())
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

fn shorten_path(path: &str) -> String {
    if let Some(home) = dirs::home_dir() {
        let home_str = home.to_string_lossy();
        if path.starts_with(home_str.as_ref()) {
            return format!("~{}", &path[home_str.len()..]);
        }
    }
    path.to_string()
}

fn display_project_path(path: &str) -> String {
    const MAX_PROJECT_PATH_LEN: usize = 36;
    shorten_path_tail(&shorten_path(path), MAX_PROJECT_PATH_LEN)
}

fn shorten_path_tail(path: &str, max_len: usize) -> String {
    if path.chars().count() <= max_len {
        return path.to_string();
    }

    let (prefix, remainder) = if let Some(stripped) = path.strip_prefix("~/") {
        ("~/", stripped)
    } else if let Some(stripped) = path.strip_prefix('/') {
        ("/", stripped)
    } else {
        ("", path)
    };

    let segments = remainder
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return truncate_tail(path, max_len);
    }

    let tail_one = segments.last().copied().unwrap_or_default().to_string();
    let tail_two = if segments.len() >= 2 {
        format!(
            "{}/{}",
            segments[segments.len() - 2],
            segments[segments.len() - 1]
        )
    } else {
        tail_one.clone()
    };

    let mut candidates = Vec::new();
    if let Some(first) = segments.first() {
        candidates.push(format!("{prefix}{first}/.../{tail_two}"));
    }
    candidates.push(format!("{prefix}.../{tail_two}"));
    candidates.push(format!("{prefix}.../{tail_one}"));

    for candidate in candidates {
        if candidate.chars().count() <= max_len {
            return candidate;
        }
    }

    truncate_tail(&tail_one, max_len)
}

fn truncate_tail(value: &str, max_len: usize) -> String {
    let value_chars = value.chars().collect::<Vec<_>>();
    if value_chars.len() <= max_len {
        return value.to_string();
    }
    if max_len <= 3 {
        return "...".to_string();
    }

    let tail = value_chars[value_chars.len() - (max_len - 3)..]
        .iter()
        .collect::<String>();
    format!("...{tail}")
}

fn terminal_height() -> usize {
    terminal_size::terminal_size()
        .map(|(_, terminal_size::Height(height))| height as usize)
        .unwrap_or(24)
}

fn pipe_to_pager(output: &str) -> Result<()> {
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
    let mut command = Command::new(&pager);
    if pager == "less" || pager.ends_with("/less") {
        command.args(["-R", "-F", "-X"]);
    }

    match command.stdin(Stdio::piped()).spawn() {
        Ok(mut child) => {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(output.as_bytes());
            }
            child.wait()?;
        }
        Err(_) => print!("{output}"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{display_project_path, render_session_list};
    use crate::analytics::SessionListRow;

    fn row(project_path: &str) -> SessionListRow {
        SessionListRow {
            provider: "cursor".to_string(),
            model: "cursor/claude-3.5-sonnet".to_string(),
            session_id: "12345678-session".to_string(),
            project_path: project_path.to_string(),
            last_active: Some("2026-03-17T09:30:00Z".to_string()),
            total_words: 100,
            total_loc: 20,
            total_added: 10,
            total_removed: 5,
        }
    }

    #[test]
    fn display_project_path_preserves_distinguishing_tail() {
        let rendered =
            display_project_path("/Users/daniel/work/company/apps/mobile/customer-portal");
        assert!(
            rendered.ends_with("apps/mobile/customer-portal")
                || rendered.ends_with("mobile/customer-portal")
        );
        assert!(rendered.contains("..."));
    }

    #[test]
    fn render_session_list_shows_distinct_project_tails() {
        let output = render_session_list(&[
            row("/Users/daniel/work/company/apps/mobile/customer-portal"),
            row("/Users/daniel/work/company/apps/mobile/admin-portal"),
        ]);

        assert!(output.contains("customer-portal"));
        assert!(output.contains("admin-portal"));
        assert!(!output.contains("/Users/daniel/work/company/apps/mobile/customer-portal"));
    }
}
