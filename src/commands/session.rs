use anyhow::Result;
use std::io::{IsTerminal, Write};
use std::process::{Command, Stdio};

use crate::analytics;
use crate::cli::SessionReportArgs;
use crate::db;

pub fn run(args: SessionReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;

    let output = if args.list_sessions {
        let rows = analytics::query_session_list_rows(&db, &args.report)?;
        render_session_list(&rows)
    } else {
        let rows = analytics::query_session_report(&db, &args.report)?;
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
    out.push_str("S2 = average user prompts per session\n");
    out.push_str("S4 = debug-loop session rate\n");
    out.push_str("S6 = mid-session error-paste rate\n");
    out.push_str("S9 = session-to-commit rate (session end + 4h)\n\n");

    if rows.is_empty() {
        out.push_str("No session rows found. Run `vca ingest` first.\n");
        return out;
    }

    if !args.report.weekly && args.report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Sessions: {}\n", row.session_count));
        out.push_str(&format!("S2 Re-Prompt Avg: {}\n", fmt_opt_decimal(row.s2_avg, 2)));
        out.push_str(&format!(
            "S4 Debug Loop Rate: {}\n",
            fmt_ratio(&row.debug_loop_rate, 2)
        ));
        out.push_str(&format!("S6 Error Paste Rate: {}\n", fmt_ratio(&row.s6_rate, 2)));
        out.push_str(&format!("S9 Session-to-Commit Rate: {}\n", fmt_ratio(&row.s9_rate, 2)));
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
    headers.push(format!("{:>10}", "S2(avg)"));
    headers.push(format!("{:>14}", "S4(loop)"));
    headers.push(format!("{:>14}", "S6(error)"));
    headers.push(format!("{:>14}", "S9(4h)"));
    out.push_str(&format!("{}\n", headers.join("  ")));

    for row in rows {
        let mut cols = vec![];
        if show_week {
            cols.push(format!("{:<10}", row.week_start.as_deref().unwrap_or("-")));
        }
        if show_group {
            cols.push(format!("{:<28}", truncate(row.group_value.as_deref().unwrap_or("(all)"), 28)));
        }
        if show_branch {
            cols.push(format!("{:<26}", truncate(row.branch_name.as_deref().unwrap_or("-"), 26)));
        }
        cols.push(format!("{:>8}", row.session_count));
        cols.push(format!("{:>10}", fmt_opt_decimal(row.s2_avg, 2)));
        cols.push(format!(
            "{:>14}",
            fmt_ratio_percent(&row.debug_loop_rate, 1)
        ));
        cols.push(format!("{:>14}", fmt_ratio_percent(&row.s6_rate, 1)));
        cols.push(format!("{:>14}", fmt_ratio_percent(&row.s9_rate, 1)));
        out.push_str(&format!("{}\n", cols.join("  ")));
    }

    out
}

fn render_session_list(rows: &[analytics::SessionListRow]) -> String {
    let mut out = String::new();
    out.push_str("Session List\n\n");
    if rows.is_empty() {
        out.push_str("No session rows found. Run `vca ingest` first.\n");
        return out;
    }

    let project_width = rows
        .iter()
        .map(|row| shorten_path(&row.project_path).len())
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

    for row in rows {
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
            shorten_path(&row.project_path),
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
        Some(value) => format!("{:.*}% ({}/{})", precision, value, metric.numerator, metric.denominator),
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
