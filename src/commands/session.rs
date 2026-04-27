use anyhow::Result;
use rusqlite::Connection;
use std::io::{IsTerminal, Write};
use std::process::{Command, Stdio};

use crate::analytics;
use crate::cli::{GroupBy, ReportArgs, SessionReportArgs};
use crate::commands::report_layout::{
    ScorecardRow, append_legend, classify_lower_better, classify_ratio_higher_better,
    classify_ratio_lower_better, fmt_opt_decimal, fmt_ratio, fmt_ratio_percent, group_label,
    render_scorecard, truncate,
};
use crate::commands::report_scope;
use crate::db;

pub fn run(args: SessionReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let resolved = report_scope::resolve_main_report_args(&args.report, args.overall);
    let options = analytics::ReportQueryOptions {
        implicit_model_default: resolved.implicit_model_default,
    };

    let output = if args.list_sessions {
        let rows = analytics::query_session_list_rows(&db, &resolved.report)?;
        render_session_list(&rows)
    } else {
        let rows = analytics::query_session_report_with_options(&db, &resolved.report, options)?;
        let show_branch_hint = rows.is_empty()
            && task_report_hidden_branch_rows_exist(&db, &resolved.report, options)?;
        render_session_report(&rows, &resolved.report, show_branch_hint)
    };

    if std::io::stdout().is_terminal() && output.lines().count() > terminal_height() {
        pipe_to_pager(&output)
    } else {
        print!("{output}");
        Ok(())
    }
}

fn render_session_report(
    rows: &[analytics::SessionReportRow],
    report: &ReportArgs,
    show_branch_hint: bool,
) -> String {
    let mut out = String::new();
    out.push_str("Session Metrics\n");

    if rows.is_empty() {
        if show_branch_hint {
            out.push_str(
                "No ticket-style task rows matched. Try `paceflow session --group-by branch` or `--overall`.\n",
            );
        } else {
            out.push_str("No session rows found. Run `paceflow ingest` first.\n");
        }
        return out;
    }

    if !report.weekly && report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Sessions analyzed: {}\n\n", row.session_count));
        let scorecard = [
            ScorecardRow {
                label: "Time to first change",
                value: match row.avg_minutes_to_first_accepted_change {
                    Some(value) => format!("{value:.2} min"),
                    None => "N/A".to_string(),
                },
                status: classify_lower_better(row.avg_minutes_to_first_accepted_change, 15.0, 45.0),
            },
            ScorecardRow {
                label: "No-output sessions",
                value: fmt_ratio(&row.no_output_session_rate, 2),
                status: classify_ratio_lower_better(&row.no_output_session_rate, 15.0, 35.0),
            },
            ScorecardRow {
                label: "Sessions to commit",
                value: fmt_ratio(&row.s9_rate, 2),
                status: classify_ratio_higher_better(&row.s9_rate, 50.0, 25.0),
            },
            ScorecardRow {
                label: "Error pastes",
                value: fmt_ratio(&row.s6_rate, 2),
                status: classify_ratio_lower_better(&row.s6_rate, 10.0, 25.0),
            },
            ScorecardRow {
                label: "Avg prompts",
                value: fmt_opt_decimal(row.s2_avg, 2),
                status: classify_lower_better(row.s2_avg, 4.0, 7.0),
            },
            ScorecardRow {
                label: "Debug loops",
                value: fmt_ratio(&row.debug_loop_rate, 2),
                status: classify_ratio_lower_better(&row.debug_loop_rate, 10.0, 25.0),
            },
        ];
        out.push_str(&render_scorecard(&scorecard));
        append_legend(
            &mut out,
            &[
                "Avg prompts: average number of user prompts per session.",
                "Time to first change: average minutes from session start to first accepted code change.",
                "Debug loops: share of sessions that look like repeated fix-retry loops.",
                "Error pastes: share of sessions where an error message was pasted mid-session.",
                "Sessions to commit: share of sessions followed by a commit within 4 hours.",
                "No-output sessions: share of sessions with no accepted code changes.",
                "Status: lower is better except Sessions to commit.",
            ],
        );
        return out;
    }

    let show_week = report.weekly;
    let show_group = report.group_by.is_some();
    let show_branch = matches!(report.group_by, Some(GroupBy::Task));
    let mut headers = vec![];
    out.push('\n');
    if show_week {
        headers.push(format!("{:<10}", "Week"));
    }
    if show_group {
        headers.push(format!("{:<28}", group_label(report.group_by)));
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

    append_legend(
        &mut out,
        &[
            "Prompts = avg prompts per session.",
            "First Chg = avg minutes to first accepted code change.",
            "Loop, Error, To Commit, and No Output = percentage rates.",
        ],
    );

    out
}

fn render_session_list(rows: &[analytics::SessionListRow]) -> String {
    let mut out = String::new();
    out.push_str("Session List\n\n");
    if rows.is_empty() {
        out.push_str("No session rows found. Run `paceflow ingest` first.\n");
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

fn task_report_hidden_branch_rows_exist(
    db: &Connection,
    report: &ReportArgs,
    options: analytics::ReportQueryOptions,
) -> Result<bool> {
    if !matches!(report.group_by, Some(GroupBy::Task)) || report.task.is_some() {
        return Ok(false);
    }

    let mut branch_report = report.clone();
    branch_report.group_by = Some(GroupBy::Branch);
    branch_report.task = None;
    branch_report.limit = 1;
    Ok(!analytics::query_session_report_with_options(db, &branch_report, options)?.is_empty())
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
    use super::{display_project_path, render_session_list, render_session_report};
    use crate::analytics::{RatioMetric, SessionListRow, SessionReportRow};
    use crate::cli::{GroupBy, ReportArgs};

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

    #[test]
    fn render_session_report_uses_scorecard_summary_with_footer() {
        let rows = vec![SessionReportRow {
            week_start: None,
            group_value: None,
            branch_name: None,
            session_count: 411,
            s2_avg: Some(5.56),
            avg_minutes_to_first_accepted_change: Some(46.63),
            debug_loop_rate: RatioMetric {
                numerator: 1,
                denominator: 411,
            },
            s6_rate: RatioMetric {
                numerator: 69,
                denominator: 411,
            },
            s9_rate: RatioMetric {
                numerator: 166,
                denominator: 411,
            },
            no_output_session_rate: RatioMetric {
                numerator: 159,
                denominator: 411,
            },
        }];
        let report = ReportArgs {
            weekly: false,
            group_by: None,
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let rendered = render_session_report(&rows, &report, false);
        assert!(rendered.contains("Sessions analyzed: 411"));
        assert!(rendered.contains("│ Signal"));
        assert!(rendered.contains("│ Time to first change"));
        assert!(rendered.contains("46.63 min"));
        assert!(rendered.contains("watch"));
        assert!(rendered.contains("risk"));
        assert!(rendered.contains("Legend:"));
        assert!(rendered.contains("Status: lower is better except Sessions to commit."));
        assert!(!rendered.contains("Average user prompts ="));
    }

    #[test]
    fn render_session_report_suggests_branch_view_for_hidden_task_rows() {
        let report = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Task),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: Some("claude".to_string()),
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let rendered = render_session_report(&[], &report, true);
        assert!(rendered.contains("No ticket-style task rows matched."));
        assert!(rendered.contains("`paceflow session --group-by branch`"));
        assert!(!rendered.contains("Run `paceflow ingest` first."));
    }
}
