use anyhow::Result;

use crate::analytics;
use crate::cli::{GroupBy, QualityReportArgs};
use crate::commands::report_scope;
use crate::db;

pub fn run(args: QualityReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let report = report_scope::resolve_report_args(&args.report);
    let rows = analytics::query_lifecycle_report(&db, &report)?;
    print!("{}", render_quality_report(&rows, &args));
    Ok(())
}

fn render_quality_report(
    rows: &[analytics::LifecycleReportRow],
    args: &QualityReportArgs,
) -> String {
    let mut out = String::new();
    out.push_str("Quality Metrics\n");
    out.push_str("L1 code churn rate = share of AI-added lines on heavy AI commits that were later removed within the churn window\n");
    out.push_str("L4 revert rate = share of heavy AI commits that were later reverted\n\n");

    if rows.is_empty() {
        out.push_str("No quality rows found. Run `paceflow ingest` first.\n");
        return out;
    }

    if !args.report.weekly && args.report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Heavy commits: {}\n", row.heavy_commit_count));
        out.push_str(&format!(
            "L1 Code Churn Rate: {}\n",
            fmt_ratio(&row.code_churn_rate, 2)
        ));
        out.push_str(&format!(
            "L4 Revert Rate: {}\n",
            fmt_ratio(&row.revert_rate, 2)
        ));
        return out;
    }

    let show_week = args.report.weekly;
    let show_group = args.report.group_by.is_some();
    let show_branch = matches!(args.report.group_by, Some(GroupBy::Task));

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
    headers.push(format!("{:>8}", "Heavy"));
    headers.push(format!("{:>12}", "L1(churn)"));
    headers.push(format!("{:>12}", "L4(revert)"));
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
        cols.push(format!("{:>8}", row.heavy_commit_count));
        cols.push(format!(
            "{:>12}",
            fmt_ratio_percent(&row.code_churn_rate, 1)
        ));
        cols.push(format!("{:>12}", fmt_ratio_percent(&row.revert_rate, 1)));
        out.push_str(&format!("{}\n", cols.join("  ")));
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
