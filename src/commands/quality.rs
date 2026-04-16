use anyhow::Result;

use crate::analytics;
use crate::cli::{GroupBy, QualityReportArgs, ReportArgs};
use crate::commands::report_layout::{
    ScorecardRow, append_legend, classify_ratio_lower_better, fmt_ratio, fmt_ratio_percent,
    group_label, render_scorecard, truncate,
};
use crate::commands::report_scope;
use crate::db;

pub fn run(args: QualityReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let resolved = report_scope::resolve_main_report_args(&args.report, args.overall);
    let rows = analytics::query_lifecycle_report_with_options(
        &db,
        &resolved.report,
        analytics::ReportQueryOptions {
            implicit_model_default: resolved.implicit_model_default,
        },
    )?;
    print!("{}", render_quality_report(&rows, &resolved.report));
    Ok(())
}

fn render_quality_report(rows: &[analytics::LifecycleReportRow], report: &ReportArgs) -> String {
    let mut out = String::new();
    out.push_str("Quality Metrics\n");

    if rows.is_empty() {
        out.push_str("No quality rows found. Run `paceflow ingest` first.\n");
        return out;
    }

    if !report.weekly && report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!(
            "Heavy commits analyzed: {}\n\n",
            row.heavy_commit_count
        ));
        let scorecard = [
            ScorecardRow {
                label: "Code churn",
                value: fmt_ratio(&row.code_churn_rate, 2),
                status: classify_ratio_lower_better(&row.code_churn_rate, 15.0, 30.0),
            },
            ScorecardRow {
                label: "Bug-after-merge",
                value: fmt_ratio(&row.bug_after_merge_rate, 2),
                status: classify_ratio_lower_better(&row.bug_after_merge_rate, 15.0, 30.0),
            },
            ScorecardRow {
                label: "Reverts",
                value: fmt_ratio(&row.revert_rate, 2),
                status: classify_ratio_lower_better(&row.revert_rate, 2.0, 5.0),
            },
        ];
        out.push_str(&render_scorecard(&scorecard));
        append_legend(
            &mut out,
            &[
                "Code churn: share of AI-added lines on heavy AI commits that were later removed within the churn window.",
                "Bug-after-merge: share of merged heavy AI commits that drew a later fix-like commit within 60 days.",
                "Reverts: share of heavy AI commits that were later reverted.",
                "Status: lower is better for all quality signals.",
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
    headers.push(format!("{:>8}", "Heavy"));
    headers.push(format!("{:>12}", "Churn Rate"));
    headers.push(format!("{:>10}", "Bug Rate"));
    headers.push(format!("{:>12}", "Revert Rate"));
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
        cols.push(format!(
            "{:>10}",
            fmt_ratio_percent(&row.bug_after_merge_rate, 1)
        ));
        cols.push(format!("{:>12}", fmt_ratio_percent(&row.revert_rate, 1)));
        out.push_str(&format!("{}\n", cols.join("  ")));
    }

    append_legend(
        &mut out,
        &["Churn Rate, Bug Rate, and Revert Rate = percentage rates."],
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{LifecycleReportRow, RatioMetric};
    use crate::cli::ReportArgs;

    #[test]
    fn render_quality_report_uses_scorecard_summary_with_footer() {
        let rows = vec![LifecycleReportRow {
            week_start: None,
            group_value: None,
            branch_name: None,
            heavy_commit_count: 3,
            code_churn_rate: RatioMetric {
                numerator: 4,
                denominator: 20,
            },
            bug_after_merge_rate: RatioMetric {
                numerator: 1,
                denominator: 3,
            },
            revert_rate: RatioMetric {
                numerator: 0,
                denominator: 3,
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
            model: None,
            limit: 50,
        };

        let rendered = render_quality_report(&rows, &report);
        assert!(rendered.contains("Heavy commits analyzed: 3"));
        assert!(rendered.contains("│ Signal"));
        assert!(rendered.contains("│ Code churn"));
        assert!(rendered.contains("20.00% (4/20)"));
        assert!(rendered.contains("33.33% (1/3)"));
        assert!(rendered.contains("good"));
        assert!(rendered.contains("watch"));
        assert!(rendered.contains("Legend:"));
        assert!(!rendered.contains("L1 code churn rate ="));
    }

    #[test]
    fn render_quality_report_shows_l3_grouped_column() {
        let rows = vec![LifecycleReportRow {
            week_start: None,
            group_value: Some("codex".to_string()),
            branch_name: None,
            heavy_commit_count: 3,
            code_churn_rate: RatioMetric {
                numerator: 4,
                denominator: 20,
            },
            bug_after_merge_rate: RatioMetric {
                numerator: 1,
                denominator: 3,
            },
            revert_rate: RatioMetric {
                numerator: 0,
                denominator: 3,
            },
        }];
        let report = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Provider),
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            model: None,
            limit: 50,
        };

        let rendered = render_quality_report(&rows, &report);
        assert!(rendered.contains("Churn Rate"));
        assert!(rendered.contains("Bug Rate"));
        assert!(rendered.contains("Revert Rate"));
        assert!(!rendered.contains("L3(bug)"));
        assert!(rendered.contains("33.3%"));
    }
}
