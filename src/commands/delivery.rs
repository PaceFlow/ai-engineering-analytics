use anyhow::Result;

use crate::analytics;
use crate::cli::{DeliveryReportArgs, GroupBy, ReportArgs};
use crate::commands::report_layout::{
    MetricStatus, ScorecardRow, append_legend, classify_ratio_higher_better, fmt_ratio,
    fmt_ratio_percent, group_label, render_scorecard, truncate,
};
use crate::commands::report_scope;
use crate::db;

pub fn run(args: DeliveryReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let resolved = report_scope::resolve_main_report_args(&args.report, args.overall);
    let rows = analytics::query_change_report_with_options(
        &db,
        &resolved.report,
        analytics::ReportQueryOptions {
            implicit_model_default: resolved.implicit_model_default,
        },
    )?;
    print!("{}", render_delivery_report(&rows, &resolved.report));
    Ok(())
}

fn render_delivery_report(rows: &[analytics::ChangeReportRow], report: &ReportArgs) -> String {
    let mut out = String::new();
    out.push_str("Delivery Metrics\n");

    if rows.is_empty() {
        out.push_str("No delivery rows found. Run `paceflow ingest` first.\n");
        return out;
    }

    if !report.weekly && report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Commits analyzed: {}\n", row.commit_count));
        out.push_str(&format!(
            "Heavy commits: {} ({})\n",
            row.heavy_commit_count,
            fmt_share(row.heavy_commit_count, row.commit_count)
        ));
        if row.github_pr_heavy_eligible > 0 {
            out.push_str(&format!(
                "GitHub PR lookup coverage: {} / {} heavy GitHub commits (completed lookup)\n\n",
                row.github_pr_heavy_ready, row.github_pr_heavy_eligible
            ));
        } else {
            out.push('\n');
        }

        let scorecard = [
            ScorecardRow {
                label: "Mainline Reach",
                value: fmt_ratio(&row.merge_rate, 2),
                status: classify_ratio_higher_better(&row.merge_rate, 75.0, 50.0),
            },
            ScorecardRow {
                label: "PR reach",
                value: fmt_optional_ratio(&row.pr_reach_rate, row.github_pr_metrics_available, 2),
                status: classify_optional_ratio_higher_better(
                    &row.pr_reach_rate,
                    row.github_pr_metrics_available,
                    70.0,
                    40.0,
                ),
            },
            ScorecardRow {
                label: "PR merge",
                value: fmt_optional_ratio(&row.pr_merge_rate, row.github_pr_metrics_available, 2),
                status: classify_optional_ratio_higher_better(
                    &row.pr_merge_rate,
                    row.github_pr_metrics_available,
                    75.0,
                    50.0,
                ),
            },
        ];
        out.push_str(&render_scorecard(&scorecard));
        append_legend(
            &mut out,
            &[
                "Heavy commits: commits where matched AI-attributed lines are at least half of changed lines.",
                "GitHub PR lookup coverage: heavy commits on github.com with completed lookup (resolved or no PR) vs all heavy commits on github.com.",
                "PR reach: among completed lookups, share where a pull request existed.",
                "Mainline reach: share of heavy AI commits that later reached mainline.",
                "PR merge: among PR-linked commits with completed lookup, share whose PR merged.",
                "PR reach / merge show N/A when no GitHub-heavy commits or no completed lookups yet.",
                "Status: higher is better for all delivery signals.",
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
    headers.push(format!("{:>8}", "Commits"));
    headers.push(format!("{:>8}", "Heavy"));
    headers.push(format!("{:>9}", "PR sync"));
    headers.push(format!("{:>10}", "PR Reach"));
    headers.push(format!("{:>15}", "Mainline Reach"));
    headers.push(format!("{:>12}", "PR Merge"));
    if show_branch {
        headers.push(format!("{:>12}", "± LOC commits"));
    }
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
        cols.push(format!("{:>8}", row.commit_count));
        cols.push(format!("{:>8}", row.heavy_commit_count));
        cols.push(format!("{:>9}", fmt_github_pr_lookup_coverage(row)));
        cols.push(format!(
            "{:>10}",
            fmt_optional_ratio_percent(&row.pr_reach_rate, row.github_pr_metrics_available, 1)
        ));
        cols.push(format!("{:>15}", fmt_ratio_percent(&row.merge_rate, 1)));
        cols.push(format!(
            "{:>14}",
            fmt_optional_ratio_percent(&row.pr_merge_rate, row.github_pr_metrics_available, 1)
        ));
        if show_branch {
            cols.push(format!(
                "{:>12}",
                fmt_task_branch_diff_stat(row.task_branch_lines_added, row.task_branch_lines_removed)
            ));
        }
        out.push_str(&format!("{}\n", cols.join("  ")));
    }

    let mut legend: Vec<&'static str> = vec![
        "PR sync = completed GitHub PR lookups / heavy commits on github.com (same scope as PR reach).",
        "PR Reach and PR Merge use completed lookups only; sync the rest with `PACEFLOW_GITHUB_TOKEN` and ingest.",
        "PR Reach, Mainline Reach, and PR Merge = percentage rates (PR merge N/A when no PR-linked commits in the completed set).",
    ];
    if matches!(report.group_by, Some(GroupBy::Task)) {
        legend.push(
            "± LOC commits: sum of lines added/removed from ingested git stats (`fact_commit`) for commits attributed to this task branch (not git diff vs staging).",
        );
    }
    append_legend(&mut out, &legend);

    out
}

fn fmt_github_pr_lookup_coverage(row: &analytics::ChangeReportRow) -> String {
    if row.github_pr_heavy_eligible == 0 {
        return "—".to_string();
    }
    format!(
        "{}/{}",
        row.github_pr_heavy_ready, row.github_pr_heavy_eligible
    )
}

fn fmt_optional_ratio(
    metric: &analytics::RatioMetric,
    available: bool,
    precision: usize,
) -> String {
    if !available {
        return "N/A".to_string();
    }
    fmt_ratio(metric, precision)
}

fn fmt_optional_ratio_percent(
    metric: &analytics::RatioMetric,
    available: bool,
    precision: usize,
) -> String {
    if !available {
        return "N/A".to_string();
    }
    fmt_ratio_percent(metric, precision)
}

fn classify_optional_ratio_higher_better(
    metric: &analytics::RatioMetric,
    available: bool,
    good_at_least: f64,
    watch_at_least: f64,
) -> MetricStatus {
    if !available {
        return MetricStatus::Unavailable;
    }

    classify_ratio_higher_better(metric, good_at_least, watch_at_least)
}

fn fmt_share(numerator: i64, denominator: i64) -> String {
    if denominator > 0 {
        format!(
            "{:.2}% of commits",
            numerator as f64 / denominator as f64 * 100.0
        )
    } else {
        "N/A".to_string()
    }
}

fn fmt_task_branch_diff_stat(added: i64, removed: i64) -> String {
    format!("+{}/-{}", added, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{ChangeReportRow, RatioMetric};
    use crate::cli::ReportArgs;

    #[test]
    fn render_delivery_report_uses_scorecard_summary_with_footer() {
        let rows = vec![ChangeReportRow {
            week_start: None,
            group_value: None,
            branch_name: None,
            repo_root: None,
            commit_count: 4,
            heavy_commit_count: 2,
            github_pr_heavy_eligible: 2,
            github_pr_heavy_ready: 2,
            pr_reach_rate: RatioMetric {
                numerator: 1,
                denominator: 2,
            },
            merge_rate: RatioMetric {
                numerator: 2,
                denominator: 2,
            },
            pr_merge_rate: RatioMetric {
                numerator: 1,
                denominator: 1,
            },
            github_pr_metrics_available: true,
            task_branch_lines_added: 0,
            task_branch_lines_removed: 0,
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

        let rendered = render_delivery_report(&rows, &report);
        assert!(rendered.contains("Commits analyzed: 4"));
        assert!(rendered.contains("Heavy commits: 2 (50.00% of commits)"));
        assert!(rendered.contains("GitHub PR lookup coverage: 2 / 2 heavy GitHub commits"));
        assert!(rendered.contains("│ Signal"));
        assert!(rendered.contains("│ Mainline Reach"));
        assert!(rendered.contains("50.00% (1/2)"));
        assert!(rendered.contains("100.00% (2/2)"));
        assert!(rendered.contains("good"));
        assert!(rendered.contains("watch"));
        assert!(rendered.contains("Legend:"));
        assert!(!rendered.contains("Heavy commits ="));
    }

    #[test]
    fn render_delivery_report_uses_na_for_github_metrics_without_complete_sync() {
        let rows = vec![ChangeReportRow {
            week_start: None,
            group_value: Some("codex".to_string()),
            branch_name: None,
            repo_root: None,
            commit_count: 4,
            heavy_commit_count: 2,
            github_pr_heavy_eligible: 2,
            github_pr_heavy_ready: 0,
            pr_reach_rate: RatioMetric {
                numerator: 0,
                denominator: 2,
            },
            merge_rate: RatioMetric {
                numerator: 1,
                denominator: 2,
            },
            pr_merge_rate: RatioMetric {
                numerator: 0,
                denominator: 0,
            },
            github_pr_metrics_available: false,
            task_branch_lines_added: 0,
            task_branch_lines_removed: 0,
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

        let rendered = render_delivery_report(&rows, &report);
        assert!(rendered.contains("PR Reach"));
        assert!(rendered.contains("Mainline Reach"));
        assert!(rendered.contains("PR Merge"));
        assert!(!rendered.contains("Merge Rate"));
        assert!(!rendered.contains("C1(PR)"));
        assert!(rendered.contains("N/A"));
    }

    #[test]
    fn render_delivery_report_marks_unavailable_pr_metrics_in_summary() {
        let rows = vec![ChangeReportRow {
            week_start: None,
            group_value: None,
            branch_name: None,
            repo_root: None,
            commit_count: 4,
            heavy_commit_count: 2,
            github_pr_heavy_eligible: 2,
            github_pr_heavy_ready: 0,
            pr_reach_rate: RatioMetric {
                numerator: 0,
                denominator: 2,
            },
            merge_rate: RatioMetric {
                numerator: 1,
                denominator: 2,
            },
            pr_merge_rate: RatioMetric {
                numerator: 0,
                denominator: 0,
            },
            github_pr_metrics_available: false,
            task_branch_lines_added: 0,
            task_branch_lines_removed: 0,
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

        let rendered = render_delivery_report(&rows, &report);
        assert!(rendered.contains("unavailable"));
        assert!(rendered.contains("GitHub PR lookup coverage: 0 / 2 heavy GitHub commits"));
        assert!(rendered.contains(
            "PR reach / merge show N/A when no GitHub-heavy commits or no completed lookups yet.",
        ));
    }
}
