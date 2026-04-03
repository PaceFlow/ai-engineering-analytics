use anyhow::Result;
use std::collections::HashMap;
use std::process::Command;

use crate::analytics;
use crate::cli::{DeliveryReportArgs, GroupBy};
use crate::commands::report_scope;
use crate::db;

#[derive(Debug, Clone, Copy)]
struct DiffStat {
    added: i64,
    removed: i64,
}

pub fn run(args: DeliveryReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let report = report_scope::resolve_report_args(&args.report);
    let rows = analytics::query_change_report(&db, &report)?;
    print!("{}", render_delivery_report(&rows, &args));
    Ok(())
}

fn render_delivery_report(
    rows: &[analytics::ChangeReportRow],
    args: &DeliveryReportArgs,
) -> String {
    let mut out = String::new();
    out.push_str("Delivery Metrics\n");
    out.push_str("Heavy commits = commits where matched AI-attributed lines are at least half of changed lines\n");
    out.push_str(
        "C1 PR reach rate = share of heavy GitHub AI commits that reached a pull request\n",
    );
    out.push_str("C2 merge rate = share of heavy AI commits that later reached mainline\n\n");
    out.push_str(
        "C3 PR merge rate = share of PR-linked heavy GitHub AI commits whose PR merged\n\n",
    );

    if rows.is_empty() {
        out.push_str("No delivery rows found. Run `paceflow ingest` first.\n");
        return out;
    }

    if !args.report.weekly && args.report.group_by.is_none() {
        let row = &rows[0];
        out.push_str(&format!("Commits: {}\n", row.commit_count));
        out.push_str(&format!("Heavy commits: {}\n", row.heavy_commit_count));
        out.push_str(&format!(
            "C1 PR Reach Rate: {}\n",
            fmt_optional_ratio(&row.pr_reach_rate, row.github_pr_metrics_available, 2)
        ));
        out.push_str(&format!(
            "C2 Merge Rate: {}\n",
            fmt_ratio(&row.merge_rate, 2)
        ));
        out.push_str(&format!(
            "C3 PR Merge Rate: {}\n",
            fmt_optional_ratio(&row.pr_merge_rate, row.github_pr_metrics_available, 2)
        ));
        return out;
    }

    let mut cache = HashMap::new();
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
    headers.push(format!("{:>8}", "Commits"));
    headers.push(format!("{:>8}", "Heavy"));
    headers.push(format!("{:>10}", "C1(PR)"));
    headers.push(format!("{:>12}", "C2(merge)"));
    headers.push(format!("{:>14}", "C3(PR merge)"));
    if show_branch {
        headers.push(format!("{:>12}", "vs Staging"));
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
        cols.push(format!(
            "{:>10}",
            fmt_optional_ratio_percent(&row.pr_reach_rate, row.github_pr_metrics_available, 1)
        ));
        cols.push(format!("{:>12}", fmt_ratio_percent(&row.merge_rate, 1)));
        cols.push(format!(
            "{:>14}",
            fmt_optional_ratio_percent(&row.pr_merge_rate, row.github_pr_metrics_available, 1)
        ));
        if show_branch {
            cols.push(format!(
                "{:>12}",
                fmt_diff(
                    row.repo_root.as_deref(),
                    row.branch_name.as_deref(),
                    &mut cache
                )
            ));
        }
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

fn fmt_ratio_percent(metric: &analytics::RatioMetric, precision: usize) -> String {
    match metric.percent() {
        Some(value) => format!("{:.*}%", precision, value),
        None => "N/A".to_string(),
    }
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

fn fmt_diff(
    repo_root: Option<&str>,
    branch_name: Option<&str>,
    cache: &mut HashMap<(String, String), Option<DiffStat>>,
) -> String {
    let (Some(repo_root), Some(branch_name)) = (repo_root, branch_name) else {
        return "N/A".to_string();
    };
    let stat = diff_vs_staging_for_branch(repo_root, branch_name, cache);
    match stat {
        Some(value) => format!("+{}/-{}", value.added, value.removed),
        None => "N/A".to_string(),
    }
}

fn diff_vs_staging_for_branch(
    repo_root: &str,
    branch_name: &str,
    cache: &mut HashMap<(String, String), Option<DiffStat>>,
) -> Option<DiffStat> {
    if repo_root == "(unknown)" {
        return None;
    }

    let key = (repo_root.to_string(), branch_name.to_string());
    if let Some(cached) = cache.get(&key) {
        return *cached;
    }

    let staging_ref = resolve_ref(
        repo_root,
        &["refs/heads/staging", "refs/remotes/origin/staging"],
    );
    let branch_ref = resolve_ref(
        repo_root,
        &[
            &format!("refs/heads/{branch_name}"),
            &format!("refs/remotes/origin/{branch_name}"),
        ],
    );

    let (Some(staging_ref), Some(branch_ref)) = (staging_ref, branch_ref) else {
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
        .output()
        .ok()?;
    if !output.status.success() {
        cache.insert(key, None);
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut added = 0i64;
    let mut removed = 0i64;
    for line in stdout.lines() {
        let mut parts = line.splitn(3, '\t');
        let Some(added_raw) = parts.next() else {
            continue;
        };
        let Some(removed_raw) = parts.next() else {
            continue;
        };
        if let Ok(value) = added_raw.parse::<i64>() {
            added += value;
        }
        if let Ok(value) = removed_raw.parse::<i64>() {
            removed += value;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{ChangeReportRow, RatioMetric};
    use crate::cli::ReportArgs;

    #[test]
    fn render_delivery_report_shows_c1_and_c3_summary_metrics() {
        let rows = vec![ChangeReportRow {
            week_start: None,
            group_value: None,
            branch_name: None,
            repo_root: None,
            commit_count: 4,
            heavy_commit_count: 2,
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
        }];
        let args = DeliveryReportArgs {
            report: ReportArgs {
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
            },
        };

        let rendered = render_delivery_report(&rows, &args);
        assert!(rendered.contains("C1 PR Reach Rate: 50.00% (1/2)"));
        assert!(rendered.contains("C2 Merge Rate: 100.00% (2/2)"));
        assert!(rendered.contains("C3 PR Merge Rate: 100.00% (1/1)"));
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
        }];
        let args = DeliveryReportArgs {
            report: ReportArgs {
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
            },
        };

        let rendered = render_delivery_report(&rows, &args);
        assert!(rendered.contains("C1(PR)"));
        assert!(rendered.contains("C3(PR merge)"));
        assert!(rendered.contains("N/A"));
    }
}
