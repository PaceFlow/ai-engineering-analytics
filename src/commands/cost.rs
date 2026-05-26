use anyhow::Result;

use crate::analytics;
use crate::cli::{CostReportArgs, GroupBy, ReportArgs};
use crate::commands::report_layout::{append_legend, group_label, truncate};
use crate::commands::report_scope;
use crate::db;

pub fn run(args: CostReportArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let resolved = report_scope::resolve_main_report_args(&args.report, args.overall);
    let rows = analytics::query_cost_report(&db, &resolved.report)?;
    print!(
        "{}",
        render_cost_report(&rows, &resolved.report, resolved.repo_auto_injected)
    );
    Ok(())
}

fn render_cost_report(
    rows: &[analytics::CostReportRow],
    report: &ReportArgs,
    repo_auto_injected: bool,
) -> String {
    let mut out = String::new();
    out.push_str("Cost Metrics\n");
    if rows.is_empty() {
        if repo_auto_injected {
            out.push_str(
                "No cost rows found for the current repo. Run `paceflow ingest` first, or pass `--all-projects` to include data from other ingested repos.\n",
            );
        } else {
            out.push_str("No cost rows found. Run `paceflow ingest` first.\n");
        }
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
    headers.push(format!("{:>8}", "Accepted"));
    headers.push(format!("{:>10}", "Cost"));
    headers.push(format!("{:>11}", "Tokens"));
    headers.push(format!("{:>12}", "$/Session"));
    headers.push(format!("{:>12}", "$/Acc Sess"));
    headers.push(format!("{:>10}", "$/LOC"));
    headers.push(format!("{:>12}", "$/Mainline"));
    headers.push(format!("{:>10}", "Coverage"));
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
        cols.push(format!("{:>8}", row.accepted_session_count));
        cols.push(format!("{:>10}", fmt_money(row.total_cost_usd)));
        cols.push(format!("{:>11}", fmt_count(row.total_tokens)));
        cols.push(format!(
            "{:>12}",
            fmt_money(div(row.total_cost_usd, row.session_count))
        ));
        cols.push(format!(
            "{:>12}",
            fmt_money(div(row.total_cost_usd, row.accepted_session_count))
        ));
        cols.push(format!(
            "{:>10}",
            fmt_money(div(row.total_cost_usd, row.accepted_total_changed_lines))
        ));
        cols.push(format!(
            "{:>12}",
            fmt_money(div(row.total_cost_usd, row.mainline_change_count))
        ));
        cols.push(format!(
            "{:>10}",
            fmt_coverage(row.priced_session_count, row.usage_session_count)
        ));
        out.push_str(&format!("{}\n", cols.join("  ")));
    }

    append_legend(
        &mut out,
        &[
            "Cost = actual provider cost when present, otherwise API-equivalent estimated model cost.",
            "$/LOC uses accepted changed lines as the denominator.",
            "$/Mainline uses attributed heavy AI commits that reached mainline as the denominator.",
            "Coverage = sessions with priced cost / sessions with token usage.",
        ],
    );
    out
}

fn div(numerator: Option<f64>, denominator: i64) -> Option<f64> {
    if denominator <= 0 {
        return None;
    }
    numerator.map(|value| value / denominator as f64)
}

fn fmt_money(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("${value:.4}"),
        None => "N/A".to_string(),
    }
}

fn fmt_count(value: i64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn fmt_coverage(priced: i64, usage: i64) -> String {
    if usage <= 0 {
        return "N/A".to_string();
    }
    format!("{:.1}%", priced as f64 / usage as f64 * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::CostReportRow;

    #[test]
    fn render_cost_report_includes_denominators_and_coverage() {
        let rows = vec![CostReportRow {
            week_start: None,
            group_value: Some("codex/gpt-5.5".to_string()),
            branch_name: None,
            session_count: 2,
            accepted_session_count: 1,
            priced_session_count: 1,
            usage_session_count: 2,
            accepted_total_changed_lines: 25,
            total_tokens: 12_500,
            total_cost_usd: Some(1.25),
            mainline_change_count: 1,
        }];
        let report = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Model),
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

        let rendered = render_cost_report(&rows, &report, false);

        assert!(rendered.contains("Cost Metrics"));
        assert!(rendered.contains("codex/gpt-5.5"));
        assert!(rendered.contains("$1.2500"));
        assert!(rendered.contains("12.5K"));
        assert!(rendered.contains("50.0%"));
    }

    #[test]
    fn render_cost_report_suggests_all_projects_when_repo_auto_injected() {
        let report = ReportArgs {
            weekly: false,
            group_by: None,
            from: None,
            to: None,
            repo: Some("/tmp/sample-repo".to_string()),
            all_projects: false,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: 50,
        };

        let rendered = render_cost_report(&[], &report, true);
        assert!(rendered.contains("No cost rows found for the current repo."));
        assert!(rendered.contains("`--all-projects`"));
    }
}
