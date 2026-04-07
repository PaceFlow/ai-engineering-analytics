use anyhow::Result;
use rusqlite::Connection;

use crate::analytics;
use crate::change_intel::commit_assoc;
use crate::change_intel::pipeline;
use crate::db;
use crate::github;
use crate::ingest_progress::{
    IngestExecutionPlan, IngestProgress, IngestProgressObserver, ProviderWorkPlan,
};
use crate::providers;

pub fn run(verbose: bool) -> Result<()> {
    let mut db = db::open()?;
    let providers = providers::all_providers();
    println!("Planning ingest...");

    let mut provider_plans = Vec::with_capacity(providers.len());
    for provider in &providers {
        provider_plans.push(ProviderWorkPlan {
            provider_name: provider.name().to_string(),
            session_plan: provider.plan_session_work()?,
            code_change_plan: pipeline::plan_provider_code_changes(provider.name())?,
        });
    }
    let (association_units_estimate, commit_materialization_units_estimate) =
        estimate_execution_units(&db, &provider_plans)?;
    let execution_plan = IngestExecutionPlan::new(
        provider_plans,
        association_units_estimate,
        commit_materialization_units_estimate,
    );
    let mut progress = IngestProgress::new(&execution_plan);

    let mut grand_total = 0usize;
    for provider in &providers {
        let provider_name = provider.name();
        let provider_plan = execution_plan
            .provider_plan(provider_name)
            .ok_or_else(|| anyhow::anyhow!("missing provider plan for {}", provider_name))?;

        if verbose {
            println!("Ingesting {} ...", provider_name);
        }
        let session_result = {
            let result = {
                let mut observer = progress.stage(
                    format_provider_stage(provider_name, "sessions"),
                    provider_plan.session_units(),
                );
                provider.ingest(
                    &db,
                    &provider_plan.session_plan,
                    verbose,
                    Some(&mut observer),
                )
            };
            progress.finish_stage();
            result
        };
        match session_result {
            Ok(n) => {
                if verbose {
                    println!("  {} rows written", n);
                }
                grand_total += n;
            }
            Err(e) => println!("  error: {}", e),
        }

        let change_result = {
            let result = {
                let mut observer = progress.stage(
                    format_provider_stage(provider_name, "changes"),
                    provider_plan.code_change_units(),
                );
                pipeline::ingest_provider_code_changes(
                    &mut db,
                    &provider_plan.code_change_plan,
                    verbose,
                    Some(&mut observer),
                )
            };
            progress.finish_stage();
            result
        };
        match change_result {
            Ok(summary) => {
                let mut line = format!(
                    "  code changes [{}]: sources={} skipped={} calls={} ops={} parse_errors={}",
                    summary.provider,
                    summary.sources_discovered,
                    summary.sources_skipped,
                    summary.tool_calls_inspected,
                    summary.ops_upserted,
                    summary.parse_errors
                );
                if summary.legacy_sessions_considered > 0
                    || summary.legacy_entries_inspected > 0
                    || summary.legacy_diff_rows_found > 0
                    || summary.legacy_ops_upserted > 0
                    || summary.legacy_parse_errors > 0
                {
                    line.push_str(&format!(
                        " legacy[sessions={} entries={} diffs={} ops={} errors={}]",
                        summary.legacy_sessions_considered,
                        summary.legacy_entries_inspected,
                        summary.legacy_diff_rows_found,
                        summary.legacy_ops_upserted,
                        summary.legacy_parse_errors
                    ));
                }
                if verbose {
                    println!("{}", line);
                }
            }
            Err(e) => {
                println!("  code changes error: {}", e);
            }
        }
    }

    {
        {
            let mut observer = progress.stage("Session Events", 1);
            analytics::refresh_session_events(&db)?;
            observer.advance("session events refreshed");
        }
        progress.finish_stage();
    }

    if verbose {
        println!("\nAssociating commits ...");
    }
    let association_plan = commit_assoc::plan(&db)?;
    progress.replace_future_units(
        execution_plan.association_units_estimate,
        association_plan.total_units(),
    );
    let assoc_summary = {
        let result = {
            let mut observer = progress.stage("Commit Association", association_plan.total_units());
            commit_assoc::run_with_plan(&mut db, &association_plan, verbose, Some(&mut observer))
        };
        progress.finish_stage();
        result?
    };
    if verbose {
        for repo in &assoc_summary.repo_summaries {
            if repo.skipped_non_git {
                println!("  {} skipped (not a git repo)", repo.repo_root);
                continue;
            }

            println!(
                "  {} commits={} attributed={} heavy={} errors={}",
                repo.repo_root,
                repo.commits_scanned,
                repo.commits_attributed,
                repo.heavy_commits,
                repo.errors
            );
        }

        println!(
            "\nAssociation summary: repos_considered={} selected={} processed={} non_git_skipped={} commits_scanned={} commits_attributed={} heavy_commits={} errors={}",
            assoc_summary.repos_considered,
            assoc_summary.repos_selected,
            assoc_summary.repos_processed,
            assoc_summary.repos_skipped_non_git,
            assoc_summary.commits_scanned,
            assoc_summary.commits_attributed,
            assoc_summary.heavy_commits,
            assoc_summary.errors
        );
    }

    analytics::refresh_session_events(&db)?;

    let commit_materialization_units = count_commit_materialization_units(&db)?;
    progress.replace_future_units(
        execution_plan.commit_materialization_units_estimate,
        commit_materialization_units,
    );
    let commit_refresh = {
        let result = {
            let mut observer =
                progress.stage("Commit Materialization", commit_materialization_units);
            analytics::refresh_commit_events(&mut db, verbose, Some(&mut observer))
        };
        progress.finish_stage();
        result?
    };
    if verbose {
        println!(
            "Commit events materialized: repos={}/{} commits={}/{} elapsed={:.1}s",
            commit_refresh.repos_processed,
            commit_refresh.repos_total,
            commit_refresh.commits_processed,
            commit_refresh.commits_total,
            commit_refresh.elapsed_ms as f64 / 1000.0
        );
    }

    progress.finish();

    let github_token_configured = github::auth::github_token_source()?.is_some();
    let github_sync_plan = if github_token_configured {
        Some(github::sync::plan_github_pull_requests(&db)?)
    } else {
        None
    };
    let github_summary = match github_sync_plan {
        Some(plan) if plan.total_units() > 0 => {
            let mut github_progress = IngestProgress::new_for_total_units(plan.total_units());
            let result = {
                let result = {
                    let mut observer = github_progress.stage("GitHub Sync", plan.total_units());
                    github::sync::sync_github_pull_requests(&mut db, verbose, Some(&mut observer))
                };
                github_progress.finish_stage();
                result
            };
            result?
        }
        _ => github::sync::sync_github_pull_requests(&mut db, verbose, None)?,
    };
    let github_updated = github_summary.commit_lookups_enqueued > 0
        || github_summary.open_pull_requests_refreshed > 0
        || github_summary.issue_scans_enqueued > 0;
    let compact_github_status = if verbose {
        None
    } else if github_updated {
        Some("GitHub sync: updated")
    } else if github_token_configured {
        (github_summary.repos_considered > 0).then_some("GitHub sync: no updates")
    } else {
        Some("GitHub sync: skipped")
    };
    if verbose {
        if github_updated {
            println!(
                "GitHub PR sync: repos={} lookups={}/{} resolved={} no_pr={} failed={} rate_limited={} prs={} pr_commits={} refreshed_open_prs={} issue_scans={}/{} refreshed_issue_scans={} issues={} issue_fix_prs={} pr_removed_hashes={}",
                github_summary.repos_considered,
                github_summary.commit_lookups_completed,
                github_summary.commit_lookups_enqueued,
                github_summary.resolved_commits,
                github_summary.no_pr_commits,
                github_summary.failed_commits,
                github_summary.rate_limited_commits,
                github_summary.pull_requests_upserted,
                github_summary.pull_request_commits_upserted,
                github_summary.open_pull_requests_refreshed,
                github_summary.issue_scans_completed,
                github_summary.issue_scans_enqueued,
                github_summary.issue_scans_refreshed,
                github_summary.issues_upserted,
                github_summary.issue_fix_pull_requests_upserted,
                github_summary.pull_request_removed_hashes_upserted
            );
        } else if github_summary.repos_considered > 0 {
            if github_token_configured {
                println!("GitHub PR sync: no pending GitHub updates for eligible repos");
            } else {
                println!(
                    "GitHub PR sync: skipped remote fetch (run `paceflow github token` or set PACEFLOW_GITHUB_TOKEN to enable refresh)"
                );
            }
        }
    }
    if github_summary.repos_considered > 0 {
        if verbose {
            println!("\nRe-materializing commit events with GitHub evidence ...");
        }
        let commit_refresh = analytics::refresh_commit_events(&mut db, verbose, None)?;
        if verbose {
            println!(
                "Commit events materialized: repos={}/{} commits={}/{} elapsed={:.1}s",
                commit_refresh.repos_processed,
                commit_refresh.repos_total,
                commit_refresh.commits_processed,
                commit_refresh.commits_total,
                commit_refresh.elapsed_ms as f64 / 1000.0
            );
        }
    }
    if let Some(status) = compact_github_status {
        println!("{status}");
    }
    println!("Ingest complete");
    println!("Rows written: {}", grand_total);
    Ok(())
}

fn format_provider_stage(provider_name: &str, phase: &str) -> String {
    format!("{provider_name} {phase}")
}

fn estimate_execution_units(
    db: &Connection,
    provider_plans: &[ProviderWorkPlan],
) -> Result<(usize, usize)> {
    let existing_repo_count: i64 = db.query_row(
        "SELECT COUNT(DISTINCT repo_root)
         FROM fact_session_code_change
         WHERE repo_root IS NOT NULL AND TRIM(repo_root) != ''",
        [],
        |row| row.get(0),
    )?;
    let existing_commit_count: i64 =
        db.query_row("SELECT COUNT(*) FROM fact_commit", [], |row| row.get(0))?;
    let existing_repo_count = existing_repo_count.max(0) as usize;
    let existing_commit_count = existing_commit_count.max(0) as usize;

    let planned_work_units = provider_plans
        .iter()
        .map(|plan| plan.session_units() + plan.code_change_units())
        .sum::<usize>();
    let estimated_repo_count = existing_repo_count.max(usize::from(planned_work_units > 0));
    let estimated_commit_count = existing_commit_count
        .max(planned_work_units)
        .max(estimated_repo_count * 25);
    let association_units_estimate = estimated_repo_count + estimated_commit_count;
    let commit_materialization_units_estimate = estimated_commit_count;

    Ok((
        association_units_estimate,
        commit_materialization_units_estimate,
    ))
}

fn count_commit_materialization_units(db: &Connection) -> Result<usize> {
    let commit_count: i64 =
        db.query_row("SELECT COUNT(*) FROM fact_commit", [], |row| row.get(0))?;
    Ok(commit_count.max(0) as usize)
}
