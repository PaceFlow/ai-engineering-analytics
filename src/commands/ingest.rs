use anyhow::Result;

use crate::analytics;
use crate::change_intel::commit_assoc;
use crate::change_intel::pipeline;
use crate::db;
use crate::providers;

pub fn run(verbose: bool) -> Result<()> {
    let mut db = db::open()?;
    let providers = providers::all_providers();

    let mut grand_total = 0usize;
    for provider in &providers {
        let provider_name = provider.name();
        println!("Ingesting {} ...", provider_name);
        match provider.ingest(&db, verbose) {
            Ok(n) => {
                println!("  {} rows written", n);
                grand_total += n;
            }
            Err(e) => println!("  error: {}", e),
        }

        match pipeline::ingest_provider_code_changes(&mut db, provider_name, verbose) {
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
                println!("{}", line);
            }
            Err(e) => {
                println!("  code changes error: {}", e);
            }
        }
    }

    analytics::refresh_session_events(&db)?;

    println!("\nAssociating commits ...");
    let assoc_summary = commit_assoc::run(&mut db, verbose)?;
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

    println!("\nMaterializing commit events ...");
    let commit_refresh = analytics::refresh_commit_events(&mut db, verbose)?;
    println!(
        "Commit events materialized: repos={}/{} commits={}/{} elapsed={}",
        commit_refresh.repos_processed,
        commit_refresh.repos_total,
        commit_refresh.commits_processed,
        commit_refresh.commits_total,
        format!("{:.1}s", commit_refresh.elapsed_ms as f64 / 1000.0)
    );
    println!("\nTotal rows written: {}", grand_total);
    Ok(())
}
