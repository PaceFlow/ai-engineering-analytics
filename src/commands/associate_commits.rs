use anyhow::Result;

use crate::change_intel::commit_assoc;
use crate::change_intel::commit_assoc::types::RunOptions;
use crate::cli::AssociateCommitsArgs;
use crate::db;

pub fn run(args: AssociateCommitsArgs, verbose: bool) -> Result<()> {
    let db = db::open()?;
    let options = RunOptions {
        repos: args.repo,
        recompute: args.recompute,
        max_commits: args.max_commits,
        include_merges: args.include_merges,
    };

    println!("Associating commits ...");
    let summary = commit_assoc::run(&db, options, verbose)?;

    for repo in &summary.repo_summaries {
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
        "\nSummary: repos_considered={} selected={} processed={} non_git_skipped={} commits_scanned={} commits_attributed={} heavy_commits={} errors={}",
        summary.repos_considered,
        summary.repos_selected,
        summary.repos_processed,
        summary.repos_skipped_non_git,
        summary.commits_scanned,
        summary.commits_attributed,
        summary.heavy_commits,
        summary.errors
    );

    Ok(())
}
