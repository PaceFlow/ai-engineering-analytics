mod cli;
mod change_intel;
mod commands;
mod db;
mod error;
mod events;
mod metrics;
mod path_utils;
mod providers;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest => commands::ingest::run(cli.verbose)?,
        Commands::AssociateCommits(args) => commands::associate_commits::run(args, cli.verbose)?,
        Commands::Stats => commands::stats::run()?,
        Commands::TaskStats(args) => commands::task_stats::run(args)?,
    }
    Ok(())
}
