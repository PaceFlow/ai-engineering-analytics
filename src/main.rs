mod cli;
mod change_intel;
mod commands;
mod db;
mod error;
mod events;
mod path_utils;
mod providers;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest(args) => commands::ingest::run(args.with_code_changes, cli.verbose)?,
        Commands::AssociateCommits(args) => commands::associate_commits::run(args, cli.verbose)?,
        Commands::Stats => commands::stats::run()?,
    }
    Ok(())
}
