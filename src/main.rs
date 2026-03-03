mod cli;
mod commands;
mod db;
mod error;
mod events;
mod providers;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest => commands::ingest::run(cli.verbose)?,
        Commands::Stats => commands::stats::run()?,
    }
    Ok(())
}
