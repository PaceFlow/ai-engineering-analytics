mod analytics;
mod cli;
mod change_intel;
mod commands;
mod db;
mod error;
mod path_utils;
mod providers;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest => commands::ingest::run(cli.verbose)?,
        Commands::Session(args) => commands::session::run(args)?,
        Commands::Change(args) => commands::change::run(args)?,
        Commands::Lifecycle(args) => commands::lifecycle::run(args)?,
        Commands::EventStream(args) => commands::event_stream::run(args)?,
    }
    Ok(())
}
