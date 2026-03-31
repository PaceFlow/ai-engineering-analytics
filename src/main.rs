mod analytics;
mod change_intel;
mod cli;
mod commands;
mod cursor_paths;
mod db;
mod error;
mod ingest_progress;
mod path_utils;
mod providers;
mod sync_identity;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest => commands::ingest::run(cli.verbose)?,
        Commands::Session(args) => commands::session::run(args)?,
        Commands::Delivery(args) => commands::delivery::run(args)?,
        Commands::Quality(args) => commands::quality::run(args)?,
        Commands::EventStream(args) => commands::event_stream::run(args)?,
    }
    Ok(())
}
