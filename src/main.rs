use clap::Parser;
use paceflow::cli::{Cli, Commands};
use paceflow::commands;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest(args) => commands::ingest::run(cli.verbose, args)?,
        Commands::Session(args) => commands::session::run(args)?,
        Commands::Delivery(args) => commands::delivery::run(args)?,
        Commands::Quality(args) => commands::quality::run(args)?,
        Commands::Cost(args) => commands::cost::run(args)?,
        Commands::EventStream(args) => commands::event_stream::run(args)?,
        Commands::GitHub(args) => commands::github::run(args)?,
        Commands::Sync(args) => commands::sync::run(args)?,
        Commands::Hooks(args) => commands::hooks::run(args)?,
        Commands::Tui(args) => commands::tui::run(args)?,
    }
    Ok(())
}
