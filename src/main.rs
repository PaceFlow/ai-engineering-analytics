use ai_engineering_analytics::cli::{Cli, Commands};
use ai_engineering_analytics::commands;
use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ingest => commands::ingest::run(cli.verbose)?,
        Commands::Session(args) => commands::session::run(args)?,
        Commands::Delivery(args) => commands::delivery::run(args)?,
        Commands::Quality(args) => commands::quality::run(args)?,
        Commands::EventStream(args) => commands::event_stream::run(args)?,
        Commands::GitHub(args) => commands::github::run(args)?,
        Commands::Tui(args) => commands::tui::run(args)?,
    }
    Ok(())
}
