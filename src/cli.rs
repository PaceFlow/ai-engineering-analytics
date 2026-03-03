use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "vca", about = "Vibe coding analytics — measure how productive you are with your coding sessions")]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Ingest conversation data from all providers into the local DB
    Ingest,
    /// Show analytics: chars written per LOC changed
    Stats,
}
