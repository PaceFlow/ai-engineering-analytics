use clap::{Args, Parser, Subcommand};

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
    Ingest(IngestArgs),
    /// Show analytics: chars written per LOC changed
    Stats,
}

#[derive(Args, Debug, Clone)]
pub struct IngestArgs {
    /// Also ingest code-change hashes for later git-diff matching
    #[arg(long)]
    pub with_code_changes: bool,
}
