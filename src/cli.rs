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
    /// Associate git commits with AI-generated code via line-hash matching
    AssociateCommits(AssociateCommitsArgs),
    /// Show analytics: chars written per LOC changed
    Stats,
}

#[derive(Args, Debug, Clone)]
pub struct IngestArgs {
    /// Also ingest code-change hashes for later git-diff matching
    #[arg(long)]
    pub with_code_changes: bool,
}

#[derive(Args, Debug, Clone)]
pub struct AssociateCommitsArgs {
    /// Restrict to one or more repository roots from change_ops
    #[arg(long = "repo")]
    pub repo: Vec<String>,
    /// Recompute commit attribution in scope
    #[arg(long)]
    pub recompute: bool,
    /// Optional cap on number of commits processed per repo
    #[arg(long)]
    pub max_commits: Option<usize>,
    /// Include merge commits (disabled by default)
    #[arg(long)]
    pub include_merges: bool,
}
