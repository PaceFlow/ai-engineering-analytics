use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "vca",
    about = "Vibe coding analytics for AI-assisted development sessions",
    after_help = "Quick start:\n  vca ingest --with-code-changes\n  vca associate-commits --repo /absolute/repo/path\n  vca task-stats --limit 20\n  vca stats\n\nDiscover options:\n  vca --help\n  vca <command> --help"
)]
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
    /// Show global quality metrics and per-session productivity stats
    Stats,
    /// Show per-task quality metrics (S2, S4, S6, S9) and change size vs staging
    TaskStats(TaskStatsArgs),
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

#[derive(Args, Debug, Clone)]
pub struct TaskStatsArgs {
    /// Restrict to a specific task key (ticket format, e.g. ABC-123)
    #[arg(long)]
    pub task: Option<String>,
    /// Max number of tasks to display
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}
