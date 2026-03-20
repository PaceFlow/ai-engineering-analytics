use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "vca",
    about = "Vibe coding analytics for AI-assisted development sessions",
    after_help = "Quick start:\n  vca ingest\n  vca session\n  vca change\n  vca lifecycle\n\nManual validation:\n  vca event-stream --stream session-base\n\nDiscover options:\n  vca --help\n  vca <command> --help"
)]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Build the full analytics data model from local sessions, code changes, and git history
    Ingest,
    /// Show session KPIs and session-level breakdowns
    Session(SessionReportArgs),
    /// Show change KPIs and change-level breakdowns
    Change(ChangeReportArgs),
    /// Show lifecycle KPIs and lifecycle-level breakdowns
    Lifecycle(LifecycleReportArgs),
    /// Print analytics-ready base-view rows as NDJSON for manual validation
    EventStream(EventStreamArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GroupBy {
    Repo,
    Provider,
    Task,
    Model,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EventCategory {
    Session,
    Change,
    Lifecycle,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EventStreamKind {
    SessionBase,
    TaskSessionBase,
    ChangeBase,
    CommitSessionBase,
    TaskCommitBase,
    All,
}

#[derive(Args, Debug, Clone)]
pub struct ReportArgs {
    /// Bucket results by calendar week
    #[arg(long)]
    pub weekly: bool,
    /// Group aggregated results by a dimension
    #[arg(long, value_enum)]
    pub group_by: Option<GroupBy>,
    /// Inclusive start date (YYYY-MM-DD)
    #[arg(long, value_name = "YYYY-MM-DD")]
    pub from: Option<String>,
    /// Inclusive end date (YYYY-MM-DD)
    #[arg(long, value_name = "YYYY-MM-DD")]
    pub to: Option<String>,
    /// Restrict to a specific repository root
    #[arg(long)]
    pub repo: Option<String>,
    /// Restrict to a provider
    #[arg(long)]
    pub provider: Option<String>,
    /// Restrict to a specific task key (ticket format, e.g. ABC-123)
    #[arg(long)]
    pub task: Option<String>,
    /// Restrict to a specific model name
    #[arg(long)]
    pub model: Option<String>,
    /// Max number of grouped rows to display
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}

#[derive(Args, Debug, Clone)]
pub struct SessionReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
    /// List per-session productivity rows instead of KPI aggregations
    #[arg(long)]
    pub list_sessions: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ChangeReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
}

#[derive(Args, Debug, Clone)]
pub struct LifecycleReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
}

#[derive(Args, Debug, Clone)]
pub struct EventStreamArgs {
    /// Restrict output to a KPI category
    #[arg(long, value_enum, default_value_t = EventCategory::All)]
    pub category: EventCategory,
    /// Restrict output to a specific base stream
    #[arg(long, value_enum, default_value_t = EventStreamKind::All)]
    pub stream: EventStreamKind,
    /// Inclusive start date (YYYY-MM-DD)
    #[arg(long, value_name = "YYYY-MM-DD")]
    pub from: Option<String>,
    /// Inclusive end date (YYYY-MM-DD)
    #[arg(long, value_name = "YYYY-MM-DD")]
    pub to: Option<String>,
    /// Restrict to a specific repository root
    #[arg(long)]
    pub repo: Option<String>,
    /// Restrict to a provider
    #[arg(long)]
    pub provider: Option<String>,
    /// Restrict to a specific task key (ticket format, e.g. ABC-123)
    #[arg(long)]
    pub task: Option<String>,
    /// Restrict to a specific model name
    #[arg(long)]
    pub model: Option<String>,
    /// Max number of stream rows to display
    #[arg(long)]
    pub limit: Option<usize>,
    /// Pretty-print each event as formatted JSON instead of NDJSON
    #[arg(long)]
    pub pretty: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_session_group_by_repo() {
        let cli = Cli::parse_from(["vca", "session", "--group-by", "repo"]);
        match cli.command {
            Commands::Session(args) => assert_eq!(args.report.group_by, Some(GroupBy::Repo)),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_change_weekly_group_by_task() {
        let cli = Cli::parse_from(["vca", "change", "--weekly", "--group-by", "task"]);
        match cli.command {
            Commands::Change(args) => {
                assert!(args.report.weekly);
                assert_eq!(args.report.group_by, Some(GroupBy::Task));
            }
            _ => panic!("expected change command"),
        }
    }

    #[test]
    fn parses_lifecycle_model_filter() {
        let cli = Cli::parse_from(["vca", "lifecycle", "--model", "gpt-5"]);
        match cli.command {
            Commands::Lifecycle(args) => assert_eq!(args.report.model.as_deref(), Some("gpt-5")),
            _ => panic!("expected lifecycle command"),
        }
    }

    #[test]
    fn parses_event_stream_defaults() {
        let cli = Cli::parse_from(["vca", "event-stream"]);
        match cli.command {
            Commands::EventStream(args) => {
                assert_eq!(args.category, EventCategory::All);
                assert_eq!(args.stream, EventStreamKind::All);
                assert!(!args.pretty);
            }
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_category_session() {
        let cli = Cli::parse_from(["vca", "event-stream", "--category", "session"]);
        match cli.command {
            Commands::EventStream(args) => assert_eq!(args.category, EventCategory::Session),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_stream_and_task_filter() {
        let cli = Cli::parse_from([
            "vca",
            "event-stream",
            "--stream",
            "task-commit-base",
            "--task",
            "PAC-999",
        ]);
        match cli.command {
            Commands::EventStream(args) => {
                assert_eq!(args.stream, EventStreamKind::TaskCommitBase);
                assert_eq!(args.task.as_deref(), Some("PAC-999"));
            }
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_pretty_flag() {
        let cli = Cli::parse_from(["vca", "event-stream", "--pretty"]);
        match cli.command {
            Commands::EventStream(args) => assert!(args.pretty),
            _ => panic!("expected event-stream command"),
        }
    }
}
