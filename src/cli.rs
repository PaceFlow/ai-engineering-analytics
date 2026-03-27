use clap::{Args, Parser, Subcommand, ValueEnum};

const SESSION_AFTER_HELP: &str = "Examples:\n  aieng session\n  aieng session --group-by provider\n  aieng session --list-sessions\n\nMetrics:\n  Average user prompts: average number of user prompts per session.\n  Avg time to first accepted change: minutes from session start to the first accepted code change.\n  Debug loop rate: share of sessions that look like repeated fix-retry cycles.\n  Error paste rate: share of sessions where an error message was pasted mid-session.\n  Session-to-commit rate: share of sessions followed by a commit within 4 hours.\n  No-output session rate: share of sessions with no accepted code changes.";
const CHANGE_AFTER_HELP: &str = "Examples:\n  aieng change\n  aieng change --group-by provider\n  aieng change --group-by task --task ABC-123\n\nMetrics:\n  Heavy commits: commits where matched AI-attributed lines are at least half of changed lines.\n  C2 merge rate: share of heavy AI commits that later reached mainline.";
const LIFECYCLE_AFTER_HELP: &str = "Examples:\n  aieng lifecycle\n  aieng lifecycle --group-by provider\n  aieng lifecycle --group-by task --task ABC-123\n\nMetrics:\n  L1 code churn rate: share of AI-added lines on heavy AI commits that were removed again within the churn window.\n  L4 revert rate: share of heavy AI commits that were later reverted.";

#[derive(Parser)]
#[command(
    name = "aieng",
    about = "Local-first analytics for improving agent-assisted engineering outcomes",
    after_help = "Quick start:\n  aieng ingest\n  aieng session\n  aieng change\n  aieng lifecycle\n\nStart here:\n  aieng session          # find noisy or productive sessions\n  aieng change           # see whether AI-heavy work shipped\n  aieng lifecycle        # see whether accepted code held up\n\nManual validation:\n  aieng event-stream --stream session-base\n\nDiscover options:\n  aieng --help\n  aieng <command> --help"
)]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Build the analytics data model from local sessions, code changes, and git history
    Ingest,
    /// Show session efficiency and delivery metrics
    Session(SessionReportArgs),
    /// Show commit attribution and merge outcome metrics
    Change(ChangeReportArgs),
    /// Show churn and revert follow-through for heavy AI commits
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
    /// Show results across all tracked projects instead of defaulting to the current repo
    #[arg(long)]
    pub all_projects: bool,
    /// Restrict to a provider (for change/lifecycle this can include `human`)
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
#[command(after_help = SESSION_AFTER_HELP)]
pub struct SessionReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
    /// List per-session productivity rows instead of KPI aggregations
    #[arg(long)]
    pub list_sessions: bool,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = CHANGE_AFTER_HELP)]
pub struct ChangeReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = LIFECYCLE_AFTER_HELP)]
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
    /// Restrict to a provider (for commit-session streams this can include `human`)
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
    use clap::{CommandFactory, Parser};

    #[test]
    fn parses_session_group_by_repo() {
        let cli = Cli::parse_from(["aieng", "session", "--group-by", "repo"]);
        match cli.command {
            Commands::Session(args) => assert_eq!(args.report.group_by, Some(GroupBy::Repo)),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_change_weekly_group_by_task() {
        let cli = Cli::parse_from(["aieng", "change", "--weekly", "--group-by", "task"]);
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
        let cli = Cli::parse_from(["aieng", "lifecycle", "--model", "gpt-5"]);
        match cli.command {
            Commands::Lifecycle(args) => assert_eq!(args.report.model.as_deref(), Some("gpt-5")),
            _ => panic!("expected lifecycle command"),
        }
    }

    #[test]
    fn parses_report_all_projects_flag() {
        let cli = Cli::parse_from(["aieng", "session", "--all-projects"]);
        match cli.command {
            Commands::Session(args) => assert!(args.report.all_projects),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_event_stream_defaults() {
        let cli = Cli::parse_from(["aieng", "event-stream"]);
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
        let cli = Cli::parse_from(["aieng", "event-stream", "--category", "session"]);
        match cli.command {
            Commands::EventStream(args) => assert_eq!(args.category, EventCategory::Session),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_stream_and_task_filter() {
        let cli = Cli::parse_from([
            "aieng",
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
        let cli = Cli::parse_from(["aieng", "event-stream", "--pretty"]);
        match cli.command {
            Commands::EventStream(args) => assert!(args.pretty),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn session_help_explains_metrics() {
        let mut command = Cli::command();
        let mut buffer = Vec::new();
        command
            .find_subcommand_mut("session")
            .expect("session subcommand")
            .write_long_help(&mut buffer)
            .expect("write session help");
        let help = String::from_utf8(buffer).expect("utf8");

        assert!(help.contains("Average user prompts"));
        assert!(help.contains("Debug loop rate"));
        assert!(help.contains("Session-to-commit rate"));
    }

    #[test]
    fn change_and_lifecycle_help_explain_metrics_and_human_provider_context() {
        let mut command = Cli::command();
        let mut change_buffer = Vec::new();
        command
            .find_subcommand_mut("change")
            .expect("change subcommand")
            .write_long_help(&mut change_buffer)
            .expect("write change help");
        let change_help = String::from_utf8(change_buffer).expect("utf8");
        assert!(change_help.contains("Heavy commits"));
        assert!(change_help.contains("C2 merge rate"));

        let mut command = Cli::command();
        let mut lifecycle_buffer = Vec::new();
        command
            .find_subcommand_mut("lifecycle")
            .expect("lifecycle subcommand")
            .write_long_help(&mut lifecycle_buffer)
            .expect("write lifecycle help");
        let lifecycle_help = String::from_utf8(lifecycle_buffer).expect("utf8");
        assert!(lifecycle_help.contains("L1 code churn rate"));
        assert!(lifecycle_help.contains("L4 revert rate"));
    }
}
