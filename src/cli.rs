use clap::{Args, Parser, Subcommand, ValueEnum};

const SESSION_AFTER_HELP: &str = "Examples:\n  paceflow session                 # default: grouped by model\n  paceflow session --model codex/gpt-5.4\n  paceflow session --overall\n  paceflow session --group-by provider\n  paceflow session --list-sessions\n\nMetrics:\n  Average user prompts: average number of user prompts per session.\n  Avg time to first accepted change: minutes from session start to the first accepted code change.\n  Debug loop rate: share of sessions that look like repeated fix-retry cycles.\n  Error paste rate: share of sessions where an error message was pasted mid-session.\n  Session-to-commit rate: share of sessions followed by a commit within 4 hours.\n  No-output session rate: share of sessions with no accepted code changes.";
const DELIVERY_AFTER_HELP: &str = "Examples:\n  paceflow delivery                # default: grouped by model\n  paceflow delivery --model codex/gpt-5.4\n  paceflow delivery --overall\n  paceflow delivery --group-by provider\n  paceflow delivery --group-by task --task ABC-123\n\nMetrics:\n  Heavy commits: commits where matched AI-attributed lines are at least half of changed lines.\n  PR sync: completed GitHub PR lookups per heavy commit on github.com (see table column).\n  PR reach rate: among completed lookups, share where a pull request existed.\n  Mainline reach rate: share of heavy AI commits that later reached mainline.\n  PR merge rate: among completed PR-linked lookups, share whose PR merged.";
const QUALITY_AFTER_HELP: &str = "Examples:\n  paceflow quality                 # default: grouped by model\n  paceflow quality --model codex/gpt-5.4\n  paceflow quality --overall\n  paceflow quality --group-by provider\n  paceflow quality --group-by task --task ABC-123\n\nMetrics:\n  Code churn rate: share of AI-added lines on heavy AI commits that were removed again within the churn window.\n  Bug-after-merge rate: share of merged heavy AI commits that drew a later fix-like commit within 60 days.\n  Revert rate: share of heavy AI commits that were later reverted.";
const GITHUB_AFTER_HELP: &str = "Examples:\n  paceflow github token\n\nGitHub token setup:\n  Use this command to save, replace, or delete the local GitHub token used for PR sync during ingest.";

#[derive(Parser)]
#[command(
    name = "paceflow",
    about = "Local-first analytics for improving agent-assisted engineering outcomes",
    after_help = "Quick start:\n  paceflow ingest\n  paceflow session\n  paceflow delivery\n  paceflow quality\n\nStart here:\n  paceflow session       # default: compare workflow trust by model\n  paceflow delivery      # default: compare ship-rate by model\n  paceflow quality       # default: compare durability by model\n\nManual validation:\n  paceflow event-stream --stream session-base\n\nDiscover options:\n  paceflow --help\n  paceflow <command> --help"
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
    Delivery(DeliveryReportArgs),
    /// Show churn, bug-fix, and revert follow-through for heavy AI commits
    Quality(QualityReportArgs),
    /// Print analytics-ready base-view rows as NDJSON for manual validation
    EventStream(EventStreamArgs),
    #[command(name = "github")]
    /// Manage GitHub token setup for live PR sync
    GitHub(GitHubArgs),
}

#[derive(Args, Debug, Clone)]
#[command(after_help = GITHUB_AFTER_HELP)]
pub struct GitHubArgs {
    #[command(subcommand)]
    pub command: GitHubCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum GitHubCommands {
    /// Save, replace, or delete the local GitHub token
    Token,
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
    Delivery,
    Quality,
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
    /// Restrict to a provider (for delivery/quality this can include `human`)
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
    /// Show the overall report instead of the default model-grouped comparison
    #[arg(long, conflicts_with = "group_by")]
    pub overall: bool,
    /// List per-session productivity rows instead of KPI aggregations
    #[arg(long)]
    pub list_sessions: bool,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = DELIVERY_AFTER_HELP)]
pub struct DeliveryReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
    /// Show the overall report instead of the default model-grouped comparison
    #[arg(long, conflicts_with = "group_by")]
    pub overall: bool,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = QUALITY_AFTER_HELP)]
pub struct QualityReportArgs {
    #[command(flatten)]
    pub report: ReportArgs,
    /// Show the overall report instead of the default model-grouped comparison
    #[arg(long, conflicts_with = "group_by")]
    pub overall: bool,
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
    use clap::{CommandFactory, Parser, error::ErrorKind};

    #[test]
    fn parses_session_group_by_repo() {
        let cli = Cli::parse_from(["paceflow", "session", "--group-by", "repo"]);
        match cli.command {
            Commands::Session(args) => assert_eq!(args.report.group_by, Some(GroupBy::Repo)),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_delivery_weekly_group_by_task() {
        let cli = Cli::parse_from(["paceflow", "delivery", "--weekly", "--group-by", "task"]);
        match cli.command {
            Commands::Delivery(args) => {
                assert!(args.report.weekly);
                assert_eq!(args.report.group_by, Some(GroupBy::Task));
            }
            _ => panic!("expected delivery command"),
        }
    }

    #[test]
    fn parses_quality_model_filter() {
        let cli = Cli::parse_from(["paceflow", "quality", "--model", "gpt-5"]);
        match cli.command {
            Commands::Quality(args) => assert_eq!(args.report.model.as_deref(), Some("gpt-5")),
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn parses_report_all_projects_flag() {
        let cli = Cli::parse_from(["paceflow", "session", "--all-projects"]);
        match cli.command {
            Commands::Session(args) => assert!(args.report.all_projects),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_session_overall_flag() {
        let cli = Cli::parse_from(["paceflow", "session", "--overall"]);
        match cli.command {
            Commands::Session(args) => assert!(args.overall),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn overall_conflicts_with_group_by() {
        let result =
            Cli::try_parse_from(["paceflow", "delivery", "--overall", "--group-by", "model"]);
        assert!(result.is_err());
        let err = result.err().expect("expected clap conflict");
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parses_event_stream_defaults() {
        let cli = Cli::parse_from(["paceflow", "event-stream"]);
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
        let cli = Cli::parse_from(["paceflow", "event-stream", "--category", "session"]);
        match cli.command {
            Commands::EventStream(args) => assert_eq!(args.category, EventCategory::Session),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_delivery_category_and_task_filter() {
        let cli = Cli::parse_from([
            "paceflow",
            "event-stream",
            "--category",
            "delivery",
            "--stream",
            "task-commit-base",
            "--task",
            "PAC-999",
        ]);
        match cli.command {
            Commands::EventStream(args) => {
                assert_eq!(args.category, EventCategory::Delivery);
                assert_eq!(args.stream, EventStreamKind::TaskCommitBase);
                assert_eq!(args.task.as_deref(), Some("PAC-999"));
            }
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_event_stream_pretty_flag() {
        let cli = Cli::parse_from(["paceflow", "event-stream", "--pretty"]);
        match cli.command {
            Commands::EventStream(args) => assert!(args.pretty),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_github_token_command() {
        let cli = Cli::parse_from(["paceflow", "github", "token"]);
        match cli.command {
            Commands::GitHub(args) => match args.command {
                GitHubCommands::Token => {}
            },
            _ => panic!("expected github command"),
        }
    }

    #[test]
    fn rejects_legacy_change_and_lifecycle_commands() {
        assert!(Cli::try_parse_from(["paceflow", "change"]).is_err());
        assert!(Cli::try_parse_from(["paceflow", "lifecycle"]).is_err());
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
    fn delivery_and_quality_help_explain_metrics_and_human_provider_context() {
        let mut command = Cli::command();
        let mut delivery_buffer = Vec::new();
        command
            .find_subcommand_mut("delivery")
            .expect("delivery subcommand")
            .write_long_help(&mut delivery_buffer)
            .expect("write delivery help");
        let delivery_help = String::from_utf8(delivery_buffer).expect("utf8");
        assert!(delivery_help.contains("Heavy commits"));
        assert!(delivery_help.contains("PR reach rate"));
        assert!(delivery_help.contains("Mainline reach rate"));
        assert!(delivery_help.contains("PR merge rate"));

        let mut command = Cli::command();
        let mut quality_buffer = Vec::new();
        command
            .find_subcommand_mut("quality")
            .expect("quality subcommand")
            .write_long_help(&mut quality_buffer)
            .expect("write quality help");
        let quality_help = String::from_utf8(quality_buffer).expect("utf8");
        assert!(quality_help.contains("Code churn rate"));
        assert!(quality_help.contains("Bug-after-merge rate"));
        assert!(quality_help.contains("Revert rate"));
    }
}
