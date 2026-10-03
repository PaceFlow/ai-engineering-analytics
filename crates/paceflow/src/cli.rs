use clap::{Args, Parser, Subcommand};
pub use vca::cli::*;

const SYNC_AFTER_HELP: &str = "Examples:\n  paceflow sync config\n  paceflow sync status\n  paceflow sync push --all-projects\n  paceflow sync schedule install\n\nSync setup:\n  Use `paceflow sync config` to authenticate with the PaceFlow backend and choose a default organization.\n  Sync uploads normalized local analytics events so shared org views stay consistent across devices.";
const SYNC_SCHEDULE_AFTER_HELP: &str = "Examples:\n  paceflow sync schedule install\n  paceflow sync schedule status\n  paceflow sync schedule uninstall\n  paceflow sync schedule run\n\nSchedule setup:\n  Installs a user-level Paceflow schedule that runs ingest and sync push --all-projects every 6 hours.";
const HOOKS_AFTER_HELP: &str = "Examples:\n  paceflow hooks install                         # install the composite pre-commit hook in the current repo\n  paceflow hooks install --repo /path/to/repo    # install in a specific repo\n  paceflow hooks install --force                 # overwrite an existing foreign hook instead of chaining it\n  paceflow hooks status\n  paceflow hooks pre-commit --repo .             # dry-run the gate against the current repo\n  paceflow hooks uninstall\n\nHook setup:\n  Paceflow installs a composite pre-commit hook that (1) verifies sync is configured locally and that the periodic sync schedule can be installed, then (2) runs the repository's own pre-commit checks so they are not skipped.\n  The gate fails the commit if `paceflow sync config` has not been run (or the PACEFLOW_SYNC_* env vars are not set), and installs the periodic sync schedule on first use.\n\nCoexistence:\n  If a foreign pre-commit hook already exists, Paceflow backs it up to `pre-commit.paceflow-chained` and runs it after the gate (use --force to overwrite instead). `paceflow hooks uninstall` restores the backup.\n  If a `.pre-commit-config.yaml` is present and `pre-commit` is on PATH, the composite hook invokes `pre-commit run --hook-stage pre-commit` — so you do not need to run `pre-commit install`. `paceflow hooks status` warns when configured checks would not run.";

/// Version string baked at build time, e.g.
/// `0.2.0 (abc123def456 clean, 2026-05-13T15:00:00+03:00)`.
/// The git metadata comes from `build.rs`; if `git` is unavailable the
/// fields fall back to `unknown` so the binary still reports a sensible
/// version.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("VCA_GIT_HASH"),
    " ",
    env!("VCA_GIT_DIRTY"),
    ", ",
    env!("VCA_GIT_COMMIT_TIME"),
    ")"
);
const TOP_LEVEL_AFTER_HELP: &str = "Quick start:\n  paceflow ingest\n  paceflow session\n  paceflow delivery\n  paceflow quality\n  paceflow cost\n\nStart here:\n  paceflow session       # default: compare workflow trust by model\n  paceflow delivery      # default: compare ship-rate by model\n  paceflow quality       # default: compare durability by model\n  paceflow cost          # default: compare spend by model\n\nTeam setup:\n  paceflow github token              # save the GitHub token used for PR sync\n  paceflow sync config               # authenticate and pick a default PaceFlow org\n  paceflow sync schedule install     # install the 6-hour ingest + push schedule\n  paceflow hooks install             # install the pre-commit setup gate in this repo\n\nManual validation:\n  paceflow event-stream --stream session-base\n\nDiscover options:\n  paceflow --help\n  paceflow <command> --help";

#[derive(Parser)]
#[command(
    name = "paceflow",
    version = VERSION,
    about = "Local-first analytics for improving agent-assisted engineering outcomes",
    after_help = TOP_LEVEL_AFTER_HELP
)]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(flatten)]
    Analytics(vca::cli::Commands),
    #[command(name = "sync")]
    /// Configure and push shared analytics sync to the PaceFlow backend
    Sync(SyncArgs),
    #[command(name = "hooks")]
    /// Install and manage Paceflow git hooks
    Hooks(HooksArgs),
}

#[derive(Args, Debug, Clone)]
#[command(after_help = SYNC_AFTER_HELP)]
pub struct SyncArgs {
    #[command(subcommand)]
    pub command: SyncCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum SyncCommands {
    /// Authenticate and save the default PaceFlow organization for sync
    Config,
    /// Upload pending normalized analytics events for the current repo or all projects
    Push(SyncPushArgs),
    /// Show local pending sync state and remote org sync status
    Status(SyncStatusArgs),
    /// Install, inspect, or run the periodic all-projects sync schedule
    Schedule(SyncScheduleArgs),
    /// Delete saved sync credentials and clear local sync cursors
    Reset,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = SYNC_SCHEDULE_AFTER_HELP)]
pub struct SyncScheduleArgs {
    #[command(subcommand)]
    pub command: SyncScheduleCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum SyncScheduleCommands {
    /// Install or update the user-level periodic all-projects sync schedule
    Install,
    /// Show whether the periodic all-projects sync schedule is installed
    Status,
    /// Remove the Paceflow-managed periodic all-projects sync schedule
    Uninstall,
    /// Run one scheduled ingest and all-projects sync push pass
    Run,
}

#[derive(Args, Debug, Clone)]
#[command(after_help = HOOKS_AFTER_HELP)]
pub struct HooksArgs {
    #[command(subcommand)]
    pub command: HooksCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum HooksCommands {
    /// Install the Paceflow-managed pre-commit hook (composes with existing hooks)
    Install(HooksInstallArgs),
    /// Remove the Paceflow-managed pre-commit setup gate
    Uninstall(HooksRepoArgs),
    /// Show whether the Paceflow-managed pre-commit hook is installed
    Status(HooksRepoArgs),
    /// Run the local-only pre-commit setup gate
    #[command(name = "pre-commit")]
    PreCommit(HooksRepoArgs),
}

#[derive(Args, Debug, Clone)]
pub struct HooksRepoArgs {
    /// Restrict hook management to a specific repository root or path inside a repository
    #[arg(long)]
    pub repo: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct HooksInstallArgs {
    /// Restrict hook management to a specific repository root or path inside a repository
    #[arg(long)]
    pub repo: Option<String>,
    /// Overwrite an existing foreign pre-commit hook instead of chaining to it
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug, Clone)]
pub struct SyncPushArgs {
    /// Show results across all tracked projects instead of defaulting to the current repo
    #[arg(long)]
    pub all_projects: bool,
    /// Restrict sync to a specific repository root
    #[arg(long)]
    pub repo: Option<String>,
    /// Max number of events to upload per request
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
}

#[derive(Args, Debug, Clone)]
pub struct SyncStatusArgs {
    /// Show results across all tracked projects instead of defaulting to the current repo
    #[arg(long)]
    pub all_projects: bool,
    /// Restrict status to a specific repository root
    #[arg(long)]
    pub repo: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser, error::ErrorKind};

    #[test]
    fn parses_session_group_by_repo() {
        let cli = Cli::parse_from(["paceflow", "session", "--group-by", "repo"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Session(args)) => {
                assert_eq!(args.report.group_by, Some(GroupBy::Repo))
            }
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_delivery_weekly_group_by_task() {
        let cli = Cli::parse_from(["paceflow", "delivery", "--weekly", "--group-by", "task"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Delivery(args)) => {
                assert!(args.report.weekly);
                assert_eq!(args.report.group_by, Some(GroupBy::Task));
            }
            _ => panic!("expected delivery command"),
        }
    }

    #[test]
    fn parses_quality_group_by_branch() {
        let cli = Cli::parse_from(["paceflow", "quality", "--group-by", "branch"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Quality(args)) => {
                assert_eq!(args.report.group_by, Some(GroupBy::Branch))
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn parses_quality_model_filter() {
        let cli = Cli::parse_from(["paceflow", "quality", "--model", "gpt-5"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Quality(args)) => {
                assert_eq!(args.report.model.as_deref(), Some("gpt-5"))
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn parses_cost_group_by_task() {
        let cli = Cli::parse_from(["paceflow", "cost", "--group-by", "task"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Cost(args)) => {
                assert_eq!(args.report.group_by, Some(GroupBy::Task))
            }
            _ => panic!("expected cost command"),
        }
    }

    #[test]
    fn parses_report_branch_filter() {
        let cli = Cli::parse_from(["paceflow", "session", "--branch", "fix/test"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Session(args)) => {
                assert_eq!(args.report.branch.as_deref(), Some("fix/test"))
            }
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_report_all_projects_flag() {
        let cli = Cli::parse_from(["paceflow", "session", "--all-projects"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Session(args)) => {
                assert!(args.report.all_projects)
            }
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_session_overall_flag() {
        let cli = Cli::parse_from(["paceflow", "session", "--overall"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::Session(args)) => assert!(args.overall),
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
            Commands::Analytics(vca::cli::Commands::EventStream(args)) => {
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
            Commands::Analytics(vca::cli::Commands::EventStream(args)) => {
                assert_eq!(args.category, EventCategory::Session)
            }
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
            Commands::Analytics(vca::cli::Commands::EventStream(args)) => {
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
            Commands::Analytics(vca::cli::Commands::EventStream(args)) => assert!(args.pretty),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_github_token_command() {
        let cli = Cli::parse_from(["paceflow", "github", "token"]);
        match cli.command {
            Commands::Analytics(vca::cli::Commands::GitHub(args)) => match args.command {
                GitHubCommands::Token => {}
            },
            _ => panic!("expected github command"),
        }
    }

    #[test]
    fn parses_sync_push_all_projects() {
        let cli = Cli::parse_from(["paceflow", "sync", "push", "--all-projects"]);
        match cli.command {
            Commands::Sync(args) => match args.command {
                SyncCommands::Push(push) => assert!(push.all_projects),
                _ => panic!("expected sync push command"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_sync_status_repo_filter() {
        let cli = Cli::parse_from(["paceflow", "sync", "status", "--repo", "/tmp/repo"]);
        match cli.command {
            Commands::Sync(args) => match args.command {
                SyncCommands::Status(status) => {
                    assert_eq!(status.repo.as_deref(), Some("/tmp/repo"))
                }
                _ => panic!("expected sync status command"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_sync_schedule_commands() {
        for (command, expected) in [
            ("install", "install"),
            ("status", "status"),
            ("uninstall", "uninstall"),
            ("run", "run"),
        ] {
            let cli = Cli::parse_from(["paceflow", "sync", "schedule", command]);
            match cli.command {
                Commands::Sync(args) => match args.command {
                    SyncCommands::Schedule(schedule) => match (schedule.command, expected) {
                        (SyncScheduleCommands::Install, "install") => {}
                        (SyncScheduleCommands::Status, "status") => {}
                        (SyncScheduleCommands::Uninstall, "uninstall") => {}
                        (SyncScheduleCommands::Run, "run") => {}
                        _ => panic!("unexpected sync schedule command"),
                    },
                    _ => panic!("expected sync schedule command"),
                },
                _ => panic!("expected sync command"),
            }
        }
    }

    #[test]
    fn parses_hooks_install_repo_filter() {
        let cli = Cli::parse_from(["paceflow", "hooks", "install", "--repo", "/tmp/repo"]);
        match cli.command {
            Commands::Hooks(args) => match args.command {
                HooksCommands::Install(hooks) => {
                    assert_eq!(hooks.repo.as_deref(), Some("/tmp/repo"));
                    assert!(!hooks.force);
                }
                _ => panic!("expected hooks install command"),
            },
            _ => panic!("expected hooks command"),
        }
    }

    #[test]
    fn parses_hooks_install_force() {
        let cli = Cli::parse_from(["paceflow", "hooks", "install", "--force"]);
        match cli.command {
            Commands::Hooks(args) => match args.command {
                HooksCommands::Install(hooks) => {
                    assert!(hooks.force);
                    assert_eq!(hooks.repo, None);
                }
                _ => panic!("expected hooks install command"),
            },
            _ => panic!("expected hooks command"),
        }
    }

    #[test]
    fn parses_hooks_pre_commit_repo_filter() {
        let cli = Cli::parse_from(["paceflow", "hooks", "pre-commit", "--repo", "/tmp/repo"]);
        match cli.command {
            Commands::Hooks(args) => match args.command {
                HooksCommands::PreCommit(hooks) => {
                    assert_eq!(hooks.repo.as_deref(), Some("/tmp/repo"))
                }
                _ => panic!("expected hooks pre-commit command"),
            },
            _ => panic!("expected hooks command"),
        }
    }

    #[test]
    fn parses_hooks_status_repo_filter() {
        let cli = Cli::parse_from(["paceflow", "hooks", "status", "--repo", "/tmp/repo"]);
        match cli.command {
            Commands::Hooks(args) => match args.command {
                HooksCommands::Status(hooks) => {
                    assert_eq!(hooks.repo.as_deref(), Some("/tmp/repo"))
                }
                _ => panic!("expected hooks status command"),
            },
            _ => panic!("expected hooks command"),
        }
    }

    #[test]
    fn parses_hooks_uninstall_repo_filter() {
        let cli = Cli::parse_from(["paceflow", "hooks", "uninstall", "--repo", "/tmp/repo"]);
        match cli.command {
            Commands::Hooks(args) => match args.command {
                HooksCommands::Uninstall(hooks) => {
                    assert_eq!(hooks.repo.as_deref(), Some("/tmp/repo"))
                }
                _ => panic!("expected hooks uninstall command"),
            },
            _ => panic!("expected hooks command"),
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
    fn version_embeds_pkg_version_and_git_metadata() {
        let cmd = Cli::command();
        let version = cmd.get_version().expect("version should be set on Cli");

        assert!(
            version.starts_with(env!("CARGO_PKG_VERSION")),
            "version should start with the Cargo package version: got {version:?}"
        );
        assert!(
            version.contains(env!("VCA_GIT_HASH")),
            "version should include the git hash from build.rs: got {version:?}"
        );
        let dirty_flag = env!("VCA_GIT_DIRTY");
        assert!(
            ["clean", "dirty", "unknown"].contains(&dirty_flag),
            "VCA_GIT_DIRTY should be clean/dirty/unknown: got {dirty_flag:?}"
        );
        assert!(
            version.contains(dirty_flag),
            "version should include the dirty flag: got {version:?}"
        );
    }

    #[test]
    fn top_level_help_advertises_team_setup_commands() {
        let mut command = Cli::command();
        let mut buffer = Vec::new();
        command
            .write_long_help(&mut buffer)
            .expect("write top-level help");
        let help = String::from_utf8(buffer).expect("utf8");

        assert!(help.contains("Team setup:"));
        assert!(help.contains("paceflow github token"));
        assert!(help.contains("paceflow sync config"));
        assert!(help.contains("paceflow sync schedule install"));
        assert!(help.contains("paceflow hooks install"));
    }

    #[test]
    fn hooks_help_documents_pre_commit_dry_run_and_sync_prereq() {
        let mut command = Cli::command();
        let mut buffer = Vec::new();
        command
            .find_subcommand_mut("hooks")
            .expect("hooks subcommand")
            .write_long_help(&mut buffer)
            .expect("write hooks help");
        let help = String::from_utf8(buffer).expect("utf8");

        assert!(help.contains("paceflow hooks install"));
        assert!(help.contains("paceflow hooks pre-commit --repo ."));
        assert!(help.contains("paceflow sync config"));
    }

    #[test]
    fn delivery_quality_and_cost_help_explain_metrics_and_human_provider_context() {
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
        assert!(delivery_help.contains("Mainline lead"));
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

        let mut command = Cli::command();
        let mut cost_buffer = Vec::new();
        command
            .find_subcommand_mut("cost")
            .expect("cost subcommand")
            .write_long_help(&mut cost_buffer)
            .expect("write cost help");
        let cost_help = String::from_utf8(cost_buffer).expect("utf8");
        assert!(cost_help.contains("API-equivalent model cost"));
        assert!(cost_help.contains("Cost/accepted LOC"));
        assert!(cost_help.contains("Coverage"));
    }
}
