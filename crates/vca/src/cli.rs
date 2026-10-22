use clap::{Args, Parser, Subcommand, ValueEnum};

const SESSION_AFTER_HELP: &str = "Examples:\n  vca session                 # default: grouped by model\n  vca session --model codex/gpt-5.4\n  vca session --overall\n  vca session --group-by provider\n  vca session --group-by branch\n  vca session --branch fix/cursor-new-partial-fate-schema\n  vca session --list-sessions\n\nMetrics:\n  Average user prompts: average number of user prompts per session.\n  Avg time to first accepted change: minutes from session start to the first accepted code change.\n  Debug loop rate: share of sessions that look like repeated fix-retry cycles.\n  Error paste rate: share of sessions where an error message was pasted mid-session.\n  Session-to-commit rate: share of sessions followed by a commit within 4 hours.\n  No-output session rate: share of sessions with no accepted code changes.";
const DELIVERY_AFTER_HELP: &str = "Examples:\n  vca delivery                # default: grouped by model\n  vca delivery --model codex/gpt-5.4\n  vca delivery --overall\n  vca delivery --group-by provider\n  vca delivery --group-by task --task ABC-123\n  vca delivery --group-by branch\n  vca delivery --branch fix/cursor-new-partial-fate-schema\n\nMetrics:\n  Heavy commits: commits where matched AI-attributed lines are at least half of changed lines.\n  PR sync: completed GitHub PR lookups per heavy commit on github.com (see table column).\n  PR reach rate: among completed lookups, share where a pull request existed.\n  Mainline reach rate: share of heavy AI commits that later reached mainline.\n  Mainline lead: average hours from commit time to mainline reach (prefer a later mainline reach timestamp; otherwise use later PR merged time).\n  PR merge rate: among completed PR-linked lookups, share whose PR merged.";
const QUALITY_AFTER_HELP: &str = "Examples:\n  vca quality                 # default: grouped by model\n  vca quality --model codex/gpt-5.4\n  vca quality --overall\n  vca quality --group-by provider\n  vca quality --group-by task --task ABC-123\n  vca quality --group-by branch\n  vca quality --branch fix/cursor-new-partial-fate-schema\n\nMetrics:\n  Code churn rate: share of AI-added lines on heavy AI commits that were removed again within the churn window.\n  Bug-after-merge rate: share of merged heavy AI commits that drew a later fix-like commit within 60 days.\n  Revert rate: share of heavy AI commits that were later reverted.";
const COST_AFTER_HELP: &str = "Examples:\n  vca cost                    # default: grouped by model\n  vca cost --overall\n  vca cost --group-by provider\n  vca cost --group-by task --task ABC-123\n  vca cost --provider=opencode --all-projects   # cross-repo provider totals\n\nScoped reports default to the current git repo (unless --all-projects). Filters such as --provider still apply after that scope.\n\nMetrics:\n  Cost: API-equivalent model cost when token usage can be priced.\n  Cost/accepted LOC: priced session cost divided by accepted changed lines.\n  Coverage: sessions with priced cost over sessions with token usage.";
const GITHUB_AFTER_HELP: &str = "Examples:\n  vca github token\n\nGitHub token setup:\n  Use this command to save, replace, or delete the local GitHub token used for PR sync during ingest.";

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
const TOP_LEVEL_AFTER_HELP: &str = "Quick start:\n  vca ingest\n  vca tui\n  vca sessions\n  vca tasks\n  vca usage\n  vca quota\n  vca context\n  vca session\n  vca delivery\n  vca quality\n  vca cost\n\nOptional GitHub setup:\n  vca github token\n\nManual validation:\n  vca event-stream --stream session-base\n\nDiscover options:\n  vca <command> --help";

#[derive(Parser)]
#[command(
    name = "vca",
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
    /// Build the analytics data model from local sessions, code changes, and git history
    Ingest(IngestArgs),
    /// Show session efficiency and delivery metrics
    Session(SessionReportArgs),
    /// Show commit attribution and merge outcome metrics
    Delivery(DeliveryReportArgs),
    /// Show churn, bug-fix, and revert follow-through for heavy AI commits
    Quality(QualityReportArgs),
    /// Show token usage and estimated cost per useful output
    Cost(CostReportArgs),
    /// Print analytics-ready base-view rows as NDJSON for manual validation
    EventStream(EventStreamArgs),
    #[command(name = "github")]
    /// Manage GitHub token setup for live PR sync
    GitHub(GitHubArgs),
    #[command(name = "tui")]
    /// Launch the interactive terminal dashboard
    Tui(TuiArgs),
    /// Token usage and API-equivalent cost by tool, model, project, session or time
    #[command(after_help = USAGE_AFTER_HELP)]
    Usage(UsageArgs),
    /// Subscription quota windows: observed percentages and token burn
    #[command(after_help = QUOTA_AFTER_HELP)]
    Quota(QuotaArgs),
    /// Rank or group root sessions (subagents included) by cost, tokens or activity
    #[command(after_help = SESSIONS_AFTER_HELP)]
    Sessions(SessionsArgs),
    /// Tasks inside sessions: time and cost to completion, by model and harness
    #[command(after_help = TASKS_AFTER_HELP)]
    Tasks(TasksArgs),
    /// Show one session: tasks, agents, usage by model, context and commits
    Show(ShowArgs),
    /// Review one session in a local HTML page: timeline plus key numbers to approve or reject
    #[command(after_help = REVIEW_AFTER_HELP)]
    Review(ReviewArgs),
    /// Reference corpus: sample real sessions, approve their numbers, snapshot and replay them
    #[command(after_help = CORPUS_AFTER_HELP)]
    Corpus(CorpusArgs),
    /// What filled the context and what carrying it cost
    #[command(after_help = CONTEXT_AFTER_HELP)]
    Context(ContextArgs),
    /// Later AI work that changed earlier AI lines, defect evidence and correction spend
    #[command(after_help = REWORK_AFTER_HELP)]
    Rework(ReworkArgs),
    /// The team's 12 habits: how often each is followed and what that correlates with
    #[command(after_help = HABITS_AFTER_HELP)]
    Habits(HabitsArgs),
    /// Work stages: cost by stage (explore, plan, implement, verify, review…), stage paths, session kinds
    Stages(StagesArgs),
    /// Model prices (LiteLLM's public price map): status, update, show a model
    Pricing(PricingArgs),
    /// Optional AI task splitting and labels via Jev on OpenRouter (opt-in, previewable)
    #[command(after_help = CLASSIFY_AFTER_HELP)]
    Classify(ClassifyArgs),
}

const REWORK_AFTER_HELP: &str = "Examples:\n  vca rework                          # spend split, suspected defects, relations\n  vca rework --kind defect_correction\n  vca rework --days 90 --here\n\nEvidence levels: L0 AI lines later changed · L1 later fix signal · L2 fix linked to a bug issue ·\nL3 classifier says defect correction · L4 confirmed by you. Correlation, not causation.";

const HABITS_AFTER_HELP: &str = "Examples:\n  vca habits                       # adoption and outcomes for all 12 habits\n  vca habits --habit 7 --list      # tasks behind one habit, with evidence\n  vca habits --days 90 --here\n\nLabels come from local heuristics until you classify with Jev (`vca classify run`), which adds\nturn stages, request kinds and flags such as unrequested changes. Correlation, not causation.";

const REVIEW_AFTER_HELP: &str = "Examples:\n  vca review 1a2b3c                         # writes review-<session>.html in the current directory\n  vca review claude:1a2b3c --out /tmp/r.html\n  vca review --check expectations.json         # exit 1 if an approved number changed\n\nThe page is a local file with the whole transcript; it is never uploaded. Mark each number ✓ or ✗,\nthen Export to get expectations JSON that `--check` (and the test suite) asserts.";

const CORPUS_AFTER_HELP: &str = "Examples:\n  vca corpus review --project nothing-phone --project WarGames   # sample, review, save\n  vca corpus review --per 3 --seed 7\n  vca corpus list\n  vca corpus wrong                     # every number marked ✗, with your notes\n  vca corpus check                     # replay every snapshot and compare\n  vca corpus check claude-7956ac77      # one snapshot\n\nSnapshots go to ~/.vca/corpus/<provider>-<id>/: the session's source files (for Cursor and\nOpenCode only that session's rows), a git bundle of its repository, the price map and the\napproved numbers. Nothing is uploaded.";

#[derive(Args, Debug, Clone)]
pub struct CorpusArgs {
    #[command(subcommand)]
    pub command: CorpusCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum CorpusCommands {
    /// Open the local review app: sampled sessions, review pages, "Save as reference"
    Review(CorpusReviewArgs),
    /// Snapshot one session with an expectations file (what the app's "Save as reference" does)
    Save(CorpusSaveArgs),
    /// List saved snapshots
    List,
    /// Every number marked ✗ in the corpus, with the reviewer's note
    Wrong,
    /// Rebuild each snapshot's numbers from its files alone and compare with the approved ones
    Check(CorpusCheckArgs),
}

#[derive(Args, Debug, Clone)]
pub struct CorpusReviewArgs {
    /// Only sessions whose repository path contains this (case-insensitive); repeatable
    #[arg(long = "project")]
    pub projects: Vec<String>,
    /// Tool filter (claude, codex, cursor, opencode); repeatable
    #[arg(long = "provider")]
    pub providers: Vec<String>,
    /// Sessions sampled per project and tool
    #[arg(long, default_value_t = 2)]
    pub per: usize,
    /// Sampling seed; the same seed gives the same picks
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Skip sessions with fewer turns
    #[arg(long, default_value_t = 2)]
    pub min_turns: i64,
    /// Port on 127.0.0.1 (0 picks a free one)
    #[arg(long, default_value_t = 0)]
    pub port: u16,
    /// Do not open a browser
    #[arg(long)]
    pub no_open: bool,
}

#[derive(Args, Debug, Clone)]
pub struct CorpusSaveArgs {
    /// Session id, id prefix, or provider:id
    pub session: String,
    /// Expectations JSON (from the review page's Export)
    #[arg(long)]
    pub expectations: std::path::PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct CorpusCheckArgs {
    /// Snapshot folder names or paths (default: all)
    pub snapshots: Vec<String>,
    /// Keep each replay's temporary folder for inspection
    #[arg(long)]
    pub keep: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ReviewArgs {
    /// Session id, id prefix, or provider:id (not needed with --check)
    #[arg(required_unless_present = "check")]
    pub session: Option<String>,
    /// Where to write the HTML page (default review-<provider>-<id>.html)
    #[arg(long)]
    pub out: Option<std::path::PathBuf>,
    /// Compare approved numbers in an expectations file instead of writing the page
    #[arg(long, conflicts_with_all = ["out", "json"])]
    pub check: Option<std::path::PathBuf>,
    /// Print the review data as JSON instead of writing the page
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Clone)]
pub struct HabitsArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// One habit (number or id), e.g. 7 or 07_tests_can_fail
    #[arg(long)]
    pub habit: Option<String>,
    /// List the tasks behind the habit (needs --habit)
    #[arg(long, requires = "habit")]
    pub list: bool,
}

#[derive(Args, Debug, Clone)]
pub struct StagesArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
}

#[derive(Args, Debug, Clone)]
pub struct PricingArgs {
    #[command(subcommand)]
    pub command: Option<PricingCommands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PricingCommands {
    /// Where prices come from and when they were fetched (default)
    Status,
    /// Download the latest LiteLLM price map now
    Update,
    /// Show how a model name resolves and its rates per million tokens
    Show {
        model: String,
        /// Prompt size (input + cache tokens) of one request, to show tier rates
        #[arg(long, default_value_t = 0)]
        prompt_tokens: i64,
    },
}

#[derive(Args, Debug, Clone)]
pub struct ReworkArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// Only relations with this rework kind (defect_correction, requirement_change, refinement, …)
    #[arg(long)]
    pub kind: Option<String>,
}

const CLASSIFY_AFTER_HELP: &str = "Examples:\n  vca classify plan --days 7                 # what would be sent, estimated cost\n  vca classify preview <session> --pass segment\n  vca classify key set                       # or export OPENROUTER_API_KEY\n  vca classify run --days 7 --max-usd 0.05 --yes\n  vca classify set <session> --task 0 intent bug_fix\n  vca classify set-turn <session> 5 1        # move turn 5 to task 1\n  vca classify eval\n\nNothing is sent unless you run `classify run` and confirm. Without it, each session is one task\nand labels come from local heuristics (marked h).";

#[derive(Args, Debug, Clone)]
pub struct ClassifyArgs {
    #[command(subcommand)]
    pub command: ClassifyCommands,
}

#[derive(Args, Debug, Clone)]
pub struct ClassifySelectArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// Specific sessions (id, prefix or provider:id); repeatable
    #[arg(long)]
    pub session: Vec<String>,
    /// Content level: evidence_only | user_text | conversation | full
    #[arg(long)]
    pub level: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ClassifyCommands {
    /// Count units, tokens and cost a run would send (sends nothing)
    Plan(ClassifySelectArgs),
    /// Print the exact request one unit would send
    Preview {
        /// Session id, prefix or provider:id
        session: String,
        /// segment | feedback | labels | completion
        #[arg(long, default_value = "labels")]
        pass: String,
        /// Turn (segment, feedback) or task (labels, completion) index
        #[arg(long)]
        unit: Option<i64>,
        /// Content level override
        #[arg(long)]
        level: Option<String>,
    },
    /// Send the selection to the classifier
    Run {
        #[command(flatten)]
        select: ClassifySelectArgs,
        /// Stop before spending more than this (USD)
        #[arg(long, default_value_t = 0.50)]
        max_usd: f64,
        /// Comma list: segment, feedback, labels, completion, rework, all
        #[arg(long, default_value = "all")]
        passes: String,
        /// Do not ask for confirmation
        #[arg(long)]
        yes: bool,
    },
    /// Settings, key source, spend and label coverage
    Status,
    /// Manage the OpenRouter API key
    Key {
        #[command(subcommand)]
        command: ClassifyKeyCommands,
    },
    /// Change classifier settings
    Config {
        /// Default content level
        #[arg(long)]
        level: Option<String>,
        /// Monthly classifier budget (USD)
        #[arg(long)]
        monthly_budget: Option<f64>,
        /// Only send these projects (glob on repo root); repeatable
        #[arg(long)]
        allow_project: Vec<String>,
        /// Never send these projects (glob on repo root); repeatable
        #[arg(long)]
        deny_project: Vec<String>,
        /// Clear allow and deny lists before adding
        #[arg(long)]
        reset_projects: bool,
    },
    /// Override a label (wins over classifier and heuristic labels)
    Set {
        session: String,
        /// Task index (task questions)
        #[arg(long, conflicts_with = "turn")]
        task: Option<i64>,
        /// Turn index (exchange questions such as feedback)
        #[arg(long)]
        turn: Option<i64>,
        /// Question id, e.g. intent, outcome, feedback, p_repeated_failure
        question: String,
        label: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Move a user turn (and the turns after it, until the next assignment) to a task
    SetTurn {
        session: String,
        turn: i64,
        task: i64,
    },
    /// Set the turn where a task was completed (`none` = not completed)
    SetCompletion {
        session: String,
        task: i64,
        turn: String,
    },
    /// Remove all manual overrides for a session
    ClearOverrides { session: String },
    /// Accuracy of classifier and heuristic labels against your overrides and gold labels
    Eval {
        #[arg(long)]
        question: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum ClassifyKeyCommands {
    /// Save a key (reads stdin when omitted)
    Set { key: Option<String> },
    /// Delete the saved key
    Delete,
}

const USAGE_AFTER_HELP: &str = "Examples:\n  vca usage                       # by tool, last 30 days\n  vca usage --group-by model\n  vca usage --group-by session --limit 20\n  vca usage --group-by hour --days 2\n\nEvery usage event is counted once, so groups add up to the total. Costs are API list-price estimates.";
const QUOTA_AFTER_HELP: &str = "Examples:\n  vca quota\n  vca quota --provider codex --days 7\n  vca quota mark claude --kind limit_hit          # record that you hit a limit now\n  vca quota mark claude --at 2026-10-03T15:47:00Z --kind limit_hit\n\nCodex logs quota percentages; other tools only show token burn per window.";
const SESSIONS_AFTER_HELP: &str = "Examples:\n  vca sessions                    # most expensive first\n  vca sessions --sort tokens\n  vca sessions --group-by cost    # cost bands; also tokens, model, tool, project, intent, outcome, week\n  vca sessions --provider claude --search parser";
const TASKS_AFTER_HELP: &str = "Examples:\n  vca tasks                                   # most expensive tasks\n  vca tasks --sort time --completed-only\n  vca tasks --group-by model --group-by tool --intent feature\n\nWithout the optional classifier every session is one task.";
const CONTEXT_AFTER_HELP: &str = "Examples:\n  vca context                     # by kind (instructions, tool results, baseline, ...)\n  vca context --group-by dir      # which directories cost the most to keep in context\n  vca context --group-by instruction\n  vca context --session 93cba0e8 --group-by target";

#[derive(Args, Debug, Clone)]
pub struct InsightFilterArgs {
    /// Only include the last N days (0 = all time)
    #[arg(long, default_value_t = 30)]
    pub days: u32,
    /// Start date (YYYY-MM-DD), overrides --days
    #[arg(long)]
    pub from: Option<String>,
    /// End date (YYYY-MM-DD)
    #[arg(long)]
    pub to: Option<String>,
    /// Tool filter (claude, codex, cursor, opencode); repeatable
    #[arg(long)]
    pub provider: Vec<String>,
    /// Model substring filter
    #[arg(long)]
    pub model: Option<String>,
    /// Repository root filter
    #[arg(long)]
    pub repo: Option<String>,
    /// Limit to the git repository of the current directory
    #[arg(long)]
    pub here: bool,
    /// Text search in titles, first requests, branches and ids
    #[arg(long)]
    pub search: Option<String>,
    /// Intent label filter (e.g. feature, bug_fix, research_explain)
    #[arg(long)]
    pub intent: Option<String>,
    /// Outcome label filter (e.g. completed_verified, stalled)
    #[arg(long)]
    pub outcome: Option<String>,
    /// Minimum API-equivalent cost
    #[arg(long)]
    pub min_cost: Option<f64>,
    /// Max rows
    #[arg(long, default_value_t = 30)]
    pub limit: usize,
    /// Print JSON instead of a table
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Clone)]
pub struct UsageArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// tool | model | project | session | agent | day | hour
    #[arg(long, default_value = "tool")]
    pub group_by: String,
}

#[derive(Args, Debug, Clone)]
pub struct QuotaArgs {
    #[command(subcommand)]
    pub command: Option<QuotaCommands>,
    #[command(flatten)]
    pub filters: InsightFilterArgs,
}

#[derive(Subcommand, Debug, Clone)]
pub enum QuotaCommands {
    /// Record a quota observation (e.g. you hit a limit)
    Mark {
        /// Tool the observation applies to (claude, cursor, codex, opencode)
        provider: String,
        /// When it happened (RFC 3339); defaults to now
        #[arg(long)]
        at: Option<String>,
        /// limit_hit | reset | plan_change | note
        #[arg(long, default_value = "limit_hit")]
        kind: String,
        /// Free-text note
        #[arg(long)]
        note: Option<String>,
    },
}

#[derive(Args, Debug, Clone)]
pub struct SessionsArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// cost | tokens | fresh | cache | started | turns | failures | agents
    #[arg(long, default_value = "cost")]
    pub sort: String,
    /// Group instead of listing: cost | tokens | model | tool | project | intent | outcome | branch | week | day
    #[arg(long)]
    pub group_by: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct TasksArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// cost | cost-to-complete | tokens | time | wall | started | attempts | failures
    #[arg(long, default_value = "cost")]
    pub sort: String,
    /// Group by one or more dimensions (model, tool, intent, project, cost, tokens, week)
    #[arg(long)]
    pub group_by: Vec<String>,
    /// Only tasks with an observed completion
    #[arg(long)]
    pub completed_only: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ShowArgs {
    /// Session id, id prefix, or provider:id
    pub session: String,
    #[arg(long)]
    pub tasks: bool,
    #[arg(long)]
    pub agents: bool,
    #[arg(long)]
    pub usage: bool,
    #[arg(long)]
    pub context: bool,
    #[arg(long)]
    pub delivery: bool,
    /// Turn-by-turn timeline
    #[arg(long)]
    pub turns: bool,
    /// Rows per section
    #[arg(long, default_value_t = 15)]
    pub limit: usize,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ContextArgs {
    #[command(flatten)]
    pub filters: InsightFilterArgs,
    /// kind | tool | dir | mcp | instruction | purpose | target
    #[arg(long, default_value = "kind")]
    pub group_by: String,
    /// Restrict to one session (id, prefix or provider:id)
    #[arg(long)]
    pub session: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct IngestArgs {
    /// Delete the local analytics database and rebuild it from scratch.
    ///
    /// Normal ingest is incremental: already-ingested sessions and unchanged
    /// code-change sources are skipped, so changes to parsing/derivation logic
    /// do not propagate to existing rows. A fresh ingest fixes that by wiping
    /// the database first, then rebuilding from local sources and git history.
    /// GitHub data is re-fetched and sync-push state is reset. Local files
    /// (GitHub token) are not touched.
    #[arg(long)]
    pub fresh: bool,
    /// Do not download LiteLLM model prices; use the cached or bundled copy
    #[arg(long)]
    pub offline_pricing: bool,
}

#[derive(Args, Debug, Clone)]
pub struct TuiArgs {
    /// Show results across all tracked projects (default for the explorer; the classic dashboard scopes to the current repo)
    #[arg(long)]
    pub all_projects: bool,
    /// Limit the explorer to the git repository of the current directory
    #[arg(long)]
    pub here: bool,
    /// Open the previous 4-tab metrics dashboard instead of the explorer
    #[arg(long)]
    pub classic: bool,
    /// Max number of grouped rows to display
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
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
    Branch,
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
    /// Restrict to a specific branch name
    #[arg(long)]
    pub branch: Option<String>,
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
#[command(after_help = COST_AFTER_HELP)]
pub struct CostReportArgs {
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
        let cli = Cli::parse_from(["vca", "session", "--group-by", "repo"]);
        match cli.command {
            Commands::Session(args) => assert_eq!(args.report.group_by, Some(GroupBy::Repo)),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_delivery_weekly_group_by_task() {
        let cli = Cli::parse_from(["vca", "delivery", "--weekly", "--group-by", "task"]);
        match cli.command {
            Commands::Delivery(args) => {
                assert!(args.report.weekly);
                assert_eq!(args.report.group_by, Some(GroupBy::Task));
            }
            _ => panic!("expected delivery command"),
        }
    }

    #[test]
    fn parses_quality_group_by_branch() {
        let cli = Cli::parse_from(["vca", "quality", "--group-by", "branch"]);
        match cli.command {
            Commands::Quality(args) => assert_eq!(args.report.group_by, Some(GroupBy::Branch)),
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn parses_quality_model_filter() {
        let cli = Cli::parse_from(["vca", "quality", "--model", "gpt-5"]);
        match cli.command {
            Commands::Quality(args) => assert_eq!(args.report.model.as_deref(), Some("gpt-5")),
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn parses_cost_group_by_task() {
        let cli = Cli::parse_from(["vca", "cost", "--group-by", "task"]);
        match cli.command {
            Commands::Cost(args) => assert_eq!(args.report.group_by, Some(GroupBy::Task)),
            _ => panic!("expected cost command"),
        }
    }

    #[test]
    fn parses_report_branch_filter() {
        let cli = Cli::parse_from(["vca", "session", "--branch", "fix/test"]);
        match cli.command {
            Commands::Session(args) => assert_eq!(args.report.branch.as_deref(), Some("fix/test")),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_report_all_projects_flag() {
        let cli = Cli::parse_from(["vca", "session", "--all-projects"]);
        match cli.command {
            Commands::Session(args) => assert!(args.report.all_projects),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn parses_session_overall_flag() {
        let cli = Cli::parse_from(["vca", "session", "--overall"]);
        match cli.command {
            Commands::Session(args) => assert!(args.overall),
            _ => panic!("expected session command"),
        }
    }

    #[test]
    fn overall_conflicts_with_group_by() {
        let result = Cli::try_parse_from(["vca", "delivery", "--overall", "--group-by", "model"]);
        assert!(result.is_err());
        let err = result.err().expect("expected clap conflict");
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
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
    fn parses_event_stream_delivery_category_and_task_filter() {
        let cli = Cli::parse_from([
            "vca",
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
        let cli = Cli::parse_from(["vca", "event-stream", "--pretty"]);
        match cli.command {
            Commands::EventStream(args) => assert!(args.pretty),
            _ => panic!("expected event-stream command"),
        }
    }

    #[test]
    fn parses_github_token_command() {
        let cli = Cli::parse_from(["vca", "github", "token"]);
        match cli.command {
            Commands::GitHub(args) => match args.command {
                GitHubCommands::Token => {}
            },
            _ => panic!("expected github command"),
        }
    }

    #[test]
    fn rejects_legacy_change_and_lifecycle_commands() {
        assert!(Cli::try_parse_from(["vca", "change"]).is_err());
        assert!(Cli::try_parse_from(["vca", "lifecycle"]).is_err());
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
