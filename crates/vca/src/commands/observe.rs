//! Public work, changes and trend commands, shared by both products.
use crate::insights::{
    setup,
    trends::{self, Options, Report, Scope},
    work,
};
use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use rusqlite::{Connection, params};

#[derive(Debug, Clone, Default, Args)]
pub struct ScopeArgs {
    #[arg(long, global = true, conflicts_with = "all_projects")]
    pub repo: Option<String>,
    #[arg(long, global = true)]
    pub all_projects: bool,
    #[arg(long, global = true)]
    pub provider: Vec<String>,
    #[arg(long, global = true)]
    pub model: Option<String>,
    #[arg(long, global = true)]
    pub intent: Option<String>,
    #[arg(long, global = true)]
    pub json: bool,
}
impl ScopeArgs {
    pub fn scope(&self) -> Result<Scope> {
        let repo = if self.all_projects {
            None
        } else if let Some(repo) = &self.repo {
            Some(
                crate::path_utils::canonicalize(std::path::Path::new(repo))
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| repo.clone()),
            )
        } else {
            crate::path_utils::detect_repo_root(&std::env::current_dir()?)
                .map(|p| p.to_string_lossy().into_owned())
        };
        Ok(Scope {
            repo,
            providers: self.provider.clone(),
            model: self.model.clone(),
            intent: self.intent.clone(),
            ..Scope::default()
        })
    }
}
#[derive(Debug, Clone, Args)]
pub struct TrendsArgs {
    #[command(flatten)]
    pub scope: ScopeArgs,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub timezone: Option<String>,
    #[arg(long)]
    pub daily: bool,
    /// Compare against the previous equal period (default: four completed weeks each).
    #[arg(long)]
    pub compare: bool,
    #[arg(long, requires = "baseline_to")]
    pub baseline_from: Option<String>,
    #[arg(long, requires = "baseline_from")]
    pub baseline_to: Option<String>,
    /// Milestone ID; compare 28 days either side, excluding its day.
    #[arg(long,conflicts_with_all=["from","to","baseline_from","baseline_to"])]
    pub around: Option<i64>,
    #[arg(long)]
    pub verified_only: bool,
    #[arg(long)]
    pub include_estimated: bool,
    #[arg(long, default_value_t = 7)]
    pub follow_up_days: u32,
}
impl TrendsArgs {
    pub fn options(&self) -> Result<Options> {
        let mut scope = self.scope.scope()?;
        scope.verified_only = self.verified_only;
        scope.include_estimated = self.include_estimated;
        Ok(Options {
            scope,
            from: self.from.clone(),
            to: self.to.clone(),
            timezone: self.timezone.clone(),
            daily: self.daily,
            compare: self.compare,
            baseline_from: self.baseline_from.clone(),
            baseline_to: self.baseline_to.clone(),
            around: self.around,
            follow_up_days: self.follow_up_days,
        })
    }
}
#[derive(Debug, Clone, Args)]
pub struct WorkArgs {
    #[command(flatten)]
    pub scope: ScopeArgs,
    #[command(subcommand)]
    pub command: Option<WorkCommand>,
}
#[derive(Debug, Clone, Subcommand)]
pub enum WorkCommand {
    List,
    Show {
        id: String,
    },
    /// Join two member IDs (or provider:session#task references).
    Link {
        from: String,
        to: String,
        #[arg(long, default_value = "continuation")]
        kind: String,
        #[arg(long, default_value = "Manual link")]
        note: String,
    },
    /// Detach one member from all membership links; keeps rejection decisions.
    Unlink {
        id: String,
    },
    Suggestions,
    Accept {
        id: String,
    },
    Reject {
        id: String,
    },
}
#[derive(Debug, Clone, Args)]
pub struct ChangesArgs {
    #[command(flatten)]
    pub scope: ScopeArgs,
    #[command(subcommand)]
    pub command: Option<ChangesCommand>,
}
#[derive(Debug, Clone, Args)]
pub struct MilestoneArgs {
    pub title: String,
    /// RFC3339 timestamp, or local YYYY-MM-DD at the reporting timezone's midnight.
    #[arg(long)]
    pub at: String,
    #[arg(long, default_value = "workflow")]
    pub kind: String,
    #[arg(long, default_value = "")]
    pub note: String,
    #[arg(long)]
    pub observation: Option<String>,
}
#[derive(Debug, Clone, Subcommand)]
pub enum ChangesCommand {
    List,
    Add(MilestoneArgs),
    Edit {
        id: i64,
        #[command(flatten)]
        milestone: MilestoneArgs,
    },
    Remove {
        id: i64,
    },
    /// Observe current files and session evidence. Additional roots are persisted.
    Capture {
        #[arg(long)]
        skill_root: Vec<std::path::PathBuf>,
        #[arg(long)]
        timezone: Option<String>,
    },
}
#[derive(Debug, Clone, Args)]
pub struct AutoArgs {
    #[command(flatten)]
    pub scope: ScopeArgs,
    #[command(subcommand)]
    pub command: AutoCommand,
}
#[derive(Debug, Clone, Subcommand)]
pub enum AutoCommand {
    /// Enable the external classifier globally for every current and future ingested project.
    Enable {
        #[arg(long)]
        monthly_budget: Option<f64>,
        #[arg(long)]
        project_monthly_budget: Option<f64>,
        #[arg(long)]
        max_run_usd: Option<f64>,
        #[arg(long)]
        backfill_days: Option<u32>,
    },
    Disable,
    Status,
    /// Optional project spending and coverage overrides; activation remains global.
    Limits {
        #[arg(long)]
        monthly_budget: Option<f64>,
        #[arg(long)]
        max_run_usd: Option<f64>,
        #[arg(long)]
        backfill_days: Option<u32>,
    },
}
pub fn run_trends(args: TrendsArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let report = trends::report(&conn, &args.options()?)?;
    if args.scope.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", report_text(&report));
    }
    Ok(())
}
pub fn report_text(report: &Report) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Engineering trends · {} · {}",
        report.timezone,
        report.scope.repo.as_deref().unwrap_or("all projects")
    );
    let _ = writeln!(
        out,
        "Cutoff {} · {}\n",
        report.data_cutoff, report.metric_version
    );
    let _ = writeln!(
        out,
        "Period                  Started Done Open Abandoned  Cycle median/p90  Tokens median/p90  Activity min"
    );
    for p in &report.periods {
        let _ = writeln!(
            out,
            "{}..{}{} {:>4} {:>4} {:>4} {:>5}  {}/{}  {}/{}  {:.1}",
            p.from,
            p.to,
            if p.provisional { "*" } else { " " },
            p.started,
            p.completed,
            p.open,
            p.abandoned,
            num(p.cycle_minutes.median),
            num(p.cycle_minutes.p90),
            num(p.tokens_per_completed.median),
            num(p.tokens_per_completed.p90),
            p.work_activity_minutes
        );
        let _ = writeln!(
            out,
            "  Samples: {} cycle, {} tokens; missing tokens {}; excluded completion samples {}; {} unclassified, {} heuristic; {} pending links",
            p.cycle_minutes.samples,
            p.tokens_per_completed.samples,
            p.tokens_per_completed.missing,
            p.cycle_minutes.excluded,
            p.coverage.unclassified_intents,
            p.coverage.heuristic_intents,
            p.coverage.pending_links
        );
        let _ = writeln!(
            out,
            "  Native usage complete: {}/{} work items; empty conversations excluded: {}. Runtime outcomes: {} succeeded, {} failed (goal completion assessed separately).",
            p.coverage.native_complete,
            p.coverage.work_items,
            p.coverage.empty_conversations_excluded,
            p.runtime_succeeded,
            p.runtime_failed
        );
        let _ = writeln!(
            out,
            "  Goal mix (work / activity / native tokens): {}",
            p.goal_mix
                .iter()
                .map(|m| format!(
                    "{} {} / {} / {}",
                    m.label,
                    pct(m.work_share),
                    pct(m.activity_share),
                    pct(m.token_share)
                ))
                .collect::<Vec<_>>()
                .join(" · ")
        );
        let _ = writeln!(
            out,
            "  Activity mix: {}",
            p.activity_mix
                .iter()
                .map(|m| format!(
                    "{} {} ({:.1} min)",
                    m.label,
                    pct(m.activity_share),
                    m.activity_minutes
                ))
                .collect::<Vec<_>>()
                .join(" · ")
        );
        let _ = writeln!(
            out,
            "  Usage: {} native, {} estimated tokens; {} missing work items. Follow-up: {} mature, {} pending; reopen {}, correction {}",
            p.native_tokens.total,
            p.estimated_tokens.total,
            p.coverage.missing_usage,
            p.mature_followups,
            p.pending_followups,
            pct(p.reopen_rate),
            pct(p.correction_rate)
        );
    }
    if let Some(c) = &report.comparison {
        let _ = writeln!(
            out,
            "\nComparison {}..{} → {}..{}{}",
            c.before.from,
            c.before.to,
            c.after.from,
            c.after.to,
            if c.after.provisional {
                " (provisional)"
            } else {
                ""
            }
        );
        for d in &c.deltas {
            let _ = writeln!(
                out,
                "  {}: {} → {} · Δ {} {} · {}{}",
                d.metric,
                num(d.before),
                num(d.after),
                num(d.absolute),
                d.unit,
                d.relative_percent
                    .map(|v| format!("{v:+.1}%"))
                    .unwrap_or_default(),
                d.note.as_deref().unwrap_or("")
            );
        }
        let _ = writeln!(
            out,
            "  Spanning change: {} work items; their lifecycle is excluded from setup attribution.",
            c.before
                .spanning_change_ids
                .iter()
                .chain(&c.after.spanning_change_ids)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
        for b in &c.breakdowns {
            let _ = writeln!(
                out,
                "  {} = {}: items {} → {}, cycle {} → {}, tokens {} → {}",
                b.dimension,
                b.key,
                b.before_items,
                b.after_items,
                num(b.before_cycle.median),
                num(b.after_cycle.median),
                num(b.before_tokens.median),
                num(b.after_tokens.median)
            );
        }
    }
    for m in &report.milestones {
        let _ = writeln!(out, "Change #{} · {} · {}", m.id, m.at, m.title);
    }
    let changed = report
        .observations
        .iter()
        .filter(|o| o.source == "filesystem")
        .count();
    let _ = writeln!(
        out,
        "Setup observations: {} ({} current-file observations; see changes --json)",
        report.observations.len(),
        changed
    );
    for warning in &report.warnings {
        let _ = writeln!(out, "Note: {warning}");
    }
    let _ = writeln!(
        out,
        "* partial period; samples below 10 are small. Activity is estimated work activity, not measured human hours. JSON includes evidence and contributing IDs."
    );
    out
}
fn num(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.1}"))
        .unwrap_or_else(|| "—".into())
}
fn pct(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.1}%", v * 100.0))
        .unwrap_or_else(|| "—".into())
}
fn envelope<T: serde::Serialize>(
    conn: &Connection,
    scope: &Scope,
    data: T,
) -> Result<serde_json::Value> {
    let (tz, warning) = trends::timezone(conn, None)?;
    let review_decisions: i64 = conn.query_row(
        "SELECT COUNT(*) FROM work_saved_decision WHERE review_reason IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    Ok(
        serde_json::json!({"schema_version":1,"metric_version":trends::METRIC_VERSION,"data_cutoff":chrono::Utc::now().to_rfc3339(),"timezone":tz.name(),"scope":scope,"period_boundaries":null,"provenance":{"classification_version":crate::classify::questions::QUESTION_SET_VERSION},"coverage":{"saved_decisions_needing_review":review_decisions},"warnings":warning.into_iter().collect::<Vec<_>>(),"data":data}),
    )
}
pub fn run_work(args: WorkArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let scope = args.scope.scope()?;
    match args.command.unwrap_or(WorkCommand::List) {
        WorkCommand::List => {
            let rows: Vec<_> = work::items(&conn)?
                .into_iter()
                .filter(|w| scope.includes(w))
                .collect();
            if args.scope.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&envelope(&conn, &scope, &rows)?)?
                );
            } else {
                for w in rows {
                    println!(
                        "{} {} · {} · {} chats · {} min · {}",
                        w.id,
                        w.state,
                        w.intent,
                        w.conversations,
                        num(w.cycle_minutes()),
                        w.title
                    );
                }
            }
        }
        WorkCommand::Show { id } => {
            let rows: Vec<_> = work::items(&conn)?
                .into_iter()
                .filter(|w| {
                    (w.id.starts_with(&id) || w.members.iter().any(|m| m.id.starts_with(&id)))
                        && scope.includes(w)
                })
                .collect();
            if rows.len() != 1 {
                bail!("Reference must identify one work item in scope");
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&envelope(&conn, &scope, &rows[0])?)?
            );
        }
        WorkCommand::Link {
            from,
            to,
            kind,
            note,
        } => {
            let a = work::resolve_member(&conn, &from)?;
            let b = work::resolve_member(&conn, &to)?;
            for id in [&a, &b] {
                let current: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM derived_work_member WHERE identity_id=?1)",
                    [id],
                    |r| r.get(0),
                )?;
                if !current {
                    bail!("Choose current task identities; this member is stale");
                }
            }
            conn.execute(
                "UPDATE work_identity SET review_reason=NULL WHERE id IN (?1,?2)",
                params![a, b],
            )?;
            let id = work::insert_relation(
                &conn,
                &a,
                &b,
                &kind,
                "accepted",
                "manual",
                &note,
                Some(1.0),
            )?;
            println!("Linked {id}");
        }
        WorkCommand::Unlink { id } => {
            let id = work::resolve_member(&conn, &id)?;
            let count=conn.execute("UPDATE work_relation SET decision='rejected',source='manual',evidence='Manually detached' WHERE (from_id=?1 OR to_id=?1) AND kind IN ('continuation','reopens')",[id])?;
            println!("Detached {count} links");
        }
        WorkCommand::Suggestions => {
            let items = work::items(&conn)?;
            let ids: std::collections::BTreeSet<_> = items
                .iter()
                .filter(|w| scope.includes(w))
                .flat_map(|w| w.members.iter().map(|m| m.id.clone()))
                .collect();
            let rows: Vec<_> = work::relations(&conn)?
                .into_iter()
                .filter(|r| {
                    (r.decision == "suggested" || r.review_reason.is_some())
                        && (ids.contains(&r.from_id) || ids.contains(&r.to_id))
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&envelope(
                    &conn,
                    &scope,
                    serde_json::json!({"relationships":rows,"saved_decisions":work::pending_decisions(&conn,scope.repo.as_deref())?})
                )?)?
            );
        }
        WorkCommand::Accept { id } => {
            work::decide(&conn, &id, true)?;
            println!("Accepted");
        }
        WorkCommand::Reject { id } => {
            work::decide(&conn, &id, false)?;
            println!("Rejected");
        }
    }
    Ok(())
}
pub fn milestone_time(conn: &Connection, at: &str) -> Result<String> {
    if at.len() == 10 {
        let (tz, _) = trends::timezone(conn, None)?;
        let date = chrono::NaiveDate::parse_from_str(at, "%Y-%m-%d")?;
        Ok(crate::insights::structure::ms_to_iso(trends::midnight(
            tz, date,
        )?))
    } else {
        Ok(chrono::DateTime::parse_from_rfc3339(at)?
            .with_timezone(&chrono::Utc)
            .to_rfc3339())
    }
}
pub fn run_changes(args: ChangesArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let scope = args.scope.scope()?;
    match args.command.unwrap_or(ChangesCommand::List) {
        ChangesCommand::List => {
            let data = serde_json::json!({"milestones":setup::milestones(&conn,scope.repo.as_deref())?,"observations":setup::observations(&conn,scope.repo.as_deref())?});
            if args.scope.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&envelope(&conn, &scope, data)?)?
                );
            } else {
                for m in setup::milestones(&conn, scope.repo.as_deref())? {
                    println!("#{} {} · {} · {}", m.id, m.at, m.kind, m.title);
                }
                for o in setup::observations(&conn, scope.repo.as_deref())? {
                    println!(
                        "{} {} · {} · {} ({})",
                        o.id,
                        o.effective_at.as_deref().unwrap_or(&o.observed_at),
                        o.state,
                        o.identity,
                        o.source
                    );
                }
            }
        }
        ChangesCommand::Add(m) => {
            let id = setup::save_milestone(
                &conn,
                None,
                &m.title,
                &milestone_time(&conn, &m.at)?,
                scope.repo.as_deref(),
                &m.kind,
                &m.note,
                m.observation.as_deref(),
            )?;
            println!("Milestone #{id}");
        }
        ChangesCommand::Edit { id, milestone: m } => {
            setup::save_milestone(
                &conn,
                Some(id),
                &m.title,
                &milestone_time(&conn, &m.at)?,
                scope.repo.as_deref(),
                &m.kind,
                &m.note,
                m.observation.as_deref(),
            )?;
            println!("Updated #{id}");
        }
        ChangesCommand::Remove { id } => {
            if conn.execute("DELETE FROM change_milestone WHERE id=?1", [id])? == 0 {
                bail!("Milestone not found");
            }
            println!("Removed #{id}");
        }
        ChangesCommand::Capture {
            skill_root,
            timezone,
        } => {
            let (tz, _) = trends::timezone(&conn, timezone.as_deref())?;
            conn.execute(
                "INSERT OR REPLACE INTO analytics_setting VALUES('timezone',?1)",
                [tz.name()],
            )?;
            if !skill_root.is_empty() {
                conn.execute(
                    "INSERT OR REPLACE INTO analytics_setting VALUES('skill_roots',?1)",
                    [serde_json::to_string(&skill_root)?],
                )?;
            }
            let count = setup::capture(&conn, scope.repo.as_deref(), &[])?;
            println!("Captured {count} new observations · timezone {tz}");
        }
    }
    Ok(())
}
pub fn run_auto(conn: &Connection, args: AutoArgs) -> Result<()> {
    let mut scope = args.scope.scope()?;
    if args.scope.repo.is_none() {
        scope.repo = None;
    }
    let mut config = crate::classify::config::ClassifierConfig::load()?;
    if matches!(
        args.command,
        AutoCommand::Enable { .. } | AutoCommand::Disable
    ) && args.scope.repo.is_some()
    {
        bail!(
            "Classifier enable/disable applies globally. Omit --repo; use allow_projects/deny_projects to exclude specific projects."
        );
    }
    match args.command {
        AutoCommand::Status => {
            let mut result = envelope(
                conn,
                &scope,
                crate::classify::automatic::status(conn, scope.repo.as_deref(), &config)?,
            )?;
            result["automatic_classification"] = serde_json::json!({"enabled":config.enabled,"scope":"all_ingested_projects","monthly_budget_usd":config.monthly_budget_usd,"project_monthly_budget_usd":config.project_monthly_budget_usd,"max_run_usd":config.max_run_usd,"backfill_days":config.backfill_days,"allow_projects":config.allow_projects,"deny_projects":config.deny_projects});
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        AutoCommand::Enable {
            monthly_budget,
            project_monthly_budget,
            max_run_usd,
            backfill_days,
        } => {
            if let Some(value) = monthly_budget {
                config.monthly_budget_usd = value;
            }
            if let Some(value) = project_monthly_budget {
                config.project_monthly_budget_usd = value;
            }
            if let Some(value) = max_run_usd {
                config.max_run_usd = value;
            }
            if let Some(value) = backfill_days {
                config.backfill_days = value;
            }
            for value in [
                config.monthly_budget_usd,
                config.project_monthly_budget_usd,
                config.max_run_usd,
            ] {
                if !value.is_finite() || value <= 0.0 {
                    bail!("Spending caps must be positive finite amounts");
                }
            }
            config.enabled = true;
            config.save()?;
            crate::classify::automatic::ensure_projects(conn, &config)?;
            if backfill_days.is_some() {
                let since = (chrono::Utc::now()
                    - chrono::Duration::days(config.backfill_days as i64))
                .to_rfc3339();
                conn.execute("UPDATE classifier_auto_project SET since=?1 WHERE julianday(since)>julianday(?1)",[since])?;
            }
            println!(
                "Enabled the external classifier globally. All current and future ingested projects are covered, subject to project exclusions and spending caps. Future ingestion sends eligible redacted excerpts. Global monthly cap ${:.2}; default project monthly cap ${:.2}; run cap ${:.2}; initial backfill {} days. No content sent by this command.",
                config.monthly_budget_usd,
                config.project_monthly_budget_usd,
                config.max_run_usd,
                config.backfill_days
            );
            if crate::classify::config::api_key()?.is_none() {
                println!(
                    "Classification pending: configure the classifier API key before ingestion can send excerpts."
                );
            }
        }
        AutoCommand::Disable => {
            config.enabled = false;
            config.save()?;
            println!(
                "Disabled automatic classification globally. Existing labels and spending history are retained."
            );
        }
        AutoCommand::Limits {
            monthly_budget,
            max_run_usd,
            backfill_days,
        } => {
            let project_scope = args.scope.scope()?;
            let repo = project_scope.repo.as_deref().ok_or_else(|| {
                anyhow::anyhow!("Choose a project with --repo or run inside its repository")
            })?;
            let previous = crate::classify::automatic::status(conn, Some(repo), &config)?
                .into_iter()
                .next();
            let budget = monthly_budget.unwrap_or_else(|| {
                previous
                    .as_ref()
                    .map_or(config.project_monthly_budget_usd, |p| p.monthly_budget)
            });
            let run_cap = max_run_usd.unwrap_or_else(|| {
                previous
                    .as_ref()
                    .map_or(config.max_run_usd, |p| p.max_run_usd)
            });
            crate::classify::automatic::configure_limits(
                conn,
                repo,
                budget,
                run_cap,
                backfill_days.unwrap_or(config.backfill_days),
            )?;
            if backfill_days.is_none()
                && let Some(previous) = previous
            {
                conn.execute(
                    "UPDATE classifier_auto_project SET since=?2 WHERE repo_root=?1",
                    rusqlite::params![repo, previous.since],
                )?;
            }
            println!(
                "Updated limits and historical coverage for {repo}. Global classifier activation is unchanged."
            );
        }
    }
    Ok(())
}
