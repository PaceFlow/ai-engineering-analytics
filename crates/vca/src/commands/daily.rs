//! Daily work-item reports and read-only source setup, shared with Paceflow.
use super::observe::ScopeArgs;
use crate::insights::{daily, delivery, observation};
use anyhow::{Result, ensure};
use clap::{Args, Subcommand};
use rusqlite::OptionalExtension;
use std::fmt::Write;

#[derive(Debug, Clone, Args)]
pub struct DailyArgs {
    #[command(flatten)]
    pub scope: ScopeArgs,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub timezone: Option<String>,
    #[arg(long, conflicts_with = "json")]
    pub csv: bool,
    #[arg(long)]
    pub cohorts: bool,
    #[arg(long, default_value_t = 7)]
    pub follow_up_days: u32,
    #[arg(long)]
    pub include_estimated: bool,
    /// List contributors for one day, independently of the compact report.
    #[arg(long)]
    pub details: Option<String>,
    #[arg(long, default_value = "active")]
    pub metric: String,
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}
impl DailyArgs {
    pub fn options(&self) -> Result<daily::Options> {
        let mut scope = self.scope.scope()?;
        scope.include_estimated = self.include_estimated;
        ensure!(self.follow_up_days > 0, "Follow-up days must be positive");
        Ok(daily::Options {
            scope,
            from: self.from.clone(),
            to: self.to.clone(),
            timezone: self.timezone.clone(),
            follow_up_days: self.follow_up_days,
        })
    }
}
pub fn run(args: DailyArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let options = args.options()?;
    conn.execute_batch("BEGIN DEFERRED")?;
    if let Some(date) = &args.details {
        let evidence =
            daily::contributors(&conn, &options, date, &args.metric, args.offset, args.limit)?;
        println!("{}", serde_json::to_string_pretty(&evidence)?);
    } else {
        let report = daily::report(&conn, &options)?;
        if args.scope.json {
            println!("{}", serde_json::to_string(&report)?);
        } else if args.csv {
            print!("{}", csv(&report, args.cohorts));
        } else {
            print!("{}", text(&report, args.cohorts));
        }
    }
    conn.execute_batch("COMMIT")?;
    Ok(())
}
fn optional(v: Option<f64>) -> String {
    v.map(|n| format!("{n:.2}")).unwrap_or_else(|| "—".into())
}
pub fn text(report: &daily::Report, cohorts: bool) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Daily engineering work · {} · {}",
        report.timezone,
        report.scope.repo.as_deref().unwrap_or("all projects")
    );
    let _ = writeln!(
        out,
        "Published {} · generated {}",
        report.published_revision.as_deref().unwrap_or("not yet"),
        report.generated_at
    );
    if cohorts {
        let _ = writeln!(
            out,
            "PR-creation cohorts · deployed within {} days; pending is not failure",
            report.follow_up_days
        );
        let _ = writeln!(
            out,
            "Date        Opened Bug PRs Mature Pending Deployed Closed/unmerged Share"
        );
        for c in &report.cohorts {
            let _ = writeln!(
                out,
                "{} {:>6} {:>7} {:>6} {:>7} {:>8} {:>15} {}",
                c.date,
                c.prs_opened,
                c.bug_fix_prs,
                c.mature,
                c.pending_followup,
                c.deployed_within_window,
                c.closed_without_merge,
                c.share_deployed_within_window
                    .map(|v| format!("{:.1}%", 100.0 * v))
                    .unwrap_or_else(|| "—".into())
            );
        }
    } else {
        let _ = writeln!(
            out,
            "Date        Start Active Done Open Bug active/done  Tokens  Reported $  PR merged/deployed  Leading model"
        );
        for d in &report.days {
            let model = d
                .models
                .iter()
                .max_by_key(|m| m.usage_events)
                .map(|m| format!("{} ({} usage events)", m.model, m.usage_events))
                .unwrap_or_else(|| "—".into());
            let _ = writeln!(
                out,
                "{}{} {:>4} {:>6} {:>4} {:>4} {:>8} {:>8} {:>10} {:>5}/{:<5} {}",
                d.date,
                if d.partial { "*" } else { " " },
                d.started,
                d.active_work,
                d.completed,
                d.open,
                format!("{}/{}", d.bug_work_active, d.bug_work_completed),
                d.native_tokens.total,
                optional(d.reported_cost_usd),
                d.pull_requests.merged,
                d.pull_requests.deployed,
                model
            );
            let _ = writeln!(
                out,
                "  activity {:.1}m · attempts {} · corrections {} · labels {}/{} · usage {}/{} · work without PR {} · deployment pending {}",
                d.work_activity_minutes,
                d.attempts,
                d.corrections,
                d.coverage
                    .work_items
                    .saturating_sub(d.coverage.heuristic_intents + d.coverage.unclassified_intents),
                d.coverage.work_items,
                d.coverage.available_usage_events,
                d.coverage.usage_events,
                d.coverage.work_without_pr,
                d.pull_requests.pending_delivery
            );
            if !d.goal_mix.is_empty() {
                let _ = writeln!(
                    out,
                    "  goals: {}",
                    d.goal_mix
                        .iter()
                        .map(|m| format!("{} {}", m.label, m.work_items))
                        .collect::<Vec<_>>()
                        .join(" · ")
                );
            }
            if !d.activity_mix.is_empty() {
                let _ = writeln!(
                    out,
                    "  stages: {}",
                    d.activity_mix
                        .iter()
                        .map(|m| format!("{} {:.1}m", m.label, m.activity_minutes))
                        .collect::<Vec<_>>()
                        .join(" · ")
                );
            }
        }
    }
    for w in &report.warnings {
        let _ = writeln!(out, "Note: {w}");
    }
    let _ = writeln!(
        out,
        "* partial/uncertain source coverage. Completion and deployment are independent. Activity is estimated work activity, not human hours. Bug work and bug-fix PRs are different units."
    );
    out
}
pub fn csv(report: &daily::Report, cohorts: bool) -> String {
    let mut out = String::new();
    if cohorts {
        out.push_str("date,prs_opened,bug_fix_prs,mature,pending_followup,deployed_within_window,closed_without_merge,share_deployed_within_window\n");
        for c in &report.cohorts {
            let _ = writeln!(
                out,
                "{},{},{},{},{},{},{},{}",
                c.date,
                c.prs_opened,
                c.bug_fix_prs,
                c.mature,
                c.pending_followup,
                c.deployed_within_window,
                c.closed_without_merge,
                c.share_deployed_within_window
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            );
        }
    } else {
        out.push_str("date,partial,started,active_work,completed,open,bug_work_active,native_tokens,estimated_tokens,reported_cost_usd,api_equivalent_usd,attempts,corrections,work_activity_minutes,prs_opened,prs_merged,prs_deployed,bug_fix_prs_deployed,pending_delivery,available_usage_events,usage_events,work_without_pr,bug_work_started,bug_work_completed,bug_work_verified,bug_work_open\n");
        for d in &report.days {
            let _ = writeln!(
                out,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                d.date,
                d.partial,
                d.started,
                d.active_work,
                d.completed,
                d.open,
                d.bug_work_active,
                d.native_tokens.total,
                d.estimated_tokens.total,
                d.reported_cost_usd
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                d.api_equivalent_usd
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                d.attempts,
                d.corrections,
                d.work_activity_minutes,
                d.pull_requests.opened,
                d.pull_requests.merged,
                d.pull_requests.deployed,
                d.pull_requests.bug_fix_prs_deployed,
                d.pull_requests.pending_delivery,
                d.coverage.available_usage_events,
                d.coverage.usage_events,
                d.coverage.work_without_pr,
                d.bug_work_started,
                d.bug_work_completed,
                d.bug_work_verified,
                d.bug_work_open
            );
        }
    }
    out
}

#[derive(Debug, Clone, Args)]
pub struct SourcesArgs {
    #[command(subcommand)]
    pub command: SourceCommand,
    #[arg(long, global = true)]
    pub json: bool,
}
#[derive(Debug, Clone, Subcommand)]
pub enum SourceCommand {
    /// Add/update a JSON source profile. Credentials are environment-variable references only.
    Add {
        file: std::path::PathBuf,
    },
    List,
    Status,
    Sync {
        name: Option<String>,
    },
    /// Import versioned PR, deployment, and coverage records (JSON or NDJSON).
    Import {
        file: std::path::PathBuf,
    },
    /// Explicit contribution link. A rejected link survives automatic discovery.
    Link {
        member: String,
        pr: String,
        #[arg(long, default_value = "implementation")]
        role: String,
        #[arg(long, default_value = "accepted")]
        decision: String,
        #[arg(long, default_value = "")]
        note: String,
    },
    Intent {
        pr: String,
        intent: String,
    },
    Alias {
        alias: String,
        canonical: String,
    },
    Show {
        pr: String,
    },
}
pub fn sources(args: SourcesArgs) -> Result<()> {
    sources_for(args, crate::app::AppContext::VCA)
}
pub fn sources_for(args: SourcesArgs, context: crate::app::AppContext) -> Result<()> {
    let conn = crate::db::open()?;
    let result = match args.command {
        SourceCommand::Add { file } => {
            let s: crate::delivery_sources::Source = serde_json::from_slice(&std::fs::read(file)?)?;
            crate::delivery_sources::save(&conn, &s)?;
            crate::insights::refresh(&conn)?;
            serde_json::json!({"source":s.name,"saved":true})
        }
        SourceCommand::List => serde_json::to_value(crate::delivery_sources::sources(&conn)?)?,
        SourceCommand::Status => serde_json::to_value(observation::coverage(&conn)?)?,
        SourceCommand::Sync { name } => serde_json::to_value(crate::delivery_sources::sync_for(
            &conn,
            name.as_deref(),
            context,
        )?)?,
        SourceCommand::Import { file } => {
            serde_json::json!({"imported":delivery::import(&conn,&std::fs::read(file)?)?})
        }
        SourceCommand::Link {
            member,
            pr,
            role,
            decision,
            note,
        } => {
            let member = crate::insights::work::resolve_member(&conn, &member)?;
            delivery::link(&conn, &member, &pr, &role, &decision, "manual", &note)?;
            serde_json::json!({"member":member,"pr":pr,"decision":decision})
        }
        SourceCommand::Intent { pr, intent } => {
            delivery::set_intent(&conn, &pr, &intent, "override")?;
            serde_json::json!({"pr":pr,"intent":intent})
        }
        SourceCommand::Alias { alias, canonical } => {
            observation::set_alias(&conn, &alias, &canonical, "manual")?;
            crate::insights::refresh(&conn)?;
            serde_json::json!({"alias":alias,"canonical":canonical})
        }
        SourceCommand::Show { pr } => serde_json::to_value(
            delivery::evidence(&conn)?
                .into_iter()
                .filter(|e| e.pr.id == pr || e.pr.url == pr)
                .collect::<Vec<_>>(),
        )?,
    };
    // Mutations only affect enrichment, never goal completion.
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({"schema_version":1,"data":result}))?
    );
    Ok(())
}
#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    #[arg(long)]
    pub json: bool,
}
pub fn status(args: StatusArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let config = crate::classify::config::ClassifierConfig::load()?;
    let revision = conn
        .query_row(
            "SELECT value FROM analytics_setting WHERE key='published_revision'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    let jobs=conn.prepare("SELECT id,state,passes,spent_usd,max_usd,error FROM classification_job ORDER BY id DESC LIMIT 20")?.query_map([],|r|Ok(serde_json::json!({"id":r.get::<_,i64>(0)?,"state":r.get::<_,String>(1)?,"passes":r.get::<_,String>(2)?,"spent_usd":r.get::<_,f64>(3)?,"max_usd":r.get::<_,f64>(4)?,"error":r.get::<_,Option<String>>(5)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let unresolved: i64 = conn.query_row(
        "SELECT count(*) FROM work_relation WHERE decision='suggested'",
        [],
        |r| r.get(0),
    )?;
    let uncertain: i64 = conn.query_row(
        "SELECT count(*) FROM derived_work_index WHERE intent_source IN ('heuristic','missing')",
        [],
        |r| r.get(0),
    )?;
    let result = serde_json::json!({"schema_version":1,"generated_at":chrono::Utc::now().to_rfc3339(),"published_revision":revision,"automatic_classification":config.enabled,"classification_projects":crate::classify::automatic::status(&conn,None,&config)?,"sources":observation::coverage(&conn)?,"classification_jobs":jobs,"uncertain_work_intents":uncertain,"unresolved_work_links":unresolved,"classification_monthly_spend_usd":crate::classify::run::monthly_spend(&conn)?});
    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "Published {} · automatic classification {} · uncertain intents {} · link suggestions {}",
            revision.as_deref().unwrap_or("not yet"),
            config.enabled,
            uncertain,
            unresolved
        );
        for s in observation::coverage(&conn)? {
            println!(
                "{} · observed through {} · {}{}",
                s.source_id,
                s.observed_through.as_deref().unwrap_or("unknown"),
                if s.complete {
                    "complete scan"
                } else {
                    "partial/unknown"
                },
                s.error.map(|e| format!(" · {e}")).unwrap_or_default()
            );
        }
        for job in jobs {
            println!(
                "Classification job {} · {} · ${} / ${}",
                job["id"], job["state"], job["spent_usd"], job["max_usd"]
            );
        }
    }
    Ok(())
}
