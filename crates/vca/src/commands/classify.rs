//! `vca classify`: optional Jev classification (plan, preview, run, overrides, eval).

use anyhow::{Result, bail};
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal, Read, Write};

use crate::app::AppContext;
use crate::classify::client::OpenRouterClient;
use crate::classify::config::{self, ClassifierConfig};
use crate::classify::heuristic::{task_unit_key, turn_unit_key};
use crate::classify::questions::{labels_for, unit_kind_for};
use crate::classify::run::{self, Passes, RunOptions, RunSummary, SessionRef};
use crate::classify::state::Level;
use crate::cli::{ClassifyArgs, ClassifyCommands, ClassifyKeyCommands, ClassifySelectArgs};
use crate::commands::insights::{filters_from_args, resolve_session};
use crate::db;
use crate::explore::{self, SessionSort, format as fmt};

const NOTICE: &str = "Classification sends redacted conversation excerpts to OpenRouter, which routes them to TypeSafe's \
Jev model. Requests ask for zero data retention (zdr=true) and fail if no such endpoint exists; check the \
provider's data policy before use. Code blocks are stripped at the default level; secrets, emails and home \
paths are redacted at every level.";

pub fn run(args: ClassifyArgs, _context: AppContext) -> Result<()> {
    let conn = db::open()?;
    let config = ClassifierConfig::load()?;
    match args.command {
        ClassifyCommands::Plan(select) => {
            let level = level_of(&select, &config)?;
            let sessions = select_sessions(&conn, &select)?;
            let summary = run::plan(&conn, &config, &sessions, level)?;
            if select.filters.json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                print_plan(&summary, level, &config, run::monthly_spend(&conn)?);
            }
            Ok(())
        }
        ClassifyCommands::Preview {
            session,
            pass,
            unit,
            level,
        } => {
            let level = match level {
                Some(value) => parse_level(&value)?,
                None => config.level(),
            };
            let (provider, session_id) = resolve_session(&conn, &session)?;
            let reference = session_ref(&conn, provider, session_id)?;
            let preview = run::preview(&conn, &config, &reference, &pass, unit, level)?;
            println!("{}", serde_json::to_string_pretty(&preview)?);
            Ok(())
        }
        ClassifyCommands::Run {
            select,
            max_usd,
            passes,
            yes,
        } => run_classifier(&conn, &config, &select, max_usd, &passes, yes),
        ClassifyCommands::Status => status(&conn, &config),
        ClassifyCommands::Key { command } => match command {
            ClassifyKeyCommands::Set { key } => {
                let key = match key {
                    Some(key) => key,
                    None => {
                        if std::io::stdin().is_terminal() {
                            eprint!("OpenRouter API key: ");
                            std::io::stderr().flush()?;
                        }
                        let mut text = String::new();
                        std::io::stdin().read_to_string(&mut text)?;
                        text
                    }
                };
                let path = config::save_api_key(&key)?;
                println!("Saved OpenRouter key to {} (mode 0600).", path.display());
                Ok(())
            }
            ClassifyKeyCommands::Delete => {
                if config::delete_api_key()? {
                    println!("Deleted the saved OpenRouter key.");
                } else {
                    println!("No saved OpenRouter key.");
                }
                Ok(())
            }
        },
        ClassifyCommands::Config {
            level,
            monthly_budget,
            allow_project,
            deny_project,
            reset_projects,
        } => {
            let mut config = config;
            if let Some(level) = level {
                config.default_level = parse_level(&level)?.as_str().into();
            }
            if let Some(budget) = monthly_budget {
                config.monthly_budget_usd = budget.max(0.0);
            }
            if reset_projects {
                config.allow_projects.clear();
                config.deny_projects.clear();
            }
            config.allow_projects.extend(allow_project);
            config.deny_projects.extend(deny_project);
            let path = config.save()?;
            println!("Saved {}:", path.display());
            println!("{}", serde_json::to_string_pretty(&config)?);
            Ok(())
        }
        ClassifyCommands::Set {
            session,
            task,
            turn,
            question,
            label,
            note,
        } => set_label(
            &conn,
            &session,
            task,
            turn,
            &question,
            &label,
            note.as_deref(),
        ),
        ClassifyCommands::SetTurn {
            session,
            turn,
            task,
        } => {
            let (provider, session_id) = resolve_session(&conn, &session)?;
            conn.execute(
                "INSERT OR REPLACE INTO task_turn_assignment(provider, session_id, turn_index, task_index, source, confidence)
                 VALUES (?1, ?2, ?3, ?4, 'override', 1.0)",
                params![provider, session_id, turn, task],
            )?;
            crate::insights::refresh(&conn)?;
            println!(
                "Turn {turn} of {provider}:{session_id} now starts task {task} (later turns follow until the next assignment)."
            );
            Ok(())
        }
        ClassifyCommands::SetCompletion {
            session,
            task,
            turn,
        } => {
            let (provider, session_id) = resolve_session(&conn, &session)?;
            let completion: Option<i64> = match turn.as_str() {
                "none" | "not_completed" => None,
                value => Some(
                    value
                        .parse()
                        .map_err(|_| anyhow::anyhow!("turn must be a number or `none`"))?,
                ),
            };
            conn.execute(
                "INSERT OR REPLACE INTO task_completion_mark(provider, session_id, task_index, completion_turn, source, confidence)
                 VALUES (?1, ?2, ?3, ?4, 'override', 1.0)",
                params![provider, session_id, task, completion],
            )?;
            crate::insights::refresh(&conn)?;
            match completion {
                Some(turn) => println!("Task {task} marked completed at turn {turn}."),
                None => println!("Task {task} marked not completed."),
            }
            Ok(())
        }
        ClassifyCommands::ClearOverrides { session } => {
            let (provider, session_id) = resolve_session(&conn, &session)?;
            let prefix = format!("{provider}|{session_id}|");
            let mut removed = 0;
            removed += conn.execute(
                "DELETE FROM task_turn_assignment WHERE provider = ?1 AND session_id = ?2 AND source = 'override'",
                params![provider, session_id],
            )?;
            removed += conn.execute(
                "DELETE FROM task_completion_mark WHERE provider = ?1 AND session_id = ?2 AND source = 'override'",
                params![provider, session_id],
            )?;
            removed += conn.execute(
                "DELETE FROM classification_override WHERE substr(unit_key, 1, length(?1)) = ?1",
                params![prefix],
            )?;
            crate::insights::refresh(&conn)?;
            println!("Removed {removed} overrides.");
            Ok(())
        }
        ClassifyCommands::Eval { question, json } => eval(&conn, question.as_deref(), json),
    }
}

/// Classifier amounts are often fractions of a cent.
fn small_usd(value: f64) -> String {
    if value > 0.0 && value < 0.01 {
        format!("${value:.5}")
    } else {
        format!("${value:.2}")
    }
}

fn parse_level(value: &str) -> Result<Level> {
    Level::parse(value).ok_or_else(|| {
        anyhow::anyhow!("unknown level '{value}' (evidence_only, user_text, conversation, full)")
    })
}

fn level_of(select: &ClassifySelectArgs, config: &ClassifierConfig) -> Result<Level> {
    match &select.level {
        Some(value) => parse_level(value),
        None => Ok(config.level()),
    }
}

fn session_ref(conn: &Connection, provider: String, session_id: String) -> Result<SessionRef> {
    let repo_root: Option<String> = conn.query_row(
        "SELECT MAX(repo_root) FROM derived_task WHERE provider = ?1 AND session_id = ?2",
        params![provider, session_id],
        |row| row.get(0),
    )?;
    Ok(SessionRef {
        provider,
        session_id,
        repo_root,
    })
}

fn select_sessions(conn: &Connection, select: &ClassifySelectArgs) -> Result<Vec<SessionRef>> {
    if !select.session.is_empty() {
        return select
            .session
            .iter()
            .map(|input| {
                let (provider, session_id) = resolve_session(conn, input)?;
                session_ref(conn, provider, session_id)
            })
            .collect();
    }
    let filters = filters_from_args(&select.filters);
    Ok(
        explore::sessions(conn, &filters, SessionSort::Cost, select.filters.limit)?
            .into_iter()
            .map(|row| SessionRef {
                provider: row.provider,
                session_id: row.session_id,
                repo_root: row.repo_root,
            })
            .collect(),
    )
}

fn print_plan(summary: &RunSummary, level: Level, config: &ClassifierConfig, month_spent: f64) {
    println!("Classify with Jev ({} via OpenRouter)", config.model);
    println!(
        "Selection: {} sessions ({} excluded by project rules), content level {}",
        summary.sessions,
        summary.excluded_projects,
        level.as_str()
    );
    let unit_names = [
        ("turn", "user turns (task segmentation)"),
        ("exchange", "exchanges (feedback)"),
        ("task", "tasks (labels)"),
        ("task_completion", "tasks (completion turn)"),
    ];
    for (kind, label) in unit_names {
        if let Some(count) = summary.units.get(kind) {
            println!("  {count:>6}  {label}");
        }
    }
    println!(
        "Already cached: {} units · to send: ~{} input tokens · est. cost {} · month left {}",
        summary.cached,
        fmt::tokens(summary.est_tokens as i64),
        small_usd(summary.est_usd),
        small_usd((config.monthly_budget_usd - month_spent).max(0.0))
    );
    println!(
        "Task counts are from the current segmentation; splitting sessions adds task units. Redactions so far: {}.",
        summary.redactions
    );
}

fn run_classifier(
    conn: &Connection,
    config: &ClassifierConfig,
    select: &ClassifySelectArgs,
    max_usd: f64,
    passes: &str,
    yes: bool,
) -> Result<()> {
    let level = level_of(select, config)?;
    let passes = Passes::parse(passes).ok_or_else(|| {
        anyhow::anyhow!("--passes takes segment, feedback, labels, completion or all")
    })?;
    let Some(key) = config::api_key()? else {
        bail!(
            "No OpenRouter key. Set {} or run `vca classify key set`.",
            config::KEY_ENV
        );
    };
    let sessions = select_sessions(conn, select)?;
    let month_spent = run::monthly_spend(conn)?;
    let estimate = run::plan(conn, config, &sessions, level)?;
    print_plan(&estimate, level, config, month_spent);
    println!("Run cap: {}\n", small_usd(max_usd));
    if !yes {
        println!("{NOTICE}\n");
        if !std::io::stdin().is_terminal() {
            bail!("Re-run with --yes to send.");
        }
        eprint!("Send? [y/N] ");
        std::io::stderr().flush()?;
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes") {
            println!("Nothing sent.");
            return Ok(());
        }
    }
    let client = OpenRouterClient::new(&config.endpoint, &key)?;
    let options = RunOptions {
        level,
        max_usd,
        passes,
        dry_run: false,
    };
    let summary = run::run(conn, &client, config, &sessions, &options)?;
    if select.filters.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }
    println!(
        "Sent {} requests ({} cached, {} failed, {} invalid answers, {} degraded) · {} input tokens · {}",
        summary.sent,
        summary.cached,
        summary.failed,
        summary.invalid_answers,
        summary.degraded,
        fmt::tokens(summary.input_tokens),
        small_usd(summary.cost_usd)
    );
    println!(
        "{} sessions split into several tasks · {} completion turns set",
        summary.tasks_split, summary.completions
    );
    if let Some(reason) = &summary.stopped {
        println!("Stopped: {reason}. Remaining units stay unclassified; re-run to continue.");
    }
    Ok(())
}

fn status(conn: &Connection, config: &ClassifierConfig) -> Result<()> {
    let key_source = if std::env::var(config::KEY_ENV).is_ok_and(|v| !v.trim().is_empty()) {
        format!("env {}", config::KEY_ENV)
    } else if config::api_key()?.is_some() {
        format!("saved ({})", config::key_path()?.display())
    } else {
        "none".to_string()
    };
    println!(
        "Model {} · level {} · key {}",
        config.model, config.default_level, key_source
    );
    println!(
        "Projects: allow {:?} · deny {:?} · zdr {} · fallbacks {}",
        config.allow_projects,
        config.deny_projects,
        config.provider.zdr,
        config.provider.allow_fallbacks
    );
    let (runs, spent_total): (i64, f64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(cost_usd), 0) FROM classifier_run",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    println!(
        "Spend: {} this month of {} budget · {} all time over {} runs",
        small_usd(run::monthly_spend(conn)?),
        small_usd(config.monthly_budget_usd),
        small_usd(spent_total),
        runs
    );
    let (tasks, jev_tasks, split_sessions): (i64, i64, i64) = conn.query_row(
        "SELECT COUNT(*),
                SUM(EXISTS(SELECT 1 FROM classification_result r WHERE r.source = 'jev'
                    AND r.unit_kind = 'task' AND r.superseded_by IS NULL
                    AND r.unit_key = t.provider || '|' || t.session_id || '|' || t.task_index)),
                (SELECT COUNT(DISTINCT provider || session_id) FROM derived_task WHERE task_index > 0)
         FROM derived_task t",
        [],
        |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0), row.get(2)?)),
    )?;
    let overrides: i64 = conn.query_row(
        "SELECT (SELECT COUNT(*) FROM classification_override)
              + (SELECT COUNT(*) FROM task_turn_assignment WHERE source = 'override')
              + (SELECT COUNT(*) FROM task_completion_mark WHERE source = 'override')",
        [],
        |row| row.get(0),
    )?;
    println!(
        "Coverage: {jev_tasks}/{tasks} tasks labelled by Jev · {split_sessions} sessions split into tasks · {overrides} manual overrides"
    );
    let failures: Vec<(String, i64)> = conn
        .prepare(
            "SELECT status, COUNT(*) FROM classifier_request WHERE status NOT IN ('ok') GROUP BY status",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if !failures.is_empty() {
        let text: Vec<String> = failures.iter().map(|(s, n)| format!("{s} {n}")).collect();
        println!("Request problems: {}", text.join(" · "));
    }
    Ok(())
}

fn set_label(
    conn: &Connection,
    session: &str,
    task: Option<i64>,
    turn: Option<i64>,
    question: &str,
    label: &str,
    note: Option<&str>,
) -> Result<()> {
    let Some(labels) = labels_for(question) else {
        bail!("unknown question '{question}'");
    };
    if !labels.contains(&label) {
        bail!(
            "'{label}' is not a label of {question}: {}",
            labels.join(", ")
        );
    }
    let (provider, session_id) = resolve_session(conn, session)?;
    let unit_kind = unit_kind_for(question);
    let unit_key = match (unit_kind, task, turn) {
        ("task", Some(task), None) => task_unit_key(&provider, &session_id, task),
        ("task", None, None) => task_unit_key(&provider, &session_id, 0),
        ("exchange", None, Some(turn)) => turn_unit_key(&provider, &session_id, turn),
        ("exchange", _, None) => bail!("{question} is a per-turn question; pass --turn"),
        _ => bail!(
            "{question} takes {}",
            if unit_kind == "task" {
                "--task"
            } else {
                "--turn"
            }
        ),
    };
    conn.execute(
        "INSERT OR REPLACE INTO classification_override(unit_kind, unit_key, question_id, label, note)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![unit_kind, unit_key, question, label, note],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO classification_gold(unit_kind, unit_key, question_id, label, origin)
         VALUES (?1, ?2, ?3, ?4, 'override')",
        params![unit_kind, unit_key, question, label],
    )?;
    println!("{question} = {label} for {unit_key} (override).");
    Ok(())
}

#[derive(Debug, Default, serde::Serialize)]
pub struct EvalRow {
    pub question: String,
    pub source: String,
    pub n: usize,
    pub accuracy: f64,
    pub macro_f1: f64,
    pub top_confusions: Vec<(String, String, usize)>,
}

/// Compare stored labels with gold labels (overrides count as gold).
pub fn evaluate(conn: &Connection, question: Option<&str>) -> Result<Vec<EvalRow>> {
    let mut stmt = conn.prepare(
        "SELECT g.question_id, r.source, g.label, r.label
         FROM classification_gold g
         JOIN classification_result r
           ON r.unit_kind = g.unit_kind AND r.unit_key = g.unit_key AND r.question_id = g.question_id
          AND r.superseded_by IS NULL AND r.label IS NOT NULL
         WHERE (?1 IS NULL OR g.question_id = ?1)
         ORDER BY g.question_id, r.source",
    )?;
    let mut pairs: BTreeMap<(String, String), Vec<(String, String)>> = BTreeMap::new();
    for row in stmt.query_map(params![question], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })? {
        let (question, source, gold, predicted) = row?;
        pairs
            .entry((question, source))
            .or_default()
            .push((gold, predicted));
    }
    Ok(pairs
        .into_iter()
        .map(|((question, source), pairs)| {
            let n = pairs.len();
            let correct = pairs.iter().filter(|(g, p)| g == p).count();
            let classes: BTreeSet<&String> = pairs.iter().flat_map(|(g, p)| [g, p]).collect();
            let f1s: Vec<f64> = classes
                .iter()
                .map(|class| {
                    let tp = pairs
                        .iter()
                        .filter(|(g, p)| g == *class && p == *class)
                        .count() as f64;
                    let fp = pairs
                        .iter()
                        .filter(|(g, p)| g != *class && p == *class)
                        .count() as f64;
                    let fn_ = pairs
                        .iter()
                        .filter(|(g, p)| g == *class && p != *class)
                        .count() as f64;
                    if tp == 0.0 {
                        0.0
                    } else {
                        2.0 * tp / (2.0 * tp + fp + fn_)
                    }
                })
                .collect();
            let mut confusions: BTreeMap<(String, String), usize> = BTreeMap::new();
            for (gold, predicted) in pairs.iter().filter(|(g, p)| g != p) {
                *confusions
                    .entry((gold.clone(), predicted.clone()))
                    .or_default() += 1;
            }
            let mut top: Vec<(String, String, usize)> = confusions
                .into_iter()
                .map(|((g, p), n)| (g, p, n))
                .collect();
            top.sort_by_key(|(_, _, n)| std::cmp::Reverse(*n));
            top.truncate(3);
            EvalRow {
                question,
                source,
                n,
                accuracy: correct as f64 / n.max(1) as f64,
                macro_f1: f1s.iter().sum::<f64>() / f1s.len().max(1) as f64,
                top_confusions: top,
            }
        })
        .collect())
}

fn eval(conn: &Connection, question: Option<&str>, json: bool) -> Result<()> {
    let rows = evaluate(conn, question)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!(
            "No gold labels yet. Correct labels with `vca classify set` (each override becomes a gold label)."
        );
        return Ok(());
    }
    let table: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            vec![
                row.question.clone(),
                row.source.clone(),
                row.n.to_string(),
                format!("{:.0}%", row.accuracy * 100.0),
                format!("{:.2}", row.macro_f1),
                row.top_confusions
                    .iter()
                    .map(|(g, p, n)| format!("{g}→{p} ×{n}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ]
        })
        .collect();
    print!(
        "{}",
        fmt::table(
            &[
                "Question",
                "Source",
                "n",
                "Accuracy",
                "Macro-F1",
                "Top confusions"
            ],
            &table
        )
    );
    println!(
        "Overrides are biased toward mistakes (you correct what looks wrong), so accuracy here is a lower bound."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_computes_accuracy_and_macro_f1() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        for (key, gold, jev) in [
            ("a", "bug_fix", "bug_fix"),
            ("b", "feature", "bug_fix"),
            ("c", "feature", "feature"),
        ] {
            conn.execute(
                "INSERT INTO classification_gold(unit_kind, unit_key, question_id, label, origin)
                 VALUES ('task', ?1, 'intent', ?2, 'override')",
                params![key, gold],
            )?;
            conn.execute(
                "INSERT INTO classification_result(unit_kind, unit_key, question_id, question_version, source, label, band, cache_key)
                 VALUES ('task', ?1, 'intent', 'v', 'jev', ?2, 'confident', ?1)",
                params![key, jev],
            )?;
        }
        let rows = evaluate(&conn, Some("intent"))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].n, 3);
        assert!((rows[0].accuracy - 2.0 / 3.0).abs() < 1e-9);
        // bug_fix F1 = 2/3, feature F1 = 2/3.
        assert!((rows[0].macro_f1 - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(
            rows[0].top_confusions[0],
            ("feature".into(), "bug_fix".into(), 1)
        );
        Ok(())
    }
}
