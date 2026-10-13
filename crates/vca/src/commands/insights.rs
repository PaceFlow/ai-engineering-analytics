//! `usage`, `quota`, `sessions`, `tasks`, `show` and `context` commands.

use anyhow::{Result, bail};
use rusqlite::Connection;

use crate::app::AppContext;
use crate::cli::{
    ContextArgs, InsightFilterArgs, QuotaArgs, QuotaCommands, SessionsArgs, ShowArgs, TasksArgs,
    UsageArgs,
};
use crate::db;
use crate::explore::{
    self, ContextDim, Filters, GroupDim, SessionSort, TaskSort, UsageDim, format as fmt, short_id,
    short_model, short_repo,
};

pub fn filters_from_args(args: &InsightFilterArgs) -> Filters {
    let today = chrono::Local::now().date_naive();
    let from = args.from.clone().or_else(|| {
        (args.days > 0).then(|| {
            (today - chrono::Duration::days(args.days as i64 - 1))
                .format("%Y-%m-%d")
                .to_string()
        })
    });
    let repo = args.repo.clone().or_else(|| {
        args.here.then(|| {
            std::env::current_dir()
                .ok()
                .and_then(|dir| crate::path_utils::detect_repo_root(&dir))
                .map(|root| crate::path_utils::path_to_string(&root))
        })?
    });
    Filters {
        from,
        to: args.to.clone(),
        providers: args.provider.clone(),
        model: args.model.clone(),
        repo,
        search: args.search.clone(),
        intent: args.intent.clone(),
        outcome: args.outcome.clone(),
        min_cost: args.min_cost,
        ..Filters::default()
    }
}

fn open() -> Result<Connection> {
    db::open()
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_footer(filters: &Filters) {
    let chips = filters.chips();
    let scope = if chips.is_empty() {
        "all data".to_string()
    } else {
        chips.join(" · ")
    };
    println!(
        "\n{scope} · ~ = API list-price estimate (pricing {}), not an invoice",
        crate::cost::pricing::pricing_version()
    );
}

pub fn run_usage(args: UsageArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    let filters = filters_from_args(&args.filters);
    let dim = UsageDim::parse(&args.group_by)
        .ok_or_else(|| anyhow::anyhow!("unknown --group-by {}", args.group_by))?;
    let rows = explore::usage_totals(&conn, &filters, dim)?;
    if args.filters.json {
        return print_json(&rows);
    }
    if rows.is_empty() {
        println!("No usage found. Run `vca ingest` first or widen the filters (--days 0 for all).");
        return Ok(());
    }
    let total_cost: f64 = rows.iter().filter_map(|row| row.cost).sum();
    let mut table_rows: Vec<Vec<String>> = rows
        .iter()
        .take(args.filters.limit)
        .map(|row| {
            let key = match dim {
                UsageDim::Model => short_model(&row.key),
                UsageDim::Project => short_repo(&row.key),
                _ => fmt::truncate(&row.key, 48),
            };
            vec![
                key,
                fmt::usd(row.cost),
                fmt::percent(row.cost.map(|cost| cost / total_cost.max(1e-9))),
                fmt::usd_plain(row.reported_cost),
                fmt::tokens(row.fresh_tokens),
                fmt::tokens(row.cache_read_tokens),
                fmt::tokens(row.output_tokens),
                row.sessions.to_string(),
                row.calls.to_string(),
                gaps(row.unpriced_tokens, row.estimated_tokens),
            ]
        })
        .collect();
    table_rows.push(vec![
        "Σ total".into(),
        fmt::usd(Some(total_cost)),
        "100%".into(),
        fmt::usd_plain(
            rows.iter()
                .filter_map(|row| row.reported_cost)
                .reduce(|a, b| a + b),
        ),
        fmt::tokens(rows.iter().map(|row| row.fresh_tokens).sum()),
        fmt::tokens(rows.iter().map(|row| row.cache_read_tokens).sum()),
        fmt::tokens(rows.iter().map(|row| row.output_tokens).sum()),
        String::new(),
        rows.iter().map(|row| row.calls).sum::<i64>().to_string(),
        gaps(
            rows.iter().map(|row| row.unpriced_tokens).sum(),
            rows.iter().map(|row| row.estimated_tokens).sum(),
        ),
    ]);
    print!(
        "{}",
        fmt::table(
            &[
                dim_header(dim),
                ">API-equiv",
                ">Share",
                ">Reported",
                ">Fresh tok",
                ">Cache read",
                ">Output",
                ">Sessions",
                ">Calls",
                "Gaps",
            ],
            &table_rows
        )
    );
    print_footer(&filters);
    println!(
        "Fresh tok = uncached input + cache writes + output. Reported = provider-reported cost (OpenCode)."
    );
    Ok(())
}

fn gaps(unpriced: i64, estimated: i64) -> String {
    let mut parts = Vec::new();
    if unpriced > 0 {
        parts.push(format!("{} unpriced", fmt::tokens(unpriced)));
    }
    if estimated > 0 {
        parts.push(format!("~{} est. tokens", fmt::tokens(estimated)));
    }
    parts.join(", ")
}

fn dim_header(dim: UsageDim) -> &'static str {
    match dim {
        UsageDim::Tool => "Tool",
        UsageDim::Model => "Model",
        UsageDim::Project => "Project",
        UsageDim::Session => "Session",
        UsageDim::AgentKind => "Agent",
        UsageDim::Day => "Day",
        UsageDim::Hour => "Hour",
    }
}

pub fn run_quota(args: QuotaArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    if let Some(QuotaCommands::Mark {
        provider,
        at,
        kind,
        note,
    }) = args.command
    {
        let at = at.unwrap_or_else(|| {
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        });
        crate::insights::quota::record_observation(&conn, &provider, &at, &kind, note.as_deref())?;
        println!("Recorded {kind} for {provider} at {at}.");
        return Ok(());
    }
    let filters = filters_from_args(&args.filters);
    let windows = explore::quota_windows(&conn, &filters)?;
    if args.filters.json {
        return print_json(&windows);
    }
    if windows.is_empty() {
        println!("No usage in this period.");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = windows
        .iter()
        .take(args.filters.limit)
        .map(|window| {
            let observed = match (window.start_percent, window.end_percent) {
                (Some(start), Some(end)) => {
                    let limit = if window.reached_limit_at.is_some() {
                        " ◆limit"
                    } else {
                        ""
                    };
                    format!("{start:.0}% → {end:.0}%{limit}")
                }
                _ if window.limit_hits_logged > 0 => format!(
                    "no % data, {} limit hit(s) logged",
                    window.limit_hits_logged
                ),
                _ => "no % data".to_string(),
            };
            vec![
                window.provider.clone(),
                window.name.clone(),
                format!(
                    "{} → {}",
                    fmt::when(window.window_start.as_deref().or(Some(&window.first_seen))),
                    fmt::when(window.resets_at.as_deref())
                ),
                observed,
                fmt::tokens(if window.fresh_tokens > 0 {
                    window.fresh_tokens
                } else {
                    window.tokens
                }),
                fmt::usd(window.cost),
                if window.unobserved_minutes > 0 {
                    format!("{}m unobserved", window.unobserved_minutes)
                } else {
                    String::new()
                },
            ]
        })
        .collect();
    print!(
        "{}",
        fmt::table(
            &[
                "Tool",
                "Window",
                "Start → reset",
                "Quota used",
                ">Tokens",
                ">API-equiv",
                "Notes"
            ],
            &rows
        )
    );
    let top = windows
        .iter()
        .filter(|window| window.observed)
        .max_by(|a, b| {
            let rise = |w: &&explore::QuotaWindowRow| {
                w.end_percent.unwrap_or(0.0) - w.start_percent.unwrap_or(0.0)
            };
            rise(a)
                .partial_cmp(&rise(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    if let Some(top) = top {
        println!(
            "\nLargest contributors to {} {} (allocated by token cost share, not causal):",
            top.provider, top.name
        );
        let contributors = explore::window_contributors(&conn, top)?;
        let rows: Vec<Vec<String>> = contributors
            .iter()
            .take(8)
            .map(|c| {
                vec![
                    format!("{}:{}", c.provider, short_id(&c.root_session_id)),
                    fmt::truncate(&c.title, 50),
                    c.delta_percent
                        .map(|d| format!("≈{d:.1}pp"))
                        .unwrap_or_default(),
                    fmt::tokens(c.tokens),
                    fmt::usd(c.cost),
                    if c.background_tokens > 0 {
                        format!("{} background", fmt::tokens(c.background_tokens))
                    } else {
                        String::new()
                    },
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Session",
                    "Title",
                    ">Δ quota",
                    ">Tokens",
                    ">API-equiv",
                    "Notes"
                ],
                &rows
            )
        );
    }
    print_footer(&filters);
    println!(
        "Observed % comes from provider logs (Codex). Other tools record no quota data: windows show token burn only."
    );
    println!(
        "Log a limit you hit with `vca quota mark claude --kind limit_hit` to compare future windows."
    );
    Ok(())
}

pub fn run_sessions(args: SessionsArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    let filters = filters_from_args(&args.filters);
    let sort = match args.sort.as_str() {
        "cost" => SessionSort::Cost,
        "tokens" => SessionSort::Tokens,
        "fresh" => SessionSort::FreshTokens,
        "cache" => SessionSort::CacheRatio,
        "started" | "recent" => SessionSort::Started,
        "turns" => SessionSort::Turns,
        "failures" => SessionSort::Failures,
        "agents" => SessionSort::Agents,
        other => bail!("unknown --sort {other}"),
    };
    if let Some(group) = args.group_by.as_deref() {
        let dim =
            GroupDim::parse(group).ok_or_else(|| anyhow::anyhow!("unknown --group-by {group}"))?;
        let groups = explore::groups(&conn, &filters, dim)?;
        if args.filters.json {
            return print_json(&groups);
        }
        let rows: Vec<Vec<String>> = groups
            .iter()
            .map(|g| {
                vec![
                    g.key.clone(),
                    g.sessions.to_string(),
                    g.tasks.to_string(),
                    fmt::usd(g.cost),
                    fmt::percent(Some(g.share)),
                    fmt::tokens(g.fresh_tokens),
                    fmt::usd(g.median_cost_per_task),
                    fmt::percent(g.completion_rate),
                    g.stalled.to_string(),
                    g.loops.to_string(),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    dim.label(),
                    ">Sessions",
                    ">Tasks",
                    ">API-equiv",
                    ">Share",
                    ">Fresh tok",
                    ">Med $/task",
                    ">Done",
                    ">Stalled",
                    ">Loops"
                ],
                &rows
            )
        );
        print_footer(&filters);
        return Ok(());
    }
    let rows = explore::sessions(&conn, &filters, sort, args.filters.limit)?;
    if args.filters.json {
        return print_json(&rows);
    }
    if rows.is_empty() {
        println!(
            "No sessions found. Run `vca ingest` first or widen the filters (--days 0 for all)."
        );
        return Ok(());
    }
    let table_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut flags = String::new();
            if row.legacy_usage {
                flags.push('!');
            }
            if row.source_status != "ok" {
                flags.push_str("src-missing");
            }
            vec![
                fmt::when(row.started_at.as_deref()),
                row.provider.clone(),
                short_id(&row.session_id),
                fmt::truncate(
                    &row.primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default(),
                    18,
                ),
                row.repo_root.as_deref().map(short_repo).unwrap_or_default(),
                fmt::truncate(&row.title, 44),
                fmt::usd(row.cost),
                fmt::tokens(row.fresh_tokens),
                row.agents.to_string(),
                row.tasks.to_string(),
                fmt::label(row.outcome.as_deref(), row.label_source.as_deref(), None),
                format!("{}/{}", row.completed_tasks, row.tasks),
                flags,
            ]
        })
        .collect();
    print!(
        "{}",
        fmt::table(
            &[
                "Started",
                "Tool",
                "Id",
                "Model",
                "Project",
                "Title / first ask",
                ">API-equiv",
                ">Fresh",
                ">Agt",
                ">Tasks",
                "Outcome",
                ">Done",
                ""
            ],
            &table_rows
        )
    );
    print_footer(&filters);
    println!(
        "Totals include subagents. h = heuristic label, ? = tentative, ◌ = unclassified, ! = legacy usage rows."
    );
    Ok(())
}

pub fn run_tasks(args: TasksArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    let mut filters = filters_from_args(&args.filters);
    let sort = match args.sort.as_str() {
        "cost" => TaskSort::Cost,
        "cost-to-complete" | "done-cost" => TaskSort::CostToComplete,
        "tokens" => TaskSort::Tokens,
        "time" | "active" => TaskSort::ActiveTime,
        "wall" => TaskSort::WallTime,
        "started" | "recent" => TaskSort::Started,
        "attempts" => TaskSort::Attempts,
        "failures" => TaskSort::Failures,
        other => bail!("unknown --sort {other}"),
    };
    if !args.group_by.is_empty() {
        let dims: Vec<GroupDim> = args
            .group_by
            .iter()
            .map(|value| {
                GroupDim::parse(value).ok_or_else(|| anyhow::anyhow!("unknown --group-by {value}"))
            })
            .collect::<Result<_>>()?;
        let mut all = explore::tasks(&conn, &filters, TaskSort::Started, 1_000_000)?;
        if args.completed_only {
            all.retain(|task| task.is_completed());
        }
        // Composite grouping: key = join of each dimension's key.
        let mut grouped: std::collections::BTreeMap<String, Vec<explore::TaskRow>> =
            Default::default();
        for task in &all {
            let key = dims
                .iter()
                .map(|dim| dim.key(task).0)
                .collect::<Vec<_>>()
                .join(" · ");
            grouped.entry(key).or_default().push(task.clone());
        }
        let total_cost: f64 = all.iter().filter_map(|task| task.cost_total).sum();
        let mut rows: Vec<(String, explore::GroupRow)> = grouped
            .into_iter()
            .map(|(key, tasks)| {
                let refs: Vec<&explore::TaskRow> = tasks.iter().collect();
                let row = explore::summarize_tasks(&refs, key.clone(), filters.clone(), total_cost);
                (key, row)
            })
            .collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.1.tasks));
        if args.filters.json {
            return print_json(&rows.iter().map(|(_, row)| row).collect::<Vec<_>>());
        }
        let header = dims
            .iter()
            .map(|dim| dim.label())
            .collect::<Vec<_>>()
            .join(" · ");
        let table_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|(key, g)| {
                vec![
                    key.clone(),
                    g.tasks.to_string(),
                    g.completed.to_string(),
                    fmt::percent(g.completion_rate),
                    fmt::usd(g.median_cost_to_complete),
                    fmt::minutes(g.median_active_to_complete),
                    fmt::usd(g.cost),
                    if g.tasks < 15 {
                        "low n".into()
                    } else {
                        String::new()
                    },
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    &header,
                    ">Tasks",
                    ">Done",
                    ">Done %",
                    ">Med cost→done",
                    ">Med active→done",
                    ">Total",
                    "Note"
                ],
                &table_rows
            )
        );
        print_footer(&filters);
        println!(
            "Compare like with like: add --intent and keep the size of work similar. Without Jev, each session is one task."
        );
        return Ok(());
    }
    filters.min_cost = filters.min_cost.or(args.filters.min_cost);
    let mut rows = explore::tasks(&conn, &filters, sort, args.filters.limit)?;
    if args.completed_only {
        rows.retain(|task| task.is_completed());
    }
    if args.filters.json {
        return print_json(&rows);
    }
    if rows.is_empty() {
        println!("No tasks found. Run `vca ingest` first or widen the filters (--days 0 for all).");
        return Ok(());
    }
    let table_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|task| {
            vec![
                fmt::when(task.started_at.as_deref()),
                format!(
                    "{}:{}#{}",
                    task.provider,
                    short_id(&task.session_id),
                    task.task_index
                ),
                fmt::truncate(&task.title, 40),
                fmt::truncate(
                    &task
                        .primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default(),
                    16,
                ),
                fmt::label(
                    task.intent.as_deref(),
                    task.intent_source.as_deref(),
                    task.intent_band.as_deref(),
                ),
                fmt::label(
                    task.outcome.as_deref(),
                    task.outcome_source.as_deref(),
                    task.outcome_band.as_deref(),
                ),
                fmt::minutes(task.wall_minutes),
                fmt::minutes(
                    task.active_minutes_to_complete
                        .or(task.active_minutes_total),
                ),
                fmt::usd(task.cost_to_complete.or(task.cost_total)),
                format!("{}", task.attempts),
                if task.loop_attempts > 0 {
                    format!("{} loop", task.loop_attempts)
                } else {
                    String::new()
                },
                task.completion_source.clone(),
            ]
        })
        .collect();
    print!(
        "{}",
        fmt::table(
            &[
                "Started",
                "Task",
                "First request",
                "Model",
                "Intent",
                "Outcome",
                ">Wall",
                ">Active",
                ">Cost→done",
                ">Att",
                "Loops",
                "Done by"
            ],
            &table_rows
        )
    );
    print_footer(&filters);
    println!(
        "Cost→done = spend until the completion point (total when not completed). Done by: commit, validation_pass, jev, override or none."
    );
    Ok(())
}

pub fn run_show(args: ShowArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    let (provider, session_id) = resolve_session(&conn, &args.session)?;
    let Some(detail) = explore::session_detail(&conn, &provider, &session_id)? else {
        bail!("session {} not found", args.session);
    };
    if args.json {
        return print_json(&detail);
    }
    let s = &detail.session;
    println!(
        "{} · {} · {}",
        s.provider,
        s.session_id,
        fmt::truncate(&s.title, 80)
    );
    println!(
        "{} · branch {} · {} → {} · {} turns · {} task(s) · {} subagent(s)",
        s.repo_root
            .as_deref()
            .map(short_repo)
            .unwrap_or_else(|| "(no repo)".into()),
        s.git_branch.as_deref().unwrap_or("—"),
        fmt::when(s.started_at.as_deref()),
        fmt::when(s.ended_at.as_deref()),
        s.turns,
        s.tasks,
        s.agents
    );
    println!(
        "API-equiv {} (subagents {}) · fresh {} · cache read {} · models {}",
        fmt::usd(s.cost),
        fmt::usd(s.subagent_cost),
        fmt::tokens(s.fresh_tokens),
        fmt::tokens(s.cache_read_tokens),
        s.models
            .split(',')
            .map(short_model)
            .collect::<Vec<_>>()
            .join(", ")
    );
    let all =
        !(args.tasks || args.agents || args.usage || args.context || args.delivery || args.turns);

    if all || args.tasks {
        println!("\nTasks");
        let rows: Vec<Vec<String>> = detail
            .tasks
            .iter()
            .map(|t| {
                vec![
                    format!("T{}", t.task_index),
                    fmt::truncate(&t.title, 44),
                    fmt::label(
                        t.intent.as_deref(),
                        t.intent_source.as_deref(),
                        t.intent_band.as_deref(),
                    ),
                    fmt::label(
                        t.outcome.as_deref(),
                        t.outcome_source.as_deref(),
                        t.outcome_band.as_deref(),
                    ),
                    fmt::when(t.started_at.as_deref()),
                    fmt::when(t.completed_at.as_deref()),
                    fmt::minutes(t.active_minutes_to_complete.or(t.active_minutes_total)),
                    fmt::usd(t.cost_to_complete.or(t.cost_total)),
                    fmt::usd(t.post_completion_cost),
                    format!("{} ({}✗)", t.validations, t.validation_failures),
                    t.completion_source.clone(),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Task",
                    "First request",
                    "Intent",
                    "Outcome",
                    "Started",
                    "Done",
                    ">Active",
                    ">Cost→done",
                    ">After",
                    ">Validations",
                    "Done by"
                ],
                &rows
            )
        );
        let loops: Vec<&explore::AttemptRow> =
            detail.attempts.iter().filter(|a| a.loop_flag).collect();
        if !loops.is_empty() {
            println!(
                "Unproductive loop: {} attempts failed with the same error and no new user input ({})",
                loops.len(),
                fmt::truncate(loops[0].error_signature.as_deref().unwrap_or(""), 60)
            );
        }
    }
    if all || args.agents {
        println!("\nAgents (self totals)");
        let rows: Vec<Vec<String>> = detail
            .agents
            .iter()
            .map(|a| {
                vec![
                    if a.session_id == s.session_id {
                        "● main".into()
                    } else {
                        format!("└ {}", a.session_id.rsplit('/').next().unwrap_or(""))
                    },
                    a.agent_type.clone().unwrap_or_else(|| a.agent_kind.clone()),
                    fmt::truncate(a.title.as_deref().unwrap_or(""), 36),
                    a.turns.to_string(),
                    fmt::tokens(a.fresh_tokens),
                    fmt::usd(a.cost),
                    if a.background_tokens > 0 {
                        format!("{} background", fmt::tokens(a.background_tokens))
                    } else {
                        String::new()
                    },
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Agent",
                    "Type",
                    "Description",
                    ">Turns",
                    ">Fresh",
                    ">API-equiv",
                    "Notes"
                ],
                &rows
            )
        );
    }
    if all || args.usage {
        println!("\nUsage by model");
        let rows: Vec<Vec<String>> = detail
            .models
            .iter()
            .map(|m| {
                vec![
                    short_model(&m.model),
                    m.calls.to_string(),
                    fmt::tokens(m.fresh_tokens),
                    fmt::tokens(m.cache_read_tokens),
                    fmt::tokens(m.output_tokens),
                    fmt::usd(m.cost),
                    format!(
                        "{}, pricing {}",
                        m.token_source,
                        m.price_match.as_deref().unwrap_or("none")
                    ),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Model",
                    ">Calls",
                    ">Fresh",
                    ">Cache read",
                    ">Output",
                    ">API-equiv",
                    "Grade"
                ],
                &rows
            )
        );
    }
    if all || args.context {
        let rebuilds: f64 = detail.calls.iter().filter_map(|c| c.rebuild_extra).sum();
        let peak = detail
            .calls
            .iter()
            .map(|c| c.context_tokens)
            .max()
            .unwrap_or(0);
        println!(
            "\nContext · peak {} tokens · {} calls · cache rebuilds {} ({} extra)",
            fmt::tokens(peak),
            detail.calls.len(),
            detail
                .calls
                .iter()
                .filter(|c| c.cache_event == "rebuild")
                .count(),
            fmt::usd(Some(rebuilds))
        );
        let rows: Vec<Vec<String>> = detail
            .context_kinds
            .iter()
            .take(10)
            .map(|k| {
                vec![
                    k.key.clone(),
                    k.items.to_string(),
                    fmt::tokens(k.tokens),
                    fmt::usd(k.total_usd),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(&["Kind", ">Items", ">Tokens", ">Write+carry"], &rows)
        );
        println!("Top items");
        let rows: Vec<Vec<String>> = detail
            .context_items
            .iter()
            .take(args.limit)
            .map(|i| {
                vec![
                    i.tool_name.clone().unwrap_or_else(|| i.kind.clone()),
                    fmt::truncate(i.target.as_deref().unwrap_or(""), 50),
                    fmt::tokens(i.tokens),
                    format!("c{}", i.entered_call),
                    i.carried_calls.to_string(),
                    fmt::usd(i.write_usd),
                    fmt::usd(i.carry_usd.map(|c| c + i.rebuild_usd.unwrap_or(0.0))),
                    fmt::usd(i.total_usd),
                    i.purpose.clone().unwrap_or_default(),
                    i.instruction_ref.clone().unwrap_or_default(),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Item", "Target", ">Tokens", ">In", ">Carried", ">Write", ">Carry", ">Total",
                    "Purpose", "Instr."
                ],
                &rows
            )
        );
    }
    if all || args.delivery {
        println!("\nCommits");
        if detail.commits.is_empty() {
            println!("  none matched (no committed AI-written lines from this session)");
        }
        let rows: Vec<Vec<String>> = detail
            .commits
            .iter()
            .map(|c| {
                vec![
                    c.sha.chars().take(8).collect(),
                    fmt::when(Some(&c.commit_time)),
                    fmt::truncate(&c.subject, 44),
                    format!("{:.0}%", c.ai_share * 100.0),
                    match c.merged_to_mainline {
                        Some(true) => "✓ mainline".into(),
                        Some(false) => "not on mainline".into(),
                        None => "·".into(),
                    },
                    c.pr_number
                        .map(|n| {
                            format!(
                                "#{n}{}",
                                if c.pr_merged == Some(true) {
                                    " ✓"
                                } else {
                                    ""
                                }
                            )
                        })
                        .unwrap_or_default(),
                    c.fix_signal
                        .clone()
                        .map(|s| format!("later fix signal ({s})"))
                        .unwrap_or_default(),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "Commit",
                    "Time",
                    "Subject",
                    ">AI share",
                    "Mainline",
                    "PR",
                    "Later"
                ],
                &rows
            )
        );
    }
    if args.turns {
        println!("\nTurns");
        let rows: Vec<Vec<String>> = detail
            .turns
            .iter()
            .map(|t| {
                vec![
                    t.turn_index.to_string(),
                    fmt::when(t.started_at.as_deref()),
                    t.task_index
                        .map(|task| format!("T{task}"))
                        .unwrap_or_default(),
                    fmt::truncate(&t.user_text, 56),
                    format!("{}/{}", t.edit_calls, t.tool_calls),
                    if t.validation_runs > 0 {
                        format!("{}({}✗)", t.validation_runs, t.validation_failures)
                    } else {
                        String::new()
                    },
                    fmt::tokens(t.tokens),
                    fmt::usd(Some(t.cost)),
                    fmt::label(t.feedback.as_deref(), t.feedback_source.as_deref(), None),
                ]
            })
            .collect();
        print!(
            "{}",
            fmt::table(
                &[
                    "#",
                    "Time",
                    "Task",
                    "User message",
                    ">Edits/tools",
                    ">Valid.",
                    ">Tokens",
                    ">API-equiv",
                    "Feedback"
                ],
                &rows
            )
        );
    }
    Ok(())
}

/// Accept `provider:id`, a full id, or a unique id prefix.
pub fn resolve_session(conn: &Connection, input: &str) -> Result<(String, String)> {
    let (provider, prefix) = match input.split_once(':') {
        Some((provider, id)) if !provider.contains('/') => {
            (Some(provider.to_string()), id.to_string())
        }
        _ => (None, input.to_string()),
    };
    let mut stmt = conn.prepare(
        "SELECT provider, session_id FROM metadata_sessions
         WHERE session_id LIKE ?1 || '%' AND (?2 IS NULL OR provider = ?2)
           AND COALESCE(NULLIF(root_session_id, ''), session_id) = session_id
         LIMIT 5",
    )?;
    let matches = stmt
        .query_map(rusqlite::params![prefix, provider], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<Vec<(String, String)>>>()?;
    match matches.len() {
        0 => bail!("no root session matches {input}"),
        1 => Ok(matches.into_iter().next().expect("one match")),
        _ => {
            if let Some(exact) = matches.iter().find(|(_, id)| *id == prefix) {
                return Ok(exact.clone());
            }
            bail!(
                "{input} is ambiguous: {}",
                matches
                    .iter()
                    .map(|(p, id)| format!("{p}:{id}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

pub fn run_context(args: ContextArgs, _context: AppContext) -> Result<()> {
    let conn = open()?;
    let mut filters = filters_from_args(&args.filters);
    if let Some(session) = args.session.as_deref() {
        filters.session = Some(resolve_session(&conn, session)?);
    }
    let dim = ContextDim::parse(&args.group_by)
        .ok_or_else(|| anyhow::anyhow!("unknown --group-by {}", args.group_by))?;
    let rows = explore::context_groups(&conn, &filters, dim)?;
    if args.filters.json {
        return print_json(&rows);
    }
    if rows.is_empty() {
        println!(
            "No context data. Context composition needs per-call token usage (Claude Code, Codex, OpenCode)."
        );
        return Ok(());
    }
    let table_rows: Vec<Vec<String>> = rows
        .iter()
        .take(args.filters.limit)
        .map(|r| {
            vec![
                fmt::truncate(&r.key, 56),
                r.items.to_string(),
                r.sessions.to_string(),
                fmt::tokens(r.tokens),
                fmt::usd(r.write_usd),
                fmt::usd(r.carry_usd),
                fmt::usd(r.rebuild_usd),
                fmt::usd(r.total_usd),
                fmt::usd(r.per_session_usd),
                fmt::percent(r.unused_share),
                format!("{:.0}", r.avg_carried_calls),
            ]
        })
        .collect();
    print!(
        "{}",
        fmt::table(
            &[
                dim.label(),
                ">Items",
                ">Sessions",
                ">Tokens",
                ">Write",
                ">Carry",
                ">Rebuild",
                ">Total",
                ">/session",
                ">Unused",
                ">Avg carried"
            ],
            &table_rows
        )
    );
    print_footer(&filters);
    println!(
        "Write = cost when the item entered context; Carry = cache reads on every later call; Rebuild = extra cost when the cache expired."
    );
    println!(
        "Token sizes are estimates calibrated to observed context growth. Instruction refs are path matches, not proof of cause."
    );
    Ok(())
}
