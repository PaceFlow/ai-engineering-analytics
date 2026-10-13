//! `x` explain panel: where the selected number or label comes from.

use super::app::{App, DetailTab, Screen, UsageTab, usage_rows};
use super::data::Data;
use crate::explore::{format as fmt, short_model};

const COST_NOTE: &str =
    "Costs are API list-price equivalents from the versioned pricing table, not invoices.";

pub fn explain(app: &App) -> Option<(String, Vec<String>)> {
    let frame = app.frame();
    let data = frame.data.as_ref()?;
    let i = frame.selected;
    let mut lines: Vec<String> = Vec::new();
    let title: String;
    match (&frame.screen, data) {
        (Screen::Overview, Data::Overview(o)) => {
            title = "Overview".into();
            lines.push(format!(
                "Tasks in range: {} · subagent sessions: {}",
                o.tasks_total, o.subagent_count
            ));
            lines.push(format!(
                "Labels: {} from Jev/overrides, {} heuristic (h). Without Jev every session is one task.",
                o.classified_jev, o.classified_heuristic
            ));
            lines.push(format!(
                "Corrections: {} of {} user replies read as corrections (feedback label).",
                o.corrections, o.exchanges
            ));
            if o.legacy_sessions > 0 {
                lines.push(format!(
                    "! {} sessions still have legacy usage rows whose source file is gone (possible duplicate counting).",
                    o.legacy_sessions
                ));
            }
            lines.push(COST_NOTE.into());
            lines.push(
                "Tab moves between panels; Enter opens the rows behind the selected line.".into(),
            );
        }
        (_, Data::Sessions(rows)) => {
            let row = rows.get(i)?;
            title = format!("Session {}:{}", row.provider, row.session_id);
            lines.push(row.title.clone());
            lines.push(String::new());
            lines.push(format!(
                "Models: {}",
                row.models
                    .split(',')
                    .map(short_model)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            lines.push(format!(
                "API-equiv {} (subagents {}) · reported {} · {} unpriced tokens · {} text-estimated tokens",
                fmt::usd(row.cost),
                fmt::usd(row.subagent_cost),
                fmt::usd_plain(row.reported_cost),
                fmt::tokens(row.unpriced_tokens),
                fmt::tokens(row.estimated_tokens)
            ));
            lines.push(format!(
                "Tokens: {} total, {} fresh (input + cache writes + output), {} cache reads ({:.0}× fresh)",
                fmt::tokens(row.tokens_total),
                fmt::tokens(row.fresh_tokens),
                fmt::tokens(row.cache_read_tokens),
                row.cache_read_tokens as f64 / row.fresh_tokens.max(1) as f64
            ));
            lines.push(format!(
                "{} task(s), {} completed · {} turns · {} attempts · {} failing validations · {} loop attempts",
                row.tasks, row.completed_tasks, row.turns, row.attempts, row.validation_failures, row.loop_attempts
            ));
            if row.legacy_usage {
                lines.push(
                    "! Legacy usage rows: the source was not re-read with corrected accounting."
                        .into(),
                );
            }
            if row.source_status != "ok" {
                lines.push(
                    "! Source file missing: stored facts are kept but cannot be refreshed.".into(),
                );
            }
            lines.push(COST_NOTE.into());
        }
        (_, Data::Tasks(rows)) => {
            let row = rows.get(i)?;
            title = format!(
                "Task {}:{}#{}",
                row.provider, row.session_id, row.task_index
            );
            lines.push(row.title.clone());
            lines.push(String::new());
            lines.push(format!(
                "Intent {} ({}) · outcome {} ({})",
                row.intent.as_deref().unwrap_or("◌"),
                row.intent_source.as_deref().unwrap_or("unclassified"),
                row.outcome.as_deref().unwrap_or("◌"),
                row.outcome_source.as_deref().unwrap_or("unclassified")
            ));
            lines.push(format!(
                "Segmentation: {} · completion: {}",
                row.segmentation_source, row.completion_source
            ));
            lines.push(match row.completion_source.as_str() {
                "commit" => {
                    "Done = first commit with this session's AI lines after the last edit.".into()
                }
                "validation_pass" => {
                    "Done = last passing test/build/lint after the last edit.".into()
                }
                "none" => {
                    "Not observed complete: wall/active time shown up to the last activity only."
                        .into()
                }
                other => format!("Done = {other}."),
            });
            lines.push(format!(
                "Wall {} · active {} (idle gaps >30m removed) · agent {} vs waiting on you {}",
                fmt::minutes(row.wall_minutes),
                fmt::minutes(row.active_minutes_to_complete.or(row.active_minutes_total)),
                fmt::minutes(row.agent_minutes),
                fmt::minutes(row.user_minutes)
            ));
            lines.push(format!(
                "Cost to complete {} · after completion {} · subagents {} · total {}",
                fmt::usd(row.cost_to_complete),
                fmt::usd(row.post_completion_cost),
                fmt::usd(row.subagent_cost),
                fmt::usd(row.cost_total)
            ));
            lines.push(format!(
                "{} attempts, {} validations ({} failed), {} in unproductive loops, {} model change(s)",
                row.attempts, row.validations, row.validation_failures, row.loop_attempts, row.model_changes
            ));
            lines.push(COST_NOTE.into());
        }
        (Screen::Groups { .. }, Data::Groups(rows)) => {
            let row = rows.get(i)?;
            title = format!("Group {}", row.key);
            lines.push(format!(
                "{} tasks in {} sessions · {} completed ({})",
                row.tasks,
                row.sessions,
                row.completed,
                fmt::percent(row.completion_rate)
            ));
            lines.push(format!(
                "Median cost per task {} · median cost to complete {} · median active time to complete {}",
                fmt::usd(row.median_cost_per_task),
                fmt::usd(row.median_cost_to_complete),
                fmt::minutes(row.median_active_to_complete)
            ));
            lines.push(format!(
                "{} unclassified · {} with heuristic labels · {} unpriced",
                row.unclassified, row.heuristic_labels, row.unpriced_tasks
            ));
            lines.push(
                "Every task belongs to exactly one group, so group costs add up to the total."
                    .into(),
            );
            lines.push("Compare groups with similar work: filter by intent first (f).".into());
        }
        (Screen::Detail { tab, .. }, Data::Detail(Some(detail))) => {
            match tab {
                DetailTab::Conversation => {
                    let turn = detail.turns.get(i)?;
                    title = format!("Turn {}", turn.turn_index);
                    lines.push(turn.user_text.clone());
                    lines.push(String::new());
                    lines.push(format!(
                        "{} tool calls ({} edits) · {} validations ({} failed) · {} tokens · {}",
                        turn.tool_calls,
                        turn.edit_calls,
                        turn.validation_runs,
                        turn.validation_failures,
                        fmt::tokens(turn.tokens),
                        fmt::usd(Some(turn.cost))
                    ));
                    lines.push(format!(
                        "Feedback label: {} ({})",
                        turn.feedback.as_deref().unwrap_or("◌"),
                        turn.feedback_source.as_deref().unwrap_or("unclassified")
                    ));
                }
                DetailTab::Tasks => {
                    if let Some(task) = detail.tasks.get(i) {
                        title = format!("Task {}", task.task_index);
                        lines.push(task.title.clone());
                        lines.push(format!(
                            "Completion: {} · {}",
                            task.completion_source,
                            fmt::when(task.completed_at.as_deref())
                        ));
                    } else {
                        let attempt = detail.attempts.get(i - detail.tasks.len())?;
                        title = format!("Attempt {}", attempt.attempt_index);
                        lines.push(format!(
                            "{} edit call(s), {} lines, ended by {}",
                            attempt.edit_calls, attempt.lines_changed, attempt.ended_by
                        ));
                        lines.push(format!(
                            "Validation: {} {}",
                            attempt.validation_kind.as_deref().unwrap_or("none"),
                            attempt.validation_result
                        ));
                        if let Some(signature) = &attempt.error_signature {
                            lines.push(format!("Error signature: {signature}"));
                        }
                        if attempt.loop_flag {
                            lines.push("LOOP: one of 3+ consecutive failures with this signature and no new user input.".into());
                        }
                    }
                }
                DetailTab::Agents => {
                    let agent = detail.agents.get(i)?;
                    title = format!("Agent {}", agent.session_id);
                    lines.push(format!(
                        "Kind {} · type {}",
                        agent.agent_kind,
                        agent.agent_type.as_deref().unwrap_or("—")
                    ));
                    lines.push(agent.title.clone().unwrap_or_default());
                    lines.push(format!(
                    "Self totals: {} tokens, {} fresh, {}; {} tokens used >10 minutes after your last message",
                    fmt::tokens(agent.tokens),
                    fmt::tokens(agent.fresh_tokens),
                    fmt::usd(agent.cost),
                    fmt::tokens(agent.background_tokens)
                ));
                }
                DetailTab::Usage => {
                    let model = detail.models.get(i)?;
                    title = format!("Model {}", model.model);
                    lines.push(format!(
                        "{} calls · fresh {} · cache read {} · output {} · {}",
                        model.calls,
                        fmt::tokens(model.fresh_tokens),
                        fmt::tokens(model.cache_read_tokens),
                        fmt::tokens(model.output_tokens),
                        fmt::usd(model.cost)
                    ));
                    lines.push(format!(
                        "Token source {} · price match {} · pricing {}",
                        model.token_source,
                        model.price_match.as_deref().unwrap_or("none (unpriced)"),
                        crate::cost::pricing::pricing_version()
                    ));
                    lines.push(COST_NOTE.into());
                }
                DetailTab::Context => {
                    let item = detail.context_items.get(i)?;
                    title = format!("Context item {}", item.item_key);
                    lines.push(format!(
                        "{} · {}",
                        item.kind,
                        item.target.clone().unwrap_or_default()
                    ));
                    lines.push(format!(
                        "~{} tokens entered at call {} and were carried through {} later calls",
                        fmt::tokens(item.tokens),
                        item.entered_call,
                        item.carried_calls
                    ));
                    lines.push(format!(
                        "Write {} + carry {} + rebuild {} = {}",
                        fmt::usd(item.write_usd),
                        fmt::usd(item.carry_usd),
                        fmt::usd(item.rebuild_usd),
                        fmt::usd(item.total_usd)
                    ));
                    lines.push(format!(
                        "Purpose: {} · instruction reference: {}",
                        item.purpose.as_deref().unwrap_or("n/a"),
                        item.instruction_ref.as_deref().unwrap_or("none")
                    ));
                    lines.push("Size is estimated from characters and calibrated to observed context growth.".into());
                    lines.push("Instruction references are path matches with AGENTS.md / CLAUDE.md lines, not proof of cause.".into());
                }
                DetailTab::Delivery => {
                    let commit = detail.commits.get(i)?;
                    title = format!("Commit {}", commit.sha.chars().take(10).collect::<String>());
                    lines.push(commit.subject.clone());
                    lines.push(format!(
                    "AI share {:.0}% · matched lines {:.0} · mainline {:?} · PR {:?} merged {:?}",
                    commit.ai_share * 100.0,
                    commit.matched_lines,
                    commit.merged_to_mainline,
                    commit.pr_number,
                    commit.pr_merged
                ));
                    if let Some(signal) = &commit.fix_signal {
                        lines.push(format!(
                        "Later fix signal ({signal}): a later fix-like commit or bug issue touched these AI-written lines. Evidence, not proof of a defect."
                    ));
                    }
                }
            }
        }
        (
            Screen::Usage {
                tab: UsageTab::Timeline,
                ..
            },
            Data::Usage(buckets, spikes),
        ) => {
            let rows = usage_rows(buckets);
            let (key, items) = rows.get(i)?;
            title = format!("Usage {key}");
            for item in items {
                lines.push(format!(
                    "{:<9} {} · fresh {} · {} sessions · {} unattended tokens",
                    item.provider,
                    fmt::usd(Some(item.cost)),
                    fmt::tokens(item.fresh_tokens),
                    item.sessions,
                    fmt::tokens(item.background_tokens)
                ));
            }
            if spikes
                .iter()
                .any(|spike| spike.bucket.starts_with(key.as_str()))
            {
                lines.push("▲ contains a spike hour (>3× median and ≥ p95 hourly burn).".into());
            }
            lines.push("Enter lists the sessions active that day.".into());
        }
        (_, Data::Windows(rows)) => {
            let row = rows.get(i)?;
            title = format!("{} {}", row.provider, row.name);
            if row.observed {
                lines.push(
                    "Observed: percentages come from the provider's own rate-limit records.".into(),
                );
                lines.push(format!(
                    "{} → {} · unobserved gaps {} min",
                    row.start_percent
                        .map(|v| format!("{v:.0}%"))
                        .unwrap_or_default(),
                    row.end_percent
                        .map(|v| format!("{v:.0}%"))
                        .unwrap_or_default(),
                    row.unobserved_minutes
                ));
            } else {
                lines.push("Estimated: this tool records no quota data. The window shape comes from billing.json defaults.".into());
                lines.push("Token burn is shown; a percentage of your limit is unknown.".into());
                lines.push(format!(
                    "Limit hits you logged in this window: {} (M logs one now)",
                    row.limit_hits_logged
                ));
            }
            lines.push("Enter shows the sessions that used this window.".into());
        }
        (_, Data::Contributors(rows)) => {
            let row = rows.get(i)?;
            title = format!("{}:{}", row.provider, row.root_session_id);
            lines.push(row.title.clone());
            lines.push(format!(
                "Δ quota {} · {} tokens · {} · {} unattended · {} from subagents",
                row.delta_percent
                    .map(|d| format!("≈{d:.1} pp"))
                    .unwrap_or_else(|| "n/a".into()),
                fmt::tokens(row.tokens),
                fmt::usd(row.cost),
                fmt::tokens(row.background_tokens),
                fmt::tokens(row.subagent_tokens)
            ));
            lines.push("Each rise in the percentage is split over the usage since the previous rise, by cost share.".into());
            lines.push("Concurrent sessions share the window: this is an allocation, not a measurement of cause.".into());
        }
        (_, Data::ContextGroups(rows)) => {
            let row = rows.get(i)?;
            title = format!("Context: {}", row.key);
            lines.push(format!(
                "{} items in {} sessions · ~{} tokens",
                row.items,
                row.sessions,
                fmt::tokens(row.tokens)
            ));
            lines.push(format!(
                "Write {} (entering) + carry {} (cache reads on every later call) + rebuild {} = {}",
                fmt::usd(row.write_usd),
                fmt::usd(row.carry_usd),
                fmt::usd(row.rebuild_usd),
                fmt::usd(row.total_usd)
            ));
            lines.push(format!(
                "Unused after reading: {} · average carried calls {:.0}",
                fmt::percent(row.unused_share),
                row.avg_carried_calls
            ));
            lines.push(
                "baseline = system prompt and tool schemas not logged in the transcript.".into(),
            );
            lines.push("unexplained = growth the logged items do not account for.".into());
        }
        (_, Data::ContextItems(rows)) => {
            let row = rows.get(i)?;
            title = format!("Item {}", row.item_key);
            lines.push(row.target.clone().unwrap_or_else(|| row.kind.clone()));
            lines.push(format!(
                "~{} tokens · entered call {} · carried {}",
                fmt::tokens(row.tokens),
                row.entered_call,
                row.carried_calls
            ));
            lines.push(format!(
                "Total {} · purpose {}",
                fmt::usd(row.total_usd),
                row.purpose.as_deref().unwrap_or("n/a")
            ));
            lines.push("Enter opens the session's Context tab.".into());
        }
        (_, Data::Ladder(rows, _)) => {
            let row = rows.get(i)?;
            title = format!("Delivery ladder: {}", row.intent);
            lines.push(
                "Edited: at least one edit · Validated: a passing test/build/lint run".into(),
            );
            lines.push("Committed: AI-written lines found in a commit · Mainline: that commit reached the default branch".into());
            lines.push("Later fix: a later fix-like commit or bug issue touched those lines (evidence, not causation)".into());
            if !row.code_intent {
                lines.push(
                    "Non-code intent: judge it by outcome (answered/decided), not by commits."
                        .into(),
                );
            }
            lines.push(format!(
                "{:.0}% of these intent labels are heuristic.",
                row.heuristic_share * 100.0
            ));
        }
        (_, Data::Practices(rows)) => {
            let row = rows.get(i)?;
            title = row.name.to_string();
            lines.push(row.definition.to_string());
            lines.push(String::new());
            lines.push(row.finding.clone());
            lines.push(format!(
                "Sample: with {} · without {} ({})",
                row.with_n, row.without_n, row.confidence
            ));
            lines.push(String::new());
            lines.push(format!("Practice: {}", row.advice));
            lines.push("Correlation, not causation. Enter opens the matching tasks; o opens the comparison group.".into());
        }
        _ => return None,
    }
    let _ = app;
    Some((title, lines))
}
