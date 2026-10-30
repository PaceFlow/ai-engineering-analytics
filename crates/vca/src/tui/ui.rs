//! Rendering. Pure function of [`App`]; no data access.

use ratatui::Frame as TermFrame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Paragraph, Row, Sparkline, Table, TableState, Wrap,
};

use super::app::{
    App, DetailTab, FILTER_FIELDS, Popup, Screen, TIME_PRESETS, Unit, UsageTab, usage_rows,
};
use super::data::{Data, OverviewData};
use crate::classify::state::Level;
use crate::explore::{
    ContextDim, GroupDim, SessionDetail, format as fmt, short_id, short_model, short_repo,
};

const CY: Color = Color::Rgb(0x22, 0xd3, 0xee);
const GN: Color = Color::Rgb(0x4a, 0xde, 0x80);
const RD: Color = Color::Rgb(0xf8, 0x71, 0x71);
const AM: Color = Color::Rgb(0xfb, 0xbf, 0x24);
const BR: Color = Color::Rgb(0xf4, 0xf4, 0xf5);
const SU: Color = Color::Rgb(0xa1, 0xa1, 0xaa);
const MU: Color = Color::Rgb(0x71, 0x71, 0x7a);
const D4: Color = Color::Rgb(0x46, 0x46, 0x4f);
const SEL_BG: Color = Color::Rgb(0x0e, 0x2a, 0x30);

fn dim() -> Style {
    Style::default().fg(MU)
}

fn head() -> Style {
    Style::default().fg(SU).add_modifier(Modifier::BOLD)
}

fn right(text: impl Into<String>) -> Cell<'static> {
    Cell::from(Line::from(text.into()).alignment(Alignment::Right))
}

fn left(text: impl Into<String>) -> Cell<'static> {
    Cell::from(text.into())
}

fn colored(text: impl Into<String>, color: Color) -> Cell<'static> {
    Cell::from(Span::styled(text.into(), Style::default().fg(color)))
}

fn outcome_color(outcome: Option<&str>) -> Color {
    match outcome {
        Some("completed_verified") | Some("answered") => GN,
        Some("stalled") | Some("abandoned_redirected") => RD,
        Some("partial") | Some("completed_unverified") | Some("blocked_external") => AM,
        _ => SU,
    }
}

pub fn draw(frame: &mut TermFrame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(area);
    render_header(frame, app, chunks[0]);
    render_breadcrumbs(frame, app, chunks[1]);
    render_body(frame, app, chunks[2]);
    render_footer(frame, app, chunks[3]);
    if let Some(popup) = &app.popup {
        render_popup(frame, app, popup, area);
    }
}

fn render_header(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let time = TIME_PRESETS
        .get(app.time_preset)
        .map(|(_, label)| *label)
        .unwrap_or("custom");
    let left_text = Line::from(vec![
        Span::styled(
            app.title,
            Style::default().fg(CY).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" · {}", app.frame().screen.title()),
            Style::default().fg(BR),
        ),
    ]);
    let right_text = Line::from(Span::styled(
        format!(
            "{time} · $ = API list-price equiv. · {}",
            chrono::Local::now().format("%H:%M")
        ),
        dim(),
    ))
    .alignment(Alignment::Right);
    frame.render_widget(Paragraph::new(left_text), area);
    frame.render_widget(Paragraph::new(right_text), area);
}

fn render_breadcrumbs(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let mut spans = vec![Span::styled(app.breadcrumbs(), Style::default().fg(SU))];
    let chips = current.filters.chips();
    if !chips.is_empty() {
        spans.push(Span::styled(
            format!("   [{}]", chips.join(" · ")),
            Style::default().fg(CY),
        ));
    }
    if let Some(sort) = app.sort_label() {
        spans.push(Span::styled(format!("   sort: {sort} ↓"), dim()));
    }
    if !app.marked.is_empty() {
        spans.push(Span::styled(
            format!("   {} marked", app.marked.len()),
            Style::default().fg(AM),
        ));
    }
    if current.loading {
        spans.push(Span::styled("   ⟳ loading", Style::default().fg(AM)));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_footer(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let line = if let Some(message) = &app.message {
        Line::from(Span::styled(message.clone(), Style::default().fg(AM)))
    } else if let Some(error) = &app.frame().error {
        Line::from(Span::styled(
            format!("error: {error} · R retry"),
            Style::default().fg(RD),
        ))
    } else {
        let hints = match &app.frame().screen {
            Screen::Trends { .. } => {
                "Tab view · Enter contributors/compare milestone · n new milestone · f filter · Esc back"
            }
            Screen::WorkItems { .. } => "Enter open · L relationships · Esc back",
            Screen::WorkDetail { .. } => {
                "Enter conversation · L relationships · l link · d detach member · x evidence · Esc back"
            }
            Screen::WorkLinks { .. } => "a accept · d reject · l link · x evidence · Esc back",
            Screen::Overview => {
                "Tab panel · Enter open · 1-9 screens · t time · f filter · x explain · ? help · q quit"
            }
            Screen::List { .. } => {
                "Enter open · u sessions/tasks/work items · s sort · m mark · c compare · / search · f filter · Esc back"
            }
            Screen::Groups { .. } => {
                "g/G dimension · Enter open group · f filter · x explain · Esc back"
            }
            Screen::Detail { .. } => {
                "Tab section · [ ] prev/next · w skill window · x explain · m mark · Esc back"
            }
            Screen::Usage {
                tab: UsageTab::Timeline,
                ..
            } => "Tab section · b day/hour · Enter sessions that day · x explain",
            Screen::Usage {
                tab: UsageTab::Windows,
                ..
            } => "Tab section · Enter contributors · M log limit hit now · x explain",
            Screen::Usage {
                tab: UsageTab::Context,
                ..
            } => "Tab section · g group by · Enter items · x explain",
            Screen::Window { .. } => {
                "Enter open session · [ ] next/prev · M log limit hit · Esc back"
            }
            Screen::ContextItems { .. } => "Enter open session context · x explain · Esc back",
            Screen::Skills => "Skill calls per session · x explain · Esc back",
            Screen::Delivery => "Enter tasks behind a row · x stage definitions · Esc back",
            Screen::Practices => "Enter tasks with · o tasks without · x definition · Esc back",
            Screen::Compare { .. } => "Esc back · m in lists to change the selection",
        };
        Line::from(Span::styled(hints, dim()))
    };
    frame.render_widget(Paragraph::new(line), area);
}

fn render_body(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(data) = current.data.as_ref() else {
        let text = if current.loading {
            "loading…"
        } else {
            "no data"
        };
        frame.render_widget(Paragraph::new(Span::styled(text, dim())), area);
        return;
    };
    match (&current.screen, data) {
        (Screen::Trends { .. }, Data::Trends(_))
        | (Screen::Trends { .. }, Data::Daily(_))
        | (_, Data::WorkItems(_))
        | (_, Data::WorkDetail(_))
        | (_, Data::WorkLinks(_)) => super::trend_ui::render(frame, app, area),
        (Screen::Overview, Data::Overview(o)) => render_overview(frame, app, o, area),
        (
            Screen::List {
                unit: Unit::Sessions,
                ..
            },
            Data::Sessions(_),
        ) => render_sessions(frame, app, area),
        (
            Screen::List {
                unit: Unit::Tasks, ..
            },
            Data::Tasks(_),
        ) => render_tasks(frame, app, area),
        (Screen::Groups { dim }, Data::Groups(_)) => render_groups(frame, app, *dim, area),
        (Screen::Detail { tab, .. }, Data::Detail(Some(detail))) => {
            render_detail(frame, app, detail, *tab, area)
        }
        (Screen::Detail { .. }, Data::Detail(None)) => frame.render_widget(
            Paragraph::new("Session not found (it may be a subagent: open its parent)"),
            area,
        ),
        (Screen::Usage { tab, ctx_dim, .. }, _) => {
            render_usage(frame, app, *tab, *ctx_dim, data, area)
        }
        (Screen::Window { window }, Data::Contributors(_)) => {
            render_window(frame, app, window, area)
        }
        (Screen::ContextItems { .. }, Data::ContextItems(_)) => {
            render_context_items(frame, app, area)
        }
        (Screen::Skills, Data::Skills(_)) => render_skill_frequency(frame, app, area),
        (Screen::Delivery, Data::Ladder(..)) => render_ladder(frame, app, area),
        (Screen::Practices, Data::Practices(_)) => render_practices(frame, app, area),
        (Screen::Compare { .. }, Data::Compare(_)) => render_compare(frame, app, area),
        _ => frame.render_widget(Paragraph::new(Span::styled("loading…", dim())), area),
    }
}

/// Table with selection and a persisted scroll offset.
pub(super) fn render_table(
    frame: &mut TermFrame<'_>,
    app: &App,
    area: Rect,
    headers: &[&str],
    widths: &[Constraint],
    rows: Vec<Row<'static>>,
    selected: Option<usize>,
) {
    let header = Row::new(
        headers
            .iter()
            .map(|h| {
                if let Some(stripped) = h.strip_prefix('>') {
                    Cell::from(Line::from(stripped.to_string()).alignment(Alignment::Right))
                        .style(head())
                } else {
                    Cell::from(h.to_string()).style(head())
                }
            })
            .collect::<Vec<_>>(),
    );
    let table = Table::new(rows, widths.to_vec())
        .header(header)
        .column_spacing(2)
        .row_highlight_style(
            Style::default()
                .bg(SEL_BG)
                .fg(BR)
                .add_modifier(Modifier::BOLD),
        );
    let current = app.frame();
    let mut state = TableState::default()
        .with_offset(current.offset.get())
        .with_selected(selected);
    frame.render_stateful_widget(table, area, &mut state);
    current.offset.set(state.offset());
}

fn panel_block(title: &str, focused: bool) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { CY } else { D4 }))
        .title(Span::styled(
            format!(" {title} "),
            Style::default().fg(if focused { CY } else { SU }),
        ))
}

fn render_overview(frame: &mut TermFrame<'_>, app: &App, o: &OverviewData, area: Rect) {
    let current = app.frame();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Min(6),
        ])
        .split(area);
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[0]);
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[1]);
    let sel = |panel: usize| (current.panel == panel).then_some(current.selected);

    // Spend & tokens.
    let total_cost: f64 = o.by_tool.iter().filter_map(|row| row.cost).sum();
    let total_fresh: i64 = o.by_tool.iter().map(|row| row.fresh_tokens).sum();
    let total_read: i64 = o.by_tool.iter().map(|row| row.cache_read_tokens).sum();
    let block = panel_block("Spend & tokens", current.panel == 0);
    let inner = block.inner(top[0]);
    frame.render_widget(block, top[0]);
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Min(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    format!("API-equiv {}", fmt::usd(Some(total_cost))),
                    Style::default().fg(BR).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        "   reported {}",
                        fmt::usd_plain(
                            o.by_tool
                                .iter()
                                .filter_map(|r| r.reported_cost)
                                .reduce(|a, b| a + b)
                        )
                    ),
                    dim(),
                ),
            ]),
            Line::from(Span::styled(
                format!(
                    "fresh {} · cache read {} · {} tasks · {} subagents{}",
                    fmt::tokens(total_fresh),
                    fmt::tokens(total_read),
                    o.tasks_total,
                    o.subagent_count,
                    if o.legacy_sessions > 0 {
                        format!(" · {} ! legacy", o.legacy_sessions)
                    } else {
                        String::new()
                    }
                ),
                dim(),
            )),
        ]),
        parts[0],
    );
    let mut days: Vec<(String, f64)> = Vec::new();
    for bucket in &o.daily {
        match days.last_mut() {
            Some((key, cost)) if *key == bucket.bucket => *cost += bucket.cost,
            _ => days.push((bucket.bucket.clone(), bucket.cost)),
        }
    }
    let spark: Vec<u64> = days.iter().map(|(_, cost)| (cost * 100.0) as u64).collect();
    frame.render_widget(
        Sparkline::default()
            .block(Block::default().title(Span::styled("daily api-equiv", dim())))
            .data(&spark)
            .style(Style::default().fg(CY)),
        parts[1],
    );
    let tool_rows: Vec<Row> = o
        .by_tool
        .iter()
        .map(|row| {
            Row::new(vec![
                left(row.key.clone()),
                right(fmt::usd(row.cost)),
                right(fmt::tokens(row.fresh_tokens)),
                right(row.sessions.to_string()),
                left(
                    if row.estimated_tokens > 0 {
                        "~est. tokens"
                    } else if row.unpriced_tokens > 0 {
                        "unpriced"
                    } else {
                        ""
                    }
                    .to_string(),
                ),
            ])
        })
        .collect();
    render_mini_table(
        frame,
        parts[2],
        &["Tool", ">API-eq", ">Fresh", ">Sess", ""],
        &[
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(5),
            Constraint::Min(4),
        ],
        tool_rows,
        sel(0),
    );

    // Quota.
    let block = panel_block("Quota windows (observed / estimated)", current.panel == 1);
    let inner = block.inner(top[1]);
    frame.render_widget(block, top[1]);
    let window_rows: Vec<Row> = o
        .windows
        .iter()
        .map(|w| {
            let used = match (w.start_percent, w.end_percent) {
                (Some(start), Some(end)) => format!(
                    "{start:.0}%→{end:.0}%{}",
                    if w.reached_limit_at.is_some() {
                        " ◆"
                    } else {
                        ""
                    }
                ),
                _ => "no % data".to_string(),
            };
            Row::new(vec![
                left(w.provider.clone()),
                left(fmt::truncate(&w.name, 18)),
                left(fmt::when(w.window_start.as_deref())),
                colored(used, if w.observed { BR } else { MU }),
                right(fmt::tokens(if w.fresh_tokens > 0 {
                    w.fresh_tokens
                } else {
                    w.tokens
                })),
            ])
        })
        .collect();
    render_mini_table(
        frame,
        inner,
        &["Tool", "Window", "Start", "Used", ">Tokens"],
        &[
            Constraint::Length(8),
            Constraint::Length(18),
            Constraint::Length(11),
            Constraint::Length(12),
            Constraint::Min(6),
        ],
        window_rows,
        sel(1),
    );

    // Intent.
    let block = panel_block("Where it went (intent, tasks)", current.panel == 2);
    let inner = block.inner(middle[0]);
    frame.render_widget(block, middle[0]);
    let intent_rows: Vec<Row> = o
        .intents
        .iter()
        .map(|g| {
            let bar = "█".repeat(((g.share * 20.0).round() as usize).min(20));
            Row::new(vec![
                left(fmt::truncate(&g.key, 18)),
                right(fmt::usd(g.cost)),
                right(fmt::percent(Some(g.share))),
                colored(bar, CY),
                left(
                    if g.heuristic_labels * 2 > g.tasks {
                        "h"
                    } else {
                        ""
                    }
                    .to_string(),
                ),
            ])
        })
        .collect();
    render_mini_table(
        frame,
        inner,
        &["Intent", ">API-eq", ">Share", "", ""],
        &[
            Constraint::Length(18),
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(20),
            Constraint::Min(1),
        ],
        intent_rows,
        sel(2),
    );

    // Outcomes.
    let block = panel_block("Conversation health (outcome)", current.panel == 3);
    let inner = block.inner(middle[1]);
    frame.render_widget(block, middle[1]);
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(2), Constraint::Length(2)])
        .split(inner);
    let outcome_rows: Vec<Row> = o
        .outcomes
        .iter()
        .map(|g| {
            Row::new(vec![
                colored(
                    fmt::truncate(&g.key, 22),
                    outcome_color(Some(g.key.as_str())),
                ),
                right(g.tasks.to_string()),
                right(fmt::usd(g.cost)),
            ])
        })
        .collect();
    render_mini_table(
        frame,
        parts[0],
        &["Outcome", ">Tasks", ">API-eq"],
        &[
            Constraint::Length(22),
            Constraint::Length(6),
            Constraint::Min(8),
        ],
        outcome_rows,
        sel(3),
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                format!(
                    "loops: {} tasks ({}) · cache rebuilds {}",
                    o.loop_tasks,
                    fmt::usd(Some(o.loops_cost)),
                    fmt::usd(Some(o.rebuild_cost))
                ),
                dim(),
            )),
            Line::from(Span::styled(
                format!(
                    "corrections: {} of {} replies · labels: {} jev, {} heuristic",
                    o.corrections, o.exchanges, o.classified_jev, o.classified_heuristic
                ),
                dim(),
            )),
        ]),
        parts[1],
    );

    // Top sessions.
    let block = panel_block("Top sessions by API-equiv", current.panel == 4);
    let inner = block.inner(rows[2]);
    frame.render_widget(block, rows[2]);
    let session_rows: Vec<Row> = o
        .top_sessions
        .iter()
        .map(|s| {
            Row::new(vec![
                right(fmt::usd(s.cost)),
                left(s.provider.clone()),
                left(fmt::truncate(
                    &s.primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default(),
                    16,
                )),
                left(s.repo_root.as_deref().map(short_repo).unwrap_or_default()),
                left(fmt::truncate(&s.title, 60)),
                colored(
                    fmt::label(s.outcome.as_deref(), s.label_source.as_deref(), None),
                    outcome_color(s.outcome.as_deref()),
                ),
                right(format!("{} agt", s.agents)),
            ])
        })
        .collect();
    render_mini_table(
        frame,
        inner,
        &[
            ">API-eq", "Tool", "Model", "Project", "Title", "Outcome", ">Agents",
        ],
        &[
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(14),
            Constraint::Min(20),
            Constraint::Length(22),
            Constraint::Length(6),
        ],
        session_rows,
        sel(4),
    );
}

fn render_mini_table(
    frame: &mut TermFrame<'_>,
    area: Rect,
    headers: &[&str],
    widths: &[Constraint],
    rows: Vec<Row<'static>>,
    selected: Option<usize>,
) {
    let header = Row::new(
        headers
            .iter()
            .map(|h| match h.strip_prefix('>') {
                Some(stripped) => {
                    Cell::from(Line::from(stripped.to_string()).alignment(Alignment::Right))
                        .style(head())
                }
                None => Cell::from(h.to_string()).style(head()),
            })
            .collect::<Vec<_>>(),
    );
    let table = Table::new(rows, widths.to_vec())
        .header(header)
        .column_spacing(1)
        .row_highlight_style(
            Style::default()
                .bg(SEL_BG)
                .fg(BR)
                .add_modifier(Modifier::BOLD),
        );
    let mut state = TableState::default().with_selected(selected);
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_sessions(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::Sessions(rows)) = current.data.as_ref() else {
        return;
    };
    let total: f64 = rows.iter().filter_map(|r| r.cost).sum();
    let parts = split_footer(area);
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|s| {
            let marked = if app.is_marked(&s.provider, &s.session_id, 0) {
                "•"
            } else {
                " "
            };
            let mut flags = String::new();
            if s.legacy_usage {
                flags.push('!');
            }
            if s.estimated_tokens > 0 {
                flags.push('~');
            }
            Row::new(vec![
                colored(marked, AM),
                left(fmt::when(s.started_at.as_deref())),
                left(s.provider.clone()),
                left(fmt::truncate(
                    &s.primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default(),
                    16,
                )),
                left(s.repo_root.as_deref().map(short_repo).unwrap_or_default()),
                left(fmt::truncate(&s.title, 60)),
                right(fmt::usd(s.cost)),
                right(fmt::tokens(s.fresh_tokens)),
                right(s.agents.to_string()),
                right(s.tasks.to_string()),
                colored(
                    fmt::label(s.outcome.as_deref(), s.label_source.as_deref(), None),
                    outcome_color(s.outcome.as_deref()),
                ),
                left(if s.commits > 0 {
                    format!("{} commit", s.commits)
                } else {
                    "·".into()
                }),
                colored(flags, AM),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[0],
        &[
            "",
            "Started",
            "Tool",
            "Model",
            "Project",
            "Title / first ask",
            ">API-eq",
            ">Fresh",
            ">Agt",
            ">Tsk",
            "Outcome",
            "Delivery",
            "",
        ],
        &[
            Constraint::Length(1),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(14),
            Constraint::Min(20),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(22),
            Constraint::Length(9),
            Constraint::Length(2),
        ],
        table_rows,
        Some(current.selected),
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!(
                "{} sessions · Σ {} · totals include subagents · h heuristic ? tentative ◌ unclassified ! legacy ~ estimated",
                rows.len(),
                fmt::usd(Some(total))
            ),
            dim(),
        )),
        parts[1],
    );
}

fn render_tasks(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::Tasks(rows)) = current.data.as_ref() else {
        return;
    };
    let parts = split_footer(area);
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|t| {
            let marked = if app.is_marked(&t.provider, &t.session_id, t.task_index) {
                "•"
            } else {
                " "
            };
            let done = if t.supported_completed_at.is_some() {
                t.completion_source.clone()
            } else {
                "not observed".into()
            };
            Row::new(vec![
                colored(marked, AM),
                left(fmt::when(t.started_at.as_deref())),
                left(format!("{}#{}", short_id(&t.session_id), t.task_index)),
                left(t.provider.clone()),
                left(fmt::truncate(
                    &t.primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default(),
                    16,
                )),
                left(fmt::truncate(&t.title, 50)),
                left(fmt::label(
                    t.intent.as_deref(),
                    t.intent_source.as_deref(),
                    t.intent_band.as_deref(),
                )),
                colored(
                    fmt::label(
                        t.outcome.as_deref(),
                        t.outcome_source.as_deref(),
                        t.outcome_band.as_deref(),
                    ),
                    outcome_color(t.outcome.as_deref()),
                ),
                right(fmt::minutes(t.wall_minutes)),
                right(fmt::minutes(
                    t.active_minutes_to_complete.or(t.active_minutes_total),
                )),
                right(fmt::usd(t.cost_to_complete.or(t.cost_total))),
                right(if t.loop_attempts > 0 {
                    format!("{}↻", t.loop_attempts)
                } else {
                    t.attempts.to_string()
                }),
                colored(
                    done,
                    if t.supported_completed_at.is_some() {
                        SU
                    } else {
                        MU
                    },
                ),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[0],
        &[
            "",
            "Started",
            "Task",
            "Tool",
            "Model",
            "First request",
            "Intent",
            "Outcome",
            ">Wall",
            ">Active",
            ">Cost→done",
            ">Att",
            "Done by",
        ],
        &[
            Constraint::Length(1),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Min(18),
            Constraint::Length(19),
            Constraint::Length(22),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(4),
            Constraint::Length(15),
        ],
        table_rows,
        Some(current.selected),
    );
    let completed = rows
        .iter()
        .filter(|t| t.supported_completed_at.is_some())
        .count();
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!(
                "{} tasks · {} with observed completion · cost→done stops at the completion point · ↻ loop attempts · without Jev each session is one task",
                rows.len(),
                completed
            ),
            dim(),
        )),
        parts[1],
    );
}

fn split_footer(area: Rect) -> std::rc::Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area)
}

fn tab_bar<T: Copy + PartialEq>(
    tabs: &[T],
    current: T,
    label: impl Fn(T) -> &'static str,
) -> Line<'static> {
    let mut spans = Vec::new();
    for tab in tabs {
        let text = format!(" {} ", label(*tab));
        if *tab == current {
            spans.push(Span::styled(text, Style::default().fg(Color::Black).bg(CY)));
        } else {
            spans.push(Span::styled(text, Style::default().fg(SU)));
        }
        spans.push(Span::raw(" "));
    }
    Line::from(spans)
}

fn render_groups(frame: &mut TermFrame<'_>, app: &App, dim_index: usize, area: Rect) {
    let current = app.frame();
    let Some(Data::Groups(rows)) = current.data.as_ref() else {
        return;
    };
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
    let selected_dim = GroupDim::ALL[dim_index % GroupDim::ALL.len()];
    frame.render_widget(
        Paragraph::new(tab_bar(&GroupDim::ALL, selected_dim, |d| d.label())),
        parts[0],
    );
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|g| {
            Row::new(vec![
                left(fmt::truncate(&g.key, 30)),
                right(g.sessions.to_string()),
                right(g.tasks.to_string()),
                right(fmt::usd(g.cost)),
                right(fmt::percent(Some(g.share))),
                colored("█".repeat(((g.share * 12.0).round() as usize).min(12)), CY),
                right(fmt::tokens(g.fresh_tokens)),
                right(fmt::usd(g.median_cost_per_task)),
                right(fmt::percent(g.completion_rate)),
                right(fmt::usd(g.median_cost_to_complete)),
                right(fmt::minutes(g.median_active_to_complete)),
                right(g.loops.to_string()),
                left(format!(
                    "{}{}",
                    if g.unclassified > 0 {
                        format!("{}◌ ", g.unclassified)
                    } else {
                        String::new()
                    },
                    if g.tasks < 15 { "low n" } else { "" }
                )),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[1],
        &[
            selected_dim.label(),
            ">Sess",
            ">Tasks",
            ">API-eq",
            ">Share",
            "",
            ">Fresh",
            ">Med $/task",
            ">Done",
            ">Med $→done",
            ">Med time",
            ">Loops",
            "",
        ],
        &[
            Constraint::Min(16),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(5),
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(5),
            Constraint::Length(11),
            Constraint::Length(9),
            Constraint::Length(5),
            Constraint::Length(10),
        ],
        table_rows,
        Some(current.selected),
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            "Groups of tasks; costs add up to the total. Token bands use fresh tokens. Compare like with like: filter by intent (f).",
            dim(),
        )),
        parts[2],
    );
}

fn render_session_overview(frame: &mut TermFrame<'_>, detail: &SessionDetail, area: Rect) {
    let native_usage = detail.native_usage_available;
    let tool_calls: i64 = detail.tool_summary.iter().map(|row| row.calls).sum();
    let failed_tools: i64 = detail.tool_summary.iter().map(|row| row.failed).sum();
    let skill_calls = detail
        .skills
        .iter()
        .filter(|row| row.kind == "call")
        .count();
    let skill_listings = detail
        .skills
        .iter()
        .filter(|row| row.kind == "listed")
        .count();
    let skill_injections = detail
        .skills
        .iter()
        .filter(|row| row.kind == "injected")
        .count();
    let skill_loads = detail
        .skills
        .iter()
        .filter(|row| row.kind == "loaded")
        .count();
    let peak = detail
        .calls
        .iter()
        .map(|call| call.context_tokens)
        .max()
        .unwrap_or(0);
    let partial = detail
        .calls
        .iter()
        .filter(|call| call.capture_completeness != "observed sizes")
        .count();
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Percentage(48),
            Constraint::Min(5),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(format!(
                "{} model requests · peak observed context {} tokens · {} requests with partial or unavailable capture",
                detail.calls.len(), if native_usage { fmt::tokens(peak) } else { "unavailable".into() }, partial
            )),
            Line::from(format!(
                "{} tool calls ({} failed) · {} explicit skill calls · {} listings · {} injections · {} file loads",
                tool_calls, failed_tools, skill_calls, skill_listings, skill_injections, skill_loads
            )),
            Line::from(Span::styled(
                "Context sizes and repeated footprint are estimates from captured material; native usage appears in Context.",
                dim(),
            )),
        ])
        .block(Block::default().title("Session at a glance").borders(Borders::ALL)),
        regions[0],
    );

    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
        .split(regions[1]);
    let mut context_lines = Vec::new();
    for report in &detail.investigations {
        context_lines.push(Line::from(format!(
            "Work {} · {} · purpose {}",
            &report.work_id[..report.work_id.len().min(8)],
            report.state,
            report.corrective_purpose
        )));
        for line in report.statements.iter().chain(report.gaps.iter()).take(5) {
            context_lines.push(Line::from(line.clone()));
        }
    }
    if detail.context_sources.is_empty() {
        context_lines.push(Line::from("No captured context items"));
    } else {
        for source in &detail.context_sources {
            context_lines.push(Line::from(format!(
                "{} · {} items · {} chars · ~{} repeated chars",
                source.source,
                source.items,
                fmt::tokens(source.captured_chars),
                fmt::tokens(source.repeated_chars)
            )));
        }
    }
    frame.render_widget(
        Paragraph::new(context_lines)
            .block(
                Block::default()
                    .title("Context built from")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        middle[0],
    );
    let mut tool_lines = Vec::new();
    if detail.tool_summary.is_empty() {
        tool_lines.push(Line::from("No tool calls captured"));
    } else {
        for tool in &detail.tool_summary {
            tool_lines.push(Line::from(format!(
                "{} · {} calls · {} failed",
                tool.category, tool.calls, tool.failed
            )));
        }
    }
    frame.render_widget(
        Paragraph::new(tool_lines)
            .block(
                Block::default()
                    .title("Tools by type")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        middle[1],
    );

    let mut skill_counts = std::collections::BTreeMap::<&str, (usize, usize)>::new();
    for skill in &detail.skills {
        let entry = skill_counts.entry(&skill.skill_id).or_default();
        if skill.kind == "call" {
            entry.0 += 1;
        } else if skill.kind == "injected" || skill.kind == "loaded" {
            entry.1 += 1;
        }
    }
    let mut skill_lines = Vec::new();
    for (id, (calls, other)) in skill_counts {
        skill_lines.push(Line::from(format!(
            "{id} · {calls} calls · {other} injections/loads"
        )));
    }
    if skill_lines.is_empty() {
        skill_lines.push(Line::from("No named skills observed"));
    }
    skill_lines.push(Line::from(Span::styled(
        "Tab: Conversation → Tasks → Agents → Usage → Context → Skills → Delivery. Select a request in Context and press x for its origins.",
        dim(),
    )));
    frame.render_widget(
        Paragraph::new(skill_lines)
            .block(
                Block::default()
                    .title("Skills observed")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        regions[2],
    );
}

fn render_detail(
    frame: &mut TermFrame<'_>,
    app: &App,
    detail: &SessionDetail,
    tab: DetailTab,
    area: Rect,
) {
    let s = &detail.session;
    let native_usage = detail.native_usage_available;
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(3),
        ])
        .split(area);
    let completed = detail
        .tasks
        .iter()
        .filter(|t| t.supported_completed_at.is_some())
        .count();
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                format!("{} · {}", s.provider, fmt::truncate(&s.title, 120)),
                Style::default().fg(BR).add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(
                format!(
                    "{} · branch {} · {} → {} · {} turns · {} task(s), {} completed · {} subagent(s)",
                    s.repo_root.as_deref().map(short_repo).unwrap_or_else(|| "(no repo)".into()),
                    s.git_branch.as_deref().unwrap_or("—"),
                    fmt::when(s.started_at.as_deref()),
                    fmt::when(s.ended_at.as_deref()),
                    s.turns,
                    s.tasks,
                    completed,
                    s.agents
                ),
                Style::default().fg(SU),
            )),
            Line::from(Span::styled(
                format!(
                    "API-eq {} tree (subagents {}) · fresh {} · cache read {} · models {}",
                    fmt::usd(if native_usage { s.cost } else { None }),
                    fmt::usd(s.subagent_cost),
                    if native_usage { fmt::tokens(s.fresh_tokens) } else { "—".into() },
                    if native_usage { fmt::tokens(s.cache_read_tokens) } else { "—".into() },
                    s.models.split(',').map(short_model).collect::<Vec<_>>().join(" → ")
                ),
                Style::default().fg(SU),
            )),
        ]),
        parts[0],
    );
    frame.render_widget(
        Paragraph::new(tab_bar(&DetailTab::ALL, tab, |t| t.label())),
        parts[1],
    );
    let body = parts[2];
    let selected = Some(app.frame().selected);
    match tab {
        DetailTab::Overview => render_session_overview(frame, detail, body),
        DetailTab::Conversation => {
            let rows: Vec<Row> = detail
                .turns
                .iter()
                .map(|t| {
                    let validation = if t.validation_runs > 0 {
                        format!(
                            "{}✓ {}✗",
                            t.validation_runs - t.validation_failures,
                            t.validation_failures
                        )
                    } else {
                        String::new()
                    };
                    Row::new(vec![
                        right(t.turn_index.to_string()),
                        left(fmt::when(t.started_at.as_deref())),
                        left(
                            t.task_index
                                .map(|task| format!("T{task}"))
                                .unwrap_or_default(),
                        ),
                        left(fmt::truncate(&t.user_text, 90)),
                        left(
                            t.request_kind
                                .as_deref()
                                .map(|r| r.replace('_', " "))
                                .unwrap_or_default(),
                        ),
                        colored(
                            t.stage
                                .as_deref()
                                .map(|stage| {
                                    format!(
                                        "{}{}",
                                        crate::insights::stages::short_stage(stage),
                                        match t.stage_source.as_deref() {
                                            Some("heuristic") => " h",
                                            Some("override") => "✎",
                                            _ => "",
                                        }
                                    )
                                })
                                .unwrap_or_default(),
                            CY,
                        ),
                        right(format!("{}/{}", t.edit_calls, t.tool_calls)),
                        colored(validation, if t.validation_failures > 0 { RD } else { GN }),
                        right(fmt::tokens(t.tokens)),
                        right(fmt::usd(Some(t.cost))),
                        colored(
                            fmt::label(t.feedback.as_deref(), t.feedback_source.as_deref(), None),
                            if t.feedback
                                .as_deref()
                                .is_some_and(|f| f.starts_with("correct_"))
                            {
                                RD
                            } else {
                                SU
                            },
                        ),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                body,
                &[
                    ">#",
                    "Time",
                    "Task",
                    "You said",
                    "Asked for",
                    "Stage",
                    ">Edit/tools",
                    "Validation",
                    ">Tokens",
                    ">API-eq",
                    "Feedback",
                ],
                &[
                    Constraint::Length(3),
                    Constraint::Length(11),
                    Constraint::Length(4),
                    Constraint::Min(24),
                    Constraint::Length(16),
                    Constraint::Length(11),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Length(7),
                    Constraint::Length(8),
                    Constraint::Length(22),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Tasks => {
            let mut rows: Vec<Row> = detail
                .tasks
                .iter()
                .map(|t| {
                    Row::new(vec![
                        left(format!("T{}", t.task_index)),
                        left(match &t.stage_path {
                            Some(path) => format!(
                                "{} · {}",
                                fmt::truncate(&t.title, 32),
                                fmt::truncate(path, 40)
                            ),
                            None => fmt::truncate(&t.title, 50),
                        }),
                        left(fmt::label(
                            t.intent.as_deref(),
                            t.intent_source.as_deref(),
                            t.intent_band.as_deref(),
                        )),
                        colored(
                            fmt::label(
                                t.outcome.as_deref(),
                                t.outcome_source.as_deref(),
                                t.outcome_band.as_deref(),
                            ),
                            outcome_color(t.outcome.as_deref()),
                        ),
                        left(fmt::when(t.supported_completion_time().as_deref())),
                        right(fmt::minutes(
                            t.active_minutes_to_complete.or(t.active_minutes_total),
                        )),
                        right(fmt::usd(t.cost_to_complete.or(t.cost_total))),
                        right(fmt::usd(t.post_completion_cost)),
                        left(t.completion_source.clone()),
                    ])
                })
                .collect();
            for attempt in &detail.attempts {
                let result_color = match attempt.validation_result.as_str() {
                    "pass" => GN,
                    "fail" => RD,
                    _ => MU,
                };
                rows.push(Row::new(vec![
                    colored(format!("  A{}", attempt.attempt_index), MU),
                    colored(
                        format!(
                            "T{} · {} edit(s), {} lines → {} {}",
                            attempt.task_index,
                            attempt.edit_calls,
                            attempt.lines_changed,
                            attempt
                                .validation_kind
                                .as_deref()
                                .unwrap_or("no validation"),
                            fmt::truncate(attempt.error_signature.as_deref().unwrap_or(""), 40)
                        ),
                        SU,
                    ),
                    left(String::new()),
                    colored(
                        format!(
                            "{}{}",
                            attempt.validation_result,
                            if attempt.loop_flag { " LOOP" } else { "" }
                        ),
                        if attempt.loop_flag { RD } else { result_color },
                    ),
                    left(attempt.ended_by.clone()),
                    left(String::new()),
                    left(String::new()),
                    left(String::new()),
                    left(String::new()),
                ]));
            }
            render_table(
                frame,
                app,
                body,
                &[
                    "Task",
                    "First request · stages / attempt",
                    "Intent",
                    "Outcome",
                    "Done",
                    ">Active",
                    ">Cost→done",
                    ">After",
                    "Done by",
                ],
                &[
                    Constraint::Length(5),
                    Constraint::Min(30),
                    Constraint::Length(19),
                    Constraint::Length(22),
                    Constraint::Length(13),
                    Constraint::Length(7),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Length(15),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Agents => {
            let rows: Vec<Row> = detail
                .agents
                .iter()
                .map(|a| {
                    let name = if a.session_id == s.session_id {
                        "● main".to_string()
                    } else {
                        format!("└ {}", a.session_id.rsplit('/').next().unwrap_or(""))
                    };
                    Row::new(vec![
                        left(name),
                        left(a.agent_type.clone().unwrap_or_else(|| a.agent_kind.clone())),
                        left(fmt::truncate(a.title.as_deref().unwrap_or(""), 40)),
                        left(fmt::truncate(
                            &a.models
                                .split(',')
                                .map(short_model)
                                .collect::<Vec<_>>()
                                .join(","),
                            24,
                        )),
                        right(a.turns.to_string()),
                        right(if native_usage {
                            fmt::tokens(a.fresh_tokens)
                        } else {
                            "—".into()
                        }),
                        right(fmt::usd(if native_usage { a.cost } else { None })),
                        left(if a.background_tokens > 0 {
                            format!("{} unattended", fmt::tokens(a.background_tokens))
                        } else {
                            String::new()
                        }),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                body,
                &[
                    "Agent",
                    "Type",
                    "Description",
                    "Models",
                    ">Turns",
                    ">Fresh",
                    ">API-eq",
                    "Notes",
                ],
                &[
                    Constraint::Length(26),
                    Constraint::Length(14),
                    Constraint::Min(20),
                    Constraint::Length(24),
                    Constraint::Length(6),
                    Constraint::Length(7),
                    Constraint::Length(9),
                    Constraint::Length(18),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Usage => {
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(4), Constraint::Min(3)])
                .split(body);
            let burn: Vec<u64> = detail
                .turns
                .iter()
                .map(|t| (t.cost * 100.0) as u64)
                .collect();
            frame.render_widget(
                Sparkline::default()
                    .block(Block::default().title(Span::styled(
                        if native_usage {
                            "API-equiv per turn"
                        } else {
                            "Native usage unavailable"
                        },
                        dim(),
                    )))
                    .data(&burn)
                    .style(Style::default().fg(CY)),
                split[0],
            );
            let rows: Vec<Row> = detail
                .models
                .iter()
                .map(|m| {
                    Row::new(vec![
                        left(short_model(&m.model)),
                        right(m.calls.to_string()),
                        right(if native_usage {
                            fmt::tokens(m.fresh_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(m.cache_read_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(m.output_tokens)
                        } else {
                            "—".into()
                        }),
                        right(fmt::usd(if native_usage { m.cost } else { None })),
                        left(format!(
                            "{} · pricing {}",
                            m.token_source,
                            m.price_match.as_deref().unwrap_or("none")
                        )),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[1],
                &[
                    "Model",
                    ">Calls",
                    ">Fresh",
                    ">Cache rd",
                    ">Output",
                    ">API-eq",
                    "Grade",
                ],
                &[
                    Constraint::Length(24),
                    Constraint::Length(6),
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Min(16),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Context => {
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(4),
                    Constraint::Length(4),
                    Constraint::Min(3),
                ])
                .split(body);
            let added: Vec<u64> = detail
                .calls
                .iter()
                .map(|c| c.new_tokens.max(0) as u64)
                .collect();
            let resent: Vec<u64> = detail
                .calls
                .iter()
                .map(|c| c.carried_tokens.max(0) as u64)
                .collect();
            frame.render_widget(
                Sparkline::default()
                    .block(
                        Block::default()
                            .title(Span::styled("Estimated context added per request", dim())),
                    )
                    .data(&added)
                    .style(Style::default().fg(AM)),
                split[0],
            );
            frame.render_widget(
                Sparkline::default()
                    .block(Block::default().title(Span::styled(
                        "Estimated context carried / resent per request",
                        dim(),
                    )))
                    .data(&resent)
                    .style(Style::default().fg(CY)),
                split[1],
            );
            let rows: Vec<Row> = detail
                .calls
                .iter()
                .map(|call| {
                    let sources = detail
                        .context_items
                        .iter()
                        .filter(|item| item.entered_call == call.call_index)
                        .take(3)
                        .map(|item| item.target.as_deref().unwrap_or(&item.kind))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Row::new(vec![
                        right(call.call_index.to_string()),
                        right(
                            call.task_index
                                .map(|task| task.to_string())
                                .unwrap_or_default(),
                        ),
                        right(if native_usage {
                            fmt::tokens(call.new_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(call.carried_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(call.fresh_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(call.cached_tokens)
                        } else {
                            "—".into()
                        }),
                        right(if native_usage {
                            fmt::tokens(call.output_tokens)
                        } else {
                            "—".into()
                        }),
                        right(fmt::usd(call.reported_cost)),
                        left(call.cache_event.clone()),
                        left(call.capture_completeness.clone()),
                        left(fmt::truncate(&sources, 48)),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[2],
                &[
                    ">Req",
                    ">Task",
                    ">~Added",
                    ">~Resent",
                    ">Input",
                    ">Cached",
                    ">Output",
                    ">Reported $",
                    "Event",
                    "Capture",
                    "Introduced by",
                ],
                &[
                    Constraint::Length(4),
                    Constraint::Length(5),
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(13),
                    Constraint::Min(16),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Skills => {
            let window = app.frame().panel % 3;
            let label = ["task end", "session end", "next skill"][window];
            let listed = detail.skills.iter().filter(|s| s.kind == "listed").count();
            let injected = detail
                .skills
                .iter()
                .filter(|s| s.kind == "injected")
                .count();
            let loaded = detail.skills.iter().filter(|s| s.kind == "loaded").count();
            let succeeded = detail
                .skills
                .iter()
                .filter(|s| s.kind == "call" && s.status == "ok")
                .count();
            let failed = detail
                .skills
                .iter()
                .filter(|s| {
                    s.kind == "call"
                        && matches!(s.status.as_str(), "error" | "rejected" | "interrupted")
                })
                .count();
            let unknown = detail
                .skills
                .iter()
                .filter(|s| s.kind == "call" && s.status == "unknown")
                .count();
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(3)])
                .split(body);
            frame.render_widget(Paragraph::new(format!(
                "Calls/session: {succeeded} successful · {failed} failed · {unknown} unknown · {loaded} loaded · {injected} injected · {listed} listings\nDownstream window: {label} (w to change). Windows overlap and are not additive."
            )).block(Block::default().title("Skill evidence")), split[0]);
            let rows: Vec<Row> = detail
                .skills
                .iter()
                .map(|skill| {
                    let usage = match window {
                        1 => &skill.session,
                        2 => &skill.next_skill,
                        _ => &skill.task,
                    };
                    Row::new(vec![
                        left(fmt::truncate(&skill.skill_id, 24)),
                        left(format!("{} / {}", skill.kind, skill.status)),
                        right(skill.task_index.map(|i| i.to_string()).unwrap_or_default()),
                        right(format!("{}", skill.body_chars)),
                        right(fmt::tokens(skill.body_est_tokens)),
                        right(skill.carried_calls.to_string()),
                        right(usage.calls.to_string()),
                        right(fmt::tokens(
                            usage.input_tokens + usage.cached_tokens + usage.output_tokens,
                        )),
                        right(fmt::usd(usage.reported_cost_usd.or(usage.api_equiv_usd))),
                        left(skill.follow_through.clone()),
                        left(skill.outcome.clone()),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[1],
                &[
                    "Skill", "Evidence", ">Task", ">Chars", ">~Body", ">Carry", ">Calls",
                    ">Tokens", ">Cost*", "Follow", "Outcome",
                ],
                &[
                    Constraint::Length(24),
                    Constraint::Length(19),
                    Constraint::Length(5),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(6),
                    Constraint::Length(6),
                    Constraint::Length(9),
                    Constraint::Length(9),
                    Constraint::Length(12),
                    Constraint::Min(10),
                ],
                rows,
                selected,
            );
        }
        DetailTab::Delivery => {
            let rows: Vec<Row> = detail
                .commits
                .iter()
                .map(|c| {
                    Row::new(vec![
                        left(c.sha.chars().take(8).collect::<String>()),
                        left(fmt::when(Some(&c.commit_time))),
                        left(fmt::truncate(&c.subject, 50)),
                        right(format!("{:.0}%", c.ai_share * 100.0)),
                        colored(
                            match c.merged_to_mainline {
                                Some(true) => "✓ mainline",
                                Some(false) => "not yet",
                                None => "·",
                            },
                            if c.merged_to_mainline == Some(true) {
                                GN
                            } else {
                                MU
                            },
                        ),
                        left(
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
                        ),
                        colored(
                            c.fix_signal
                                .clone()
                                .map(|s| format!("later fix signal ({s})"))
                                .unwrap_or_default(),
                            AM,
                        ),
                    ])
                })
                .collect();
            if rows.is_empty() {
                frame.render_widget(
                    Paragraph::new(Span::styled(
                        "No commit contains AI-written lines from this session. Planning, research and review sessions are judged by outcome instead.",
                        dim(),
                    ))
                    .wrap(Wrap { trim: true }),
                    body,
                );
            } else {
                render_table(
                    frame,
                    app,
                    body,
                    &[
                        "Commit",
                        "Time",
                        "Subject",
                        ">AI share",
                        "Mainline",
                        "PR",
                        "Later",
                    ],
                    &[
                        Constraint::Length(8),
                        Constraint::Length(11),
                        Constraint::Min(20),
                        Constraint::Length(8),
                        Constraint::Length(10),
                        Constraint::Length(8),
                        Constraint::Length(30),
                    ],
                    rows,
                    selected,
                );
            }
        }
        DetailTab::TurnContext => {
            let parts = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(2), Constraint::Min(3)])
                .split(body);
            frame.render_widget(
                Paragraph::new(Span::styled(
                    "User turns are coding-agent boundaries, not confirmed external runs. Prior/new sizes are captured material; ~tokens use character estimates.",
                    dim(),
                ))
                .wrap(Wrap { trim: true }),
                parts[0],
            );
            let rows: Vec<Row> = detail
                .turn_context
                .iter()
                .map(|turn| {
                    Row::new(vec![
                        right(turn.turn_index.to_string()),
                        left(fmt::when(turn.started_at.as_deref())),
                        right(fmt::minutes(turn.since_previous_minutes)),
                        right(turn.model_requests.to_string()),
                        right(turn.tool_calls.to_string()),
                        right(
                            turn.first_context_tokens
                                .map(fmt::tokens)
                                .unwrap_or_else(|| "—".into()),
                        ),
                        right(
                            turn.last_context_tokens
                                .map(fmt::tokens)
                                .unwrap_or_else(|| "—".into()),
                        ),
                        right(fmt::tokens(turn.prior_captured_chars)),
                        right(fmt::tokens(turn.prior_est_tokens)),
                        right(fmt::tokens(turn.new_captured_chars)),
                        right(fmt::tokens(turn.new_est_tokens)),
                        left(turn.capture.clone()),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                parts[1],
                &[
                    ">Turn",
                    "Time",
                    ">Gap",
                    ">Req",
                    ">Tools",
                    ">First in",
                    ">Last in",
                    ">Prior chars",
                    ">~Prior",
                    ">New chars",
                    ">~New",
                    "Capture",
                ],
                &[
                    Constraint::Length(5),
                    Constraint::Length(11),
                    Constraint::Length(8),
                    Constraint::Length(5),
                    Constraint::Length(6),
                    Constraint::Length(9),
                    Constraint::Length(9),
                    Constraint::Length(12),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(7),
                    Constraint::Min(12),
                ],
                rows,
                selected,
            );
        }
    }
}

fn render_usage(
    frame: &mut TermFrame<'_>,
    app: &App,
    tab: UsageTab,
    ctx_dim: usize,
    data: &Data,
    area: Rect,
) {
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(tab_bar(&UsageTab::ALL, tab, |t| t.label())),
        parts[0],
    );
    let selected = Some(app.frame().selected);
    match (tab, data) {
        (UsageTab::Timeline, Data::Usage(buckets, spikes)) => {
            let rows = usage_rows(buckets);
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(6), Constraint::Min(3)])
                .split(parts[1]);
            let series: Vec<u64> = rows
                .iter()
                .rev()
                .map(|(_, items)| (items.iter().map(|b| b.cost).sum::<f64>() * 100.0) as u64)
                .collect();
            frame.render_widget(
                Sparkline::default()
                    .block(Block::default().title(Span::styled(
                        "API-equiv per bucket (oldest → newest)",
                        dim(),
                    )))
                    .data(&series)
                    .style(Style::default().fg(CY)),
                split[0],
            );
            let table_rows: Vec<Row> = rows
                .iter()
                .map(|(key, items)| {
                    let cost: f64 = items.iter().map(|b| b.cost).sum();
                    let fresh: i64 = items.iter().map(|b| b.fresh_tokens).sum();
                    let unattended: i64 = items.iter().map(|b| b.background_tokens).sum();
                    let unpriced: i64 = items.iter().map(|b| b.unpriced_tokens).sum();
                    let spike = spikes.iter().any(|s| s.bucket.starts_with(key.as_str()));
                    let tools = items
                        .iter()
                        .map(|b| format!("{} {}", b.provider, fmt::usd(Some(b.cost))))
                        .collect::<Vec<_>>()
                        .join(" · ");
                    Row::new(vec![
                        colored(if spike { "▲" } else { " " }, RD),
                        left(key.clone()),
                        right(fmt::usd(Some(cost))),
                        right(fmt::tokens(fresh)),
                        right(fmt::tokens(unattended)),
                        left(tools),
                        left(if unpriced > 0 {
                            format!("{} unpriced", fmt::tokens(unpriced))
                        } else {
                            String::new()
                        }),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[1],
                &[
                    "",
                    "Bucket",
                    ">API-eq",
                    ">Fresh",
                    ">Unattended",
                    "By tool",
                    "",
                ],
                &[
                    Constraint::Length(1),
                    Constraint::Length(16),
                    Constraint::Length(9),
                    Constraint::Length(7),
                    Constraint::Length(10),
                    Constraint::Min(20),
                    Constraint::Length(14),
                ],
                table_rows,
                selected,
            );
            frame.render_widget(
                Paragraph::new(Span::styled("▲ spike: >3× median and ≥ p95 hourly burn · unattended = >10 min after your last message", dim())),
                parts[2],
            );
        }
        (UsageTab::Windows, Data::Windows(windows)) => {
            let rows: Vec<Row> = windows
                .iter()
                .map(|w| {
                    let used = match (w.start_percent, w.end_percent) {
                        (Some(start), Some(end)) => format!(
                            "{start:.0}% → {end:.0}%{}",
                            if w.reached_limit_at.is_some() {
                                " ◆ limit"
                            } else {
                                ""
                            }
                        ),
                        _ if w.limit_hits_logged > 0 => {
                            format!("no % data · {} hit(s) logged ◆", w.limit_hits_logged)
                        }
                        _ => "no % data".into(),
                    };
                    Row::new(vec![
                        left(w.provider.clone()),
                        left(w.name.clone()),
                        left(format!(
                            "{} → {}",
                            fmt::when(w.window_start.as_deref().or(Some(&w.first_seen))),
                            fmt::when(w.resets_at.as_deref())
                        )),
                        colored(used, if w.observed { BR } else { MU }),
                        right(fmt::tokens(if w.fresh_tokens > 0 {
                            w.fresh_tokens
                        } else {
                            w.tokens
                        })),
                        right(fmt::usd(w.cost)),
                        left(if w.unobserved_minutes > 0 {
                            format!("{}m unobserved", w.unobserved_minutes)
                        } else {
                            String::new()
                        }),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                parts[1],
                &[
                    "Tool",
                    "Window",
                    "Start → reset",
                    "Quota used",
                    ">Tokens",
                    ">API-eq",
                    "Notes",
                ],
                &[
                    Constraint::Length(9),
                    Constraint::Length(20),
                    Constraint::Length(25),
                    Constraint::Length(26),
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Min(10),
                ],
                rows,
                selected,
            );
            frame.render_widget(
                Paragraph::new(Span::styled("Observed % from provider logs (Codex). Other tools: token burn only, % unknown. M logs a limit hit.", dim())),
                parts[2],
            );
        }
        (UsageTab::Context, Data::ContextGroups(groups)) => {
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Min(3)])
                .split(parts[1]);
            let current_dim = ContextDim::ALL[ctx_dim % ContextDim::ALL.len()];
            frame.render_widget(
                Paragraph::new(tab_bar(&ContextDim::ALL, current_dim, |d| d.label())),
                split[0],
            );
            let rows: Vec<Row> = groups
                .iter()
                .map(|g| {
                    Row::new(vec![
                        left(fmt::truncate(&g.key, 50)),
                        right(g.items.to_string()),
                        right(g.sessions.to_string()),
                        right(fmt::tokens(g.tokens)),
                        right(fmt::usd(g.write_usd)),
                        right(fmt::usd(g.carry_usd)),
                        right(fmt::usd(g.rebuild_usd)),
                        right(fmt::usd(g.total_usd)),
                        right(fmt::usd(g.per_session_usd)),
                        right(fmt::percent(g.unused_share)),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[1],
                &[
                    current_dim.label(),
                    ">Items",
                    ">Sess",
                    ">Tokens",
                    ">Write",
                    ">Carry",
                    ">Rebuild",
                    ">Total",
                    ">/session",
                    ">Unused",
                ],
                &[
                    Constraint::Min(20),
                    Constraint::Length(6),
                    Constraint::Length(5),
                    Constraint::Length(7),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Length(6),
                ],
                rows,
                selected,
            );
            frame.render_widget(
                Paragraph::new(Span::styled(
                    "Write = entering context · Carry = re-read on every later call · token sizes calibrated estimates · g regroups",
                    dim(),
                )),
                parts[2],
            );
        }
        _ => frame.render_widget(Paragraph::new(Span::styled("loading…", dim())), parts[1]),
    }
}

fn render_window(
    frame: &mut TermFrame<'_>,
    app: &App,
    window: &crate::explore::QuotaWindowRow,
    area: Rect,
) {
    let current = app.frame();
    let Some(Data::Contributors(rows)) = current.data.as_ref() else {
        return;
    };
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
    let observed = match (window.start_percent, window.end_percent) {
        (Some(start), Some(end)) => format!("{start:.0}% → {end:.0}% observed"),
        _ => "no % data (estimated window)".into(),
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                format!(
                    "{} {} · {} → {} · {}",
                    window.provider,
                    window.name,
                    fmt::when(window.window_start.as_deref()),
                    fmt::when(window.resets_at.as_deref()),
                    observed
                ),
                Style::default().fg(BR).add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(
                format!(
                    "{} tokens · {} · unobserved {} min · logged limit hits {}",
                    fmt::tokens(window.tokens),
                    fmt::usd(window.cost),
                    window.unobserved_minutes,
                    window.limit_hits_logged
                ),
                dim(),
            )),
        ]),
        parts[0],
    );
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|c| {
            Row::new(vec![
                left(format!("{}:{}", c.provider, short_id(&c.root_session_id))),
                left(fmt::truncate(&c.title, 60)),
                right(
                    c.delta_percent
                        .map(|d| format!("≈{d:.1}pp"))
                        .unwrap_or_else(|| "—".into()),
                ),
                right(fmt::tokens(c.tokens)),
                right(fmt::usd(c.cost)),
                right(fmt::tokens(c.subagent_tokens)),
                right(fmt::tokens(c.background_tokens)),
                colored(
                    c.outcome.clone().unwrap_or_default(),
                    outcome_color(c.outcome.as_deref()),
                ),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[1],
        &[
            "Session",
            "Title",
            ">Δ quota",
            ">Tokens",
            ">API-eq",
            ">Subagents",
            ">Unattended",
            "Outcome",
        ],
        &[
            Constraint::Length(18),
            Constraint::Min(20),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(20),
        ],
        table_rows,
        Some(current.selected),
    );
    frame.render_widget(
        Paragraph::new(Span::styled("≈ allocated by cost share between percentage rises; concurrent sessions share the window (not causal)", dim())),
        parts[2],
    );
}

fn render_skill_frequency(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let Some(Data::Skills(skills)) = app.frame().data.as_ref() else {
        return;
    };
    let rows: Vec<Row> = skills
        .iter()
        .map(|skill| {
            Row::new(vec![
                left(fmt::truncate(&skill.skill_id, 42)),
                right(skill.calls.to_string()),
                right(skill.successful.to_string()),
                right(skill.failed.to_string()),
                right(skill.unknown_calls.to_string()),
                right(skill.sessions.to_string()),
                right(format!("{:.2}", skill.calls_per_session)),
                right(skill.injected.to_string()),
                right(skill.loaded.to_string()),
                right(skill.listed.to_string()),
                right(skill.distinct_tasks.to_string()),
                right(
                    skill
                        .eligible_tasks
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "?".into()),
                ),
                right(skill.verified_tasks.to_string()),
                right(fmt::usd(skill.cost_per_verified_usd)),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        area,
        &[
            "Skill",
            ">Calls",
            ">OK",
            ">Failed",
            ">Unknown",
            ">Sessions",
            ">Calls/sess",
            ">Injected",
            ">Loaded",
            ">Listings",
            ">Tasks",
            ">Matched",
            ">Verified",
            ">$/verified",
        ],
        &[
            Constraint::Min(18),
            Constraint::Length(7),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(11),
        ],
        rows,
        Some(app.frame().selected),
    );
}

fn render_context_items(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::ContextItems(rows)) = current.data.as_ref() else {
        return;
    };
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|i| {
            Row::new(vec![
                left(format!("{}:{}", i.provider, short_id(&i.session_id))),
                left(fmt::truncate(i.tool_name.as_deref().unwrap_or(&i.kind), 16)),
                left(fmt::truncate(i.target.as_deref().unwrap_or(""), 60)),
                right(fmt::tokens(i.tokens)),
                right(i.carried_calls.to_string()),
                right(fmt::usd(i.total_usd)),
                left(i.purpose.clone().unwrap_or_default()),
                left(i.instruction_ref.clone().unwrap_or_default()),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        area,
        &[
            "Session", "Item", "Target", ">Tokens", ">Carried", ">Total", "Purpose", "Instr.",
        ],
        &[
            Constraint::Length(18),
            Constraint::Length(16),
            Constraint::Min(20),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(12),
        ],
        table_rows,
        Some(current.selected),
    );
}

fn render_ladder(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::Ladder(rows, rework)) = current.data.as_ref() else {
        return;
    };
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(9)])
        .split(area);
    render_rework(frame, rework, outer[1]);
    let parts = split_footer(outer[0]);
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|r| {
            let code = |value: i64| {
                if r.code_intent {
                    value.to_string()
                } else {
                    "·".into()
                }
            };
            Row::new(vec![
                colored(r.intent.clone(), if r.code_intent { BR } else { SU }),
                right(r.tasks.to_string()),
                right(fmt::percent(Some(
                    r.completed as f64 / r.tasks.max(1) as f64,
                ))),
                right(r.stalled.to_string()),
                right(r.edited.to_string()),
                right(r.validated.to_string()),
                right(code(r.committed)),
                right(code(r.mainline)),
                right(code(r.pr_merged)),
                colored(code(r.later_fix), if r.later_fix > 0 { AM } else { SU }),
                right(code(r.reverted)),
                right(fmt::usd(r.cost)),
                left(if r.heuristic_share > 0.5 { "h" } else { "" }.to_string()),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[0],
        &[
            "Intent",
            ">Tasks",
            ">Done",
            ">Stalled",
            ">Edited",
            ">Valid.✓",
            ">Committed",
            ">Mainline",
            ">PR ✓",
            ">Later fix",
            ">Reverted",
            ">API-eq",
            "",
        ],
        &[
            Constraint::Min(18),
            Constraint::Length(6),
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(1),
        ],
        table_rows,
        Some(current.selected),
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            "Progress (done/stalled) and delivery evidence are separate. · = non-code intent, judged by outcome. Later fix = evidence, not cause.",
            dim(),
        )),
        parts[1],
    );
}

fn render_rework(frame: &mut TermFrame<'_>, rework: &super::data::ReworkData, area: Rect) {
    let split = &rework.split;
    let share = |value: f64| {
        if split.total > 0.0 {
            format!("{:.0}%", value / split.total * 100.0)
        } else {
            "—".into()
        }
    };
    let mut lines = vec![Line::from(vec![
        Span::styled("Spend  ", Style::default().fg(CY)),
        Span::styled(
            format!(
                "loops {} {}",
                fmt::usd(Some(split.loops)),
                share(split.loops)
            ),
            Style::default().fg(if split.loops > 0.0 { RD } else { SU }),
        ),
        Span::raw(" · "),
        Span::styled(
            format!(
                "corrections {} {}",
                fmt::usd(Some(split.corrections)),
                share(split.corrections)
            ),
            Style::default().fg(if split.corrections > 0.0 { AM } else { SU }),
        ),
        Span::raw(" · "),
        Span::styled(
            format!(
                "requirement changes {} {}",
                fmt::usd(Some(split.requirement_changes)),
                share(split.requirement_changes)
            ),
            Style::default().fg(SU),
        ),
        Span::raw(" · "),
        Span::styled(
            format!("other {}", fmt::usd(Some(split.normal))),
            Style::default().fg(SU),
        ),
    ])];
    lines.push(Line::from(Span::styled(
        format!(
            "{} relations where later work changed earlier AI lines · suspected defects by evidence level:",
            rework.relations
        ),
        dim(),
    )));
    if rework.defects.is_empty() {
        lines.push(Line::from(Span::styled("  none", dim())));
    }
    for defect in rework.defects.iter().take(5) {
        lines.push(Line::from(vec![
            Span::styled(
                format!("  L{} ", defect.level),
                Style::default().fg(if defect.level >= 3 { RD } else { AM }),
            ),
            Span::styled(
                format!("{:<36}", fmt::truncate(&defect.title, 34)),
                Style::default().fg(BR),
            ),
            Span::styled(
                format!(
                    "{} later · {} lines · corrections {} · {}",
                    defect.corrections,
                    defect.overlap_lines,
                    fmt::usd(Some(defect.correction_cost)),
                    fmt::truncate(&defect.signals.join("; "), 60)
                ),
                Style::default().fg(SU),
            ),
        ]));
    }
    frame.render_widget(
        Paragraph::new(lines).block(panel_block(
            "Rework · touched AI-written lines (correlation, not cause) · `vca rework` for the full list",
            false,
        )),
        area,
    );
}

fn render_practices(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::Practices(rows)) = current.data.as_ref() else {
        return;
    };
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(4)])
        .split(area);
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|r| {
            Row::new(vec![
                left(r.name.to_string()),
                right(r.with_n.to_string()),
                left(r.with_value.clone()),
                left(r.without_value.clone()),
                colored(
                    r.confidence.to_string(),
                    if r.confidence == "low n" { AM } else { SU },
                ),
            ])
        })
        .collect();
    render_table(
        frame,
        app,
        parts[0],
        &["Pattern", ">With", "With", "Without / note", "Conf."],
        &[
            Constraint::Length(46),
            Constraint::Length(6),
            Constraint::Length(30),
            Constraint::Min(20),
            Constraint::Length(8),
        ],
        table_rows,
        Some(current.selected),
    );
    if let Some(row) = rows.get(current.selected) {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(row.finding.clone(), Style::default().fg(SU))),
                Line::from(Span::styled(format!("Practice: {}", row.advice), Style::default().fg(CY))),
                Line::from(Span::styled("Correlation, not causation · Enter tasks with · o tasks without · x definition", dim())),
            ])
            .wrap(Wrap { trim: true }),
            parts[1],
        );
    }
}

fn render_compare(frame: &mut TermFrame<'_>, app: &App, area: Rect) {
    let current = app.frame();
    let Some(Data::Compare(columns)) = current.data.as_ref() else {
        return;
    };
    if columns.is_empty() {
        frame.render_widget(Paragraph::new("Nothing to compare"), area);
        return;
    }
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(columns.len() as u16 + 1),
        ])
        .split(area);
    let mut headers: Vec<String> = vec!["".into()];
    headers.extend(columns.iter().map(|c| c.label.clone()));
    let header_refs: Vec<&str> = headers.iter().map(String::as_str).collect();
    let dimensions = columns[0].values.len();
    let rows: Vec<Row> = (0..dimensions)
        .map(|index| {
            let mut cells = vec![Cell::from(columns[0].values[index].0.clone()).style(head())];
            cells.extend(columns.iter().map(|c| left(c.values[index].1.clone())));
            Row::new(cells)
        })
        .collect();
    let mut widths = vec![Constraint::Length(26)];
    widths.extend(columns.iter().map(|_| Constraint::Min(20)));
    render_table(
        frame,
        app,
        parts[0],
        &header_refs,
        &widths,
        rows,
        Some(current.selected),
    );
    let lines: Vec<Line> = std::iter::once(Line::from(Span::styled("First request:", head())))
        .chain(columns.iter().map(|c| {
            Line::from(Span::styled(
                format!("{}: {}", c.label, fmt::truncate(&c.first_request, 140)),
                Style::default().fg(SU),
            ))
        }))
        .collect();
    frame.render_widget(Paragraph::new(lines), parts[1]);
}

fn centered(area: Rect, width_pct: u16, height_pct: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - height_pct) / 2),
            Constraint::Percentage(height_pct),
            Constraint::Percentage((100 - height_pct) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_pct) / 2),
            Constraint::Percentage(width_pct),
            Constraint::Percentage((100 - width_pct) / 2),
        ])
        .split(vertical[1])[1]
}

fn render_popup(frame: &mut TermFrame<'_>, app: &App, popup: &Popup, area: Rect) {
    match popup {
        Popup::ObserveInput { kind, text } => {
            let title = if kind == "milestone" {
                "Record milestone: YYYY-MM-DD | title"
            } else {
                "Link: FROM_MEMBER TO_MEMBER [kind]"
            };
            frame.render_widget(Clear, area);
            frame.render_widget(
                Paragraph::new(format!("{text}▏\n\nEnter save · Esc cancel"))
                    .block(Block::default().borders(Borders::ALL).title(title)),
                area,
            );
        }
        Popup::Help => {
            let rect = centered(area, 80, 84);
            frame.render_widget(Clear, rect);
            let lines: Vec<Line> = HELP
                .iter()
                .map(|(key, text)| {
                    Line::from(vec![
                        Span::styled(format!("{key:<14}"), Style::default().fg(CY)),
                        Span::styled(text.to_string(), Style::default().fg(SU)),
                    ])
                })
                .chain(GLYPHS.iter().map(|(glyph, text)| {
                    Line::from(vec![
                        Span::styled(format!("{glyph:<14}"), Style::default().fg(AM)),
                        Span::styled(text.to_string(), dim()),
                    ])
                }))
                .collect();
            frame.render_widget(
                Paragraph::new(lines).block(panel_block("Keys and glyphs (Esc closes)", true)),
                rect,
            );
        }
        Popup::Explain { title, lines } => {
            let rect = centered(area, 78, 60);
            frame.render_widget(Clear, rect);
            let body: Vec<Line> = lines
                .iter()
                .map(|line| Line::from(Span::styled(line.clone(), Style::default().fg(SU))))
                .collect();
            frame.render_widget(
                Paragraph::new(body)
                    .wrap(Wrap { trim: false })
                    .block(panel_block(&format!("{title} (Esc closes)"), true)),
                rect,
            );
        }
        Popup::Search { text } => {
            let rect = centered(area, 60, 12);
            frame.render_widget(Clear, rect);
            frame.render_widget(
                Paragraph::new(Line::from(vec![
                    Span::styled("/ ", Style::default().fg(CY)),
                    Span::styled(format!("{text}█"), Style::default().fg(BR)),
                ]))
                .block(panel_block(
                    "Search titles, first requests, branches, ids · Enter apply · Esc cancel",
                    true,
                )),
                rect,
            );
        }
        Popup::Filters { field, editing } => {
            let rect = centered(area, 70, 50);
            frame.render_widget(Clear, rect);
            let lines: Vec<Line> = FILTER_FIELDS
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    let value = match (index == *field, editing) {
                        (true, Some(buffer)) => format!("{buffer}█"),
                        _ => app.filter_value(index),
                    };
                    let style = if index == *field {
                        Style::default().fg(Color::Black).bg(CY)
                    } else {
                        Style::default().fg(SU)
                    };
                    Line::from(vec![
                        Span::styled(format!(" {name:<10} "), style),
                        Span::raw("  "),
                        Span::styled(value, Style::default().fg(BR)),
                    ])
                })
                .collect();
            frame.render_widget(
                Paragraph::new(lines).block(panel_block("Filters · ↑↓ field · Enter/space edit or cycle · d clear · D clear all · Esc apply", true)),
                rect,
            );
        }
        Popup::Edit {
            target,
            field,
            choice,
        } => {
            let rect = centered(area, 64, 40);
            frame.render_widget(Clear, rect);
            let mut lines: Vec<Line> = Vec::new();
            for (index, (name, options)) in target.fields().iter().enumerate() {
                let value = if index == *field {
                    options.get(*choice).cloned().unwrap_or_default()
                } else {
                    options
                        .get(target.current(index))
                        .cloned()
                        .unwrap_or_default()
                };
                let (label_style, value_text) = if index == *field {
                    (
                        Style::default().fg(Color::Black).bg(CY),
                        format!("◀ {value} ▶"),
                    )
                } else {
                    (Style::default().fg(SU), format!("  {value}"))
                };
                lines.push(Line::from(vec![
                    Span::styled(format!(" {name:<13}"), label_style),
                    Span::raw(" "),
                    Span::styled(value_text, Style::default().fg(BR)),
                ]));
            }
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Your labels win over classifier and heuristic labels and become gold labels for `vca classify eval`.",
                dim(),
            )));
            frame.render_widget(
                Paragraph::new(lines)
                    .wrap(Wrap { trim: false })
                    .block(panel_block(
                        &format!(
                            "Edit {} · ↑↓ field · ←→ value · Enter save · Esc cancel",
                            target.title()
                        ),
                        true,
                    )),
                rect,
            );
        }
        Popup::Classify {
            sessions,
            level,
            max_usd,
            plan,
            pending,
            error,
        } => {
            let rect = centered(area, 84, 56);
            frame.render_widget(Clear, rect);
            let text = |value: String| Span::styled(value, Style::default().fg(SU));
            let mut lines = vec![Line::from(vec![
                Span::styled(" Selection  ", Style::default().fg(CY)),
                text(format!(
                    "{} session{}",
                    sessions.len(),
                    if sessions.len() == 1 { "" } else { "s" }
                )),
            ])];
            match plan {
                Some(plan) => {
                    let unit = |kind: &str| plan.units.get(kind).copied().unwrap_or(0);
                    lines.push(Line::from(vec![
                        Span::styled(" Units      ", Style::default().fg(CY)),
                        text(format!(
                            "{} turns to segment · {} exchanges · {} tasks (labels + completion)",
                            unit("turn"),
                            unit("exchange"),
                            unit("task")
                        )),
                    ]));
                    lines.push(Line::from(vec![
                        Span::styled(" Cached     ", Style::default().fg(CY)),
                        text(format!(
                            "{} units · excluded by project rules: {}",
                            plan.cached, plan.excluded_projects
                        )),
                    ]));
                    lines.push(Line::from(vec![
                        Span::styled(" Estimate   ", Style::default().fg(CY)),
                        text(format!(
                            "~{} input tokens · ~${:.4} · run cap ${:.2} · redactions {}",
                            crate::explore::format::tokens(plan.est_tokens as i64),
                            plan.est_usd,
                            max_usd,
                            plan.redactions
                        )),
                    ]));
                }
                None => lines.push(Line::from(Span::styled(" ⟳ estimating…", dim()))),
            }
            let levels = [
                Level::EvidenceOnly,
                Level::UserText,
                Level::Conversation,
                Level::Full,
            ];
            let mut level_spans = vec![Span::styled(" Content    ", Style::default().fg(CY))];
            for candidate in levels {
                let style = if candidate == *level {
                    Style::default().fg(Color::Black).bg(CY)
                } else {
                    dim()
                };
                level_spans.push(Span::styled(format!(" {} ", candidate.as_str()), style));
                level_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(level_spans));
            lines.push(Line::from(Span::styled(
                "            code blocks stripped below `full`; secrets, emails and home paths always redacted",
                dim(),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                " Sent to OpenRouter → TypeSafe Jev (typesafe/jev-1.13) with zdr=true and fallbacks off.",
                Style::default().fg(AM),
            )));
            if *pending == Some("run") {
                lines.push(Line::from(Span::styled(
                    " ⟳ classifying… other views refresh when it finishes",
                    Style::default().fg(CY),
                )));
            }
            if let Some(error) = error {
                lines.push(Line::from(Span::styled(
                    format!(" {error}"),
                    Style::default().fg(RD),
                )));
            }
            frame.render_widget(
                Paragraph::new(lines).wrap(Wrap { trim: false }).block(panel_block(
                    "Classify with Jev · ←→ content level · +/- cap · p preview · Enter send · Esc cancel",
                    true,
                )),
                rect,
            );
        }
    }
}

const HELP: [(&str, &str); 26] = [
    (
        "1 … 7",
        "Overview · Sessions/Tasks · Groups · Usage & Quota · Delivery & Rework · Practices · Compare",
    ),
    (
        "Enter",
        "drill into the selected row (pushes a view; filters are inherited)",
    ),
    (
        "Esc / Bksp",
        "back to the previous view, exactly as you left it",
    ),
    (
        "[ / ]",
        "previous / next item of the parent list without going back",
    ),
    ("H", "home (Overview)"),
    ("j k ↑ ↓", "move · PgUp PgDn page · Home End"),
    ("Tab", "next section / panel (Shift-Tab back)"),
    ("u", "Sessions ↔ Tasks"),
    ("s", "cycle sort"),
    ("g / G", "cycle group dimension (Groups, Context & Tools)"),
    ("b", "day ↔ hour buckets (Usage timeline)"),
    ("t", "cycle time range 7d / 30d / 90d / all"),
    ("f", "filter editor"),
    ("/", "search"),
    ("m", "mark the selected session or task for comparison"),
    ("c", "compare marked rows"),
    ("x", "explain the selected number or label"),
    ("o", "Practices: open the comparison group"),
    ("M", "log a quota limit hit now (quota windows)"),
    (
        "e",
        "edit labels, task start or completion of the selected task or turn",
    ),
    (
        "K",
        "classify the selected or marked sessions with Jev (shows cost first)",
    ),
    ("R", "reload"),
    ("q", "quit"),
    ("", ""),
    (
        "Classifier",
        "Labels are heuristic (h) until you classify (K or `vca classify run`).",
    ),
    ("", ""),
];

const GLYPHS: [(&str, &str); 8] = [
    ("~$", "API list-price estimate (not an invoice)"),
    ("—", "missing / not available from this tool"),
    ("·", "not applicable"),
    ("h", "heuristic label"),
    ("?", "tentative classifier label"),
    ("✎", "your manual label"),
    ("◌", "unclassified"),
    (
        "! ≈ ◆ ▲",
        "legacy data · allocated share · limit reached/logged · usage spike",
    ),
];
