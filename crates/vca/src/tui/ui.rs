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
            Screen::Overview => {
                "Tab panel · Enter open · 1-7 screens · t time · f filter · x explain · ? help · q quit"
            }
            Screen::List { .. } => {
                "Enter open · u sessions/tasks · s sort · m mark · c compare · / search · f filter · Esc back"
            }
            Screen::Groups { .. } => {
                "g/G dimension · Enter open group · f filter · x explain · Esc back"
            }
            Screen::Detail { .. } => "Tab section · [ ] prev/next · x explain · m mark · Esc back",
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
        (Screen::Delivery, Data::Ladder(_)) => render_ladder(frame, app, area),
        (Screen::Practices, Data::Practices(_)) => render_practices(frame, app, area),
        (Screen::Compare { .. }, Data::Compare(_)) => render_compare(frame, app, area),
        _ => frame.render_widget(Paragraph::new(Span::styled("loading…", dim())), area),
    }
}

/// Table with selection and a persisted scroll offset.
fn render_table(
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
            let done = if t.completed_at.is_some() {
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
                colored(done, if t.completed_at.is_some() { SU } else { MU }),
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
    let completed = rows.iter().filter(|t| t.completed_at.is_some()).count();
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

fn render_detail(
    frame: &mut TermFrame<'_>,
    app: &App,
    detail: &SessionDetail,
    tab: DetailTab,
    area: Rect,
) {
    let s = &detail.session;
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
        .filter(|t| t.completed_at.is_some())
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
                    fmt::usd(s.cost),
                    fmt::usd(s.subagent_cost),
                    fmt::tokens(s.fresh_tokens),
                    fmt::tokens(s.cache_read_tokens),
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
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Length(7),
                    Constraint::Length(8),
                    Constraint::Length(26),
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
                        left(fmt::when(t.completed_at.as_deref())),
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
                    "First request / attempt",
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
                        right(fmt::tokens(a.fresh_tokens)),
                        right(fmt::usd(a.cost)),
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
                    .block(Block::default().title(Span::styled("API-equiv per turn", dim())))
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
                        right(fmt::tokens(m.fresh_tokens)),
                        right(fmt::tokens(m.cache_read_tokens)),
                        right(fmt::tokens(m.output_tokens)),
                        right(fmt::usd(m.cost)),
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
                .constraints([Constraint::Length(5), Constraint::Min(3)])
                .split(body);
            let sizes: Vec<u64> = detail
                .calls
                .iter()
                .map(|c| c.context_tokens.max(0) as u64)
                .collect();
            let peak = sizes.iter().max().copied().unwrap_or(0);
            let rebuilds: f64 = detail.calls.iter().filter_map(|c| c.rebuild_extra).sum();
            let compactions = detail
                .calls
                .iter()
                .filter(|c| c.cache_event == "compaction")
                .count();
            frame.render_widget(
                Sparkline::default()
                    .block(Block::default().title(Span::styled(
                        format!(
                            "context per call · peak {} · {} calls · {} compaction(s) · cache rebuilds {} extra",
                            fmt::tokens(peak as i64),
                            detail.calls.len(),
                            compactions,
                            fmt::usd(Some(rebuilds))
                        ),
                        dim(),
                    )))
                    .data(&sizes)
                    .style(Style::default().fg(AM)),
                split[0],
            );
            let rows: Vec<Row> = detail
                .context_items
                .iter()
                .map(|i| {
                    Row::new(vec![
                        left(fmt::truncate(i.tool_name.as_deref().unwrap_or(&i.kind), 18)),
                        left(fmt::truncate(i.target.as_deref().unwrap_or(""), 60)),
                        right(fmt::tokens(i.tokens)),
                        right(format!("c{}", i.entered_call)),
                        right(i.carried_calls.to_string()),
                        right(fmt::usd(i.write_usd)),
                        right(fmt::usd(
                            i.carry_usd.map(|c| c + i.rebuild_usd.unwrap_or(0.0)),
                        )),
                        right(fmt::usd(i.total_usd)),
                        left(i.purpose.clone().unwrap_or_default()),
                        left(i.instruction_ref.clone().unwrap_or_default()),
                    ])
                })
                .collect();
            render_table(
                frame,
                app,
                split[1],
                &[
                    "Item", "Target", ">Tokens", ">In", ">Carried", ">Write", ">Carry", ">Total",
                    "Purpose", "Instr.",
                ],
                &[
                    Constraint::Length(18),
                    Constraint::Min(20),
                    Constraint::Length(7),
                    Constraint::Length(5),
                    Constraint::Length(7),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(14),
                    Constraint::Length(12),
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
    let Some(Data::Ladder(rows)) = current.data.as_ref() else {
        return;
    };
    let parts = split_footer(area);
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
    }
}

const HELP: [(&str, &str); 24] = [
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
    ("R", "reload"),
    ("q", "quit"),
    ("", ""),
    (
        "Classifier",
        "Labels are heuristic (h) until you run `vca classify` with Jev.",
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
