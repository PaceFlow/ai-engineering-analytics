use std::collections::HashMap;
use std::io;
use std::time::Duration as StdDuration;

use anyhow::Result;
use chrono::{Duration, Local, NaiveDate};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table};

use crate::analytics::{
    self, ChangeReportRow, LifecycleReportRow, ReportQueryOptions, SessionReportRow,
};
use crate::cli::{GroupBy, ReportArgs, TuiArgs};
use crate::commands::report_scope;
use crate::db;

// ── PALETTE (mirrors terminal-design index.html CSS variables) ──────────────
const CY: Color = Color::Rgb(0x22, 0xd3, 0xee);
const GN: Color = Color::Rgb(0x4a, 0xde, 0x80);
const RD: Color = Color::Rgb(0xf8, 0x71, 0x71);
const AM: Color = Color::Rgb(0xfb, 0xbf, 0x24);
const BR: Color = Color::Rgb(0xf4, 0xf4, 0xf5);
const SU: Color = Color::Rgb(0xa1, 0xa1, 0xaa);
const MU: Color = Color::Rgb(0x71, 0x71, 0x7a);
const D3: Color = Color::Rgb(0x33, 0x33, 0x38);
const D4: Color = Color::Rgb(0x46, 0x46, 0x4f);
const D5: Color = Color::Rgb(0x5c, 0x5c, 0x66);
const SEL_BG: Color = Color::Rgb(0x0e, 0x2a, 0x30);

const GROUPS: [GroupBy; 4] = [
    GroupBy::Model,
    GroupBy::Provider,
    GroupBy::Task,
    GroupBy::Branch,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Verdict,
    Sessions,
    Delivery,
    Quality,
}

impl Tab {
    const ALL: [Tab; 4] = [Tab::Verdict, Tab::Sessions, Tab::Delivery, Tab::Quality];

    fn index(self) -> usize {
        match self {
            Tab::Verdict => 0,
            Tab::Sessions => 1,
            Tab::Delivery => 2,
            Tab::Quality => 3,
        }
    }

    fn from_digit(digit: u32) -> Option<Self> {
        match digit {
            1 => Some(Tab::Verdict),
            2 => Some(Tab::Sessions),
            3 => Some(Tab::Delivery),
            4 => Some(Tab::Quality),
            _ => None,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Tab::Verdict => "Verdict",
            Tab::Sessions => "Sessions",
            Tab::Delivery => "Delivery",
            Tab::Quality => "Quality",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Window {
    D7,
    D30,
    D90,
}

impl Window {
    const ALL: [Window; 3] = [Window::D7, Window::D30, Window::D90];

    fn days(self) -> i64 {
        match self {
            Window::D7 => 7,
            Window::D30 => 30,
            Window::D90 => 90,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Window::D7 => "7d",
            Window::D30 => "30d",
            Window::D90 => "90d",
        }
    }

    fn next(self) -> Self {
        match self {
            Window::D7 => Window::D30,
            Window::D30 => Window::D90,
            Window::D90 => Window::D7,
        }
    }
}

#[derive(Debug, Clone)]
struct StatDelta {
    value: String,
    delta: String,
    color: Color,
}

#[derive(Debug, Clone)]
struct VerdictStats {
    sessions: StatDelta,
    first_chg: StatDelta,
    mainline: StatDelta,
    bug_rate: StatDelta,
}

#[derive(Debug, Clone)]
struct VerdictRow {
    label: String,
    first_chg: Option<f64>,
    loop_rate: Option<f64>,
    mainline: Option<f64>,
    bug_rate: Option<f64>,
}

#[derive(Default)]
struct Reports {
    session: Vec<SessionReportRow>,
    delivery: Vec<ChangeReportRow>,
    quality: Vec<LifecycleReportRow>,
    delivery_baseline: Option<ChangeReportRow>,
    quality_baseline: Option<LifecycleReportRow>,
    stats: Option<VerdictStats>,
}

struct App {
    base_args: ReportArgs,
    tab: Tab,
    group_idx: usize,
    window: Window,
    selected: usize,
    legend: bool,
    reports: Reports,
    message: Option<String>,
    should_quit: bool,
}

impl App {
    fn new(args: TuiArgs) -> Self {
        let base = ReportArgs {
            weekly: false,
            group_by: Some(GroupBy::Model),
            from: None,
            to: None,
            repo: None,
            all_projects: args.all_projects,
            provider: None,
            task: None,
            branch: None,
            model: None,
            limit: args.limit,
        };
        let base_args = report_scope::resolve_report_args(&base);
        Self {
            base_args,
            tab: Tab::Verdict,
            group_idx: 0,
            window: Window::D30,
            selected: 0,
            legend: false,
            reports: Reports::default(),
            message: None,
            should_quit: false,
        }
    }

    fn group(&self) -> GroupBy {
        GROUPS[self.group_idx]
    }

    /// Args for the current window, optionally overriding group-by / provider.
    fn scoped_args(
        &self,
        today: NaiveDate,
        group: Option<GroupBy>,
        provider: Option<String>,
    ) -> ReportArgs {
        let days = self.window.days();
        let to = today;
        let from = today - Duration::days(days - 1);
        let mut args = self.base_args.clone();
        args.from = Some(from.to_string());
        args.to = Some(to.to_string());
        args.weekly = false;
        args.group_by = group;
        args.provider = provider;
        args.task = None;
        args.branch = None;
        args.model = None;
        args
    }

    /// Args for the window immediately preceding the current one (for deltas).
    fn prev_args(&self, today: NaiveDate, group: Option<GroupBy>) -> ReportArgs {
        let days = self.window.days();
        let cur_from = today - Duration::days(days - 1);
        let prev_to = cur_from - Duration::days(1);
        let prev_from = prev_to - Duration::days(days - 1);
        let mut args = self.base_args.clone();
        args.from = Some(prev_from.to_string());
        args.to = Some(prev_to.to_string());
        args.weekly = false;
        args.group_by = group;
        args.provider = None;
        args.task = None;
        args.branch = None;
        args.model = None;
        args
    }

    fn reload(&mut self) -> Result<()> {
        let db = db::open()?;
        analytics::create_reporting_views(&db)?;
        let today = Local::now().date_naive();
        let group = self.group();
        let grouped = ReportQueryOptions {
            implicit_model_default: group == GroupBy::Model,
        };
        let overall = ReportQueryOptions {
            implicit_model_default: false,
        };

        let cur = self.scoped_args(today, Some(group), None);
        self.reports.session = analytics::query_session_report_with_options(&db, &cur, grouped)?;
        self.reports.delivery = analytics::query_change_report_with_options(&db, &cur, grouped)?;
        self.reports.quality = analytics::query_lifecycle_report_with_options(&db, &cur, grouped)?;

        let cur_all = self.scoped_args(today, None, None);
        let session_cur = analytics::query_session_report_with_options(&db, &cur_all, overall)?;
        let delivery_cur = analytics::query_change_report_with_options(&db, &cur_all, overall)?;
        let quality_cur = analytics::query_lifecycle_report_with_options(&db, &cur_all, overall)?;

        let prev_all = self.prev_args(today, None);
        let session_prev = analytics::query_session_report_with_options(&db, &prev_all, overall)?;
        let delivery_prev = analytics::query_change_report_with_options(&db, &prev_all, overall)?;
        let quality_prev = analytics::query_lifecycle_report_with_options(&db, &prev_all, overall)?;

        let baseline = self.scoped_args(today, None, Some("human".to_string()));
        self.reports.delivery_baseline =
            analytics::query_change_report_with_options(&db, &baseline, overall)?
                .into_iter()
                .next();
        self.reports.quality_baseline =
            analytics::query_lifecycle_report_with_options(&db, &baseline, overall)?
                .into_iter()
                .next();

        self.reports.stats = Some(compute_stats(
            self.window,
            session_cur.first(),
            delivery_cur.first(),
            quality_cur.first(),
            session_prev.first(),
            delivery_prev.first(),
            quality_prev.first(),
        ));

        let len = self.current_len();
        self.selected = self.selected.min(len.saturating_sub(1));
        Ok(())
    }

    fn verdict_rows(&self) -> Vec<VerdictRow> {
        let group = self.group();
        let mut sessions: HashMap<String, (Option<f64>, Option<f64>)> = HashMap::new();
        for row in &self.reports.session {
            sessions.insert(
                row_label(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                ),
                (
                    row.avg_minutes_to_first_accepted_change,
                    row.debug_loop_rate.percent(),
                ),
            );
        }
        let mut quality: HashMap<String, Option<f64>> = HashMap::new();
        for row in &self.reports.quality {
            quality.insert(
                row_label(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                ),
                row.bug_after_merge_rate.percent(),
            );
        }
        let mut rows: Vec<VerdictRow> = self
            .reports
            .delivery
            .iter()
            .map(|row| {
                let label = row_label(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                );
                let (first_chg, loop_rate) = sessions.get(&label).copied().unwrap_or((None, None));
                let bug_rate = quality.get(&label).copied().flatten();
                VerdictRow {
                    label,
                    first_chg,
                    loop_rate,
                    mainline: row.merge_rate.percent(),
                    bug_rate,
                }
            })
            .collect();
        rows.sort_by(|a, b| {
            b.mainline
                .partial_cmp(&a.mainline)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        rows
    }

    /// Reloads, capturing any query error into a status message instead of
    /// propagating it (which would tear down the whole TUI mid-session).
    fn reload_safe(&mut self) {
        match self.reload() {
            Ok(()) => self.message = None,
            Err(err) => self.message = Some(format!("reload failed: {err}")),
        }
    }

    fn current_len(&self) -> usize {
        match self.tab {
            Tab::Verdict => self.verdict_rows().len(),
            Tab::Sessions => self.reports.session.len(),
            Tab::Delivery => self.reports.delivery.len(),
            Tab::Quality => self.reports.quality.len(),
        }
    }

    fn on_key(&mut self, key: event::KeyEvent) -> Result<()> {
        if key.kind != KeyEventKind::Press {
            return Ok(());
        }
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char(ch @ '1'..='4') => {
                if let Some(tab) = ch.to_digit(10).and_then(Tab::from_digit) {
                    self.tab = tab;
                    self.selected = 0;
                    self.legend = false;
                }
            }
            KeyCode::Char('g') => {
                self.group_idx = (self.group_idx + 1) % GROUPS.len();
                self.selected = 0;
                self.reload_safe();
            }
            KeyCode::Char('w') => {
                self.window = self.window.next();
                self.selected = 0;
                self.reload_safe();
            }
            KeyCode::Char('l') | KeyCode::Char('L') => self.legend = !self.legend,
            KeyCode::Char('r') => self.reload_safe(),
            KeyCode::Esc => {
                if self.legend {
                    self.legend = false;
                } else {
                    self.message = None;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(self.current_len().saturating_sub(1));
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
            }
            _ => {}
        }
        Ok(())
    }
}

pub fn run(args: TuiArgs) -> Result<()> {
    let mut app = App::new(args);
    app.reload()?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_app(&mut terminal, &mut app);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &mut App) -> Result<()> {
    while !app.should_quit {
        terminal.draw(|frame| draw(frame, app))?;
        if event::poll(StdDuration::from_millis(200))?
            && let Event::Key(key) = event::read()?
        {
            app.on_key(key)?;
        }
    }
    Ok(())
}

// ── RENDERING ───────────────────────────────────────────────────────────────
fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // header
            Constraint::Length(1), // spacer
            Constraint::Length(1), // tabs
            Constraint::Length(1), // spacer
            Constraint::Length(1), // control bar
            Constraint::Length(1), // spacer
            Constraint::Min(6),    // body
            Constraint::Length(1), // footer
        ])
        .split(area);
    render_header(frame, app, chunks[0]);
    render_tabs(frame, app, chunks[2]);
    render_control(frame, app, chunks[4]);
    let body = chunks[6];
    match app.tab {
        Tab::Verdict => render_verdict(frame, app, body),
        Tab::Sessions => render_sessions(frame, app, body),
        Tab::Delivery => render_delivery(frame, app, body),
        Tab::Quality => render_quality(frame, app, body),
    }
    render_footer(frame, app.message.as_deref(), chunks[7]);
    if app.legend {
        render_legend(frame, app, centered_rect(86, 92, area));
    }
}

fn render_header(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(10), Constraint::Length(7)])
        .split(area);
    let left = Line::from(vec![
        Span::styled(
            "AI Engineering Analytics",
            Style::default().fg(BR).add_modifier(Modifier::BOLD),
        ),
        Span::styled("   ", Style::default()),
        Span::styled(group_label(app.group()), Style::default().fg(SU)),
        Span::styled(" · ", Style::default().fg(D4)),
        Span::styled(app.window.label(), Style::default().fg(SU)),
    ]);
    frame.render_widget(Paragraph::new(left), cols[0]);
    let time = Local::now().format("%H:%M").to_string();
    frame.render_widget(
        Paragraph::new(Span::styled(time, Style::default().fg(D4))).alignment(Alignment::Right),
        cols[1],
    );
}

fn render_tabs(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let mut spans = Vec::new();
    for tab in Tab::ALL {
        let on = tab == app.tab;
        let num = format!("[{}]", tab.index() + 1);
        let (num_color, label_color, modifier) = if on {
            (CY, CY, Modifier::BOLD)
        } else {
            (D4, MU, Modifier::empty())
        };
        spans.push(Span::styled(num, Style::default().fg(num_color)));
        spans.push(Span::styled(
            format!(" {}  ", tab.title()),
            Style::default().fg(label_color).add_modifier(modifier),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_control(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let mut spans = vec![Span::styled("group ", Style::default().fg(D5))];
    for (idx, group) in GROUPS.iter().enumerate() {
        spans.push(pill(group_label(*group), idx == app.group_idx));
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled("  ·  ", Style::default().fg(D3)));
    spans.push(Span::styled("window ", Style::default().fg(D5)));
    for window in Window::ALL {
        spans.push(pill(window.label(), window == app.window));
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled("   L legend", Style::default().fg(D5)));
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn pill(text: &str, active: bool) -> Span<'static> {
    if active {
        Span::styled(
            format!(" {text} "),
            Style::default()
                .fg(CY)
                .bg(SEL_BG)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(format!(" {text} "), Style::default().fg(MU))
    }
}

fn render_footer(frame: &mut ratatui::Frame<'_>, message: Option<&str>, area: Rect) {
    if let Some(message) = message {
        frame.render_widget(
            Paragraph::new(Span::styled(
                format!("⚠ {message}  (Esc to dismiss)"),
                Style::default().fg(RD),
            )),
            area,
        );
        return;
    }
    let key = |k: &'static str| Span::styled(k, Style::default().fg(CY));
    let sep = || Span::styled("  ·  ", Style::default().fg(D3));
    let lbl = |t: &'static str| Span::styled(t, Style::default().fg(MU));
    let line = Line::from(vec![
        key("1-4"),
        Span::raw(" "),
        lbl("tab"),
        sep(),
        key("↑↓"),
        Span::raw(" "),
        lbl("row"),
        sep(),
        key("g"),
        Span::raw(" "),
        lbl("group"),
        sep(),
        key("w"),
        Span::raw(" "),
        lbl("window"),
        sep(),
        key("L"),
        Span::raw(" "),
        lbl("legend"),
        sep(),
        key("q"),
        Span::raw(" "),
        lbl("quit"),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_verdict(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.verdict_rows();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // statement
            Constraint::Length(1), // spacer
            Constraint::Length(2), // sub statement
            Constraint::Length(1), // spacer
            Constraint::Length(2), // stat strip
            Constraint::Length(1), // spacer
            Constraint::Min(4),    // table
            Constraint::Length(2), // insight
        ])
        .split(area);

    let (statement, sub) = verdict_narrative(&rows, app.reports.delivery_baseline.as_ref());
    frame.render_widget(
        Paragraph::new(Span::styled(statement, Style::default().fg(BR))),
        chunks[0],
    );
    frame.render_widget(
        Paragraph::new(Span::styled(sub, Style::default().fg(MU)))
            .wrap(ratatui::widgets::Wrap { trim: true }),
        chunks[2],
    );
    render_stat_strip(frame, app, chunks[4]);

    let header = Row::new(vec![
        header_cell(""),
        header_cell("First Chg"),
        header_cell("Debug Loop"),
        Cell::from(Span::styled("Mainline ↓", Style::default().fg(CY))),
        header_cell("Bug Rate"),
    ]);
    let mut table_rows: Vec<Row> = rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let style = selection_style(idx == app.selected);
            Row::new(vec![
                Cell::from(Span::styled(
                    truncate(&row.label, 26),
                    Style::default().fg(if idx == app.selected { BR } else { SU }),
                )),
                cell_value(fmt_minsec(row.first_chg), color_first_chg(row.first_chg)),
                cell_value(fmt_pct(row.loop_rate), color_loop(row.loop_rate)),
                cell_value(fmt_pct(row.mainline), color_mainline(row.mainline)),
                cell_value(fmt_pct(row.bug_rate), color_bug(row.bug_rate)),
            ])
            .style(style)
            .bottom_margin(1)
        })
        .collect();
    if let Some(base) = app.reports.delivery_baseline.as_ref() {
        let bug = app
            .reports
            .quality_baseline
            .as_ref()
            .and_then(|q| q.bug_after_merge_rate.percent());
        table_rows.push(baseline_row(vec![
            Cell::from(Span::styled("you · no-AI", Style::default().fg(D5))),
            dash_cell(),
            dash_cell(),
            cell_value(
                fmt_pct(base.merge_rate.percent()),
                color_mainline(base.merge_rate.percent()),
            ),
            cell_value(fmt_pct(bug), color_bug(bug)),
        ]));
    }
    let widths = [
        Constraint::Min(20),
        Constraint::Length(11),
        Constraint::Length(11),
        Constraint::Length(11),
        Constraint::Length(10),
    ];
    if rows.is_empty() && app.reports.delivery_baseline.is_none() {
        frame.render_widget(empty_paragraph(), chunks[6]);
    } else {
        frame.render_widget(
            Table::new(table_rows, widths).header(spaced_header(header)),
            chunks[6],
        );
    }
    frame.render_widget(insight(verdict_insight(&rows, app)), chunks[7]);
}

fn render_stat_strip(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let Some(stats) = app.reports.stats.as_ref() else {
        return;
    };
    let items = [
        (&stats.sessions, "sessions"),
        (&stats.first_chg, "first chg"),
        (&stats.mainline, "mainline"),
        (&stats.bug_rate, "bug rate"),
    ];
    let mut spans = Vec::new();
    for (stat, label) in items {
        spans.push(Span::styled(
            stat.value.clone(),
            Style::default().fg(BR).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            stat.delta.clone(),
            Style::default().fg(stat.color),
        ));
        spans.push(Span::styled(
            format!(" {label}    "),
            Style::default().fg(D5),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_sessions(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let chunks = body_chunks(area);
    let header = Row::new(vec![
        header_cell(""),
        header_cell("Sessions"),
        header_cell("Avg Prompts"),
        header_cell("First Chg"),
        header_cell("Loop %"),
        header_cell("Error %"),
        header_cell("To Commit %"),
        header_cell("No Output %"),
    ]);
    let group = app.group();
    let table_rows: Vec<Row> = app
        .reports
        .session
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let style = selection_style(idx == app.selected);
            Row::new(vec![
                label_cell(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                    idx == app.selected,
                ),
                cell_value(row.session_count.to_string(), SU),
                cell_value(fmt_opt(row.s2_avg), SU),
                cell_value(
                    fmt_opt(row.avg_minutes_to_first_accepted_change),
                    color_first_chg(row.avg_minutes_to_first_accepted_change),
                ),
                cell_value(
                    fmt_pct(row.debug_loop_rate.percent()),
                    color_loop(row.debug_loop_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.s6_rate.percent()),
                    color_error(row.s6_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.s9_rate.percent()),
                    color_to_commit(row.s9_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.no_output_session_rate.percent()),
                    color_no_output(row.no_output_session_rate.percent()),
                ),
            ])
            .style(style)
            .bottom_margin(1)
        })
        .collect();
    let widths = [
        Constraint::Min(18),
        Constraint::Length(9),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(12),
        Constraint::Length(12),
    ];
    if table_rows.is_empty() {
        frame.render_widget(empty_paragraph(), chunks[0]);
    } else {
        frame.render_widget(
            Table::new(table_rows, widths).header(spaced_header(header)),
            chunks[0],
        );
    }
    frame.render_widget(insight(sessions_insight(app)), chunks[1]);
}

fn render_delivery(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let chunks = body_chunks(area);
    let header = Row::new(vec![
        header_cell(""),
        header_cell("Commits"),
        header_cell("Heavy"),
        header_cell("PR sync"),
        header_cell("PR Reach %"),
        header_cell("Mainline %"),
        header_cell("PR Merge %"),
    ]);
    let group = app.group();
    let mut table_rows: Vec<Row> = app
        .reports
        .delivery
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let pr_merge = if row.github_pr_metrics_available {
                cell_value(
                    fmt_pct(row.pr_merge_rate.percent()),
                    color_pr_merge(row.pr_merge_rate.percent()),
                )
            } else {
                dash_cell()
            };
            let pr_sync = if row.github_pr_heavy_eligible > 0 {
                cell_value(
                    format!(
                        "{}/{}",
                        row.github_pr_heavy_ready, row.github_pr_heavy_eligible
                    ),
                    D5,
                )
            } else {
                dash_cell()
            };
            Row::new(vec![
                label_cell(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                    idx == app.selected,
                ),
                cell_value(row.commit_count.to_string(), SU),
                cell_value(row.heavy_commit_count.to_string(), SU),
                pr_sync,
                cell_value(
                    fmt_pct(row.pr_reach_rate.percent()),
                    color_pr_reach(row.pr_reach_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.merge_rate.percent()),
                    color_mainline(row.merge_rate.percent()),
                ),
                pr_merge,
            ])
            .style(selection_style(idx == app.selected))
            .bottom_margin(1)
        })
        .collect();
    if let Some(base) = app.reports.delivery_baseline.as_ref() {
        table_rows.push(baseline_row(vec![
            Cell::from(Span::styled("you · no-AI", Style::default().fg(D5))),
            cell_value(base.commit_count.to_string(), D4),
            dash_cell(),
            dash_cell(),
            cell_value(
                fmt_pct(base.pr_reach_rate.percent()),
                color_pr_reach(base.pr_reach_rate.percent()),
            ),
            cell_value(
                fmt_pct(base.merge_rate.percent()),
                color_mainline(base.merge_rate.percent()),
            ),
            dash_cell(),
        ]));
    }
    let widths = [
        Constraint::Min(18),
        Constraint::Length(9),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(11),
        Constraint::Length(11),
        Constraint::Length(11),
    ];
    if app.reports.delivery.is_empty() && app.reports.delivery_baseline.is_none() {
        frame.render_widget(empty_paragraph(), chunks[0]);
    } else {
        frame.render_widget(
            Table::new(table_rows, widths).header(spaced_header(header)),
            chunks[0],
        );
    }
    frame.render_widget(insight(delivery_insight(app)), chunks[1]);
}

fn render_quality(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let chunks = body_chunks(area);
    let header = Row::new(vec![
        header_cell(""),
        header_cell("Heavy"),
        header_cell("Churn %"),
        header_cell("Bug Rate %"),
        header_cell("Revert %"),
    ]);
    let group = app.group();
    let mut table_rows: Vec<Row> = app
        .reports
        .quality
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            Row::new(vec![
                label_cell(
                    group,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                    idx == app.selected,
                ),
                cell_value(row.heavy_commit_count.to_string(), SU),
                cell_value(
                    fmt_pct(row.code_churn_rate.percent()),
                    color_churn(row.code_churn_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.bug_after_merge_rate.percent()),
                    color_bug(row.bug_after_merge_rate.percent()),
                ),
                cell_value(
                    fmt_pct(row.revert_rate.percent()),
                    color_revert(row.revert_rate.percent()),
                ),
            ])
            .style(selection_style(idx == app.selected))
            .bottom_margin(1)
        })
        .collect();
    if let Some(base) = app.reports.quality_baseline.as_ref() {
        table_rows.push(baseline_row(vec![
            Cell::from(Span::styled("you · no-AI", Style::default().fg(D5))),
            dash_cell(),
            cell_value(
                fmt_pct(base.code_churn_rate.percent()),
                color_churn(base.code_churn_rate.percent()),
            ),
            cell_value(
                fmt_pct(base.bug_after_merge_rate.percent()),
                color_bug(base.bug_after_merge_rate.percent()),
            ),
            cell_value(
                fmt_pct(base.revert_rate.percent()),
                color_revert(base.revert_rate.percent()),
            ),
        ]));
    }
    let widths = [
        Constraint::Min(18),
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(11),
        Constraint::Length(10),
    ];
    if app.reports.quality.is_empty() && app.reports.quality_baseline.is_none() {
        frame.render_widget(empty_paragraph(), chunks[0]);
    } else {
        frame.render_widget(
            Table::new(table_rows, widths).header(spaced_header(header)),
            chunks[0],
        );
    }
    frame.render_widget(insight(quality_insight(app)), chunks[1]);
}

fn render_legend(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    frame.render_widget(Clear, area);
    let entries = legend_entries(app.tab);
    let mut lines = Vec::new();
    for (idx, (key, value)) in entries.iter().enumerate() {
        lines.push(Line::from(vec![
            Span::styled(format!("{key:<14}"), Style::default().fg(CY)),
            Span::styled(*value, Style::default().fg(MU)),
        ]));
        if idx + 1 < entries.len() {
            lines.push(Line::from(""));
        }
    }
    let title = format!(
        "Legend · {} · how each metric is calculated",
        app.tab.title()
    );
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(D4))
                    .title(title),
            )
            .wrap(ratatui::widgets::Wrap { trim: true }),
        area,
    );
}

// ── NARRATIVE / INSIGHTS ─────────────────────────────────────────────────────
fn verdict_narrative(rows: &[VerdictRow], baseline: Option<&ChangeReportRow>) -> (String, String) {
    let ranked: Vec<&VerdictRow> = rows.iter().filter(|r| r.mainline.is_some()).collect();
    if ranked.is_empty() {
        return (
            "No data yet for this window.".to_string(),
            "Run `paceflow ingest` or widen the window to populate the verdict.".to_string(),
        );
    }
    let leader = ranked[0];
    let statement = format!(
        "{} leads mainline reach — {} of AI-heavy commits reaching main.",
        leader.label,
        fmt_pct(leader.mainline)
    );
    let laggard = ranked[ranked.len() - 1];
    let sub = if ranked.len() > 1 {
        format!(
            "{} is the weakest group at {} mainline reach and {} bug rate.",
            laggard.label,
            fmt_pct(laggard.mainline),
            fmt_pct(laggard.bug_rate)
        )
    } else {
        let base = baseline
            .and_then(|b| b.merge_rate.percent())
            .map(|p| fmt_pct(Some(p)))
            .unwrap_or_else(|| "N/A".to_string());
        format!("Your no-AI mainline baseline is {base}.")
    };
    (statement, sub)
}

fn verdict_insight(rows: &[VerdictRow], app: &App) -> String {
    let leader = rows.iter().find(|r| r.mainline.is_some());
    let baseline = app
        .reports
        .delivery_baseline
        .as_ref()
        .and_then(|b| b.merge_rate.percent());
    match (leader, baseline) {
        (Some(leader), Some(base)) => {
            let leader_ml = leader.mainline.unwrap_or(0.0);
            let diff = leader_ml - base;
            if diff >= 0.0 {
                format!(
                    "{} is {:.0}pp above your no-AI mainline baseline of {:.0}%.",
                    leader.label, diff, base
                )
            } else {
                format!(
                    "Even the leader {} sits {:.0}pp below your no-AI baseline of {:.0}%.",
                    leader.label,
                    diff.abs(),
                    base
                )
            }
        }
        (Some(leader), None) => format!(
            "{} leads, but no no-AI baseline is available for comparison.",
            leader.label
        ),
        _ => "No groups to rank for this window.".to_string(),
    }
}

fn sessions_insight(app: &App) -> String {
    let group = app.group();
    let worst = app.reports.session.iter().max_by(|a, b| {
        a.no_output_session_rate
            .percent()
            .unwrap_or(0.0)
            .partial_cmp(&b.no_output_session_rate.percent().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    match worst {
        Some(row) => format!(
            "{} has the highest no-output rate at {}, with a {} error-paste rate.",
            row_label(
                group,
                row.group_value.as_deref(),
                row.branch_name.as_deref()
            ),
            fmt_pct(row.no_output_session_rate.percent()),
            fmt_pct(row.s6_rate.percent())
        ),
        None => "No session rows for this window.".to_string(),
    }
}

fn delivery_insight(app: &App) -> String {
    let group = app.group();
    let best = app.reports.delivery.iter().max_by(|a, b| {
        a.merge_rate
            .percent()
            .unwrap_or(0.0)
            .partial_cmp(&b.merge_rate.percent().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let baseline = app
        .reports
        .delivery_baseline
        .as_ref()
        .and_then(|b| b.merge_rate.percent());
    match (best, baseline) {
        (Some(row), Some(base)) => format!(
            "{} leads mainline reach at {} vs a no-AI baseline of {:.0}%.",
            row_label(
                group,
                row.group_value.as_deref(),
                row.branch_name.as_deref()
            ),
            fmt_pct(row.merge_rate.percent()),
            base
        ),
        (Some(row), None) => format!(
            "{} leads mainline reach at {}.",
            row_label(
                group,
                row.group_value.as_deref(),
                row.branch_name.as_deref()
            ),
            fmt_pct(row.merge_rate.percent())
        ),
        _ => "No delivery rows for this window.".to_string(),
    }
}

fn quality_insight(app: &App) -> String {
    let group = app.group();
    let worst = app.reports.quality.iter().max_by(|a, b| {
        a.bug_after_merge_rate
            .percent()
            .unwrap_or(0.0)
            .partial_cmp(&b.bug_after_merge_rate.percent().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    match worst {
        Some(row) => format!(
            "{} carries the highest bug-after-merge rate at {}, churn {}.",
            row_label(
                group,
                row.group_value.as_deref(),
                row.branch_name.as_deref()
            ),
            fmt_pct(row.bug_after_merge_rate.percent()),
            fmt_pct(row.code_churn_rate.percent())
        ),
        None => "No quality rows for this window.".to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_stats(
    window: Window,
    session_cur: Option<&SessionReportRow>,
    delivery_cur: Option<&ChangeReportRow>,
    quality_cur: Option<&LifecycleReportRow>,
    session_prev: Option<&SessionReportRow>,
    delivery_prev: Option<&ChangeReportRow>,
    quality_prev: Option<&LifecycleReportRow>,
) -> VerdictStats {
    let sessions_cur = session_cur.map(|r| r.session_count).unwrap_or(0);
    let sessions_prev = session_prev.map(|r| r.session_count).unwrap_or(0);
    let first_cur = session_cur.and_then(|r| r.avg_minutes_to_first_accepted_change);
    let first_prev = session_prev.and_then(|r| r.avg_minutes_to_first_accepted_change);
    let mainline_cur = delivery_cur.and_then(|r| r.merge_rate.percent());
    let mainline_prev = delivery_prev.and_then(|r| r.merge_rate.percent());
    let bug_cur = quality_cur.and_then(|r| r.bug_after_merge_rate.percent());
    let bug_prev = quality_prev.and_then(|r| r.bug_after_merge_rate.percent());

    VerdictStats {
        sessions: StatDelta {
            value: sessions_cur.to_string(),
            ..count_delta(sessions_cur, sessions_prev, window)
        },
        first_chg: StatDelta {
            value: fmt_minsec(first_cur),
            ..minsec_delta(first_cur, first_prev)
        },
        mainline: StatDelta {
            value: fmt_pct(mainline_cur),
            ..pp_delta(mainline_cur, mainline_prev, false)
        },
        bug_rate: StatDelta {
            value: fmt_pct(bug_cur),
            ..pp_delta(bug_cur, bug_prev, true)
        },
    }
}

fn count_delta(cur: i64, prev: i64, window: Window) -> StatDelta {
    let diff = cur - prev;
    let arrow = if diff >= 0 { '▲' } else { '▼' };
    let color = if diff == 0 {
        D5
    } else if diff > 0 {
        GN
    } else {
        RD
    };
    let sign = if diff >= 0 { "+" } else { "−" };
    StatDelta {
        value: String::new(),
        delta: format!("{arrow} {sign}{} vs prev {}", diff.abs(), window.label()),
        color,
    }
}

fn minsec_delta(cur: Option<f64>, prev: Option<f64>) -> StatDelta {
    match (cur, prev) {
        (Some(c), Some(p)) => {
            let diff = ((c - p) * 60.0).round() as i64;
            let arrow = if diff <= 0 { '▼' } else { '▲' };
            let color = if diff == 0 {
                D5
            } else if diff < 0 {
                GN
            } else {
                RD
            };
            let abs = diff.abs();
            let sign = if diff < 0 { "−" } else { "+" };
            StatDelta {
                value: String::new(),
                delta: format!("{arrow} {sign}{}m{:02}s", abs / 60, abs % 60),
                color,
            }
        }
        _ => StatDelta {
            value: String::new(),
            delta: "—".to_string(),
            color: D5,
        },
    }
}

fn pp_delta(cur: Option<f64>, prev: Option<f64>, lower_is_better: bool) -> StatDelta {
    match (cur, prev) {
        (Some(c), Some(p)) => {
            let diff = c - p;
            let up = diff >= 0.0;
            let arrow = if up { '▲' } else { '▼' };
            let improved = if lower_is_better { !up } else { up };
            let color = if diff.abs() < 0.05 {
                D5
            } else if improved {
                GN
            } else {
                RD
            };
            let sign = if up { "+" } else { "−" };
            StatDelta {
                value: String::new(),
                delta: format!("{arrow} {sign}{:.0}pp", diff.abs()),
                color,
            }
        }
        _ => StatDelta {
            value: String::new(),
            delta: "—".to_string(),
            color: D5,
        },
    }
}

// ── THRESHOLD COLORS (mirrors the COL{} map in index.html) ───────────────────
fn color_first_chg(v: Option<f64>) -> Color {
    match v {
        None => D4,
        Some(x) if x <= 3.0 => GN,
        Some(x) if x <= 6.0 => AM,
        _ => RD,
    }
}

fn color_loop(v: Option<f64>) -> Color {
    threshold_low(v, 2.0, 6.0)
}

fn color_mainline(v: Option<f64>) -> Color {
    threshold_high(v, 70.0, 55.0)
}

fn color_bug(v: Option<f64>) -> Color {
    match v {
        None => D4,
        Some(x) if x <= 0.0 => GN,
        Some(x) if x <= 8.0 => AM,
        _ => RD,
    }
}

fn color_error(v: Option<f64>) -> Color {
    threshold_low(v, 10.0, 20.0)
}

fn color_to_commit(v: Option<f64>) -> Color {
    threshold_high(v, 70.0, 50.0)
}

fn color_no_output(v: Option<f64>) -> Color {
    threshold_low(v, 8.0, 18.0)
}

fn color_pr_reach(v: Option<f64>) -> Color {
    threshold_high(v, 80.0, 60.0)
}

fn color_pr_merge(v: Option<f64>) -> Color {
    threshold_high(v, 70.0, 50.0)
}

fn color_churn(v: Option<f64>) -> Color {
    threshold_low(v, 5.0, 15.0)
}

fn color_revert(v: Option<f64>) -> Color {
    threshold_low(v, 3.0, 10.0)
}

/// Lower values are better: <= good -> green, <= warn -> amber, else red.
fn threshold_low(v: Option<f64>, good: f64, warn: f64) -> Color {
    match v {
        None => D4,
        Some(x) if x <= good => GN,
        Some(x) if x <= warn => AM,
        _ => RD,
    }
}

/// Higher values are better: >= good -> green, >= warn -> amber, else red.
fn threshold_high(v: Option<f64>, good: f64, warn: f64) -> Color {
    match v {
        None => D4,
        Some(x) if x >= good => GN,
        Some(x) if x >= warn => AM,
        _ => RD,
    }
}

// ── CELL / ROW HELPERS ───────────────────────────────────────────────────────
fn header_cell(text: &'static str) -> Cell<'static> {
    Cell::from(Span::styled(text, Style::default().fg(MU)))
}

fn cell_value(text: String, color: Color) -> Cell<'static> {
    Cell::from(Span::styled(text, Style::default().fg(color)))
}

fn dash_cell() -> Cell<'static> {
    Cell::from(Span::styled("—", Style::default().fg(D4)))
}

fn label_cell(
    group: GroupBy,
    group_value: Option<&str>,
    branch_name: Option<&str>,
    selected: bool,
) -> Cell<'static> {
    let label = row_label(group, group_value, branch_name);
    let color = if selected { BR } else { SU };
    Cell::from(Span::styled(
        truncate(&label, 26),
        Style::default().fg(color),
    ))
}

fn selection_style(selected: bool) -> Style {
    if selected {
        Style::default().bg(SEL_BG)
    } else {
        Style::default()
    }
}

fn baseline_row(cells: Vec<Cell<'static>>) -> Row<'static> {
    Row::new(cells)
        .style(Style::default().fg(D4))
        .bottom_margin(1)
}

/// Adds a blank line under the header row so it isn't flush against the data.
fn spaced_header(header: Row<'static>) -> Row<'static> {
    header.bottom_margin(1)
}

fn insight(text: String) -> Paragraph<'static> {
    Paragraph::new(Line::from(vec![
        Span::styled("● ", Style::default().fg(CY)),
        Span::styled(text, Style::default().fg(MU)),
    ]))
    .wrap(ratatui::widgets::Wrap { trim: true })
}

fn empty_paragraph() -> Paragraph<'static> {
    Paragraph::new(Span::styled(
        "No rows found. Run `paceflow ingest` first or widen the window.",
        Style::default().fg(MU),
    ))
}

fn body_chunks(area: Rect) -> std::rc::Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(4), Constraint::Length(2)])
        .split(area)
}

// ── FORMATTING ───────────────────────────────────────────────────────────────
fn row_label(group: GroupBy, group_value: Option<&str>, branch_name: Option<&str>) -> String {
    if group == GroupBy::Branch {
        branch_name.or(group_value).unwrap_or("(all)").to_string()
    } else {
        group_value.unwrap_or("(all)").to_string()
    }
}

fn group_label(group: GroupBy) -> &'static str {
    match group {
        GroupBy::Repo => "repo",
        GroupBy::Provider => "provider",
        GroupBy::Task => "task",
        GroupBy::Branch => "branch",
        GroupBy::Model => "model",
    }
}

fn fmt_pct(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.0}%"))
        .unwrap_or_else(|| "N/A".to_string())
}

fn fmt_opt(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.1}"))
        .unwrap_or_else(|| "N/A".to_string())
}

fn fmt_minsec(value: Option<f64>) -> String {
    match value {
        Some(m) if m.is_finite() && m >= 0.0 => {
            let total = (m * 60.0).round() as i64;
            format!("{}m{:02}", total / 60, total % 60)
        }
        _ => "—".to_string(),
    }
}

fn truncate(input: &str, max_len: usize) -> String {
    if input.chars().count() <= max_len {
        return input.to_string();
    }
    if max_len <= 3 {
        return "...".to_string();
    }
    let mut out = input.chars().take(max_len - 3).collect::<String>();
    out.push_str("...");
    out
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn legend_entries(tab: Tab) -> &'static [(&'static str, &'static str)] {
    match tab {
        Tab::Verdict => &[
            (
                "First Chg",
                "avg minutes from session start to the first accepted code change. Sessions that never produced an accepted change are excluded, so this measures speed-to-traction, not engagement.",
            ),
            (
                "Debug Loop",
                "share of sessions that fell into an error-fix loop. A session is flagged when the same normalized error signature recurs across 5+ user turns with assistant replies between them.",
            ),
            (
                "Mainline %",
                "of heavy AI commits, the share that reached the main branch. Uses squash-aware content matching, so a commit still counts when its content lands via a squash or rebase merge.",
            ),
            (
                "Bug Rate %",
                "of merged heavy AI commits, the share that later drew a fix-like commit touching the same files within 60 days. A proxy for defects that escaped to main.",
            ),
        ],
        Tab::Sessions => &[
            (
                "Sessions",
                "count of AI coding sessions in this group and window. One session is one continuous assistant conversation tied to an editor.",
            ),
            (
                "Avg Prompts",
                "average user prompts per session, counting only non-empty user turns. High values can signal back-and-forth or under-specified asks.",
            ),
            (
                "First Chg",
                "avg minutes from session start to the first accepted diff. Sessions with no accepted change are excluded from the average.",
            ),
            (
                "Loop %",
                "share of sessions flagged as a debug loop — the same normalized error signature recurring across 5+ user turns.",
            ),
            (
                "Error %",
                "share of sessions where an error-like message was pasted back into the chat after the first user message.",
            ),
            (
                "To Commit %",
                "share of sessions with at least one matched git commit between session start and session end + 4 hours.",
            ),
            (
                "No Output %",
                "share of sessions that had user turns but produced no accepted code change — effort that did not turn into a diff.",
            ),
        ],
        Tab::Delivery => &[
            (
                "Commits",
                "count of all git commits in the window for this group, AI-heavy or not.",
            ),
            (
                "Heavy",
                "commits where AI-attributed lines are >= 50% of changed lines. The reach/merge rates below are computed over heavy commits only.",
            ),
            (
                "PR sync",
                "ready / eligible heavy commits for PR metrics. Requires a GitHub sync; shows — until commits are linked to pull requests.",
            ),
            (
                "PR Reach %",
                "of heavy commits (GitHub-linked), the share that opened a pull request. Measures whether AI work even entered review.",
            ),
            (
                "Mainline %",
                "of heavy commits, the share that reached main, including squash-aware content matching across squash/rebase merges.",
            ),
            (
                "PR Merge %",
                "of PR-linked heavy commits, the share whose PR merged. Shows — when GitHub sync is unavailable for the group.",
            ),
        ],
        Tab::Quality => &[
            (
                "Heavy",
                "heavy AI commits (>= 50% AI-attributed changed lines) tracked for quality signals. Light assists are excluded.",
            ),
            (
                "Churn %",
                "of AI-added lines that reached main, the share removed again from main within 14 days. High churn means code landed but did not stick.",
            ),
            (
                "Bug Rate %",
                "of merged heavy commits, the share that drew a later fix-like commit on the same files within 60 days.",
            ),
            (
                "Revert %",
                "share of heavy commits later reverted — detected when a commit body contains `This reverts commit <sha>`.",
            ),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::RatioMetric;
    use ratatui::backend::TestBackend;

    fn ratio(numerator: i64, denominator: i64) -> RatioMetric {
        RatioMetric {
            numerator,
            denominator,
        }
    }

    fn sample_app() -> App {
        let mut app = App::new(TuiArgs {
            all_projects: true,
            limit: 50,
        });
        app.reports.session = vec![
            SessionReportRow {
                week_start: None,
                group_value: Some("claude-sonnet-4".to_string()),
                branch_name: None,
                session_count: 38,
                s2_avg: Some(6.2),
                avg_minutes_to_first_accepted_change: Some(2.9),
                debug_loop_rate: ratio(2, 100),
                s6_rate: ratio(8, 100),
                s9_rate: ratio(72, 100),
                no_output_session_rate: ratio(6, 100),
            },
            SessionReportRow {
                week_start: None,
                group_value: Some("cursor/default".to_string()),
                branch_name: None,
                session_count: 9,
                s2_avg: Some(12.1),
                avg_minutes_to_first_accepted_change: None,
                debug_loop_rate: ratio(14, 100),
                s6_rate: ratio(42, 100),
                s9_rate: ratio(28, 100),
                no_output_session_rate: ratio(44, 100),
            },
        ];
        app.reports.delivery = vec![
            ChangeReportRow {
                week_start: None,
                group_value: Some("claude-sonnet-4".to_string()),
                branch_name: None,
                repo_root: None,
                commit_count: 27,
                heavy_commit_count: 19,
                github_pr_heavy_eligible: 19,
                github_pr_heavy_ready: 19,
                pr_reach_rate: ratio(89, 100),
                merge_rate: ratio(79, 100),
                pr_merge_rate: ratio(81, 100),
                github_pr_metrics_available: true,
                avg_commit_to_mainline_hours: Some(18.0),
                task_branch_lines_added: 120,
                task_branch_lines_removed: 30,
            },
            ChangeReportRow {
                week_start: None,
                group_value: Some("cursor/default".to_string()),
                branch_name: None,
                repo_root: None,
                commit_count: 3,
                heavy_commit_count: 1,
                github_pr_heavy_eligible: 0,
                github_pr_heavy_ready: 0,
                pr_reach_rate: ratio(22, 100),
                merge_rate: ratio(17, 100),
                pr_merge_rate: ratio(0, 0),
                github_pr_metrics_available: false,
                avg_commit_to_mainline_hours: None,
                task_branch_lines_added: 10,
                task_branch_lines_removed: 5,
            },
        ];
        app.reports.quality = vec![
            LifecycleReportRow {
                week_start: None,
                group_value: Some("claude-sonnet-4".to_string()),
                branch_name: None,
                heavy_commit_count: 19,
                code_churn_rate: ratio(4, 100),
                bug_after_merge_rate: ratio(0, 100),
                revert_rate: ratio(2, 100),
            },
            LifecycleReportRow {
                week_start: None,
                group_value: Some("cursor/default".to_string()),
                branch_name: None,
                heavy_commit_count: 1,
                code_churn_rate: ratio(31, 100),
                bug_after_merge_rate: ratio(22, 100),
                revert_rate: ratio(18, 100),
            },
        ];
        app.reports.delivery_baseline = Some(ChangeReportRow {
            week_start: None,
            group_value: Some("human".to_string()),
            branch_name: None,
            repo_root: None,
            commit_count: 28,
            heavy_commit_count: 0,
            github_pr_heavy_eligible: 0,
            github_pr_heavy_ready: 0,
            pr_reach_rate: ratio(78, 100),
            merge_rate: ratio(71, 100),
            pr_merge_rate: ratio(0, 0),
            github_pr_metrics_available: false,
            avg_commit_to_mainline_hours: None,
            task_branch_lines_added: 0,
            task_branch_lines_removed: 0,
        });
        app.reports.quality_baseline = Some(LifecycleReportRow {
            week_start: None,
            group_value: Some("human".to_string()),
            branch_name: None,
            heavy_commit_count: 0,
            code_churn_rate: ratio(8, 100),
            bug_after_merge_rate: ratio(6, 100),
            revert_rate: ratio(7, 100),
        });
        app.reports.stats = Some(compute_stats(
            app.window,
            app.reports.session.first(),
            app.reports.delivery.first(),
            app.reports.quality.first(),
            None,
            None,
            None,
        ));
        app
    }

    fn render_text(app: &App) -> String {
        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal.draw(|frame| draw(frame, app)).expect("draw");
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    }

    #[test]
    fn renders_verdict_shell_tabs_and_controls() {
        let app = sample_app();
        let text = render_text(&app);
        assert!(text.contains("[1] Verdict"));
        assert!(text.contains("[2] Sessions"));
        assert!(text.contains("[3] Delivery"));
        assert!(text.contains("[4] Quality"));
        assert!(text.contains("group"));
        assert!(text.contains("window"));
        assert!(text.contains("30d"));
        assert!(text.contains("L legend"));
    }

    #[test]
    fn renders_verdict_ranking_with_baseline_row() {
        let app = sample_app();
        let text = render_text(&app);
        assert!(text.contains("claude-sonnet-4"));
        assert!(text.contains("Mainline"));
        assert!(text.contains("you · no-AI"));
        assert!(text.contains("79%"));
    }

    #[test]
    fn verdict_rows_sorted_by_mainline_desc() {
        let app = sample_app();
        let rows = app.verdict_rows();
        assert_eq!(rows[0].label, "claude-sonnet-4");
        assert_eq!(rows[1].label, "cursor/default");
        assert_eq!(rows[0].mainline, Some(79.0));
    }

    #[test]
    fn number_keys_switch_tabs_and_session_table_renders() {
        let mut app = sample_app();
        app.on_key(event::KeyEvent::from(KeyCode::Char('2')))
            .expect("tab key");
        assert_eq!(app.tab, Tab::Sessions);
        let text = render_text(&app);
        assert!(text.contains("Avg Prompts"));
        assert!(text.contains("No Output %"));
        assert!(text.contains("cursor/default"));
    }

    #[test]
    fn delivery_tab_renders_baseline_and_pr_sync_dash() {
        let mut app = sample_app();
        app.tab = Tab::Delivery;
        let text = render_text(&app);
        assert!(text.contains("PR Reach %"));
        assert!(text.contains("you · no-AI"));
    }

    #[test]
    fn quality_tab_renders_threshold_columns() {
        let mut app = sample_app();
        app.tab = Tab::Quality;
        let text = render_text(&app);
        assert!(text.contains("Churn %"));
        assert!(text.contains("Bug Rate %"));
        assert!(text.contains("Revert %"));
        assert!(text.contains("you · no-AI"));
    }

    #[test]
    fn legend_overlay_shows_tab_specific_entries() {
        let mut app = sample_app();
        app.legend = true;
        let verdict_text = render_text(&app);
        assert!(verdict_text.contains("Legend · Verdict"));
        assert!(verdict_text.contains("Debug Loop"));

        app.tab = Tab::Sessions;
        let session_text = render_text(&app);
        assert!(session_text.contains("Legend · Sessions"));
        assert!(session_text.contains("Avg Prompts"));
    }

    #[test]
    fn window_key_cycles_through_presets() {
        let mut app = sample_app();
        assert_eq!(app.window, Window::D30);
        // reload() hits the DB, so exercise the pure transition directly.
        app.window = app.window.next();
        assert_eq!(app.window, Window::D90);
        app.window = app.window.next();
        assert_eq!(app.window, Window::D7);
    }

    #[test]
    fn group_label_covers_design_dimensions() {
        assert_eq!(group_label(GroupBy::Model), "model");
        assert_eq!(group_label(GroupBy::Provider), "provider");
        assert_eq!(group_label(GroupBy::Task), "task");
        assert_eq!(group_label(GroupBy::Branch), "branch");
    }
}
