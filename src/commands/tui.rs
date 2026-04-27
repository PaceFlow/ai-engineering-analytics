use std::collections::BTreeMap;
use std::io;
use std::time::Duration as StdDuration;

use anyhow::Result;
use chrono::{Datelike, Duration, Local, NaiveDate};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};

use crate::analytics::{
    self, ChangeReportRow, LifecycleReportRow, SessionListRow, SessionReportRow,
};
use crate::cli::{GroupBy, ReportArgs, TuiArgs};
use crate::commands::report_scope;
use crate::db;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Overview,
    Session,
    Delivery,
    Quality,
}

impl Tab {
    fn next(self) -> Self {
        match self {
            Self::Overview => Self::Session,
            Self::Session => Self::Delivery,
            Self::Delivery => Self::Quality,
            Self::Quality => Self::Overview,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Overview => Self::Quality,
            Self::Session => Self::Overview,
            Self::Delivery => Self::Session,
            Self::Quality => Self::Delivery,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Session => "Session",
            Self::Delivery => "Delivery",
            Self::Quality => "Quality",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePreset {
    Today,
    ThisWeek,
    LastWeek,
    Last30d,
    Custom,
}

impl DatePreset {
    fn next(self) -> Self {
        match self {
            Self::Today => Self::ThisWeek,
            Self::ThisWeek => Self::LastWeek,
            Self::LastWeek => Self::Last30d,
            Self::Last30d => Self::Custom,
            Self::Custom => Self::Today,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Today => Self::Custom,
            Self::ThisWeek => Self::Today,
            Self::LastWeek => Self::ThisWeek,
            Self::Last30d => Self::LastWeek,
            Self::Custom => Self::Last30d,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Today => "Today",
            Self::ThisWeek => "This week",
            Self::LastWeek => "Last week",
            Self::Last30d => "Last 30d",
            Self::Custom => "Custom",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DateRange {
    from: NaiveDate,
    to: NaiveDate,
}

impl DateRange {
    fn label(&self) -> String {
        format!("{}..{}", self.from, self.to)
    }
}

#[derive(Debug, Clone)]
struct FilterEntry {
    kind: GroupBy,
    value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    CustomDate,
    ScopeFilter,
}

#[derive(Debug, Clone, Default)]
struct Reports {
    session: Vec<SessionReportRow>,
    delivery: Vec<ChangeReportRow>,
    delivery_over_time: Vec<ChangeReportRow>,
    quality: Vec<LifecycleReportRow>,
    session_list: Vec<SessionListRow>,
}

struct App {
    base_args: ReportArgs,
    active_tab: Tab,
    date_preset: DatePreset,
    custom_range: DateRange,
    group_by: GroupBy,
    weekly: bool,
    selected: usize,
    session_metric: usize,
    delivery_metric: usize,
    quality_metric: usize,
    filters: Vec<FilterEntry>,
    show_session_list: bool,
    reports: Reports,
    input_mode: Option<InputMode>,
    input: String,
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
        let today = Local::now().date_naive();
        Self {
            base_args,
            active_tab: Tab::Overview,
            date_preset: DatePreset::Last30d,
            custom_range: resolve_date_preset(DatePreset::Last30d, today, None),
            group_by: GroupBy::Model,
            weekly: false,
            selected: 0,
            session_metric: 5,
            delivery_metric: 0,
            quality_metric: 0,
            filters: Vec::new(),
            show_session_list: false,
            reports: Reports::default(),
            input_mode: None,
            input: String::new(),
            message: None,
            should_quit: false,
        }
    }

    fn reload(&mut self) -> Result<()> {
        let db = db::open()?;
        analytics::create_reporting_views(&db)?;
        let options = analytics::ReportQueryOptions {
            implicit_model_default: self.group_by == GroupBy::Model && !self.weekly,
        };
        let report = self.report_args();
        self.reports.session = analytics::query_session_report_with_options(&db, &report, options)?;
        self.reports.delivery = analytics::query_change_report_with_options(&db, &report, options)?;
        self.reports.delivery_over_time = analytics::query_change_report_with_options(
            &db,
            &self.delivery_over_time_args(),
            options,
        )?;
        self.reports.quality =
            analytics::query_lifecycle_report_with_options(&db, &report, options)?;
        self.reports.session_list = if self.show_session_list {
            analytics::query_session_list_rows(&db, &report)?
        } else {
            Vec::new()
        };
        self.selected = self.selected.min(self.current_len().saturating_sub(1));
        Ok(())
    }

    fn report_args(&self) -> ReportArgs {
        let mut args = self.base_args.clone();
        let today = Local::now().date_naive();
        let range = resolve_date_preset(self.date_preset, today, Some(&self.custom_range));
        args.from = Some(range.from.to_string());
        args.to = Some(range.to.to_string());
        args.weekly = self.weekly;
        args.group_by = Some(self.group_by);
        for filter in &self.filters {
            match filter.kind {
                GroupBy::Repo => args.repo = Some(filter.value.clone()),
                GroupBy::Provider => args.provider = Some(filter.value.clone()),
                GroupBy::Task => args.task = Some(filter.value.clone()),
                GroupBy::Branch => args.branch = Some(filter.value.clone()),
                GroupBy::Model => args.model = Some(filter.value.clone()),
            }
        }
        args
    }

    fn delivery_over_time_args(&self) -> ReportArgs {
        let mut args = self.report_args();
        args.weekly = true;
        args.group_by = None;
        args
    }

    fn current_len(&self) -> usize {
        match self.active_tab {
            Tab::Overview => overview_items(self).len(),
            Tab::Session if self.show_session_list => self.reports.session_list.len(),
            Tab::Session => self.reports.session.len(),
            Tab::Delivery if self.weekly => self.reports.delivery_over_time.len(),
            Tab::Delivery => self.reports.delivery.len(),
            Tab::Quality => self.reports.quality.len(),
        }
    }

    fn on_key(&mut self, key: event::KeyEvent) -> Result<()> {
        if key.kind != KeyEventKind::Press {
            return Ok(());
        }
        if let Some(mode) = self.input_mode {
            return self.on_input_key(mode, key);
        }
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.message = Some(help_text().to_string()),
            KeyCode::Tab => self.switch_tab(self.active_tab.next())?,
            KeyCode::BackTab => self.switch_tab(self.active_tab.previous())?,
            KeyCode::Char('d') => {
                self.date_preset = self.date_preset.next();
                self.message = Some(format!("Date: {}", self.date_preset.label()));
                self.reload()?;
            }
            KeyCode::Left if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.date_preset = self.date_preset.previous();
                self.reload()?;
            }
            KeyCode::Right if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.date_preset = self.date_preset.next();
                self.reload()?;
            }
            KeyCode::Char('c') => {
                self.date_preset = DatePreset::Custom;
                self.input_mode = Some(InputMode::CustomDate);
                self.input = self.custom_range.label();
                self.message = Some("Enter date range as YYYY-MM-DD..YYYY-MM-DD".to_string());
            }
            KeyCode::Char('/') => {
                self.input_mode = Some(InputMode::ScopeFilter);
                self.input.clear();
                self.message = Some(
                    "Filter format: model=..., provider=..., repo=..., task=..., branch=..."
                        .to_string(),
                );
            }
            KeyCode::Char('r') => self.reload()?,
            KeyCode::Char('w') => {
                self.weekly = !self.weekly;
                self.selected = 0;
                self.reload()?;
            }
            KeyCode::Char('g') => {
                self.group_by = next_group(self.group_by);
                self.show_session_list = false;
                self.selected = 0;
                self.reload()?;
            }
            KeyCode::Char(ch) if ch.is_ascii_digit() => {
                self.set_metric_digit(ch)?;
            }
            KeyCode::Down => {
                self.selected = (self.selected + 1).min(self.current_len().saturating_sub(1));
            }
            KeyCode::Up => {
                self.selected = self.selected.saturating_sub(1);
            }
            KeyCode::Right => {
                self.selected = (self.selected + 1).min(self.current_len().saturating_sub(1));
            }
            KeyCode::Left => {
                self.selected = self.selected.saturating_sub(1);
            }
            KeyCode::Enter => self.drilldown()?,
            KeyCode::Backspace => self.back()?,
            KeyCode::Esc => self.message = None,
            _ => {}
        }
        Ok(())
    }

    fn on_input_key(&mut self, mode: InputMode, key: event::KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = None;
                self.input.clear();
                self.message = None;
            }
            KeyCode::Enter => {
                match mode {
                    InputMode::CustomDate => self.apply_custom_date()?,
                    InputMode::ScopeFilter => self.apply_scope_filter()?,
                }
                self.input_mode = None;
                self.input.clear();
                self.reload()?;
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Char(ch) => self.input.push(ch),
            _ => {}
        }
        Ok(())
    }

    fn switch_tab(&mut self, tab: Tab) -> Result<()> {
        self.active_tab = tab;
        self.selected = 0;
        self.show_session_list = false;
        self.reload()
    }

    fn set_metric_digit(&mut self, ch: char) -> Result<()> {
        let Some(index) = ch.to_digit(10).map(|n| n.saturating_sub(1) as usize) else {
            return Ok(());
        };
        match self.active_tab {
            Tab::Session if index < SESSION_METRICS.len() => self.session_metric = index,
            Tab::Delivery if index < DELIVERY_METRICS.len() => self.delivery_metric = index,
            Tab::Quality if index < QUALITY_METRICS.len() => self.quality_metric = index,
            _ => {}
        }
        Ok(())
    }

    fn drilldown(&mut self) -> Result<()> {
        if self.drilldown_state() {
            self.reload()?;
        }
        Ok(())
    }

    fn drilldown_state(&mut self) -> bool {
        if self.active_tab == Tab::Overview {
            if let Some(item) = overview_items(self).get(self.selected) {
                self.active_tab = item.tab;
                self.selected = item.row_index;
            }
            return false;
        }
        if self.show_session_list {
            return false;
        }
        let Some(value) = self.selected_group_value() else {
            return false;
        };
        self.filters.push(FilterEntry {
            kind: self.group_by,
            value,
        });
        if self.active_tab == Tab::Session && self.group_by == GroupBy::Branch {
            self.show_session_list = true;
        } else if let Some(next) = next_drill_group(self.active_tab, self.group_by) {
            self.group_by = next;
        }
        self.selected = 0;
        true
    }

    fn back(&mut self) -> Result<()> {
        if self.back_state() {
            self.reload()?;
        }
        Ok(())
    }

    fn back_state(&mut self) -> bool {
        if self.show_session_list {
            self.show_session_list = false;
            return true;
        }
        if let Some(filter) = self.filters.pop() {
            self.group_by = filter.kind;
            self.selected = 0;
            return true;
        }
        false
    }

    fn selected_group_value(&self) -> Option<String> {
        match self.active_tab {
            Tab::Session => self.reports.session.get(self.selected).and_then(|row| {
                selected_group_value(
                    self.group_by,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                )
            }),
            Tab::Delivery => self.reports.delivery.get(self.selected).and_then(|row| {
                selected_group_value(
                    self.group_by,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                )
            }),
            Tab::Quality => self.reports.quality.get(self.selected).and_then(|row| {
                selected_group_value(
                    self.group_by,
                    row.group_value.as_deref(),
                    row.branch_name.as_deref(),
                )
            }),
            _ => None,
        }
    }

    fn apply_custom_date(&mut self) -> Result<()> {
        let normalized = self.input.replace(" to ", "..").replace(',', "..");
        let parts = normalized.split("..").collect::<Vec<_>>();
        if parts.len() != 2 {
            self.message = Some("Invalid date range. Use YYYY-MM-DD..YYYY-MM-DD".to_string());
            return Ok(());
        }
        let from = NaiveDate::parse_from_str(parts[0].trim(), "%Y-%m-%d");
        let to = NaiveDate::parse_from_str(parts[1].trim(), "%Y-%m-%d");
        match (from, to) {
            (Ok(from), Ok(to)) if from <= to => {
                self.custom_range = DateRange { from, to };
                self.date_preset = DatePreset::Custom;
                self.message = Some(format!("Date: {}", self.custom_range.label()));
            }
            _ => {
                self.message = Some("Invalid date range. Keeping previous range.".to_string());
            }
        }
        Ok(())
    }

    fn apply_scope_filter(&mut self) -> Result<()> {
        let Some((kind, value)) = self.input.split_once('=') else {
            self.message = Some("Invalid filter. Use kind=value.".to_string());
            return Ok(());
        };
        let kind = match kind.trim().to_ascii_lowercase().as_str() {
            "repo" => GroupBy::Repo,
            "provider" => GroupBy::Provider,
            "model" => GroupBy::Model,
            "task" => GroupBy::Task,
            "branch" => GroupBy::Branch,
            _ => {
                self.message = Some("Unknown filter kind.".to_string());
                return Ok(());
            }
        };
        let value = value.trim();
        if value.is_empty() {
            self.message = Some("Filter value cannot be empty.".to_string());
            return Ok(());
        }
        self.filters.retain(|filter| filter.kind != kind);
        self.filters.push(FilterEntry {
            kind,
            value: value.to_string(),
        });
        self.selected = 0;
        self.message = Some(format!("Applied {}={}", group_label(kind), value));
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

fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(2),
        ])
        .split(area);
    render_tabs(frame, app, chunks[0]);
    render_filters(frame, app, chunks[1]);
    match app.active_tab {
        Tab::Overview => render_overview(frame, app, chunks[2]),
        Tab::Session => render_report(frame, app, chunks[2], Tab::Session),
        Tab::Delivery => render_report(frame, app, chunks[2], Tab::Delivery),
        Tab::Quality => render_report(frame, app, chunks[2], Tab::Quality),
    }
    render_footer(frame, app, chunks[3]);
    if app.input_mode.is_some() {
        render_input(frame, app, centered_rect(70, 20, area));
    } else if let Some(message) = app.message.as_deref()
        && message.contains('\n')
    {
        render_message(frame, message, centered_rect(70, 55, area));
    }
}

fn render_tabs(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let titles = [Tab::Overview, Tab::Session, Tab::Delivery, Tab::Quality]
        .iter()
        .map(|tab| {
            if *tab == app.active_tab {
                Span::styled(
                    format!(" {} ", tab.title()),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Span::styled(
                    format!(" {} ", tab.title()),
                    Style::default().fg(Color::Gray),
                )
            }
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(Line::from(titles)).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_filters(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let today = Local::now().date_naive();
    let range = resolve_date_preset(app.date_preset, today, Some(&app.custom_range));
    let filters = if app.filters.is_empty() {
        "none".to_string()
    } else {
        app.filters
            .iter()
            .map(|filter| format!("{}={}", group_label(filter.kind), filter.value))
            .collect::<Vec<_>>()
            .join("  ")
    };
    let line = Line::from(vec![
        Span::styled(
            " Date ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ),
        Span::raw(format!(" {} {} ", app.date_preset.label(), range.label())),
        Span::styled(
            " Group ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ),
        Span::raw(format!(" {} ", group_label(app.group_by))),
        Span::styled(
            " View ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ),
        Span::raw(if app.weekly { " weekly " } else { " grouped " }),
        Span::styled(
            " Filters ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ),
        Span::raw(format!(" {filters}")),
    ]);
    frame.render_widget(
        Paragraph::new(line)
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_overview(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(44), Constraint::Percentage(56)])
        .split(area);
    let items = overview_items(app);
    let risk_by_label = risk_counts(&items);
    render_bar_lines(
        frame,
        chunks[0],
        "Risk Count By Bucket",
        &risk_by_label
            .iter()
            .map(|(label, value)| ChartPoint {
                label: label.clone(),
                value: Some(*value as f64),
                value_label: value.to_string(),
            })
            .collect::<Vec<_>>(),
        app.selected,
    );
    let rows = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let marker = if index == app.selected { ">" } else { " " };
            Line::from(vec![
                Span::styled(marker, Style::default().fg(Color::Cyan)),
                Span::raw(format!(
                    " {:<8} {:<28} {:<20} {}",
                    item.tab.title(),
                    truncate(&item.label, 28),
                    item.metric,
                    item.value
                )),
            ])
        })
        .collect::<Vec<_>>();
    let empty = vec![Line::raw("No risk rows found for this date range.")];
    frame.render_widget(
        Paragraph::new(if rows.is_empty() { empty } else { rows })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Needs Attention"),
            )
            .wrap(Wrap { trim: false }),
        chunks[1],
    );
}

fn render_report(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect, tab: Tab) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(chunks[0]);
    if tab == Tab::Session && app.show_session_list {
        render_session_list(frame, app, area);
        return;
    }
    if tab == Tab::Delivery && app.weekly {
        let points = delivery_over_time_points(app);
        render_bar_lines(
            frame,
            top[0],
            &format!("{} Over Time", metric_name(app, tab)),
            &points,
            app.selected,
        );
    } else {
        let points = chart_points(app, tab);
        render_bar_lines(frame, top[0], &chart_title(app, tab), &points, app.selected);
    }
    render_detail(frame, app, tab, top[1]);
    render_table(frame, app, tab, chunks[1]);
}

fn render_session_list(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app
        .reports
        .session_list
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let style = if index == app.selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(row.provider.clone()),
                Cell::from(truncate(&row.model, 18)),
                Cell::from(truncate(&row.session_id, 10)),
                Cell::from(truncate(&row.project_path, 34)),
                Cell::from(row.last_active.clone().unwrap_or_else(|| "?".to_string())),
                Cell::from(row.total_loc.to_string()),
            ])
            .style(style)
        });
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(20),
            Constraint::Length(12),
            Constraint::Min(20),
            Constraint::Length(18),
            Constraint::Length(8),
        ],
    )
    .header(Row::new([
        "Provider",
        "Model",
        "Session",
        "Project",
        "Last active",
        "LOC",
    ]))
    .block(Block::default().borders(Borders::ALL).title("Session List"));
    frame.render_widget(table, area);
}

fn render_detail(frame: &mut ratatui::Frame<'_>, app: &App, tab: Tab, area: Rect) {
    let mut lines = Vec::new();
    lines.push(Line::styled(
        format!("Metric: {}", metric_name(app, tab)),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    lines.push(Line::raw(format!("Group: {}", group_label(app.group_by))));
    lines.push(Line::raw(format!(
        "Drilldown: {}",
        next_drill_group(tab, app.group_by)
            .map(group_label)
            .unwrap_or("detail")
    )));
    lines.push(Line::raw(""));
    if let Some(row) = detail_lines(app, tab) {
        lines.extend(row);
    } else {
        lines.push(Line::raw(
            "No rows found. Run `paceflow ingest` first or widen the date range.",
        ));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Detail"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_table(frame: &mut ratatui::Frame<'_>, app: &App, tab: Tab, area: Rect) {
    match tab {
        Tab::Session => {
            let rows = app.reports.session.iter().enumerate().map(|(index, row)| {
                table_row_style(
                    index,
                    app.selected,
                    vec![
                        label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
                        row.session_count.to_string(),
                        fmt_opt(row.s2_avg),
                        fmt_opt(row.avg_minutes_to_first_accepted_change),
                        fmt_pct(row.debug_loop_rate.percent()),
                        fmt_pct(row.s9_rate.percent()),
                        fmt_pct(row.no_output_session_rate.percent()),
                    ],
                )
            });
            frame.render_widget(
                Table::new(
                    rows,
                    [
                        Constraint::Min(18),
                        Constraint::Length(9),
                        Constraint::Length(9),
                        Constraint::Length(10),
                        Constraint::Length(9),
                        Constraint::Length(10),
                        Constraint::Length(11),
                    ],
                )
                .header(Row::new([
                    "Bucket",
                    "Sessions",
                    "Prompts",
                    "First Chg",
                    "Loop",
                    "To Commit",
                    "No Output",
                ]))
                .block(Block::default().borders(Borders::ALL).title("Rows")),
                area,
            );
        }
        Tab::Delivery => {
            let rows = app.reports.delivery.iter().enumerate().map(|(index, row)| {
                table_row_style(
                    index,
                    app.selected,
                    vec![
                        label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
                        row.commit_count.to_string(),
                        row.heavy_commit_count.to_string(),
                        row.pr_reach_rate.numerator.to_string(),
                        delivery_loc(row).to_string(),
                        format!(
                            "{}/{}",
                            row.github_pr_heavy_ready, row.github_pr_heavy_eligible
                        ),
                        fmt_pct(row.merge_rate.percent()),
                        fmt_pct(row.pr_reach_rate.percent()),
                        fmt_pct(row.pr_merge_rate.percent()),
                    ],
                )
            });
            frame.render_widget(
                Table::new(
                    rows,
                    [
                        Constraint::Min(18),
                        Constraint::Length(8),
                        Constraint::Length(7),
                        Constraint::Length(7),
                        Constraint::Length(9),
                        Constraint::Length(8),
                        Constraint::Length(10),
                        Constraint::Length(9),
                        Constraint::Length(9),
                    ],
                )
                .header(Row::new([
                    "Bucket", "Commits", "Heavy", "PRs", "LOC", "PR sync", "Mainline", "PR reach",
                    "PR merge",
                ]))
                .block(Block::default().borders(Borders::ALL).title("Rows")),
                area,
            );
        }
        Tab::Quality => {
            let rows = app.reports.quality.iter().enumerate().map(|(index, row)| {
                table_row_style(
                    index,
                    app.selected,
                    vec![
                        label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
                        row.heavy_commit_count.to_string(),
                        fmt_pct(row.code_churn_rate.percent()),
                        fmt_pct(row.bug_after_merge_rate.percent()),
                        fmt_pct(row.revert_rate.percent()),
                    ],
                )
            });
            frame.render_widget(
                Table::new(
                    rows,
                    [
                        Constraint::Min(18),
                        Constraint::Length(8),
                        Constraint::Length(10),
                        Constraint::Length(10),
                        Constraint::Length(10),
                    ],
                )
                .header(Row::new(["Bucket", "Heavy", "Churn", "Bug", "Revert"]))
                .block(Block::default().borders(Borders::ALL).title("Rows")),
                area,
            );
        }
        Tab::Overview => {}
    }
}

fn table_row_style(index: usize, selected: usize, cells: Vec<String>) -> Row<'static> {
    let style = if index == selected {
        Style::default().fg(Color::Black).bg(Color::Cyan)
    } else {
        Style::default()
    };
    Row::new(cells).style(style)
}

fn render_bar_lines(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    title: &str,
    points: &[ChartPoint],
    selected: usize,
) {
    let max = points
        .iter()
        .filter_map(|point| point.value)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let width = area.width.saturating_sub(30).clamp(8, 44) as usize;
    let lines = if points.is_empty() {
        vec![Line::raw(
            "No rows found. Run `paceflow ingest` first or widen the date range.",
        )]
    } else {
        points
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let value = point.value.unwrap_or(0.0).max(0.0);
                let filled = ((value / max) * width as f64).round() as usize;
                let marker = if index == selected { ">" } else { " " };
                let style = if index == selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Green)
                };
                Line::from(vec![
                    Span::styled(marker, Style::default().fg(Color::Cyan)),
                    Span::raw(format!(" {:<18} ", truncate(&point.label, 18))),
                    Span::styled("█".repeat(filled.max(1)), style),
                    Span::raw(format!(" {}", point.value_label)),
                ])
            })
            .collect()
    };
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_footer(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let message = app.message.as_deref().unwrap_or(
        "Tab switch  1-7 metric  g group  d date  Shift+Left/Right date  c custom date  w weekly  Enter drill  Backspace back  / filter  ? help  q quit",
    );
    frame.render_widget(
        Paragraph::new(message)
            .style(Style::default().fg(Color::Gray))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_input(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    frame.render_widget(Clear, area);
    let title = match app.input_mode {
        Some(InputMode::CustomDate) => "Custom Date",
        Some(InputMode::ScopeFilter) => "Scope Filter",
        None => "Input",
    };
    frame.render_widget(
        Paragraph::new(app.input.as_str())
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}

fn render_message(frame: &mut ratatui::Frame<'_>, message: &str, area: Rect) {
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(message)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn chart_points(app: &App, tab: Tab) -> Vec<ChartPoint> {
    match tab {
        Tab::Session => app
            .reports
            .session
            .iter()
            .map(|row| {
                let value = session_metric_value(row, app.session_metric);
                ChartPoint {
                    label: label_for_row(
                        app,
                        row.group_value.as_deref(),
                        row.branch_name.as_deref(),
                    ),
                    value,
                    value_label: fmt_metric_value(
                        value,
                        session_metric_is_percent(app.session_metric),
                    ),
                }
            })
            .collect(),
        Tab::Delivery => app
            .reports
            .delivery
            .iter()
            .map(|row| {
                let value = delivery_metric_value(row, app.delivery_metric);
                ChartPoint {
                    label: label_for_row(
                        app,
                        row.group_value.as_deref(),
                        row.branch_name.as_deref(),
                    ),
                    value,
                    value_label: fmt_metric_value(
                        value,
                        delivery_metric_is_percent(app.delivery_metric),
                    ),
                }
            })
            .collect(),
        Tab::Quality => app
            .reports
            .quality
            .iter()
            .map(|row| {
                let value = quality_metric_value(row, app.quality_metric);
                ChartPoint {
                    label: label_for_row(
                        app,
                        row.group_value.as_deref(),
                        row.branch_name.as_deref(),
                    ),
                    value,
                    value_label: fmt_metric_value(value, true),
                }
            })
            .collect(),
        Tab::Overview => Vec::new(),
    }
}

fn delivery_over_time_points(app: &App) -> Vec<ChartPoint> {
    app.reports
        .delivery_over_time
        .iter()
        .map(|row| {
            let value = delivery_metric_value(row, app.delivery_metric);
            ChartPoint {
                label: row
                    .week_start
                    .clone()
                    .unwrap_or_else(|| "(unknown)".to_string()),
                value,
                value_label: fmt_metric_value(
                    value,
                    delivery_metric_is_percent(app.delivery_metric),
                ),
            }
        })
        .collect()
}

fn detail_lines(app: &App, tab: Tab) -> Option<Vec<Line<'static>>> {
    match tab {
        Tab::Session => app.reports.session.get(app.selected).map(|row| {
            vec![
                Line::raw(format!(
                    "Bucket: {}",
                    label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref())
                )),
                Line::raw(format!("Sessions: {}", row.session_count)),
                Line::raw(format!("Avg prompts: {}", fmt_opt(row.s2_avg))),
                Line::raw(format!(
                    "First change: {} min",
                    fmt_opt(row.avg_minutes_to_first_accepted_change)
                )),
                Line::raw(format!(
                    "Debug loops: {}",
                    fmt_pct(row.debug_loop_rate.percent())
                )),
                Line::raw(format!("Errors pasted: {}", fmt_pct(row.s6_rate.percent()))),
                Line::raw(format!("To commit: {}", fmt_pct(row.s9_rate.percent()))),
                Line::raw(format!(
                    "No output: {}",
                    fmt_pct(row.no_output_session_rate.percent())
                )),
            ]
        }),
        Tab::Delivery if app.weekly => {
            app.reports.delivery_over_time.get(app.selected).map(|row| {
                vec![
                    Line::raw(format!(
                        "Week: {}",
                        row.week_start.as_deref().unwrap_or("(unknown)")
                    )),
                    Line::raw(format!("Commits: {}", row.commit_count)),
                    Line::raw(format!("Heavy commits: {}", row.heavy_commit_count)),
                    Line::raw(format!("PRs: {}", row.pr_reach_rate.numerator)),
                    Line::raw(format!("LOC: {}", delivery_loc(row))),
                    Line::raw(format!(
                        "Mainline reach: {}",
                        fmt_pct(row.merge_rate.percent())
                    )),
                    Line::raw(format!(
                        "PR reach: {}",
                        fmt_pct(row.pr_reach_rate.percent())
                    )),
                    Line::raw(format!(
                        "PR merge: {}",
                        fmt_pct(row.pr_merge_rate.percent())
                    )),
                ]
            })
        }
        Tab::Delivery => app.reports.delivery.get(app.selected).map(|row| {
            vec![
                Line::raw(format!(
                    "Bucket: {}",
                    label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref())
                )),
                Line::raw(format!("Commits: {}", row.commit_count)),
                Line::raw(format!("Heavy commits: {}", row.heavy_commit_count)),
                Line::raw(format!("PRs: {}", row.pr_reach_rate.numerator)),
                Line::raw(format!("LOC: {}", delivery_loc(row))),
                Line::raw(format!(
                    "PR sync: {}/{}",
                    row.github_pr_heavy_ready, row.github_pr_heavy_eligible
                )),
                Line::raw(format!(
                    "Mainline reach: {}",
                    fmt_pct(row.merge_rate.percent())
                )),
                Line::raw(format!(
                    "PR reach: {}",
                    fmt_pct(row.pr_reach_rate.percent())
                )),
                Line::raw(format!(
                    "PR merge: {}",
                    fmt_pct(row.pr_merge_rate.percent())
                )),
                Line::raw(format!(
                    "Task LOC: +{} / -{}",
                    row.task_branch_lines_added, row.task_branch_lines_removed
                )),
            ]
        }),
        Tab::Quality => app.reports.quality.get(app.selected).map(|row| {
            vec![
                Line::raw(format!(
                    "Bucket: {}",
                    label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref())
                )),
                Line::raw(format!("Heavy commits: {}", row.heavy_commit_count)),
                Line::raw(format!(
                    "Code churn: {}",
                    fmt_pct(row.code_churn_rate.percent())
                )),
                Line::raw(format!(
                    "Bug-after-merge: {}",
                    fmt_pct(row.bug_after_merge_rate.percent())
                )),
                Line::raw(format!("Reverts: {}", fmt_pct(row.revert_rate.percent()))),
            ]
        }),
        Tab::Overview => None,
    }
}

#[derive(Debug, Clone)]
struct ChartPoint {
    label: String,
    value: Option<f64>,
    value_label: String,
}

#[derive(Debug, Clone)]
struct OverviewItem {
    tab: Tab,
    row_index: usize,
    label: String,
    metric: &'static str,
    value: String,
    risk_score: u8,
}

struct RiskRule {
    metric: &'static str,
    value: Option<f64>,
    threshold: f64,
    lower_is_better: bool,
}

fn overview_items(app: &App) -> Vec<OverviewItem> {
    let mut items = Vec::new();
    for (index, row) in app.reports.session.iter().enumerate() {
        push_if_risky(
            &mut items,
            Tab::Session,
            index,
            label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
            RiskRule {
                metric: "No output",
                value: row.no_output_session_rate.percent(),
                threshold: 35.0,
                lower_is_better: true,
            },
        );
        push_if_risky(
            &mut items,
            Tab::Session,
            index,
            label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
            RiskRule {
                metric: "To commit",
                value: row.s9_rate.percent(),
                threshold: 25.0,
                lower_is_better: false,
            },
        );
    }
    for (index, row) in app.reports.delivery.iter().enumerate() {
        push_if_risky(
            &mut items,
            Tab::Delivery,
            index,
            label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
            RiskRule {
                metric: "Mainline",
                value: row.merge_rate.percent(),
                threshold: 50.0,
                lower_is_better: false,
            },
        );
        if row.github_pr_metrics_available {
            push_if_risky(
                &mut items,
                Tab::Delivery,
                index,
                label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
                RiskRule {
                    metric: "PR merge",
                    value: row.pr_merge_rate.percent(),
                    threshold: 50.0,
                    lower_is_better: false,
                },
            );
        }
    }
    for (index, row) in app.reports.quality.iter().enumerate() {
        push_if_risky(
            &mut items,
            Tab::Quality,
            index,
            label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
            RiskRule {
                metric: "Churn",
                value: row.code_churn_rate.percent(),
                threshold: 30.0,
                lower_is_better: true,
            },
        );
        push_if_risky(
            &mut items,
            Tab::Quality,
            index,
            label_for_row(app, row.group_value.as_deref(), row.branch_name.as_deref()),
            RiskRule {
                metric: "Reverts",
                value: row.revert_rate.percent(),
                threshold: 5.0,
                lower_is_better: true,
            },
        );
    }
    items.sort_by(|a, b| {
        b.risk_score
            .cmp(&a.risk_score)
            .then_with(|| a.label.cmp(&b.label))
    });
    items.truncate(15);
    items
}

fn push_if_risky(
    items: &mut Vec<OverviewItem>,
    tab: Tab,
    row_index: usize,
    label: String,
    rule: RiskRule,
) {
    let Some(value) = rule.value else {
        return;
    };
    let risky = if rule.lower_is_better {
        value > rule.threshold
    } else {
        value < rule.threshold
    };
    if risky {
        let distance = if rule.lower_is_better {
            value - rule.threshold
        } else {
            rule.threshold - value
        };
        items.push(OverviewItem {
            tab,
            row_index,
            label,
            metric: rule.metric,
            value: fmt_pct(Some(value)),
            risk_score: distance.round().clamp(1.0, 100.0) as u8,
        });
    }
}

fn risk_counts(items: &[OverviewItem]) -> Vec<(String, usize)> {
    let mut counts = BTreeMap::new();
    for item in items {
        *counts.entry(item.label.clone()).or_insert(0) += 1;
    }
    counts.into_iter().collect()
}

fn selected_group_value(
    group_by: GroupBy,
    group_value: Option<&str>,
    branch_name: Option<&str>,
) -> Option<String> {
    if group_by == GroupBy::Branch {
        branch_name.or(group_value).map(ToOwned::to_owned)
    } else {
        group_value.map(ToOwned::to_owned)
    }
}

fn next_group(group: GroupBy) -> GroupBy {
    match group {
        GroupBy::Model => GroupBy::Provider,
        GroupBy::Provider => GroupBy::Repo,
        GroupBy::Repo => GroupBy::Task,
        GroupBy::Task => GroupBy::Branch,
        GroupBy::Branch => GroupBy::Model,
    }
}

fn next_drill_group(tab: Tab, group: GroupBy) -> Option<GroupBy> {
    match tab {
        Tab::Session => match group {
            GroupBy::Model => Some(GroupBy::Provider),
            GroupBy::Provider => Some(GroupBy::Repo),
            GroupBy::Repo => Some(GroupBy::Branch),
            GroupBy::Branch => None,
            GroupBy::Task => Some(GroupBy::Branch),
        },
        Tab::Delivery | Tab::Quality => match group {
            GroupBy::Model => Some(GroupBy::Repo),
            GroupBy::Repo => Some(GroupBy::Task),
            GroupBy::Task => Some(GroupBy::Branch),
            GroupBy::Provider => Some(GroupBy::Repo),
            GroupBy::Branch => None,
        },
        Tab::Overview => None,
    }
}

fn resolve_date_preset(
    preset: DatePreset,
    today: NaiveDate,
    custom: Option<&DateRange>,
) -> DateRange {
    match preset {
        DatePreset::Today => DateRange {
            from: today,
            to: today,
        },
        DatePreset::ThisWeek => {
            let from = today - Duration::days(today.weekday().num_days_from_monday() as i64);
            DateRange { from, to: today }
        }
        DatePreset::LastWeek => {
            let this_monday = today - Duration::days(today.weekday().num_days_from_monday() as i64);
            let from = this_monday - Duration::days(7);
            let to = this_monday - Duration::days(1);
            DateRange { from, to }
        }
        DatePreset::Last30d => DateRange {
            from: today - Duration::days(29),
            to: today,
        },
        DatePreset::Custom => custom.cloned().unwrap_or(DateRange {
            from: today - Duration::days(29),
            to: today,
        }),
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

const SESSION_METRICS: [&str; 6] = [
    "Avg prompts",
    "First change",
    "Debug loops",
    "Error pastes",
    "To commit",
    "No output",
];
const DELIVERY_METRICS: [&str; 7] = [
    "Commits",
    "Heavy commits",
    "PRs",
    "LOC",
    "Mainline",
    "PR reach",
    "PR merge",
];
const QUALITY_METRICS: [&str; 3] = ["Churn", "Bug-after-merge", "Reverts"];

fn metric_name(app: &App, tab: Tab) -> &'static str {
    match tab {
        Tab::Session => SESSION_METRICS[app.session_metric],
        Tab::Delivery => DELIVERY_METRICS[app.delivery_metric],
        Tab::Quality => QUALITY_METRICS[app.quality_metric],
        Tab::Overview => "Risk",
    }
}

fn chart_title(app: &App, tab: Tab) -> String {
    let prefix = if app.weekly { "Weekly" } else { "Grouped" };
    format!(
        "{prefix} {} By {}",
        metric_name(app, tab),
        group_label(app.group_by)
    )
}

fn session_metric_value(row: &SessionReportRow, metric: usize) -> Option<f64> {
    match metric {
        0 => row.s2_avg,
        1 => row.avg_minutes_to_first_accepted_change,
        2 => row.debug_loop_rate.percent(),
        3 => row.s6_rate.percent(),
        4 => row.s9_rate.percent(),
        _ => row.no_output_session_rate.percent(),
    }
}

fn session_metric_is_percent(metric: usize) -> bool {
    metric >= 2
}

fn delivery_metric_value(row: &ChangeReportRow, metric: usize) -> Option<f64> {
    match metric {
        0 => Some(row.commit_count as f64),
        1 => Some(row.heavy_commit_count as f64),
        2 => Some(row.pr_reach_rate.numerator as f64),
        3 => Some(delivery_loc(row) as f64),
        4 => row.merge_rate.percent(),
        5 => row.pr_reach_rate.percent(),
        _ => row.pr_merge_rate.percent(),
    }
}

fn delivery_metric_is_percent(metric: usize) -> bool {
    metric >= 4
}

fn delivery_loc(row: &ChangeReportRow) -> i64 {
    row.task_branch_lines_added + row.task_branch_lines_removed
}

fn quality_metric_value(row: &LifecycleReportRow, metric: usize) -> Option<f64> {
    match metric {
        0 => row.code_churn_rate.percent(),
        1 => row.bug_after_merge_rate.percent(),
        _ => row.revert_rate.percent(),
    }
}

fn label_for_row(app: &App, group_value: Option<&str>, branch_name: Option<&str>) -> String {
    if app.group_by == GroupBy::Branch {
        branch_name.or(group_value).unwrap_or("(all)").to_string()
    } else {
        group_value.unwrap_or("(all)").to_string()
    }
}

fn fmt_metric_value(value: Option<f64>, percent: bool) -> String {
    if percent {
        fmt_pct(value)
    } else {
        fmt_opt(value)
    }
}

fn fmt_pct(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}%"))
        .unwrap_or_else(|| "N/A".to_string())
}

fn fmt_opt(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}"))
        .unwrap_or_else(|| "N/A".to_string())
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

fn help_text() -> &'static str {
    "Keys\n\
     Tab / Shift+Tab: switch report\n\
     1-7: switch metric in the active report\n\
     g: cycle group-by model/provider/repo/task/branch\n\
     d: cycle date preset; Shift+Left/Right cycles dates backward/forward\n\
     c: edit custom date range as YYYY-MM-DD..YYYY-MM-DD\n\
     w: toggle weekly trend mode\n\
     Arrow keys: move selected chart bucket/table row\n\
     Enter: drill into the selected bucket\n\
     Backspace: go back one drilldown level\n\
     /: add a scope filter such as model=codex/gpt-5.4 or task=ABC-123\n\
     r: reload from SQLite\n\
     Esc: close this overlay\n\
     q: quit"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::RatioMetric;
    use ratatui::backend::TestBackend;

    fn sample_app() -> App {
        let mut app = App::new(TuiArgs {
            all_projects: true,
            limit: 50,
        });
        app.reports.session = vec![SessionReportRow {
            week_start: None,
            group_value: Some("codex/gpt-5.4".to_string()),
            branch_name: None,
            session_count: 8,
            s2_avg: Some(6.2),
            avg_minutes_to_first_accepted_change: Some(18.0),
            debug_loop_rate: RatioMetric {
                numerator: 2,
                denominator: 5,
            },
            s6_rate: RatioMetric {
                numerator: 1,
                denominator: 5,
            },
            s9_rate: RatioMetric {
                numerator: 1,
                denominator: 8,
            },
            no_output_session_rate: RatioMetric {
                numerator: 4,
                denominator: 8,
            },
        }];
        app.reports.delivery = vec![ChangeReportRow {
            week_start: None,
            group_value: Some("codex/gpt-5.4".to_string()),
            branch_name: None,
            repo_root: None,
            commit_count: 9,
            heavy_commit_count: 6,
            github_pr_heavy_eligible: 4,
            github_pr_heavy_ready: 4,
            pr_reach_rate: RatioMetric {
                numerator: 3,
                denominator: 4,
            },
            merge_rate: RatioMetric {
                numerator: 2,
                denominator: 6,
            },
            pr_merge_rate: RatioMetric {
                numerator: 1,
                denominator: 3,
            },
            github_pr_metrics_available: true,
            task_branch_lines_added: 120,
            task_branch_lines_removed: 30,
        }];
        app.reports.delivery_over_time = vec![
            ChangeReportRow {
                week_start: Some("2026-04-13".to_string()),
                group_value: None,
                branch_name: None,
                repo_root: None,
                commit_count: 4,
                heavy_commit_count: 2,
                github_pr_heavy_eligible: 2,
                github_pr_heavy_ready: 2,
                pr_reach_rate: RatioMetric {
                    numerator: 1,
                    denominator: 2,
                },
                merge_rate: RatioMetric {
                    numerator: 1,
                    denominator: 2,
                },
                pr_merge_rate: RatioMetric {
                    numerator: 1,
                    denominator: 1,
                },
                github_pr_metrics_available: true,
                task_branch_lines_added: 70,
                task_branch_lines_removed: 20,
            },
            ChangeReportRow {
                week_start: Some("2026-04-20".to_string()),
                group_value: None,
                branch_name: None,
                repo_root: None,
                commit_count: 5,
                heavy_commit_count: 4,
                github_pr_heavy_eligible: 2,
                github_pr_heavy_ready: 2,
                pr_reach_rate: RatioMetric {
                    numerator: 2,
                    denominator: 2,
                },
                merge_rate: RatioMetric {
                    numerator: 1,
                    denominator: 4,
                },
                pr_merge_rate: RatioMetric {
                    numerator: 0,
                    denominator: 2,
                },
                github_pr_metrics_available: true,
                task_branch_lines_added: 120,
                task_branch_lines_removed: 30,
            },
        ];
        app.reports.quality = vec![LifecycleReportRow {
            week_start: None,
            group_value: Some("codex/gpt-5.4".to_string()),
            branch_name: None,
            heavy_commit_count: 6,
            code_churn_rate: RatioMetric {
                numerator: 45,
                denominator: 100,
            },
            bug_after_merge_rate: RatioMetric {
                numerator: 2,
                denominator: 6,
            },
            revert_rate: RatioMetric {
                numerator: 1,
                denominator: 6,
            },
        }];
        app
    }

    fn render_text(app: &App) -> String {
        let backend = TestBackend::new(120, 34);
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
    fn renders_overview_e2e_buffer_with_date_chart_and_risk_rows() {
        let app = sample_app();

        let text = render_text(&app);

        assert!(text.contains("Overview"));
        assert!(text.contains("Last 30d"));
        assert!(text.contains("Risk Count By Bucket"));
        assert!(text.contains("Needs Attention"));
        assert!(text.contains("codex/gpt-5.4"));
        assert!(text.contains("No output"));
    }

    #[test]
    fn renders_session_chart_after_keyboard_metric_selection() {
        let mut app = sample_app();
        app.active_tab = Tab::Session;

        app.on_key(event::KeyEvent::new(KeyCode::Char('3'), KeyModifiers::NONE))
            .expect("metric key");
        let text = render_text(&app);

        assert!(text.contains("Grouped Debug loops By model"));
        assert!(text.contains("Rows"));
        assert!(text.contains("Loop"));
        assert!(text.contains("40.0%"));
    }

    #[test]
    fn renders_delivery_volume_metrics_as_weekly_over_time_chart() {
        let mut app = sample_app();
        app.active_tab = Tab::Delivery;
        app.weekly = true;

        app.on_key(event::KeyEvent::new(KeyCode::Char('4'), KeyModifiers::NONE))
            .expect("loc metric key");
        let text = render_text(&app);

        assert!(text.contains("LOC Over Time"));
        assert!(text.contains("2026-04-13"));
        assert!(text.contains("2026-04-20"));
        assert!(text.contains("Commits"));
        assert!(text.contains("PRs"));
        assert!(text.contains("LOC"));
        assert!(text.contains("150.0"));
    }

    #[test]
    fn drilldown_state_applies_selected_bucket_and_moves_to_next_breakdown() {
        let mut app = sample_app();
        app.active_tab = Tab::Session;
        app.group_by = GroupBy::Model;
        app.selected = 0;

        assert!(app.drilldown_state());

        assert_eq!(app.group_by, GroupBy::Provider);
        assert_eq!(app.filters.len(), 1);
        assert_eq!(app.filters[0].kind, GroupBy::Model);
        assert_eq!(app.filters[0].value, "codex/gpt-5.4");
    }

    #[test]
    fn back_state_pops_drilldown_filter_and_restores_breakdown() {
        let mut app = sample_app();
        app.active_tab = Tab::Delivery;
        app.group_by = GroupBy::Repo;
        app.filters.push(FilterEntry {
            kind: GroupBy::Model,
            value: "codex/gpt-5.4".to_string(),
        });

        assert!(app.back_state());

        assert_eq!(app.group_by, GroupBy::Model);
        assert!(app.filters.is_empty());
    }

    #[test]
    fn resolves_last_30d_as_inclusive_window() {
        let today = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let range = resolve_date_preset(DatePreset::Last30d, today, None);
        assert_eq!(range.from, NaiveDate::from_ymd_opt(2026, 3, 29).unwrap());
        assert_eq!(range.to, today);
    }

    #[test]
    fn resolves_this_week_from_monday_to_today() {
        let today = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let range = resolve_date_preset(DatePreset::ThisWeek, today, None);
        assert_eq!(range.from, NaiveDate::from_ymd_opt(2026, 4, 27).unwrap());
        assert_eq!(range.to, today);
    }

    #[test]
    fn resolves_last_week_to_previous_monday_sunday() {
        let today = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let range = resolve_date_preset(DatePreset::LastWeek, today, None);
        assert_eq!(range.from, NaiveDate::from_ymd_opt(2026, 4, 20).unwrap());
        assert_eq!(range.to, NaiveDate::from_ymd_opt(2026, 4, 26).unwrap());
    }

    #[test]
    fn picks_expected_drilldowns() {
        assert_eq!(
            next_drill_group(Tab::Session, GroupBy::Model),
            Some(GroupBy::Provider)
        );
        assert_eq!(
            next_drill_group(Tab::Session, GroupBy::Repo),
            Some(GroupBy::Branch)
        );
        assert_eq!(
            next_drill_group(Tab::Delivery, GroupBy::Model),
            Some(GroupBy::Repo)
        );
        assert_eq!(
            next_drill_group(Tab::Quality, GroupBy::Task),
            Some(GroupBy::Branch)
        );
    }

    #[test]
    fn selected_branch_uses_branch_name() {
        assert_eq!(
            selected_group_value(GroupBy::Branch, Some("task"), Some("feature/demo")),
            Some("feature/demo".to_string())
        );
    }
}
