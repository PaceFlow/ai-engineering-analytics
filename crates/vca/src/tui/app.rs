//! TUI state: a navigation stack of frames, key handling and drill-down.
//!
//! Every frame owns its filters, sort, grouping, selection and scroll offset.
//! `Enter` pushes a frame whose filters are the parent's plus the selected
//! constraint; `Esc` pops back to the parent exactly as it was.

use std::cell::Cell;

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use super::data::{Action, Data, Loader, Query};
use crate::classify::questions::FEEDBACK;
use crate::classify::run::RunSummary;
use crate::classify::state::Level;
use crate::explore::{
    Bucket, ContextDim, CostBand, Filters, GroupDim, QuotaWindowRow, SessionSort, TaskSort,
    merge_filters,
};

pub type TaskKey = (String, String, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Sessions,
    Tasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailTab {
    Conversation,
    Tasks,
    Agents,
    Usage,
    Context,
    Delivery,
}

impl DetailTab {
    pub const ALL: [DetailTab; 6] = [
        Self::Conversation,
        Self::Tasks,
        Self::Agents,
        Self::Usage,
        Self::Context,
        Self::Delivery,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Conversation => "Conversation",
            Self::Tasks => "Tasks",
            Self::Agents => "Agents",
            Self::Usage => "Usage",
            Self::Context => "Context",
            Self::Delivery => "Code & Delivery",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageTab {
    Timeline,
    Windows,
    Context,
}

impl UsageTab {
    pub const ALL: [UsageTab; 3] = [Self::Timeline, Self::Windows, Self::Context];

    pub fn label(self) -> &'static str {
        match self {
            Self::Timeline => "Timeline",
            Self::Windows => "Quota windows",
            Self::Context => "Context & Tools",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Overview,
    List {
        unit: Unit,
        sort: usize,
    },
    Groups {
        dim: usize,
    },
    Detail {
        provider: String,
        session: String,
        tab: DetailTab,
        focus_task: Option<i64>,
    },
    Usage {
        tab: UsageTab,
        bucket: Bucket,
        ctx_dim: usize,
    },
    Window {
        window: Box<QuotaWindowRow>,
    },
    ContextItems {
        group: Option<(ContextDim, String)>,
    },
    Delivery,
    Practices,
    Compare {
        keys: Vec<TaskKey>,
    },
}

impl Screen {
    pub fn title(&self) -> String {
        match self {
            Self::Overview => "Overview".into(),
            Self::List {
                unit: Unit::Sessions,
                ..
            } => "Sessions".into(),
            Self::List {
                unit: Unit::Tasks, ..
            } => "Tasks".into(),
            Self::Groups { dim } => format!("Groups({})", GroupDim::ALL[*dim].label()),
            Self::Detail { session, .. } => {
                format!("Session {}", crate::explore::short_id(session))
            }
            Self::Usage { .. } => "Usage & Quota".into(),
            Self::Window { window } => format!("{} {}", window.provider, window.name),
            Self::ContextItems {
                group: Some((_, key)),
            } => format!("Items: {}", crate::explore::format::truncate(key, 24)),
            Self::ContextItems { group: None } => "Context items".into(),
            Self::Delivery => "Delivery & Rework".into(),
            Self::Practices => "Practices".into(),
            Self::Compare { keys } => format!("Compare ({})", keys.len()),
        }
    }
}

pub struct Frame {
    pub screen: Screen,
    pub filters: Filters,
    pub selected: usize,
    pub offset: Cell<usize>,
    pub panel: usize,
    pub data: Option<Data>,
    pub request: u64,
    pub loading: bool,
    pub error: Option<String>,
    /// Secondary data (e.g. contributors under a window list).
    pub extra: Option<Data>,
    pub extra_request: u64,
}

impl Frame {
    pub fn new(screen: Screen, filters: Filters) -> Self {
        Self {
            screen,
            filters,
            selected: 0,
            offset: Cell::new(0),
            panel: 0,
            data: None,
            request: 0,
            loading: false,
            error: None,
            extra: None,
            extra_request: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Popup {
    Help,
    Explain {
        title: String,
        lines: Vec<String>,
    },
    Filters {
        field: usize,
        editing: Option<String>,
    },
    Search {
        text: String,
    },
    /// Correct labels, task boundaries or completion (`e`).
    Edit {
        target: EditTarget,
        field: usize,
        choice: usize,
    },
    /// Consent screen for sending a selection to the classifier (`K`).
    Classify {
        sessions: Vec<(String, String)>,
        level: Level,
        max_usd: f64,
        plan: Option<Box<RunSummary>>,
        pending: Option<&'static str>,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EditTarget {
    Task {
        provider: String,
        session: String,
        task: i64,
        turns: Vec<i64>,
        intent: Option<String>,
        outcome: Option<String>,
    },
    Turn {
        provider: String,
        session: String,
        turn: i64,
        task: Option<i64>,
        tasks: i64,
        feedback: Option<String>,
    },
}

pub const CLEAR: &str = "(remove my label)";

impl EditTarget {
    pub fn title(&self) -> String {
        match self {
            Self::Task { task, session, .. } => {
                format!("T{task} · {}", crate::explore::short_id(session))
            }
            Self::Turn { turn, session, .. } => {
                format!("turn {turn} · {}", crate::explore::short_id(session))
            }
        }
    }

    /// Editable fields with their options; the last option clears the override.
    pub fn fields(&self) -> Vec<(&'static str, Vec<String>)> {
        let owned = |labels: &[&str]| -> Vec<String> {
            labels
                .iter()
                .map(|l| l.to_string())
                .chain([CLEAR.to_string()])
                .collect()
        };
        match self {
            Self::Task { turns, .. } => {
                let mut fields = vec![("intent", owned(&INTENTS)), ("outcome", owned(&OUTCOMES))];
                if !turns.is_empty() {
                    let options = turns
                        .iter()
                        .map(|turn| format!("turn {turn}"))
                        .chain(["not completed".to_string(), CLEAR.to_string()])
                        .collect();
                    fields.push(("completed at", options));
                }
                fields
            }
            Self::Turn { tasks, .. } => {
                let feedback: Vec<&str> = FEEDBACK.iter().map(|(name, _)| *name).collect();
                let task_options = (0..*tasks)
                    .map(|task| format!("T{task}"))
                    .chain(["new task".to_string(), CLEAR.to_string()])
                    .collect();
                vec![
                    ("feedback", owned(&feedback)),
                    ("starts task", task_options),
                ]
            }
        }
    }

    /// Option index of the current value of each field.
    pub fn current(&self, field: usize) -> usize {
        let fields = self.fields();
        let Some((_, options)) = fields.get(field) else {
            return 0;
        };
        let find = |value: Option<&str>| {
            value
                .and_then(|v| options.iter().position(|o| o == v))
                .unwrap_or(0)
        };
        match (self, field) {
            (Self::Task { intent, .. }, 0) => find(intent.as_deref()),
            (Self::Task { outcome, .. }, 1) => find(outcome.as_deref()),
            (Self::Turn { feedback, .. }, 0) => find(feedback.as_deref()),
            (Self::Turn { task, .. }, 1) => task.map(|t| t as usize).unwrap_or(0),
            _ => 0,
        }
    }

    pub fn action(&self, field: usize, choice: usize) -> Option<Action> {
        let fields = self.fields();
        let (name, options) = fields.get(field)?;
        let option = options.get(choice)?.clone();
        let cleared = option == CLEAR;
        Some(match self {
            Self::Task {
                provider,
                session,
                task,
                ..
            } => {
                if *name == "completed at" {
                    let completion = if cleared {
                        None
                    } else if option == "not completed" {
                        Some(None)
                    } else {
                        Some(option.strip_prefix("turn ")?.parse().ok())
                    };
                    Action::SetCompletion {
                        provider: provider.clone(),
                        session: session.clone(),
                        task: *task,
                        completion,
                    }
                } else {
                    Action::SetLabel {
                        unit_kind: "task".into(),
                        unit_key: crate::classify::heuristic::task_unit_key(
                            provider, session, *task,
                        ),
                        question: name.to_string(),
                        label: (!cleared).then_some(option),
                    }
                }
            }
            Self::Turn {
                provider,
                session,
                turn,
                tasks,
                ..
            } => {
                if *name == "starts task" {
                    let task = if cleared {
                        None
                    } else if option == "new task" {
                        Some(*tasks)
                    } else {
                        Some(option.strip_prefix('T')?.parse::<i64>().ok()?)
                    };
                    Action::AssignTurn {
                        provider: provider.clone(),
                        session: session.clone(),
                        turn: *turn,
                        task,
                    }
                } else {
                    Action::SetLabel {
                        unit_kind: "exchange".into(),
                        unit_key: crate::classify::heuristic::turn_unit_key(
                            provider, session, *turn,
                        ),
                        question: "feedback".into(),
                        label: (!cleared).then_some(option),
                    }
                }
            }
        })
    }
}

pub const TIME_PRESETS: [(u32, &str); 4] = [(7, "7d"), (30, "30d"), (90, "90d"), (0, "all")];
pub const PROVIDERS: [&str; 4] = ["claude", "codex", "cursor", "opencode"];
pub const INTENTS: [&str; 12] = [
    "feature",
    "bug_fix",
    "debug_investigate",
    "refactor",
    "tests",
    "research_explain",
    "planning_design",
    "review",
    "ops_config",
    "docs",
    "data_scripting",
    "other",
];
pub const OUTCOMES: [&str; 8] = [
    "completed_verified",
    "completed_unverified",
    "answered",
    "partial",
    "stalled",
    "abandoned_redirected",
    "blocked_external",
    "unclear",
];
pub const FILTER_FIELDS: [&str; 9] = [
    "Time",
    "Tools",
    "Model",
    "Project",
    "Search",
    "Intent",
    "Outcome",
    "Cost band",
    "Min cost",
];

pub struct App {
    pub frames: Vec<Frame>,
    pub loader: Box<dyn Loader>,
    pub next_request: u64,
    pub popup: Option<Popup>,
    pub marked: Vec<TaskKey>,
    pub message: Option<String>,
    pub should_quit: bool,
    pub time_preset: usize,
    pub title: &'static str,
    /// Request id of the latest write or classifier action.
    pub action_request: u64,
}

pub fn date_from_days(days: u32) -> Option<String> {
    (days > 0).then(|| {
        (chrono::Local::now().date_naive() - chrono::Duration::days(days as i64 - 1))
            .format("%Y-%m-%d")
            .to_string()
    })
}

impl App {
    pub fn new(loader: Box<dyn Loader>, base: Filters, title: &'static str) -> Self {
        let mut filters = base;
        if filters.from.is_none() {
            filters.from = date_from_days(30);
        }
        let mut app = Self {
            frames: Vec::new(),
            loader,
            next_request: 1,
            popup: None,
            marked: Vec::new(),
            message: None,
            should_quit: false,
            time_preset: 1,
            title,
            action_request: 0,
        };
        app.push(Frame::new(Screen::Overview, filters));
        app
    }

    pub fn frame(&self) -> &Frame {
        self.frames.last().expect("at least one frame")
    }

    fn frame_mut(&mut self) -> &mut Frame {
        self.frames.last_mut().expect("at least one frame")
    }

    pub fn push(&mut self, frame: Frame) {
        self.frames.push(frame);
        self.load();
    }

    fn pop(&mut self) {
        if self.frames.len() > 1 {
            self.frames.pop();
        }
    }

    pub fn breadcrumbs(&self) -> String {
        self.frames
            .iter()
            .map(|frame| frame.screen.title())
            .collect::<Vec<_>>()
            .join(" › ")
    }

    /// The query for the current frame's main data.
    pub fn query_for(frame: &Frame) -> Query {
        let filters = frame.filters.clone();
        match &frame.screen {
            Screen::Overview => Query::Overview(filters),
            Screen::List {
                unit: Unit::Sessions,
                sort,
            } => Query::Sessions(filters, SessionSort::ALL[*sort % SessionSort::ALL.len()]),
            Screen::List {
                unit: Unit::Tasks,
                sort,
            } => Query::Tasks(filters, TaskSort::ALL[*sort % TaskSort::ALL.len()]),
            Screen::Groups { dim } => {
                Query::Groups(filters, GroupDim::ALL[*dim % GroupDim::ALL.len()])
            }
            Screen::Detail {
                provider, session, ..
            } => Query::Detail(provider.clone(), session.clone()),
            Screen::Usage {
                tab: UsageTab::Timeline,
                bucket,
                ..
            } => Query::Usage(filters, *bucket),
            Screen::Usage {
                tab: UsageTab::Windows,
                ..
            } => Query::Windows(filters),
            Screen::Usage {
                tab: UsageTab::Context,
                ctx_dim,
                ..
            } => Query::ContextGroups(filters, ContextDim::ALL[*ctx_dim % ContextDim::ALL.len()]),
            Screen::Window { window } => Query::Contributors((**window).clone()),
            Screen::ContextItems { group } => Query::ContextItems(filters, group.clone()),
            Screen::Delivery => Query::Ladder(filters),
            Screen::Practices => Query::Practices(filters),
            Screen::Compare { keys } => Query::Compare(keys.clone()),
        }
    }

    pub fn load(&mut self) {
        let id = self.next_request;
        self.next_request += 1;
        let query = Self::query_for(self.frame());
        let frame = self.frame_mut();
        frame.request = id;
        frame.loading = true;
        frame.error = None;
        self.loader.request(id, query);
        self.drain();
    }

    /// Apply finished results to the frames that asked for them.
    pub fn drain(&mut self) {
        for (id, result) in self.loader.poll() {
            if id == self.action_request {
                self.on_action_result(result);
                continue;
            }
            for frame in self.frames.iter_mut() {
                if frame.request == id {
                    frame.loading = false;
                    match result {
                        Ok(data) => {
                            frame.data = Some(data);
                            frame.error = None;
                        }
                        Err(ref error) => frame.error = Some(error.clone()),
                    }
                    break;
                }
            }
        }
        let count = self.row_count();
        let frame = self.frame_mut();
        if count > 0 && frame.selected >= count {
            frame.selected = count - 1;
        }
    }

    pub fn row_count(&self) -> usize {
        let frame = self.frame();
        let Some(data) = frame.data.as_ref() else {
            return 0;
        };
        match (&frame.screen, data) {
            (Screen::Overview, Data::Overview(o)) => match frame.panel {
                0 => o.by_tool.len(),
                1 => o.windows.len(),
                2 => o.intents.len(),
                3 => o.outcomes.len(),
                _ => o.top_sessions.len(),
            },
            (_, Data::Sessions(rows)) => rows.len(),
            (_, Data::Tasks(rows)) => rows.len(),
            (_, Data::Groups(rows)) => rows.len(),
            (Screen::Detail { tab, .. }, Data::Detail(Some(detail))) => match tab {
                DetailTab::Conversation => detail.turns.len(),
                DetailTab::Tasks => detail.tasks.len() + detail.attempts.len(),
                DetailTab::Agents => detail.agents.len(),
                DetailTab::Usage => detail.models.len(),
                DetailTab::Context => detail.context_items.len(),
                DetailTab::Delivery => detail.commits.len(),
            },
            (_, Data::Usage(buckets, _)) => usage_rows(buckets).len(),
            (_, Data::Windows(rows)) => rows.len(),
            (_, Data::Contributors(rows)) => rows.len(),
            (_, Data::ContextGroups(rows)) => rows.len(),
            (_, Data::ContextItems(rows)) => rows.len(),
            (_, Data::Ladder(rows)) => rows.len(),
            (_, Data::Practices(rows)) => rows.len(),
            (_, Data::Compare(columns)) => columns.first().map(|c| c.values.len()).unwrap_or(0),
            _ => 0,
        }
    }

    fn top_level(&mut self, screen: Screen) {
        let filters = self.frames[0].filters.clone();
        self.frames.clear();
        self.push(Frame::new(screen, filters));
    }

    pub fn on_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }
        self.message = None;
        if self.popup.is_some() {
            self.on_popup_key(key);
            return;
        }
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.popup = Some(Popup::Help),
            KeyCode::Char('1') => self.top_level(Screen::Overview),
            KeyCode::Char('2') => self.top_level(Screen::List {
                unit: Unit::Sessions,
                sort: 0,
            }),
            KeyCode::Char('3') => self.top_level(Screen::Groups { dim: 0 }),
            KeyCode::Char('4') => self.top_level(Screen::Usage {
                tab: UsageTab::Timeline,
                bucket: Bucket::Day,
                ctx_dim: 1,
            }),
            KeyCode::Char('5') => self.top_level(Screen::Delivery),
            KeyCode::Char('6') => self.top_level(Screen::Practices),
            KeyCode::Char('7') => {
                if self.marked.len() >= 2 {
                    let keys = self.marked.clone();
                    self.push(Frame::new(
                        Screen::Compare { keys },
                        self.frame().filters.clone(),
                    ));
                } else {
                    self.message =
                        Some("Mark at least two rows with m, then press c or 7 to compare".into());
                }
            }
            KeyCode::Char('H') => {
                self.frames.truncate(1);
                if self.frame().screen != Screen::Overview {
                    self.top_level(Screen::Overview);
                }
            }
            KeyCode::Esc | KeyCode::Backspace => self.pop(),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::PageDown => self.move_selection(15),
            KeyCode::PageUp => self.move_selection(-15),
            KeyCode::Home => self.frame_mut().selected = 0,
            KeyCode::End => {
                let count = self.row_count();
                self.frame_mut().selected = count.saturating_sub(1);
            }
            KeyCode::Tab => self.cycle_tab(1),
            KeyCode::BackTab => self.cycle_tab(-1),
            KeyCode::Enter => self.drill(),
            KeyCode::Char(']') => self.sibling(1),
            KeyCode::Char('[') => self.sibling(-1),
            KeyCode::Char('s') => self.cycle_sort(),
            KeyCode::Char('u') => self.toggle_unit(),
            KeyCode::Char('g') => self.cycle_group(if shift { -1 } else { 1 }),
            KeyCode::Char('G') => self.cycle_group(-1),
            KeyCode::Char('b') => self.toggle_bucket(),
            KeyCode::Char('t') => self.cycle_time(),
            KeyCode::Char('f') => {
                self.popup = Some(Popup::Filters {
                    field: 0,
                    editing: None,
                })
            }
            KeyCode::Char('/') => {
                let text = self.frame().filters.search.clone().unwrap_or_default();
                self.popup = Some(Popup::Search { text });
            }
            KeyCode::Char('m') => self.toggle_mark(),
            KeyCode::Char('c') => {
                if self.marked.len() >= 2 {
                    let keys = self.marked.clone();
                    self.push(Frame::new(
                        Screen::Compare { keys },
                        self.frame().filters.clone(),
                    ));
                } else {
                    self.message = Some("Mark at least two sessions or tasks with m first".into());
                }
            }
            KeyCode::Char('x') => self.explain(),
            KeyCode::Char('o') => self.drill_alternative(),
            KeyCode::Char('R') => self.load(),
            KeyCode::Char('M') => self.log_limit_hit(),
            KeyCode::Char('e') => self.open_edit(),
            KeyCode::Char('K') => self.open_classify(),
            _ => {}
        }
    }

    fn move_selection(&mut self, delta: i64) {
        let count = self.row_count();
        let frame = self.frame_mut();
        if count == 0 {
            frame.selected = 0;
            return;
        }
        let next = (frame.selected as i64 + delta).clamp(0, count as i64 - 1);
        frame.selected = next as usize;
    }

    fn cycle_tab(&mut self, delta: i64) {
        let frame = self.frame_mut();
        let cycle =
            |index: usize, len: usize| ((index as i64 + delta).rem_euclid(len as i64)) as usize;
        let reload = match &mut frame.screen {
            Screen::Detail { tab, .. } => {
                let index = DetailTab::ALL.iter().position(|t| t == tab).unwrap_or(0);
                *tab = DetailTab::ALL[cycle(index, DetailTab::ALL.len())];
                frame.selected = 0;
                false
            }
            Screen::Usage { tab, .. } => {
                let index = UsageTab::ALL.iter().position(|t| t == tab).unwrap_or(0);
                *tab = UsageTab::ALL[cycle(index, UsageTab::ALL.len())];
                frame.selected = 0;
                frame.data = None;
                true
            }
            Screen::Overview => {
                frame.panel = cycle(frame.panel, 5);
                frame.selected = 0;
                false
            }
            _ => false,
        };
        if reload {
            self.load();
        }
    }

    fn cycle_sort(&mut self) {
        let frame = self.frame_mut();
        if let Screen::List { sort, unit } = &mut frame.screen {
            let len = match unit {
                Unit::Sessions => SessionSort::ALL.len(),
                Unit::Tasks => TaskSort::ALL.len(),
            };
            *sort = (*sort + 1) % len;
            self.load();
        }
    }

    pub fn sort_label(&self) -> Option<&'static str> {
        match &self.frame().screen {
            Screen::List {
                unit: Unit::Sessions,
                sort,
            } => Some(SessionSort::ALL[*sort % SessionSort::ALL.len()].label()),
            Screen::List {
                unit: Unit::Tasks,
                sort,
            } => Some(TaskSort::ALL[*sort % TaskSort::ALL.len()].label()),
            _ => None,
        }
    }

    fn toggle_unit(&mut self) {
        let frame = self.frame_mut();
        if let Screen::List { unit, sort } = &mut frame.screen {
            *unit = match unit {
                Unit::Sessions => Unit::Tasks,
                Unit::Tasks => Unit::Sessions,
            };
            *sort = 0;
            frame.selected = 0;
            self.load();
        }
    }

    fn cycle_group(&mut self, delta: i64) {
        let frame = self.frame_mut();
        let cycle =
            |index: usize, len: usize| ((index as i64 + delta).rem_euclid(len as i64)) as usize;
        match &mut frame.screen {
            Screen::Groups { dim } => {
                *dim = cycle(*dim, GroupDim::ALL.len());
                frame.selected = 0;
                self.load();
            }
            Screen::Usage {
                tab: UsageTab::Context,
                ctx_dim,
                ..
            } => {
                *ctx_dim = cycle(*ctx_dim, ContextDim::ALL.len());
                frame.selected = 0;
                self.load();
            }
            _ => {}
        }
    }

    fn toggle_bucket(&mut self) {
        let frame = self.frame_mut();
        if let Screen::Usage {
            bucket,
            tab: UsageTab::Timeline,
            ..
        } = &mut frame.screen
        {
            *bucket = match bucket {
                Bucket::Day => Bucket::Hour,
                Bucket::Hour => Bucket::Day,
            };
            frame.selected = 0;
            self.load();
        }
    }

    fn cycle_time(&mut self) {
        self.time_preset = (self.time_preset + 1) % TIME_PRESETS.len();
        let (days, label) = TIME_PRESETS[self.time_preset];
        let frame = self.frame_mut();
        frame.filters.from = date_from_days(days);
        frame.filters.to = None;
        self.message = Some(format!("time range: {label}"));
        self.load();
    }

    fn toggle_mark(&mut self) {
        let Some(key) = self.selected_task_key() else {
            self.message = Some("Select a session or task to mark".into());
            return;
        };
        if let Some(index) = self.marked.iter().position(|k| *k == key) {
            self.marked.remove(index);
        } else {
            self.marked.push(key);
        }
        self.message = Some(format!("{} marked for compare (c)", self.marked.len()));
    }

    /// Task key of the selected row, when the row is a session or task.
    pub fn selected_task_key(&self) -> Option<TaskKey> {
        let frame = self.frame();
        match frame.data.as_ref()? {
            Data::Sessions(rows) => rows
                .get(frame.selected)
                .map(|r| (r.provider.clone(), r.session_id.clone(), 0)),
            Data::Tasks(rows) => rows
                .get(frame.selected)
                .map(|r| (r.provider.clone(), r.session_id.clone(), r.task_index)),
            Data::Detail(Some(detail)) => {
                if let Screen::Detail {
                    tab: DetailTab::Tasks,
                    ..
                } = frame.screen
                {
                    detail
                        .tasks
                        .get(frame.selected)
                        .map(|t| (t.provider.clone(), t.session_id.clone(), t.task_index))
                } else {
                    Some((
                        detail.session.provider.clone(),
                        detail.session.session_id.clone(),
                        0,
                    ))
                }
            }
            _ => None,
        }
    }

    pub fn is_marked(&self, provider: &str, session: &str, task: i64) -> bool {
        self.marked
            .iter()
            .any(|(p, s, t)| p == provider && s == session && *t == task)
    }

    fn child(&self, screen: Screen, extra: Filters) -> Frame {
        Frame::new(screen, merge_filters(&self.frame().filters, &extra))
    }

    fn drill(&mut self) {
        let frame = self.frame();
        let selected = frame.selected;
        let Some(data) = frame.data.as_ref() else {
            return;
        };
        let next: Option<Frame> = match (&frame.screen, data) {
            (Screen::Overview, Data::Overview(o)) => match frame.panel {
                0 => o.by_tool.get(selected).map(|row| {
                    self.child(
                        Screen::List {
                            unit: Unit::Sessions,
                            sort: 0,
                        },
                        Filters {
                            providers: vec![row.key.clone()],
                            ..Filters::default()
                        },
                    )
                }),
                1 => o.windows.get(selected).map(|window| {
                    Frame::new(
                        Screen::Window {
                            window: Box::new(window.clone()),
                        },
                        frame.filters.clone(),
                    )
                }),
                2 => o.intents.get(selected).map(|g| {
                    self.child(
                        Screen::List {
                            unit: Unit::Tasks,
                            sort: 0,
                        },
                        g.filter.clone(),
                    )
                }),
                3 => o.outcomes.get(selected).map(|g| {
                    self.child(
                        Screen::List {
                            unit: Unit::Tasks,
                            sort: 0,
                        },
                        g.filter.clone(),
                    )
                }),
                _ => o
                    .top_sessions
                    .get(selected)
                    .map(|row| detail_frame(&frame.filters, &row.provider, &row.session_id, None)),
            },
            (_, Data::Sessions(rows)) => rows
                .get(selected)
                .map(|row| detail_frame(&frame.filters, &row.provider, &row.session_id, None)),
            (_, Data::Tasks(rows)) => rows.get(selected).map(|row| {
                detail_frame(
                    &frame.filters,
                    &row.provider,
                    &row.session_id,
                    Some(row.task_index),
                )
            }),
            (Screen::Groups { .. }, Data::Groups(rows)) => rows.get(selected).map(|g| {
                Frame::new(
                    Screen::List {
                        unit: Unit::Tasks,
                        sort: 0,
                    },
                    g.filter.clone(),
                )
            }),
            (
                Screen::Usage {
                    tab: UsageTab::Timeline,
                    bucket,
                    ..
                },
                Data::Usage(buckets, _),
            ) => usage_rows(buckets).get(selected).map(|(key, _)| {
                let day: String = key.chars().take(10).collect();
                let _ = bucket;
                self.child(
                    Screen::List {
                        unit: Unit::Sessions,
                        sort: 0,
                    },
                    Filters {
                        from: Some(day.clone()),
                        to: Some(day),
                        ..Filters::default()
                    },
                )
            }),
            (_, Data::Windows(rows)) => rows.get(selected).map(|window| {
                Frame::new(
                    Screen::Window {
                        window: Box::new(window.clone()),
                    },
                    frame.filters.clone(),
                )
            }),
            (_, Data::Contributors(rows)) => rows
                .get(selected)
                .map(|row| detail_frame(&frame.filters, &row.provider, &row.root_session_id, None)),
            (Screen::Usage { ctx_dim, .. }, Data::ContextGroups(rows)) => {
                rows.get(selected).map(|row| {
                    Frame::new(
                        Screen::ContextItems {
                            group: Some((
                                ContextDim::ALL[*ctx_dim % ContextDim::ALL.len()],
                                row.key.clone(),
                            )),
                        },
                        frame.filters.clone(),
                    )
                })
            }
            (_, Data::ContextItems(rows)) => rows.get(selected).map(|item| {
                let root = item
                    .session_id
                    .split('/')
                    .next()
                    .unwrap_or(&item.session_id)
                    .to_string();
                let mut detail = detail_frame(&frame.filters, &item.provider, &root, None);
                if let Screen::Detail { tab, .. } = &mut detail.screen {
                    *tab = DetailTab::Context;
                }
                detail
            }),
            (_, Data::Ladder(rows)) => rows.get(selected).map(|row| {
                self.child(
                    Screen::List {
                        unit: Unit::Tasks,
                        sort: 0,
                    },
                    Filters {
                        task_keys: row.task_keys.clone(),
                        task_keys_label: Some(format!("intent {}", row.intent)),
                        ..Filters::default()
                    },
                )
            }),
            (_, Data::Practices(rows)) => rows
                .get(selected)
                .filter(|row| !row.with_keys.is_empty())
                .map(|row| {
                    Frame::new(
                        Screen::List {
                            unit: Unit::Tasks,
                            sort: 0,
                        },
                        Filters {
                            task_keys: row.with_keys.clone(),
                            task_keys_label: Some(format!("{}: with", row.id)),
                            ..frame.filters.clone()
                        },
                    )
                }),
            (Screen::Detail { .. }, Data::Detail(Some(_))) => {
                self.explain();
                None
            }
            _ => None,
        };
        if let Some(next) = next {
            self.push(next);
        }
    }

    /// `o`: open the comparison group of a practice ("without").
    fn drill_alternative(&mut self) {
        let frame = self.frame();
        if let Some(Data::Practices(rows)) = frame.data.as_ref()
            && let Some(row) = rows.get(frame.selected)
            && !row.without_keys.is_empty()
        {
            let next = Frame::new(
                Screen::List {
                    unit: Unit::Tasks,
                    sort: 0,
                },
                Filters {
                    task_keys: row.without_keys.clone(),
                    task_keys_label: Some(format!("{}: without", row.id)),
                    ..frame.filters.clone()
                },
            );
            self.push(next);
        }
    }

    /// `[` / `]` in a detail view: open the previous/next row of the parent list.
    fn sibling(&mut self, delta: i64) {
        if self.frames.len() < 2 {
            return;
        }
        let parent_index = self.frames.len() - 2;
        let count = {
            let parent = &self.frames[parent_index];
            match parent.data.as_ref() {
                Some(Data::Sessions(rows)) => rows.len(),
                Some(Data::Tasks(rows)) => rows.len(),
                Some(Data::Contributors(rows)) => rows.len(),
                _ => 0,
            }
        };
        if count == 0 {
            return;
        }
        let parent = &mut self.frames[parent_index];
        let next = (parent.selected as i64 + delta).clamp(0, count as i64 - 1) as usize;
        if next == parent.selected {
            return;
        }
        parent.selected = next;
        let (provider, session, task) = match parent.data.as_ref() {
            Some(Data::Sessions(rows)) => (
                rows[next].provider.clone(),
                rows[next].session_id.clone(),
                None,
            ),
            Some(Data::Tasks(rows)) => (
                rows[next].provider.clone(),
                rows[next].session_id.clone(),
                Some(rows[next].task_index),
            ),
            Some(Data::Contributors(rows)) => (
                rows[next].provider.clone(),
                rows[next].root_session_id.clone(),
                None,
            ),
            _ => return,
        };
        let tab = match &self.frame().screen {
            Screen::Detail { tab, .. } => *tab,
            _ => return,
        };
        let filters = self.frames[parent_index].filters.clone();
        self.frames.pop();
        let mut frame = detail_frame(&filters, &provider, &session, task);
        if let Screen::Detail { tab: current, .. } = &mut frame.screen {
            *current = tab;
        }
        self.push(frame);
    }

    /// Run a write or classifier action on the loader thread.
    pub fn act(&mut self, action: Action) {
        let id = self.next_request;
        self.next_request += 1;
        self.action_request = id;
        self.loader.request(id, Query::Action(action));
        self.drain();
    }

    fn on_action_result(&mut self, result: Result<Data, String>) {
        let classify = matches!(self.popup, Some(Popup::Classify { .. }));
        match result {
            Ok(Data::ClassifyPlan(summary)) => {
                if let Some(Popup::Classify { plan, pending, .. }) = self.popup.as_mut() {
                    *plan = Some(summary);
                    *pending = None;
                }
            }
            Ok(Data::Done(text)) => {
                let preview = matches!(
                    self.popup,
                    Some(Popup::Classify {
                        pending: Some("preview"),
                        ..
                    })
                );
                if preview {
                    self.popup = Some(Popup::Explain {
                        title: "Exact request for the first task (labels pass)".into(),
                        lines: text.lines().map(str::to_string).collect(),
                    });
                    return;
                }
                if classify {
                    self.popup = None;
                }
                self.message = Some(text);
                self.load();
            }
            Ok(_) => {}
            Err(error) => {
                if let Some(Popup::Classify {
                    error: slot,
                    pending,
                    ..
                }) = self.popup.as_mut()
                {
                    *slot = Some(error);
                    *pending = None;
                } else {
                    self.message = Some(format!("Could not save: {error}"));
                }
            }
        }
    }

    /// What `e` edits for the selected row, if anything.
    pub fn edit_target(&self) -> Option<EditTarget> {
        let frame = self.frame();
        match (&frame.screen, frame.data.as_ref()?) {
            (Screen::Detail { tab, .. }, Data::Detail(Some(detail))) => match tab {
                DetailTab::Tasks => {
                    let task = detail.tasks.get(frame.selected)?;
                    let turns = detail
                        .turns
                        .iter()
                        .filter(|turn| turn.task_index == Some(task.task_index))
                        .map(|turn| turn.turn_index)
                        .collect();
                    Some(EditTarget::Task {
                        provider: task.provider.clone(),
                        session: task.session_id.clone(),
                        task: task.task_index,
                        turns,
                        intent: task.intent.clone(),
                        outcome: task.outcome.clone(),
                    })
                }
                DetailTab::Conversation => {
                    let turn = detail.turns.get(frame.selected)?;
                    Some(EditTarget::Turn {
                        provider: detail.session.provider.clone(),
                        session: turn.session_id.clone(),
                        turn: turn.turn_index,
                        task: turn.task_index,
                        tasks: detail.tasks.len() as i64,
                        feedback: turn.feedback.clone(),
                    })
                }
                _ => None,
            },
            (_, Data::Tasks(rows)) => {
                let task = rows.get(frame.selected)?;
                Some(EditTarget::Task {
                    provider: task.provider.clone(),
                    session: task.session_id.clone(),
                    task: task.task_index,
                    turns: Vec::new(),
                    intent: task.intent.clone(),
                    outcome: task.outcome.clone(),
                })
            }
            _ => None,
        }
    }

    fn open_edit(&mut self) {
        match self.edit_target() {
            Some(target) => {
                let choice = target.current(0);
                self.popup = Some(Popup::Edit {
                    target,
                    field: 0,
                    choice,
                });
            }
            None => {
                self.message = Some(
                    "e edits a task (Tasks list, session Tasks tab) or a turn (Conversation tab)"
                        .into(),
                )
            }
        }
    }

    /// Sessions `K` would classify: marked rows, else the selected one.
    pub fn classify_selection(&self) -> Vec<(String, String)> {
        let mut sessions: Vec<(String, String)> = Vec::new();
        let mut add = |provider: &str, session: &str| {
            let key = (provider.to_string(), session.to_string());
            if !sessions.contains(&key) {
                sessions.push(key);
            }
        };
        if !self.marked.is_empty() {
            for (provider, session, _) in &self.marked {
                add(provider, session);
            }
            return sessions;
        }
        let frame = self.frame();
        if let Screen::Detail {
            provider, session, ..
        } = &frame.screen
        {
            add(provider, session);
        } else if let Some((provider, session, _)) = self.selected_task_key() {
            add(&provider, &session);
        }
        sessions
    }

    fn open_classify(&mut self) {
        let sessions = self.classify_selection();
        if sessions.is_empty() {
            self.message =
                Some("Select a session or task (or mark several with m), then press K".into());
            return;
        }
        let level = crate::classify::config::ClassifierConfig::load()
            .map(|config| config.level())
            .unwrap_or(Level::Conversation);
        self.popup = Some(Popup::Classify {
            sessions: sessions.clone(),
            level,
            max_usd: 0.10,
            plan: None,
            pending: Some("plan"),
            error: None,
        });
        self.act(Action::ClassifyPlan { sessions, level });
    }

    fn log_limit_hit(&mut self) {
        let frame = self.frame();
        let provider = match (frame.data.as_ref(), &frame.screen) {
            (Some(Data::Windows(rows)), _) => rows.get(frame.selected).map(|w| w.provider.clone()),
            (_, Screen::Window { window }) => Some(window.provider.clone()),
            _ => None,
        };
        let Some(provider) = provider else {
            self.message =
                Some("Select a quota window (4, Tab to Quota windows) to log a limit hit".into());
            return;
        };
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let result = crate::db::open().and_then(|conn| {
            crate::insights::quota::record_observation(
                &conn,
                &provider,
                &now,
                "limit_hit",
                Some("logged from tui"),
            )
        });
        self.message = Some(match result {
            Ok(()) => format!("Logged a {provider} limit hit at {now}"),
            Err(error) => format!("Could not log: {error}"),
        });
        self.load();
    }

    pub fn explain(&mut self) {
        if let Some((title, lines)) = super::explain::explain(self) {
            self.popup = Some(Popup::Explain { title, lines });
        }
    }

    fn on_popup_key(&mut self, key: KeyEvent) {
        let popup = self.popup.take().expect("popup present");
        match popup {
            Popup::Edit {
                target,
                mut field,
                mut choice,
            } => {
                let fields = target.fields();
                let options = fields.get(field).map(|(_, o)| o.len()).unwrap_or(1).max(1);
                match key.code {
                    KeyCode::Esc | KeyCode::Char('e') => return,
                    KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => {
                        field = (field + 1) % fields.len().max(1);
                        choice = target.current(field);
                    }
                    KeyCode::Up | KeyCode::Char('k') | KeyCode::BackTab => {
                        field = (field + fields.len().max(1) - 1) % fields.len().max(1);
                        choice = target.current(field);
                    }
                    KeyCode::Right | KeyCode::Char('l') | KeyCode::Char(' ') => {
                        choice = (choice + 1) % options
                    }
                    KeyCode::Left | KeyCode::Char('h') => choice = (choice + options - 1) % options,
                    KeyCode::Enter => {
                        if let Some(action) = target.action(field, choice) {
                            self.act(action);
                        }
                        return;
                    }
                    _ => {}
                }
                self.popup = Some(Popup::Edit {
                    target,
                    field,
                    choice,
                });
            }
            Popup::Classify {
                sessions,
                mut level,
                mut max_usd,
                plan,
                pending,
                error,
            } => {
                let busy = pending.is_some();
                match key.code {
                    KeyCode::Esc | KeyCode::Char('K') => {
                        if pending == Some("run") {
                            self.message = Some(
                                "Classification keeps running; results appear when it finishes"
                                    .into(),
                            );
                        }
                    }
                    KeyCode::Left | KeyCode::Right | KeyCode::Char('l') if !busy => {
                        const LEVELS: [Level; 4] = [
                            Level::EvidenceOnly,
                            Level::UserText,
                            Level::Conversation,
                            Level::Full,
                        ];
                        let index = LEVELS.iter().position(|l| *l == level).unwrap_or(2);
                        let next = if key.code == KeyCode::Left {
                            index + 3
                        } else {
                            index + 1
                        };
                        level = LEVELS[next % 4];
                        self.popup = Some(Popup::Classify {
                            sessions: sessions.clone(),
                            level,
                            max_usd,
                            plan: None,
                            pending: Some("plan"),
                            error: None,
                        });
                        self.act(Action::ClassifyPlan { sessions, level });
                    }
                    KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('-') if !busy => {
                        max_usd = if key.code == KeyCode::Char('-') {
                            (max_usd / 2.0).max(0.01)
                        } else {
                            (max_usd * 2.0).min(5.0)
                        };
                        self.popup = Some(Popup::Classify {
                            sessions,
                            level,
                            max_usd,
                            plan,
                            pending,
                            error,
                        });
                    }
                    KeyCode::Char('p') if !busy => {
                        let first = sessions[0].clone();
                        self.popup = Some(Popup::Classify {
                            sessions,
                            level,
                            max_usd,
                            plan,
                            pending: Some("preview"),
                            error: None,
                        });
                        self.act(Action::ClassifyPreview {
                            session: first,
                            level,
                        });
                    }
                    KeyCode::Enter if !busy => {
                        self.popup = Some(Popup::Classify {
                            sessions: sessions.clone(),
                            level,
                            max_usd,
                            plan,
                            pending: Some("run"),
                            error: None,
                        });
                        self.act(Action::ClassifyRun {
                            sessions,
                            level,
                            max_usd,
                        });
                    }
                    _ => {
                        self.popup = Some(Popup::Classify {
                            sessions,
                            level,
                            max_usd,
                            plan,
                            pending,
                            error,
                        })
                    }
                }
            }
            Popup::Help | Popup::Explain { .. } => match key.code {
                KeyCode::Esc
                | KeyCode::Char('q')
                | KeyCode::Char('?')
                | KeyCode::Char('x')
                | KeyCode::Enter => {}
                _ => self.popup = Some(popup),
            },
            Popup::Search { mut text } => match key.code {
                KeyCode::Esc => {}
                KeyCode::Enter => {
                    let trimmed = text.trim().to_string();
                    self.frame_mut().filters.search = (!trimmed.is_empty()).then_some(trimmed);
                    self.frame_mut().selected = 0;
                    self.load();
                }
                KeyCode::Backspace => {
                    text.pop();
                    self.popup = Some(Popup::Search { text });
                }
                KeyCode::Char(c) => {
                    text.push(c);
                    self.popup = Some(Popup::Search { text });
                }
                _ => self.popup = Some(Popup::Search { text }),
            },
            Popup::Filters {
                field,
                editing: Some(mut buffer),
            } => match key.code {
                KeyCode::Esc => {
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: None,
                    })
                }
                KeyCode::Enter => {
                    self.set_text_filter(field, buffer.trim());
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: None,
                    });
                }
                KeyCode::Backspace => {
                    buffer.pop();
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: Some(buffer),
                    });
                }
                KeyCode::Char(c) => {
                    buffer.push(c);
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: Some(buffer),
                    });
                }
                _ => {
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: Some(buffer),
                    })
                }
            },
            Popup::Filters {
                field,
                editing: None,
            } => match key.code {
                KeyCode::Esc | KeyCode::Char('f') => {
                    self.frame_mut().selected = 0;
                    self.load();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.popup = Some(Popup::Filters {
                        field: (field + 1) % FILTER_FIELDS.len(),
                        editing: None,
                    })
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.popup = Some(Popup::Filters {
                        field: (field + FILTER_FIELDS.len() - 1) % FILTER_FIELDS.len(),
                        editing: None,
                    })
                }
                KeyCode::Char('d') => {
                    self.clear_filter(field);
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: None,
                    });
                }
                KeyCode::Char('D') => {
                    for index in 0..FILTER_FIELDS.len() {
                        self.clear_filter(index);
                    }
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: None,
                    });
                }
                KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right | KeyCode::Left => {
                    let back = key.code == KeyCode::Left;
                    if let Some(editing) = self.cycle_filter(field, back) {
                        self.popup = Some(Popup::Filters {
                            field,
                            editing: Some(editing),
                        });
                    } else {
                        self.popup = Some(Popup::Filters {
                            field,
                            editing: None,
                        });
                    }
                }
                _ => {
                    self.popup = Some(Popup::Filters {
                        field,
                        editing: None,
                    })
                }
            },
        }
    }

    /// Cycle an enumerated filter, or return the current text to edit.
    fn cycle_filter(&mut self, field: usize, back: bool) -> Option<String> {
        let step = |index: Option<usize>, len: usize| -> Option<usize> {
            match (index, back) {
                (None, false) => Some(0),
                (None, true) => Some(len - 1),
                (Some(i), false) if i + 1 < len => Some(i + 1),
                (Some(i), true) if i > 0 => Some(i - 1),
                _ => None,
            }
        };
        let filters = &mut self.frame_mut().filters;
        match field {
            0 => {
                self.time_preset = (self.time_preset
                    + if back { TIME_PRESETS.len() - 1 } else { 1 })
                    % TIME_PRESETS.len();
                let days = TIME_PRESETS[self.time_preset].0;
                let filters = &mut self.frame_mut().filters;
                filters.from = date_from_days(days);
                filters.to = None;
                None
            }
            1 => {
                let current = PROVIDERS
                    .iter()
                    .position(|p| filters.providers == vec![p.to_string()]);
                filters.providers = step(current, PROVIDERS.len())
                    .map(|i| vec![PROVIDERS[i].to_string()])
                    .unwrap_or_default();
                None
            }
            5 => {
                let current = filters
                    .intent
                    .as_deref()
                    .and_then(|i| INTENTS.iter().position(|x| *x == i));
                filters.intent = step(current, INTENTS.len()).map(|i| INTENTS[i].to_string());
                None
            }
            6 => {
                let current = filters
                    .outcome
                    .as_deref()
                    .and_then(|o| OUTCOMES.iter().position(|x| *x == o));
                filters.outcome = step(current, OUTCOMES.len()).map(|i| OUTCOMES[i].to_string());
                None
            }
            7 => {
                let current = filters
                    .cost_band
                    .and_then(|band| CostBand::ALL.iter().position(|b| *b == band));
                filters.cost_band = step(current, CostBand::ALL.len()).map(|i| CostBand::ALL[i]);
                None
            }
            2 => Some(filters.model.clone().unwrap_or_default()),
            3 => Some(filters.repo.clone().unwrap_or_default()),
            4 => Some(filters.search.clone().unwrap_or_default()),
            8 => Some(filters.min_cost.map(|v| v.to_string()).unwrap_or_default()),
            _ => None,
        }
    }

    fn set_text_filter(&mut self, field: usize, value: &str) {
        let value = (!value.is_empty()).then(|| value.to_string());
        let filters = &mut self.frame_mut().filters;
        match field {
            2 => filters.model = value,
            3 => filters.repo = value,
            4 => filters.search = value,
            8 => filters.min_cost = value.and_then(|v| v.parse().ok()),
            _ => {}
        }
    }

    fn clear_filter(&mut self, field: usize) {
        let filters = &mut self.frame_mut().filters;
        match field {
            0 => {
                filters.from = None;
                filters.to = None;
                self.time_preset = 3;
            }
            1 => filters.providers.clear(),
            2 => filters.model = None,
            3 => filters.repo = None,
            4 => filters.search = None,
            5 => filters.intent = None,
            6 => filters.outcome = None,
            7 => filters.cost_band = None,
            8 => filters.min_cost = None,
            _ => {}
        }
    }

    pub fn filter_value(&self, field: usize) -> String {
        let filters = &self.frame().filters;
        let or_any = |value: Option<String>| value.unwrap_or_else(|| "any".into());
        match field {
            0 => match (&filters.from, &filters.to) {
                (None, None) => "all time".into(),
                (Some(from), None) => format!("since {from}"),
                (from, to) => format!(
                    "{}..{}",
                    from.clone().unwrap_or_default(),
                    to.clone().unwrap_or_default()
                ),
            },
            1 => {
                if filters.providers.is_empty() {
                    "all tools".into()
                } else {
                    filters.providers.join(",")
                }
            }
            2 => or_any(filters.model.clone()),
            3 => or_any(filters.repo.clone()),
            4 => or_any(filters.search.clone()),
            5 => or_any(filters.intent.clone()),
            6 => or_any(filters.outcome.clone()),
            7 => or_any(filters.cost_band.map(|b| b.label().to_string())),
            8 => or_any(filters.min_cost.map(|v| format!("${v}"))),
            _ => String::new(),
        }
    }
}

pub fn detail_frame(filters: &Filters, provider: &str, session: &str, task: Option<i64>) -> Frame {
    let mut frame = Frame::new(
        Screen::Detail {
            provider: provider.to_string(),
            session: session.to_string(),
            tab: if task.is_some() {
                DetailTab::Tasks
            } else {
                DetailTab::Conversation
            },
            focus_task: task,
        },
        filters.clone(),
    );
    if let Some(task) = task {
        frame.selected = task.max(0) as usize;
    }
    frame
}

/// Timeline rows: one per bucket, providers summed.
pub fn usage_rows(
    buckets: &[crate::explore::UsageBucket],
) -> Vec<(String, Vec<&crate::explore::UsageBucket>)> {
    let mut rows: Vec<(String, Vec<&crate::explore::UsageBucket>)> = Vec::new();
    for bucket in buckets {
        match rows.last_mut() {
            Some((key, items)) if *key == bucket.bucket => items.push(bucket),
            _ => rows.push((bucket.bucket.clone(), vec![bucket])),
        }
    }
    rows.reverse();
    rows
}
