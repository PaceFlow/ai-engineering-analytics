use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use terminal_size::{Width, terminal_size};

use crate::change_intel::pipeline::ProviderCodeChangePlan;
use crate::providers::ProviderSessionPlan;

/// How often the TTY ticker repaints the progress line when nothing else
/// drives a render.
const DEFAULT_TTY_TICK: Duration = Duration::from_millis(200);
/// How often the non-TTY ticker emits a `...still working` heartbeat line so
/// piped logs don't look frozen during long warmup phases.
const DEFAULT_NONTTY_HEARTBEAT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ProviderWorkPlan {
    pub provider_name: String,
    pub session_plan: ProviderSessionPlan,
    pub code_change_plan: ProviderCodeChangePlan,
}

impl ProviderWorkPlan {
    pub fn session_units(&self) -> usize {
        self.session_plan.item_count()
    }

    pub fn code_change_units(&self) -> usize {
        self.code_change_plan.progress_unit_count()
    }
}

#[derive(Debug, Clone)]
pub struct IngestExecutionPlan {
    pub provider_plans: Vec<ProviderWorkPlan>,
    pub association_units_estimate: usize,
    pub commit_materialization_units_estimate: usize,
    pub total_units: usize,
}

impl IngestExecutionPlan {
    pub fn new(
        provider_plans: Vec<ProviderWorkPlan>,
        association_units_estimate: usize,
        commit_materialization_units_estimate: usize,
    ) -> Self {
        let provider_units = provider_plans
            .iter()
            .map(|plan| plan.session_units() + plan.code_change_units())
            .sum::<usize>();
        let total_units =
            provider_units + 1 + association_units_estimate + commit_materialization_units_estimate;

        Self {
            provider_plans,
            association_units_estimate,
            commit_materialization_units_estimate,
            total_units,
        }
    }

    pub fn provider_plan(&self, provider_name: &str) -> Option<&ProviderWorkPlan> {
        self.provider_plans
            .iter()
            .find(|plan| plan.provider_name == provider_name)
    }
}

pub trait IngestProgressObserver {
    fn advance(&mut self, item_label: &str);

    /// Label an opaque sub-phase so the progress line can say "what" we are
    /// doing even when the unit counter is not moving. Default no-op so
    /// existing observers (codex, github, …) compile without edits.
    fn set_phase(&mut self, _phase: &str) {}

    fn replace_future_units(&mut self, _old_units: usize, _new_units: usize) {}
}

#[derive(Debug, Clone)]
struct StageState {
    label: String,
    started_at_units: usize,
    planned_units: usize,
}

#[derive(Debug)]
struct ProgressInner {
    total_units: usize,
    completed_units: usize,
    stage: Option<StageState>,
    current_item: Option<String>,
    current_phase: Option<String>,
    line_active: bool,
    last_render_width: usize,
}

pub struct IngestProgress {
    started_at: Instant,
    tty: bool,
    inner: Arc<Mutex<ProgressInner>>,
    done: Arc<AtomicBool>,
    #[cfg_attr(not(test), allow(dead_code))]
    tick_count: Arc<AtomicU64>,
    ticker: Option<JoinHandle<()>>,
}

impl IngestProgress {
    pub fn new(plan: &IngestExecutionPlan) -> Self {
        Self::new_with_tty(plan.total_units.max(1), std::io::stdout().is_terminal())
    }

    pub fn new_for_total_units(total_units: usize) -> Self {
        Self::new_with_tty(total_units.max(1), std::io::stdout().is_terminal())
    }

    fn new_with_tty(total_units: usize, tty: bool) -> Self {
        Self::new_with_tty_intervals(total_units, tty, DEFAULT_TTY_TICK, DEFAULT_NONTTY_HEARTBEAT)
    }

    fn new_with_tty_intervals(
        total_units: usize,
        tty: bool,
        tick_interval: Duration,
        heartbeat_interval: Duration,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ProgressInner {
            total_units,
            completed_units: 0,
            stage: None,
            current_item: None,
            current_phase: None,
            line_active: false,
            last_render_width: 0,
        }));
        let done = Arc::new(AtomicBool::new(false));
        let tick_count = Arc::new(AtomicU64::new(0));
        let started_at = Instant::now();
        let ticker = Self::spawn_ticker(
            Arc::clone(&inner),
            Arc::clone(&done),
            Arc::clone(&tick_count),
            started_at,
            tty,
            tick_interval,
            heartbeat_interval,
        );
        Self {
            started_at,
            tty,
            inner,
            done,
            tick_count,
            ticker,
        }
    }

    fn spawn_ticker(
        inner: Arc<Mutex<ProgressInner>>,
        done: Arc<AtomicBool>,
        tick_count: Arc<AtomicU64>,
        started_at: Instant,
        tty: bool,
        tick: Duration,
        heartbeat: Duration,
    ) -> Option<JoinHandle<()>> {
        thread::Builder::new()
            .name("paceflow-progress-ticker".into())
            .spawn(move || {
                let mut last_heartbeat = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    // `park_timeout` is like `sleep(tick)` except that
                    // `stop_ticker()` can wake us up immediately via
                    // `thread().unpark()`. Without this, `Drop` would block
                    // for the full tick duration — fine in prod (200ms) but
                    // fatal in tests that use very large intervals to
                    // "disable" the ticker.
                    thread::park_timeout(tick);
                    tick_count.fetch_add(1, Ordering::Relaxed);
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let Ok(mut guard) = inner.lock() else {
                        return;
                    };
                    if tty {
                        if guard.line_active {
                            render_tty_locked(&mut guard, started_at);
                        }
                    } else if guard.stage.is_some() && last_heartbeat.elapsed() >= heartbeat {
                        emit_heartbeat_locked(&guard, started_at);
                        last_heartbeat = Instant::now();
                    }
                }
            })
            .ok()
    }

    pub fn stage<'a>(
        &'a mut self,
        label: impl Into<String>,
        planned_units: usize,
    ) -> StageProgress<'a> {
        self.start_stage(label.into(), planned_units);
        StageProgress { progress: self }
    }

    pub fn finish_stage(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        let Some(stage) = inner.stage.take() else {
            return;
        };

        let target_units = stage.started_at_units + stage.planned_units;
        if inner.completed_units < target_units {
            inner.completed_units = target_units;
            inner.current_item = Some("done".to_string());
            if self.tty {
                render_tty_locked(&mut inner, self.started_at);
            }
        }

        inner.current_item = None;
        inner.current_phase = None;
        if self.tty && inner.line_active {
            println!();
            let _ = std::io::stdout().flush();
            inner.line_active = false;
            inner.last_render_width = 0;
        }
    }

    pub fn finish(&mut self) {
        if self.tty {
            let mut inner = self.inner.lock().unwrap();
            if inner.line_active {
                println!();
                inner.line_active = false;
            }
        } else {
            let inner = self.inner.lock().unwrap();
            println!(
                "Ingest progress: {} ({}/{}) elapsed={}",
                percent_label_of(inner.completed_units, inner.total_units),
                inner.completed_units,
                inner.total_units,
                format_elapsed(self.started_at.elapsed().as_millis())
            );
        }
        self.stop_ticker();
    }

    fn stop_ticker(&mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.ticker.take() {
            // Wake the ticker thread out of `park_timeout` so `join` returns
            // immediately instead of waiting for the full tick interval.
            handle.thread().unpark();
            let _ = handle.join();
        }
    }

    pub fn replace_future_units(&mut self, old_units: usize, new_units: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.total_units = inner
            .total_units
            .saturating_sub(old_units)
            .saturating_add(new_units);
        inner.total_units = inner.total_units.max(inner.completed_units).max(1);
        if self.tty {
            render_tty_locked(&mut inner, self.started_at);
        }
    }

    fn start_stage(&mut self, label: String, planned_units: usize) {
        self.finish_stage();
        let mut inner = self.inner.lock().unwrap();
        let started_at_units = inner.completed_units;
        inner.stage = Some(StageState {
            label: label.clone(),
            started_at_units,
            planned_units,
        });
        inner.current_item = None;
        inner.current_phase = None;

        if !self.tty {
            println!("Stage: {}", label);
        } else {
            render_tty_locked(&mut inner, self.started_at);
        }
    }

    fn advance(&mut self, delta: usize, item_label: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.completed_units = inner
            .completed_units
            .saturating_add(delta)
            .min(inner.total_units);
        inner.current_item = Some(item_label.to_string());
        if self.tty {
            render_tty_locked(&mut inner, self.started_at);
        }
    }

    fn set_phase(&mut self, phase: &str) {
        let mut inner = self.inner.lock().unwrap();
        if phase.is_empty() {
            inner.current_phase = None;
        } else {
            inner.current_phase = Some(phase.to_string());
        }
        if self.tty {
            render_tty_locked(&mut inner, self.started_at);
        }
    }

    #[cfg(test)]
    fn completed_units(&self) -> usize {
        self.inner.lock().unwrap().completed_units
    }

    #[cfg(test)]
    fn percent_complete(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        percent_complete_of(inner.completed_units, inner.total_units)
    }

    #[cfg(test)]
    fn render_line_for_tests(&self, width: usize) -> String {
        let inner = self.inner.lock().unwrap();
        render_line(&inner, self.started_at, width)
    }

    #[cfg(test)]
    fn tick_count(&self) -> u64 {
        self.tick_count.load(Ordering::Relaxed)
    }
}

impl Drop for IngestProgress {
    fn drop(&mut self) {
        self.stop_ticker();
    }
}

pub struct StageProgress<'a> {
    progress: &'a mut IngestProgress,
}

impl IngestProgressObserver for StageProgress<'_> {
    fn advance(&mut self, item_label: &str) {
        self.progress.advance(1, item_label);
    }

    fn set_phase(&mut self, phase: &str) {
        self.progress.set_phase(phase);
    }

    fn replace_future_units(&mut self, old_units: usize, new_units: usize) {
        self.progress.replace_future_units(old_units, new_units);
    }
}

fn render_tty_locked(inner: &mut ProgressInner, started_at: Instant) {
    let width = terminal_size()
        .map(|(Width(width), _)| width as usize)
        .unwrap_or(100)
        .max(60);
    let line = render_line(inner, started_at, width);
    let pad = inner.last_render_width.saturating_sub(line.len());
    print!("\r{}{}", line, " ".repeat(pad));
    let _ = std::io::stdout().flush();
    inner.line_active = true;
    inner.last_render_width = line.len();
}

fn emit_heartbeat_locked(inner: &ProgressInner, started_at: Instant) {
    let stage_label = inner
        .stage
        .as_ref()
        .map(|stage| stage.label.as_str())
        .unwrap_or("Ingest");
    let elapsed = format_elapsed(started_at.elapsed().as_millis());
    match inner.current_phase.as_deref() {
        Some(phase) if !phase.is_empty() => {
            println!("  ...still working: {stage_label} [{phase}] (elapsed={elapsed})");
        }
        _ => {
            println!("  ...still working: {stage_label} (elapsed={elapsed})");
        }
    }
    let _ = std::io::stdout().flush();
}

fn percent_complete_of(completed: usize, total: usize) -> usize {
    if total == 0 {
        100
    } else {
        ((completed as f64 / total as f64) * 100.0).round() as usize
    }
}

fn percent_label_of(completed: usize, total: usize) -> String {
    format!("{:>3}%", percent_complete_of(completed, total))
}

fn render_line(inner: &ProgressInner, started_at: Instant, width: usize) -> String {
    let stage_label = inner
        .stage
        .as_ref()
        .map(|stage| stage.label.as_str())
        .unwrap_or("Ingest");
    let elapsed = format_elapsed(started_at.elapsed().as_millis());
    let status = format!(
        "{} {}/{} {}",
        percent_label_of(inner.completed_units, inner.total_units),
        inner.completed_units,
        inner.total_units,
        elapsed
    );
    let prefix = format!("{} ", truncate(stage_label, 24));
    let phase_suffix = match inner.current_phase.as_deref() {
        Some(phase) if !phase.is_empty() => format!("  {}", truncate(phase, 28)),
        _ => String::new(),
    };
    let suffix = format!(" {status}{phase_suffix}");
    let reserved = prefix.len() + suffix.len() + 3;
    let bar_width = width.saturating_sub(reserved).clamp(10, 40);
    let filled = if inner.total_units == 0 {
        bar_width
    } else {
        ((inner.completed_units as f64 / inner.total_units as f64) * bar_width as f64).round()
            as usize
    }
    .min(bar_width);
    let bar = format!(
        "[{}{}]",
        "=".repeat(filled),
        " ".repeat(bar_width.saturating_sub(filled))
    );

    format!("{prefix}{bar}{suffix}")
}

fn truncate(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        return value.to_string();
    }

    let keep = max_len.saturating_sub(1);
    let mut out = String::new();
    for ch in value.chars().take(keep) {
        out.push(ch);
    }
    out.push('~');
    out
}

fn format_elapsed(elapsed_ms: u128) -> String {
    format!("{:.1}s", elapsed_ms as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_intel::commit_assoc::types::{AssociationWorkPlan, RepoAssociationPlan};

    #[test]
    fn percent_is_monotonic() {
        let mut progress = IngestProgress::new_with_tty(4, false);
        progress.start_stage("sessions".to_string(), 4);
        assert_eq!(progress.percent_complete(), 0);
        progress.advance(1, "one");
        assert_eq!(progress.percent_complete(), 25);
        progress.advance(1, "two");
        assert_eq!(progress.percent_complete(), 50);
        progress.advance(2, "done");
        assert_eq!(progress.percent_complete(), 100);
    }

    #[test]
    fn skipped_items_still_advance_when_stage_finishes() {
        let mut progress = IngestProgress::new_with_tty(3, false);
        progress.start_stage("sessions".to_string(), 3);
        progress.advance(1, "first");
        progress.finish_stage();
        assert_eq!(progress.completed_units(), 3);
        assert_eq!(progress.percent_complete(), 100);
    }

    #[test]
    fn repo_without_commits_still_counts_as_work() {
        let plan = AssociationWorkPlan {
            repo_plans: vec![RepoAssociationPlan {
                repo_root: "/tmp/repo".to_string(),
                commits: Vec::new(),
                ..Default::default()
            }],
        };

        assert_eq!(plan.total_units(), 2);
    }

    #[test]
    fn render_line_omits_current_item_details() {
        let mut progress = IngestProgress::new_with_tty_intervals(
            10,
            false,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        progress.start_stage("Sessions".to_string(), 10);
        progress.advance(1, "/tmp/repo/src/main.rs");

        let line = progress.render_line_for_tests(80);
        assert!(line.contains("Sessions"));
        assert!(line.contains("10%"));
        assert!(!line.contains("/tmp/repo/src/main.rs"));
    }

    #[test]
    fn set_phase_updates_render_line_without_advancing() {
        let mut progress = IngestProgress::new_with_tty_intervals(
            10,
            false,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        progress.start_stage("cursor sessions".to_string(), 10);
        progress.advance(1, "session-1");
        let completed_before = progress.completed_units();

        progress.set_phase("bubbles");
        let line = progress.render_line_for_tests(100);
        assert_eq!(progress.completed_units(), completed_before);
        assert!(line.contains("cursor sessions"));
        assert!(line.contains("bubbles"));

        progress.set_phase("");
        let cleared = progress.render_line_for_tests(100);
        assert!(!cleared.contains("bubbles"));
    }

    #[test]
    fn ticker_keeps_ticking_without_events() {
        let progress = IngestProgress::new_with_tty_intervals(
            10,
            false,
            Duration::from_millis(20),
            Duration::from_secs(3600),
        );
        let before = progress.tick_count();

        // Poll with a generous upper bound so slow CI schedulers do not flake
        // the test just because the first few ticks get starved. 2 seconds is
        // plenty for a 20ms-interval ticker to fire at least twice.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let after = progress.tick_count();
            if after >= before + 2 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "ticker did not fire twice within 2s (before={before}, latest={after})"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
