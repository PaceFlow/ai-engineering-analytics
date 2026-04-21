use std::io::{IsTerminal, Write};
use std::time::Instant;

use terminal_size::{Width, terminal_size};

use crate::change_intel::pipeline::ProviderCodeChangePlan;
use crate::providers::ProviderSessionPlan;

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

    fn replace_future_units(&mut self, _old_units: usize, _new_units: usize) {}
}

#[derive(Debug, Clone)]
struct StageState {
    label: String,
    started_at_units: usize,
    planned_units: usize,
}

pub struct IngestProgress {
    total_units: usize,
    completed_units: usize,
    started_at: Instant,
    tty: bool,
    stage: Option<StageState>,
    current_item: Option<String>,
    line_active: bool,
    last_render_width: usize,
}

impl IngestProgress {
    pub fn new(plan: &IngestExecutionPlan) -> Self {
        Self::new_with_tty(plan.total_units.max(1), std::io::stdout().is_terminal())
    }

    pub fn new_for_total_units(total_units: usize) -> Self {
        Self::new_with_tty(total_units.max(1), std::io::stdout().is_terminal())
    }

    fn new_with_tty(total_units: usize, tty: bool) -> Self {
        Self {
            total_units,
            completed_units: 0,
            started_at: Instant::now(),
            tty,
            stage: None,
            current_item: None,
            line_active: false,
            last_render_width: 0,
        }
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
        let Some(stage) = self.stage.take() else {
            return;
        };

        let target_units = stage.started_at_units + stage.planned_units;
        if self.completed_units < target_units {
            self.completed_units = target_units;
            self.current_item = Some("done".to_string());
            self.render();
        }

        self.current_item = None;
        if self.tty && self.line_active {
            println!();
            let _ = std::io::stdout().flush();
            self.line_active = false;
            self.last_render_width = 0;
        }
    }

    pub fn finish(&mut self) {
        if self.tty {
            if self.line_active {
                println!();
                self.line_active = false;
            }
            return;
        }

        println!(
            "Ingest progress: {} ({}/{}) elapsed={}",
            self.percent_label(),
            self.completed_units,
            self.total_units,
            format_elapsed(self.started_at.elapsed().as_millis())
        );
    }

    pub fn replace_future_units(&mut self, old_units: usize, new_units: usize) {
        self.total_units = self
            .total_units
            .saturating_sub(old_units)
            .saturating_add(new_units);
        self.total_units = self.total_units.max(self.completed_units).max(1);
        self.render();
    }

    fn start_stage(&mut self, label: String, planned_units: usize) {
        self.finish_stage();
        self.stage = Some(StageState {
            label: label.clone(),
            started_at_units: self.completed_units,
            planned_units,
        });
        self.current_item = None;

        if !self.tty {
            println!("Stage: {}", label);
        }

        self.render();
    }

    fn advance(&mut self, delta: usize, item_label: &str) {
        self.completed_units = self
            .completed_units
            .saturating_add(delta)
            .min(self.total_units);
        self.current_item = Some(item_label.to_string());
        self.render();
    }

    fn percent_complete(&self) -> usize {
        if self.total_units == 0 {
            100
        } else {
            ((self.completed_units as f64 / self.total_units as f64) * 100.0).round() as usize
        }
    }

    fn percent_label(&self) -> String {
        format!("{:>3}%", self.percent_complete())
    }

    fn render(&mut self) {
        if !self.tty {
            return;
        }

        let width = terminal_size()
            .map(|(Width(width), _)| width as usize)
            .unwrap_or(100)
            .max(60);
        let line = self.render_line(width);
        let pad = self.last_render_width.saturating_sub(line.len());
        print!("\r{}{}", line, " ".repeat(pad));
        let _ = std::io::stdout().flush();
        self.line_active = true;
        self.last_render_width = line.len();
    }

    fn render_line(&self, width: usize) -> String {
        let stage_label = self
            .stage
            .as_ref()
            .map(|stage| stage.label.as_str())
            .unwrap_or("Ingest");
        let elapsed = format_elapsed(self.started_at.elapsed().as_millis());
        let status = format!(
            "{} {}/{} {}",
            self.percent_label(),
            self.completed_units,
            self.total_units,
            elapsed
        );
        let prefix = format!("{} ", truncate(stage_label, 24));
        let suffix = format!(" {}", status);
        let reserved = prefix.len() + suffix.len() + 3;
        let bar_width = width.saturating_sub(reserved).clamp(10, 40);
        let filled = if self.total_units == 0 {
            bar_width
        } else {
            ((self.completed_units as f64 / self.total_units as f64) * bar_width as f64).round()
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
}

pub struct StageProgress<'a> {
    progress: &'a mut IngestProgress,
}

impl IngestProgressObserver for StageProgress<'_> {
    fn advance(&mut self, item_label: &str) {
        self.progress.advance(1, item_label);
    }

    fn replace_future_units(&mut self, old_units: usize, new_units: usize) {
        self.progress.replace_future_units(old_units, new_units);
    }
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
        assert_eq!(progress.completed_units, 3);
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
        let mut progress = IngestProgress::new_with_tty(10, true);
        progress.start_stage("Sessions".to_string(), 10);
        progress.advance(1, "/tmp/repo/src/main.rs");

        let line = progress.render_line(80);
        assert!(line.contains("Sessions"));
        assert!(line.contains("10%"));
        assert!(!line.contains("/tmp/repo/src/main.rs"));
    }
}
