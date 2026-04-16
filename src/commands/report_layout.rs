use crate::analytics::RatioMetric;
use crate::cli::GroupBy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricStatus {
    Good,
    Watch,
    Risk,
    Unavailable,
}

impl MetricStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Good => "good",
            Self::Watch => "watch",
            Self::Risk => "risk",
            Self::Unavailable => "unavailable",
        }
    }
}

pub struct ScorecardRow<'a> {
    pub label: &'a str,
    pub value: String,
    pub status: MetricStatus,
}

pub fn classify_lower_better(
    value: Option<f64>,
    good_below: f64,
    watch_through: f64,
) -> MetricStatus {
    match value {
        Some(value) if value < good_below => MetricStatus::Good,
        Some(value) if value <= watch_through => MetricStatus::Watch,
        Some(_) => MetricStatus::Risk,
        None => MetricStatus::Unavailable,
    }
}

pub fn classify_higher_better(
    value: Option<f64>,
    good_at_least: f64,
    watch_at_least: f64,
) -> MetricStatus {
    match value {
        Some(value) if value >= good_at_least => MetricStatus::Good,
        Some(value) if value >= watch_at_least => MetricStatus::Watch,
        Some(_) => MetricStatus::Risk,
        None => MetricStatus::Unavailable,
    }
}

pub fn classify_ratio_lower_better(
    metric: &RatioMetric,
    good_below: f64,
    watch_through: f64,
) -> MetricStatus {
    classify_lower_better(metric.percent(), good_below, watch_through)
}

pub fn classify_ratio_higher_better(
    metric: &RatioMetric,
    good_at_least: f64,
    watch_at_least: f64,
) -> MetricStatus {
    classify_higher_better(metric.percent(), good_at_least, watch_at_least)
}

pub fn fmt_opt_decimal(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format!("{value:.precision$}"))
        .unwrap_or_else(|| "N/A".to_string())
}

pub fn fmt_ratio(metric: &RatioMetric, precision: usize) -> String {
    match metric.percent() {
        Some(value) => format!(
            "{value:.precision$}% ({}/{})",
            metric.numerator, metric.denominator
        ),
        None => "N/A".to_string(),
    }
}

pub fn fmt_ratio_percent(metric: &RatioMetric, precision: usize) -> String {
    match metric.percent() {
        Some(value) => format!("{value:.precision$}%"),
        None => "N/A".to_string(),
    }
}

pub fn render_scorecard(rows: &[ScorecardRow<'_>]) -> String {
    let headers = ["Signal", "Value", "Status"];
    let widths = [
        rows.iter()
            .map(|row| row.label.chars().count())
            .max()
            .unwrap_or(0)
            .max(headers[0].chars().count()),
        rows.iter()
            .map(|row| row.value.chars().count())
            .max()
            .unwrap_or(0)
            .max(headers[1].chars().count()),
        rows.iter()
            .map(|row| row.status.label().chars().count())
            .max()
            .unwrap_or(0)
            .max(headers[2].chars().count()),
    ];

    let mut out = String::new();
    append_border(&mut out, '┌', '┬', '┐', &widths);
    append_row(&mut out, &headers, &widths);
    append_border(&mut out, '├', '┼', '┤', &widths);
    for row in rows {
        append_row(
            &mut out,
            &[row.label, row.value.as_str(), row.status.label()],
            &widths,
        );
    }
    append_border(&mut out, '└', '┴', '┘', &widths);
    out
}

pub fn append_legend(out: &mut String, lines: &[&str]) {
    out.push('\n');
    out.push_str("Legend:\n");
    for line in lines {
        out.push_str("- ");
        out.push_str(line);
        out.push('\n');
    }
}

pub fn group_label(group_by: Option<GroupBy>) -> &'static str {
    match group_by {
        Some(GroupBy::Repo) => "Repo",
        Some(GroupBy::Provider) => "Provider",
        Some(GroupBy::Task) => "Task",
        Some(GroupBy::Model) => "Model",
        None => "Group",
    }
}

pub fn truncate(input: &str, max_len: usize) -> String {
    if input.chars().count() <= max_len {
        return input.to_string();
    }
    if max_len <= 3 {
        return "...".to_string();
    }
    let mut out = String::new();
    for ch in input.chars().take(max_len - 3) {
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn append_border(out: &mut String, left: char, join: char, right: char, widths: &[usize; 3]) {
    out.push(left);
    for (index, width) in widths.iter().enumerate() {
        out.push_str(&"─".repeat(width + 2));
        out.push(if index + 1 == widths.len() {
            right
        } else {
            join
        });
    }
    out.push('\n');
}

fn append_row(out: &mut String, cells: &[&str; 3], widths: &[usize; 3]) {
    out.push('│');
    for (index, (cell, width)) in cells.iter().zip(widths.iter()).enumerate() {
        out.push(' ');
        out.push_str(cell);
        out.push_str(&" ".repeat(width.saturating_sub(cell.chars().count()) + 1));
        out.push('│');
        if index + 1 == widths.len() {
            out.push('\n');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_lower_better_uses_boundaries() {
        assert_eq!(
            classify_lower_better(Some(3.9), 4.0, 7.0),
            MetricStatus::Good
        );
        assert_eq!(
            classify_lower_better(Some(4.0), 4.0, 7.0),
            MetricStatus::Watch
        );
        assert_eq!(
            classify_lower_better(Some(7.0), 4.0, 7.0),
            MetricStatus::Watch
        );
        assert_eq!(
            classify_lower_better(Some(7.1), 4.0, 7.0),
            MetricStatus::Risk
        );
        assert_eq!(
            classify_lower_better(None, 4.0, 7.0),
            MetricStatus::Unavailable
        );
    }

    #[test]
    fn classify_higher_better_uses_boundaries() {
        assert_eq!(
            classify_higher_better(Some(75.0), 75.0, 50.0),
            MetricStatus::Good
        );
        assert_eq!(
            classify_higher_better(Some(50.0), 75.0, 50.0),
            MetricStatus::Watch
        );
        assert_eq!(
            classify_higher_better(Some(49.9), 75.0, 50.0),
            MetricStatus::Risk
        );
    }
}
