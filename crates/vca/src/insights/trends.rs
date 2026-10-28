//! Calendar-period observations shared by CLI and TUI. No causal attribution.
use super::{
    setup,
    work::{self, Tokens, WorkItem},
};
use anyhow::{Result, bail};
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const METRIC_VERSION: &str = "work-trends-2";
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Scope {
    pub repo: Option<String>,
    pub providers: Vec<String>,
    pub model: Option<String>,
    pub intent: Option<String>,
    pub verified_only: bool,
    pub include_estimated: bool,
}
impl Scope {
    pub fn includes(&self, w: &WorkItem) -> bool {
        self.intent.as_ref().is_none_or(|s| s == &w.intent)
            && self.repo.as_ref().is_none_or(|r| {
                w.members
                    .iter()
                    .any(|m| m.task.repo_root.as_ref() == Some(r))
            })
            && (self.providers.is_empty()
                || w.members
                    .iter()
                    .any(|m| self.providers.contains(&m.task.provider)))
            && self.model.as_ref().is_none_or(|s| {
                w.usage
                    .iter()
                    .any(|u| u.model.as_ref().is_some_and(|m| m.contains(s)))
            })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Options {
    pub scope: Scope,
    pub from: Option<String>,
    pub to: Option<String>,
    pub timezone: Option<String>,
    pub daily: bool,
    pub compare: bool,
    pub baseline_from: Option<String>,
    pub baseline_to: Option<String>,
    pub around: Option<i64>,
    pub follow_up_days: u32,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            scope: Scope::default(),
            from: None,
            to: None,
            timezone: None,
            daily: false,
            compare: false,
            baseline_from: None,
            baseline_to: None,
            around: None,
            follow_up_days: 7,
        }
    }
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Distribution {
    pub median: Option<f64>,
    pub p90: Option<f64>,
    pub samples: usize,
    pub missing: usize,
    pub excluded: usize,
    pub small_sample: bool,
    pub work_ids: Vec<String>,
}
fn distribution(values: Vec<(String, Option<f64>)>, excluded: usize) -> Distribution {
    let missing = values.iter().filter(|(_, v)| v.is_none()).count();
    let mut known: Vec<_> = values
        .into_iter()
        .filter_map(|(id, v)| v.filter(|v| v.is_finite()).map(|v| (id, v)))
        .collect();
    known.sort_by(|a, b| a.1.total_cmp(&b.1));
    let n = known.len();
    let median = if n == 0 {
        None
    } else if n % 2 == 0 {
        Some((known[n / 2 - 1].1 + known[n / 2].1) / 2.0)
    } else {
        Some(known[n / 2].1)
    };
    let p90 = (n > 0).then(|| known[((n as f64 * 0.9).ceil() as usize).saturating_sub(1)].1);
    Distribution {
        median,
        p90,
        samples: n,
        missing,
        excluded,
        small_sample: n < 10,
        work_ids: known.into_iter().map(|(id, _)| id).collect(),
    }
}
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Mix {
    pub label: String,
    pub work_ids: Vec<String>,
    pub work_items: usize,
    pub work_share: Option<f64>,
    pub activity_minutes: f64,
    pub activity_share: Option<f64>,
    pub native_tokens: i64,
    pub estimated_tokens: i64,
    pub token_share: Option<f64>,
}
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Coverage {
    pub work_items: usize,
    pub native_complete: usize,
    pub estimated_or_mixed: usize,
    pub missing_usage: usize,
    pub undated_usage_events: usize,
    pub unknown_completion: usize,
    pub verified_completions: usize,
    pub heuristic_intents: usize,
    pub unclassified_intents: usize,
    pub stale_or_mixed_labels: bool,
    pub pending_links: usize,
    pub identities_needing_review: usize,
    pub whole_session_members: usize,
    pub empty_conversations_excluded: usize,
    pub before_first_observation: bool,
    pub source_incomplete: bool,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Period {
    pub from: String,
    pub to: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub observed_end_ms: i64,
    pub provisional: bool,
    pub observed_days: f64,
    pub started: usize,
    pub completed: usize,
    pub open: usize,
    pub abandoned: usize,
    pub active_work: usize,
    pub count_work_ids: BTreeMap<String, Vec<String>>,
    pub cycle_minutes: Distribution,
    pub active_minutes: Distribution,
    pub tokens_per_completed: Distribution,
    pub conversations: Distribution,
    pub request_context: Distribution,
    pub peak_context: Distribution,
    pub open_age_minutes: Distribution,
    pub native_tokens: Tokens,
    pub estimated_tokens: Tokens,
    pub reported_cost_usd: Option<f64>,
    pub api_equivalent_usd: Option<f64>,
    pub unpriced_events: usize,
    pub work_activity_minutes: f64,
    pub goal_mix: Vec<Mix>,
    pub activity_mix: Vec<Mix>,
    pub tokens_by_state: BTreeMap<String, Tokens>,
    pub attempts: i64,
    pub validation_failures: i64,
    pub user_corrections: i64,
    pub runtime_succeeded: usize,
    pub runtime_failed: usize,
    pub mature_followups: usize,
    pub pending_followups: usize,
    pub reopened_within_window: usize,
    pub corrected_within_window: usize,
    pub reopen_rate: Option<f64>,
    pub correction_rate: Option<f64>,
    pub coverage: Coverage,
    pub contributing_work_ids: Vec<String>,
    pub spanning_change_ids: Vec<String>,
    pub pull_requests: super::daily::PrMetrics,
    pub contributing_pr_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Delta {
    pub metric: String,
    pub before: Option<f64>,
    pub after: Option<f64>,
    pub absolute: Option<f64>,
    pub relative_percent: Option<f64>,
    pub unit: String,
    pub note: Option<String>,
    pub before_samples: usize,
    pub after_samples: usize,
    pub before_missing: usize,
    pub after_missing: usize,
    pub before_excluded: usize,
    pub after_excluded: usize,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Breakdown {
    pub dimension: String,
    pub key: String,
    pub before_items: usize,
    pub after_items: usize,
    pub before_cycle: Distribution,
    pub after_cycle: Distribution,
    pub before_tokens: Distribution,
    pub after_tokens: Distribution,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Comparison {
    pub before: Period,
    pub after: Period,
    pub deltas: Vec<Delta>,
    pub breakdowns: Vec<Breakdown>,
    pub milestone: Option<setup::Milestone>,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Report {
    pub schema_version: u32,
    pub metric_version: String,
    pub data_cutoff: String,
    pub latest_activity: Option<i64>,
    pub earliest_activity: Option<i64>,
    pub provider_observed_through: BTreeMap<String, i64>,
    pub timezone: String,
    pub scope: Scope,
    pub classification_versions: Vec<String>,
    pub warnings: Vec<String>,
    pub periods: Vec<Period>,
    pub comparison: Option<Comparison>,
    pub milestones: Vec<setup::Milestone>,
    pub observations: Vec<setup::Observation>,
    pub source_errors: BTreeMap<String, String>,
    pub unattributed_sessions: usize,
}

pub fn timezone(conn: &Connection, explicit: Option<&str>) -> Result<(Tz, Option<String>)> {
    if let Some(explicit) = explicit {
        return Ok((
            explicit
                .parse()
                .map_err(|_| anyhow::anyhow!("Unknown IANA timezone: {explicit}"))?,
            None,
        ));
    }
    if let Some(saved) = conn
        .query_row(
            "SELECT value FROM analytics_setting WHERE key='timezone'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        return Ok((
            saved
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid saved timezone {saved}"))?,
            None,
        ));
    }
    if let Ok(name) = iana_time_zone::get_timezone()
        && let Ok(tz) = name.parse::<Tz>()
    {
        return Ok((tz, None));
    }
    Ok((
        chrono_tz::UTC,
        Some("Local timezone unavailable; using UTC. Set --timezone explicitly.".into()),
    ))
}
pub fn midnight(tz: Tz, date: NaiveDate) -> Result<i64> {
    // Some zones move at midnight. Choose the earliest existing instant of that date.
    let start = date.and_hms_opt(0, 0, 0).unwrap();
    for minute in 0..=180 {
        if let Some(at) = tz
            .from_local_datetime(&(start + Duration::minutes(minute)))
            .earliest()
        {
            return Ok(at.timestamp_millis());
        }
    }
    bail!("No valid start of day for {date} in {tz}")
}
fn date(s: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d")?)
}
fn fmt(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}
pub fn report(conn: &Connection, options: &Options) -> Result<Report> {
    report_at(conn, options, Utc::now())
}
pub fn report_at(conn: &Connection, options: &Options, now: DateTime<Utc>) -> Result<Report> {
    if options.follow_up_days == 0 {
        bail!("Follow-up days must be positive");
    }
    if options.baseline_from.is_some() != options.baseline_to.is_some() {
        bail!("Provide both baseline dates");
    }
    if options.around.is_some()
        && (options.from.is_some() || options.to.is_some() || options.baseline_from.is_some())
    {
        bail!("Milestone comparison cannot also have explicit periods");
    }
    let (tz, warning) = timezone(conn, options.timezone.as_deref())?;
    let mut warnings = warning.into_iter().collect::<Vec<_>>();
    let today = now.with_timezone(&tz).date_naive();
    let monday = today - Duration::days(today.weekday().num_days_from_monday() as i64);
    let all = work::items(conn)?;
    let selected: Vec<_> = all.iter().filter(|w| options.scope.includes(w)).collect();
    let source_scope = Scope {
        model: None,
        intent: None,
        ..options.scope.clone()
    };
    let earliest_activity = all
        .iter()
        .filter(|w| source_scope.includes(w))
        .filter_map(|w| w.started_at)
        .min();
    let history_start = earliest_activity
        .map(|at| {
            let day = DateTime::from_timestamp_millis(at)
                .expect("valid activity timestamp")
                .with_timezone(&tz)
                .date_naive();
            midnight(tz, day)
        })
        .transpose()?;
    let mut source_errors = BTreeMap::new();
    let mut q = conn.prepare(
        "SELECT substr(key,14),value FROM analytics_setting WHERE key LIKE 'source_error:%'",
    )?;
    for row in q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (provider, error) = row?;
        if options.scope.providers.is_empty() || options.scope.providers.contains(&provider) {
            warnings.push(format!("Incomplete {provider} source coverage: {error}. Missing activity must not be interpreted as zero."));
            source_errors.insert(provider, error);
        }
    }
    let mut q=conn.prepare("SELECT provider,COUNT(*) FROM metadata_sessions WHERE repository_id IS NULL GROUP BY provider")?;
    let unattributed_sessions = q
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as usize))
        })?
        .filter_map(Result::ok)
        .filter(|(p, _)| options.scope.providers.is_empty() || options.scope.providers.contains(p))
        .map(|(_, n)| n)
        .sum::<usize>();
    if unattributed_sessions > 0 {
        warnings.push(format!("{unattributed_sessions} sessions have no repository identity; project-scoped reports exclude them."));
    }
    let milestones = setup::milestones(conn, options.scope.repo.as_deref())?;
    let marker = if let Some(id) = options.around {
        Some(
            milestones
                .iter()
                .find(|m| m.id == id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Milestone not found in selected scope"))?,
        )
    } else {
        None
    };
    let marker_day = marker
        .as_ref()
        .map(|m| DateTime::parse_from_rfc3339(&m.at).map(|d| d.with_timezone(&tz).date_naive()))
        .transpose()?;
    let from = if let Some(day) = marker_day {
        day + Duration::days(1)
    } else if let Some(s) = &options.from {
        date(s)?
    } else if options.compare {
        monday - Duration::days(28)
    } else {
        monday - Duration::days(56)
    };
    let to = if let Some(day) = marker_day {
        day + Duration::days(28)
    } else if let Some(s) = &options.to {
        date(s)?
    } else if options.compare {
        monday - Duration::days(1)
    } else {
        today
    };
    if to < from {
        bail!("End date is before start date");
    }
    if (to - from).num_days() > 3660 {
        bail!("Report ranges are limited to ten years");
    }
    let transition = marker_day
        .map(|d| Ok::<_, anyhow::Error>((midnight(tz, d)?, midnight(tz, d + Duration::days(1))?)))
        .transpose()?;
    let mut q = conn.prepare(
        "SELECT substr(key,19),value FROM analytics_setting WHERE key LIKE 'observed_provider:%'",
    )?;
    let mut observed: BTreeMap<String, i64> = q
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .filter_map(|r| {
            r.ok()
                .and_then(|(p, v)| super::structure::parse_ts(&v).map(|t| (p, t)))
        })
        .collect();
    let recorded_coverage: Vec<_> = super::observation::coverage(conn)?
        .into_iter()
        .filter(|s| {
            s.kind != "pull_requests_and_deployments"
                && s.repo_root.as_ref().is_none_or(|r| {
                    options.scope.repo.as_ref().is_none_or(|selected| {
                        super::observation::canonical_repo(conn, r).ok()
                            == super::observation::canonical_repo(conn, selected).ok()
                    })
                })
                && (options.scope.providers.is_empty()
                    || options.scope.providers.contains(&s.provider))
        })
        .collect();
    let source_end = recorded_coverage
        .iter()
        .filter_map(|s| {
            s.observed_through
                .as_deref()
                .and_then(super::structure::parse_ts)
        })
        .min();
    for source in &recorded_coverage {
        if !source.complete || source.error.is_some() {
            observed.remove(&source.provider);
        }
    }
    let delivery_evidence = super::delivery::evidence(conn)?
        .into_iter()
        .filter(|e| {
            options.scope.repo.as_ref().is_none_or(|r| {
                super::observation::canonical_repo(conn, r).is_ok_and(|r| r == e.pr.repo_key)
            }) && ((options.scope.providers.is_empty()
                && options.scope.model.is_none()
                && options.scope.intent.is_none())
                || e.work_ids
                    .iter()
                    .any(|id| selected.iter().any(|w| &w.id == id)))
        })
        .collect::<Vec<_>>();
    let context = Context {
        all: &all,
        scope: &options.scope,
        tz,
        now: now.timestamp_millis(),
        followup: options.follow_up_days,
        observed: &observed,
        history_start,
        source_incomplete: !source_errors.is_empty()
            || recorded_coverage
                .iter()
                .any(|s| !s.complete || s.error.is_some()),
        source_end,
        transition,
        delivery_evidence: &delivery_evidence,
    };
    let mut periods = Vec::new();
    let mut cursor = from;
    while cursor <= to {
        let end = if options.daily {
            cursor + Duration::days(1)
        } else {
            cursor + Duration::days(7 - cursor.weekday().num_days_from_monday() as i64)
        };
        let end = end.min(to + Duration::days(1));
        periods.push(summarize(&context, &selected, cursor, end)?);
        cursor = end;
    }
    let comparison = if options.compare || marker.is_some() || options.baseline_from.is_some() {
        let after = summarize(&context, &selected, from, to + Duration::days(1))?;
        let (bfrom, bto) = if let Some(day) = marker_day {
            (day - Duration::days(28), day)
        } else if let (Some(a), Some(b)) = (&options.baseline_from, &options.baseline_to) {
            (date(a)?, date(b)? + Duration::days(1))
        } else {
            (from - Duration::days((to - from).num_days() + 1), from)
        };
        if bto <= bfrom {
            bail!("Invalid baseline period");
        }
        let before = summarize(&context, &selected, bfrom, bto)?;
        let deltas = deltas(&before, &after);
        let mut breakdowns = Vec::new();
        for dimension in ["repository", "intent", "provider/model"] {
            let mut groups: BTreeMap<String, Vec<&WorkItem>> = BTreeMap::new();
            for w in &selected {
                let key = match dimension {
                    "intent" => w.intent.clone(),
                    "repository" => w
                        .members
                        .iter()
                        .filter_map(|m| m.task.repo_root.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .join(" + "),
                    _ => w
                        .members
                        .iter()
                        .map(|m| {
                            format!(
                                "{}/{}",
                                m.task.provider,
                                m.task.primary_model.as_deref().unwrap_or("unknown")
                            )
                        })
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .join(" + "),
                };
                groups
                    .entry(if key.is_empty() {
                        "unknown".into()
                    } else {
                        key
                    })
                    .or_default()
                    .push(w);
            }
            for (key, group) in groups {
                let a = summarize(&context, &group, bfrom, bto)?;
                let b = summarize(&context, &group, from, to + Duration::days(1))?;
                if a.active_work + b.active_work > 0 {
                    breakdowns.push(Breakdown {
                        dimension: dimension.into(),
                        key,
                        before_items: a.active_work,
                        after_items: b.active_work,
                        before_cycle: a.cycle_minutes,
                        after_cycle: b.cycle_minutes,
                        before_tokens: a.tokens_per_completed,
                        after_tokens: b.tokens_per_completed,
                    });
                }
            }
        }
        Some(Comparison {
            before,
            after,
            deltas,
            breakdowns,
            milestone: marker,
        })
    } else {
        None
    };
    let selected_sessions = serde_json::to_string(
        &selected
            .iter()
            .flat_map(|w| {
                w.members
                    .iter()
                    .map(|m| format!("{}|{}", m.task.provider, m.task.session_id))
            })
            .collect::<BTreeSet<_>>(),
    )?;
    let mut stmt=conn.prepare("SELECT DISTINCT question_version || ':' || COALESCE(model_version,source) FROM classification_result WHERE superseded_by IS NULL AND EXISTS(SELECT 1 FROM json_each(?1) s WHERE unit_key=s.value OR substr(unit_key,1,length(s.value)+1)=s.value || '|') ORDER BY 1")?;
    let versions = stmt
        .query_map([&selected_sessions], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let stale:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM classification_result WHERE source='jev' AND superseded_by IS NULL AND question_version NOT IN (?1,?2) AND EXISTS(SELECT 1 FROM json_each(?3) s WHERE unit_key=s.value OR substr(unit_key,1,length(s.value)+1)=s.value || '|'))",rusqlite::params![crate::classify::questions::QUESTION_SET_VERSION,crate::classify::questions::SKILL_QUESTION_VERSION,selected_sessions],|r|r.get(0))?;
    if stale {
        warnings.push("Some labels use an older policy. Backfill both periods before interpreting the change.".into());
    }
    if selected.iter().any(|w| {
        w.members
            .iter()
            .any(|m| m.task.segmentation_source == "whole_session")
    }) {
        warnings.push(
            "Some conversations are unsplit: their work-item counts and intent mix are coarse."
                .into(),
        );
    }
    if selected.iter().any(|w| w.observed_activity)
        && selected
            .iter()
            .filter(|w| w.observed_activity)
            .all(|w| matches!(w.intent_source.as_str(), "heuristic" | "missing"))
    {
        warnings.push("Goal labels use heuristics only. Enable the external classifier or apply manual labels before interpreting goal mix.".into());
    }
    if selected.iter().any(|w| w.observed_activity)
        && selected.iter().all(|w| w.completions.is_empty())
    {
        warnings.push("No supported goal completions are available. Cycle time and tokens per completed goal are unavailable; open counts include completion-unknown work. Runtime success is separate evidence.".into());
    }
    if comparison.is_some() {
        warnings.push("Differences describe observed work. Completion selection, workload mix, model changes and missing data can affect the comparison.".into());
    }
    for p in &mut periods {
        p.coverage.stale_or_mixed_labels = stale
            || p.coverage.heuristic_intents > 0
                && p.coverage.heuristic_intents < p.coverage.work_items;
    }
    let mut comparison = comparison;
    if let Some(c) = &mut comparison {
        c.before.coverage.stale_or_mixed_labels |= stale;
        c.after.coverage.stale_or_mixed_labels |= stale;
        let rate = |p: &Period| {
            (p.coverage.work_items > 0)
                .then(|| p.coverage.native_complete as f64 / p.coverage.work_items as f64)
        };
        if let Some((a, b)) = rate(&c.before).zip(rate(&c.after))
            && (a - b).abs() >= 0.10
        {
            warnings.push(format!("Complete native-usage coverage differs: {:.1}% before, {:.1}% after. Per-work efficiency comparisons exclude missing usage.",a*100.0,b*100.0));
        }
    }
    let earliest = comparison
        .as_ref()
        .map(|c| c.before.start_ms.min(c.after.start_ms))
        .unwrap_or(midnight(tz, from)?);
    if history_start.is_some_and(|at| earliest < at) {
        let day = DateTime::from_timestamp_millis(history_start.unwrap())
            .expect("valid history timestamp")
            .with_timezone(&tz)
            .date_naive();
        warnings.push(format!("Recorded history starts on {day}. Earlier dates in this report have unverified coverage and must not be interpreted as proven inactivity."));
    }
    let latest = midnight(tz, to + Duration::days(1))?;
    let observations = setup::observations(conn, options.scope.repo.as_deref())?
        .into_iter()
        .filter(|o| {
            super::structure::parse_ts(o.effective_at.as_deref().unwrap_or(&o.observed_at))
                .is_some_and(|t| t >= earliest && t < latest)
        })
        .collect();
    Ok(Report {
        schema_version: 1,
        metric_version: METRIC_VERSION.into(),
        data_cutoff: now.to_rfc3339(),
        latest_activity: selected.iter().filter_map(|w| w.last_activity_at).max(),
        earliest_activity,
        timezone: tz.name().into(),
        scope: options.scope.clone(),
        classification_versions: versions,
        provider_observed_through: observed.clone(),
        warnings,
        periods,
        comparison,
        milestones: milestones
            .into_iter()
            .filter(|m| {
                super::structure::parse_ts(&m.at).is_some_and(|t| t >= earliest && t < latest)
            })
            .collect(),
        observations,
        source_errors,
        unattributed_sessions,
    })
}
struct Context<'a> {
    delivery_evidence: &'a [super::delivery::PrEvidence],
    all: &'a [WorkItem],
    scope: &'a Scope,
    tz: Tz,
    now: i64,
    followup: u32,
    observed: &'a BTreeMap<String, i64>,
    history_start: Option<i64>,
    source_incomplete: bool,
    source_end: Option<i64>,
    transition: Option<(i64, i64)>,
}
fn completed_at(w: &WorkItem, cutoff: i64) -> Option<&work::Completion> {
    let member = w
        .members
        .iter()
        .filter(|m| {
            m.task
                .started_at
                .as_deref()
                .and_then(super::structure::parse_ts)
                .is_some_and(|s| s < cutoff)
        })
        .max_by(|a, b| a.task.started_at.cmp(&b.task.started_at))?;
    w.completions.iter().rev().find(|c| {
        c.at < cutoff
            && c.member_id == member.id
            && w.reopens
                .iter()
                .filter(|r| **r < cutoff)
                .all(|r| *r <= c.at)
    })
}
fn summarize(
    ctx: &Context<'_>,
    works: &[&WorkItem],
    from: NaiveDate,
    end: NaiveDate,
) -> Result<Period> {
    let start = midnight(ctx.tz, from)?;
    let finish = midnight(ctx.tz, end)?;
    let cutoff = finish.min(ctx.now);
    let contains = |t: i64| t >= start && t < cutoff;
    let mut p = Period {
        from: fmt(from),
        to: fmt(end - Duration::days(1)),
        start_ms: start,
        end_ms: finish,
        observed_end_ms: cutoff,
        provisional: finish > ctx.now
            || ctx.source_end.is_some_and(|end| finish > end)
            || ctx.source_incomplete,
        observed_days: if cutoff <= start {
            0.0
        } else {
            let local_day = DateTime::from_timestamp_millis(cutoff)
                .expect("valid report cutoff")
                .with_timezone(&ctx.tz)
                .date_naive();
            let day_start = midnight(ctx.tz, local_day)?;
            let day_end = midnight(ctx.tz, local_day + Duration::days(1))?;
            (local_day - from).num_days() as f64
                + (cutoff - day_start) as f64 / (day_end - day_start) as f64
        },
        started: 0,
        completed: 0,
        open: 0,
        abandoned: 0,
        active_work: 0,
        count_work_ids: BTreeMap::new(),
        cycle_minutes: distribution(vec![], 0),
        active_minutes: distribution(vec![], 0),
        tokens_per_completed: distribution(vec![], 0),
        conversations: distribution(vec![], 0),
        request_context: distribution(vec![], 0),
        peak_context: distribution(vec![], 0),
        open_age_minutes: distribution(vec![], 0),
        native_tokens: Tokens::default(),
        estimated_tokens: Tokens::default(),
        reported_cost_usd: None,
        api_equivalent_usd: None,
        unpriced_events: 0,
        work_activity_minutes: 0.0,
        goal_mix: vec![],
        activity_mix: vec![],
        tokens_by_state: BTreeMap::new(),
        attempts: 0,
        validation_failures: 0,
        user_corrections: 0,
        runtime_succeeded: 0,
        runtime_failed: 0,
        mature_followups: 0,
        pending_followups: 0,
        reopened_within_window: 0,
        corrected_within_window: 0,
        reopen_rate: None,
        correction_rate: None,
        coverage: Coverage::default(),
        contributing_work_ids: vec![],
        spanning_change_ids: vec![],
        pull_requests: Default::default(),
        contributing_pr_ids: vec![],
    };
    let mut pr_leads = vec![];
    let mut deployment_lags = vec![];
    let mut delivery_leads = vec![];
    let mut failures = BTreeSet::new();
    let mut rollbacks = BTreeSet::new();
    for e in ctx.delivery_evidence {
        let ts = |s: Option<&str>| s.and_then(super::structure::parse_ts);
        let opened = ts(e.pr.created_at.as_deref());
        let merged = ts(e.pr.merged_at.as_deref());
        let deployed = ts(e.deployed_at.as_deref());
        let closed = ts(e.pr.closed_at.as_deref());
        let inside = |at: Option<i64>| at.is_some_and(|t| t >= start && t < finish.min(ctx.now));
        p.pull_requests.opened += usize::from(inside(opened));
        p.pull_requests.merged += usize::from(inside(merged));
        p.pull_requests.closed_without_merge += usize::from(inside(closed) && merged.is_none());
        let active = inside(opened)
            || inside(merged)
            || inside(deployed)
            || inside(closed)
            || inside(ts(e.pr.updated_at.as_deref()));
        if active {
            p.pull_requests.active += 1;
            p.pull_requests.bug_fix_prs += usize::from(e.intent == "bug_fix");
            p.contributing_pr_ids.push(e.pr.id.clone());
        }
        p.pull_requests.pending_delivery += usize::from(
            merged.is_some_and(|m| m < finish.min(ctx.now))
                && deployed.is_none_or(|d| d >= finish.min(ctx.now)),
        );
        if inside(merged)
            && let (Some(a), Some(b)) = (opened, merged)
            && b >= a
        {
            pr_leads.push((b - a) as f64 / 60000.0);
        }
        if inside(deployed) {
            p.pull_requests.deployed += 1;
            p.pull_requests.bug_fix_prs_deployed += usize::from(e.intent == "bug_fix");
            if let (Some(a), Some(b)) = (merged, deployed)
                && b >= a
            {
                deployment_lags.push((b - a) as f64 / 60000.0);
            }
            let associated: Vec<_> = works
                .iter()
                .filter(|w| e.work_ids.contains(&w.id))
                .collect();
            let spans = ctx.transition.is_some_and(|(a, b)| {
                associated.iter().any(|w| {
                    w.started_at.is_some_and(|s| s < a) && deployed.is_some_and(|d| d >= b)
                })
            });
            if !spans
                && let (Some(a), Some(b)) = (
                    associated.iter().filter_map(|w| w.started_at).min(),
                    deployed,
                )
                && b >= a
            {
                delivery_leads.push((b - a) as f64 / 60000.0);
            }
        }
        for d in &e.deployments {
            if inside(ts(Some(&d.occurred_at))) {
                if ["failure", "failed", "error"].contains(&d.status.as_str()) {
                    failures.insert(&d.id);
                }
                if d.status == "rollback" || d.rollback_of.is_some() {
                    rollbacks.insert(&d.id);
                }
            }
        }
    }
    p.pull_requests.failed_deployment_events = failures.len();
    p.pull_requests.rollback_events = rollbacks.len();
    p.pull_requests.pr_lead_minutes = super::daily::distribution(pr_leads, 0);
    p.pull_requests.deployment_lag_minutes = super::daily::distribution(deployment_lags, 0);
    p.pull_requests.engineering_delivery_minutes = super::daily::distribution(delivery_leads, 0);
    p.coverage.before_first_observation = ctx.history_start.is_some_and(|at| start < at);
    p.coverage.source_incomplete = ctx.source_incomplete;
    let mut goals: BTreeMap<String, Mix> = BTreeMap::new();
    let mut stages: BTreeMap<String, Mix> = BTreeMap::new();
    let (mut cycles, mut active, mut tokens, mut conversations, mut ages, mut contexts, mut peaks) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let mut excluded = 0;
    for w in works {
        if !w.observed_activity {
            if w.started_at.is_some_and(contains) {
                p.coverage.empty_conversations_excluded += w.conversations;
            }
            continue;
        }
        let begun = w.started_at.is_some_and(|s| s < cutoff);
        if !begun {
            continue;
        }
        let done = completed_at(w, cutoff);
        let abandoned = done.is_none()
            && w.state == "abandoned"
            && w.last_activity_at.is_some_and(|s| s < cutoff);
        let state = if done.is_some() {
            "completed"
        } else if abandoned {
            "abandoned"
        } else {
            "open"
        };
        if w.started_at.is_some_and(contains) {
            p.started += 1;
            p.count_work_ids
                .entry("started".into())
                .or_default()
                .push(w.id.clone());
        }
        if state == "open" {
            p.open += 1;
            p.count_work_ids
                .entry("open".into())
                .or_default()
                .push(w.id.clone());
            ages.push((
                w.id.clone(),
                w.started_at.map(|s| (cutoff - s) as f64 / 60_000.0),
            ));
        }
        if abandoned && w.last_activity_at.is_some_and(contains) {
            p.abandoned += 1;
            p.count_work_ids
                .entry("abandoned".into())
                .or_default()
                .push(w.id.clone());
        }
        let usage: Vec<_> = w
            .usage
            .iter()
            .filter(|u| u.ts.is_some_and(contains))
            .collect();
        let intervals: Vec<_> = w
            .activity
            .iter()
            .filter_map(|a| {
                let s = a.start_ms.max(start);
                let e = a.end_ms.min(cutoff);
                (e > s).then_some((s, e, a.stage.clone()))
            })
            .collect();
        let completed_in = done.is_some_and(|c| contains(c.at));
        let touched = !usage.is_empty()
            || !intervals.is_empty()
            || w.execution_outcomes
                .iter()
                .any(|e| e.at.is_some_and(contains))
            || completed_in
            || w.started_at.is_some_and(contains);
        if !touched {
            continue;
        }
        p.active_work += 1;
        p.contributing_work_ids.push(w.id.clone());
        for event in w
            .execution_outcomes
            .iter()
            .filter(|e| e.at.is_some_and(contains))
        {
            p.runtime_succeeded += usize::from(event.outcome == "succeeded");
            p.runtime_failed += usize::from(event.outcome == "failed");
        }
        p.coverage.work_items += 1;
        p.coverage.native_complete += usize::from(w.native_tokens().is_some());
        p.coverage.missing_usage += usize::from(w.members_missing_usage > 0);
        p.coverage.estimated_or_mixed += usize::from(w.usage.iter().any(|u| !u.native));
        p.coverage.undated_usage_events += w.usage.iter().filter(|u| u.ts.is_none()).count();
        p.coverage.unknown_completion += usize::from(done.is_none());
        p.coverage.verified_completions +=
            usize::from(completed_in && done.is_some_and(|c| c.verified));
        p.coverage.heuristic_intents += usize::from(w.intent_source == "heuristic");
        p.coverage.unclassified_intents += usize::from(w.intent == "unclassified");
        p.coverage.identities_needing_review += w
            .members
            .iter()
            .filter(|m| m.review_reason.is_some())
            .count();
        p.coverage.pending_links += w
            .relations
            .iter()
            .filter(|r| r.decision == "suggested")
            .count();
        p.coverage.whole_session_members += w
            .members
            .iter()
            .filter(|m| m.task.segmentation_source == "whole_session")
            .count();
        let g = goals.entry(w.intent.clone()).or_insert_with(|| Mix {
            label: w.intent.clone(),
            ..Mix::default()
        });
        g.work_ids.push(w.id.clone());
        let work_minutes =
            work::union_minutes(intervals.iter().map(|(s, e, _)| (*s, *e)).collect());
        g.activity_minutes += work_minutes;
        p.work_activity_minutes += work_minutes;
        // Split overlapping activity across distinct concurrent stages; shares remain additive.
        let bounds: BTreeSet<_> = intervals.iter().flat_map(|(s, e, _)| [*s, *e]).collect();
        let bounds: Vec<_> = bounds.into_iter().collect();
        for pair in bounds.windows(2) {
            let labels: BTreeSet<_> = intervals
                .iter()
                .filter(|(s, e, _)| *s < pair[1] && *e > pair[0])
                .map(|(_, _, l)| l.clone())
                .collect();
            let n = labels.len();
            for label in labels {
                let row = stages.entry(label.clone()).or_insert_with(|| Mix {
                    label,
                    ..Mix::default()
                });
                row.activity_minutes += (pair[1] - pair[0]) as f64 / 60_000.0 / n as f64;
                row.work_ids.push(w.id.clone());
            }
        }
        let mut peak = None::<i64>;
        for u in usage {
            let row = stages.entry(u.stage.clone()).or_insert_with(|| Mix {
                label: u.stage.clone(),
                ..Mix::default()
            });
            row.work_ids.push(w.id.clone());
            if u.available {
                if u.native {
                    p.native_tokens.add(&u.tokens);
                    g.native_tokens += u.tokens.total;
                    row.native_tokens += u.tokens.total;
                } else {
                    p.estimated_tokens.add(&u.tokens);
                    g.estimated_tokens += u.tokens.total;
                    row.estimated_tokens += u.tokens.total;
                }
                p.tokens_by_state
                    .entry(state.into())
                    .or_default()
                    .add(&u.tokens);
            }
            if let Some(cost) = u.reported_cost {
                *p.reported_cost_usd.get_or_insert(0.0) += cost;
            }
            if let Some(cost) = u.api_equivalent_cost {
                *p.api_equivalent_usd.get_or_insert(0.0) += cost;
            } else {
                p.unpriced_events += 1;
            }
            if u.native {
                contexts.push((w.id.clone(), u.context.map(|v| v as f64)));
                peak = peak.max(u.context);
            }
        }
        peaks.push((w.id.clone(), peak.map(|v| v as f64)));
        let spanning = ctx.transition.is_some_and(|(s, e)| {
            w.started_at.is_some_and(|at| at < s) && w.last_activity_at.is_none_or(|at| at >= s)
                || w.started_at.is_some_and(|at| at >= s && at < e)
        });
        if spanning {
            p.spanning_change_ids.push(w.id.clone());
        }
        if completed_in {
            p.completed += 1;
            p.count_work_ids
                .entry("completed".into())
                .or_default()
                .push(w.id.clone());
            let c = done.unwrap();
            if spanning || ctx.scope.verified_only && !c.verified {
                excluded += 1;
            } else {
                cycles.push((
                    w.id.clone(),
                    w.started_at.map(|s| (c.at - s).max(0) as f64 / 60_000.0),
                ));
                active.push((
                    w.id.clone(),
                    Some(work::union_minutes(
                        w.activity
                            .iter()
                            .filter_map(|a| {
                                let end = a.end_ms.min(c.at);
                                (end > a.start_ms).then_some((a.start_ms, end))
                            })
                            .collect(),
                    )),
                ));
                let us: Vec<_> = w
                    .usage
                    .iter()
                    .filter(|u| u.ts.is_none_or(|t| t <= c.at))
                    .collect();
                let measured = w.members_missing_usage == 0
                    && !us.is_empty()
                    && us
                        .iter()
                        .all(|u| u.available && (u.native || ctx.scope.include_estimated));
                tokens.push((
                    w.id.clone(),
                    measured.then(|| us.iter().map(|u| u.tokens.total).sum::<i64>() as f64),
                ));
                conversations.push((
                    w.id.clone(),
                    Some(
                        w.members
                            .iter()
                            .filter(|m| {
                                m.task
                                    .started_at
                                    .as_deref()
                                    .and_then(super::structure::parse_ts)
                                    .is_some_and(|t| t <= c.at)
                            })
                            .map(|m| (&m.task.provider, &m.task.session_id))
                            .collect::<BTreeSet<_>>()
                            .len() as f64,
                    ),
                ));
            }
            for member in w.members.iter().filter(|m| {
                m.task
                    .started_at
                    .as_deref()
                    .and_then(super::structure::parse_ts)
                    .is_some_and(|t| t <= c.at)
            }) {
                p.attempts += member.task.attempts;
                p.validation_failures += member.task.validation_failures;
            }
            p.user_corrections += w.corrections;
        }
        // Follow-up outcome is attached to the initial completion cohort, with a fixed observation horizon.
        if let Some(c) = w.initial_completed_at.filter(|c| contains(*c)) {
            let end = c + ctx.followup as i64 * 86_400_000;
            let observation_end = w
                .members
                .iter()
                .map(|m| ctx.observed.get(&m.task.provider).copied())
                .collect::<Option<Vec<_>>>()
                .and_then(|times| times.into_iter().min());
            if observation_end.is_none_or(|observed| end > observed.min(ctx.now)) {
                p.pending_followups += 1;
            } else {
                p.mature_followups += 1;
                p.reopened_within_window +=
                    usize::from(w.reopens.iter().any(|t| *t > c && *t <= end));
                let corrected = w
                    .relations
                    .iter()
                    .filter(|r| {
                        r.kind == "corrects_previous_work"
                            && r.decision == "accepted"
                            && r.review_reason.is_none()
                            && w.members.iter().any(|m| m.id == r.from_id)
                    })
                    .any(|r| {
                        ctx.all
                            .iter()
                            .flat_map(|w| &w.members)
                            .find(|m| m.id == r.to_id)
                            .and_then(|m| {
                                m.task
                                    .started_at
                                    .as_deref()
                                    .and_then(super::structure::parse_ts)
                            })
                            .is_some_and(|t| t > c && t <= end)
                    });
                p.corrected_within_window += usize::from(corrected);
            }
        }
    }
    p.cycle_minutes = distribution(cycles, excluded);
    p.active_minutes = distribution(active, excluded);
    p.tokens_per_completed = distribution(tokens, excluded);
    p.conversations = distribution(conversations, excluded);
    p.open_age_minutes = distribution(ages, 0);
    p.request_context = distribution(contexts, 0);
    p.peak_context = distribution(peaks, 0);
    let denom = p.native_tokens.total;
    if p.mature_followups > 0 {
        p.reopen_rate = Some(p.reopened_within_window as f64 / p.mature_followups as f64);
        p.correction_rate = Some(p.corrected_within_window as f64 / p.mature_followups as f64);
    }
    for mut row in goals.into_values() {
        row.work_ids.sort();
        row.work_ids.dedup();
        row.work_items = row.work_ids.len();
        row.work_share = (p.active_work > 0).then(|| row.work_items as f64 / p.active_work as f64);
        row.activity_share =
            (p.work_activity_minutes > 0.0).then(|| row.activity_minutes / p.work_activity_minutes);
        row.token_share = (denom > 0).then(|| row.native_tokens as f64 / denom as f64);
        p.goal_mix.push(row);
    }
    for mut row in stages.into_values() {
        row.work_ids.sort();
        row.work_ids.dedup();
        row.work_items = row.work_ids.len();
        row.work_share = (p.active_work > 0).then(|| row.work_items as f64 / p.active_work as f64);
        row.activity_share =
            (p.work_activity_minutes > 0.0).then(|| row.activity_minutes / p.work_activity_minutes);
        row.token_share = (denom > 0).then(|| row.native_tokens as f64 / denom as f64);
        p.activity_mix.push(row);
    }
    Ok(p)
}
fn deltas(a: &Period, b: &Period) -> Vec<Delta> {
    let mut values = Vec::new();
    for (metric, x, y) in [
        ("cycle_minutes", &a.cycle_minutes, &b.cycle_minutes),
        ("active_minutes", &a.active_minutes, &b.active_minutes),
        (
            "tokens_per_completed",
            &a.tokens_per_completed,
            &b.tokens_per_completed,
        ),
        (
            "conversations_per_completed",
            &a.conversations,
            &b.conversations,
        ),
    ] {
        values.push(delta(
            metric,
            x.median,
            y.median,
            "value",
            (x.samples, y.samples),
            (x.missing, y.missing),
            (x.excluded, y.excluded),
        ));
    }
    for (metric, x, y) in [
        (
            "native_tokens",
            (a.coverage.native_complete > 0 || a.native_tokens.total > 0)
                .then_some(a.native_tokens.total as f64),
            (b.coverage.native_complete > 0 || b.native_tokens.total > 0)
                .then_some(b.native_tokens.total as f64),
        ),
        (
            "work_activity_minutes",
            Some(a.work_activity_minutes),
            Some(b.work_activity_minutes),
        ),
        (
            "reported_cost_usd",
            a.reported_cost_usd,
            b.reported_cost_usd,
        ),
        (
            "api_equivalent_usd",
            a.api_equivalent_usd,
            b.api_equivalent_usd,
        ),
    ] {
        values.push(delta(
            metric,
            x,
            y,
            "value",
            (a.active_work, b.active_work),
            (a.coverage.missing_usage, b.coverage.missing_usage),
            (0, 0),
        ));
    }
    for (metric, x, y) in [
        ("completed", a.completed, b.completed),
        ("started", a.started, b.started),
        ("open", a.open, b.open),
        ("abandoned", a.abandoned, b.abandoned),
        ("prs_opened", a.pull_requests.opened, b.pull_requests.opened),
        ("prs_merged", a.pull_requests.merged, b.pull_requests.merged),
        (
            "prs_deployed",
            a.pull_requests.deployed,
            b.pull_requests.deployed,
        ),
    ] {
        values.push(delta(
            metric,
            Some(x as f64),
            Some(y as f64),
            "count",
            (a.active_work, b.active_work),
            (0, 0),
            (0, 0),
        ));
    }
    for (label, x, y) in [
        (
            "bug_fix_token_share",
            a.goal_mix
                .iter()
                .find(|m| m.label == "bug_fix")
                .and_then(|m| m.token_share)
                .or((a.native_tokens.total > 0).then_some(0.0)),
            b.goal_mix
                .iter()
                .find(|m| m.label == "bug_fix")
                .and_then(|m| m.token_share)
                .or((b.native_tokens.total > 0).then_some(0.0)),
        ),
        (
            "debug_activity_share",
            a.activity_mix
                .iter()
                .find(|m| m.label == "debug_fix")
                .and_then(|m| m.activity_share)
                .or((a.work_activity_minutes > 0.0).then_some(0.0)),
            b.activity_mix
                .iter()
                .find(|m| m.label == "debug_fix")
                .and_then(|m| m.activity_share)
                .or((b.work_activity_minutes > 0.0).then_some(0.0)),
        ),
        ("reopen_rate", a.reopen_rate, b.reopen_rate),
        ("correction_rate", a.correction_rate, b.correction_rate),
    ] {
        values.push(delta(
            label,
            x,
            y,
            "percentage_points",
            (a.active_work, b.active_work),
            (0, 0),
            (0, 0),
        ));
    }
    values
}
fn delta(
    metric: &str,
    a: Option<f64>,
    b: Option<f64>,
    unit: &str,
    samples: (usize, usize),
    missing: (usize, usize),
    excluded: (usize, usize),
) -> Delta {
    Delta {
        metric: metric.into(),
        before: a,
        after: b,
        absolute: a.zip(b).map(|(a, b)| {
            (b - a)
                * if unit == "percentage_points" {
                    100.0
                } else {
                    1.0
                }
        }),
        relative_percent: if unit == "percentage_points" {
            None
        } else {
            a.zip(b)
                .filter(|(a, _)| *a != 0.0)
                .map(|(a, b)| (b - a) / a.abs() * 100.0)
        },
        unit: unit.into(),
        note: a
            .zip(b)
            .filter(|(a, b)| *a == 0.0 && *b > 0.0)
            .map(|_| "new activity".into()),
        before_samples: samples.0,
        after_samples: samples.1,
        before_missing: missing.0,
        after_missing: missing.1,
        before_excluded: excluded.0,
        after_excluded: excluded.1,
    }
}
