//! Compact daily queries over the published work-item index. No network or classifier calls.
use super::{
    delivery, observation,
    structure::parse_ts,
    trends,
    work::{self, Tokens},
};
use anyhow::{Result, ensure};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct Options {
    pub scope: trends::Scope,
    pub from: Option<String>,
    pub to: Option<String>,
    pub timezone: Option<String>,
    pub follow_up_days: u32,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            scope: Default::default(),
            from: None,
            to: None,
            timezone: None,
            follow_up_days: 7,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Distribution {
    pub median: Option<f64>,
    pub p90: Option<f64>,
    pub samples: usize,
    pub missing: usize,
}
pub fn distribution(mut values: Vec<f64>, missing: usize) -> Distribution {
    values.retain(|v| v.is_finite());
    values.sort_by(f64::total_cmp);
    let n = values.len();
    Distribution {
        median: if n == 0 {
            None
        } else if n.is_multiple_of(2) {
            Some((values[n / 2 - 1] + values[n / 2]) / 2.0)
        } else {
            Some(values[n / 2])
        },
        p90: (n > 0).then(|| values[((n as f64 * 0.9).ceil() as usize).saturating_sub(1)]),
        samples: n,
        missing,
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ModelUsage {
    pub model: String,
    pub provider: String,
    pub usage_events: usize,
    pub recorded_invocations: Option<usize>,
    pub available_events: usize,
    pub suspect_zero_events: usize,
    pub explicit_zero_events: usize,
    pub missing_events: usize,
    pub native_tokens: Tokens,
    pub estimated_tokens: Tokens,
    pub reported_cost_usd: Option<f64>,
    pub api_equivalent_usd: Option<f64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Mix {
    pub label: String,
    pub work_items: usize,
    pub work_share: Option<f64>,
    pub activity_minutes: f64,
    pub native_tokens: i64,
    pub estimated_tokens: i64,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Coverage {
    pub work_items: usize,
    pub heuristic_intents: usize,
    pub unclassified_intents: usize,
    pub completion_unknown: usize,
    pub missing_usage_work: usize,
    pub usage_events: usize,
    pub available_usage_events: usize,
    pub suspect_zero_events: usize,
    pub explicit_zero_events: usize,
    pub missing_events: usize,
    pub estimated_events: usize,
    pub work_without_pr: usize,
    pub prs_without_work: usize,
    pub shared_work: usize,
    pub uncertain_pr_intents: usize,
    pub source_complete: bool,
    pub truncated_history: bool,
    pub stale_snapshot: bool,
    pub unknown_watermark: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PrMetrics {
    pub opened: usize,
    pub active: usize,
    pub merged: usize,
    pub closed_without_merge: usize,
    pub bug_fix_prs: usize,
    pub deployed: usize,
    pub bug_fix_prs_deployed: usize,
    pub pending_delivery: usize,
    pub failed_deployment_events: usize,
    pub rollback_events: usize,
    pub engineering_delivery_minutes: Distribution,
    pub pr_lead_minutes: Distribution,
    pub deployment_lag_minutes: Distribution,
    pub tokens_per_delivered_pr: Distribution,
    pub activity_per_delivered_pr: Distribution,
    pub conversations_per_delivered_pr: Distribution,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Day {
    pub date: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub observed_through_ms: Option<i64>,
    pub partial: bool,
    pub started: usize,
    pub active_work: usize,
    pub completed: usize,
    pub completion_with_verification: usize,
    pub open: usize,
    pub abandoned: usize,
    pub bug_work_active: usize,
    pub bug_work_started: usize,
    pub bug_work_completed: usize,
    pub bug_work_verified: usize,
    pub bug_work_open: usize,
    pub attempts: usize,
    pub corrections: usize,
    pub reopened: usize,
    pub validation_failures: usize,
    pub work_activity_minutes: f64,
    pub cycle_minutes: Distribution,
    pub tokens_per_completed: Distribution,
    pub conversations_per_completed: Distribution,
    pub request_context: Distribution,
    pub peak_context: Distribution,
    pub native_tokens: Tokens,
    pub estimated_tokens: Tokens,
    pub reported_cost_usd: Option<f64>,
    pub api_equivalent_usd: Option<f64>,
    pub models: Vec<ModelUsage>,
    pub goal_mix: Vec<Mix>,
    pub activity_mix: Vec<Mix>,
    pub coverage: Coverage,
    pub pull_requests: PrMetrics,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cohort {
    pub date: String,
    pub prs_opened: usize,
    pub bug_fix_prs: usize,
    pub mature: usize,
    pub pending_followup: usize,
    pub deployed_within_window: usize,
    pub closed_without_merge: usize,
    pub share_deployed_within_window: Option<f64>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub metric_version: String,
    pub generated_at: String,
    pub published_revision: Option<String>,
    pub timezone: String,
    pub scope: trends::Scope,
    pub from: String,
    pub to: String,
    pub latest_activity: Option<i64>,
    pub sources: Vec<observation::SourceCoverage>,
    pub warnings: Vec<String>,
    pub days: Vec<Day>,
    pub cohorts: Vec<Cohort>,
    pub follow_up_days: u32,
    pub classification_versions: Vec<String>,
    #[serde(skip)]
    pub work_by_day: BTreeMap<String, Vec<String>>,
    #[serde(skip)]
    pub evidence_by_day: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}
#[derive(Clone)]
struct IndexedWork {
    id: String,
    intent: String,
    source: String,
    start: Option<i64>,
    done: Option<i64>,
    last: Option<i64>,
    state: String,
    verified: bool,
    conversations: usize,
    missing: usize,
}
struct Usage {
    id: String,
    work: String,
    ts: Option<i64>,
    provider: String,
    model: String,
    native: bool,
    availability: String,
    tokens: Tokens,
    reported: Option<f64>,
    api: Option<f64>,
    stage: String,
    context: Option<i64>,
}
struct Interval {
    work: String,
    start: i64,
    end: i64,
    stage: String,
}
fn optional_sum(sum: &mut Option<f64>, value: Option<f64>) {
    if let Some(v) = value {
        *sum = Some(sum.unwrap_or(0.0) + v);
    }
}
fn instant(value: Option<String>) -> Option<i64> {
    value.as_deref().and_then(parse_ts)
}

pub fn report(conn: &Connection, options: &Options) -> Result<Report> {
    report_at(conn, options, Utc::now())
}
pub fn report_at(conn: &Connection, options: &Options, now: DateTime<Utc>) -> Result<Report> {
    let snapshot = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    let result = report_inner(conn, options, now)?;
    if let Some(snapshot) = snapshot {
        snapshot.commit()?;
    }
    Ok(result)
}
fn report_inner(conn: &Connection, options: &Options, now: DateTime<Utc>) -> Result<Report> {
    let (tz, timezone_warning) = trends::timezone(conn, options.timezone.as_deref())?;
    let today = now.with_timezone(&tz).date_naive();
    let from = options
        .from
        .as_ref()
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?
        .unwrap_or(today - Duration::days(28));
    let to = options
        .to
        .as_ref()
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?
        .unwrap_or(today);
    ensure!(to >= from, "End date is before start date");
    ensure!(
        (to - from).num_days() <= 3660,
        "Daily range exceeds ten years"
    );
    let mut scope = options.scope.clone();
    scope.repo = scope
        .repo
        .as_deref()
        .map(|r| observation::canonical_repo(conn, r))
        .transpose()?;
    let mut warnings = timezone_warning.into_iter().collect::<Vec<_>>();
    let published = conn
        .query_row(
            "SELECT value FROM analytics_setting WHERE key='published_revision'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    if published.is_none() {
        warnings.push("No published daily index. Run ingest to refresh retained facts.".into());
    }
    let works = load_work(conn, &scope)?;
    let ids: BTreeSet<_> = works.iter().map(|w| w.id.clone()).collect();
    let usage = load_usage(conn, &scope, &ids)?;
    let intervals = load_intervals(conn, &ids)?;
    let mut evidence = delivery::evidence(conn)?;
    evidence.retain(|e| {
        let repo_match = scope.repo.as_ref().is_none_or(|repo| {
            observation::canonical_repo(conn, &e.pr.repo_key).is_ok_and(|r| &r == repo)
        });
        let work_match = scope.providers.is_empty() && scope.model.is_none()
            || e.work_ids.iter().any(|id| ids.contains(id));
        repo_match && work_match && scope.intent.as_ref().is_none_or(|i| i == &e.intent)
    });
    let all_sources = observation::coverage(conn)?;
    let sources: Vec<_> = all_sources
        .into_iter()
        .filter(|s| {
            (s.repo_root.as_ref().is_none_or(|r| {
                scope.repo.as_ref().is_none_or(|repo| {
                    observation::canonical_repo(conn, r).is_ok_and(|v| &v == repo)
                })
            })) && (scope.providers.is_empty()
                || scope.providers.contains(&s.provider)
                || ["github", "bitbucket"].contains(&s.provider.as_str()))
        })
        .collect();
    let provider_set: BTreeSet<_> = usage.iter().map(|u| u.provider.as_str()).collect();
    let relevant: Vec<_> = sources
        .iter()
        .filter(|s| provider_set.contains(s.provider.as_str()))
        .collect();
    let source_complete = !relevant.is_empty()
        && relevant.iter().all(|s| s.complete && s.error.is_none())
        && provider_set
            .iter()
            .all(|p| relevant.iter().any(|s| s.provider == *p));
    let observed = relevant
        .iter()
        .filter_map(|s| s.observed_through.as_deref().and_then(parse_ts))
        .min();
    let history_from = relevant
        .iter()
        .filter_map(|s| s.observed_from.as_deref().and_then(parse_ts))
        .max();
    let latest = works
        .iter()
        .filter_map(|w| w.last)
        .chain(usage.iter().filter_map(|u| u.ts))
        .max();
    if !source_complete {
        warnings.push("Source coverage is incomplete or has no recorded watermark; empty periods are not proven inactivity.".into());
    }
    if usage.iter().any(|u| u.availability == "suspect_zero") {
        warnings.push("Zero-filled usage records are uncertain, not measured zero; token totals are observed amounts only.".into());
    }
    if works.iter().any(|w| w.source == "heuristic") {
        warnings.push("Some work intents retain heuristic labels; inspect label coverage before comparing work mix.".into());
    }
    if evidence.is_empty() {
        warnings.push("No PR evidence in scope. Work-item completion, usage, stages and rework remain available.".into());
    }
    let linked: BTreeSet<_> = evidence
        .iter()
        .flat_map(|e| e.work_ids.iter().cloned())
        .collect();
    let shared: BTreeSet<_> = evidence
        .iter()
        .filter(|e| e.shared_work)
        .flat_map(|e| e.work_ids.iter().cloned())
        .collect();
    let attempt_events = events(
        conn,
        "SELECT work_id,ts,kind FROM published_work_event WHERE kind!='correction'",
    )?;
    let correction_events = events(
        conn,
        "SELECT work_id,ts,kind FROM published_work_event WHERE kind='correction'",
    )?;
    let intents: BTreeMap<_, _> = works
        .iter()
        .map(|w| (w.id.as_str(), w.intent.as_str()))
        .collect();
    let mut work_by_day = BTreeMap::new();
    let mut evidence_by_day = BTreeMap::new();
    let lifecycle=conn.prepare("SELECT work_id,at_ms,state,verified FROM published_work_lifecycle ORDER BY at_ms,CASE state WHEN 'open' THEN 0 ELSE 1 END")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,String>(2)?,r.get::<_,bool>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let lifecycle = lifecycle.into_iter().fold(
        BTreeMap::<String, Vec<(i64, String, bool)>>::new(),
        |mut map, (id, at, state, verified)| {
            map.entry(id).or_default().push((at, state, verified));
            map
        },
    );
    let conversation_starts=conn.prepare("SELECT work_id,provider,session_id,MIN(started_ms) FROM published_work_conversation GROUP BY work_id,provider,session_id")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut days = vec![];
    let mut cohorts = vec![];
    for n in 0..=(to - from).num_days() {
        let date = from + Duration::days(n);
        let start = trends::midnight(tz, date)?;
        let end = trends::midnight(tz, date + Duration::days(1))?;
        let cutoff = end.min(now.timestamp_millis());
        let works = works
            .iter()
            .cloned()
            .map(|mut w| {
                if let Some((at, state, verified)) = lifecycle
                    .get(&w.id)
                    .and_then(|events| events.iter().rev().find(|(at, _, _)| *at < cutoff))
                {
                    w.done = (state == "completed").then_some(*at);
                    w.verified = *verified;
                }
                w
            })
            .collect::<Vec<_>>();
        let contains = |ts: Option<i64>| ts.is_some_and(|v| v >= start && v < cutoff);
        let mut day = Day {
            reopened: lifecycle
                .values()
                .flatten()
                .filter(|(at, state, _)| state == "reopened" && contains(Some(*at)))
                .count(),
            date: date.to_string(),
            start_ms: start,
            end_ms: end,
            observed_through_ms: observed.map(|t| t.min(cutoff)),
            partial: end > now.timestamp_millis()
                || observed.is_none_or(|t| t < end)
                || !source_complete
                || history_from.is_some_and(|t| t > start),
            ..Day::default()
        };
        day.coverage.truncated_history = history_from.is_some_and(|t| t > start);
        day.coverage.stale_snapshot = relevant.iter().any(|s| s.kind == "snapshot")
            && observed.is_none_or(|t| t < end.min(now.timestamp_millis()));
        day.coverage.unknown_watermark = observed.is_none();
        let day_usage: Vec<_> = usage.iter().filter(|u| contains(u.ts)).collect();
        let day_intervals: Vec<_> = intervals
            .iter()
            .filter(|i| i.start < cutoff && i.end > start)
            .collect();
        let mut touched: BTreeSet<String> = day_usage
            .iter()
            .map(|u| u.work.clone())
            .chain(day_intervals.iter().map(|i| i.work.clone()))
            .collect();
        touched.extend(
            works
                .iter()
                .filter(|w| contains(w.start) || contains(w.done))
                .map(|w| w.id.clone()),
        );
        let mut cycles = vec![];
        let mut completed_tokens = vec![];
        let mut missing_completed_tokens = 0;
        let mut conversations = vec![];
        let mut goals: BTreeMap<String, Mix> = BTreeMap::new();
        let mut stages: BTreeMap<String, (Mix, BTreeSet<String>)> = BTreeMap::new();
        for w in &works {
            if w.start.is_some_and(|t| t < cutoff)
                && w.done.is_none_or(|t| t >= cutoff)
                && !(w.state == "abandoned" && w.last.is_some_and(|t| t < cutoff))
            {
                day.open += 1;
                day.bug_work_open += usize::from(w.intent == "bug_fix");
            }
            if !touched.contains(&w.id) {
                continue;
            }
            day.started += usize::from(contains(w.start));
            day.bug_work_started += usize::from(contains(w.start) && w.intent == "bug_fix");
            day.active_work += 1;
            day.bug_work_active += usize::from(w.intent == "bug_fix");
            day.abandoned += usize::from(w.state == "abandoned" && contains(w.last));
            day.coverage.heuristic_intents += usize::from(w.source == "heuristic");
            day.coverage.unclassified_intents +=
                usize::from(w.source == "missing" || w.intent == "unclassified");
            day.coverage.completion_unknown += usize::from(w.done.is_none_or(|t| t >= cutoff));
            day.coverage.missing_usage_work += usize::from(w.missing > 0);
            day.coverage.work_without_pr += usize::from(!linked.contains(&w.id));
            day.coverage.shared_work += usize::from(shared.contains(&w.id));
            let own_intervals: Vec<_> = day_intervals
                .iter()
                .filter(|i| i.work == w.id)
                .map(|i| (i.start.max(start), i.end.min(cutoff)))
                .collect();
            let activity = work::union_minutes(own_intervals);
            day.work_activity_minutes += activity;
            let goal = goals.entry(w.intent.clone()).or_insert_with(|| Mix {
                label: w.intent.clone(),
                ..Mix::default()
            });
            goal.work_items += 1;
            goal.activity_minutes += activity;
            if contains(w.done) {
                day.completed += 1;
                day.bug_work_completed += usize::from(w.intent == "bug_fix");
                day.bug_work_verified += usize::from(w.intent == "bug_fix" && w.verified);
                day.completion_with_verification += usize::from(w.verified);
                if !scope.verified_only || w.verified {
                    if let (Some(s), Some(d)) = (w.start, w.done) {
                        cycles.push((d - s).max(0) as f64 / 60000.0);
                    }
                    conversations.push(
                        conversation_starts
                            .iter()
                            .filter(|(id, _, _, at)| id == &w.id && Some(*at) <= w.done)
                            .count() as f64,
                    );
                    let us: Vec<_> = usage
                        .iter()
                        .filter(|u| u.work == w.id && u.ts.is_some_and(|t| Some(t) <= w.done))
                        .collect();
                    if w.missing == 0
                        && !us.is_empty()
                        && us
                            .iter()
                            .all(|u| available(u) && (u.native || scope.include_estimated))
                    {
                        completed_tokens
                            .push(us.iter().map(|u| u.tokens.total).sum::<i64>() as f64);
                    } else {
                        missing_completed_tokens += 1;
                    }
                }
            }
        }
        let mut models: BTreeMap<(String, String), ModelUsage> = BTreeMap::new();
        let mut contexts = vec![];
        let mut peaks: BTreeMap<String, i64> = BTreeMap::new();
        for u in &day_usage {
            let model = models
                .entry((u.provider.clone(), u.model.clone()))
                .or_insert_with(|| ModelUsage {
                    provider: u.provider.clone(),
                    model: u.model.clone(),
                    recorded_invocations: (["opencode", "claude"].contains(&u.provider.as_str()))
                        .then_some(0),
                    ..ModelUsage::default()
                });
            model.usage_events += 1;
            if let Some(n) = model.recorded_invocations.as_mut() {
                *n += 1;
            }
            day.coverage.usage_events += 1;
            model.available_events += usize::from(available(u));
            day.coverage.available_usage_events += usize::from(available(u));
            model.explicit_zero_events += usize::from(u.availability == "explicit_zero");
            model.missing_events += usize::from(u.availability == "missing");
            day.coverage.explicit_zero_events += usize::from(u.availability == "explicit_zero");
            day.coverage.missing_events += usize::from(u.availability == "missing");
            model.suspect_zero_events += usize::from(u.availability == "suspect_zero");
            day.coverage.suspect_zero_events += usize::from(u.availability == "suspect_zero");
            day.coverage.estimated_events += usize::from(!u.native);
            let goal_intent = intents
                .get(u.work.as_str())
                .copied()
                .unwrap_or("unclassified")
                .to_string();
            let goal = goals.entry(goal_intent.clone()).or_insert_with(|| Mix {
                label: goal_intent,
                ..Mix::default()
            });
            let (stage, stage_ids) = stages.entry(u.stage.clone()).or_insert_with(|| {
                (
                    Mix {
                        label: u.stage.clone(),
                        ..Mix::default()
                    },
                    BTreeSet::new(),
                )
            });
            stage_ids.insert(u.work.clone());
            if available(u) {
                if u.native {
                    day.native_tokens.add(&u.tokens);
                    model.native_tokens.add(&u.tokens);
                    goal.native_tokens += u.tokens.total;
                    stage.native_tokens += u.tokens.total;
                } else {
                    day.estimated_tokens.add(&u.tokens);
                    model.estimated_tokens.add(&u.tokens);
                    goal.estimated_tokens += u.tokens.total;
                    stage.estimated_tokens += u.tokens.total;
                }
            }
            optional_sum(&mut model.reported_cost_usd, u.reported);
            optional_sum(&mut model.api_equivalent_usd, u.api);
            optional_sum(&mut day.reported_cost_usd, u.reported);
            optional_sum(&mut day.api_equivalent_usd, u.api);
            if available(u)
                && u.native
                && let Some(v) = u.context
            {
                contexts.push(v as f64);
                peaks
                    .entry(u.work.clone())
                    .and_modify(|p| *p = (*p).max(v))
                    .or_insert(v);
            }
        }
        // Sweep per work item so concurrent stages share time, rather than multiplying it.
        for wid in &touched {
            let mut events: BTreeMap<i64, Vec<(String, i32)>> = BTreeMap::new();
            for i in day_intervals.iter().filter(|i| &i.work == wid) {
                events
                    .entry(i.start.max(start))
                    .or_default()
                    .push((i.stage.clone(), 1));
                events
                    .entry(i.end.min(cutoff))
                    .or_default()
                    .push((i.stage.clone(), -1));
            }
            let mut active: BTreeMap<String, i32> = BTreeMap::new();
            let mut prior = None;
            for (at, changes) in events {
                if let Some(p) = prior {
                    let n = active.values().filter(|n| **n > 0).count();
                    if n > 0 {
                        for (label, count) in &active {
                            if *count > 0 {
                                let (mix, stage_ids) =
                                    stages.entry(label.clone()).or_insert_with(|| {
                                        (
                                            Mix {
                                                label: label.clone(),
                                                ..Mix::default()
                                            },
                                            BTreeSet::new(),
                                        )
                                    });
                                mix.activity_minutes += (at - p) as f64 / 60000.0 / n as f64;
                                stage_ids.insert(wid.clone());
                            }
                        }
                    }
                }
                for (label, delta) in changes {
                    *active.entry(label).or_default() += delta;
                }
                prior = Some(at);
            }
        }
        day.coverage.work_items = day.active_work;
        day.coverage.source_complete = source_complete;
        day.attempts = attempt_events
            .iter()
            .filter(|(id, ts, _)| ids.contains(id) && contains(*ts))
            .count();
        day.validation_failures = attempt_events
            .iter()
            .filter(|(id, ts, status)| ids.contains(id) && contains(*ts) && status == "failed")
            .count();
        day.corrections = correction_events
            .iter()
            .filter(|(id, ts, _)| ids.contains(id) && contains(*ts))
            .count();
        day.cycle_minutes = distribution(cycles, 0);
        day.tokens_per_completed = distribution(completed_tokens, missing_completed_tokens);
        day.conversations_per_completed = distribution(conversations, 0);
        day.request_context = distribution(
            contexts,
            day.coverage.usage_events - day.coverage.available_usage_events,
        );
        day.peak_context = distribution(peaks.values().map(|v| *v as f64).collect(), 0);
        day.goal_mix = goals
            .into_values()
            .map(|mut g| {
                g.work_share =
                    (day.active_work > 0).then(|| g.work_items as f64 / day.active_work as f64);
                g
            })
            .collect();
        day.activity_mix = stages
            .into_values()
            .map(|(mut g, ws)| {
                g.work_items = ws.len();
                g.work_share =
                    (day.active_work > 0).then(|| g.work_items as f64 / day.active_work as f64);
                g
            })
            .collect();
        day.models = models.into_values().collect();
        let mut delivery_lead = vec![];
        let mut pr_lead = vec![];
        let mut lag = vec![];
        let mut pr_tokens = vec![];
        let mut pr_activity = vec![];
        let mut pr_conversations = vec![];
        let mut incomplete_pr_usage = 0;
        let mut cohort = Cohort {
            date: date.to_string(),
            prs_opened: 0,
            bug_fix_prs: 0,
            mature: 0,
            pending_followup: 0,
            deployed_within_window: 0,
            closed_without_merge: 0,
            share_deployed_within_window: None,
        };
        let mut failed_events = BTreeSet::new();
        let mut rollback_events = BTreeSet::new();
        for e in &evidence {
            let created = e.pr.created_at.as_deref().and_then(parse_ts);
            let merged = e.pr.merged_at.as_deref().and_then(parse_ts);
            let closed = e.pr.closed_at.as_deref().and_then(parse_ts);
            let deployed = e.deployed_at.as_deref().and_then(parse_ts);
            day.pull_requests.opened += usize::from(contains(created));
            day.pull_requests.merged += usize::from(contains(merged));
            day.pull_requests.closed_without_merge +=
                usize::from(contains(closed) && merged.is_none());
            let pr_active = contains(created)
                || contains(merged)
                || contains(closed)
                || contains(deployed)
                || e.work_ids.iter().any(|id| touched.contains(id))
                || e.pr
                    .updated_at
                    .as_deref()
                    .and_then(parse_ts)
                    .is_some_and(|t| contains(Some(t)));
            if pr_active {
                day.pull_requests.active += 1;
                day.pull_requests.bug_fix_prs += usize::from(e.intent == "bug_fix");
                day.coverage.prs_without_work += usize::from(e.work_ids.is_empty());
                day.coverage.uncertain_pr_intents += usize::from(e.intent == "unknown");
            }
            if merged.is_some_and(|m| m < cutoff) && deployed.is_none_or(|d| d >= cutoff) {
                day.pull_requests.pending_delivery += 1;
            }
            failed_events.extend(
                e.deployments
                    .iter()
                    .filter(|d| {
                        contains(parse_ts(&d.occurred_at))
                            && ["failed", "failure", "error"].contains(&d.status.as_str())
                    })
                    .map(|d| d.id.clone()),
            );
            rollback_events.extend(
                e.deployments
                    .iter()
                    .filter(|d| {
                        contains(parse_ts(&d.occurred_at))
                            && (d.status == "rollback" || d.rollback_of.is_some())
                    })
                    .map(|d| d.id.clone()),
            );
            if contains(merged)
                && let (Some(a), Some(b)) = (created, merged)
                && b >= a
            {
                pr_lead.push((b - a) as f64 / 60000.0);
            }
            if contains(deployed) {
                day.pull_requests.deployed += 1;
                day.pull_requests.bug_fix_prs_deployed += usize::from(e.intent == "bug_fix");
                let associated: Vec<_> = works
                    .iter()
                    .filter(|w| e.work_ids.contains(&w.id))
                    .collect();
                if let (Some(first), Some(done)) =
                    (associated.iter().filter_map(|w| w.start).min(), deployed)
                    && done >= first
                {
                    delivery_lead.push((done - first) as f64 / 60000.0);
                }
                if let (Some(a), Some(b)) = (merged, deployed) {
                    lag.push((b - a).max(0) as f64 / 60000.0);
                }
                let us: Vec<_> = usage
                    .iter()
                    .filter(|u| {
                        e.work_ids.contains(&u.work) && u.ts.is_some_and(|t| Some(t) <= deployed)
                    })
                    .collect();
                if !e.shared_work
                    && !associated.is_empty()
                    && associated.iter().all(|w| w.missing == 0)
                    && !us.is_empty()
                    && us
                        .iter()
                        .all(|u| available(u) && (u.native || scope.include_estimated))
                {
                    pr_tokens.push(us.iter().map(|u| u.tokens.total).sum::<i64>() as f64);
                    pr_conversations
                        .push(associated.iter().map(|w| w.conversations).sum::<usize>() as f64);
                    pr_activity.push(work::union_minutes(
                        intervals
                            .iter()
                            .filter(|i| e.work_ids.contains(&i.work))
                            .map(|i| (i.start, i.end.min(deployed.unwrap())))
                            .collect(),
                    ));
                } else {
                    incomplete_pr_usage += 1;
                }
            }
            if contains(created) {
                cohort.prs_opened += 1;
                cohort.bug_fix_prs += usize::from(e.intent == "bug_fix");
                let window =
                    created.unwrap() + i64::from(options.follow_up_days.max(1)) * 86_400_000;
                let release_sources: Vec<_> = sources
                    .iter()
                    .filter(|s| {
                        s.repo_root.as_ref().is_some_and(|r| {
                            observation::canonical_repo(conn, r).is_ok_and(|r| r == e.pr.repo_key)
                        }) && s.kind == "pull_requests_and_deployments"
                    })
                    .collect();
                let mature = window <= now.timestamp_millis()
                    && !release_sources.is_empty()
                    && release_sources.iter().all(|s| {
                        s.complete
                            && s.observed_from
                                .as_deref()
                                .and_then(parse_ts)
                                .is_some_and(|t| t <= created.unwrap())
                            && s.observed_through
                                .as_deref()
                                .and_then(parse_ts)
                                .is_some_and(|t| t >= window)
                    });
                if mature {
                    cohort.mature += 1;
                    cohort.deployed_within_window +=
                        usize::from(deployed.is_some_and(|d| d >= created.unwrap() && d <= window));
                    cohort.closed_without_merge +=
                        usize::from(merged.is_none() && closed.is_some_and(|c| c <= window));
                } else {
                    cohort.pending_followup += 1;
                }
            }
        }
        day.pull_requests.failed_deployment_events = failed_events.len();
        day.pull_requests.rollback_events = rollback_events.len();
        day.pull_requests.engineering_delivery_minutes = distribution(delivery_lead, 0);
        day.pull_requests.pr_lead_minutes = distribution(pr_lead, 0);
        day.pull_requests.deployment_lag_minutes = distribution(lag, 0);
        day.pull_requests.tokens_per_delivered_pr = distribution(pr_tokens, incomplete_pr_usage);
        day.pull_requests.activity_per_delivered_pr =
            distribution(pr_activity, incomplete_pr_usage);
        day.pull_requests.conversations_per_delivered_pr =
            distribution(pr_conversations, incomplete_pr_usage);
        cohort.share_deployed_within_window = (cohort.mature > 0)
            .then(|| cohort.deployed_within_window as f64 / cohort.mature as f64);
        let mut evidence_ids = BTreeMap::new();
        evidence_ids.insert("active".into(), touched.iter().cloned().collect());
        evidence_ids.insert(
            "started".into(),
            works
                .iter()
                .filter(|w| contains(w.start))
                .map(|w| w.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "completed".into(),
            works
                .iter()
                .filter(|w| contains(w.done))
                .map(|w| w.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "bug_completed".into(),
            works
                .iter()
                .filter(|w| w.intent == "bug_fix" && contains(w.done))
                .map(|w| w.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "bug_work".into(),
            works
                .iter()
                .filter(|w| w.intent == "bug_fix" && touched.contains(&w.id))
                .map(|w| w.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "open".into(),
            works
                .iter()
                .filter(|w| {
                    w.start.is_some_and(|t| t < cutoff)
                        && w.done.is_none_or(|t| t >= cutoff)
                        && !(w.state == "abandoned" && w.last.is_some_and(|t| t < cutoff))
                })
                .map(|w| w.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "deployed".into(),
            evidence
                .iter()
                .filter(|e| contains(e.deployed_at.as_deref().and_then(parse_ts)))
                .map(|e| e.pr.id.clone())
                .collect(),
        );
        evidence_ids.insert(
            "usage".into(),
            day_usage.iter().map(|u| u.id.clone()).collect(),
        );
        evidence_by_day.insert(day.date.clone(), evidence_ids);
        work_by_day.insert(day.date.clone(), touched.into_iter().collect());
        days.push(day);
        cohorts.push(cohort);
    }
    let classification_versions=conn.prepare("SELECT DISTINCT question_version||':'||COALESCE(model_version,source) FROM classification_result WHERE superseded_by IS NULL ORDER BY 1")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<_>>()?;
    Ok(Report {
        schema_version: 1,
        metric_version: "work-daily-1".into(),
        generated_at: now.to_rfc3339(),
        published_revision: published,
        timezone: tz.name().into(),
        scope,
        from: from.to_string(),
        to: to.to_string(),
        latest_activity: latest,
        sources,
        warnings,
        days,
        cohorts,
        follow_up_days: options.follow_up_days.max(1),
        classification_versions,
        work_by_day,
        evidence_by_day,
    })
}
fn available(u: &Usage) -> bool {
    ["native", "estimated", "explicit_zero"].contains(&u.availability.as_str())
}
fn events(conn: &Connection, sql: &str) -> Result<Vec<(String, Option<i64>, String)>> {
    Ok(conn
        .prepare(sql)?
        .query_map([], |r| Ok((r.get(0)?, instant(r.get(1)?), r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?)
}
fn load_work(conn: &Connection, scope: &trends::Scope) -> Result<Vec<IndexedWork>> {
    let mut q=conn.prepare("SELECT w.work_id,w.intent,w.intent_source,w.started_ms,w.completed_ms,w.last_ms,w.state,w.verified,w.conversations,w.missing_usage FROM derived_work_index w WHERE w.observed=1")?;
    let all = q
        .query_map([], |r| {
            Ok(IndexedWork {
                id: r.get(0)?,
                intent: r.get(1)?,
                source: r.get(2)?,
                start: r.get(3)?,
                done: r.get(4)?,
                last: r.get(5)?,
                state: r.get(6)?,
                verified: r.get(7)?,
                conversations: r.get::<_, i64>(8)? as usize,
                missing: r.get::<_, i64>(9)? as usize,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut q=conn.prepare("SELECT m.work_id,m.repo_root,m.provider,t.models FROM derived_work_index_member m JOIN published_work_models t USING(provider,session_id,task_index)")?;
    let allowed = q
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, repo, provider, models)| {
            scope.repo.as_ref().is_none_or(|r| repo.as_ref() == Some(r))
                && (scope.providers.is_empty() || scope.providers.contains(provider))
                && scope
                    .model
                    .as_ref()
                    .is_none_or(|m| models.to_lowercase().contains(&m.to_lowercase()))
        })
        .map(|r| r.0)
        .collect::<BTreeSet<_>>();
    Ok(all
        .into_iter()
        .filter(|w| allowed.contains(&w.id) && scope.intent.as_ref().is_none_or(|i| i == &w.intent))
        .collect())
}
fn load_usage(
    conn: &Connection,
    scope: &trends::Scope,
    ids: &BTreeSet<String>,
) -> Result<Vec<Usage>> {
    let mut q=conn.prepare("SELECT usage_id,work_id,usage_ts,provider,model_name,token_source,availability,input_tokens,cache_write,cache_read,output_tokens,reasoning_extra,total_tokens,reported_cost_usd,api_equiv_usd,stage,context_tokens FROM published_work_usage ORDER BY usage_id")?;
    let all = q
        .query_map([], |r| {
            Ok(Usage {
                id: r.get::<_, i64>(0)?.to_string(),
                work: r.get(1)?,
                ts: instant(r.get(2)?),
                provider: r.get(3)?,
                model: r.get(4)?,
                native: r.get::<_, String>(5)? == "reported",
                availability: r.get(6)?,
                tokens: Tokens {
                    uncached_input: r.get(7)?,
                    cache_write: r.get(8)?,
                    cache_read: r.get(9)?,
                    output: r.get(10)?,
                    reasoning_extra: r.get(11)?,
                    total: r.get(12)?,
                },
                reported: r.get(13)?,
                api: r.get(14)?,
                stage: r.get(15)?,
                context: r.get(16)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut seen = BTreeSet::new();
    Ok(all
        .into_iter()
        .filter(|u| {
            ids.contains(&u.work)
                && (scope.providers.is_empty() || scope.providers.contains(&u.provider))
                && scope
                    .model
                    .as_ref()
                    .is_none_or(|m| u.model.to_lowercase().contains(&m.to_lowercase()))
                && seen.insert(u.id.clone())
        })
        .collect())
}
fn load_intervals(conn: &Connection, ids: &BTreeSet<String>) -> Result<Vec<Interval>> {
    let mut q =
        conn.prepare("SELECT work_id,start_ms,end_ms,stage FROM published_work_activity")?;
    Ok(q.query_map([], |r| {
        Ok(Interval {
            work: r.get(0)?,
            start: r.get(1)?,
            end: r.get(2)?,
            stage: r.get(3)?,
        })
    })?
    .collect::<rusqlite::Result<Vec<_>>>()?
    .into_iter()
    .filter(|i| ids.contains(&i.work))
    .collect())
}

/// Evidence is paginated independently, so a summary never repeats thousands of IDs.
pub fn contributors(
    conn: &Connection,
    options: &Options,
    date: &str,
    metric: &str,
    offset: usize,
    limit: usize,
) -> Result<serde_json::Value> {
    ensure!(limit > 0 && limit <= 500, "Evidence limit must be 1..500");
    let mut opts = options.clone();
    opts.from = Some(date.into());
    opts.to = Some(date.into());
    let report = report(conn, &opts)?;
    let ids:Vec<String>=report.evidence_by_day.get(date).and_then(|metrics|metrics.get(metric)).cloned().ok_or_else(||anyhow::anyhow!("Metric must be started, completed, bug_completed, active, bug_work, open, deployed or usage"))?;
    let total = ids.len();
    let page: Vec<_> = ids.into_iter().skip(offset).take(limit).collect();
    Ok(
        serde_json::json!({"schema_version":1,"timezone":report.timezone,"scope":report.scope,"date":date,"metric":metric,"record_type":if metric=="deployed"{"pull_request"}else if metric=="usage"{"usage_event"}else{"work_item"},"total":total,"offset":offset,"limit":limit,"ids":page,"next_offset":(offset+page.len()<total).then_some(offset+page.len())}),
    )
}
