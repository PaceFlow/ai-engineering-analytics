//! Purpose coverage over existing published records. No additional storage.
use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub corrective_goal_work: usize,
    pub unknown_goal_work: usize,
    pub heuristic_goal_work: usize,
    pub corrective_goal_ids: Vec<String>,
    pub corrective_subtypes: BTreeMap<String, usize>,
    pub native_tokens_by_activity_purpose: BTreeMap<String, i64>,
    pub activity_minutes_by_purpose: BTreeMap<String, f64>,
    pub structured_history_work: usize,
}
pub struct Index {
    goals: BTreeMap<String, (String, String, String, bool)>,
    usage: Vec<(String, String, Option<i64>, i64, String)>,
    activity: BTreeMap<String, Vec<(i64, i64, String)>>,
}
impl Index {
    pub fn load(conn: &Connection) -> Result<Self> {
        let mut goals = BTreeMap::new();
        let mut q = conn.prepare(
            "SELECT work_id,COALESCE(purpose_json,'{}') FROM derived_work_index ORDER BY work_id",
        )?;
        for row in q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (id, raw) = row?;
            let v: serde_json::Value = serde_json::from_str(&raw)?;
            goals.insert(
                id,
                (
                    v["purpose"].as_str().unwrap_or("unknown").to_owned(),
                    v["subtype"].as_str().unwrap_or("unspecified").to_owned(),
                    v["source"].as_str().unwrap_or("missing").to_owned(),
                    v["history"].as_bool().unwrap_or(false),
                ),
            );
        }
        let usage=conn.prepare("SELECT CAST(usage_id AS TEXT),work_id,usage_ts,CASE WHEN token_source='reported' AND availability IN ('native','explicit_zero') THEN total_tokens ELSE 0 END,COALESCE(activity_purpose,'unknown') FROM published_work_usage")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get::<_,Option<String>>(2)?.as_deref().and_then(super::structure::parse_ts),r.get(3)?,r.get(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let activity_rows=conn.prepare("SELECT work_id,start_ms,end_ms,COALESCE(activity_purpose,'unknown') FROM published_work_activity")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut activity: BTreeMap<String, Vec<(i64, i64, String)>> = BTreeMap::new();
        for (w, s, e, p) in activity_rows {
            activity.entry(w).or_default().push((s, e, p));
        }
        Ok(Self {
            goals,
            usage,
            activity,
        })
    }
    pub fn summarize(
        &self,
        work_ids: &BTreeSet<String>,
        usage_ids: Option<&BTreeSet<String>>,
        start: i64,
        end: i64,
    ) -> Summary {
        let mut out = Summary::default();
        for id in work_ids {
            if let Some((purpose, subtype, source, history)) = self.goals.get(id) {
                out.unknown_goal_work += usize::from(purpose == "unknown");
                out.heuristic_goal_work += usize::from(source == "heuristic");
                out.structured_history_work += usize::from(*history);
                if purpose == "corrective" {
                    out.corrective_goal_work += 1;
                    out.corrective_goal_ids.push(id.clone());
                    *out.corrective_subtypes.entry(subtype.clone()).or_default() += 1;
                }
            } else {
                out.unknown_goal_work += 1;
            }
        }
        for (id, work, ts, tokens, purpose) in &self.usage {
            if work_ids.contains(work)
                && usage_ids.is_none_or(|ids| ids.contains(id))
                && ts.is_some_and(|t| t >= start && t < end)
            {
                *out.native_tokens_by_activity_purpose
                    .entry(purpose.clone())
                    .or_default() += tokens;
            }
        }
        // Union overlapping agents per work. Assign an interval to a purpose only
        // when all overlapping evidence agrees; otherwise retain a mixed category.
        for work in work_ids {
            let mut events: BTreeMap<i64, Vec<(&str, i64)>> = BTreeMap::new();
            for (s, e, p) in self
                .activity
                .get(work)
                .into_iter()
                .flatten()
                .filter(|(s, e, _)| *s < end && *e > start)
            {
                events.entry((*s).max(start)).or_default().push((p, 1));
                events.entry((*e).min(end)).or_default().push((p, -1));
            }
            let mut active: BTreeMap<&str, i64> = BTreeMap::new();
            let mut previous = None;
            for (at, changes) in events {
                if let Some(prior) = previous {
                    let labels: Vec<_> = active
                        .iter()
                        .filter(|(_, n)| **n > 0)
                        .map(|(p, _)| *p)
                        .collect();
                    if !labels.is_empty() {
                        let label = if labels.len() == 1 {
                            labels[0]
                        } else {
                            "mixed"
                        };
                        *out.activity_minutes_by_purpose
                            .entry(label.into())
                            .or_default() += (at - prior) as f64 / 60000.;
                    }
                }
                for (label, delta) in changes {
                    *active.entry(label).or_default() += delta;
                }
                previous = Some(at);
            }
        }
        out
    }
}
