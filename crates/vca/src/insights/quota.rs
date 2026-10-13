//! Subscription quota windows.
//!
//! Codex logs the provider's own quota percentages (`rate_limits`) with every
//! token count; those windows are *observed*. Each rise in `used_percent`
//! between two snapshots is allocated to the usage events in that interval
//! in proportion to their API-equivalent cost (tokens when unpriced): an
//! *allocation*, not a causal measurement. Concurrent sessions share a window.
//!
//! Other tools record no quota telemetry; their windows are token burn in a
//! configured window shape, plus any limit hits the user logged.

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::BTreeMap;

use super::structure::{ms_to_iso, parse_ts};

/// Snapshots of one window may disagree on `resets_at` by a few seconds.
const RESET_TOLERANCE_MS: i64 = 120_000;
/// Snapshot gaps longer than this are reported as unobserved intervals.
pub const UNOBSERVED_GAP_MINUTES: i64 = 30;

pub(crate) const QUOTA_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_quota_window (
    window_id       INTEGER PRIMARY KEY,
    provider        TEXT NOT NULL,
    limit_id        TEXT NOT NULL,
    window_kind     TEXT NOT NULL,
    window_minutes  INTEGER,
    window_start    TEXT,
    resets_at       TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    start_percent   REAL NOT NULL,
    end_percent     REAL NOT NULL,
    max_percent     REAL NOT NULL,
    reached_limit_at TEXT,
    snapshots       INTEGER NOT NULL,
    unobserved_minutes INTEGER NOT NULL DEFAULT 0,
    plan_type       TEXT
);

CREATE TABLE IF NOT EXISTS derived_quota_allocation (
    window_id       INTEGER NOT NULL,
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    root_session_id TEXT NOT NULL,
    delta_percent   REAL NOT NULL,
    tokens          INTEGER NOT NULL,
    api_equiv_usd   REAL,
    background_tokens INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(window_id, provider, session_id)
);
";

#[derive(Debug, Clone)]
struct Snapshot {
    ts: i64,
    used: f64,
    resets_at: Option<i64>,
    window_minutes: Option<i64>,
    plan_type: Option<String>,
}

#[derive(Debug, Clone)]
struct Event {
    ts: i64,
    session_id: String,
    root_session_id: String,
    tokens: i64,
    cost: Option<f64>,
    background: bool,
}

pub fn refresh_quota_windows(conn: &Connection) -> Result<()> {
    conn.execute_batch("DELETE FROM derived_quota_window; DELETE FROM derived_quota_allocation;")?;

    let mut series: BTreeMap<(String, String, String), Vec<Snapshot>> = BTreeMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT provider, limit_id, window_kind, ts, used_percent, resets_at, window_minutes,
                    plan_type
             FROM fact_rate_limit_snapshot",
        )?;
        let rows = stmt.query_map([], |row| {
            let ts: String = row.get(3)?;
            let resets: Option<String> = row.get(5)?;
            Ok((
                (
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ),
                ts,
                row.get::<_, f64>(4)?,
                resets,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;
        for row in rows {
            let (key, ts, used, resets, minutes, plan) = row?;
            let Some(ts) = parse_ts(&ts) else {
                continue;
            };
            series.entry(key).or_default().push(Snapshot {
                ts,
                used,
                resets_at: resets.as_deref().and_then(parse_ts),
                window_minutes: minutes,
                plan_type: plan,
            });
        }
    }
    if series.is_empty() {
        return Ok(());
    }

    let mut events_by_provider: BTreeMap<String, Vec<Event>> = BTreeMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT provider, session_id, root_session_id, usage_ts, total_tokens, api_equiv_usd,
                    background_flag
             FROM derived_usage_event WHERE usage_ts IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            let ts: String = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?,
                ts,
                Event {
                    ts: 0,
                    session_id: row.get(1)?,
                    root_session_id: row.get(2)?,
                    tokens: row.get(4)?,
                    cost: row.get(5)?,
                    background: row.get::<_, i64>(6)? != 0,
                },
            ))
        })?;
        for row in rows {
            let (provider, ts, mut event) = row?;
            if let Some(ts) = parse_ts(&ts) {
                event.ts = ts;
                events_by_provider.entry(provider).or_default().push(event);
            }
        }
        for events in events_by_provider.values_mut() {
            events.sort_by_key(|event| event.ts);
        }
    }

    let mut window_stmt = conn.prepare(
        "INSERT INTO derived_quota_window (
            provider, limit_id, window_kind, window_minutes, window_start, resets_at, first_seen,
            last_seen, start_percent, end_percent, max_percent, reached_limit_at, snapshots,
            unobserved_minutes, plan_type
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
    )?;
    let mut allocation_stmt = conn.prepare(
        "INSERT INTO derived_quota_allocation (
            window_id, provider, session_id, root_session_id, delta_percent, tokens,
            api_equiv_usd, background_tokens
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(window_id, provider, session_id) DO UPDATE SET
            delta_percent = delta_percent + excluded.delta_percent,
            tokens = tokens + excluded.tokens,
            api_equiv_usd = COALESCE(api_equiv_usd, 0) + COALESCE(excluded.api_equiv_usd, 0),
            background_tokens = background_tokens + excluded.background_tokens",
    )?;

    for ((provider, limit_id, window_kind), mut snapshots) in series {
        snapshots.sort_by_key(|snapshot| snapshot.ts);
        snapshots.dedup_by(|a, b| a.ts == b.ts && (a.used - b.used).abs() < f64::EPSILON);
        let events = events_by_provider
            .get(&provider)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        for window in split_windows(&snapshots) {
            let first = &window[0];
            let last = window.last().expect("window has snapshots");
            let max = window.iter().map(|s| s.used).fold(0.0, f64::max);
            let reached = window
                .iter()
                .find(|s| s.used >= 100.0)
                .map(|s| ms_to_iso(s.ts));
            let unobserved: i64 = window
                .windows(2)
                .map(|pair| (pair[1].ts - pair[0].ts) / 60_000)
                .filter(|gap| *gap > UNOBSERVED_GAP_MINUTES)
                .sum();
            let resets_at = last.resets_at;
            let window_start = match (resets_at, last.window_minutes) {
                (Some(reset), Some(minutes)) => Some(reset - minutes * 60_000),
                _ => None,
            };
            window_stmt.execute(params![
                provider,
                limit_id,
                window_kind,
                last.window_minutes,
                window_start.map(ms_to_iso),
                resets_at.map(ms_to_iso),
                ms_to_iso(first.ts),
                ms_to_iso(last.ts),
                first.used,
                last.used,
                max,
                reached,
                window.len() as i64,
                unobserved,
                last.plan_type
            ])?;
            let window_id = conn.last_insert_rowid();

            // Codex reports whole percents, so usage accumulates silently and
            // the percentage ticks up later. Each rise is allocated to the
            // usage since the previous rise.
            let mut last_used = first.used;
            let mut since = window_start.unwrap_or(first.ts).min(first.ts);
            for snapshot in window.iter().skip(1) {
                let delta = snapshot.used - last_used;
                if delta <= 0.0 {
                    continue;
                }
                let interval: Vec<&Event> = events
                    .iter()
                    .filter(|event| event.ts > since && event.ts <= snapshot.ts)
                    .collect();
                last_used = snapshot.used;
                since = snapshot.ts;
                let cost_total: f64 = interval.iter().filter_map(|event| event.cost).sum();
                let token_total: i64 = interval.iter().map(|event| event.tokens).sum();
                for event in &interval {
                    let share = if cost_total > 0.0 {
                        event.cost.unwrap_or(0.0) / cost_total
                    } else if token_total > 0 {
                        event.tokens as f64 / token_total as f64
                    } else {
                        0.0
                    };
                    allocation_stmt.execute(params![
                        window_id,
                        provider,
                        event.session_id,
                        event.root_session_id,
                        delta * share,
                        event.tokens,
                        event.cost,
                        if event.background { event.tokens } else { 0 }
                    ])?;
                }
            }
        }
    }
    Ok(())
}

/// Split a snapshot series into windows: a new window starts when `resets_at`
/// moves or the percentage drops.
fn split_windows(snapshots: &[Snapshot]) -> Vec<Vec<Snapshot>> {
    let mut windows: Vec<Vec<Snapshot>> = Vec::new();
    for snapshot in snapshots {
        let starts_new = match windows.last().and_then(|window| window.last()) {
            None => true,
            Some(previous) => {
                let reset_moved = match (previous.resets_at, snapshot.resets_at) {
                    (Some(a), Some(b)) => (a - b).abs() > RESET_TOLERANCE_MS,
                    _ => false,
                };
                reset_moved || snapshot.used + 0.5 < previous.used
            }
        };
        if starts_new {
            windows.push(vec![snapshot.clone()]);
        } else if let Some(window) = windows.last_mut() {
            window.push(snapshot.clone());
        }
    }
    windows
}

/// Record a user observation such as "hit the Claude limit at 15:47".
pub fn record_observation(
    conn: &Connection,
    provider: &str,
    observed_at: &str,
    kind: &str,
    note: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_quota_observation(provider, observed_at, kind, note) VALUES (?1, ?2, ?3, ?4)",
        params![provider, observed_at, kind, note],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;
    use crate::db::session_store::{
        RateLimitRecord, SessionRecord, UsageRecord, store_session_record,
    };

    fn snapshot(ts: &str, used: f64, reset: &str) -> RateLimitRecord {
        RateLimitRecord {
            ts: ts.into(),
            limit_id: "codex".into(),
            window_kind: "primary".into(),
            window_minutes: Some(300),
            used_percent: used,
            resets_at: Some(reset.into()),
            plan_type: Some("plus".into()),
            credits_balance: None,
        }
    }

    fn usage(key: &str, ts: &str, output: i64) -> UsageRecord {
        let mut usage = UsageRecord::new(key);
        usage.ts = Some(ts.into());
        usage.model = Some("gpt-5.5".into());
        usage.input_tokens = 1000;
        usage.output_tokens = output;
        usage.reasoning_in_output = true;
        usage
    }

    #[test]
    fn quota_delta_allocation_sums_to_window_delta() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let reset = "2026-10-01T15:00:00Z";
        let mut a = SessionRecord::new("codex", "a");
        a.started_at = Some("2026-10-01T10:00:00Z".into());
        a.usage = vec![
            usage("a1", "2026-10-01T10:10:00Z", 3000),
            usage("a2", "2026-10-01T10:30:00Z", 1000),
        ];
        a.rate_limits = vec![
            snapshot("2026-10-01T10:00:00Z", 2.0, reset),
            snapshot("2026-10-01T10:10:00Z", 12.0, reset),
            snapshot("2026-10-01T10:30:00Z", 20.0, reset),
        ];
        let mut b = SessionRecord::new("codex", "b");
        b.started_at = Some("2026-10-01T10:05:00Z".into());
        b.usage = vec![usage("b1", "2026-10-01T10:20:00Z", 1000)];
        b.rate_limits = vec![snapshot("2026-10-01T10:20:00Z", 16.0, reset)];
        store_session_record(&conn, &a)?;
        store_session_record(&conn, &b)?;
        crate::db::session_store::refresh_session_roots(&conn)?;
        crate::insights::structure::refresh_session_structure(&conn)?;
        refresh_quota_windows(&conn)?;

        let (windows, start, end): (i64, f64, f64) = conn.query_row(
            "SELECT COUNT(*), MIN(start_percent), MAX(end_percent) FROM derived_quota_window",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(
            windows, 1,
            "snapshots from both sessions merge into one window"
        );
        assert_eq!((start, end), (2.0, 20.0));
        let allocated: f64 = conn.query_row(
            "SELECT SUM(delta_percent) FROM derived_quota_allocation",
            [],
            |r| r.get(0),
        )?;
        assert!((allocated - 18.0).abs() < 1e-9);
        let b_share: f64 = conn.query_row(
            "SELECT delta_percent FROM derived_quota_allocation WHERE session_id = 'b'",
            [],
            |r| r.get(0),
        )?;
        assert!((b_share - 4.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn reset_starts_a_new_window() {
        let snaps = vec![
            Snapshot {
                ts: 0,
                used: 50.0,
                resets_at: Some(1_000_000),
                window_minutes: Some(300),
                plan_type: None,
            },
            Snapshot {
                ts: 10,
                used: 90.0,
                resets_at: Some(1_000_000),
                window_minutes: Some(300),
                plan_type: None,
            },
            Snapshot {
                ts: 20,
                used: 3.0,
                resets_at: Some(19_000_000),
                window_minutes: Some(300),
                plan_type: None,
            },
        ];
        assert_eq!(split_windows(&snaps).len(), 2);
    }
}
