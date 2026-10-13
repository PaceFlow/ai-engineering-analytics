//! Relations between tasks: later AI work that removes or rewrites lines
//! earlier AI work added (by line hash, same file, within 30 days), and
//! follow-ups on the same branch. Existence and size are deterministic;
//! *why* the lines changed (`rework_kind`) is a label (heuristic or Jev).

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::structure::parse_ts;

pub const RELATION_WINDOW_DAYS: f64 = 30.0;
/// Fewer overlapping lines than this are treated as incidental and not stored.
pub const MIN_OVERLAP_LINES: i64 = 3;
const FOLLOWUP_HOURS: f64 = 48.0;

pub(crate) const RELATIONS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_task_relation (
    from_provider  TEXT NOT NULL,
    from_session   TEXT NOT NULL,
    from_task      INTEGER NOT NULL,
    to_provider    TEXT NOT NULL,
    to_session     TEXT NOT NULL,
    to_task        INTEGER NOT NULL,
    relation_kind  TEXT NOT NULL,
    overlap_lines  INTEGER NOT NULL DEFAULT 0,
    overlap_files  INTEGER NOT NULL DEFAULT 0,
    files          TEXT,
    gap_hours      REAL,
    from_ts        TEXT,
    to_ts          TEXT,
    PRIMARY KEY(from_provider, from_session, from_task, to_provider, to_session, to_task, relation_kind)
);
CREATE INDEX IF NOT EXISTS idx_derived_task_relation_to
    ON derived_task_relation(to_provider, to_session, to_task);
";

type TaskKey = (String, String, i64);

/// Unit key of a relation for classification results and overrides.
pub fn relation_unit_key(from: &TaskKey, to: &TaskKey) -> String {
    format!(
        "{}|{}|{}>{}|{}|{}",
        from.0, from.1, from.2, to.0, to.1, to.2
    )
}

struct RootTurns {
    /// (turn start ms, task index), sorted by start.
    turns: Vec<(i64, i64)>,
}

impl RootTurns {
    fn task_at(&self, ts: i64) -> Option<i64> {
        let mut current = self.turns.first().map(|(_, task)| *task);
        for (start, task) in &self.turns {
            if *start <= ts {
                current = Some(*task);
            } else {
                break;
            }
        }
        current
    }
}

fn load_root_turns(conn: &Connection) -> Result<HashMap<(String, String), RootTurns>> {
    let mut task_of_turn: HashMap<(String, String, i64), i64> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index, COALESCE(turn_list, '') FROM derived_task",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (provider, session, task, list) = row?;
        for turn in list
            .split(',')
            .filter_map(|value| value.trim().parse::<i64>().ok())
        {
            task_of_turn.insert((provider.clone(), session.clone(), turn), task);
        }
    }
    let mut out: HashMap<(String, String), RootTurns> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, turn_index, started_at FROM derived_turn
         WHERE session_id = root_session_id ORDER BY provider, session_id, turn_index",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    for row in rows {
        let (provider, session, turn, started) = row?;
        let Some(task) = task_of_turn.get(&(provider.clone(), session.clone(), turn)) else {
            continue;
        };
        let start = started.as_deref().and_then(parse_ts).unwrap_or(i64::MIN);
        out.entry((provider, session))
            .or_insert_with(|| RootTurns { turns: Vec::new() })
            .turns
            .push((start, *task));
    }
    for root in out.values_mut() {
        root.turns.sort_by_key(|(start, _)| *start);
    }
    Ok(out)
}

#[derive(Default)]
struct Edge {
    lines: i64,
    files: BTreeSet<String>,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
}

/// Rebuild `derived_task_relation`.
pub fn refresh_relations(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM derived_task_relation", [])?;
    let roots = load_root_turns(conn)?;
    let session_root: HashMap<(String, String), String> = {
        let mut stmt = conn.prepare(
            "SELECT provider, session_id, COALESCE(NULLIF(root_session_id, ''), session_id) FROM metadata_sessions",
        )?;
        stmt.query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?
    };
    let task_of_change = |provider: &str, session: &str, ts: Option<i64>| -> Option<TaskKey> {
        let root = session_root
            .get(&(provider.to_string(), session.to_string()))
            .cloned()
            .unwrap_or_else(|| session.to_string());
        let turns = roots.get(&(provider.to_string(), root.clone()))?;
        let task = turns.task_at(ts.unwrap_or(i64::MAX))?;
        Some((provider.to_string(), root, task))
    };

    // Lines a later change removed that an earlier AI change added.
    let mut stmt = conn.prepare(
        "SELECT later.provider, later.session_id, later.change_ts,
                earlier.provider, earlier.session_id, earlier.change_ts,
                later.rel_path, MIN(lh.count, eh.count)
         FROM fact_session_code_change later
         JOIN fact_session_code_change_line_hashes lh
           ON lh.code_change_id = later.id AND lh.side = '-'
         JOIN fact_session_code_change_line_hashes eh
           ON eh.side = '+' AND eh.line_hash = lh.line_hash
         JOIN fact_session_code_change earlier
           ON earlier.id = eh.code_change_id
         WHERE earlier.id <> later.id
           AND earlier.repo_root = later.repo_root
           AND earlier.rel_path = later.rel_path
           AND earlier.change_ts < later.change_ts
           AND julianday(later.change_ts) - julianday(earlier.change_ts) <= ?1",
    )?;
    let rows = stmt.query_map(params![RELATION_WINDOW_DAYS], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    let mut edges: BTreeMap<(TaskKey, TaskKey), Edge> = BTreeMap::new();
    for row in rows {
        let (lp, ls, lts, ep, es, ets, path, count) = row?;
        let later_ts = lts.as_deref().and_then(parse_ts);
        let earlier_ts = ets.as_deref().and_then(parse_ts);
        let (Some(to), Some(from)) = (
            task_of_change(&lp, &ls, later_ts),
            task_of_change(&ep, &es, earlier_ts),
        ) else {
            continue;
        };
        if to == from {
            continue;
        }
        let edge = edges.entry((from, to)).or_default();
        edge.lines += count;
        if let Some(path) = path {
            edge.files.insert(path);
        }
        edge.from_ts = edge.from_ts.max(earlier_ts);
        edge.to_ts = match (edge.to_ts, later_ts) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO derived_task_relation (
            from_provider, from_session, from_task, to_provider, to_session, to_task,
            relation_kind, overlap_lines, overlap_files, files, gap_hours, from_ts, to_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?;
    for ((from, to), edge) in &edges {
        if edge.lines < MIN_OVERLAP_LINES {
            continue;
        }
        let gap = match (edge.from_ts, edge.to_ts) {
            (Some(a), Some(b)) => Some((b - a) as f64 / 3_600_000.0),
            _ => None,
        };
        insert.execute(params![
            from.0,
            from.1,
            from.2,
            to.0,
            to.1,
            to.2,
            "touches_earlier_ai_lines",
            edge.lines,
            edge.files.len() as i64,
            edge.files
                .iter()
                .take(8)
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            gap,
            edge.from_ts.map(super::structure::ms_to_iso),
            edge.to_ts.map(super::structure::ms_to_iso),
        ])?;
    }

    // Same-branch follow-ups: a later session's first task on the branch an
    // earlier session ended on, within 48 hours.
    conn.execute(
        "INSERT OR IGNORE INTO derived_task_relation (
            from_provider, from_session, from_task, to_provider, to_session, to_task,
            relation_kind, gap_hours, from_ts, to_ts
         )
         SELECT a.provider, a.session_id, a.task_index, b.provider, b.session_id, b.task_index,
                'same_branch_followup',
                (julianday(b.started_at) - julianday(a.last_activity_at)) * 24.0,
                a.last_activity_at, b.started_at
         FROM derived_task a
         JOIN derived_task b
           ON b.repo_root = a.repo_root AND b.git_branch = a.git_branch
          AND b.session_id <> a.session_id AND b.task_index = 0
         WHERE a.git_branch IS NOT NULL AND a.git_branch NOT IN ('', 'main', 'master', 'HEAD')
           AND a.task_index = (SELECT MAX(task_index) FROM derived_task x
                               WHERE x.provider = a.provider AND x.session_id = a.session_id)
           AND b.started_at > a.last_activity_at
           AND (julianday(b.started_at) - julianday(a.last_activity_at)) * 24.0 <= ?1",
        params![FOLLOWUP_HOURS],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::session_store::{MessageRecord, SessionRecord, store_session_record};

    fn message(key: &str, ts: &str, text: &str) -> MessageRecord {
        MessageRecord {
            key: key.into(),
            ts: Some(ts.into()),
            role: "user".into(),
            text: text.into(),
            is_compaction_summary: false,
        }
    }

    fn change(
        conn: &Connection,
        session: &str,
        ts: &str,
        side: &str,
        hashes: &[&str],
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO fact_session_code_change(provider, session_id, change_ts, repo_root, rel_path, source_kind)
             VALUES ('claude', ?1, ?2, '/repo', 'src/a.rs', 'tool_write')",
            params![session, ts],
        )?;
        let id = conn.last_insert_rowid();
        for hash in hashes {
            conn.execute(
                "INSERT INTO fact_session_code_change_line_hashes(code_change_id, side, line_hash, count)
                 VALUES (?1, ?2, ?3, 1)",
                params![id, side, hash],
            )?;
        }
        Ok(())
    }

    #[test]
    fn relation_detects_later_removal_of_ai_lines() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        for (id, day) in [("early", "01"), ("late", "03")] {
            let mut record = SessionRecord::new("claude", id);
            record.started_at = Some(format!("2026-10-{day}T10:00:00Z"));
            record.ended_at = Some(format!("2026-10-{day}T11:00:00Z"));
            record.messages = vec![message("u1", &format!("2026-10-{day}T10:00:00Z"), "work")];
            store_session_record(&conn, &record)?;
        }
        crate::db::session_store::refresh_session_roots(&conn)?;
        change(
            &conn,
            "early",
            "2026-10-01T10:05:00Z",
            "+",
            &["h1", "h2", "h3", "h4"],
        )?;
        change(
            &conn,
            "late",
            "2026-10-03T10:05:00Z",
            "-",
            &["h1", "h2", "h3", "zz"],
        )?;
        crate::insights::refresh(&conn)?;
        let (from, to, lines, kind): (String, String, i64, String) = conn.query_row(
            "SELECT from_session, to_session, overlap_lines, relation_kind FROM derived_task_relation",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            (from.as_str(), to.as_str(), lines, kind.as_str()),
            ("early", "late", 3, "touches_earlier_ai_lines")
        );
        Ok(())
    }
}
