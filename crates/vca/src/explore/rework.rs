//! Rework between tasks, defect evidence and the split of spend into
//! loop, correction and requirement-change work.
//!
//! Wording rule: later work *touched* AI-written lines; nothing here says the
//! AI *caused* a bug. Evidence levels say how much supports a defect reading.

use anyhow::Result;
use rusqlite::{Connection, params};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::{Filters, TaskRow, TaskSort, tasks};
use crate::insights::relations::relation_unit_key;

type TaskKey = (String, String, i64);

/// Rework kinds that are not defects; counted separately, never as defects.
pub const NON_DEFECT_KINDS: [&str; 5] = [
    "requirement_change",
    "refinement",
    "continuation",
    "revert_abandon",
    "incidental",
];

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RelationRow {
    pub from: TaskKey,
    pub to: TaskKey,
    pub from_title: String,
    pub to_title: String,
    pub relation_kind: String,
    pub overlap_lines: i64,
    pub overlap_files: i64,
    pub files: String,
    pub gap_hours: Option<f64>,
    pub to_ts: Option<String>,
    pub rework_kind: Option<String>,
    pub rework_source: Option<String>,
    pub rework_band: Option<String>,
    pub rework_confidence: Option<f64>,
    /// Cost of the later (correcting) task; counted once, at that task.
    pub to_cost: Option<f64>,
}

/// Relations whose later task is inside the filters.
pub fn relations(conn: &Connection, filters: &Filters) -> Result<Vec<RelationRow>> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let in_scope: HashMap<TaskKey, &TaskRow> = rows.iter().map(|t| (key(t), t)).collect();
    let mut stmt = conn.prepare(
        "SELECT r.from_provider, r.from_session, r.from_task, r.to_provider, r.to_session, r.to_task,
                r.relation_kind, r.overlap_lines, r.overlap_files, COALESCE(r.files, ''), r.gap_hours,
                r.to_ts, COALESCE(f.title, ''), COALESCE(t.title, '')
         FROM derived_task_relation r
         LEFT JOIN derived_task f
           ON f.provider = r.from_provider AND f.session_id = r.from_session AND f.task_index = r.from_task
         LEFT JOIN derived_task t
           ON t.provider = r.to_provider AND t.session_id = r.to_session AND t.task_index = r.to_task
         ORDER BY r.to_ts DESC",
    )?;
    let raw = stmt
        .query_map([], |row| {
            Ok(RelationRow {
                from: (row.get(0)?, row.get(1)?, row.get(2)?),
                to: (row.get(3)?, row.get(4)?, row.get(5)?),
                relation_kind: row.get(6)?,
                overlap_lines: row.get(7)?,
                overlap_files: row.get(8)?,
                files: row.get(9)?,
                gap_hours: row.get(10)?,
                to_ts: row.get(11)?,
                from_title: row.get(12)?,
                to_title: row.get(13)?,
                rework_kind: None,
                rework_source: None,
                rework_band: None,
                rework_confidence: None,
                to_cost: None,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut label_stmt = conn.prepare(
        "SELECT label, source, band, confidence FROM view_label_effective
         WHERE unit_kind = 'relation' AND unit_key = ?1 AND question_id = 'rework_kind'",
    )?;
    let mut out = Vec::new();
    for mut row in raw {
        let Some(task) = in_scope.get(&row.to) else {
            continue;
        };
        row.to_cost = task.cost_total;
        let label = label_stmt
            .query_row(params![relation_unit_key(&row.from, &row.to)], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .ok();
        if let Some((kind, source, band, confidence)) = label {
            row.rework_kind = Some(kind);
            row.rework_source = Some(source);
            row.rework_band = Some(band);
            row.rework_confidence = confidence;
        }
        out.push(row);
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DefectRow {
    pub origin: TaskKey,
    pub title: String,
    pub started_at: Option<String>,
    /// 0–4: L0 lines later changed · L1 fix signal · L2 bug issue ·
    /// L3 classifier says defect correction (≥0.7) · L4 confirmed by you.
    pub level: u8,
    pub signals: Vec<String>,
    pub corrections: usize,
    pub overlap_lines: i64,
    /// Cost of later correcting tasks, split by overlap share. Not additive
    /// with totals (the cost belongs to the correcting tasks).
    pub correction_cost: f64,
    pub origin_cost: Option<f64>,
}

pub fn level_label(level: u8) -> &'static str {
    match level {
        0 => "L0 AI lines later changed",
        1 => "L1 later fix signal",
        2 => "L2 fix linked to bug issue",
        3 => "L3 suspected defect correction",
        _ => "L4 confirmed by you",
    }
}

/// Origin tasks whose AI-written lines later work changed, with evidence.
pub fn suspected_defects(conn: &Connection, filters: &Filters) -> Result<Vec<DefectRow>> {
    let relations = relations(conn, &Filters::default())?;
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let origins_in_scope: HashMap<TaskKey, &TaskRow> = rows.iter().map(|t| (key(t), t)).collect();
    let all_tasks = tasks(conn, &Filters::default(), TaskSort::Started, 1_000_000)?;
    let titles: HashMap<TaskKey, &TaskRow> = all_tasks.iter().map(|t| (key(t), t)).collect();

    // Overlap share of each correcting task across its origins.
    let mut to_total: HashMap<TaskKey, i64> = HashMap::new();
    for relation in relations
        .iter()
        .filter(|r| r.relation_kind == "touches_earlier_ai_lines")
    {
        *to_total.entry(relation.to.clone()).or_default() += relation.overlap_lines;
    }

    let bug_sources = session_bug_sources(conn)?;
    let mut by_origin: BTreeMap<TaskKey, DefectRow> = BTreeMap::new();
    for relation in relations
        .iter()
        .filter(|r| r.relation_kind == "touches_earlier_ai_lines")
    {
        if relation
            .rework_kind
            .as_deref()
            .is_some_and(|kind| NON_DEFECT_KINDS.contains(&kind))
        {
            continue;
        }
        let Some(origin) = origins_in_scope.get(&relation.from) else {
            continue;
        };
        let entry = by_origin
            .entry(relation.from.clone())
            .or_insert_with(|| DefectRow {
                origin: relation.from.clone(),
                title: origin.title.clone(),
                started_at: origin.started_at.clone(),
                level: 0,
                signals: Vec::new(),
                corrections: 0,
                overlap_lines: 0,
                correction_cost: 0.0,
                origin_cost: origin.cost_total,
            });
        entry.corrections += 1;
        entry.overlap_lines += relation.overlap_lines;
        let share = relation.overlap_lines as f64
            / to_total.get(&relation.to).copied().unwrap_or(1).max(1) as f64;
        entry.correction_cost += relation.to_cost.unwrap_or(0.0) * share;
        let later = titles
            .get(&relation.to)
            .map(|t| t.title.clone())
            .unwrap_or_default();
        let mut signal = |level: u8, text: String| {
            entry.level = entry.level.max(level);
            if !entry.signals.contains(&text) {
                entry.signals.push(text);
            }
        };
        signal(
            0,
            format!(
                "{} lines changed by \"{}\"",
                relation.overlap_lines,
                super::format::truncate(&later, 40)
            ),
        );
        match (
            relation.rework_kind.as_deref(),
            relation.rework_source.as_deref(),
        ) {
            (Some("defect_correction"), Some("override")) => {
                signal(4, "you marked it a defect correction".into())
            }
            (Some("defect_correction"), Some("jev"))
                if relation.rework_confidence.unwrap_or(0.0) >= 0.7 =>
            {
                signal(
                    3,
                    format!(
                        "classifier: defect correction ({:.2})",
                        relation.rework_confidence.unwrap_or(0.0)
                    ),
                )
            }
            (Some("defect_correction"), _) => signal(1, "later request reads like a fix".into()),
            _ => {}
        }
        if let Some(sources) = bug_sources.get(&(relation.from.0.clone(), relation.from.1.clone()))
        {
            if sources.iter().any(|s| s.contains("issue")) {
                signal(
                    2,
                    "a commit of this session has a later fix linked to a bug issue".into(),
                );
            } else {
                signal(
                    1,
                    "a commit of this session has a later fix-like commit".into(),
                );
            }
        }
    }
    let mut out: Vec<DefectRow> = by_origin.into_values().collect();
    out.sort_by(|a, b| {
        b.level
            .cmp(&a.level)
            .then(b.correction_cost.total_cmp(&a.correction_cost))
    });
    Ok(out)
}

fn session_bug_sources(conn: &Connection) -> Result<HashMap<(String, String), HashSet<String>>> {
    let mut stmt = conn.prepare(
        "SELECT sm.provider, COALESCE(NULLIF(s.root_session_id, ''), s.session_id), b.signal_source
         FROM fact_commit_session_match sm
         JOIN metadata_sessions s ON s.provider = sm.provider AND s.session_id = sm.session_id
         JOIN event_commit_bug_signal b ON b.repo_root = sm.repo_root AND b.commit_sha = sm.commit_sha
         WHERE sm.matched_lines > 0 AND b.bug_after_merge_flag = 1",
    )?;
    let mut out: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (provider, session, source) = row?;
        out.entry((provider, session)).or_default().insert(source);
    }
    Ok(out)
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct SpendSplit {
    pub total: f64,
    pub loops: f64,
    pub corrections: f64,
    pub requirement_changes: f64,
    pub normal: f64,
    pub turns: usize,
    pub classified_turns: usize,
}

/// Split spend by turn into disjoint categories with precedence
/// loop > correction > requirement change > normal, so they sum to the total.
pub fn spend_split(conn: &Connection, filters: &Filters) -> Result<SpendSplit> {
    let rows = tasks(conn, filters, TaskSort::Started, 1_000_000)?;
    let scope: HashSet<TaskKey> = rows.iter().map(key).collect();
    let sessions: HashSet<(String, String)> = rows
        .iter()
        .map(|t| (t.provider.clone(), t.session_id.clone()))
        .collect();

    let mut task_of_turn: HashMap<(String, String, i64), i64> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, task_index, COALESCE(turn_list, '') FROM derived_task",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })? {
        let (provider, session, task, list) = row?;
        for turn in list.split(',').filter_map(|v| v.trim().parse::<i64>().ok()) {
            task_of_turn.insert((provider.clone(), session.clone(), turn), task);
        }
    }
    let loop_turns: HashSet<(String, String, i64)> = conn
        .prepare("SELECT DISTINCT provider, session_id, turn_index FROM derived_attempt WHERE loop_flag = 1 AND turn_index IS NOT NULL")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let feedback: HashMap<String, String> = conn
        .prepare("SELECT unit_key, label FROM view_label_effective WHERE unit_kind = 'exchange' AND question_id = 'feedback'")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let defect_tasks: HashSet<TaskKey> = relations(conn, filters)?
        .into_iter()
        .filter(|r| r.rework_kind.as_deref() == Some("defect_correction"))
        .map(|r| r.to)
        .collect();

    let mut split = SpendSplit::default();
    let mut stmt = conn.prepare(
        "SELECT provider, root_session_id, root_turn_index, SUM(COALESCE(api_equiv_usd, 0))
         FROM derived_usage_event WHERE root_turn_index IS NOT NULL
         GROUP BY provider, root_session_id, root_turn_index",
    )?;
    for row in stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, f64>(3)?,
        ))
    })? {
        let (provider, session, turn, cost) = row?;
        if !sessions.contains(&(provider.clone(), session.clone())) {
            continue;
        }
        let task = task_of_turn
            .get(&(provider.clone(), session.clone(), turn))
            .copied()
            .unwrap_or(0);
        let task_key = (provider.clone(), session.clone(), task);
        if !scope.contains(&task_key) {
            continue;
        }
        split.turns += 1;
        split.total += cost;
        let label = feedback.get(&crate::classify::heuristic::turn_unit_key(
            &provider, &session, turn,
        ));
        if label.is_some() {
            split.classified_turns += 1;
        }
        if loop_turns.contains(&(provider.clone(), session.clone(), turn)) {
            split.loops += cost;
        } else if matches!(
            label.map(String::as_str),
            Some("correct_defect" | "correct_misunderstanding")
        ) || defect_tasks.contains(&task_key)
        {
            split.corrections += cost;
        } else if label.map(String::as_str) == Some("change_requirement") {
            split.requirement_changes += cost;
        } else {
            split.normal += cost;
        }
    }
    Ok(split)
}

fn key(task: &TaskRow) -> TaskKey {
    (
        task.provider.clone(),
        task.session_id.clone(),
        task.task_index,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::session_store::{
        MessageRecord, SessionRecord, UsageRecord, store_session_record,
    };

    fn seeded() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        for (id, day, asks) in [
            ("early", "01", vec!["Add the parser"]),
            (
                "late",
                "03",
                vec![
                    "Fix the parser crash",
                    "still failing, wrong output",
                    "actually make it weekly",
                ],
            ),
        ] {
            let mut record = SessionRecord::new("claude", id);
            record.started_at = Some(format!("2026-10-{day}T10:00:00Z"));
            record.ended_at = Some(format!("2026-10-{day}T12:00:00Z"));
            record.messages = asks
                .iter()
                .enumerate()
                .map(|(i, ask)| MessageRecord {
                    key: format!("u{i}"),
                    ts: Some(format!("2026-10-{day}T1{i}:00:00Z")),
                    role: "user".into(),
                    text: ask.to_string(),
                    is_compaction_summary: false,
                })
                .collect();
            record.usage = (0..asks.len())
                .map(|i| {
                    let mut usage = UsageRecord::new(format!("m{i}"));
                    usage.ts = Some(format!("2026-10-{day}T1{i}:01:00Z"));
                    usage.model = Some("claude-sonnet-4-5".into());
                    usage.input_tokens = 100_000;
                    usage.output_tokens = 1_000;
                    usage
                })
                .collect();
            store_session_record(&conn, &record)?;
        }
        crate::db::session_store::refresh_session_roots(&conn)?;
        for (session, ts, side) in [
            ("early", "2026-10-01T10:05:00Z", "+"),
            ("late", "2026-10-03T10:05:00Z", "-"),
        ] {
            conn.execute(
                "INSERT INTO fact_session_code_change(provider, session_id, change_ts, repo_root, rel_path, source_kind)
                 VALUES ('claude', ?1, ?2, '/repo', 'src/p.rs', 'tool_write')",
                params![session, ts],
            )?;
            let id = conn.last_insert_rowid();
            for hash in ["a", "b", "c", "d"] {
                conn.execute(
                    "INSERT INTO fact_session_code_change_line_hashes(code_change_id, side, line_hash, count)
                     VALUES (?1, ?2, ?3, 1)",
                    params![id, side, hash],
                )?;
            }
        }
        crate::insights::refresh(&conn)?;
        Ok(conn)
    }

    #[test]
    fn defect_evidence_levels_and_overrides() -> Result<()> {
        let conn = seeded()?;
        let filters = Filters::default();
        let defects = suspected_defects(&conn, &filters)?;
        assert_eq!(defects.len(), 1);
        assert_eq!(defects[0].origin.1, "early");
        assert_eq!(defects[0].level, 1, "{:?}", defects[0].signals);
        assert!(defects[0].correction_cost > 0.0);

        let key = relation_unit_key(
            &("claude".into(), "early".into(), 0),
            &("claude".into(), "late".into(), 0),
        );
        crate::classify::overrides::set_label(
            &conn,
            "relation",
            &key,
            "rework_kind",
            Some("defect_correction"),
            None,
        )?;
        assert_eq!(suspected_defects(&conn, &filters)?[0].level, 4);
        crate::classify::overrides::set_label(
            &conn,
            "relation",
            &key,
            "rework_kind",
            Some("requirement_change"),
            None,
        )?;
        assert!(
            suspected_defects(&conn, &filters)?.is_empty(),
            "requirement changes are not defects"
        );
        Ok(())
    }

    #[test]
    fn correction_spend_categories_are_disjoint() -> Result<()> {
        let conn = seeded()?;
        let split = spend_split(&conn, &Filters::default())?;
        assert!(split.total > 0.0);
        let sum = split.loops + split.corrections + split.requirement_changes + split.normal;
        assert!((sum - split.total).abs() < 1e-9, "{split:?}");
        // The whole later task corrects earlier AI lines (heuristic defect_correction).
        assert!(split.corrections > 0.0);
        Ok(())
    }
}
