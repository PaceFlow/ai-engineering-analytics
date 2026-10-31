//! Manual corrections. Overrides win over classifier and heuristic answers,
//! survive re-classification and double as gold labels for evaluation.
//!
//! Task overrides are stored under the goal's stable work identity
//! (`work:<identity>`) rather than its task position, so a correction stays
//! with its goal when re-segmentation renumbers tasks. `view_label_effective`
//! resolves the key to the task that currently holds the goal.

use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

use crate::classify::questions::labels_for;

const GOAL_KEY_PREFIX: &str = "work:";

/// Identity whose anchor is a stable message key, for a positional task key.
/// Legacy (index-based) anchors are no steadier than the task position.
fn stable_identity(conn: &Connection, task_key: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT m.identity_id FROM derived_work_member m
             JOIN work_identity i ON i.id = m.identity_id
             WHERE m.provider || '|' || m.session_id || '|' || m.task_index = ?1
               AND i.anchor NOT LIKE 'legacy:%' AND i.anchor NOT LIKE 'unresolved:%'",
            [task_key],
            |row| row.get(0),
        )
        .optional()?)
}

/// Current positional key of a goal-keyed task override, if the goal has a task.
fn current_task_key(conn: &Connection, goal_key: &str) -> Result<Option<String>> {
    let Some(identity) = goal_key.strip_prefix(GOAL_KEY_PREFIX) else {
        return Ok(Some(goal_key.to_owned()));
    };
    Ok(conn
        .query_row(
            "SELECT provider || '|' || session_id || '|' || task_index
             FROM derived_work_member WHERE identity_id = ?1",
            [identity],
            |row| row.get(0),
        )
        .optional()?)
}

/// Whether any task override is keyed by goal identity.
pub(crate) fn has_goal_keyed(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM classification_override
                       WHERE unit_kind = 'task' AND unit_key LIKE 'work:%')",
        [],
        |row| row.get(0),
    )?)
}

/// Install the current label view and re-key positional task overrides whose
/// task has a stable goal identity. Idempotent; runs on every open.
pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(crate::insights::schema::LABEL_EFFECTIVE_VIEW)?;
    let positional = conn
        .prepare(
            "SELECT unit_key, question_id FROM classification_override
             WHERE unit_kind = 'task' AND unit_key NOT LIKE 'work:%'",
        )?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (key, question) in positional {
        let Some(identity) = stable_identity(conn, &key)? else {
            continue;
        };
        // A goal-keyed answer already given for this question wins.
        conn.execute(
            "INSERT OR IGNORE INTO classification_override(unit_kind, unit_key, question_id, label, note, created_at)
             SELECT unit_kind, ?3, question_id, label, note, created_at FROM classification_override
             WHERE unit_kind = 'task' AND unit_key = ?1 AND question_id = ?2",
            params![key, question, format!("{GOAL_KEY_PREFIX}{identity}")],
        )?;
        conn.execute(
            "DELETE FROM classification_override
             WHERE unit_kind = 'task' AND unit_key = ?1 AND question_id = ?2",
            params![key, question],
        )?;
    }
    Ok(())
}

/// Set (or with `None` clear) a label override. Task keys may be positional
/// (`provider|session|task`) or goal keys (`work:<identity>`).
pub fn set_label(
    conn: &Connection,
    unit_kind: &str,
    unit_key: &str,
    question: &str,
    label: Option<&str>,
    note: Option<&str>,
) -> Result<()> {
    // Positional and goal key of the same task; gold labels stay positional.
    let (task_key, goal_key) = if unit_kind == "task" {
        let task_key = current_task_key(conn, unit_key)?.unwrap_or_else(|| unit_key.to_owned());
        let goal_key = match unit_key.strip_prefix(GOAL_KEY_PREFIX) {
            Some(_) => Some(unit_key.to_owned()),
            None => stable_identity(conn, unit_key)?.map(|id| format!("{GOAL_KEY_PREFIX}{id}")),
        };
        (task_key, goal_key)
    } else {
        (unit_key.to_owned(), None)
    };
    for key in std::iter::once(&task_key).chain(goal_key.as_ref()) {
        conn.execute(
            "DELETE FROM classification_override WHERE unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3",
            params![unit_kind, key, question],
        )?;
    }
    match label {
        Some(label) => {
            if let Some(labels) = labels_for(question)
                && !labels.contains(&label)
            {
                bail!(
                    "'{label}' is not a label of {question}: {}",
                    labels.join(", ")
                );
            }
            conn.execute(
                "INSERT OR REPLACE INTO classification_override(unit_kind, unit_key, question_id, label, note)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    unit_kind,
                    goal_key.as_deref().unwrap_or(&task_key),
                    question,
                    label,
                    note
                ],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO classification_gold(unit_kind, unit_key, question_id, label, origin)
                 VALUES (?1, ?2, ?3, ?4, 'override')",
                params![unit_kind, task_key, question, label],
            )?;
        }
        None => {
            conn.execute(
                "DELETE FROM classification_gold
                 WHERE unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3 AND origin = 'override'",
                params![unit_kind, task_key, question],
            )?;
        }
    }
    if matches!(unit_kind, "turn" | "task") {
        crate::insights::refresh_label_dependents(conn)?;
    }
    Ok(())
}

/// Start `task` at `turn` (later turns follow until the next assignment).
/// `None` removes the override for that turn. Rebuilds task structure.
pub fn assign_turn(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    turn: i64,
    task: Option<i64>,
) -> Result<()> {
    match task {
        Some(task) => conn.execute(
            "INSERT OR REPLACE INTO task_turn_assignment(provider, session_id, turn_index, task_index, source, confidence)
             VALUES (?1, ?2, ?3, ?4, 'override', 1.0)",
            params![provider, session_id, turn, task],
        )?,
        None => conn.execute(
            "DELETE FROM task_turn_assignment
             WHERE provider = ?1 AND session_id = ?2 AND turn_index = ?3 AND source = 'override'",
            params![provider, session_id, turn],
        )?,
    };
    crate::insights::refresh(conn)
}

/// Set a task's completion turn. `Some(None)` = not completed; `None`
/// removes the override. Rebuilds task structure.
pub fn set_completion(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    task: i64,
    completion: Option<Option<i64>>,
) -> Result<()> {
    match completion {
        Some(turn) => conn.execute(
            "INSERT OR REPLACE INTO task_completion_mark(provider, session_id, task_index, completion_turn, source, confidence)
             VALUES (?1, ?2, ?3, ?4, 'override', 1.0)",
            params![provider, session_id, task, turn],
        )?,
        None => conn.execute(
            "DELETE FROM task_completion_mark
             WHERE provider = ?1 AND session_id = ?2 AND task_index = ?3 AND source = 'override'",
            params![provider, session_id, task],
        )?,
    };
    crate::insights::refresh(conn)?;
    let member:Option<String>=conn.query_row("SELECT identity_id FROM derived_work_member WHERE provider=?1 AND session_id=?2 AND task_index=?3",params![provider,session_id,task],|r|r.get(0)).optional()?;
    if let Some(member) = member {
        let at=completion.and_then(|turn|conn.query_row("SELECT COALESCE(ended_at,started_at) FROM derived_turn WHERE provider=?1 AND session_id=?2 AND turn_index=?3",params![provider,session_id,turn],|r|r.get::<_,Option<String>>(0)).ok().flatten()).as_deref().and_then(crate::insights::structure::parse_ts);
        crate::insights::audit::record_completion(
            conn,
            &member,
            "manual",
            at,
            true,
            vec![],
            "Manual completion override cleared",
        )?;
        crate::insights::observation::publish(conn)?;
    }
    Ok(())
}

/// Remove every override of one session; returns how many were removed.
pub fn clear_session(conn: &Connection, provider: &str, session_id: &str) -> Result<usize> {
    let prefix = format!("{provider}|{session_id}|");
    let mut removed = conn.execute(
        "DELETE FROM task_turn_assignment WHERE provider = ?1 AND session_id = ?2 AND source = 'override'",
        params![provider, session_id],
    )?;
    removed += conn.execute(
        "DELETE FROM task_completion_mark WHERE provider = ?1 AND session_id = ?2 AND source = 'override'",
        params![provider, session_id],
    )?;
    removed += conn.execute(
        "DELETE FROM classification_override WHERE substr(unit_key, 1, length(?1)) = ?1",
        params![prefix],
    )?;
    removed += conn.execute(
        "DELETE FROM classification_override
         WHERE unit_kind = 'task' AND unit_key IN (
             SELECT 'work:' || id FROM work_identity WHERE provider = ?1 AND session_id = ?2)",
        params![provider, session_id],
    )?;
    let members = conn
        .prepare("SELECT identity_id FROM derived_work_member WHERE provider=?1 AND session_id=?2")?
        .query_map(params![provider, session_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for member in members {
        crate::insights::audit::record_completion(
            conn,
            &member,
            "manual",
            None,
            false,
            vec![],
            "Manual completion override cleared",
        )?;
    }
    crate::insights::refresh(conn)?;
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_win_and_clear() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        conn.execute(
            "INSERT INTO classification_result(unit_kind, unit_key, question_id, question_version, source, label, band, cache_key)
             VALUES ('task', 'claude|s|0', 'intent', 'v', 'jev', 'feature', 'confident', 'k')",
            [],
        )?;
        let effective = |conn: &Connection| -> Result<(String, String)> {
            Ok(conn.query_row(
                "SELECT label, source FROM view_label_effective WHERE unit_key = 'claude|s|0' AND question_id = 'intent'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        };
        set_label(&conn, "task", "claude|s|0", "intent", Some("bug_fix"), None)?;
        assert_eq!(effective(&conn)?, ("bug_fix".into(), "override".into()));
        assert!(
            set_label(
                &conn,
                "task",
                "claude|s|0",
                "intent",
                Some("nonsense"),
                None
            )
            .is_err()
        );
        set_label(&conn, "task", "claude|s|0", "intent", None, None)?;
        assert_eq!(effective(&conn)?, ("feature".into(), "jev".into()));
        let gold: i64 = conn.query_row("SELECT COUNT(*) FROM classification_gold", [], |row| {
            row.get(0)
        })?;
        assert_eq!(gold, 0);
        Ok(())
    }

    fn three_request_session(conn: &Connection) -> Result<()> {
        use crate::db::session_store::{MessageRecord, SessionRecord, store_session_record};
        let mut record = SessionRecord::new("codex", "s");
        record.started_at = Some("2026-10-01T10:00:00Z".into());
        record.ended_at = Some("2026-10-01T10:30:00Z".into());
        for (i, ask) in ["Add login", "Also add logout", "Fix the crash in sync"]
            .iter()
            .enumerate()
        {
            let message = |role: &str, minute: usize, text: &str| MessageRecord {
                key: format!("{role}-{i}"),
                ts: Some(format!("2026-10-01T10:{minute:02}:00Z")),
                role: role.into(),
                text: text.into(),
                is_compaction_summary: false,
            };
            record.messages.push(message("user", i * 10, ask));
            record
                .messages
                .push(message("assistant", i * 10 + 5, "Done"));
        }
        store_session_record(conn, &record)?;
        crate::db::session_store::refresh_session_roots(conn)?;
        Ok(())
    }

    fn task_of_first_turn(conn: &Connection, turn: i64) -> Result<i64> {
        Ok(conn.query_row(
            "SELECT task_index FROM derived_task
             WHERE session_id = 's' AND (',' || turn_list || ',') LIKE ('%,' || ?1 || ',%')",
            [turn],
            |row| row.get(0),
        )?)
    }

    fn intent_of(conn: &Connection, task: i64) -> Result<Option<(String, String)>> {
        Ok(conn
            .query_row(
                "SELECT label, source FROM view_label_effective
                 WHERE unit_kind = 'task' AND unit_key = ?1 AND question_id = 'intent'",
                [format!("codex|s|{task}")],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }

    #[test]
    fn override_follows_its_goal_after_resegmentation() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        three_request_session(&conn)?;
        // Turns 0 and 1 are one goal; turn 2 (the crash fix) is the second.
        assign_turn(&conn, "codex", "s", 0, Some(0))?;
        assign_turn(&conn, "codex", "s", 1, Some(0))?;
        assign_turn(&conn, "codex", "s", 2, Some(1))?;
        assert_eq!(task_of_first_turn(&conn, 2)?, 1);
        set_label(&conn, "task", "codex|s|1", "intent", Some("bug_fix"), None)?;

        // Splitting the first goal shifts the crash fix to task 2.
        assign_turn(&conn, "codex", "s", 1, Some(1))?;
        assign_turn(&conn, "codex", "s", 2, Some(2))?;
        assert_eq!(task_of_first_turn(&conn, 2)?, 2);
        assert_eq!(
            intent_of(&conn, 2)?,
            Some(("bug_fix".into(), "override".into()))
        );
        assert_ne!(
            intent_of(&conn, 1)?.map(|(_, source)| source),
            Some("override".into())
        );
        // A full rebuild keeps it on the same goal.
        crate::insights::refresh(&conn)?;
        assert_eq!(
            intent_of(&conn, 2)?,
            Some(("bug_fix".into(), "override".into()))
        );
        // Clearing by the current position removes the goal-keyed row.
        set_label(&conn, "task", "codex|s|2", "intent", None, None)?;
        assert_ne!(
            intent_of(&conn, 2)?.map(|(_, source)| source),
            Some("override".into())
        );
        Ok(())
    }

    #[test]
    fn migration_rekeys_positional_task_overrides_by_goal() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        three_request_session(&conn)?;
        assign_turn(&conn, "codex", "s", 2, Some(1))?;
        conn.execute(
            "INSERT INTO classification_override(unit_kind, unit_key, question_id, label)
             VALUES ('task', 'codex|s|1', 'intent', 'bug_fix'),
                    ('task', 'claude|gone|0', 'intent', 'docs')",
            [],
        )?;
        migrate(&conn)?;
        let keys: Vec<String> = conn
            .prepare("SELECT unit_key FROM classification_override ORDER BY unit_key")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        assert_eq!(keys[0], "claude|gone|0", "no goal identity: kept as is");
        assert!(keys[1].starts_with("work:"));
        assert_eq!(
            intent_of(&conn, 1)?,
            Some(("bug_fix".into(), "override".into()))
        );
        migrate(&conn)?;
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM classification_override", [], |row| {
                row.get::<_, i64>(0)
            })?,
            2
        );
        Ok(())
    }
}
