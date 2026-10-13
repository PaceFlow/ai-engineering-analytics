//! Manual corrections. Overrides win over classifier and heuristic answers,
//! survive re-classification and double as gold labels for evaluation.

use anyhow::{Result, bail};
use rusqlite::{Connection, params};

use crate::classify::questions::labels_for;

/// Set (or with `None` clear) a label override.
pub fn set_label(
    conn: &Connection,
    unit_kind: &str,
    unit_key: &str,
    question: &str,
    label: Option<&str>,
    note: Option<&str>,
) -> Result<()> {
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
                params![unit_kind, unit_key, question, label, note],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO classification_gold(unit_kind, unit_key, question_id, label, origin)
                 VALUES (?1, ?2, ?3, ?4, 'override')",
                params![unit_kind, unit_key, question, label],
            )?;
        }
        None => {
            conn.execute(
                "DELETE FROM classification_override WHERE unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3",
                params![unit_kind, unit_key, question],
            )?;
            conn.execute(
                "DELETE FROM classification_gold
                 WHERE unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3 AND origin = 'override'",
                params![unit_kind, unit_key, question],
            )?;
        }
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
    crate::insights::refresh(conn)
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
}
