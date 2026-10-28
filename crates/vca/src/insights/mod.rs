//! Conversation, usage and practice insights built on top of ingested facts.
pub mod billing;
pub mod context;
pub mod daily;
pub mod delivery;
pub mod habits;
pub mod observation;
pub mod quota;
pub mod relations;
pub mod schema;
pub mod setup;
pub mod stages;
pub mod structure;
pub mod tools;
pub mod trends;
pub mod work;

use anyhow::Result;
use rusqlite::Connection;

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(schema::INSIGHTS_SCHEMA)?;
    conn.execute_batch(context::CONTEXT_SCHEMA)?;
    conn.execute_batch(quota::QUOTA_SCHEMA)?;
    conn.execute_batch(relations::RELATIONS_SCHEMA)?;
    conn.execute_batch(stages::STAGES_SCHEMA)?;
    conn.execute_batch(habits::HABITS_SCHEMA)?;
    conn.execute_batch(work::SCHEMA)?;
    conn.execute_batch(setup::SCHEMA)?;
    conn.execute_batch(observation::SCHEMA)?;
    conn.execute_batch(delivery::SCHEMA)?;
    Ok(())
}

/// Rebuild every derived insight table from facts.
pub fn refresh(conn: &Connection) -> Result<()> {
    conn.execute_batch("SAVEPOINT refresh_insights")?;
    let result = refresh_inner(conn);
    match result {
        Ok(()) => {
            conn.execute_batch("RELEASE refresh_insights")?;
            Ok(())
        }
        Err(error) => {
            conn.execute_batch("ROLLBACK TO refresh_insights; RELEASE refresh_insights")?;
            Err(error)
        }
    }
}

fn refresh_inner(conn: &Connection) -> Result<()> {
    work::save_decisions(conn)?;
    structure::refresh_session_structure(conn)?;
    let saved: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM work_saved_decision WHERE review_reason IS NULL)",
        [],
        |r| r.get(0),
    )?;
    if saved {
        work::refresh(conn)?;
        if work::reconcile_decisions(conn)? {
            structure::refresh_session_structure(conn)?;
        }
    }
    relations::refresh_relations(conn)?;
    context::refresh_context_ledger(conn)?;
    quota::refresh_quota_windows(conn)?;
    crate::classify::heuristic::refresh_heuristic_labels(conn)?;
    stages::refresh_stages(conn)?;
    habits::refresh_habits(conn)?;
    work::refresh(conn)?;
    observation::publish(conn)?;
    delivery::refresh_links(conn)?;
    Ok(())
}

/// Rebuild what depends on labels only (stage rollups and habits), after a
/// label override.
pub fn refresh_label_dependents(conn: &Connection) -> Result<()> {
    conn.execute_batch("SAVEPOINT refresh_labels")?;
    let result = refresh_label_dependents_inner(conn);
    match result {
        Ok(()) => {
            conn.execute_batch("RELEASE refresh_labels")?;
            Ok(())
        }
        Err(error) => {
            conn.execute_batch("ROLLBACK TO refresh_labels; RELEASE refresh_labels")?;
            Err(error)
        }
    }
}
fn refresh_label_dependents_inner(conn: &Connection) -> Result<()> {
    stages::refresh_stages(conn)?;
    habits::refresh_habits(conn)?;
    work::refresh(conn)?;
    observation::publish(conn)?;
    Ok(())
}

#[cfg(test)]
mod observation_tests;

/// Segmentation changes only invalidate task structure before dependent passes run.
pub fn refresh_structure(conn: &Connection) -> Result<()> {
    conn.execute_batch("SAVEPOINT classify_structure")?;
    let result = (|| {
        work::save_decisions(conn)?;
        structure::refresh_session_structure(conn)?;
        work::refresh(conn)?;
        if work::reconcile_decisions(conn)? {
            structure::refresh_session_structure(conn)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            conn.execute_batch("RELEASE classify_structure")?;
            Ok(())
        }
        Err(e) => {
            conn.execute_batch("ROLLBACK TO classify_structure; RELEASE classify_structure")?;
            Err(e)
        }
    }
}
