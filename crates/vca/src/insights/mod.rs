//! Conversation, usage and practice insights built on top of ingested facts.
pub mod billing;
pub mod context;
pub mod habits;
pub mod quota;
pub mod relations;
pub mod schema;
pub mod stages;
pub mod structure;
pub mod tools;

use anyhow::Result;
use rusqlite::Connection;

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(schema::INSIGHTS_SCHEMA)?;
    conn.execute_batch(context::CONTEXT_SCHEMA)?;
    conn.execute_batch(quota::QUOTA_SCHEMA)?;
    conn.execute_batch(relations::RELATIONS_SCHEMA)?;
    conn.execute_batch(stages::STAGES_SCHEMA)?;
    conn.execute_batch(habits::HABITS_SCHEMA)?;
    Ok(())
}

/// Rebuild every derived insight table from facts.
pub fn refresh(conn: &Connection) -> Result<()> {
    structure::refresh_session_structure(conn)?;
    relations::refresh_relations(conn)?;
    context::refresh_context_ledger(conn)?;
    quota::refresh_quota_windows(conn)?;
    crate::classify::heuristic::refresh_heuristic_labels(conn)?;
    stages::refresh_stages(conn)?;
    habits::refresh_habits(conn)?;
    Ok(())
}

/// Rebuild what depends on labels only (stage rollups and habits), after a
/// label override.
pub fn refresh_label_dependents(conn: &Connection) -> Result<()> {
    stages::refresh_stages(conn)?;
    habits::refresh_habits(conn)?;
    Ok(())
}
