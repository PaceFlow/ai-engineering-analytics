//! Conversation, usage and practice insights built on top of ingested facts.
pub mod billing;
pub mod context;
pub mod quota;
pub mod relations;
pub mod schema;
pub mod structure;
pub mod tools;

use anyhow::Result;
use rusqlite::Connection;

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(schema::INSIGHTS_SCHEMA)?;
    conn.execute_batch(context::CONTEXT_SCHEMA)?;
    conn.execute_batch(quota::QUOTA_SCHEMA)?;
    conn.execute_batch(relations::RELATIONS_SCHEMA)?;
    Ok(())
}

/// Rebuild every derived insight table from facts.
pub fn refresh(conn: &Connection) -> Result<()> {
    structure::refresh_session_structure(conn)?;
    relations::refresh_relations(conn)?;
    context::refresh_context_ledger(conn)?;
    quota::refresh_quota_windows(conn)?;
    crate::classify::heuristic::refresh_heuristic_labels(conn)?;
    Ok(())
}
