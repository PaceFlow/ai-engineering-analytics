pub mod codex;
pub mod cursor;
pub mod utils;

use anyhow::Result;
use rusqlite::Connection;

pub trait Provider {
    fn name(&self) -> &str;
    /// Ingest all sessions from this provider into `db`. Returns number of fact rows written.
    fn ingest(&self, db: &Connection, verbose: bool) -> Result<usize>;
}

/// Central registry — add one line here when a new provider is implemented.
pub fn all_providers() -> Vec<Box<dyn Provider>> {
    vec![
        Box::new(codex::CodexProvider),
        Box::new(cursor::CursorProvider),
    ]
}
