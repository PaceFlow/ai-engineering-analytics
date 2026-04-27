pub mod claude;
pub mod codex;
pub mod cursor;
pub mod utils;

use anyhow::Result;
use rusqlite::Connection;
use std::path::PathBuf;

use crate::ingest_progress::IngestProgressObserver;

#[derive(Debug, Clone)]
pub enum ProviderSessionPlan {
    Claude { session_files: Vec<PathBuf> },
    Codex { session_files: Vec<PathBuf> },
    Cursor { composer_keys: Vec<String> },
}

impl ProviderSessionPlan {
    pub fn item_count(&self) -> usize {
        match self {
            Self::Claude { session_files } => session_files.len(),
            Self::Codex { session_files } => session_files.len(),
            Self::Cursor { composer_keys } => composer_keys.len(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Provider {
    Claude,
    Codex,
    Cursor,
}

impl Provider {
    pub fn name(&self) -> &str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Cursor => "cursor",
        }
    }

    pub fn plan_session_work(&self) -> Result<ProviderSessionPlan> {
        match self {
            Self::Claude => Ok(ProviderSessionPlan::Claude {
                session_files: claude::plan_session_files()?,
            }),
            Self::Codex => Ok(ProviderSessionPlan::Codex {
                session_files: codex::plan_session_files()?,
            }),
            Self::Cursor => Ok(ProviderSessionPlan::Cursor {
                composer_keys: cursor::plan_composer_rows()?,
            }),
        }
    }

    /// Ingest all sessions from this provider into `db`. Returns number of fact rows written.
    pub fn ingest(
        &self,
        db: &Connection,
        plan: &ProviderSessionPlan,
        verbose: bool,
        progress: Option<&mut dyn IngestProgressObserver>,
    ) -> Result<usize> {
        match (self, plan) {
            (Self::Claude, ProviderSessionPlan::Claude { session_files }) => {
                claude::ingest_planned_sessions(db, session_files, verbose, progress)
            }
            (Self::Codex, ProviderSessionPlan::Codex { session_files }) => {
                codex::ingest_planned_sessions(db, session_files, verbose, progress)
            }
            (Self::Cursor, ProviderSessionPlan::Cursor { composer_keys }) => {
                cursor::ingest_planned_sessions(db, composer_keys, verbose, progress)
            }
            _ => anyhow::bail!("provider plan mismatch for {}", self.name()),
        }
    }
}

/// Central registry — add one line here when a new provider is implemented.
pub fn all_providers() -> Vec<Provider> {
    vec![Provider::Claude, Provider::Codex, Provider::Cursor]
}
