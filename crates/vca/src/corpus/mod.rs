//! Reference corpus of real sessions for end-to-end tests.
//!
//! A reviewer samples sessions, checks their numbers in the review page and
//! saves the approved ones. Each snapshot holds the session's original source
//! files (or, for tools that keep every session in one database, only that
//! session's rows), a git bundle of its repository, the price map in use and the
//! approved numbers. `replay` rebuilds the numbers from the snapshot alone in a
//! temporary home and compares them with what was approved.
//!
//! Snapshots live in `~/.vca/corpus/<provider>-<id>/` and never leave the
//! machine unless packed and uploaded deliberately.

pub mod replay;
pub mod server;
pub mod snapshot;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const MANIFEST: &str = "manifest.json";
pub const EXPECTATIONS: &str = "expectations.json";
pub const BUNDLE: &str = "repo.bundle";
pub const PRICES: &str = "litellm_prices.json";

pub fn corpus_dir() -> Result<PathBuf> {
    Ok(crate::data_home::directory()?.join("corpus"))
}

/// Folder name for one session's snapshot.
pub fn snapshot_name(provider: &str, session_id: &str) -> String {
    let id: String = session_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(36)
        .collect();
    format!("{provider}-{id}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Manifest {
    pub format: u32,
    /// `provider:session_id` of the root session.
    pub key: String,
    pub provider: String,
    pub session_id: String,
    /// Root plus subagent/fork sessions included.
    pub sessions: Vec<String>,
    pub title: Option<String>,
    pub created_at: String,
    pub vca_version: String,
    pub pricing: String,
    /// The repository the session worked in, as recorded on the source machine.
    pub repo_root: Option<String>,
    pub repo_head: Option<String>,
    /// Copied files, relative to the snapshot folder.
    pub files: Vec<ManifestFile>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ManifestFile {
    pub path: String,
    /// Where it came from on the source machine.
    pub source: String,
    pub bytes: u64,
    pub md5: String,
}

/// One saved snapshot, as listed by the review app and `vca corpus list`.
#[derive(Debug, Clone, Serialize)]
pub struct SavedSnapshot {
    pub dir: PathBuf,
    pub manifest: Manifest,
    pub approved: usize,
    pub rejected: usize,
}

pub fn read_manifest(dir: &Path) -> Result<Manifest> {
    Ok(serde_json::from_str(&std::fs::read_to_string(
        dir.join(MANIFEST),
    )?)?)
}

pub fn list(root: &Path) -> Result<Vec<SavedSnapshot>> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(out);
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        let Ok(manifest) = read_manifest(&dir) else {
            continue;
        };
        let expectations: crate::explore::review::Expectations =
            std::fs::read_to_string(dir.join(EXPECTATIONS))
                .ok()
                .and_then(|text| serde_json::from_str(&text).ok())
                .unwrap_or_default();
        out.push(SavedSnapshot {
            dir,
            manifest,
            approved: expectations.approved.len(),
            rejected: expectations.rejected.len(),
        });
    }
    out.sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    Ok(out)
}

/// Stable pseudo-random order for sampling: the same seed gives the same picks.
pub fn sample_rank(seed: u64, key: &str) -> String {
    format!("{:x}", md5::compute(format!("{seed}:{key}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_names_are_filesystem_safe() {
        assert_eq!(
            snapshot_name("claude", "7956ac77-ed82-43aa"),
            "claude-7956ac77-ed82-43aa"
        );
        assert_eq!(snapshot_name("claude", "a/agent-b:c"), "claude-aagent-bc");
    }

    #[test]
    fn sample_rank_is_stable_and_seeded() {
        assert_eq!(sample_rank(1, "claude:x"), sample_rank(1, "claude:x"));
        assert_ne!(sample_rank(1, "claude:x"), sample_rank(2, "claude:x"));
    }
}
