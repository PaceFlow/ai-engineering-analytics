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
    /// HEAD's branch (`refs/heads/main`), when not detached.
    #[serde(default)]
    pub repo_head_ref: Option<String>,
    #[serde(default)]
    pub repo_remote: Option<String>,
    /// What `refs/remotes/origin/HEAD` points to.
    #[serde(default)]
    pub repo_origin_head: Option<String>,
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

/// The corpus' price map: pinned on the first save so costs replay the same
/// way for every snapshot, whatever LiteLLM publishes later.
pub fn pinned_prices(root: &Path) -> Result<Option<PathBuf>> {
    let pinned = root.join(PRICES);
    if !pinned.exists() {
        let cache = crate::cost::litellm::cache_path()?;
        if !cache.exists() {
            return Ok(None);
        }
        std::fs::create_dir_all(root)?;
        std::fs::copy(cache, &pinned)?;
    }
    Ok(Some(pinned))
}

#[derive(Debug, Clone, Serialize)]
pub struct SaveOutcome {
    pub dir: PathBuf,
    pub files: usize,
    pub bytes: u64,
    pub approved: usize,
    pub warnings: Vec<String>,
}

/// Save a reviewed session and settle its approved values against a replay of
/// all saved sessions of the same repository.
///
/// The reviewer judged the numbers in the live view; the stored values come
/// from the replay, so the corpus checks what it can actually reproduce. A
/// single-session number whose replay differs from what was reviewed is kept
/// at the replay value with a warning (that difference is worth a look). Other
/// saved sessions of the project get their cross-session numbers refreshed,
/// since the set of sessions they depend on changed.
pub fn save_reviewed(
    conn: &rusqlite::Connection,
    root: &Path,
    exe: &Path,
    provider: &str,
    session_id: &str,
    reviewed: &crate::explore::review::Expectations,
) -> Result<SaveOutcome> {
    use crate::explore::review::{is_cross_session, same};

    let prices = pinned_prices(root)?;
    let saved = snapshot::save(conn, root, provider, session_id, reviewed)?;
    let mut warnings = saved.warnings.clone();
    let key = format!("{provider}:{session_id}");
    let group = replay::groups(list(root)?)
        .into_iter()
        .find(|g| g.snapshots.iter().any(|s| s.manifest.key == key))
        .ok_or_else(|| anyhow::anyhow!("saved snapshot not found in {}", root.display()))?;
    let replay = replay::GroupReplay::build(&group, exe, prices.as_deref())?;

    let mut approved_count = 0;
    for snapshot in &group.snapshots {
        let stats = replay.stats(&snapshot.manifest.key)?;
        let path = snapshot.dir.join(EXPECTATIONS);
        if snapshot.manifest.key == key {
            let mut settled = reviewed.clone();
            settled.approved.clear();
            for (id, live) in &reviewed.approved {
                match stats.get(id) {
                    Some(replayed) => {
                        if !is_cross_session(id) && !same(live, replayed) {
                            warnings.push(format!("{id}: reviewed {live}, replay {replayed}"));
                        }
                        settled.approved.insert(id.clone(), replayed.clone());
                    }
                    None => warnings.push(format!("{id}: not produced in the replay; dropped")),
                }
            }
            // Everything nobody judged is recorded too, so a change still shows up.
            settled.locked = stats
                .iter()
                .filter(|(id, _)| {
                    !settled.approved.contains_key(*id) && !settled.rejected.contains_key(*id)
                })
                .map(|(id, value)| (id.clone(), value.clone()))
                .collect();
            approved_count = settled.approved.len();
            std::fs::write(&path, serde_json::to_string_pretty(&settled)? + "\n")?;
        } else {
            let mut other = replay::read_expectations(&snapshot.dir)?;
            let mut changed = false;
            for (id, value) in other.approved.iter_mut().chain(other.locked.iter_mut()) {
                let Some(replayed) = stats.get(id) else {
                    continue;
                };
                if same(value, replayed) {
                    continue;
                }
                if is_cross_session(id) {
                    warnings.push(format!(
                        "{}: {id} {value} → {replayed} (project changed)",
                        snapshot.manifest.key
                    ));
                    *value = replayed.clone();
                    changed = true;
                } else {
                    warnings.push(format!(
                        "{}: {id} no longer replays as approved ({value} → {replayed}); left as is",
                        snapshot.manifest.key
                    ));
                }
            }
            if changed {
                std::fs::write(&path, serde_json::to_string_pretty(&other)? + "\n")?;
            }
        }
    }
    Ok(SaveOutcome {
        dir: saved.dir,
        files: saved.files,
        bytes: saved.bytes,
        approved: approved_count,
        warnings,
    })
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
