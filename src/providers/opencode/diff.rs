use anyhow::{Result, bail, ensure};
use serde::Deserialize;
use similar::TextDiff;

/// OpenCode has stored both unified patches and before/after file snapshots.
/// Keep their interpretation identical in session metrics and commit attribution.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SessionDiffEntry {
    pub file: String,
    patch: Option<String>,
    before: Option<String>,
    after: Option<String>,
}

impl SessionDiffEntry {
    pub fn unified_patch(&self) -> Result<String> {
        if let (Some(before), Some(after)) = (&self.before, &self.after) {
            return Ok(TextDiff::from_lines(before, after)
                .unified_diff()
                .to_string());
        }
        if let Some(patch) = &self.patch {
            ensure!(
                patch.trim().is_empty() || patch.lines().any(|line| line.starts_with("@@")),
                "OpenCode diff for {} has no unified diff hunks",
                self.file
            );
            return Ok(patch.clone());
        }
        bail!(
            "OpenCode diff for {} requires patch or both before and after",
            self.file
        )
    }
}
