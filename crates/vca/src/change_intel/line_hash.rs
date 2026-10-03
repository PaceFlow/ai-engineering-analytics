use std::collections::HashMap;

use similar::{ChangeTag, TextDiff};

use crate::change_intel::types::{LineHashCount, LineSide};

#[derive(Debug, Clone)]
pub struct DiffSummary {
    pub added_lines: i64,
    pub removed_lines: i64,
    pub line_hashes: Vec<LineHashCount>,
}

/// Normalizes a source line before hashing so cosmetic reformatting does not break matching.
///
/// We trim both leading and trailing whitespace: format-on-save, linters, and pre-commit hooks
/// frequently re-indent lines between the moment a change is accepted and the moment it is
/// committed, which would otherwise change the line hash and silently drop the match.
///
/// NOTE: changing this normalization invalidates previously stored session line hashes in
/// `fact_session_code_change_line_hashes`. A full re-ingest/rebuild is required for the change to
/// take effect consistently (commit diff hashes are recomputed on every scan).
pub fn normalize_line(line: &str) -> String {
    line.trim().to_string()
}

pub fn hash_line(line: &str) -> String {
    let normalized = normalize_line(line);
    format!("{:x}", md5::compute(normalized.as_bytes()))
}

pub fn line_count(text: &str) -> i64 {
    text.lines().count() as i64
}

pub fn hashes_for_text(text: &str, side: LineSide) -> Vec<LineHashCount> {
    let mut counts: HashMap<String, i64> = HashMap::new();
    for line in text.lines() {
        let hash = hash_line(line);
        *counts.entry(hash).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .map(|(line_hash, count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect()
}

pub fn diff_with_hashes(before: &str, after: &str) -> DiffSummary {
    let mut added = 0i64;
    let mut removed = 0i64;
    let mut counts: HashMap<(LineSide, String), i64> = HashMap::new();

    let diff = TextDiff::from_lines(before, after);
    for change in diff.iter_all_changes() {
        let line = change.to_string();
        let line = line.trim_end_matches('\n').trim_end_matches('\r');
        let hash = hash_line(line);

        match change.tag() {
            ChangeTag::Insert => {
                added += 1;
                *counts.entry((LineSide::Added, hash)).or_insert(0) += 1;
            }
            ChangeTag::Delete => {
                removed += 1;
                *counts.entry((LineSide::Removed, hash)).or_insert(0) += 1;
            }
            ChangeTag::Equal => {}
        }
    }

    let line_hashes = counts
        .into_iter()
        .map(|((side, line_hash), count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect();

    DiffSummary {
        added_lines: added,
        removed_lines: removed,
        line_hashes,
    }
}
