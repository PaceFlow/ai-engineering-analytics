use similar::{ChangeTag, TextDiff};

/// Diff two text blobs line-by-line and return (lines_added, lines_removed).
pub fn diff_line_counts(before: &str, after: &str) -> (i64, i64) {
    let diff = TextDiff::from_lines(before, after);
    let mut added = 0i64;
    let mut removed = 0i64;
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Insert => added += 1,
            ChangeTag::Delete => removed += 1,
            ChangeTag::Equal => {}
        }
    }
    (added, removed)
}
