use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;

use super::types::{CommitAttribution, CommitHashRow, ProviderAttributionRow};

pub const ALGO_VERSION: &str = "commit_assoc_v1";
const FALLBACK_STRICT_WEAK_RATIO: f64 = 0.20;
const FALLBACK_MIN_RATIO: f64 = 0.80;
const FALLBACK_MIN_MATCHED_LINES: i64 = 30;
const FALLBACK_WINNER_MARGIN: f64 = 0.20;

#[derive(Debug, Default)]
struct PathMatch {
    matched_added: i64,
    matched_removed: i64,
    provider_matched: HashMap<String, f64>,
}

impl PathMatch {
    fn matched_total(&self) -> i64 {
        self.matched_added + self.matched_removed
    }
}

#[derive(Debug)]
struct FileCommitRows {
    rel_path: String,
    rows: Vec<CommitHashRow>,
    total_lines: i64,
}

pub fn compute_commit_attribution(
    conn: &Connection,
    repo_root: &str,
    commit_id: i64,
) -> Result<(CommitAttribution, Vec<ProviderAttributionRow>)> {
    let (total_added, total_removed) = load_commit_totals(conn, commit_id)?;
    let commit_total = total_added + total_removed;

    let rows = load_commit_hash_rows(conn, commit_id)?;
    let files = group_commit_rows_by_path(rows);
    let mut matched_added = 0i64;
    let mut matched_removed = 0i64;
    let mut provider_matched: HashMap<String, f64> = HashMap::new();

    let mut availability_stmt = conn.prepare(
        "SELECT co.provider, MAX(hol.count) AS avail
         FROM change_ops co
         JOIN change_op_line_hashes hol ON hol.op_id = co.id
         WHERE co.repo_root = ?1
           AND co.rel_path = ?2
           AND hol.side = ?3
           AND hol.line_hash = ?4
         GROUP BY co.provider",
    )?;
    let mut fallback_candidates_stmt = conn.prepare(
        "SELECT co.rel_path, MAX(hol.count) AS avail
         FROM change_ops co
         JOIN change_op_line_hashes hol ON hol.op_id = co.id
         WHERE co.repo_root = ?1
           AND co.rel_path IS NOT NULL
           AND hol.side = ?2
           AND hol.line_hash = ?3
         GROUP BY co.rel_path",
    )?;

    for file in files {
        let strict_match = evaluate_file_match_for_path(
            &mut availability_stmt,
            repo_root,
            &file.rel_path,
            &file.rows,
        )?;

        let strict_ratio = if file.total_lines > 0 {
            strict_match.matched_total() as f64 / file.total_lines as f64
        } else {
            0.0
        };

        let selected = if strict_ratio < FALLBACK_STRICT_WEAK_RATIO {
            if let Some(alias_path) = choose_fallback_alias_path(
                &mut fallback_candidates_stmt,
                repo_root,
                &file.rel_path,
                &file.rows,
                file.total_lines,
            )? {
                let alias_match = evaluate_file_match_for_path(
                    &mut availability_stmt,
                    repo_root,
                    &alias_path,
                    &file.rows,
                )?;

                if alias_match.matched_total() > strict_match.matched_total() {
                    alias_match
                } else {
                    strict_match
                }
            } else {
                strict_match
            }
        } else {
            strict_match
        };

        matched_added += selected.matched_added;
        matched_removed += selected.matched_removed;
        for (provider, lines) in selected.provider_matched {
            *provider_matched.entry(provider).or_insert(0.0) += lines;
        }
    }

    let matched_total = matched_added + matched_removed;
    let ai_share = if commit_total > 0 {
        matched_total as f64 / commit_total as f64
    } else {
        0.0
    };
    let heavy_ai = ai_share >= 0.5;

    let mut provider_rows: Vec<ProviderAttributionRow> = provider_matched
        .iter()
        .map(|(provider, matched_lines)| ProviderAttributionRow {
            provider: provider.clone(),
            matched_lines: *matched_lines,
            share_of_commit: if commit_total > 0 {
                *matched_lines / commit_total as f64
            } else {
                0.0
            },
            share_of_ai: if matched_total > 0 {
                *matched_lines / matched_total as f64
            } else {
                0.0
            },
        })
        .collect();
    provider_rows.sort_by(|a, b| a.provider.cmp(&b.provider));

    Ok((
        CommitAttribution {
            commit_total_lines: commit_total,
            matched_total_lines: matched_total,
            matched_added_lines: matched_added,
            matched_removed_lines: matched_removed,
            ai_share,
            heavy_ai,
        },
        provider_rows,
    ))
}

fn load_commit_totals(conn: &Connection, commit_id: i64) -> Result<(i64, i64)> {
    let row = conn.query_row(
        "SELECT total_added, total_removed FROM git_commits WHERE id = ?1",
        params![commit_id],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
    )?;
    Ok(row)
}

fn load_commit_hash_rows(conn: &Connection, commit_id: i64) -> Result<Vec<CommitHashRow>> {
    let mut stmt = conn.prepare(
        "SELECT f.rel_path, h.side, h.line_hash, h.count
         FROM git_commit_file_diffs f
         JOIN git_commit_line_hashes h ON h.file_diff_id = f.id
         WHERE f.commit_id = ?1",
    )?;

    let rows = stmt.query_map(params![commit_id], |r| {
        Ok(CommitHashRow {
            rel_path: r.get(0)?,
            side: r.get(1)?,
            line_hash: r.get(2)?,
            count: r.get(3)?,
        })
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn group_commit_rows_by_path(rows: Vec<CommitHashRow>) -> Vec<FileCommitRows> {
    let mut grouped: HashMap<String, Vec<CommitHashRow>> = HashMap::new();
    for row in rows {
        grouped.entry(row.rel_path.clone()).or_default().push(row);
    }

    let mut files: Vec<FileCommitRows> = grouped
        .into_iter()
        .map(|(rel_path, rows)| {
            let total_lines = rows.iter().map(|r| r.count).sum();
            FileCommitRows {
                rel_path,
                rows,
                total_lines,
            }
        })
        .collect();
    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    files
}

fn evaluate_file_match_for_path(
    availability_stmt: &mut rusqlite::Statement<'_>,
    repo_root: &str,
    target_path: &str,
    rows: &[CommitHashRow],
) -> Result<PathMatch> {
    let mut out = PathMatch::default();

    for row in rows {
        let avail = load_provider_availability(
            availability_stmt,
            repo_root,
            target_path,
            &row.side,
            &row.line_hash,
        )?;
        if avail.is_empty() {
            continue;
        }

        let avail_total: i64 = avail.values().sum();
        if avail_total <= 0 {
            continue;
        }

        let matched = row.count.min(avail_total);
        if matched <= 0 {
            continue;
        }

        if row.side == "+" {
            out.matched_added += matched;
        } else if row.side == "-" {
            out.matched_removed += matched;
        }

        let matched_f = matched as f64;
        let avail_total_f = avail_total as f64;
        for (provider, provider_avail) in avail {
            let contribution = matched_f * (provider_avail as f64 / avail_total_f);
            *out.provider_matched.entry(provider).or_insert(0.0) += contribution;
        }
    }

    Ok(out)
}

fn choose_fallback_alias_path(
    fallback_stmt: &mut rusqlite::Statement<'_>,
    repo_root: &str,
    strict_path: &str,
    rows: &[CommitHashRow],
    file_total_lines: i64,
) -> Result<Option<String>> {
    if file_total_lines <= 0 {
        return Ok(None);
    }

    let mut scores: HashMap<String, i64> = HashMap::new();
    for row in rows {
        let path_avail = load_path_availability_candidates(
            fallback_stmt,
            repo_root,
            &row.side,
            &row.line_hash,
        )?;

        for (path, avail) in path_avail {
            if path == strict_path {
                continue;
            }
            let matched = row.count.min(avail);
            if matched > 0 {
                *scores.entry(path).or_insert(0) += matched;
            }
        }
    }

    if scores.is_empty() {
        return Ok(None);
    }

    let strict_base = basename(strict_path).to_string();
    let mut all_candidates: Vec<(String, i64)> = scores.into_iter().collect();
    all_candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut filename_candidates: Vec<(String, i64)> = all_candidates
        .iter()
        .filter(|(path, _)| basename(path) == strict_base)
        .cloned()
        .collect();
    filename_candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    if let Some(winner_path) = pick_confident_alias(&filename_candidates, file_total_lines) {
        return Ok(Some(winner_path));
    }

    if let Some(winner_path) = pick_confident_alias(&all_candidates, file_total_lines) {
        return Ok(Some(winner_path));
    }

    Ok(None)
}

fn pick_confident_alias(candidates: &[(String, i64)], file_total_lines: i64) -> Option<String> {
    if candidates.is_empty() || file_total_lines <= 0 {
        return None;
    }

    let (winner_path, winner_matched) = &candidates[0];
    let runner_matched = candidates.get(1).map(|(_, m)| *m).unwrap_or(0);

    let winner_ratio = *winner_matched as f64 / file_total_lines as f64;
    let runner_ratio = runner_matched as f64 / file_total_lines as f64;
    let accepted = winner_ratio >= FALLBACK_MIN_RATIO
        && *winner_matched >= FALLBACK_MIN_MATCHED_LINES
        && (winner_ratio - runner_ratio) >= FALLBACK_WINNER_MARGIN;
    if accepted {
        Some(winner_path.clone())
    } else {
        None
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn load_provider_availability(
    stmt: &mut rusqlite::Statement<'_>,
    repo_root: &str,
    rel_path: &str,
    side: &str,
    line_hash: &str,
) -> Result<HashMap<String, i64>> {
    let rows = stmt.query_map(
        params![repo_root, rel_path, side, line_hash],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
    )?;

    let mut out = HashMap::new();
    for row in rows {
        let (provider, avail) = row?;
        out.insert(provider, avail);
    }
    Ok(out)
}

fn load_path_availability_candidates(
    stmt: &mut rusqlite::Statement<'_>,
    repo_root: &str,
    side: &str,
    line_hash: &str,
) -> Result<Vec<(String, i64)>> {
    let rows = stmt.query_map(params![repo_root, side, line_hash], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}
