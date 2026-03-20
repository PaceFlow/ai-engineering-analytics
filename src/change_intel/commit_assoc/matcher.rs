use anyhow::{Result, anyhow};
use rusqlite::{Connection, params};
use std::collections::HashMap;

use crate::change_intel::types::LineSide;

use super::types::{CommitAttribution, GitCommitDiff, SessionAttributionRow};

const FALLBACK_STRICT_WEAK_RATIO: f64 = 0.20;
const FALLBACK_MIN_RATIO: f64 = 0.80;
const FALLBACK_MIN_MATCHED_LINES: i64 = 30;
const FALLBACK_WINNER_MARGIN: f64 = 0.20;

type SessionAvailabilityKey = (String, LineSide, String);
type AliasCandidateKey = (LineSide, String);

#[derive(Debug, Default)]
struct PathMatch {
    matched_added: i64,
    matched_removed: i64,
    provider_matched: HashMap<String, f64>,
    session_matched: HashMap<(String, String), f64>,
}

impl PathMatch {
    fn matched_total(&self) -> i64 {
        self.matched_added + self.matched_removed
    }
}

#[derive(Debug)]
struct CommitHashRow {
    side: LineSide,
    line_hash: String,
    count: i64,
}

#[derive(Debug)]
struct FileCommitRows {
    rel_path: String,
    rows: Vec<CommitHashRow>,
    total_lines: i64,
}

#[derive(Debug, Clone)]
struct SessionAvailability {
    provider: String,
    session_id: String,
    avail: i64,
}

#[derive(Debug, Default)]
pub struct RepoMatchPreload {
    session_availability: HashMap<SessionAvailabilityKey, Vec<SessionAvailability>>,
    alias_candidates: HashMap<AliasCandidateKey, Vec<(String, i64)>>,
}

impl RepoMatchPreload {
    fn session_availability_for(
        &self,
        rel_path: &str,
        side: LineSide,
        line_hash: &str,
    ) -> &[SessionAvailability] {
        self.session_availability
            .get(&(rel_path.to_string(), side, line_hash.to_string()))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    fn alias_candidates_for(&self, side: LineSide, line_hash: &str) -> &[(String, i64)] {
        self.alias_candidates
            .get(&(side, line_hash.to_string()))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

pub fn preload_repo_match_data(conn: &Connection, repo_root: &str) -> Result<RepoMatchPreload> {
    Ok(RepoMatchPreload {
        session_availability: load_session_availability_map(conn, repo_root)?,
        alias_candidates: load_alias_candidate_map(conn, repo_root)?,
    })
}

pub fn compute_commit_attribution(
    commit: &GitCommitDiff,
    preload: &RepoMatchPreload,
) -> Result<(CommitAttribution, Vec<SessionAttributionRow>)> {
    let total_added: i64 = commit.file_diffs.iter().map(|file| file.added_lines).sum();
    let total_removed: i64 = commit.file_diffs.iter().map(|file| file.removed_lines).sum();
    let commit_total = total_added + total_removed;
    let files = build_commit_files(commit);

    let mut matched_added = 0i64;
    let mut matched_removed = 0i64;
    let mut provider_matched: HashMap<String, f64> = HashMap::new();
    let mut session_matched: HashMap<(String, String), f64> = HashMap::new();

    for file in files {
        let strict_match = evaluate_file_match_for_path(preload, &file.rel_path, &file.rows);
        let strict_ratio = if file.total_lines > 0 {
            strict_match.matched_total() as f64 / file.total_lines as f64
        } else {
            0.0
        };

        let selected = if strict_ratio < FALLBACK_STRICT_WEAK_RATIO {
            if let Some(alias_path) =
                choose_fallback_alias_path(preload, &file.rel_path, &file.rows, file.total_lines)
            {
                let alias_match = evaluate_file_match_for_path(preload, &alias_path, &file.rows);
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
        for (session_key, lines) in selected.session_matched {
            *session_matched.entry(session_key).or_insert(0.0) += lines;
        }
    }

    let matched_total = matched_added + matched_removed;
    let ai_share = if commit_total > 0 {
        matched_total as f64 / commit_total as f64
    } else {
        0.0
    };
    let heavy_ai = ai_share >= 0.5;

    let mut session_rows: Vec<SessionAttributionRow> = session_matched
        .iter()
        .map(|((provider, session_id), matched_lines)| SessionAttributionRow {
            provider: provider.clone(),
            session_id: session_id.clone(),
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
    session_rows.sort_by(|a, b| {
        a.provider
            .cmp(&b.provider)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });

    Ok((
        CommitAttribution {
            matched_total_lines: matched_total,
            matched_added_lines: matched_added,
            matched_removed_lines: matched_removed,
            ai_share,
            heavy_ai,
        },
        session_rows,
    ))
}

fn build_commit_files(commit: &GitCommitDiff) -> Vec<FileCommitRows> {
    let mut files: Vec<FileCommitRows> = commit
        .file_diffs
        .iter()
        .map(|file| {
            let rows: Vec<CommitHashRow> = file
                .line_hashes
                .iter()
                .map(|line_hash| CommitHashRow {
                    side: line_hash.side,
                    line_hash: line_hash.line_hash.clone(),
                    count: line_hash.count,
                })
                .collect();
            let total_lines = rows.iter().map(|row| row.count).sum();
            FileCommitRows {
                rel_path: file.rel_path.clone(),
                rows,
                total_lines,
            }
        })
        .collect();
    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    files
}

fn evaluate_file_match_for_path(
    preload: &RepoMatchPreload,
    target_path: &str,
    rows: &[CommitHashRow],
) -> PathMatch {
    let mut out = PathMatch::default();

    for row in rows {
        let avail = preload.session_availability_for(target_path, row.side, &row.line_hash);
        if avail.is_empty() {
            continue;
        }

        let avail_total: i64 = avail.iter().map(|a| a.avail).sum();
        if avail_total <= 0 {
            continue;
        }

        let matched = row.count.min(avail_total);
        if matched <= 0 {
            continue;
        }

        match row.side {
            LineSide::Added => out.matched_added += matched,
            LineSide::Removed => out.matched_removed += matched,
        }

        let matched_f = matched as f64;
        let avail_total_f = avail_total as f64;
        for availability in avail {
            let contribution = matched_f * (availability.avail as f64 / avail_total_f);
            *out.provider_matched
                .entry(availability.provider.clone())
                .or_insert(0.0) += contribution;
            *out.session_matched
                .entry((availability.provider.clone(), availability.session_id.clone()))
                .or_insert(0.0) += contribution;
        }
    }

    out
}

fn choose_fallback_alias_path(
    preload: &RepoMatchPreload,
    strict_path: &str,
    rows: &[CommitHashRow],
    file_total_lines: i64,
) -> Option<String> {
    if file_total_lines <= 0 {
        return None;
    }

    let mut scores: HashMap<String, i64> = HashMap::new();
    for row in rows {
        for (path, avail) in preload.alias_candidates_for(row.side, &row.line_hash) {
            if path == strict_path {
                continue;
            }
            let matched = row.count.min(*avail);
            if matched > 0 {
                *scores.entry(path.clone()).or_insert(0) += matched;
            }
        }
    }

    if scores.is_empty() {
        return None;
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
        return Some(winner_path);
    }

    pick_confident_alias(&all_candidates, file_total_lines)
}

fn pick_confident_alias(candidates: &[(String, i64)], file_total_lines: i64) -> Option<String> {
    if candidates.is_empty() || file_total_lines <= 0 {
        return None;
    }

    let (winner_path, winner_matched) = &candidates[0];
    let runner_matched = candidates.get(1).map(|(_, matched)| *matched).unwrap_or(0);

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

fn load_session_availability_map(
    conn: &Connection,
    repo_root: &str,
) -> Result<HashMap<SessionAvailabilityKey, Vec<SessionAvailability>>> {
    let mut stmt = conn.prepare(
        "SELECT co.rel_path, hol.side, hol.line_hash, co.provider, co.session_id, MAX(hol.count) AS avail
         FROM fact_session_code_change co
         JOIN fact_session_code_change_line_hashes hol ON hol.code_change_id = co.id
         WHERE co.repo_root = ?1
           AND co.rel_path IS NOT NULL
         GROUP BY co.rel_path, hol.side, hol.line_hash, co.provider, co.session_id",
    )?;

    let rows = stmt.query_map(params![repo_root], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            SessionAvailability {
                provider: row.get::<_, String>(3)?,
                session_id: row.get::<_, String>(4)?,
                avail: row.get::<_, i64>(5)?,
            },
        ))
    })?;

    let mut out: HashMap<SessionAvailabilityKey, Vec<SessionAvailability>> = HashMap::new();
    for row in rows {
        let (rel_path, side_raw, line_hash, availability) = row?;
        let side = parse_line_side(&side_raw)?;
        out.entry((rel_path, side, line_hash))
            .or_default()
            .push(availability);
    }
    Ok(out)
}

fn load_alias_candidate_map(
    conn: &Connection,
    repo_root: &str,
) -> Result<HashMap<AliasCandidateKey, Vec<(String, i64)>>> {
    let mut stmt = conn.prepare(
        "SELECT co.rel_path, hol.side, hol.line_hash, MAX(hol.count) AS avail
         FROM fact_session_code_change co
         JOIN fact_session_code_change_line_hashes hol ON hol.code_change_id = co.id
         WHERE co.repo_root = ?1
           AND co.rel_path IS NOT NULL
         GROUP BY co.rel_path, hol.side, hol.line_hash",
    )?;

    let rows = stmt.query_map(params![repo_root], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;

    let mut out: HashMap<AliasCandidateKey, Vec<(String, i64)>> = HashMap::new();
    for row in rows {
        let (rel_path, side_raw, line_hash, avail) = row?;
        let side = parse_line_side(&side_raw)?;
        out.entry((side, line_hash)).or_default().push((rel_path, avail));
    }
    for values in out.values_mut() {
        values.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    }
    Ok(out)
}

fn parse_line_side(raw: &str) -> Result<LineSide> {
    match raw {
        "+" => Ok(LineSide::Added),
        "-" => Ok(LineSide::Removed),
        other => Err(anyhow!("unsupported line side '{other}'")),
    }
}
