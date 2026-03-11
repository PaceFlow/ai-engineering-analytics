use anyhow::Result;
use std::collections::{HashMap, HashSet};

use super::git_scan::{
    branches_containing_commit, distance_to_branch_tip, first_parent_commits_range,
    first_parent_distance, list_local_head_branches, merge_base,
};
use super::types::CommitTaskAttribution;

const INTEGRATION_BRANCHES: [&str; 4] = ["main", "master", "staging", "develop"];
const FALLBACK_BRANCH_ORDER: [&str; 4] = ["staging", "main", "master", "develop"];
const UNKNOWN_LABEL: &str = "(unknown)";

#[derive(Debug, Clone, Default)]
pub struct RepoBranchContext {
    local_heads: HashSet<String>,
    branch_owned_commits: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone)]
struct BranchCandidate {
    branch: String,
    distance_to_tip: i64,
}

pub fn load_repo_branch_context(repo_root: &str) -> Result<RepoBranchContext> {
    let heads = list_local_head_branches(repo_root)?;
    let local_heads: HashSet<String> = heads.iter().cloned().collect();
    let integration_heads: Vec<String> = heads
        .iter()
        .filter(|b| is_integration_branch(b))
        .cloned()
        .collect();

    let mut branch_owned_commits: HashMap<String, HashSet<String>> = HashMap::new();
    for branch in heads.iter().filter(|b| !is_integration_branch(b)) {
        let Some(branch_point) = select_branch_point(repo_root, branch, &integration_heads)? else {
            continue;
        };
        let commits = first_parent_commits_range(repo_root, &branch_point, branch)?;
        branch_owned_commits.insert(branch.clone(), commits.into_iter().collect());
    }

    Ok(RepoBranchContext {
        local_heads,
        branch_owned_commits,
    })
}

pub fn attribute_commit_to_task(
    repo_root: &str,
    commit_sha: &str,
    ctx: &RepoBranchContext,
) -> Result<CommitTaskAttribution> {
    if ctx.local_heads.is_empty() {
        return Ok(unknown_task_row());
    }

    let mut containing = branches_containing_commit(repo_root, commit_sha)?;
    containing.retain(|b| ctx.local_heads.contains(b));
    containing.sort();
    containing.dedup();

    if containing.is_empty() {
        return Ok(unknown_task_row());
    }

    let primary: Vec<String> = containing
        .iter()
        .filter(|b| !is_integration_branch(b))
        .filter(|b| commit_is_owned_by_branch(ctx, b, commit_sha))
        .cloned()
        .collect();

    if !primary.is_empty() {
        return select_primary_candidate(repo_root, commit_sha, &primary);
    }

    Ok(select_fallback_candidate(repo_root, commit_sha, &containing))
}

fn select_primary_candidate(
    repo_root: &str,
    commit_sha: &str,
    primary: &[String],
) -> Result<CommitTaskAttribution> {
    let mut ranked = Vec::with_capacity(primary.len());
    for branch in primary {
        let distance = distance_to_branch_tip(repo_root, commit_sha, branch)?;
        ranked.push(BranchCandidate {
            branch: branch.clone(),
            distance_to_tip: distance,
        });
    }

    ranked.sort_by(|a, b| {
        a.distance_to_tip
            .cmp(&b.distance_to_tip)
            .then_with(|| a.branch.cmp(&b.branch))
    });

    let winner = &ranked[0];
    let confidence = compute_primary_confidence(&ranked);

    Ok(CommitTaskAttribution {
        branch_name: winner.branch.clone(),
        task_key: extract_task_key(&winner.branch),
        source: "task_branch".to_string(),
        is_fallback: false,
        candidate_count: primary.len() as i64,
        distance_to_tip: Some(winner.distance_to_tip),
        confidence,
    })
}

fn select_fallback_candidate(
    repo_root: &str,
    commit_sha: &str,
    containing: &[String],
) -> CommitTaskAttribution {
    for fallback in FALLBACK_BRANCH_ORDER {
        if containing.iter().any(|b| b == fallback) {
            return CommitTaskAttribution {
                branch_name: fallback.to_string(),
                task_key: extract_task_key(fallback),
                source: "integration_fallback".to_string(),
                is_fallback: true,
                candidate_count: 0,
                distance_to_tip: distance_to_branch_tip(repo_root, commit_sha, fallback).ok(),
                confidence: 0.5,
            };
        }
    }

    unknown_task_row()
}

fn unknown_task_row() -> CommitTaskAttribution {
    CommitTaskAttribution {
        branch_name: UNKNOWN_LABEL.to_string(),
        task_key: UNKNOWN_LABEL.to_string(),
        source: "integration_fallback".to_string(),
        is_fallback: true,
        candidate_count: 0,
        distance_to_tip: None,
        confidence: 0.0,
    }
}

fn is_integration_branch(branch: &str) -> bool {
    INTEGRATION_BRANCHES.iter().any(|b| b == &branch)
}

fn compute_primary_confidence(ranked: &[BranchCandidate]) -> f64 {
    if ranked.len() <= 1 {
        return 1.0;
    }

    let winner = &ranked[0];
    let runner_up = &ranked[1];
    let margin = runner_up.distance_to_tip - winner.distance_to_tip;
    if margin >= 2 {
        0.9
    } else if margin == 1 {
        0.75
    } else {
        0.6
    }
}

fn commit_is_owned_by_branch(ctx: &RepoBranchContext, branch: &str, commit_sha: &str) -> bool {
    ctx.branch_owned_commits
        .get(branch)
        .map(|set| set.contains(commit_sha))
        .unwrap_or(false)
}

fn select_branch_point(
    repo_root: &str,
    branch: &str,
    integration_heads: &[String],
) -> Result<Option<String>> {
    let mut best: Option<(String, i64, String)> = None; // (base, distance, merge_base)

    for base in integration_heads {
        if base == branch {
            continue;
        }
        let Some(mb) = merge_base(repo_root, branch, base)? else {
            continue;
        };
        let distance = first_parent_distance(repo_root, &mb, branch)?;
        match &best {
            None => best = Some((base.clone(), distance, mb)),
            Some((best_base, best_distance, _)) => {
                if distance < *best_distance || (distance == *best_distance && base < best_base) {
                    best = Some((base.clone(), distance, mb));
                }
            }
        }
    }

    Ok(best.map(|(_, _, mb)| mb))
}

fn extract_task_key(branch: &str) -> String {
    for segment in branch.split(|c: char| c == '/' || c == '_' || c == '.') {
        if let Some(ticket) = parse_ticket_prefix(segment) {
            return ticket;
        }
    }
    branch.to_string()
}

fn parse_ticket_prefix(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_uppercase() {
        i += 1;
    }
    if i == 0 || i >= bytes.len() || bytes[i] != b'-' {
        return None;
    }

    let mut j = i + 1;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }
    if j == i + 1 {
        return None;
    }

    Some(segment[..j].to_string())
}
