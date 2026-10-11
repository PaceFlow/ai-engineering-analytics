use anyhow::Result;
use std::collections::{HashMap, HashSet};

use super::git_scan::{list_commits_on_ref, list_first_parent_commits, list_local_head_branches};
use super::types::CommitTaskAttribution;

const INTEGRATION_BRANCHES: [&str; 4] = ["main", "master", "staging", "develop"];
const FALLBACK_BRANCH_ORDER: [&str; 4] = ["staging", "main", "master", "develop"];
const UNKNOWN_LABEL: &str = "(unknown)";

#[derive(Debug, Clone, Default)]
pub struct RepoBranchContext {
    local_heads: HashSet<String>,
    branch_owned_commits: HashMap<String, HashSet<String>>,
    branch_distance_to_tip: HashMap<String, HashMap<String, i64>>,
    commit_containing_branches: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
struct BranchCandidate {
    branch: String,
    distance_to_tip: i64,
}

pub fn load_repo_branch_context(repo_root: &str) -> Result<RepoBranchContext> {
    let heads = list_local_head_branches(repo_root)?;
    let local_heads: HashSet<String> = heads.iter().cloned().collect();

    // Union of the first-parent chains of every locally-present integration
    // branch (main/master/staging/develop). A task branch "owns" a commit iff
    // the commit is on the branch's own first-parent spine but is not on any
    // integration spine. This stays stable across plain merges, squash
    // merges, and rebase merges because the integration branch's first-parent
    // walk never enters the merged-in side.
    let mut integration_first_parent: HashSet<String> = HashSet::new();
    for branch in heads.iter().filter(|b| is_integration_branch(b)) {
        for sha in list_first_parent_commits(repo_root, branch)? {
            integration_first_parent.insert(sha);
        }
    }

    let mut branch_owned_commits: HashMap<String, HashSet<String>> = HashMap::new();
    for branch in heads.iter().filter(|b| !is_integration_branch(b)) {
        let owned: HashSet<String> = list_first_parent_commits(repo_root, branch)?
            .into_iter()
            .filter(|sha| !integration_first_parent.contains(sha))
            .collect();
        branch_owned_commits.insert(branch.clone(), owned);
    }

    let mut branch_distance_to_tip: HashMap<String, HashMap<String, i64>> = HashMap::new();
    let mut commit_containing_branches: HashMap<String, Vec<String>> = HashMap::new();
    for branch in &heads {
        let commits = list_commits_on_ref(repo_root, branch)?;
        let mut distances = HashMap::with_capacity(commits.len());
        for (distance, commit_sha) in commits.into_iter().enumerate() {
            distances.insert(commit_sha.clone(), distance as i64);
            commit_containing_branches
                .entry(commit_sha)
                .or_default()
                .push(branch.clone());
        }
        branch_distance_to_tip.insert(branch.clone(), distances);
    }
    for branches in commit_containing_branches.values_mut() {
        branches.sort();
        branches.dedup();
    }

    Ok(RepoBranchContext {
        local_heads,
        branch_owned_commits,
        branch_distance_to_tip,
        commit_containing_branches,
    })
}

pub fn attribute_commit_to_task(
    _repo_root: &str,
    commit_sha: &str,
    ctx: &RepoBranchContext,
) -> Result<CommitTaskAttribution> {
    if ctx.local_heads.is_empty() {
        return Ok(unknown_task_row());
    }

    let containing = ctx
        .commit_containing_branches
        .get(commit_sha)
        .cloned()
        .unwrap_or_default();

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
        return select_primary_candidate(ctx, commit_sha, &primary);
    }

    Ok(select_fallback_candidate(ctx, commit_sha, &containing))
}

fn select_primary_candidate(
    ctx: &RepoBranchContext,
    commit_sha: &str,
    primary: &[String],
) -> Result<CommitTaskAttribution> {
    let mut ranked = Vec::with_capacity(primary.len());
    for branch in primary {
        let distance = ctx
            .branch_distance_to_tip(branch, commit_sha)
            .unwrap_or_default();
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
    ctx: &RepoBranchContext,
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
                distance_to_tip: ctx.branch_distance_to_tip(fallback, commit_sha),
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

impl RepoBranchContext {
    fn branch_distance_to_tip(&self, branch: &str, commit_sha: &str) -> Option<i64> {
        self.branch_distance_to_tip
            .get(branch)
            .and_then(|commits| commits.get(commit_sha))
            .copied()
    }
}

fn extract_task_key(branch: &str) -> String {
    let bytes = branch.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let at_boundary = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        if at_boundary
            && bytes[i].is_ascii_alphabetic()
            && let Some(ticket) = parse_ticket_prefix_at(bytes, i)
        {
            return ticket;
        }
        i += 1;
    }
    branch.to_string()
}

fn parse_ticket_prefix_at(bytes: &[u8], start: usize) -> Option<String> {
    if start >= bytes.len() || !bytes[start].is_ascii_alphabetic() {
        return None;
    }

    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i >= bytes.len() || (bytes[i] != b'-' && bytes[i] != b'_') {
        return None;
    }

    let sep = i;
    let mut j = sep + 1;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }
    if j == sep + 1 {
        return None;
    }

    let prefix = std::str::from_utf8(&bytes[start..sep])
        .ok()?
        .to_ascii_uppercase();
    let number = std::str::from_utf8(&bytes[sep + 1..j]).ok()?;
    Some(format!("{}-{}", prefix, number))
}

#[cfg(test)]
mod tests {
    use super::{RepoBranchContext, attribute_commit_to_task, extract_task_key};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn extract_task_key_accepts_lowercase_and_normalizes_to_uppercase() {
        assert_eq!(extract_task_key("pac-611-untrack-an-identity"), "PAC-611");
    }

    #[test]
    fn extract_task_key_keeps_uppercase_ticket_prefixes() {
        assert_eq!(extract_task_key("PAC-611-untrack-an-identity"), "PAC-611");
    }

    #[test]
    fn extract_task_key_uses_normalized_ticket_from_branch_segment() {
        assert_eq!(
            extract_task_key("feature/pac-611-untrack-an-identity-when-linked-person-gets-deleted"),
            "PAC-611"
        );
    }

    #[test]
    fn extract_task_key_accepts_underscore_after_prefix() {
        assert_eq!(
            extract_task_key("PAC_837_update_firebase_db_rules"),
            "PAC-837"
        );
    }

    #[test]
    fn extract_task_key_accepts_lowercase_underscore_prefix() {
        assert_eq!(extract_task_key("pac_529_person_page_load_time"), "PAC-529");
    }

    #[test]
    fn extract_task_key_prefers_first_ticket_in_nested_branch() {
        assert_eq!(extract_task_key("feature/my_branch-PAC-123"), "PAC-123");
    }

    #[test]
    fn extract_task_key_falls_back_to_raw_branch_when_no_ticket() {
        assert_eq!(extract_task_key("staging"), "staging");
        assert_eq!(extract_task_key("monitor_cleanup"), "monitor_cleanup");
    }

    fn build_ctx(
        local_heads: &[&str],
        owned: &[(&str, &[&str])],
        containing: &[(&str, &[&str])],
        distances: &[(&str, &[(&str, i64)])],
    ) -> RepoBranchContext {
        let mut ctx = RepoBranchContext {
            local_heads: local_heads.iter().map(|s| s.to_string()).collect(),
            branch_owned_commits: HashMap::new(),
            branch_distance_to_tip: HashMap::new(),
            commit_containing_branches: HashMap::new(),
        };
        for (branch, commits) in owned {
            let set: HashSet<String> = commits.iter().map(|s| s.to_string()).collect();
            ctx.branch_owned_commits.insert(branch.to_string(), set);
        }
        for (commit, branches) in containing {
            ctx.commit_containing_branches.insert(
                commit.to_string(),
                branches.iter().map(|s| s.to_string()).collect(),
            );
        }
        for (branch, entries) in distances {
            let map: HashMap<String, i64> = entries
                .iter()
                .map(|(sha, d)| ((*sha).to_string(), *d))
                .collect();
            ctx.branch_distance_to_tip.insert(branch.to_string(), map);
        }
        ctx
    }

    #[test]
    fn attributes_commits_on_branch_first_parent_even_after_plain_merge() {
        // commit `abc` was originally made on `PAC-123_foo` and later merged
        // into `main` via a standard (non-fast-forward) merge commit. It now
        // appears in both branches' ancestry but only the task branch's
        // first-parent spine — i.e. only the task branch owns it.
        let ctx = build_ctx(
            &["main", "PAC-123_foo"],
            &[("PAC-123_foo", &["abc"])],
            &[("abc", &["PAC-123_foo", "main"])],
            &[("PAC-123_foo", &[("abc", 1)]), ("main", &[("abc", 0)])],
        );

        let attr = attribute_commit_to_task("/tmp/repo", "abc", &ctx).unwrap();

        assert_eq!(attr.source, "task_branch");
        assert_eq!(attr.branch_name, "PAC-123_foo");
        assert_eq!(attr.task_key, "PAC-123");
        assert!(!attr.is_fallback);
    }

    #[test]
    fn attributes_to_task_branch_when_commit_also_reachable_from_main_via_merge() {
        // Two task branches own disjoint commits; the closer branch wins.
        // `abc` is owned only by `PAC-837_firebase` even though it is also
        // reachable from `main` (merged) and from an unrelated local branch.
        let ctx = build_ctx(
            &["main", "PAC-837_firebase", "other_branch"],
            &[("PAC-837_firebase", &["abc"]), ("other_branch", &[])],
            &[("abc", &["PAC-837_firebase", "main", "other_branch"])],
            &[
                ("PAC-837_firebase", &[("abc", 0)]),
                ("main", &[("abc", 5)]),
                ("other_branch", &[("abc", 10)]),
            ],
        );

        let attr = attribute_commit_to_task("/tmp/repo", "abc", &ctx).unwrap();

        assert_eq!(attr.source, "task_branch");
        assert_eq!(attr.branch_name, "PAC-837_firebase");
        assert_eq!(attr.task_key, "PAC-837");
        assert!(!attr.is_fallback);
    }

    #[test]
    fn falls_back_to_integration_when_commit_only_on_integration_first_parent() {
        // Nothing owns this commit — it lives on the integration spine only.
        let ctx = build_ctx(
            &["main", "PAC-123_foo"],
            &[("PAC-123_foo", &[])],
            &[("abc", &["main"])],
            &[("main", &[("abc", 0)])],
        );

        let attr = attribute_commit_to_task("/tmp/repo", "abc", &ctx).unwrap();

        assert_eq!(attr.source, "integration_fallback");
        assert_eq!(attr.branch_name, "main");
        assert!(attr.is_fallback);
    }
}
