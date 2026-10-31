//! Merged pull requests recognized from mainline merge subjects, so delivery evidence exists
//! without a hosting-service token. Records collected from the hosting service take precedence.
use anyhow::Result;
use regex::Regex;
use rusqlite::{Connection, OptionalExtension, params};
use std::process::Command;
use std::sync::OnceLock;

use crate::insights::delivery::{PullRequest, save_pr};

struct MergeSubject {
    number: i64,
    head_ref: Option<String>,
}

fn parse_merge_subject(subject: &str) -> Option<MergeSubject> {
    static PATTERNS: OnceLock<[Regex; 3]> = OnceLock::new();
    let [github_merge, bitbucket_merge, squash] = PATTERNS.get_or_init(|| {
        [
            Regex::new(r"^Merge pull request #(\d+) from (\S+)").unwrap(),
            Regex::new(r"^Merged in (\S+) \(pull request #(\d+)\)").unwrap(),
            Regex::new(r"\(#(\d+)\)\s*$").unwrap(),
        ]
    });
    if let Some(c) = github_merge.captures(subject) {
        return Some(MergeSubject {
            number: c[1].parse().ok()?,
            head_ref: Some(c[2].to_string()),
        });
    }
    if let Some(c) = bitbucket_merge.captures(subject) {
        return Some(MergeSubject {
            number: c[2].parse().ok()?,
            head_ref: Some(c[1].to_string()),
        });
    }
    let c = squash.captures(subject)?;
    Some(MergeSubject {
        number: c[1].parse().ok()?,
        head_ref: None,
    })
}

fn hosted(repo_key: &str) -> Option<(&'static str, String)> {
    if let Some(repo) = repo_key.strip_prefix("git:github.com/") {
        return Some(("github", format!("https://github.com/{repo}/pull/")));
    }
    let repo = repo_key.strip_prefix("git:bitbucket.org/")?;
    Some((
        "bitbucket",
        format!("https://bitbucket.org/{repo}/pull-requests/"),
    ))
}

fn git(repo_root: &str, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(crate::path_utils::on_disk_str(repo_root))
        .args(args)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Records merged PRs found on each scanned repository's mainline since its earliest commit.
pub fn record_merged_prs(conn: &Connection) -> Result<usize> {
    let roots = conn
        .prepare("SELECT repo_root, MIN(commit_time) FROM fact_commit GROUP BY repo_root")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut recorded = 0;
    for (root, since) in roots {
        if !std::path::Path::new(&root).exists() {
            continue;
        }
        let canonical = crate::insights::observation::canonical_repo(conn, &root)?;
        let Some(repo_key) = Some(canonical)
            .filter(|key| key.starts_with("git:"))
            .or_else(|| crate::event_identity::repo_key_for_repo_root(Some(&root)))
        else {
            continue;
        };
        let Some((provider, url_prefix)) = hosted(&repo_key) else {
            continue;
        };
        let Some(mainline) = crate::analytics::resolve_mainline_ref(&root)? else {
            continue;
        };
        let since = format!("--since={since}");
        let log = git(
            &root,
            &[
                "log",
                "--first-parent",
                "--format=%H%x00%P%x00%cI%x00%s",
                &since,
                &mainline,
            ],
        )?;
        for line in log.lines() {
            let mut fields = line.splitn(4, '\0');
            let (Some(sha), Some(parents), Some(merged_at), Some(subject)) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let Some(merge) = parse_merge_subject(subject) else {
                continue;
            };
            let id = format!("{provider}:{repo_key}#{}", merge.number);
            let known = conn
                .query_row(
                    "SELECT 1 FROM pull_request_record WHERE id = ?1",
                    params![id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if known {
                continue;
            }
            let parents: Vec<&str> = parents.split_whitespace().collect();
            let (head_sha, commits) = match parents.as_slice() {
                [base, head] => {
                    let range = format!("{base}..{head}");
                    let commits = git(&root, &["rev-list", &range])?
                        .lines()
                        .map(str::to_string)
                        .collect();
                    (Some(head.to_string()), commits)
                }
                _ => (None, vec![sha.to_string()]),
            };
            save_pr(
                conn,
                &PullRequest {
                    provider: provider.into(),
                    repo_key: repo_key.clone(),
                    number: merge.number,
                    url: format!("{url_prefix}{}", merge.number),
                    title: subject.to_string(),
                    state: "merged".into(),
                    merged_at: Some(merged_at.to_string()),
                    closed_at: Some(merged_at.to_string()),
                    head_sha,
                    merge_sha: Some(sha.to_string()),
                    head_ref: merge.head_ref,
                    base_ref: mainline.rsplit('/').next().map(str::to_string),
                    commits,
                    ..PullRequest::default()
                },
            )?;
            recorded += 1;
        }
    }
    Ok(recorded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn run(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .output()
            .expect("git runs");
        assert!(output.status.success(), "git {args:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    #[test]
    fn recognizes_merge_and_squash_subjects() {
        let parsed = |s: &str| parse_merge_subject(s).map(|m| (m.number, m.head_ref));
        assert_eq!(
            parsed("Merge pull request #12 from org/fix-login"),
            Some((12, Some("org/fix-login".into())))
        );
        assert_eq!(
            parsed("Merged in agent/x (pull request #7)"),
            Some((7, Some("agent/x".into())))
        );
        assert_eq!(parsed("Fix login redirect (#31)"), Some((31, None)));
        assert_eq!(parsed("Fix login redirect"), None);
    }

    #[test]
    fn merged_prs_on_the_mainline_are_recorded_with_their_commits() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let repo = dir.path();
        run(repo, &["init", "-q", "-b", "main"]);
        run(
            repo,
            &["remote", "add", "origin", "https://github.com/org/app.git"],
        );
        std::fs::write(repo.join("a"), "a\n")?;
        run(repo, &["add", "."]);
        run(repo, &["commit", "-q", "-m", "start"]);
        run(repo, &["checkout", "-q", "-b", "feature"]);
        std::fs::write(repo.join("b"), "b\n")?;
        run(repo, &["add", "."]);
        run(repo, &["commit", "-q", "-m", "add b"]);
        let feature = run(repo, &["rev-parse", "HEAD"]);
        run(repo, &["checkout", "-q", "main"]);
        run(
            repo,
            &[
                "merge",
                "-q",
                "--no-ff",
                "-m",
                "Merge pull request #5 from org/feature",
                "feature",
            ],
        );

        let root =
            crate::change_intel::commit_assoc::git_scan::validate_git_repo(repo.to_str().unwrap())?
                .unwrap();
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        conn.execute(
            "INSERT INTO fact_commit (repo_root, commit_sha, commit_time, subject)
             VALUES (?1, ?2, '2000-01-01T00:00:00Z', 'add b')",
            params![root, feature],
        )?;

        assert_eq!(record_merged_prs(&conn)?, 1);
        let pr = crate::insights::delivery::prs(&conn)?;
        assert_eq!(pr.len(), 1);
        assert_eq!(pr[0].url, "https://github.com/org/app/pull/5");
        assert_eq!(pr[0].state, "merged");
        assert_eq!(pr[0].commits, [feature]);
        assert_eq!(record_merged_prs(&conn)?, 0);
        Ok(())
    }
}
