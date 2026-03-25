use std::path::Path;

use crate::cli::ReportArgs;
use crate::path_utils::detect_repo_root;

pub fn resolve_report_args(args: &ReportArgs) -> ReportArgs {
    let cwd = std::env::current_dir().ok();
    resolve_report_args_for_cwd(args, cwd.as_deref())
}

fn resolve_report_args_for_cwd(args: &ReportArgs, cwd: Option<&Path>) -> ReportArgs {
    let mut resolved = args.clone();
    if resolved.repo.is_none() && !resolved.all_projects {
        if let Some(cwd) = cwd {
            if let Some(repo_root) = detect_repo_root(cwd) {
                resolved.repo = Some(repo_root.to_string_lossy().into_owned());
            }
        }
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::resolve_report_args_for_cwd;
    use crate::cli::ReportArgs;
    use anyhow::Result;
    use std::path::Path;
    use std::process::Command;
    use tempfile::tempdir;

    fn base_args() -> ReportArgs {
        ReportArgs {
            weekly: false,
            group_by: None,
            from: None,
            to: None,
            repo: None,
            all_projects: false,
            provider: None,
            task: None,
            model: None,
            limit: 50,
        }
    }

    fn git(args: &[&str], cwd: &Path) -> Result<()> {
        let status = Command::new("git").current_dir(cwd).args(args).status()?;
        anyhow::ensure!(
            status.success(),
            "git {:?} failed in {}",
            args,
            cwd.display()
        );
        Ok(())
    }

    #[test]
    fn injects_current_repo_when_running_inside_repo() -> Result<()> {
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("sample-repo");
        std::fs::create_dir_all(repo_root.join("nested/dir"))?;
        git(&["init", "-q"], &repo_root)?;

        let args = base_args();
        let resolved = resolve_report_args_for_cwd(&args, Some(&repo_root.join("nested/dir")));

        assert_eq!(
            resolved.repo.as_deref(),
            Some(
                std::fs::canonicalize(&repo_root)?
                    .to_string_lossy()
                    .as_ref()
            )
        );
        Ok(())
    }

    #[test]
    fn leaves_repo_unset_outside_git_repo() -> Result<()> {
        let tempdir = tempdir()?;
        let outside_dir = tempdir.path().join("plain-dir");
        std::fs::create_dir_all(&outside_dir)?;

        let args = base_args();
        let resolved = resolve_report_args_for_cwd(&args, Some(&outside_dir));

        assert!(resolved.repo.is_none());
        Ok(())
    }

    #[test]
    fn all_projects_suppresses_auto_repo_injection() -> Result<()> {
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("sample-repo");
        std::fs::create_dir_all(&repo_root)?;
        git(&["init", "-q"], &repo_root)?;

        let mut args = base_args();
        args.all_projects = true;
        let resolved = resolve_report_args_for_cwd(&args, Some(&repo_root));

        assert!(resolved.repo.is_none());
        Ok(())
    }

    #[test]
    fn explicit_repo_wins_over_auto_detected_repo() -> Result<()> {
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("sample-repo");
        std::fs::create_dir_all(&repo_root)?;
        git(&["init", "-q"], &repo_root)?;

        let mut args = base_args();
        args.repo = Some("/tmp/explicit".to_string());
        let resolved = resolve_report_args_for_cwd(&args, Some(&repo_root));

        assert_eq!(resolved.repo.as_deref(), Some("/tmp/explicit"));
        Ok(())
    }
}
