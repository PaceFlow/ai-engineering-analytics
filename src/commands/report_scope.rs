use std::path::Path;

use crate::cli::{GroupBy, ReportArgs};
use crate::path_utils::{detect_repo_root, path_to_string};

#[derive(Debug, Clone)]
pub struct ResolvedMainReportArgs {
    pub report: ReportArgs,
    pub implicit_model_default: bool,
}

pub fn resolve_report_args(args: &ReportArgs) -> ReportArgs {
    let cwd = std::env::current_dir().ok();
    resolve_report_args_for_cwd(args, cwd.as_deref())
}

pub fn resolve_main_report_args(args: &ReportArgs, overall: bool) -> ResolvedMainReportArgs {
    let cwd = std::env::current_dir().ok();
    resolve_main_report_args_for_cwd(args, overall, cwd.as_deref())
}

fn resolve_report_args_for_cwd(args: &ReportArgs, cwd: Option<&Path>) -> ReportArgs {
    let mut resolved = args.clone();
    if resolved.repo.is_none()
        && !resolved.all_projects
        && let Some(cwd) = cwd
        && let Some(repo_root) = detect_repo_root(cwd)
    {
        resolved.repo = Some(path_to_string(&repo_root));
    }
    resolved
}

fn resolve_main_report_args_for_cwd(
    args: &ReportArgs,
    overall: bool,
    cwd: Option<&Path>,
) -> ResolvedMainReportArgs {
    let mut report = resolve_report_args_for_cwd(args, cwd);
    let implicit_model_default = !overall && report.group_by.is_none();
    if implicit_model_default {
        report.group_by = Some(GroupBy::Model);
    }

    ResolvedMainReportArgs {
        report,
        implicit_model_default,
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_main_report_args_for_cwd, resolve_report_args_for_cwd};
    use crate::cli::{GroupBy, ReportArgs};
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
            branch: None,
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

    #[test]
    fn main_reports_default_to_model_grouping() {
        let args = base_args();
        let resolved = resolve_main_report_args_for_cwd(&args, false, None);

        assert_eq!(resolved.report.group_by, Some(GroupBy::Model));
        assert!(resolved.implicit_model_default);
    }

    #[test]
    fn overall_preserves_top_level_summary_view() {
        let args = base_args();
        let resolved = resolve_main_report_args_for_cwd(&args, true, None);

        assert_eq!(resolved.report.group_by, None);
        assert!(!resolved.implicit_model_default);
    }

    #[test]
    fn explicit_grouping_beats_model_default() {
        let mut args = base_args();
        args.group_by = Some(GroupBy::Provider);

        let resolved = resolve_main_report_args_for_cwd(&args, false, None);
        assert_eq!(resolved.report.group_by, Some(GroupBy::Provider));
        assert!(!resolved.implicit_model_default);
    }
}
