use anyhow::{Context, Result, anyhow, bail};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::{HooksArgs, HooksCommands, HooksRepoArgs};
use crate::path_utils::detect_repo_root;
use crate::sync::resolved_sync_config;

const PACEFLOW_HOOK_MARKER: &str = "PACEFLOW_MANAGED_PRE_COMMIT_HOOK";

pub fn run(args: HooksArgs) -> Result<()> {
    match args.command {
        HooksCommands::Install(args) => install_hook(args.repo.as_deref()),
        HooksCommands::Uninstall(args) => uninstall_hook(args.repo.as_deref()),
        HooksCommands::Status(args) => status_hook(args.repo.as_deref()),
        HooksCommands::PreCommit(args) => run_pre_commit(args),
    }
}

fn run_pre_commit(args: HooksRepoArgs) -> Result<()> {
    let _repo_root = resolve_repo_root(args.repo.as_deref())?;
    check_sync_configured()
}

fn install_hook(repo: Option<&str>) -> Result<()> {
    let repo_root = resolve_repo_root(repo)?;
    let hook_path = pre_commit_hook_path(&repo_root)?;

    if let Some(existing) = read_existing_hook(&hook_path)?
        && !is_paceflow_managed(&existing)
    {
        bail!(
            "refusing to overwrite existing non-Paceflow pre-commit hook at {}",
            hook_path.display()
        );
    }

    if let Some(parent) = hook_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create hook directory {}", parent.display()))?;
    }
    fs::write(&hook_path, hook_contents())
        .with_context(|| format!("failed to write {}", hook_path.display()))?;
    set_executable(&hook_path)?;
    println!(
        "Installed Paceflow pre-commit setup gate at {}",
        hook_path.display()
    );
    Ok(())
}

fn uninstall_hook(repo: Option<&str>) -> Result<()> {
    let repo_root = resolve_repo_root(repo)?;
    let hook_path = pre_commit_hook_path(&repo_root)?;

    let Some(existing) = read_existing_hook(&hook_path)? else {
        println!("No pre-commit hook found at {}", hook_path.display());
        return Ok(());
    };

    if !is_paceflow_managed(&existing) {
        bail!(
            "refusing to remove existing non-Paceflow pre-commit hook at {}",
            hook_path.display()
        );
    }

    fs::remove_file(&hook_path)
        .with_context(|| format!("failed to remove {}", hook_path.display()))?;
    println!(
        "Removed Paceflow pre-commit setup gate from {}",
        hook_path.display()
    );
    Ok(())
}

fn status_hook(repo: Option<&str>) -> Result<()> {
    let repo_root = resolve_repo_root(repo)?;
    let hook_path = pre_commit_hook_path(&repo_root)?;

    match read_existing_hook(&hook_path)? {
        Some(contents) if is_paceflow_managed(&contents) => {
            println!(
                "Paceflow pre-commit setup gate is installed at {}",
                hook_path.display()
            );
        }
        Some(_) => {
            println!(
                "A non-Paceflow pre-commit hook exists at {}",
                hook_path.display()
            );
        }
        None => {
            println!(
                "Paceflow pre-commit setup gate is not installed at {}",
                hook_path.display()
            );
        }
    }
    Ok(())
}

fn check_sync_configured() -> Result<()> {
    match resolved_sync_config() {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            bail!("paceflow sync is not configured for this machine.\nRun: paceflow sync config")
        }
        Err(err) => bail!("invalid paceflow sync configuration: {err}"),
    }
}

fn resolve_repo_root(repo: Option<&str>) -> Result<PathBuf> {
    let candidate = match repo {
        Some(repo) => PathBuf::from(repo),
        None => std::env::current_dir()?,
    };
    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        std::env::current_dir()?.join(candidate)
    };
    detect_repo_root(&absolute).ok_or_else(|| {
        anyhow!(
            "could not determine a git repository root from {}",
            absolute.display()
        )
    })
}

fn pre_commit_hook_path(repo_root: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .current_dir(repo_root)
        .args(["rev-parse", "--git-path", "hooks/pre-commit"])
        .output()
        .context("failed to locate git hooks directory")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("failed to locate git hooks directory: {}", stderr.trim());
    }
    let raw = String::from_utf8(output.stdout)?.trim().to_string();
    if raw.is_empty() {
        bail!("git returned an empty pre-commit hook path");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(repo_root.join(path))
    }
}

fn read_existing_hook(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn is_paceflow_managed(contents: &str) -> bool {
    contents.contains(PACEFLOW_HOOK_MARKER)
}

fn hook_contents() -> &'static str {
    "#!/bin/sh\n\
# PACEFLOW_MANAGED_PRE_COMMIT_HOOK\n\
root=\"$(git rev-parse --show-toplevel 2>/dev/null)\" || exit 1\n\
\n\
if ! command -v paceflow >/dev/null 2>&1; then\n\
  echo \"paceflow is required for this repo. Install it, then run: paceflow sync config\" >&2\n\
  exit 1\n\
fi\n\
\n\
exec paceflow hooks pre-commit --repo \"$root\"\n"
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
        .with_context(|| format!("failed to make {} executable", path.display()))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{
        SYNC_BASE_URL_ENV_VAR, SYNC_ORGANIZATION_ID_ENV_VAR, SYNC_TOKEN_ENV_VAR, SavedSyncConfig,
        save_sync_config,
    };
    use anyhow::Result;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::process::Command;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct ScopedEnvVar {
        key: &'static str,
        original: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }

        fn unset(key: &'static str) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, original }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    fn init_repo(path: &std::path::Path) -> Result<()> {
        fs::create_dir_all(path)?;
        let status = Command::new("git").current_dir(path).arg("init").status()?;
        assert!(status.success());
        Ok(())
    }

    fn unset_sync_env() -> [ScopedEnvVar; 3] {
        [
            ScopedEnvVar::unset(SYNC_BASE_URL_ENV_VAR),
            ScopedEnvVar::unset(SYNC_ORGANIZATION_ID_ENV_VAR),
            ScopedEnvVar::unset(SYNC_TOKEN_ENV_VAR),
        ]
    }

    #[test]
    fn install_creates_managed_pre_commit_hook() -> Result<()> {
        let tempdir = tempdir()?;
        let repo = tempdir.path().join("repo");
        init_repo(&repo)?;

        install_hook(Some(repo.to_string_lossy().as_ref()))?;

        let hook = repo.join(".git/hooks/pre-commit");
        let contents = fs::read_to_string(&hook)?;
        assert!(contents.contains(PACEFLOW_HOOK_MARKER));
        assert!(contents.contains("exec paceflow hooks pre-commit --repo \"$root\""));
        assert!(!contents.contains("\r\n"));
        Ok(())
    }

    #[test]
    fn install_refuses_existing_non_paceflow_hook() -> Result<()> {
        let tempdir = tempdir()?;
        let repo = tempdir.path().join("repo");
        init_repo(&repo)?;
        let hook = repo.join(".git/hooks/pre-commit");
        fs::write(&hook, "#!/bin/sh\necho existing\n")?;

        let err = install_hook(Some(repo.to_string_lossy().as_ref())).expect_err("should refuse");

        assert!(
            err.to_string()
                .contains("existing non-Paceflow pre-commit hook")
        );
        assert_eq!(fs::read_to_string(&hook)?, "#!/bin/sh\necho existing\n");
        Ok(())
    }

    #[test]
    fn install_updates_existing_paceflow_hook() -> Result<()> {
        let tempdir = tempdir()?;
        let repo = tempdir.path().join("repo");
        init_repo(&repo)?;
        let hook = repo.join(".git/hooks/pre-commit");
        fs::write(&hook, format!("#!/bin/sh\n# {PACEFLOW_HOOK_MARKER}\nold\n"))?;

        install_hook(Some(repo.to_string_lossy().as_ref()))?;

        let contents = fs::read_to_string(&hook)?;
        assert!(contents.contains("exec paceflow hooks pre-commit --repo \"$root\""));
        assert!(!contents.contains("old"));
        Ok(())
    }

    #[test]
    fn pre_commit_check_fails_without_sync_config() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _sync_env = unset_sync_env();

        let err = check_sync_configured().expect_err("should require sync config");

        assert!(err.to_string().contains("paceflow sync is not configured"));
        Ok(())
    }

    #[test]
    fn pre_commit_check_accepts_saved_sync_config() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _sync_env = unset_sync_env();
        save_sync_config(&SavedSyncConfig {
            base_url: "https://api.example.com".to_string(),
            organization_id: "org-1".to_string(),
            organization_name: Some("Example".to_string()),
            member_email: Some("dev@example.com".to_string()),
            token: "token-1".to_string(),
        })?;

        check_sync_configured()?;
        Ok(())
    }

    #[test]
    fn pre_commit_check_accepts_env_only_sync_config() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _base = ScopedEnvVar::set(SYNC_BASE_URL_ENV_VAR, "https://api.example.com");
        let _org = ScopedEnvVar::set(SYNC_ORGANIZATION_ID_ENV_VAR, "org-1");
        let _token = ScopedEnvVar::set(SYNC_TOKEN_ENV_VAR, "token-1");

        check_sync_configured()?;
        Ok(())
    }

    #[test]
    fn pre_commit_check_accepts_env_over_saved_sync_config() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _base = ScopedEnvVar::set(SYNC_BASE_URL_ENV_VAR, "https://api.example.com");
        let _org = ScopedEnvVar::set(SYNC_ORGANIZATION_ID_ENV_VAR, "org-1");
        let _token = ScopedEnvVar::set(SYNC_TOKEN_ENV_VAR, "token-1");
        save_sync_config(&SavedSyncConfig {
            base_url: "not-a-url".to_string(),
            organization_id: "saved-org".to_string(),
            organization_name: Some("Saved".to_string()),
            member_email: Some("saved@example.com".to_string()),
            token: "saved-token".to_string(),
        })?;

        check_sync_configured()?;
        Ok(())
    }

    #[test]
    fn pre_commit_check_reports_malformed_base_url() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _base = ScopedEnvVar::set(SYNC_BASE_URL_ENV_VAR, "api.example.com");
        let _org = ScopedEnvVar::set(SYNC_ORGANIZATION_ID_ENV_VAR, "org-1");
        let _token = ScopedEnvVar::set(SYNC_TOKEN_ENV_VAR, "token-1");

        let err = check_sync_configured().expect_err("should reject malformed base URL");

        assert!(err.to_string().contains("sync base URL must start"));
        Ok(())
    }
}
