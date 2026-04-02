use anyhow::{Result, anyhow};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

pub const GITHUB_TOKEN_ENV_VAR: &str = "PACEFLOW_GITHUB_TOKEN";
const GITHUB_TOKEN_FILE_NAME: &str = "github_token";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHubTokenSource {
    Environment,
    Saved,
}

pub fn github_token_from_env() -> Option<String> {
    env::var(GITHUB_TOKEN_ENV_VAR)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn github_token() -> Result<Option<String>> {
    if let Some(token) = github_token_from_env() {
        return Ok(Some(token));
    }
    load_saved_github_token()
}

pub fn github_token_source() -> Result<Option<GitHubTokenSource>> {
    if github_token_from_env().is_some() {
        return Ok(Some(GitHubTokenSource::Environment));
    }
    if load_saved_github_token()?.is_some() {
        return Ok(Some(GitHubTokenSource::Saved));
    }
    Ok(None)
}

pub fn saved_github_token_path() -> Result<PathBuf> {
    let home = env::var_os("PACEFLOW_HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
    let app_dir = home.join(".paceflow");
    fs::create_dir_all(&app_dir)?;
    Ok(app_dir.join(GITHUB_TOKEN_FILE_NAME))
}

pub fn load_saved_github_token() -> Result<Option<String>> {
    let path = saved_github_token_path()?;
    match fs::read_to_string(path) {
        Ok(contents) => Ok(normalize_token(&contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn save_github_token(token: &str) -> Result<PathBuf> {
    let token = normalized_non_empty_token(token)?;
    let path = saved_github_token_path()?;
    write_token_file(&path, &token)?;
    Ok(path)
}

pub fn delete_saved_github_token() -> Result<bool> {
    let path = saved_github_token_path()?;
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn normalize_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalized_non_empty_token(raw: &str) -> Result<String> {
    normalize_token(raw).ok_or_else(|| anyhow!("GitHub token cannot be empty"))
}

fn write_token_file(path: &PathBuf, token: &str) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())?;
    file.write_all(b"\n")?;
    file.flush()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
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

    #[test]
    fn saved_token_round_trips() -> Result<()> {
        let _env_guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_token = ScopedEnvVar::unset(GITHUB_TOKEN_ENV_VAR);

        save_github_token("ghp_saved_token")?;

        assert_eq!(
            load_saved_github_token()?.as_deref(),
            Some("ghp_saved_token")
        );
        Ok(())
    }

    #[test]
    fn github_token_prefers_env_var_over_saved_token() -> Result<()> {
        let _env_guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_token = ScopedEnvVar::set(GITHUB_TOKEN_ENV_VAR, "ghp_env_token");
        save_github_token("ghp_saved_token")?;

        assert_eq!(github_token()?.as_deref(), Some("ghp_env_token"));
        assert_eq!(github_token_source()?, Some(GitHubTokenSource::Environment));
        Ok(())
    }

    #[test]
    fn github_token_falls_back_to_saved_token_when_env_unset() -> Result<()> {
        let _env_guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_token = ScopedEnvVar::unset(GITHUB_TOKEN_ENV_VAR);
        save_github_token("ghp_saved_token")?;

        assert_eq!(github_token()?.as_deref(), Some("ghp_saved_token"));
        assert_eq!(github_token_source()?, Some(GitHubTokenSource::Saved));
        Ok(())
    }

    #[test]
    fn delete_saved_token_removes_file() -> Result<()> {
        let _env_guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_token = ScopedEnvVar::unset(GITHUB_TOKEN_ENV_VAR);
        let path = save_github_token("ghp_saved_token")?;
        assert!(path.exists());

        assert!(delete_saved_github_token()?);
        assert!(!path.exists());
        assert!(load_saved_github_token()?.is_none());
        Ok(())
    }
}
