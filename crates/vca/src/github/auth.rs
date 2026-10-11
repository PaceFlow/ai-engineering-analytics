use anyhow::{Result, anyhow};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

pub const GITHUB_TOKEN_ENV_VAR: &str = "VCA_GITHUB_TOKEN";
const GITHUB_TOKEN_FILE_NAME: &str = "github_token";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHubTokenSource {
    Environment,
    Saved,
}

pub fn github_token_from_env() -> Option<String> {
    github_token_from_env_for(crate::app::AppContext::VCA)
}

pub fn github_token_from_env_for(context: crate::app::AppContext) -> Option<String> {
    context
        .env("GITHUB_TOKEN")
        .and_then(|v| v.into_string().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn github_token() -> Result<Option<String>> {
    github_token_for(crate::app::AppContext::VCA)
}

pub fn github_token_for(context: crate::app::AppContext) -> Result<Option<String>> {
    if let Some(token) = github_token_from_env_for(context) {
        return Ok(Some(token));
    }
    load_saved_github_token()
}

pub fn github_token_source() -> Result<Option<GitHubTokenSource>> {
    github_token_source_for(crate::app::AppContext::VCA)
}

pub fn github_token_source_for(
    context: crate::app::AppContext,
) -> Result<Option<GitHubTokenSource>> {
    if github_token_from_env_for(context).is_some() {
        return Ok(Some(GitHubTokenSource::Environment));
    }
    if load_saved_github_token()?.is_some() {
        return Ok(Some(GitHubTokenSource::Saved));
    }
    Ok(None)
}

pub fn saved_github_token_path() -> Result<PathBuf> {
    let app_dir = crate::data_home::directory()?;
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
    use crate::test_support::{ScopedEnvVar, lock_env};
    use tempfile::tempdir;

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
