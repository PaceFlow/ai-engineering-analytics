use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

fn run_git_capture(repo_root: Option<&str>, args: &[&str]) -> Option<String> {
    let mut cmd = Command::new("git");
    if let Some(repo_root) = repo_root {
        cmd.arg("-C").arg(repo_root);
    }
    let output = cmd.args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }

    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn normalize_git_remote(input: &str) -> Option<String> {
    let value = input.trim().trim_end_matches('/').trim_end_matches(".git");
    if value.is_empty() {
        return None;
    }

    if let Some(rest) = value.strip_prefix("git@") {
        let (host, path) = rest.split_once(':')?;
        let path = path.trim_start_matches('/').trim();
        if host.is_empty() || path.is_empty() {
            return None;
        }
        return Some(format!("{}/{}", host.to_ascii_lowercase(), path));
    }

    let without_scheme = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(value);
    let without_auth = without_scheme
        .rsplit_once('@')
        .map(|(_, rest)| rest)
        .unwrap_or(without_scheme);
    let (host, path) = without_auth.split_once('/')?;
    let path = path.trim_start_matches('/').trim();
    if host.is_empty() || path.is_empty() {
        return None;
    }

    Some(format!("{}/{}", host.to_ascii_lowercase(), path))
}

pub fn github_repo_from_repo_key(repo_key: &str) -> Option<(String, String)> {
    let repo = repo_key.trim().strip_prefix("git:github.com/")?;
    let (owner, name) = repo.split_once('/')?;
    let owner = owner.trim();
    let name = name.trim();
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return None;
    }
    Some((owner.to_string(), name.to_string()))
}

pub fn repo_key_for_repo_root(repo_root: Option<&str>) -> Option<String> {
    static CACHE: OnceLock<Mutex<HashMap<String, Option<String>>>> = OnceLock::new();

    let repo_root = repo_root?.trim();
    if repo_root.is_empty() || !Path::new(repo_root).exists() {
        return None;
    }

    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache
        .lock()
        .ok()
        .and_then(|guard| guard.get(repo_root).cloned())
    {
        return cached;
    }

    let resolved = run_git_capture(Some(repo_root), &["remote", "get-url", "origin"])
        .and_then(|remote| normalize_git_remote(&remote))
        .map(|remote| format!("git:{remote}"));

    if let Ok(mut guard) = cache.lock() {
        guard.insert(repo_root.to_string(), resolved.clone());
    }

    resolved
}

pub fn member_email_for_repo(repo_root: Option<&str>) -> String {
    static GLOBAL: OnceLock<String> = OnceLock::new();
    static BY_REPO: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

    let global = GLOBAL
        .get_or_init(|| {
            run_git_capture(None, &["config", "--global", "user.email"])
                .unwrap_or_else(|| "(unknown)".to_string())
        })
        .clone();

    let Some(repo_root) = repo_root.map(str::trim).filter(|value| !value.is_empty()) else {
        return global;
    };

    let cache = BY_REPO.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache
        .lock()
        .ok()
        .and_then(|guard| guard.get(repo_root).cloned())
    {
        return cached;
    }

    let resolved = run_git_capture(Some(repo_root), &["config", "user.email"]).unwrap_or(global);
    if let Ok(mut guard) = cache.lock() {
        guard.insert(repo_root.to_string(), resolved.clone());
    }

    resolved
}

pub fn device_id() -> String {
    static DEVICE_ID: OnceLock<String> = OnceLock::new();

    DEVICE_ID
        .get_or_init(|| {
            let home = dirs::home_dir()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_else(|| "(unknown-home)".to_string());
            let hostname = std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    Command::new("hostname")
                        .output()
                        .ok()
                        .filter(|output| output.status.success())
                        .and_then(|output| String::from_utf8(output.stdout).ok())
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| "(unknown-host)".to_string());
            format!("device:{:x}", md5::compute(format!("{hostname}\n{home}")))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_https_remote() {
        assert_eq!(
            normalize_git_remote("https://github.com/PaceFlow/ai-engineering-analytics.git")
                .as_deref(),
            Some("github.com/PaceFlow/ai-engineering-analytics")
        );
    }

    #[test]
    fn normalizes_ssh_remote() {
        assert_eq!(
            normalize_git_remote("git@github.com:PaceFlow/ai-engineering-analytics.git").as_deref(),
            Some("github.com/PaceFlow/ai-engineering-analytics")
        );
    }

    #[test]
    fn device_id_is_stable_within_process() {
        let left = device_id();
        let right = device_id();
        assert_eq!(left, right);
        assert!(left.starts_with("device:"));
    }

    #[test]
    fn parses_github_repo_key() {
        assert_eq!(
            github_repo_from_repo_key("git:github.com/PaceFlow/ai-engineering-analytics"),
            Some((
                "PaceFlow".to_string(),
                "ai-engineering-analytics".to_string()
            ))
        );
    }

    #[test]
    fn rejects_non_github_repo_keys() {
        assert_eq!(
            github_repo_from_repo_key("git:gitlab.com/PaceFlow/ai-engineering-analytics"),
            None
        );
    }
}
