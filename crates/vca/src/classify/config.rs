//! Classifier settings from `<data home>/classifier.json` and the OpenRouter
//! key (env `OPENROUTER_API_KEY`, else a saved 0600 file).
//!
//! ```json
//! {"enabled": true, "default_level": "conversation", "monthly_budget_usd": 2.0,
//!  "deny_projects": ["*/client-*"], "provider": {"zdr": true, "allow_fallbacks": false}}
//! ```

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::classify::state::Level;

pub const DEFAULT_MODEL: &str = "typesafe/jev-1.13";
pub const DEFAULT_ENDPOINT: &str = "https://openrouter.ai/api/alpha/decisions";
pub const KEY_ENV: &str = "OPENROUTER_API_KEY";
const CONFIG_FILE: &str = "classifier.json";
const KEY_FILE: &str = "openrouter_token";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClassifierConfig {
    /// Global opt-in for automatic external classification of all ingested projects.
    pub enabled: bool,
    pub model: String,
    pub endpoint: String,
    pub default_level: String,
    pub monthly_budget_usd: f64,
    pub project_monthly_budget_usd: f64,
    pub max_run_usd: f64,
    pub backfill_days: u32,
    /// USD per million input tokens, used for estimates only; actual cost
    /// comes from each response's `usage.cost`.
    pub input_price_per_million: f64,
    pub allow_projects: Vec<String>,
    pub deny_projects: Vec<String>,
    pub concurrency: usize,
    pub automatic_dispatch_seconds: u64,
    pub provider: ProviderPrefs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderPrefs {
    pub zdr: bool,
    pub allow_fallbacks: bool,
}

impl Default for ProviderPrefs {
    fn default() -> Self {
        Self {
            zdr: true,
            allow_fallbacks: false,
        }
    }
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            model: DEFAULT_MODEL.into(),
            endpoint: DEFAULT_ENDPOINT.into(),
            default_level: Level::Conversation.as_str().into(),
            monthly_budget_usd: 2.0,
            project_monthly_budget_usd: 2.0,
            max_run_usd: 0.50,
            backfill_days: 56,
            input_price_per_million: 0.042,
            allow_projects: Vec::new(),
            deny_projects: Vec::new(),
            concurrency: 4,
            automatic_dispatch_seconds: 60,
            provider: ProviderPrefs::default(),
        }
    }
}

impl ClassifierConfig {
    pub fn path() -> Result<PathBuf> {
        Ok(crate::data_home::directory()?.join(CONFIG_FILE))
    }

    pub fn load() -> Result<Self> {
        let path = Self::path()?;
        match fs::read_to_string(&path) {
            Ok(text) => {
                serde_json::from_str(&text).with_context(|| format!("invalid {}", path.display()))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn save(&self) -> Result<PathBuf> {
        let path = Self::path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, serde_json::to_string_pretty(self)? + "\n")?;
        Ok(path)
    }

    pub fn level(&self) -> Level {
        Level::parse(&self.default_level).unwrap_or(Level::Conversation)
    }

    pub fn estimate_usd(&self, tokens: usize) -> f64 {
        tokens as f64 * self.input_price_per_million / 1_000_000.0
    }

    /// Whether a project (repo root) may be sent. Deny wins; an empty allow
    /// list allows everything not denied.
    pub fn project_allowed(&self, repo_root: Option<&str>) -> bool {
        let root = repo_root.unwrap_or("");
        if self
            .deny_projects
            .iter()
            .any(|pattern| glob_match(pattern, root))
        {
            return false;
        }
        self.allow_projects.is_empty()
            || self
                .allow_projects
                .iter()
                .any(|pattern| glob_match(pattern, root))
    }
}

/// `*` matches any run of characters; everything else is literal.
pub fn glob_match(pattern: &str, value: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }
    let mut rest = value;
    for (index, part) in parts.iter().enumerate() {
        if index == 0 {
            match rest.strip_prefix(part) {
                Some(after) => rest = after,
                None => return false,
            }
        } else if index == parts.len() - 1 {
            return rest.ends_with(part);
        } else {
            match rest.find(part) {
                Some(position) => rest = &rest[position + part.len()..],
                None => return false,
            }
        }
    }
    true
}

// ── API key ──────────────────────────────────────────────────────────────

pub fn key_path() -> Result<PathBuf> {
    Ok(crate::data_home::directory()?.join(KEY_FILE))
}

pub fn api_key() -> Result<Option<String>> {
    if let Ok(value) = std::env::var(KEY_ENV)
        && !value.trim().is_empty()
    {
        return Ok(Some(value.trim().to_string()));
    }
    match fs::read_to_string(key_path()?) {
        Ok(text) if !text.trim().is_empty() => Ok(Some(text.trim().to_string())),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn save_api_key(key: &str) -> Result<PathBuf> {
    let key = key.trim();
    if key.is_empty() {
        return Err(anyhow!("OpenRouter key cannot be empty"));
    }
    let path = key_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    writeln!(file, "{key}")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(path)
}

pub fn delete_api_key() -> Result<bool> {
    match fs::remove_file(key_path()?) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_globs_deny_before_allow() {
        let config = ClassifierConfig {
            allow_projects: vec!["/work/*".into()],
            deny_projects: vec!["*/secret-*".into()],
            ..ClassifierConfig::default()
        };
        assert!(config.project_allowed(Some("/work/vca")));
        assert!(!config.project_allowed(Some("/work/secret-client")));
        assert!(!config.project_allowed(Some("/home/other")));
        assert!(ClassifierConfig::default().project_allowed(None));
    }

    #[test]
    fn config_defaults_are_off_and_pinned() {
        let config: ClassifierConfig = serde_json::from_str("{}").unwrap();
        assert!(!config.enabled);
        assert_eq!(config.model, DEFAULT_MODEL);
        assert!(config.provider.zdr && !config.provider.allow_fallbacks);
    }
}
