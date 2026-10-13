//! LiteLLM's public model price map, the same source LiteLLM itself loads
//! (`model_prices_and_context_window.json` in BerriAI/litellm).
//!
//! A trimmed snapshot is embedded at build time. `vca ingest` refreshes a
//! cached copy under `<data home>/.vca/pricing/` at most once a day and falls
//! back to the cache or the snapshot when offline, so pricing never blocks
//! ingest.

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value, json};
use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
/// Overrides the download URL.
pub const URL_ENV: &str = "VCA_PRICING_URL";
/// `1` skips the network (tests, air-gapped machines).
pub const OFFLINE_ENV: &str = "VCA_PRICING_OFFLINE";
pub const META_KEY: &str = "_vca_meta";
const MAX_AGE_HOURS: i64 = 24;
const MIN_ENTRIES: usize = 500;
const REQUIRED_MODELS: [&str; 2] = ["claude-sonnet-4-5", "gpt-5"];

pub const BUNDLED: &str = include_str!("../../pricing/litellm_prices.json");

pub fn cache_path() -> Result<PathBuf> {
    Ok(crate::data_home::directory()?
        .join("pricing")
        .join("litellm_prices.json"))
}

/// Keep chat-capable entries and only the fields cost calculation reads.
fn keep_field(key: &str) -> bool {
    if key == "litellm_provider" {
        return true;
    }
    let base = [
        "input_cost_per_token",
        "output_cost_per_token",
        "cache_read_input_token_cost",
        "cache_creation_input_token_cost",
        "output_cost_per_reasoning_token",
    ];
    base.iter().any(|prefix| {
        key.strip_prefix(prefix).is_some_and(|rest| {
            let rest = rest.strip_prefix("_above_1hr").unwrap_or(rest);
            rest.is_empty()
                || rest
                    .strip_prefix("_above_")
                    .and_then(|r| r.strip_suffix("_tokens"))
                    .is_some_and(|n| {
                        let n = n.strip_suffix('k').unwrap_or(n);
                        !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())
                    })
        })
    })
}

/// Trim LiteLLM's raw map to priced chat models and validate it.
pub fn trim(raw: &Value, fetched_at: &str, source_url: &str) -> Result<Value> {
    let map = raw
        .as_object()
        .ok_or_else(|| anyhow!("price map is not a JSON object"))?;
    let mut out = Map::new();
    for (name, entry) in map {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        if name == "sample_spec" || name.starts_with('_') {
            continue;
        }
        let mode = entry.get("mode").and_then(Value::as_str);
        if !matches!(mode, None | Some("chat" | "responses" | "completion")) {
            continue;
        }
        let priced = |key: &str| entry.get(key).and_then(Value::as_f64).is_some();
        if !priced("input_cost_per_token") || !priced("output_cost_per_token") {
            continue;
        }
        let kept: Map<String, Value> = entry
            .iter()
            .filter(|(key, value)| keep_field(key) && (value.is_number() || value.is_string()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        out.insert(name.clone(), Value::Object(kept));
    }
    if out.len() < MIN_ENTRIES {
        bail!("price map has only {} priced chat models", out.len());
    }
    for model in REQUIRED_MODELS {
        if !out.contains_key(model) {
            bail!("price map is missing {model}");
        }
    }
    let entries = out.len();
    out.insert(
        META_KEY.into(),
        json!({"source_url": source_url, "fetched_at": fetched_at, "entries": entries}),
    );
    Ok(Value::Object(out))
}

/// `fetched_at` of a trimmed map, if present.
pub fn fetched_at(map: &Value) -> Option<String> {
    map.get(META_KEY)?
        .get("fetched_at")?
        .as_str()
        .map(str::to_string)
}

#[derive(Debug, Clone, PartialEq)]
pub enum RefreshOutcome {
    Downloaded {
        entries: usize,
    },
    /// The cached copy is younger than a day.
    Fresh,
    /// Offline mode or no data home.
    Skipped(String),
    /// Download or validation failed; the cache or snapshot is used.
    Failed(String),
}

impl RefreshOutcome {
    pub fn message(&self) -> Option<String> {
        match self {
            Self::Downloaded { entries } => Some(format!(
                "pricing: refreshed LiteLLM model prices ({entries} models)"
            )),
            Self::Fresh | Self::Skipped(_) => None,
            Self::Failed(reason) => Some(format!(
                "pricing: using cached/bundled LiteLLM prices ({reason})"
            )),
        }
    }
}

fn offline() -> bool {
    std::env::var(OFFLINE_ENV).is_ok_and(|v| !v.is_empty() && v != "0")
}

fn cache_age_hours(path: &PathBuf) -> Option<i64> {
    let text = std::fs::read_to_string(path).ok()?;
    let map: Value = serde_json::from_str(&text).ok()?;
    let at = chrono::DateTime::parse_from_rfc3339(&fetched_at(&map)?).ok()?;
    Some((chrono::Utc::now() - at.with_timezone(&chrono::Utc)).num_hours())
}

/// Download the map when the cache is missing or older than a day (or when
/// `force`), then reload the pricing table. Never returns an error.
pub fn refresh_price_map(force: bool) -> RefreshOutcome {
    if offline() {
        return RefreshOutcome::Skipped(format!("{OFFLINE_ENV} is set"));
    }
    let path = match cache_path() {
        Ok(path) => path,
        Err(error) => return RefreshOutcome::Skipped(error.to_string()),
    };
    if !force && cache_age_hours(&path).is_some_and(|age| (0..MAX_AGE_HOURS).contains(&age)) {
        return RefreshOutcome::Fresh;
    }
    let url = std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_string());
    match download(&url).and_then(|raw| {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let trimmed = trim(&raw, &now, &url)?;
        write_atomically(&path, &serde_json::to_string(&trimmed)?)?;
        Ok(trimmed
            .as_object()
            .map(|m| m.len().saturating_sub(1))
            .unwrap_or(0))
    }) {
        Ok(entries) => {
            super::pricing::reload();
            RefreshOutcome::Downloaded { entries }
        }
        Err(error) => RefreshOutcome::Failed(format!("{error:#}")),
    }
}

fn download(url: &str) -> Result<Value> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(20));
    if url.starts_with("http://127.0.0.1") || url.starts_with("http://localhost") {
        builder = builder.no_proxy();
    }
    let client = builder.build()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let response = client.get(url).send().await.context("download failed")?;
        let status = response.status();
        if !status.is_success() {
            bail!("HTTP {status}");
        }
        let text = response.text().await?;
        serde_json::from_str(&text).context("invalid JSON")
    })
}

fn write_atomically(path: &PathBuf, text: &str) -> Result<()> {
    let dir = path.parent().ok_or_else(|| anyhow!("no cache directory"))?;
    std::fs::create_dir_all(dir)?;
    let temp = dir.join(format!(".litellm_prices.{}.tmp", std::process::id()));
    std::fs::write(&temp, text)?;
    std::fs::rename(&temp, path)?;
    Ok(())
}

/// The active map: the cached download when it parses, else the snapshot.
/// Returns the JSON text and a label for where it came from.
pub fn active_map() -> (String, &'static str) {
    if let Ok(path) = cache_path()
        && let Ok(text) = std::fs::read_to_string(&path)
        && serde_json::from_str::<Value>(&text).is_ok_and(|v| v.get(META_KEY).is_some())
    {
        return (text, "cached");
    }
    (BUNDLED.to_string(), "bundled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    pub(crate) fn raw_fixture(extra: usize) -> Value {
        let mut map = Map::new();
        map.insert("sample_spec".into(), json!({"input_cost_per_token": 0.0}));
        map.insert(
            "claude-sonnet-4-5".into(),
            json!({"input_cost_per_token": 3e-6, "output_cost_per_token": 1.5e-5,
                   "cache_read_input_token_cost": 3e-7, "litellm_provider": "anthropic",
                   "mode": "chat", "supports_vision": true}),
        );
        map.insert(
            "gpt-5".into(),
            json!({"input_cost_per_token": 1.25e-6, "output_cost_per_token": 1e-5,
                   "input_cost_per_token_batches": 6e-7, "mode": "chat"}),
        );
        map.insert(
            "dall-e-3".into(),
            json!({"input_cost_per_token": 1e-6, "output_cost_per_token": 1e-6, "mode": "image_generation"}),
        );
        for index in 0..extra {
            map.insert(
                format!("model-{index}"),
                json!({"input_cost_per_token": 1e-6, "output_cost_per_token": 2e-6, "mode": "chat"}),
            );
        }
        Value::Object(map)
    }

    #[test]
    fn trim_keeps_priced_chat_models_and_cost_fields() {
        let trimmed = trim(&raw_fixture(600), "2026-10-13T00:00:00Z", "x").unwrap();
        let map = trimmed.as_object().unwrap();
        assert!(map.contains_key("gpt-5") && !map.contains_key("dall-e-3"));
        assert!(!map.contains_key("sample_spec"));
        let sonnet = &map["claude-sonnet-4-5"];
        assert!(sonnet.get("supports_vision").is_none() && sonnet.get("mode").is_none());
        assert_eq!(sonnet["litellm_provider"], "anthropic");
        assert!(map["gpt-5"].get("input_cost_per_token_batches").is_none());
        assert_eq!(
            fetched_at(&trimmed).as_deref(),
            Some("2026-10-13T00:00:00Z")
        );
        assert!(keep_field("input_cost_per_token_above_272k_tokens"));
        assert!(keep_field(
            "cache_creation_input_token_cost_above_1hr_above_200k_tokens"
        ));
        assert!(!keep_field(
            "input_cost_per_token_above_272k_tokens_priority"
        ));
    }

    #[test]
    fn trim_rejects_small_or_incomplete_maps() {
        assert!(trim(&raw_fixture(10), "t", "x").is_err());
        let mut raw = raw_fixture(600);
        raw.as_object_mut().unwrap().remove("gpt-5");
        assert!(trim(&raw, "t", "x").is_err());
    }

    #[test]
    fn bundled_snapshot_is_valid() {
        let map: Value = serde_json::from_str(BUNDLED).unwrap();
        assert!(fetched_at(&map).is_some());
        assert!(map.as_object().unwrap().len() > MIN_ENTRIES);
    }

    fn serve(status: u16, body: String) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/prices.json", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let mut stream = reader.into_inner();
            write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (url, handle)
    }

    #[test]
    fn refresh_downloads_caches_and_falls_back() -> Result<()> {
        let _lock = crate::test_support::lock_env();
        let home = tempfile::tempdir()?;
        let _home = crate::test_support::ScopedEnvVar::set("VCA_HOME", home.path());
        let _offline = crate::test_support::ScopedEnvVar::unset(OFFLINE_ENV);

        let (url, server) = serve(200, raw_fixture(600).to_string());
        let _url = crate::test_support::ScopedEnvVar::set(URL_ENV, &url);
        assert_eq!(
            refresh_price_map(false),
            RefreshOutcome::Downloaded { entries: 602 }
        );
        server.join().unwrap();
        assert_eq!(active_map().1, "cached");
        // A day-fresh cache is not downloaded again (no server is listening).
        assert_eq!(refresh_price_map(false), RefreshOutcome::Fresh);

        let (url, server) = serve(500, "{}".into());
        let _url = crate::test_support::ScopedEnvVar::set(URL_ENV, &url);
        assert!(matches!(refresh_price_map(true), RefreshOutcome::Failed(_)));
        server.join().unwrap();
        assert_eq!(active_map().1, "cached", "failed refresh keeps the cache");

        let (url, server) = serve(200, raw_fixture(5).to_string());
        let _url = crate::test_support::ScopedEnvVar::set(URL_ENV, &url);
        assert!(matches!(refresh_price_map(true), RefreshOutcome::Failed(_)));
        server.join().unwrap();

        let _offline = crate::test_support::ScopedEnvVar::set(OFFLINE_ENV, "1");
        assert!(matches!(
            refresh_price_map(true),
            RefreshOutcome::Skipped(_)
        ));
        super::super::pricing::reload();
        Ok(())
    }
}
