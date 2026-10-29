//! Regenerate the bundled LiteLLM price snapshot.
//!
//! ```bash
//! cargo run -p vibe-coding-analytics --example update_litellm_prices
//! cargo run -p vibe-coding-analytics --example update_litellm_prices -- path/to/model_prices_and_context_window.json
//! ```
//!
//! Writes `crates/vca/pricing/litellm_prices.json` with one model per line so
//! price changes review as small diffs.

use anyhow::{Context, Result};
use serde_json::Value;
use vca::cost::litellm::{DEFAULT_URL, META_KEY, trim};

fn main() -> Result<()> {
    let (raw, source) = match std::env::args().nth(1) {
        Some(path) => (std::fs::read_to_string(&path)?, path),
        None => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let text = runtime.block_on(async {
                reqwest::get(DEFAULT_URL)
                    .await?
                    .error_for_status()?
                    .text()
                    .await
            })?;
            (text, DEFAULT_URL.to_string())
        }
    };
    let raw: Value = serde_json::from_str(&raw).context("invalid price map")?;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let source_url = if source.starts_with("http") {
        source.clone()
    } else {
        DEFAULT_URL.to_string()
    };
    let trimmed = trim(&raw, &now, &source_url)?;
    let map = trimmed.as_object().expect("trim returns an object");

    let mut lines = vec![format!(
        "{}:{}",
        serde_json::to_string(META_KEY)?,
        serde_json::to_string(&map[META_KEY])?
    )];
    for (name, entry) in map.iter().filter(|(name, _)| *name != META_KEY) {
        lines.push(format!(
            "{}:{}",
            serde_json::to_string(name)?,
            serde_json::to_string(entry)?
        ));
    }
    let out = format!("{{\n{}\n}}\n", lines.join(",\n"));
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/pricing/litellm_prices.json");
    std::fs::write(path, out)?;
    println!("wrote {} models from {source} to {path}", map.len() - 1);
    Ok(())
}
