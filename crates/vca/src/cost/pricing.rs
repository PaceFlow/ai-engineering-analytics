//! Model pricing as versioned data.
//!
//! Prices live in `crates/vca/pricing/pricing.json` (embedded at build time)
//! and may be extended or overridden by `<data home>/.vca/pricing.json`.
//! Every estimate is an API list-price equivalent: it is not an invoice and
//! does not model subscriptions, discounts or tiered premiums.

use serde::Deserialize;
use std::sync::OnceLock;

const EMBEDDED_PRICING: &str = include_str!("../../pricing/pricing.json");

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TokenUsage {
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    /// All cache-write tokens, including `cache_creation_1h_tokens`.
    pub cache_creation_tokens: i64,
    pub output_tokens: i64,
    /// Reasoning tokens billed in addition to `output_tokens`. Callers pass 0
    /// when the provider already counts reasoning inside output.
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelPricing {
    pub input_per_million: f64,
    pub cached_input_per_million: f64,
    pub cache_creation_per_million: f64,
    pub output_per_million: f64,
}

/// How a model name was resolved to a price entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceMatch {
    Exact,
    Alias,
    Family,
}

impl PriceMatch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Alias => "alias",
            Self::Family => "family",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceEntry {
    pub id: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write_5m: Option<f64>,
    pub cache_write_1h: Option<f64>,
    pub effective_from: Option<String>,
}

impl PriceEntry {
    pub fn cache_write_5m(&self) -> f64 {
        self.cache_write_5m.unwrap_or(self.input * 1.25)
    }

    pub fn cache_write_1h(&self) -> f64 {
        self.cache_write_1h.unwrap_or(self.input * 2.0)
    }

    fn model_pricing(&self) -> ModelPricing {
        ModelPricing {
            input_per_million: self.input,
            cached_input_per_million: self.cache_read,
            cache_creation_per_million: self.cache_write_5m(),
            output_per_million: self.output,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PricingTable {
    pub version: String,
    #[serde(default)]
    pub note: Option<String>,
    pub models: Vec<PriceEntry>,
}

impl PricingTable {
    pub fn parse(json: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    /// Entries from `overrides` replace entries with the same id.
    fn merged_with(mut self, overrides: PricingTable) -> Self {
        for entry in overrides.models {
            self.models.retain(|existing| existing.id != entry.id);
            self.models.push(entry);
        }
        self.version = format!("{}+local:{}", self.version, overrides.version);
        self
    }

    pub fn lookup(&self, model_name: &str) -> Option<(&PriceEntry, PriceMatch)> {
        let normalized = normalize_model_name(model_name);
        if normalized.is_empty() {
            return None;
        }
        if let Some(entry) = self.models.iter().find(|entry| entry.id == normalized) {
            return Some((entry, PriceMatch::Exact));
        }
        if let Some(entry) = self
            .models
            .iter()
            .find(|entry| entry.aliases.contains(&normalized))
        {
            return Some((entry, PriceMatch::Alias));
        }
        // Family: the longest id (or alias) that prefixes the model name at a
        // separator boundary, e.g. `gpt-5.3-codex` -> `gpt-5`.
        self.models
            .iter()
            .flat_map(|entry| {
                std::iter::once(entry.id.as_str())
                    .chain(entry.aliases.iter().map(String::as_str))
                    .map(move |name| (entry, name))
            })
            .filter(|(_, name)| prefix_at_boundary(&normalized, name))
            .max_by_key(|(_, name)| name.len())
            .map(|(entry, _)| (entry, PriceMatch::Family))
    }

    pub fn estimate(&self, model_name: &str, usage: UsageForPricing) -> Option<PricedUsage> {
        let (entry, matched) = self.lookup(model_name)?;
        let write_5m = (usage.cache_write_tokens - usage.cache_write_1h_tokens).max(0);
        let cost = (usage.input_tokens.max(0) as f64 * entry.input
            + usage.cache_read_tokens.max(0) as f64 * entry.cache_read
            + write_5m as f64 * entry.cache_write_5m()
            + usage.cache_write_1h_tokens.max(0) as f64 * entry.cache_write_1h()
            + usage.billable_output_tokens().max(0) as f64 * entry.output)
            / 1_000_000.0;
        Some(PricedUsage {
            cost_usd: cost,
            price_id: entry.id.clone(),
            matched,
        })
    }
}

/// Token counts in the shape pricing needs.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct UsageForPricing {
    pub input_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cache_write_1h_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
    pub reasoning_in_output: bool,
}

impl UsageForPricing {
    pub fn billable_output_tokens(&self) -> i64 {
        if self.reasoning_in_output {
            self.output_tokens
        } else {
            self.output_tokens.saturating_add(self.reasoning_tokens)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PricedUsage {
    pub cost_usd: f64,
    pub price_id: String,
    pub matched: PriceMatch,
}

fn prefix_at_boundary(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(['-', '.', '_', ':', '@', '[']))
}

/// The active pricing table: embedded data plus an optional local override.
pub fn table() -> &'static PricingTable {
    static TABLE: OnceLock<PricingTable> = OnceLock::new();
    TABLE.get_or_init(|| {
        let embedded = PricingTable::parse(EMBEDDED_PRICING).expect("embedded pricing is valid");
        let override_path = crate::data_home::directory()
            .ok()
            .map(|dir| dir.join("pricing.json"));
        match override_path
            .and_then(|path| std::fs::read_to_string(path).ok())
            .and_then(|text| PricingTable::parse(&text).ok())
        {
            Some(overrides) => embedded.merged_with(overrides),
            None => embedded,
        }
    })
}

pub fn pricing_version() -> &'static str {
    &table().version
}

pub fn estimate_cost_usd(model_name: &str, usage: TokenUsage) -> Option<f64> {
    let pricing = pricing_for_model(model_name)?;
    Some(cost_from_pricing(pricing, usage))
}

pub fn cost_from_pricing(pricing: ModelPricing, usage: TokenUsage) -> f64 {
    let billable_output_tokens = usage.output_tokens.saturating_add(usage.reasoning_tokens);
    (usage.input_tokens.max(0) as f64 * pricing.input_per_million
        + usage.cached_input_tokens.max(0) as f64 * pricing.cached_input_per_million
        + usage.cache_creation_tokens.max(0) as f64 * pricing.cache_creation_per_million
        + billable_output_tokens.max(0) as f64 * pricing.output_per_million)
        / 1_000_000.0
}

pub fn pricing_for_model(model_name: &str) -> Option<ModelPricing> {
    table()
        .lookup(model_name)
        .map(|(entry, _)| entry.model_pricing())
}

/// Lower-case, drop provider prefixes, date suffixes and context markers, and
/// rewrite Cursor-style names (`claude-4.5-sonnet-thinking`) to API ids.
pub fn normalize_model_name(model_name: &str) -> String {
    let mut name = model_name.trim().to_ascii_lowercase();
    for prefix in [
        "codex/",
        "openai/",
        "claude/",
        "anthropic/",
        "opencode/",
        "cursor/",
        "us.anthropic.",
        "eu.anthropic.",
        "global.anthropic.",
        "anthropic.",
        "models/",
    ] {
        if let Some(rest) = name.strip_prefix(prefix) {
            name = rest.to_string();
        }
    }
    if let Some(index) = name.find('[') {
        name.truncate(index);
    }
    if let Some(index) = name.find('@') {
        name.truncate(index);
    }
    // Trailing date stamps such as `-20250929` or `-v1:0`.
    let parts: Vec<&str> = name.split('-').collect();
    if let Some(last) = parts.last()
        && last.len() == 8
        && last.chars().all(|c| c.is_ascii_digit())
    {
        name = parts[..parts.len() - 1].join("-");
    }
    for suffix in ["-thinking", "-max", "-fast", "-high", "-low"] {
        if let Some(rest) = name.strip_suffix(suffix) {
            name = rest.to_string();
        }
    }
    // claude-4.5-sonnet / claude-4-sonnet -> claude-sonnet-4-5 / claude-sonnet-4
    if let Some(rest) = name.strip_prefix("claude-") {
        let mut segments = rest.splitn(2, '-');
        let version = segments.next().unwrap_or_default();
        let remainder = segments.next().unwrap_or_default();
        if !version.is_empty()
            && version.chars().all(|c| c.is_ascii_digit() || c == '.')
            && let Some(family) =
                ["opus", "sonnet", "haiku", "fable", "mythos"]
                    .iter()
                    .find(|family| {
                        remainder == **family || remainder.starts_with(&format!("{family}-"))
                    })
        {
            let tail = remainder.strip_prefix(family).unwrap_or_default();
            name = format!("claude-{family}-{}{tail}", version.replace('.', "-"));
        }
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charges_reasoning_tokens_at_output_rate() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            cached_input_tokens: 500_000,
            cache_creation_tokens: 100_000,
            output_tokens: 10_000,
            reasoning_tokens: 5_000,
        };

        let cost = cost_from_pricing(
            ModelPricing {
                input_per_million: 2.0,
                cached_input_per_million: 0.2,
                cache_creation_per_million: 2.5,
                output_per_million: 10.0,
            },
            usage,
        );

        assert!((cost - 2.5).abs() < 0.000001);
    }

    #[test]
    fn leaves_unknown_models_unpriced() {
        let usage = TokenUsage {
            input_tokens: 1_000,
            output_tokens: 1_000,
            ..TokenUsage::default()
        };

        assert_eq!(estimate_cost_usd("mystery-provider/model", usage), None);
    }

    #[test]
    fn resolves_exact_alias_and_family_matches() {
        let table = table();
        assert_eq!(
            table
                .lookup("claude/claude-opus-5-5")
                .map(|(e, m)| (e.id.as_str(), m)),
            Some(("claude-opus-5-5", PriceMatch::Exact))
        );
        assert_eq!(
            table
                .lookup("claude-sonnet-4-5-20250929")
                .map(|(e, m)| (e.id.as_str(), m)),
            Some(("claude-sonnet-4-5", PriceMatch::Exact))
        );
        assert_eq!(
            table
                .lookup("codex/gpt-5.3-codex")
                .map(|(e, m)| (e.id.as_str(), m)),
            Some(("gpt-5", PriceMatch::Family))
        );
        assert_eq!(
            table.lookup("gpt-5.4-mini").map(|(e, _)| e.id.as_str()),
            Some("gpt-5.4-mini")
        );
        assert_eq!(
            table
                .lookup("claude-mythos-5-1")
                .map(|(e, m)| (e.id.as_str(), m)),
            Some(("claude-fable-5-1", PriceMatch::Alias))
        );
    }

    #[test]
    fn pricing_alias_matches_cursor_model_names() {
        assert_eq!(
            normalize_model_name("cursor/claude-4.5-sonnet-thinking"),
            "claude-sonnet-4-5"
        );
        assert_eq!(normalize_model_name("claude-4-sonnet"), "claude-sonnet-4");
        assert_eq!(
            table()
                .lookup("cursor/claude-4-sonnet")
                .map(|(e, _)| e.id.as_str()),
            Some("claude-sonnet-4")
        );
        assert!(table().lookup("cursor/default").is_none());
    }

    #[test]
    fn prices_one_hour_cache_writes_at_double_input() {
        let priced = table()
            .estimate(
                "claude-opus-5-5",
                UsageForPricing {
                    cache_write_tokens: 2_000_000,
                    cache_write_1h_tokens: 1_000_000,
                    ..UsageForPricing::default()
                },
            )
            .unwrap();
        // 1M at 5m (4 * 1.25) + 1M at 1h (4 * 2)
        assert!((priced.cost_usd - 13.0).abs() < 1e-9);
    }

    #[test]
    fn reasoning_inside_output_is_billed_once() {
        let base = UsageForPricing {
            output_tokens: 1_000_000,
            reasoning_tokens: 400_000,
            reasoning_in_output: true,
            ..UsageForPricing::default()
        };
        let once = table().estimate("gpt-5.5", base).unwrap().cost_usd;
        let twice = table()
            .estimate(
                "gpt-5.5",
                UsageForPricing {
                    reasoning_in_output: false,
                    ..base
                },
            )
            .unwrap()
            .cost_usd;
        assert!((once - 30.0).abs() < 1e-9);
        assert!((twice - 42.0).abs() < 1e-9);
    }
}
