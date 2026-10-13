//! Model pricing from LiteLLM's public model price map.
//!
//! The map (see [`super::litellm`]) is the same data LiteLLM uses, and costs
//! follow LiteLLM's rules: per-token rates, long-context tiers chosen by the
//! request's prompt size (`*_above_{N}_tokens`, strictly greater than), cache
//! rates falling back to the input rate, 1-hour cache writes falling back to
//! the 5-minute rate, and reasoning billed at its own rate when it is not
//! already inside output. A local `<data home>/.vca/pricing.json` in the
//! older `{"models": [{"id", "aliases", "input", ...}]}` shape (USD per
//! million tokens) overrides or adds models. Every estimate is an API
//! list-price equivalent, not an invoice.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// How a model name was resolved to a price entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceMatch {
    /// The name (or its provider-stripped form) is a key of the map.
    Exact,
    /// Found after normalization (dates, Cursor-style names) or a local alias.
    Alias,
    /// The longest first-party key that prefixes the name.
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

/// Rates as published, USD per million tokens; `None` = not published.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct RawRates {
    input: Option<f64>,
    output: Option<f64>,
    cache_read: Option<f64>,
    cache_write: Option<f64>,
    cache_write_1h: Option<f64>,
    reasoning: Option<f64>,
}

/// Resolved rates for one request, USD per million tokens.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rates {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write_5m: f64,
    pub cache_write_1h: f64,
    pub reasoning: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceEntry {
    pub id: String,
    pub provider: Option<String>,
    pub aliases: Vec<String>,
    base: RawRates,
    /// (prompt-token threshold, rates above it), ascending.
    tiers: Vec<(i64, RawRates)>,
}

impl PriceEntry {
    /// Rates for a request with `prompt_tokens` (input + cache read + cache
    /// write), following LiteLLM: the highest tier strictly exceeded wins.
    pub fn rates(&self, prompt_tokens: i64) -> Rates {
        let tier = self
            .tiers
            .iter()
            .rev()
            .find(|(threshold, _)| prompt_tokens > *threshold)
            .map(|(_, rates)| rates);
        let pick = |f: fn(&RawRates) -> Option<f64>| tier.and_then(f).or_else(|| f(&self.base));
        let input = pick(|r| r.input).unwrap_or(0.0);
        let output = pick(|r| r.output).unwrap_or(0.0);
        let cache_write_5m = pick(|r| r.cache_write).unwrap_or(input);
        Rates {
            input,
            output,
            cache_read: pick(|r| r.cache_read).unwrap_or(input),
            cache_write_5m,
            cache_write_1h: pick(|r| r.cache_write_1h).unwrap_or(cache_write_5m),
            reasoning: pick(|r| r.reasoning).unwrap_or(output),
        }
    }

    pub fn tier_thresholds(&self) -> Vec<i64> {
        self.tiers.iter().map(|(threshold, _)| *threshold).collect()
    }

    fn from_litellm(id: &str, entry: &serde_json::Map<String, Value>) -> Option<Self> {
        let per_million = |key: &str| entry.get(key).and_then(Value::as_f64).map(|v| v * 1e6);
        let rates = |suffix: &str| RawRates {
            input: per_million(&format!("input_cost_per_token{suffix}")),
            output: per_million(&format!("output_cost_per_token{suffix}")),
            cache_read: per_million(&format!("cache_read_input_token_cost{suffix}")),
            cache_write: per_million(&format!("cache_creation_input_token_cost{suffix}")),
            cache_write_1h: per_million(&format!(
                "cache_creation_input_token_cost_above_1hr{suffix}"
            )),
            reasoning: per_million(&format!("output_cost_per_reasoning_token{suffix}")),
        };
        let base = rates("");
        base.input?;
        base.output?;
        let mut tiers: Vec<(i64, RawRates)> = entry
            .keys()
            .filter_map(|key| {
                let threshold = key
                    .strip_prefix("input_cost_per_token_above_")?
                    .strip_suffix("_tokens")?;
                let (digits, scale) = match threshold.strip_suffix('k') {
                    Some(digits) => (digits, 1_000),
                    None => (threshold, 1),
                };
                let value: i64 = digits.parse().ok()?;
                Some((value * scale, rates(&format!("_above_{threshold}_tokens"))))
            })
            .collect();
        tiers.sort_by_key(|(threshold, _)| *threshold);
        Some(Self {
            id: id.to_string(),
            provider: entry
                .get("litellm_provider")
                .and_then(Value::as_str)
                .map(str::to_string),
            aliases: Vec::new(),
            base,
            tiers,
        })
    }
}

/// Local override entry, USD per million tokens.
#[derive(Debug, Clone, Deserialize)]
struct OverrideEntry {
    id: String,
    #[serde(default)]
    aliases: Vec<String>,
    input: f64,
    output: f64,
    cache_read: Option<f64>,
    cache_write_5m: Option<f64>,
    cache_write_1h: Option<f64>,
    reasoning: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct OverrideFile {
    #[serde(default)]
    version: Option<String>,
    models: Vec<OverrideEntry>,
}

impl From<OverrideEntry> for PriceEntry {
    fn from(entry: OverrideEntry) -> Self {
        Self {
            id: normalize_model_name(&entry.id),
            provider: Some("local".into()),
            aliases: entry
                .aliases
                .iter()
                .map(|a| normalize_model_name(a))
                .collect(),
            base: RawRates {
                input: Some(entry.input),
                output: Some(entry.output),
                cache_read: entry.cache_read,
                cache_write: entry.cache_write_5m,
                cache_write_1h: entry.cache_write_1h,
                reasoning: entry.reasoning,
            },
            tiers: Vec::new(),
        }
    }
}

/// Providers whose bare model names may serve as a family fallback.
const FAMILY_PROVIDERS: [&str; 6] = [
    "anthropic",
    "openai",
    "gemini",
    "xai",
    "deepseek",
    "mistral",
];

#[derive(Debug, Clone)]
pub struct PricingTable {
    pub version: String,
    /// `cached` (downloaded) or `bundled`.
    pub source: String,
    pub fetched_at: Option<String>,
    entries: HashMap<String, PriceEntry>,
    overrides: Vec<PriceEntry>,
    /// Bare first-party keys, longest first, for family matching.
    family_keys: Vec<String>,
}

impl PricingTable {
    /// Parse a (trimmed or raw) LiteLLM price map.
    pub fn from_litellm(json: &str, source: &str) -> anyhow::Result<Self> {
        let map: Value = serde_json::from_str(json)?;
        let object = map
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("price map is not an object"))?;
        let entries: HashMap<String, PriceEntry> = object
            .iter()
            .filter(|(key, _)| !key.starts_with('_') && *key != "sample_spec")
            .filter_map(|(key, value)| {
                PriceEntry::from_litellm(key, value.as_object()?)
                    .map(|e| (key.to_ascii_lowercase(), e))
            })
            .collect();
        let mut family_keys: Vec<String> = entries
            .iter()
            .filter(|(key, entry)| {
                !key.contains('/')
                    && entry
                        .provider
                        .as_deref()
                        .is_some_and(|p| FAMILY_PROVIDERS.contains(&p))
            })
            .map(|(key, _)| key.clone())
            .collect();
        family_keys.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
        let fetched_at = super::litellm::fetched_at(&map);
        let version = format!(
            "litellm@{}",
            fetched_at
                .as_deref()
                .map(|at| &at[..at.len().min(10)])
                .unwrap_or("unknown")
        );
        Ok(Self {
            version,
            source: source.to_string(),
            fetched_at,
            entries,
            overrides: Vec::new(),
            family_keys,
        })
    }

    /// Add local overrides (`{"models": [...]}`, USD per million tokens).
    pub fn with_overrides(mut self, json: &str) -> anyhow::Result<Self> {
        let file: OverrideFile = serde_json::from_str(json)?;
        self.overrides = file.models.into_iter().map(PriceEntry::from).collect();
        self.version = format!(
            "{}+local{}",
            self.version,
            file.version.map(|v| format!(":{v}")).unwrap_or_default()
        );
        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn lookup(&self, model_name: &str) -> Option<(&PriceEntry, PriceMatch)> {
        let raw = model_name.trim().to_ascii_lowercase();
        let normalized = normalize_model_name(model_name);
        if normalized.is_empty() {
            return None;
        }
        if let Some(entry) = self
            .overrides
            .iter()
            .find(|entry| entry.id == normalized || entry.aliases.contains(&normalized))
        {
            return Some((entry, PriceMatch::Alias));
        }
        for candidate in [raw.clone(), strip_prefixes(&raw)] {
            if let Some(entry) = self.entries.get(&candidate) {
                return Some((entry, PriceMatch::Exact));
            }
        }
        if let Some(entry) = self.entries.get(&normalized) {
            return Some((entry, PriceMatch::Alias));
        }
        self.family_keys
            .iter()
            .find(|key| prefix_at_boundary(&normalized, key))
            .and_then(|key| self.entries.get(key))
            .map(|entry| (entry, PriceMatch::Family))
    }

    pub fn estimate(&self, model_name: &str, usage: UsageForPricing) -> Option<PricedUsage> {
        let (entry, matched) = self.lookup(model_name)?;
        // Tiers depend on one request's prompt size; aggregated rows use base rates.
        let prompt = if usage.per_call {
            usage.input_tokens.max(0)
                + usage.cache_read_tokens.max(0)
                + usage.cache_write_tokens.max(0)
        } else {
            0
        };
        let rates = entry.rates(prompt);
        let write_5m = (usage.cache_write_tokens - usage.cache_write_1h_tokens).max(0);
        let reasoning = if usage.reasoning_in_output {
            0.0
        } else {
            usage.reasoning_tokens.max(0) as f64 * rates.reasoning
        };
        let cost = (usage.input_tokens.max(0) as f64 * rates.input
            + usage.cache_read_tokens.max(0) as f64 * rates.cache_read
            + write_5m as f64 * rates.cache_write_5m
            + usage.cache_write_1h_tokens.max(0) as f64 * rates.cache_write_1h
            + usage.output_tokens.max(0) as f64 * rates.output
            + reasoning)
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
    /// One API request, so long-context tiers apply. False for rows that sum
    /// several requests (legacy session totals).
    pub per_call: bool,
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

fn load() -> PricingTable {
    let (json, source) = super::litellm::active_map();
    let table = PricingTable::from_litellm(&json, source)
        .or_else(|_| PricingTable::from_litellm(super::litellm::BUNDLED, "bundled"))
        .expect("bundled LiteLLM price map is valid");
    let override_text = crate::data_home::directory()
        .ok()
        .and_then(|dir| std::fs::read_to_string(dir.join("pricing.json")).ok());
    match override_text {
        Some(text) => match table.clone().with_overrides(&text) {
            Ok(with) => with,
            Err(_) => table,
        },
        None => table,
    }
}

static TABLE: RwLock<Option<Arc<PricingTable>>> = RwLock::new(None);

/// The active pricing table (LiteLLM map plus local overrides).
pub fn table() -> Arc<PricingTable> {
    if let Some(table) = TABLE.read().ok().and_then(|guard| guard.clone()) {
        return table;
    }
    let mut guard = TABLE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.get_or_insert_with(|| Arc::new(load())).clone()
}

/// Drop the loaded table so the next use reads the refreshed map.
pub fn reload() {
    if let Ok(mut guard) = TABLE.write() {
        *guard = None;
    }
}

pub fn pricing_version() -> String {
    table().version.clone()
}

const PROVIDER_PREFIXES: [&str; 11] = [
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
];

/// Lower-case and drop provider prefixes, keeping dates and suffixes.
fn strip_prefixes(name: &str) -> String {
    let mut name = name.trim().to_ascii_lowercase();
    for prefix in PROVIDER_PREFIXES {
        if let Some(rest) = name.strip_prefix(prefix) {
            name = rest.to_string();
        }
    }
    name
}

/// Lower-case, drop provider prefixes, date suffixes and context markers, and
/// rewrite Cursor-style names (`claude-4.5-sonnet-thinking`) to API ids.
pub fn normalize_model_name(model_name: &str) -> String {
    let mut name = strip_prefixes(model_name);
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

    fn fixture() -> PricingTable {
        let json = r#"{
          "_vca_meta": {"fetched_at": "2026-10-13T08:00:00Z"},
          "claude-sonnet-4-5": {"input_cost_per_token": 3e-6, "output_cost_per_token": 1.5e-5,
             "cache_read_input_token_cost": 3e-7, "cache_creation_input_token_cost": 3.75e-6,
             "cache_creation_input_token_cost_above_1hr": 6e-6,
             "input_cost_per_token_above_200k_tokens": 6e-6, "output_cost_per_token_above_200k_tokens": 2.25e-5,
             "cache_read_input_token_cost_above_200k_tokens": 6e-7,
             "litellm_provider": "anthropic"},
          "claude-sonnet-4-5-20250929": {"input_cost_per_token": 3e-6, "output_cost_per_token": 1.5e-5, "litellm_provider": "anthropic"},
          "claude-opus-5-5": {"input_cost_per_token": 4e-6, "output_cost_per_token": 2e-5,
             "cache_read_input_token_cost": 2e-7, "cache_creation_input_token_cost": 5e-6,
             "cache_creation_input_token_cost_above_1hr": 8e-6, "litellm_provider": "anthropic"},
          "gpt-5": {"input_cost_per_token": 1.25e-6, "output_cost_per_token": 1e-5,
             "cache_read_input_token_cost": 1.25e-7, "litellm_provider": "openai"},
          "gpt-5.4": {"input_cost_per_token": 2.5e-6, "output_cost_per_token": 1.5e-5,
             "input_cost_per_token_above_272k_tokens": 5e-6, "output_cost_per_token_above_272k_tokens": 2.25e-5,
             "litellm_provider": "openai"},
          "o-reasoner": {"input_cost_per_token": 1e-6, "output_cost_per_token": 4e-6,
             "output_cost_per_reasoning_token": 8e-6, "litellm_provider": "openai"},
          "openrouter/anthropic/claude-sonnet-4.5": {"input_cost_per_token": 3e-6, "output_cost_per_token": 1.5e-5, "litellm_provider": "openrouter"}
        }"#;
        PricingTable::from_litellm(json, "test").unwrap()
    }

    fn per_call(input: i64) -> UsageForPricing {
        UsageForPricing {
            input_tokens: input,
            per_call: true,
            ..UsageForPricing::default()
        }
    }

    #[test]
    fn resolves_exact_dated_prefixed_alias_and_family_names() {
        let table = fixture();
        let found = |name: &str| table.lookup(name).map(|(e, m)| (e.id.clone(), m));
        assert_eq!(
            found("claude-sonnet-4-5"),
            Some(("claude-sonnet-4-5".into(), PriceMatch::Exact))
        );
        assert_eq!(
            found("claude-sonnet-4-5-20250929"),
            Some(("claude-sonnet-4-5-20250929".into(), PriceMatch::Exact))
        );
        assert_eq!(
            found("anthropic/claude-opus-5-5"),
            Some(("claude-opus-5-5".into(), PriceMatch::Exact))
        );
        assert_eq!(
            found("openrouter/anthropic/claude-sonnet-4.5"),
            Some((
                "openrouter/anthropic/claude-sonnet-4.5".into(),
                PriceMatch::Exact
            ))
        );
        assert_eq!(
            found("cursor/claude-4.5-sonnet-thinking"),
            Some(("claude-sonnet-4-5".into(), PriceMatch::Alias))
        );
        assert_eq!(
            found("codex/gpt-5.3-codex"),
            Some(("gpt-5".into(), PriceMatch::Family))
        );
        assert_eq!(
            found("gpt-5.4-mini"),
            Some(("gpt-5.4".into(), PriceMatch::Family))
        );
        assert!(table.lookup("cursor/default").is_none());
        assert!(table.lookup("mystery-provider/model").is_none());
        assert_eq!(table.version, "litellm@2026-10-13");
    }

    #[test]
    fn long_context_tier_applies_strictly_above_threshold_per_call() {
        let table = fixture();
        let at = table
            .estimate("gpt-5.4", per_call(272_000))
            .unwrap()
            .cost_usd;
        let above = table
            .estimate("gpt-5.4", per_call(272_001))
            .unwrap()
            .cost_usd;
        assert!((at - 272_000.0 * 2.5 / 1e6).abs() < 1e-9);
        assert!((above - 272_001.0 * 5.0 / 1e6).abs() < 1e-9);
        // Summed rows never cross into a tier.
        let summed = table
            .estimate(
                "gpt-5.4",
                UsageForPricing {
                    per_call: false,
                    ..per_call(1_000_000)
                },
            )
            .unwrap()
            .cost_usd;
        assert!((summed - 2.5).abs() < 1e-9);
        // Cache reads count toward the prompt and take the tier's cache rate.
        let sonnet = table
            .estimate(
                "claude-sonnet-4-5",
                UsageForPricing {
                    input_tokens: 1_000,
                    cache_read_tokens: 250_000,
                    output_tokens: 1_000,
                    per_call: true,
                    ..UsageForPricing::default()
                },
            )
            .unwrap()
            .cost_usd;
        assert!((sonnet - (1_000.0 * 6.0 + 250_000.0 * 0.6 + 1_000.0 * 22.5) / 1e6).abs() < 1e-9);
    }

    #[test]
    fn missing_rates_fall_back_like_litellm() {
        let rates = fixture().lookup("gpt-5.4").unwrap().0.rates(0);
        assert_eq!(
            rates.cache_read, rates.input,
            "no cache-read rate → input rate"
        );
        assert_eq!(rates.cache_write_5m, rates.input);
        assert_eq!(rates.cache_write_1h, rates.cache_write_5m);
        assert_eq!(rates.reasoning, rates.output);
        let opus = fixture().lookup("claude-opus-5-5").unwrap().0.rates(0);
        assert!(
            (opus.cache_write_5m - 5.0).abs() < 1e-9 && (opus.cache_write_1h - 8.0).abs() < 1e-9
        );
        assert!((opus.cache_read - 0.2).abs() < 1e-9);
    }

    #[test]
    fn prices_one_hour_cache_writes_separately() {
        let priced = fixture()
            .estimate(
                "claude-opus-5-5",
                UsageForPricing {
                    cache_write_tokens: 2_000_000,
                    cache_write_1h_tokens: 1_000_000,
                    ..UsageForPricing::default()
                },
            )
            .unwrap();
        assert!((priced.cost_usd - 13.0).abs() < 1e-9);
    }

    #[test]
    fn reasoning_inside_output_is_billed_once_and_separate_reasoning_uses_its_rate() {
        let table = fixture();
        let base = UsageForPricing {
            output_tokens: 1_000_000,
            reasoning_tokens: 400_000,
            reasoning_in_output: true,
            ..UsageForPricing::default()
        };
        let once = table.estimate("gpt-5", base).unwrap().cost_usd;
        assert!((once - 10.0).abs() < 1e-9);
        let separate = UsageForPricing {
            reasoning_in_output: false,
            ..base
        };
        assert!((table.estimate("gpt-5", separate).unwrap().cost_usd - 14.0).abs() < 1e-9);
        assert!(
            (table.estimate("o-reasoner", separate).unwrap().cost_usd - (4.0 + 3.2)).abs() < 1e-9
        );
    }

    #[test]
    fn local_overrides_win_and_mark_the_version() {
        let table = fixture()
            .with_overrides(r#"{"version": "mine", "models": [{"id": "claude-opus-5-5", "aliases": ["opus-house"], "input": 1.0, "output": 2.0}]}"#)
            .unwrap();
        let (entry, matched) = table.lookup("opus-house").unwrap();
        assert_eq!(
            (entry.id.as_str(), matched),
            ("claude-opus-5-5", PriceMatch::Alias)
        );
        assert!(
            (table
                .estimate("claude-opus-5-5", per_call(1_000_000))
                .unwrap()
                .cost_usd
                - 1.0)
                .abs()
                < 1e-9
        );
        assert_eq!(table.version, "litellm@2026-10-13+local:mine");
    }

    #[test]
    fn normalizes_cursor_and_dated_names() {
        assert_eq!(
            normalize_model_name("cursor/claude-4.5-sonnet-thinking"),
            "claude-sonnet-4-5"
        );
        assert_eq!(normalize_model_name("claude-4-sonnet"), "claude-sonnet-4");
        assert_eq!(
            normalize_model_name("us.anthropic.claude-sonnet-4-5-20250929"),
            "claude-sonnet-4-5"
        );
    }

    #[test]
    fn bundled_map_prices_current_models() {
        let table = PricingTable::from_litellm(super::super::litellm::BUNDLED, "bundled").unwrap();
        assert!(table.len() > 500);
        for model in [
            "claude-opus-5-5",
            "claude-sonnet-4-5",
            "claude-haiku-4-5",
            "gpt-5",
            "gpt-5.3-codex",
        ] {
            assert!(table.lookup(model).is_some(), "{model} unpriced");
        }
        let opus = table.lookup("claude-opus-5-5").unwrap().0.rates(0);
        assert!((opus.input - 4.0).abs() < 1e-9 && (opus.output - 20.0).abs() < 1e-9);
    }
}
