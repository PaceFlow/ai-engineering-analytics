#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TokenUsage {
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub cache_creation_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelPricing {
    pub input_per_million: f64,
    pub cached_input_per_million: f64,
    pub cache_creation_per_million: f64,
    pub output_per_million: f64,
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
    let normalized = normalize_model_name(model_name);
    if normalized.contains("gpt-5.5") {
        return Some(ModelPricing {
            input_per_million: 5.0,
            cached_input_per_million: 0.5,
            cache_creation_per_million: 5.0,
            output_per_million: 30.0,
        });
    }
    if normalized.contains("gpt-5.4-mini") {
        return Some(ModelPricing {
            input_per_million: 0.75,
            cached_input_per_million: 0.075,
            cache_creation_per_million: 0.75,
            output_per_million: 4.5,
        });
    }
    if normalized.contains("gpt-5.4") || normalized.contains("gpt-5") {
        return Some(ModelPricing {
            input_per_million: 2.5,
            cached_input_per_million: 0.25,
            cache_creation_per_million: 2.5,
            output_per_million: 15.0,
        });
    }
    if normalized.contains("claude-opus-4-7")
        || normalized.contains("claude-opus-4-6")
        || normalized.contains("claude-opus-4-5")
    {
        return Some(ModelPricing {
            input_per_million: 5.0,
            cached_input_per_million: 0.5,
            cache_creation_per_million: 6.25,
            output_per_million: 25.0,
        });
    }
    if normalized.contains("claude-opus-4") {
        return Some(ModelPricing {
            input_per_million: 15.0,
            cached_input_per_million: 1.5,
            cache_creation_per_million: 18.75,
            output_per_million: 75.0,
        });
    }
    if normalized.contains("claude-sonnet-4")
        || normalized.contains("claude-sonnet-3-7")
        || normalized.contains("claude-3-7-sonnet")
    {
        return Some(ModelPricing {
            input_per_million: 3.0,
            cached_input_per_million: 0.3,
            cache_creation_per_million: 3.75,
            output_per_million: 15.0,
        });
    }
    if normalized.contains("claude-haiku-4-5") {
        return Some(ModelPricing {
            input_per_million: 1.0,
            cached_input_per_million: 0.1,
            cache_creation_per_million: 1.25,
            output_per_million: 5.0,
        });
    }
    if normalized.contains("claude-haiku-3-5") {
        return Some(ModelPricing {
            input_per_million: 0.8,
            cached_input_per_million: 0.08,
            cache_creation_per_million: 1.0,
            output_per_million: 4.0,
        });
    }
    None
}

fn normalize_model_name(model_name: &str) -> String {
    model_name
        .trim()
        .trim_start_matches("codex/")
        .trim_start_matches("openai/")
        .trim_start_matches("claude/")
        .trim_start_matches("anthropic/")
        .to_ascii_lowercase()
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
            cached_input_tokens: 0,
            cache_creation_tokens: 0,
            output_tokens: 1_000,
            reasoning_tokens: 0,
        };

        assert_eq!(estimate_cost_usd("mystery-provider/model", usage), None);
    }
}
