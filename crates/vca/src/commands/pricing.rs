//! `vca pricing`: where model prices come from and how a model resolves.

use anyhow::Result;

use crate::cli::{PricingArgs, PricingCommands};
use crate::cost::litellm::{self, RefreshOutcome};
use crate::cost::pricing::{self, UsageForPricing};

pub fn run(args: PricingArgs) -> Result<()> {
    match args.command.unwrap_or(PricingCommands::Status) {
        PricingCommands::Status => {
            let table = pricing::table();
            println!(
                "Prices: LiteLLM model price map ({} models), {} copy, version {}",
                table.len(),
                table.source,
                table.version
            );
            println!(
                "Fetched: {} · cache: {}",
                table.fetched_at.as_deref().unwrap_or("unknown"),
                litellm::cache_path()?.display()
            );
            println!(
                "Source: {} (refreshed by `ingest` at most daily; set {}=1 to stay offline)",
                std::env::var(litellm::URL_ENV).unwrap_or_else(|_| litellm::DEFAULT_URL.into()),
                litellm::OFFLINE_ENV
            );
            Ok(())
        }
        PricingCommands::Update => {
            match litellm::refresh_price_map(true) {
                RefreshOutcome::Downloaded { entries } => {
                    println!("Downloaded LiteLLM prices for {entries} models.");
                    println!("Run `ingest` to re-price stored usage.");
                }
                RefreshOutcome::Fresh => println!("Prices are already current."),
                RefreshOutcome::Skipped(reason) | RefreshOutcome::Failed(reason) => {
                    println!(
                        "Not updated: {reason}. Using the {} copy.",
                        pricing::table().source
                    )
                }
            }
            Ok(())
        }
        PricingCommands::Show {
            model,
            prompt_tokens,
        } => {
            let table = pricing::table();
            let Some((entry, matched)) = table.lookup(&model) else {
                println!(
                    "{model}: unpriced (no LiteLLM entry). Add it to {} to price it.",
                    crate::data_home::directory()?
                        .join("pricing.json")
                        .display()
                );
                return Ok(());
            };
            let rates = entry.rates(prompt_tokens);
            println!(
                "{model} → {} ({} match, provider {})",
                entry.id,
                matched.as_str(),
                entry.provider.as_deref().unwrap_or("?")
            );
            println!("USD per million tokens at a {prompt_tokens}-token prompt:");
            println!("  input           {:>10.4}", rates.input);
            println!("  output          {:>10.4}", rates.output);
            println!("  cache read      {:>10.4}", rates.cache_read);
            println!("  cache write 5m  {:>10.4}", rates.cache_write_5m);
            println!("  cache write 1h  {:>10.4}", rates.cache_write_1h);
            println!("  reasoning       {:>10.4}", rates.reasoning);
            let tiers = entry.tier_thresholds();
            if !tiers.is_empty() {
                let list: Vec<String> = tiers.iter().map(|t| format!(">{t}")).collect();
                println!(
                    "Long-context tiers (prompt tokens per request): {}",
                    list.join(", ")
                );
            }
            let example = table
                .estimate(
                    &model,
                    UsageForPricing {
                        input_tokens: 1_000_000,
                        output_tokens: 1_000_000,
                        per_call: false,
                        ..UsageForPricing::default()
                    },
                )
                .map(|p| p.cost_usd)
                .unwrap_or(0.0);
            println!("1M input + 1M output at base rates: ${example:.2}");
            Ok(())
        }
    }
}
