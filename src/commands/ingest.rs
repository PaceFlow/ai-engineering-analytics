use anyhow::Result;

use crate::change_intel::pipeline;
use crate::db;
use crate::providers;

pub fn run(verbose: bool) -> Result<()> {
    let db = db::open()?;
    let providers = providers::all_providers();

    let mut grand_total = 0usize;
    for provider in &providers {
        let provider_name = provider.name();
        println!("Ingesting {} ...", provider_name);
        match provider.ingest(&db, verbose) {
            Ok(n) => {
                println!("  {} events written", n);
                grand_total += n;
            }
            Err(e) => println!("  error: {}", e),
        }

        match pipeline::ingest_provider_code_changes(&db, provider_name, verbose) {
            Ok(summary) => {
                println!(
                    "  code changes [{}]: sources={} skipped={} calls={} ops={} parse_errors={}",
                    summary.provider,
                    summary.sources_discovered,
                    summary.sources_skipped,
                    summary.tool_calls_inspected,
                    summary.ops_upserted,
                    summary.parse_errors
                );
            }
            Err(e) => {
                println!("  code changes error: {}", e);
            }
        }
    }

    println!("\nTotal events written: {}", grand_total);
    Ok(())
}
