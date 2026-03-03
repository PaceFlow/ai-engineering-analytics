use anyhow::Result;

use crate::db;
use crate::providers;

pub fn run(verbose: bool) -> Result<()> {
    let db = db::open()?;
    let providers = providers::all_providers();

    let mut grand_total = 0usize;
    for provider in &providers {
        println!("Ingesting {} ...", provider.name());
        match provider.ingest(&db, verbose) {
            Ok(n) => {
                println!("  {} events written", n);
                grand_total += n;
            }
            Err(e) => println!("  error: {}", e),
        }
    }

    println!("\nTotal events written: {}", grand_total);
    Ok(())
}
