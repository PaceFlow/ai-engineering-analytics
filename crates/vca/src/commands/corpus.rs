use anyhow::{Result, bail};
use std::path::PathBuf;

use crate::cli::{CorpusArgs, CorpusCheckArgs, CorpusCommands, CorpusSaveArgs};
use crate::corpus::{self, replay, server};

pub fn run(args: CorpusArgs) -> Result<()> {
    match args.command {
        CorpusCommands::Review(args) => server::serve(
            args.port,
            server::SampleOptions {
                projects: args.projects,
                providers: args.providers,
                per_group: args.per,
                seed: args.seed,
                min_turns: args.min_turns,
            },
            !args.no_open,
        ),
        CorpusCommands::Save(args) => save(args),
        CorpusCommands::List => list(),
        CorpusCommands::Check(args) => check(args),
    }
}

fn save(args: CorpusSaveArgs) -> Result<()> {
    let conn = crate::db::open()?;
    let (provider, session) = crate::commands::insights::resolve_session(&conn, &args.session)?;
    let text = std::fs::read_to_string(&args.expectations)?;
    let expectations: crate::explore::review::Expectations = serde_json::from_str(&text)?;
    if expectations.session != format!("{provider}:{session}") {
        bail!(
            "{} is for {}, not {provider}:{session}",
            args.expectations.display(),
            expectations.session
        );
    }
    let root = corpus::corpus_dir()?;
    let saved = corpus::snapshot::save(&conn, &root, &provider, &session, &expectations)?;
    println!(
        "Saved {provider}:{session} → {} ({} files, {} KB, {} approved numbers)",
        saved.dir.display(),
        saved.files,
        saved.bytes / 1024,
        expectations.approved.len()
    );
    for warning in &saved.warnings {
        println!("  note: {warning}");
    }
    Ok(())
}

fn list() -> Result<()> {
    let root = corpus::corpus_dir()?;
    let saved = corpus::list(&root)?;
    if saved.is_empty() {
        println!(
            "No snapshots in {}. Start with `vca corpus review`.",
            root.display()
        );
        return Ok(());
    }
    for s in &saved {
        let bytes: u64 = s.manifest.files.iter().map(|f| f.bytes).sum();
        println!(
            "{:<48} {:>3} approved {:>3} rejected {:>8} KB  {}  {}",
            s.dir.file_name().unwrap_or_default().to_string_lossy(),
            s.approved,
            s.rejected,
            bytes / 1024,
            &s.manifest.created_at[..s.manifest.created_at.len().min(16)],
            s.manifest.title.as_deref().unwrap_or("")
        );
    }
    println!("{} snapshots in {}", saved.len(), root.display());
    Ok(())
}

fn check(args: CorpusCheckArgs) -> Result<()> {
    let root = corpus::corpus_dir()?;
    let dirs: Vec<PathBuf> = if args.snapshots.is_empty() {
        corpus::list(&root)?.into_iter().map(|s| s.dir).collect()
    } else {
        args.snapshots
            .iter()
            .map(|name| {
                let path = PathBuf::from(name);
                if path.is_dir() { path } else { root.join(name) }
            })
            .collect()
    };
    if dirs.is_empty() {
        bail!("no snapshots in {}", root.display());
    }
    let exe = std::env::current_exe()?;
    let mut failed = 0usize;
    for dir in &dirs {
        match replay::replay(dir, &exe, args.keep) {
            Ok(result) => {
                println!(
                    "{} {} ({} approved numbers)",
                    if result.passed { "✓" } else { "✗" },
                    result.key,
                    result.approved
                );
                if !result.passed {
                    failed += 1;
                }
                if !result.passed || args.keep {
                    for line in &result.report {
                        println!("    {line}");
                    }
                }
            }
            Err(error) => {
                failed += 1;
                println!("✗ {}: {error:#}", dir.display());
            }
        }
    }
    if failed > 0 {
        bail!(
            "{failed} of {} snapshots differ from their approved numbers",
            dirs.len()
        );
    }
    println!("all {} snapshots match", dirs.len());
    Ok(())
}
