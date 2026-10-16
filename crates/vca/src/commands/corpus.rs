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
    println!("Saving {provider}:{session} and replaying its project…");
    let exe = std::env::current_exe()?;
    let saved = corpus::save_reviewed(&conn, &root, &exe, &provider, &session, &expectations)?;
    println!(
        "Saved {provider}:{session} → {} ({} files, {} KB, {} approved numbers)",
        saved.dir.display(),
        saved.files,
        saved.bytes / 1024,
        saved.approved
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
    let all = corpus::list(&root)?;
    if all.is_empty() {
        bail!("no snapshots in {}", root.display());
    }
    // Named snapshots still replay with their whole project; only they are reported.
    let wanted = |dir: &std::path::Path| {
        args.snapshots.is_empty()
            || args.snapshots.iter().any(|name| {
                let path = PathBuf::from(name);
                dir.file_name() == path.file_name() || dir == path
            })
    };
    let groups: Vec<_> = replay::groups(all)
        .into_iter()
        .filter(|g| g.snapshots.iter().any(|s| wanted(&s.dir)))
        .collect();
    if groups.is_empty() {
        bail!("no snapshot matches {:?}", args.snapshots);
    }
    let exe = std::env::current_exe()?;
    let prices = Some(root.join(corpus::PRICES)).filter(|p| p.exists());
    let (mut checked, mut failed) = (0usize, 0usize);
    for group in &groups {
        let label = group.repo_root.as_deref().map(crate::explore::short_repo);
        match replay::check_group(group, &exe, prices.as_deref(), args.keep) {
            Ok(results) => {
                for result in results.iter().filter(|r| {
                    group
                        .snapshots
                        .iter()
                        .any(|s| s.manifest.key == r.key && wanted(&s.dir))
                }) {
                    checked += 1;
                    println!(
                        "{} {} ({} approved numbers{})",
                        if result.passed { "✓" } else { "✗" },
                        result.key,
                        result.approved,
                        label
                            .as_deref()
                            .map(|l| format!(
                                ", replayed with {} {l} sessions",
                                group.snapshots.len()
                            ))
                            .unwrap_or_default()
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
            }
            Err(error) => {
                let n = group.snapshots.iter().filter(|s| wanted(&s.dir)).count();
                checked += n;
                failed += n;
                println!(
                    "✗ {}: {error:#}",
                    label.unwrap_or_else(|| group.snapshots[0].manifest.key.clone())
                );
            }
        }
    }
    if failed > 0 {
        bail!("{failed} of {checked} snapshots differ from their approved numbers");
    }
    println!("all {checked} snapshots match");
    Ok(())
}
