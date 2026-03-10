use anyhow::Result;
use std::io::{IsTerminal, Write};

use crate::db::{self, SessionStat};
use crate::metrics::quality::{compute_quality_metrics, render_quality_metrics};

pub fn run() -> Result<()> {
    let db = db::open()?;
    let stats = db::query_stats(&db)?;
    let quality = compute_quality_metrics(&db)?;

    let mut output = String::new();
    output.push_str(&render_quality_metrics(&quality));
    output.push('\n');
    if stats.is_empty() {
        output.push_str("No session stats yet. Run `vca ingest` first.\n");
    } else {
        output.push('\n');
        output.push_str(&render_stats(&stats));
    }

    if std::io::stdout().is_terminal() && output.lines().count() > terminal_height() {
        pipe_to_pager(&output)
    } else {
        print!("{}", output);
        Ok(())
    }
}

fn render_stats(stats: &[SessionStat]) -> String {
    let shorten = |p: &str| -> String {
        if let Some(home) = dirs::home_dir() {
            let home_str = home.to_string_lossy();
            if p.starts_with(home_str.as_ref()) {
                return format!("~{}", &p[home_str.len()..]);
            }
        }
        p.to_string()
    };

    let proj_width = stats
        .iter()
        .map(|s| shorten(&s.project_path).len())
        .max()
        .unwrap_or(7)
        .max(7);

    let mut out = String::new();

    out.push_str(&format!(
        "{:<8}  {:<8}  {:<width$}  {:<16}  {:>6}  {:>7}  {:>7}  {:>9}\n",
        "Provider", "Session", "Project", "Last active", "LOC", "+Lines", "-Lines", "Words/LOC",
        width = proj_width
    ));
    out.push_str(&format!(
        "{0:─<8}  {0:─<8}  {0:─<width$}  {0:─<16}  {0:─>6}  {0:─>7}  {0:─>7}  {0:─>9}\n",
        "",
        width = proj_width
    ));

    for s in stats {
        let words_per_loc = if s.total_loc > 0 {
            format!("{:.1}", s.total_words as f64 / s.total_loc as f64)
        } else {
            "N/A".to_string()
        };

        let last_active = s.last_updated.as_deref()
            .or(s.timestamp.as_deref())
            .unwrap_or("?");
        let last_active = &last_active[..last_active.len().min(16)];
        let session_short = &s.session_id[..s.session_id.len().min(8)];

        out.push_str(&format!(
            "{:<8}  {:<8}  {:<width$}  {:<16}  {:>6}  {:>7}  {:>7}  {:>9}\n",
            s.provider,
            session_short,
            shorten(&s.project_path),
            last_active,
            s.total_loc,
            s.total_added,
            s.total_removed,
            words_per_loc,
            width = proj_width
        ));
    }

    out
}

fn terminal_height() -> usize {
    terminal_size::terminal_size()
        .map(|(_, terminal_size::Height(h))| h as usize)
        .unwrap_or(24)
}

fn pipe_to_pager(output: &str) -> Result<()> {
    use std::process::{Command, Stdio};

    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());

    let mut cmd = Command::new(&pager);
    if pager == "less" || pager.ends_with("/less") {
        // -R: pass ANSI codes through  -F: quit if output fits on one screen  -X: don't clear on exit
        cmd.args(["-R", "-F", "-X"]);
    }

    match cmd.stdin(Stdio::piped()).spawn() {
        Ok(mut child) => {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(output.as_bytes());
            }
            child.wait()?;
        }
        Err(_) => print!("{}", output),
    }
    Ok(())
}
