//! Shared number formatting for the CLI and TUI. Estimated values carry `~`.

pub fn usd(value: Option<f64>) -> String {
    match value {
        None => "—".to_string(),
        Some(value) if value.abs() >= 100.0 => format!("~${value:.0}"),
        Some(value) => format!("~${value:.2}"),
    }
}

pub fn usd_plain(value: Option<f64>) -> String {
    match value {
        None => "—".to_string(),
        Some(value) => format!("${value:.2}"),
    }
}

pub fn tokens(value: i64) -> String {
    let abs = value.unsigned_abs() as f64;
    if abs >= 1e9 {
        format!("{:.1}B", value as f64 / 1e9)
    } else if abs >= 1e6 {
        format!("{:.1}M", value as f64 / 1e6)
    } else if abs >= 1e3 {
        format!("{:.0}k", value as f64 / 1e3)
    } else {
        value.to_string()
    }
}

pub fn minutes(value: Option<f64>) -> String {
    match value {
        None => "—".to_string(),
        Some(value) if value < 60.0 => format!("{:.0}m", value),
        Some(value) => format!("{}h{:02}", (value / 60.0) as i64, (value % 60.0) as i64),
    }
}

pub fn percent(value: Option<f64>) -> String {
    match value {
        None => "—".to_string(),
        Some(value) => format!("{:.0}%", value * 100.0),
    }
}

/// Short local-ish timestamp `MM-DD HH:MM` from an ISO string.
pub fn when(ts: Option<&str>) -> String {
    match ts {
        Some(ts) if ts.len() >= 16 => format!("{} {}", &ts[5..10], &ts[11..16]),
        Some(ts) => ts.to_string(),
        None => "—".to_string(),
    }
}

pub fn truncate(text: &str, max: usize) -> String {
    let single_line: String = text
        .chars()
        .map(|c| {
            if c == '\n' || c == '\r' || c == '\t' {
                ' '
            } else {
                c
            }
        })
        .collect();
    let trimmed = single_line.trim();
    if trimmed.chars().count() <= max {
        trimmed.to_string()
    } else {
        let mut out: String = trimmed.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

/// Label with its evidence marker: `?` tentative, `h` heuristic, `✎` override.
pub fn label(label: Option<&str>, source: Option<&str>, band: Option<&str>) -> String {
    let Some(label) = label else {
        return "◌".to_string();
    };
    let marker = match (source, band) {
        (Some("override"), _) => "✎",
        (Some("heuristic"), _) => " h",
        (_, Some("tentative")) => "?",
        (_, Some("uncertain")) => " ~?",
        _ => "",
    };
    format!("{label}{marker}")
}

/// Plain table renderer: right-aligns columns whose header starts with `>`.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let columns = headers.len();
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|header| header.trim_start_matches('>').chars().count())
        .collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate().take(columns) {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }
    let right: Vec<bool> = headers
        .iter()
        .map(|header| header.starts_with('>'))
        .collect();
    let render = |cells: Vec<String>| -> String {
        cells
            .iter()
            .enumerate()
            .map(|(index, cell)| {
                let pad = widths[index].saturating_sub(cell.chars().count());
                if right[index] {
                    format!("{}{}", " ".repeat(pad), cell)
                } else {
                    format!("{}{}", cell, " ".repeat(pad))
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut out = render(
        headers
            .iter()
            .map(|header| header.trim_start_matches('>').to_string())
            .collect(),
    );
    out.push('\n');
    for row in rows {
        out.push_str(&render(row.clone()));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_numbers() {
        assert_eq!(tokens(1_234_567), "1.2M");
        assert_eq!(tokens(45_600), "46k");
        assert_eq!(usd(Some(3.456)), "~$3.46");
        assert_eq!(minutes(Some(125.0)), "2h05");
        assert_eq!(label(Some("bug_fix"), Some("heuristic"), None), "bug_fix h");
        assert_eq!(label(None, None, None), "◌");
    }

    #[test]
    fn renders_aligned_table() {
        let out = table(
            &["Name", ">Cost"],
            &[
                vec!["a".into(), "$1".into()],
                vec!["long".into(), "$10".into()],
            ],
        );
        assert_eq!(out, "Name  Cost\na       $1\nlong   $10\n");
    }
}
