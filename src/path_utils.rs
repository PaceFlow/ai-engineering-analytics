use std::path::{Path, PathBuf};

fn unquote(input: &str) -> &str {
    let trimmed = input.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0] as char;
        let last = bytes[trimmed.len() - 1] as char;
        if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

pub fn resolve_path(raw_path: &str, workdir: Option<&str>, session_cwd: Option<&str>) -> PathBuf {
    let cleaned = unquote(raw_path);
    let path = Path::new(cleaned);

    if path.is_absolute() {
        return path.to_path_buf();
    }

    if let Some(wd) = workdir {
        return Path::new(wd).join(path);
    }

    if let Some(cwd) = session_cwd {
        return Path::new(cwd).join(path);
    }

    path.to_path_buf()
}

pub fn detect_repo_root(abs_path: &Path) -> Option<PathBuf> {
    const MARKERS: &[&str] = &[".git", "package.json", "Cargo.toml", "pyproject.toml", "go.mod"];

    let mut dir = if abs_path.is_file() || abs_path.extension().is_some() {
        abs_path.parent().unwrap_or(abs_path).to_path_buf()
    } else {
        abs_path.to_path_buf()
    };

    loop {
        if MARKERS.iter().any(|marker| dir.join(marker).exists()) {
            return Some(dir);
        }

        match dir.parent() {
            Some(parent) if parent != dir => dir = parent.to_path_buf(),
            _ => break,
        }
    }

    None
}

pub fn to_rel_path(repo_root: Option<&Path>, abs_path: &Path) -> Option<String> {
    let root = repo_root?;
    let rel = abs_path.strip_prefix(root).ok()?;
    Some(rel.to_string_lossy().to_string())
}

pub fn strip_file_scheme(uri: &str) -> String {
    if let Some(p) = uri.strip_prefix("file:///") {
        format!("/{}", p)
    } else if let Some(p) = uri.strip_prefix("file://") {
        p.to_string()
    } else {
        uri.to_string()
    }
}

pub fn find_project_root(path: &str) -> String {
    let p = Path::new(path);
    let start = if p.extension().is_some() {
        p.parent().unwrap_or(p)
    } else {
        p
    };

    if let Some(root) = detect_repo_root(start) {
        return root.to_string_lossy().into_owned();
    }

    start.to_string_lossy().into_owned()
}
