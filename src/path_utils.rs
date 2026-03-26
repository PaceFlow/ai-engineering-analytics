use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

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

    if path.is_absolute() || looks_like_windows_drive_path(cleaned) {
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
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, Option<PathBuf>>>> = OnceLock::new();

    let query_dir = path_query_dir(abs_path);
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    if let Some(cached) = cache
        .lock()
        .ok()
        .and_then(|guard| guard.get(&query_dir).cloned())
    {
        return cached;
    }

    let resolved = detect_git_top_level(&query_dir);

    if let Ok(mut guard) = cache.lock() {
        guard.insert(query_dir, resolved.clone());
    }

    resolved
}

pub fn to_rel_path(repo_root: Option<&Path>, abs_path: &Path) -> Option<String> {
    let root = repo_root?;
    if let Ok(rel) = abs_path.strip_prefix(root) {
        return Some(normalize_rel_path(rel));
    }

    let normalized_root = std::fs::canonicalize(root)
        .ok()
        .unwrap_or_else(|| root.to_path_buf());
    let normalized_path = std::fs::canonicalize(abs_path).ok()?;
    let rel = normalized_path.strip_prefix(&normalized_root).ok()?;
    Some(normalize_rel_path(rel))
}

pub fn strip_file_scheme(uri: &str) -> String {
    if let Some(p) = uri.strip_prefix("file://localhost/") {
        normalize_file_uri_path(p)
    } else if let Some(p) = uri.strip_prefix("file:///") {
        normalize_file_uri_path(p)
    } else if let Some(p) = uri.strip_prefix("file://") {
        normalize_file_uri_path(p)
    } else {
        uri.to_string()
    }
}

fn normalize_file_uri_path(path: &str) -> String {
    if path.starts_with('/') || looks_like_windows_drive_path(path) {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn looks_like_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'/' || bytes[2] == b'\\')
}

fn normalize_rel_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn path_query_dir(abs_path: &Path) -> PathBuf {
    if abs_path.is_dir() {
        return abs_path.to_path_buf();
    }

    if abs_path.is_file() || abs_path.extension().is_some() {
        return abs_path.parent().unwrap_or(abs_path).to_path_buf();
    }

    abs_path.to_path_buf()
}

fn nearest_existing_dir(path: &Path) -> Option<PathBuf> {
    let mut dir = path.to_path_buf();
    loop {
        if dir.is_dir() {
            return Some(dir);
        }

        match dir.parent() {
            Some(parent) if parent != dir => dir = parent.to_path_buf(),
            _ => return None,
        }
    }
}

fn detect_git_top_level(query_dir: &Path) -> Option<PathBuf> {
    let existing_dir = nearest_existing_dir(query_dir)?;
    let output = Command::new("git")
        .arg("-C")
        .arg(&existing_dir)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8(output.stdout).ok()?;
    let root = raw.trim();
    if root.is_empty() {
        return None;
    }

    let root_path = PathBuf::from(root);
    std::fs::canonicalize(&root_path).ok().or(Some(root_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;

    fn git(args: &[&str], cwd: &Path) -> Result<()> {
        let status = Command::new("git").current_dir(cwd).args(args).status()?;
        anyhow::ensure!(
            status.success(),
            "git {:?} failed in {}",
            args,
            cwd.display()
        );
        Ok(())
    }

    #[test]
    fn detect_repo_root_uses_canonical_git_toplevel() -> Result<()> {
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("sample-repo");
        std::fs::create_dir_all(repo_root.join("profile_app/src/app"))?;
        std::fs::write(
            repo_root.join("profile_app/package.json"),
            "{\"name\":\"profile-app\"}",
        )?;
        std::fs::write(
            repo_root.join("profile_app/src/app/page.tsx"),
            "export default function Page() {}\n",
        )?;

        git(&["init", "-q"], &repo_root)?;

        let file_path = repo_root.join("profile_app/src/app/page.tsx");
        let detected = detect_repo_root(&file_path).expect("git repo should be detected");
        assert_eq!(detected, std::fs::canonicalize(&repo_root)?);
        assert_eq!(
            to_rel_path(Some(&detected), &file_path).as_deref(),
            Some("profile_app/src/app/page.tsx")
        );

        Ok(())
    }

    #[test]
    fn detect_repo_root_does_not_treat_manifest_only_dir_as_repo() -> Result<()> {
        let tempdir = tempdir()?;
        let project_root = tempdir.path().join("manifest-only");
        std::fs::create_dir_all(project_root.join("src"))?;
        std::fs::write(
            project_root.join("package.json"),
            "{\"name\":\"manifest-only\"}",
        )?;
        std::fs::write(project_root.join("src/index.ts"), "console.log('hi');\n")?;

        let detected = detect_repo_root(&project_root.join("src/index.ts"));
        assert!(detected.is_none());

        Ok(())
    }

    #[test]
    fn strip_file_scheme_keeps_windows_drive_letter_paths() {
        assert_eq!(
            strip_file_scheme("file:///C:/Users/alice/code/src/main.rs"),
            "C:/Users/alice/code/src/main.rs"
        );
        assert_eq!(
            strip_file_scheme("file://localhost/C:/Users/alice/code/src/main.rs"),
            "C:/Users/alice/code/src/main.rs"
        );
    }

    #[test]
    fn resolve_path_does_not_join_windows_drive_paths() {
        let resolved = resolve_path("C:/Users/alice/code/src/main.rs", Some("/tmp/work"), None);
        assert_eq!(resolved, PathBuf::from("C:/Users/alice/code/src/main.rs"));
    }

    #[cfg(windows)]
    #[test]
    fn detect_repo_root_and_rel_path_work_on_windows_paths() -> Result<()> {
        let tempdir = tempdir()?;
        let repo_root = tempdir.path().join("windows-repo");
        std::fs::create_dir_all(repo_root.join("src"))?;
        std::fs::write(repo_root.join("src").join("lib.rs"), "pub fn demo() {}\n")?;

        git(&["init", "-q"], &repo_root)?;

        let file_path = repo_root.join("src").join("lib.rs");
        let detected = detect_repo_root(&file_path).expect("git repo should be detected");

        assert_eq!(detected, std::fs::canonicalize(&repo_root)?);
        assert_eq!(
            to_rel_path(Some(&detected), &file_path).as_deref(),
            Some("src/lib.rs")
        );

        Ok(())
    }
}
