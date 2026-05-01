const LOCK_FILE_BASENAMES: &[&str] = &[
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "npm-shrinkwrap.json",
    "Cargo.lock",
    "Gemfile.lock",
    "poetry.lock",
    "Pipfile.lock",
    "composer.lock",
    "go.sum",
    "mix.lock",
];

pub fn is_lock_file(rel_path: &str) -> bool {
    let basename = rel_path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(rel_path)
        .trim();
    if basename.is_empty() {
        return false;
    }
    LOCK_FILE_BASENAMES
        .iter()
        .any(|name| basename.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_each_known_lock_file_basename() {
        for name in LOCK_FILE_BASENAMES {
            assert!(is_lock_file(name), "expected `{name}` to be a lock file");
        }
    }

    #[test]
    fn detects_lock_files_in_nested_paths() {
        assert!(is_lock_file("frontend/package-lock.json"));
        assert!(is_lock_file("services/api/Cargo.lock"));
        assert!(is_lock_file("a/b/c/d/pnpm-lock.yaml"));
        assert!(is_lock_file("backend\\Pipfile.lock"));
    }

    #[test]
    fn matches_case_insensitively() {
        assert!(is_lock_file("CARGO.LOCK"));
        assert!(is_lock_file("Package-Lock.JSON"));
        assert!(is_lock_file("frontend/PNPM-LOCK.YAML"));
    }

    #[test]
    fn does_not_match_unrelated_paths() {
        assert!(!is_lock_file("src/main.rs"));
        assert!(!is_lock_file("unlock.rs"));
        assert!(!is_lock_file("lockfile_utils.go"));
        assert!(!is_lock_file("vendor/package-lock.json.bak"));
        assert!(!is_lock_file("docs/Cargo.lock.md"));
        assert!(!is_lock_file(""));
    }
}
