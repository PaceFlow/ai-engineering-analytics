use anyhow::{Result, ensure};
use std::path::{Path, PathBuf};

pub const CURSOR_STATE_PATH_ENV: &str = "PACEFLOW_CURSOR_STATE_PATH";
pub const CURSOR_HISTORY_PATH_ENV: &str = "PACEFLOW_CURSOR_HISTORY_PATH";

pub fn cursor_state_path() -> Result<Option<PathBuf>> {
    let Some(path) = resolve_state_path_from(None, &default_cursor_user_roots())? else {
        return Ok(None);
    };
    if cfg!(target_os = "linux") && path.starts_with("/mnt") {
        let home = std::env::var_os("PACEFLOW_HOME")
            .map(PathBuf::from)
            .or_else(dirs::home_dir)
            .ok_or_else(|| anyhow::anyhow!("Home directory not found"))?;
        return crate::cursor_snapshot::snapshot_database(
            &path,
            &home.join(".paceflow/cursor-snapshots"),
        )
        .map(Some);
    }
    Ok(Some(path))
}

pub fn cursor_history_path() -> Result<Option<PathBuf>> {
    resolve_history_path_from(None, &default_cursor_user_roots())
}

fn resolve_state_path_from(
    override_path: Option<PathBuf>,
    user_roots: &[PathBuf],
) -> Result<Option<PathBuf>> {
    resolve_path_from(
        override_path.or_else(|| env_override(CURSOR_STATE_PATH_ENV)),
        user_roots,
        |root| root.join("globalStorage").join("state.vscdb"),
        CURSOR_STATE_PATH_ENV,
        "Cursor state.vscdb",
        false,
    )
}

fn resolve_history_path_from(
    override_path: Option<PathBuf>,
    user_roots: &[PathBuf],
) -> Result<Option<PathBuf>> {
    resolve_path_from(
        override_path.or_else(|| env_override(CURSOR_HISTORY_PATH_ENV)),
        user_roots,
        |root| root.join("History"),
        CURSOR_HISTORY_PATH_ENV,
        "Cursor History directory",
        true,
    )
}

fn resolve_path_from<F>(
    override_path: Option<PathBuf>,
    user_roots: &[PathBuf],
    build_candidate: F,
    env_var: &str,
    label: &str,
    expect_dir: bool,
) -> Result<Option<PathBuf>>
where
    F: Fn(&Path) -> PathBuf,
{
    if let Some(path) = override_path {
        validate_override(&path, env_var, label, expect_dir)?;
        return Ok(Some(path));
    }

    for root in user_roots {
        let candidate = build_candidate(root);
        if if expect_dir {
            candidate.is_dir()
        } else {
            candidate.is_file()
        } {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn validate_override(path: &Path, env_var: &str, label: &str, expect_dir: bool) -> Result<()> {
    if expect_dir {
        ensure!(
            path.is_dir(),
            "{} from {} is not a directory: {}",
            label,
            env_var,
            path.display()
        );
    } else {
        ensure!(
            path.is_file(),
            "{} from {} is not a file: {}",
            label,
            env_var,
            path.display()
        );
    }

    Ok(())
}

fn env_override(env_var: &str) -> Option<PathBuf> {
    std::env::var_os(env_var)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn default_cursor_user_roots() -> Vec<PathBuf> {
    let mut roots = build_cursor_user_roots(dirs::config_dir(), dirs::home_dir());
    // Only use the current Windows profile, never another user's history.
    if cfg!(target_os = "linux")
        && std::env::var_os("WSL_DISTRO_NAME").is_some()
        && let Ok(output) = std::process::Command::new("/mnt/c/Windows/System32/cmd.exe")
            .args(["/C", "echo", "%APPDATA%"])
            .output()
        && output.status.success()
    {
        let windows_path = String::from_utf8_lossy(&output.stdout)
            .trim()
            .replace('\\', "/");
        let bytes = windows_path.as_bytes();
        if bytes.len() > 3 && bytes[0].is_ascii_alphabetic() && &bytes[1..3] == b":/" {
            roots.push(
                PathBuf::from(format!(
                    "/mnt/{}/{}",
                    (bytes[0] as char).to_ascii_lowercase(),
                    &windows_path[3..]
                ))
                .join("Cursor/User"),
            );
        }
    }
    roots
}

fn build_cursor_user_roots(config_dir: Option<PathBuf>, home_dir: Option<PathBuf>) -> Vec<PathBuf> {
    let mut roots = Vec::new();

    if let Some(config_dir) = config_dir {
        push_unique(&mut roots, config_dir.join("Cursor").join("User"));
    }

    if let Some(home_dir) = home_dir {
        push_unique(
            &mut roots,
            home_dir
                .join("Library")
                .join("Application Support")
                .join("Cursor")
                .join("User"),
        );
        push_unique(
            &mut roots,
            home_dir.join(".config").join("Cursor").join("User"),
        );
        push_unique(
            &mut roots,
            home_dir
                .join("AppData")
                .join("Roaming")
                .join("Cursor")
                .join("User"),
        );
    }

    roots
}

fn push_unique(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.iter().any(|existing| existing == &candidate) {
        paths.push(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn state_override_path_wins() -> Result<()> {
        let tempdir = tempdir()?;
        let state_path = tempdir.path().join("state.vscdb");
        std::fs::write(&state_path, "")?;

        let resolved = resolve_state_path_from(Some(state_path.clone()), &[])?;
        assert_eq!(resolved, Some(state_path));

        Ok(())
    }

    #[test]
    fn history_override_path_wins() -> Result<()> {
        let tempdir = tempdir()?;
        let history_path = tempdir.path().join("History");
        std::fs::create_dir_all(&history_path)?;

        let resolved = resolve_history_path_from(Some(history_path.clone()), &[])?;
        assert_eq!(resolved, Some(history_path));

        Ok(())
    }

    #[test]
    fn autodiscovery_prefers_existing_user_root() -> Result<()> {
        let tempdir = tempdir()?;
        let config_dir = tempdir.path().join("config");
        let user_root = config_dir.join("Cursor").join("User");
        let state_path = user_root.join("globalStorage").join("state.vscdb");
        let history_path = user_root.join("History");
        std::fs::create_dir_all(state_path.parent().expect("state parent"))?;
        std::fs::create_dir_all(&history_path)?;
        std::fs::write(&state_path, "")?;

        let roots = build_cursor_user_roots(Some(config_dir), None);
        assert_eq!(resolve_state_path_from(None, &roots)?, Some(state_path));
        assert_eq!(resolve_history_path_from(None, &roots)?, Some(history_path));

        Ok(())
    }

    #[test]
    fn windows_roaming_candidate_is_generated() {
        let roots = build_cursor_user_roots(
            Some(PathBuf::from(r"C:\Users\alice\AppData\Roaming")),
            Some(PathBuf::from(r"C:\Users\alice")),
        );

        assert_eq!(
            roots[0],
            PathBuf::from(r"C:\Users\alice\AppData\Roaming")
                .join("Cursor")
                .join("User")
        );
    }
}
