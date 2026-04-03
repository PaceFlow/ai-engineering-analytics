use ai_engineering_analytics::github::auth::github_token;
use assert_cmd::Command;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

struct TestEnv {
    _tempdir: TempDir,
    home: PathBuf,
}

impl TestEnv {
    fn new() -> anyhow::Result<Self> {
        let tempdir = TempDir::new()?;
        let home = tempdir.path().to_path_buf();
        Ok(Self {
            _tempdir: tempdir,
            home,
        })
    }

    fn command(&self) -> anyhow::Result<Command> {
        let mut command = Command::cargo_bin("paceflow")?;
        command
            .current_dir(&self.home)
            .env("PACEFLOW_HOME", &self.home)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env_remove("HOMEDRIVE")
            .env_remove("HOMEPATH")
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("PACEFLOW_GITHUB_TOKEN")
            .env_remove("PACEFLOW_GITHUB_TEST_REPO_KEY")
            .env_remove("PACEFLOW_GITHUB_TEST_COMMIT_SHA")
            .env_remove("PACEFLOW_GITHUB_TEST_EXPECTED_PR")
            .env_remove("PACEFLOW_GITHUB_TEST_EXPECTED_MERGED")
            .env_remove("PACEFLOW_GITHUB_TEST_NO_PR_COMMIT_SHA");
        Ok(command)
    }

    fn token_path(&self) -> PathBuf {
        self.home.join(".paceflow").join("github_token")
    }
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env() -> MutexGuard<'static, ()> {
    env_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ScopedEnvVar {
    key: &'static str,
    original: Option<OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }

    fn unset(key: &'static str) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, original }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

#[test]
fn github_token_command_saves_token_to_paceflow_home() -> anyhow::Result<()> {
    let env = TestEnv::new()?;

    let assert = env
        .command()?
        .args(["github", "token"])
        .write_stdin("ghp_saved_token\n")
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    assert!(stdout.contains("Saved GitHub token"));
    assert!(!stdout.contains("ghp_saved_token"));
    assert_eq!(
        fs::read_to_string(env.token_path())?.trim(),
        "ghp_saved_token"
    );

    let _env_guard = lock_env();
    let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", &env.home);
    let _env_token = ScopedEnvVar::unset("PACEFLOW_GITHUB_TOKEN");
    assert_eq!(github_token()?.as_deref(), Some("ghp_saved_token"));
    Ok(())
}

#[test]
fn github_token_command_updates_existing_saved_token() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    fs::create_dir_all(env.home.join(".paceflow"))?;
    fs::write(env.token_path(), "old_token\n")?;

    let assert = env
        .command()?
        .args(["github", "token"])
        .write_stdin("update\nnew_saved_token\n")
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    assert!(stdout.contains("Updated saved GitHub token"));
    assert!(!stdout.contains("new_saved_token"));
    assert_eq!(
        fs::read_to_string(env.token_path())?.trim(),
        "new_saved_token"
    );
    Ok(())
}

#[test]
fn github_token_command_deletes_existing_saved_token() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    fs::create_dir_all(env.home.join(".paceflow"))?;
    fs::write(env.token_path(), "old_token\n")?;

    let assert = env
        .command()?
        .args(["github", "token"])
        .write_stdin("delete\n")
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    assert!(stdout.contains("Deleted saved GitHub token"));
    assert!(!env.token_path().exists());
    Ok(())
}
