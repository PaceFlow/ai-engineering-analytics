use anyhow::{Context, Result, anyhow, bail};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub const SCHEDULE_INTERVAL_SECONDS: u64 = 6 * 60 * 60;
pub const SCHEDULE_ARGS: [&str; 3] = ["sync", "schedule", "run"];
pub const MACOS_LABEL: &str = "dev.paceflow.sync";
pub const LINUX_SYSTEMD_SERVICE: &str = "paceflow-sync.service";
pub const LINUX_SYSTEMD_TIMER: &str = "paceflow-sync.timer";
pub const LINUX_CRON_MARKER: &str = "# PACEFLOW_MANAGED_SYNC_SCHEDULE";
pub const WINDOWS_TASK_NAME: &str = "Paceflow Sync";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleBackendKind {
    MacOsLaunchd,
    LinuxSystemd,
    LinuxCron,
    WindowsTaskScheduler,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleDefinition {
    pub backend: ScheduleBackendKind,
    pub exe: PathBuf,
    pub args: Vec<String>,
    pub interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleState {
    Missing,
    Installed(ScheduleDefinition),
    NonPaceflowArtifact,
}

pub trait ScheduleBackend {
    fn kind(&self) -> ScheduleBackendKind;
    fn state(&self) -> Result<ScheduleState>;
    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()>;
    fn uninstall(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleInstallOutcome {
    Installed,
    Updated,
    AlreadyInstalled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleStatus {
    pub backend: ScheduleBackendKind,
    pub state: ScheduleState,
}

pub fn expected_definition(backend: ScheduleBackendKind) -> Result<ScheduleDefinition> {
    let exe = env::current_exe().context("failed to locate current paceflow executable")?;
    Ok(expected_definition_with_exe(backend, exe))
}

pub fn expected_definition_with_exe(
    backend: ScheduleBackendKind,
    exe: PathBuf,
) -> ScheduleDefinition {
    ScheduleDefinition {
        backend,
        exe,
        args: SCHEDULE_ARGS.iter().map(|arg| (*arg).to_string()).collect(),
        interval_seconds: SCHEDULE_INTERVAL_SECONDS,
    }
}

pub fn install_or_update_schedule(
    backend: &mut dyn ScheduleBackend,
) -> Result<ScheduleInstallOutcome> {
    let expected = expected_definition(backend.kind())?;
    match backend.state()? {
        ScheduleState::Missing => {
            backend.install(&expected)?;
            Ok(ScheduleInstallOutcome::Installed)
        }
        ScheduleState::Installed(current) if current == expected => {
            Ok(ScheduleInstallOutcome::AlreadyInstalled)
        }
        ScheduleState::Installed(_) => {
            backend.install(&expected)?;
            Ok(ScheduleInstallOutcome::Updated)
        }
        ScheduleState::NonPaceflowArtifact => {
            anyhow::bail!("refusing to modify non-Paceflow sync schedule")
        }
    }
}

pub fn uninstall_schedule(backend: &mut dyn ScheduleBackend) -> Result<bool> {
    match backend.state()? {
        ScheduleState::Missing => Ok(false),
        ScheduleState::Installed(_) => {
            backend.uninstall()?;
            Ok(true)
        }
        ScheduleState::NonPaceflowArtifact => {
            anyhow::bail!("refusing to remove non-Paceflow sync schedule")
        }
    }
}

pub fn current_backend() -> Box<dyn ScheduleBackend> {
    let home = paceflow_home_dir().unwrap_or_else(|_| PathBuf::from("."));
    current_backend_with_home(home)
}

#[cfg(target_os = "macos")]
pub fn current_backend_with_home(home: PathBuf) -> Box<dyn ScheduleBackend> {
    Box::new(LaunchdBackend::new(home))
}

#[cfg(target_os = "windows")]
pub fn current_backend_with_home(_home: PathBuf) -> Box<dyn ScheduleBackend> {
    Box::new(WindowsTaskBackend::new())
}

#[cfg(all(unix, not(target_os = "macos")))]
pub fn current_backend_with_home(home: PathBuf) -> Box<dyn ScheduleBackend> {
    if systemd_user_available() {
        Box::new(SystemdBackend::new(home))
    } else {
        Box::new(CronBackend::new())
    }
}

#[cfg(not(any(unix, target_os = "windows")))]
pub fn current_backend_with_home(_home: PathBuf) -> Box<dyn ScheduleBackend> {
    Box::new(CronBackend::new())
}

pub fn schedule_status(backend: &dyn ScheduleBackend) -> Result<ScheduleStatus> {
    Ok(ScheduleStatus {
        backend: backend.kind(),
        state: backend.state()?,
    })
}

pub trait ScheduledJobRunner {
    fn ingest(&mut self) -> Result<()>;
    fn push_all_projects(&mut self) -> Result<()>;
}

pub struct CommandScheduledJobRunner;

impl ScheduledJobRunner for CommandScheduledJobRunner {
    fn ingest(&mut self) -> Result<()> {
        run_paceflow_command(["ingest"])
    }

    fn push_all_projects(&mut self) -> Result<()> {
        run_paceflow_command(["sync", "push", "--all-projects"])
    }
}

pub fn run_scheduled_sync() -> Result<()> {
    run_scheduled_sync_with_runner(&mut CommandScheduledJobRunner)
}

pub fn run_scheduled_sync_with_runner(runner: &mut dyn ScheduledJobRunner) -> Result<()> {
    let paths = ScheduleRuntimePaths::new()?;
    fs::create_dir_all(&paths.app_dir)?;
    fs::create_dir_all(&paths.log_dir)?;

    let lock = match acquire_lock(&paths.lock_path)? {
        Some(lock) => lock,
        None => {
            append_log(
                &paths.log_path,
                "Skipped: another scheduled sync is already running",
            )?;
            println!("Another Paceflow scheduled sync is already running; skipping.");
            return Ok(());
        }
    };

    append_log(&paths.log_path, "Started scheduled sync")?;
    let result = runner.ingest().and_then(|()| runner.push_all_projects());
    match &result {
        Ok(()) => append_log(&paths.log_path, "Finished scheduled sync")?,
        Err(err) => append_log(&paths.log_path, &format!("Scheduled sync failed: {err}"))?,
    }
    drop(lock);
    let _ = fs::remove_file(&paths.lock_path);
    result
}

struct ScheduleRuntimePaths {
    app_dir: PathBuf,
    log_dir: PathBuf,
    lock_path: PathBuf,
    log_path: PathBuf,
}

impl ScheduleRuntimePaths {
    fn new() -> Result<Self> {
        let app_dir = paceflow_home_dir()?.join(".paceflow");
        let log_dir = app_dir.join("logs");
        Ok(Self {
            lock_path: app_dir.join("sync_schedule.lock"),
            log_path: log_dir.join("sync-schedule.log"),
            app_dir,
            log_dir,
        })
    }
}

fn acquire_lock(path: &Path) -> Result<Option<fs::File>> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            writeln!(file, "pid={}", std::process::id())?;
            Ok(Some(file))
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to create {}", path.display())),
    }
}

fn append_log(path: &Path, message: &str) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{message}")?;
    Ok(())
}

fn run_paceflow_command<const N: usize>(args: [&str; N]) -> Result<()> {
    let exe = env::current_exe().context("failed to locate current paceflow executable")?;
    let status = Command::new(exe)
        .args(args)
        .stdin(Stdio::null())
        .status()
        .context("failed to run paceflow scheduled command")?;
    if !status.success() {
        bail!("paceflow scheduled command failed with {status}");
    }
    Ok(())
}

fn paceflow_home_dir() -> Result<PathBuf> {
    env::var_os("PACEFLOW_HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow!("Home directory not found"))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_user_available() -> bool {
    Command::new("systemctl")
        .args(["--user", "is-system-running"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct LaunchdBackend {
    home: PathBuf,
}

#[cfg(target_os = "macos")]
impl LaunchdBackend {
    fn new(home: PathBuf) -> Self {
        Self { home }
    }

    fn plist_path(&self) -> PathBuf {
        self.home
            .join("Library/LaunchAgents")
            .join(format!("{MACOS_LABEL}.plist"))
    }
}

#[cfg(target_os = "macos")]
impl ScheduleBackend for LaunchdBackend {
    fn kind(&self) -> ScheduleBackendKind {
        ScheduleBackendKind::MacOsLaunchd
    }

    fn state(&self) -> Result<ScheduleState> {
        read_managed_file_state(&self.plist_path(), parse_launchd_definition)
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        let path = self.plist_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Unload any previous registration before rewriting the file. We don't care if this fails
        // (e.g. the agent was never loaded, or `launchctl unload` returns a spurious error 5 on
        // modern macOS) — the subsequent load is what actually matters. Silence stderr/stdout so
        // diagnostic warnings don't surface to the user.
        let _ = Command::new("launchctl")
            .arg("unload")
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        fs::write(&path, launchd_plist(expected))?;
        let status = Command::new("launchctl")
            .arg("load")
            .arg(&path)
            .status()
            .context("failed to run launchctl load")?;
        if !status.success() {
            bail!("launchctl load failed for {}", path.display());
        }
        Ok(())
    }

    fn uninstall(&mut self) -> Result<()> {
        let path = self.plist_path();
        let _ = Command::new("launchctl")
            .arg("unload")
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        remove_if_exists(&path)
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
#[derive(Debug)]
struct SystemdBackend {
    home: PathBuf,
}

#[cfg(all(unix, not(target_os = "macos")))]
impl SystemdBackend {
    fn new(home: PathBuf) -> Self {
        Self { home }
    }

    fn unit_dir(&self) -> PathBuf {
        self.home.join(".config/systemd/user")
    }

    fn service_path(&self) -> PathBuf {
        self.unit_dir().join(LINUX_SYSTEMD_SERVICE)
    }

    fn timer_path(&self) -> PathBuf {
        self.unit_dir().join(LINUX_SYSTEMD_TIMER)
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
impl ScheduleBackend for SystemdBackend {
    fn kind(&self) -> ScheduleBackendKind {
        ScheduleBackendKind::LinuxSystemd
    }

    fn state(&self) -> Result<ScheduleState> {
        let service_state =
            read_managed_file_state(&self.service_path(), parse_systemd_definition)?;
        let timer_state = read_managed_file_state(&self.timer_path(), |_| ScheduleDefinition {
            backend: ScheduleBackendKind::LinuxSystemd,
            exe: PathBuf::from("(timer)"),
            args: Vec::new(),
            interval_seconds: SCHEDULE_INTERVAL_SECONDS,
        })?;
        match (service_state, timer_state) {
            (ScheduleState::Missing, ScheduleState::Missing) => Ok(ScheduleState::Missing),
            (ScheduleState::NonPaceflowArtifact, _) | (_, ScheduleState::NonPaceflowArtifact) => {
                Ok(ScheduleState::NonPaceflowArtifact)
            }
            (ScheduleState::Installed(service), ScheduleState::Installed(_)) => {
                Ok(ScheduleState::Installed(service))
            }
            _ => Ok(ScheduleState::Installed(ScheduleDefinition {
                backend: ScheduleBackendKind::LinuxSystemd,
                exe: PathBuf::from("stale"),
                args: Vec::new(),
                interval_seconds: 0,
            })),
        }
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        fs::create_dir_all(self.unit_dir())?;
        fs::write(self.service_path(), systemd_service(expected))?;
        fs::write(self.timer_path(), systemd_timer(expected))?;
        run_systemctl(["daemon-reload"])?;
        run_systemctl(["enable", "--now", LINUX_SYSTEMD_TIMER])?;
        Ok(())
    }

    fn uninstall(&mut self) -> Result<()> {
        let _ = run_systemctl(["disable", "--now", LINUX_SYSTEMD_TIMER]);
        remove_if_exists(&self.service_path())?;
        remove_if_exists(&self.timer_path())?;
        let _ = run_systemctl(["daemon-reload"]);
        Ok(())
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
#[derive(Debug)]
struct CronBackend;

#[cfg(all(unix, not(target_os = "macos")))]
impl CronBackend {
    fn new() -> Self {
        Self
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
impl ScheduleBackend for CronBackend {
    fn kind(&self) -> ScheduleBackendKind {
        ScheduleBackendKind::LinuxCron
    }

    fn state(&self) -> Result<ScheduleState> {
        let crontab = read_crontab().unwrap_or_default();
        if !crontab.contains(LINUX_CRON_MARKER) {
            return Ok(ScheduleState::Missing);
        }
        Ok(ScheduleState::Installed(parse_cron_definition(&crontab)))
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        let crontab = read_crontab().unwrap_or_default();
        let cleaned = remove_managed_cron_block(&crontab);
        let next = format!(
            "{}{}\n{}\n0 */6 * * * {}\n{}\n",
            cleaned,
            if cleaned.trim().is_empty() { "" } else { "\n" },
            LINUX_CRON_MARKER,
            shell_quote_command(expected),
            LINUX_CRON_MARKER
        );
        write_crontab(&next)
    }

    fn uninstall(&mut self) -> Result<()> {
        let crontab = read_crontab().unwrap_or_default();
        write_crontab(&remove_managed_cron_block(&crontab))
    }
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
struct WindowsTaskBackend;

#[cfg(target_os = "windows")]
impl WindowsTaskBackend {
    fn new() -> Self {
        Self
    }
}

#[cfg(target_os = "windows")]
impl ScheduleBackend for WindowsTaskBackend {
    fn kind(&self) -> ScheduleBackendKind {
        ScheduleBackendKind::WindowsTaskScheduler
    }

    fn state(&self) -> Result<ScheduleState> {
        let output = Command::new("schtasks")
            .args(["/Query", "/TN", WINDOWS_TASK_NAME, "/XML"])
            .output();
        let Ok(output) = output else {
            return Ok(ScheduleState::Missing);
        };
        if !output.status.success() {
            return Ok(ScheduleState::Missing);
        }
        // schtasks /Query /XML emits UTF-16 LE with a BOM; fall back to a lossy UTF-8
        // decode for robustness against other output paths.
        let xml = decode_schtasks_xml(&output.stdout);
        if !xml.contains("PACEFLOW_MANAGED_SYNC_SCHEDULE") {
            return Ok(ScheduleState::NonPaceflowArtifact);
        }
        Ok(ScheduleState::Installed(parse_windows_task_definition(
            &xml,
        )))
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        let xml_path = env::temp_dir().join("paceflow-sync-task.xml");
        // schtasks /Create /XML rejects UTF-8 bytes whose prolog declares any encoding
        // (it expects UTF-16 LE with a BOM and bails with
        // "(1,40)::ERROR: unable to switch the encoding" otherwise).
        fs::write(&xml_path, windows_task_xml_bytes(expected))?;
        let status = Command::new("schtasks")
            .args([
                "/Create",
                "/F",
                "/TN",
                WINDOWS_TASK_NAME,
                "/XML",
                &xml_path.to_string_lossy(),
            ])
            .status()
            .context("failed to run schtasks /Create")?;
        let _ = fs::remove_file(xml_path);
        if !status.success() {
            bail!("schtasks /Create failed");
        }
        Ok(())
    }

    fn uninstall(&mut self) -> Result<()> {
        let status = Command::new("schtasks")
            .args(["/Delete", "/F", "/TN", WINDOWS_TASK_NAME])
            .status()
            .context("failed to run schtasks /Delete")?;
        if !status.success() {
            bail!("schtasks /Delete failed");
        }
        Ok(())
    }
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn read_managed_file_state(
    path: &Path,
    parse: fn(&str) -> ScheduleDefinition,
) -> Result<ScheduleState> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ScheduleState::Missing);
        }
        Err(err) => return Err(err.into()),
    };
    if !contents.contains("PACEFLOW_MANAGED_SYNC_SCHEDULE") {
        return Ok(ScheduleState::NonPaceflowArtifact);
    }
    Ok(ScheduleState::Installed(parse(&contents)))
}

#[cfg(target_os = "macos")]
fn parse_launchd_definition(contents: &str) -> ScheduleDefinition {
    let (exe, args) =
        split_exe_and_args(parse_launchd_program_arguments(contents).unwrap_or_default());
    ScheduleDefinition {
        backend: ScheduleBackendKind::MacOsLaunchd,
        exe,
        args,
        interval_seconds: parse_launchd_start_interval(contents).unwrap_or(0),
    }
}

#[cfg(target_os = "macos")]
fn parse_launchd_program_arguments(contents: &str) -> Option<Vec<String>> {
    use regex::Regex;
    let array_re = Regex::new(r"(?s)<array>(.*?)</array>").ok()?;
    let array_content = array_re.captures(contents)?.get(1)?.as_str();
    let string_re = Regex::new(r"<string>([^<]*)</string>").ok()?;
    Some(
        string_re
            .captures_iter(array_content)
            .map(|c| unescape_xml(&c[1]))
            .collect(),
    )
}

#[cfg(target_os = "macos")]
fn parse_launchd_start_interval(contents: &str) -> Option<u64> {
    use regex::Regex;
    let re = Regex::new(r"<key>StartInterval</key>\s*<integer>(\d+)</integer>").ok()?;
    re.captures(contents)?.get(1)?.as_str().parse().ok()
}

#[cfg(all(unix, not(target_os = "macos")))]
fn parse_systemd_definition(contents: &str) -> ScheduleDefinition {
    let exec_start = contents
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart="))
        .unwrap_or_default();
    let (exe, args) = split_exe_and_args(shell_tokenize(exec_start));
    let interval_seconds = contents
        .lines()
        .find_map(|line| line.strip_prefix("# IntervalSeconds="))
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(0);
    ScheduleDefinition {
        backend: ScheduleBackendKind::LinuxSystemd,
        exe,
        args,
        interval_seconds,
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn parse_cron_definition(crontab: &str) -> ScheduleDefinition {
    let mut inside_managed_block = false;
    let mut command_line: Option<&str> = None;
    let mut six_hour_marker_found = false;
    for line in crontab.lines() {
        if line.trim() == LINUX_CRON_MARKER {
            inside_managed_block = !inside_managed_block;
            continue;
        }
        if !inside_managed_block {
            continue;
        }
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("0 */6 * * *") {
            six_hour_marker_found = true;
            command_line = Some(rest.trim_start());
            break;
        }
    }
    let (exe, args) = split_exe_and_args(shell_tokenize(command_line.unwrap_or_default()));
    ScheduleDefinition {
        backend: ScheduleBackendKind::LinuxCron,
        exe,
        args,
        interval_seconds: if six_hour_marker_found {
            SCHEDULE_INTERVAL_SECONDS
        } else {
            0
        },
    }
}

#[cfg(any(target_os = "windows", test))]
fn parse_windows_task_definition(xml: &str) -> ScheduleDefinition {
    use regex::Regex;
    let command = Regex::new(r"(?s)<Command>(.*?)</Command>")
        .ok()
        .and_then(|re| re.captures(xml))
        .and_then(|caps| caps.get(1))
        .map(|m| unescape_xml(m.as_str()))
        .unwrap_or_default();
    let arguments = Regex::new(r"(?s)<Arguments>(.*?)</Arguments>")
        .ok()
        .and_then(|re| re.captures(xml))
        .and_then(|caps| caps.get(1))
        .map(|m| unescape_xml(m.as_str()))
        .unwrap_or_default();
    let (exe, args) = if command.eq_ignore_ascii_case("powershell.exe") {
        parse_windows_hidden_powershell_arguments(&arguments)
            .unwrap_or_else(|| (PathBuf::from("stale"), Vec::new()))
    } else {
        let args = if arguments.trim().is_empty() {
            Vec::new()
        } else {
            arguments
                .split_whitespace()
                .map(|s| s.to_string())
                .collect()
        };
        (
            if command.is_empty() {
                PathBuf::from("stale")
            } else {
                PathBuf::from(command)
            },
            args,
        )
    };
    let interval_seconds = if xml.contains("<Interval>PT6H</Interval>") {
        SCHEDULE_INTERVAL_SECONDS
    } else {
        0
    };
    ScheduleDefinition {
        backend: ScheduleBackendKind::WindowsTaskScheduler,
        exe,
        args,
        interval_seconds,
    }
}

#[cfg(any(target_os = "windows", test))]
fn parse_windows_hidden_powershell_arguments(arguments: &str) -> Option<(PathBuf, Vec<String>)> {
    use regex::Regex;
    let file_re = Regex::new(r"-FilePath\s+'((?:''|[^'])*)'").ok()?;
    let args_re = Regex::new(r"-ArgumentList\s+@\((?P<args>[^)]*)\)").ok()?;
    let quoted_arg_re = Regex::new(r"'((?:''|[^'])*)'").ok()?;

    let exe = file_re
        .captures(arguments)?
        .get(1)
        .map(|m| powershell_unquote_single_quoted(m.as_str()))?;
    let args = args_re
        .captures(arguments)
        .and_then(|captures| captures.name("args"))
        .map(|m| {
            quoted_arg_re
                .captures_iter(m.as_str())
                .filter_map(|capture| capture.get(1))
                .map(|m| powershell_unquote_single_quoted(m.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some((PathBuf::from(exe), args))
}

#[cfg(any(target_os = "windows", test))]
fn powershell_unquote_single_quoted(value: &str) -> String {
    value.replace("''", "'")
}

#[cfg(any(target_os = "macos", target_os = "windows", test))]
fn unescape_xml(value: &str) -> String {
    value
        .replace("&apos;", "'")
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn split_exe_and_args(mut tokens: Vec<String>) -> (PathBuf, Vec<String>) {
    if tokens.is_empty() {
        return (PathBuf::from("stale"), Vec::new());
    }
    let exe = PathBuf::from(tokens.remove(0));
    (exe, tokens)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn shell_tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if in_single {
            if c == '\'' {
                let mut lookahead = chars.clone();
                if lookahead.next() == Some('\\')
                    && lookahead.next() == Some('\'')
                    && lookahead.next() == Some('\'')
                {
                    chars.next();
                    chars.next();
                    chars.next();
                    current.push('\'');
                } else {
                    in_single = false;
                }
            } else {
                current.push(c);
            }
        } else if c.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else if c == '\'' {
            in_single = true;
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(target_os = "macos")]
fn launchd_plist(expected: &ScheduleDefinition) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key><string>{MACOS_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{}
    </array>
    <key>StartInterval</key><integer>{}</integer>
    <key>StandardOutPath</key><string>{}/.paceflow/logs/sync-schedule.out.log</string>
    <key>StandardErrorPath</key><string>{}/.paceflow/logs/sync-schedule.err.log</string>
    <key>Comment</key><string>PACEFLOW_MANAGED_SYNC_SCHEDULE; {}</string>
  </dict>
</plist>
"#,
        render_launchd_args(expected),
        expected.interval_seconds,
        env::var("HOME").unwrap_or_else(|_| "$HOME".to_string()),
        env::var("HOME").unwrap_or_else(|_| "$HOME".to_string()),
        shell_quote_command(expected)
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_service(expected: &ScheduleDefinition) -> String {
    format!(
        "[Unit]\nDescription=Paceflow periodic sync\n# PACEFLOW_MANAGED_SYNC_SCHEDULE\n# IntervalSeconds={}\n\n[Service]\nType=oneshot\nExecStart={}\n",
        expected.interval_seconds,
        shell_quote_command(expected)
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_timer(_expected: &ScheduleDefinition) -> String {
    "[Unit]\nDescription=Run Paceflow periodic sync every 6 hours\n# PACEFLOW_MANAGED_SYNC_SCHEDULE\n\n[Timer]\nOnBootSec=5m\nOnUnitActiveSec=21600\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n".to_string()
}

#[cfg(all(unix, not(target_os = "macos")))]
fn run_systemctl<const N: usize>(args: [&str; N]) -> Result<()> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("failed to run systemctl --user")?;
    if !status.success() {
        bail!("systemctl --user failed");
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn read_crontab() -> Result<String> {
    let output = Command::new("crontab").arg("-l").output()?;
    if !output.status.success() {
        return Ok(String::new());
    }
    Ok(String::from_utf8(output.stdout)?)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn write_crontab(contents: &str) -> Result<()> {
    let mut child = Command::new("crontab")
        .arg("-")
        .stdin(Stdio::piped())
        .spawn()
        .context("failed to run crontab -")?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| anyhow!("failed to open crontab stdin"))?
        .write_all(contents.as_bytes())?;
    let status = child.wait()?;
    if !status.success() {
        bail!("crontab update failed");
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn remove_managed_cron_block(crontab: &str) -> String {
    let mut output = Vec::new();
    let mut in_managed_block = false;
    for line in crontab.lines() {
        if line.trim() == LINUX_CRON_MARKER {
            in_managed_block = !in_managed_block;
            continue;
        }
        if !in_managed_block {
            output.push(line);
        }
    }
    output.join("\n")
}

#[cfg(any(target_os = "windows", test))]
fn windows_task_xml(expected: &ScheduleDefinition) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>PACEFLOW_MANAGED_SYNC_SCHEDULE</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>
      <Repetition>
        <Interval>PT6H</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author"><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <Enabled>true</Enabled>
  </Settings>
    <Actions Context="Author">
    <Exec>
      <Command>{}</Command>
      <Arguments>{}</Arguments>
    </Exec>
  </Actions>
  <!-- {} every {} seconds -->
</Task>
"#,
        escape_xml("powershell.exe"),
        escape_xml(&windows_hidden_powershell_arguments(expected)),
        shell_quote_command(expected),
        expected.interval_seconds
    )
}

#[cfg(any(target_os = "windows", test))]
fn windows_hidden_powershell_arguments(expected: &ScheduleDefinition) -> String {
    let exe = powershell_single_quote(&expected.exe.to_string_lossy());
    let args = expected
        .args
        .iter()
        .map(|arg| powershell_single_quote(arg))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command \"Start-Process -FilePath {exe} -ArgumentList @({args}) -WindowStyle Hidden -Wait\""
    )
}

#[cfg(any(target_os = "windows", test))]
fn powershell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(any(target_os = "windows", test))]
fn windows_task_xml_bytes(expected: &ScheduleDefinition) -> Vec<u8> {
    encode_utf16_le_with_bom(&windows_task_xml(expected))
}

#[cfg(any(target_os = "windows", test))]
fn encode_utf16_le_with_bom(s: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + s.len() * 2);
    bytes.extend_from_slice(&[0xFF, 0xFE]);
    for unit in s.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

#[cfg(any(target_os = "windows", test))]
fn decode_schtasks_xml(bytes: &[u8]) -> String {
    if bytes.len() >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE {
        let payload = &bytes[2..];
        // Drop a trailing odd byte rather than failing — schtasks output is always
        // an even number of bytes after the BOM in practice, but be defensive.
        let unit_count = payload.len() / 2;
        let units: Vec<u16> = (0..unit_count)
            .map(|i| u16::from_le_bytes([payload[i * 2], payload[i * 2 + 1]]))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

#[cfg(target_os = "macos")]
fn render_launchd_args(expected: &ScheduleDefinition) -> String {
    std::iter::once(expected.exe.to_string_lossy().to_string())
        .chain(expected.args.iter().cloned())
        .map(|arg| format!("      <string>{}</string>", escape_xml(&arg)))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(any(
    target_os = "macos",
    target_os = "windows",
    all(unix, not(target_os = "macos"))
))]
fn shell_quote_command(expected: &ScheduleDefinition) -> String {
    std::iter::once(expected.exe.to_string_lossy().to_string())
        .chain(expected.args.iter().cloned())
        .map(|part| shell_quote(&part))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn shell_quote(value: &str) -> String {
    if !value.is_empty() && value.chars().all(is_shell_safe_char) {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn is_shell_safe_char(c: char) -> bool {
    matches!(
        c,
        'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '/' | '+' | '=' | ':' | ',' | '@'
    )
}

#[cfg(target_os = "windows")]
fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && !value
            .chars()
            .any(|c| c.is_whitespace() || c == '"' || c == '\\')
    {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('"', "\\\""))
}

#[cfg(any(target_os = "macos", target_os = "windows", test))]
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ScopedEnvVar, lock_env};
    use std::fs;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct FakeBackend {
        kind: ScheduleBackendKind,
        state: ScheduleState,
        installs: Vec<ScheduleDefinition>,
        uninstall_count: usize,
    }

    #[cfg(unix)]
    fn fake_exe() -> PathBuf {
        PathBuf::from("/tmp/paceflow test/bin/paceflow")
    }

    impl FakeBackend {
        fn new(state: ScheduleState) -> Self {
            Self {
                kind: ScheduleBackendKind::LinuxSystemd,
                state,
                installs: Vec::new(),
                uninstall_count: 0,
            }
        }
    }

    impl ScheduleBackend for FakeBackend {
        fn kind(&self) -> ScheduleBackendKind {
            self.kind
        }

        fn state(&self) -> Result<ScheduleState> {
            Ok(self.state.clone())
        }

        fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
            self.state = ScheduleState::Installed(expected.clone());
            self.installs.push(expected.clone());
            Ok(())
        }

        fn uninstall(&mut self) -> Result<()> {
            self.state = ScheduleState::Missing;
            self.uninstall_count += 1;
            Ok(())
        }
    }

    #[test]
    fn install_missing_schedule_writes_expected_six_hour_command() -> Result<()> {
        let mut backend = FakeBackend::new(ScheduleState::Missing);

        let outcome = install_or_update_schedule(&mut backend)?;

        assert_eq!(outcome, ScheduleInstallOutcome::Installed);
        assert_eq!(
            backend.installs,
            vec![expected_definition(ScheduleBackendKind::LinuxSystemd)?]
        );
        Ok(())
    }

    #[test]
    fn install_matching_schedule_is_noop() -> Result<()> {
        let expected = expected_definition(ScheduleBackendKind::LinuxSystemd)?;
        let mut backend = FakeBackend::new(ScheduleState::Installed(expected));

        let outcome = install_or_update_schedule(&mut backend)?;

        assert_eq!(outcome, ScheduleInstallOutcome::AlreadyInstalled);
        assert!(backend.installs.is_empty());
        Ok(())
    }

    #[test]
    fn install_stale_managed_schedule_updates_to_expected_definition() -> Result<()> {
        let mut stale = expected_definition(ScheduleBackendKind::LinuxSystemd)?;
        stale.interval_seconds = 60;
        let mut backend = FakeBackend::new(ScheduleState::Installed(stale));

        let outcome = install_or_update_schedule(&mut backend)?;

        assert_eq!(outcome, ScheduleInstallOutcome::Updated);
        assert_eq!(backend.installs, vec![expected_definition(backend.kind)?]);
        Ok(())
    }

    #[test]
    fn install_refuses_non_paceflow_artifact() {
        let mut backend = FakeBackend::new(ScheduleState::NonPaceflowArtifact);

        let err = install_or_update_schedule(&mut backend).expect_err("should refuse");

        assert!(err.to_string().contains("non-Paceflow"));
        assert!(backend.installs.is_empty());
    }

    #[test]
    fn uninstall_removes_only_installed_paceflow_schedule() -> Result<()> {
        let expected = expected_definition(ScheduleBackendKind::LinuxSystemd)?;
        let mut backend = FakeBackend::new(ScheduleState::Installed(expected));

        assert!(uninstall_schedule(&mut backend)?);
        assert_eq!(backend.uninstall_count, 1);
        assert_eq!(backend.state, ScheduleState::Missing);
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_uses_absolute_executable_in_program_arguments() {
        let definition =
            expected_definition_with_exe(ScheduleBackendKind::MacOsLaunchd, fake_exe());

        let plist = launchd_plist(&definition);

        assert!(plist.contains("<string>/tmp/paceflow test/bin/paceflow</string>"));
        assert!(plist.contains("<string>sync</string>"));
        assert!(plist.contains("<string>schedule</string>"));
        assert!(plist.contains("<string>run</string>"));
        assert!(plist.contains("21600"));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn systemd_service_quotes_absolute_executable_path() {
        let definition =
            expected_definition_with_exe(ScheduleBackendKind::LinuxSystemd, fake_exe());

        let service = systemd_service(&definition);

        assert!(service.contains("ExecStart='/tmp/paceflow test/bin/paceflow' sync schedule run"));
    }

    #[cfg(any(target_os = "windows", test))]
    #[test]
    fn windows_task_xml_runs_paceflow_through_hidden_powershell() {
        let definition = expected_definition_with_exe(
            ScheduleBackendKind::WindowsTaskScheduler,
            PathBuf::from(r"C:\Program Files\Paceflow\paceflow.exe"),
        );

        let xml = windows_task_xml(&definition);

        assert!(xml.contains("<Command>powershell.exe</Command>"));
        assert!(xml.contains("-NoProfile"));
        assert!(xml.contains("-WindowStyle Hidden"));
        assert!(xml.contains("Start-Process"));
        assert!(xml.contains(r"C:\Program Files\Paceflow\paceflow.exe"));
        assert!(xml.contains("sync"));
        assert!(xml.contains("schedule"));
        assert!(xml.contains("run"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_launchd_definition_round_trips_through_rendered_plist() {
        let original = expected_definition_with_exe(ScheduleBackendKind::MacOsLaunchd, fake_exe());

        let plist = launchd_plist(&original);
        let parsed = parse_launchd_definition(&plist);

        assert_eq!(parsed, original);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_launchd_definition_detects_stale_exe_so_install_can_update() {
        let stale = expected_definition_with_exe(
            ScheduleBackendKind::MacOsLaunchd,
            PathBuf::from("/old/paceflow/target/debug/paceflow"),
        );
        let plist = launchd_plist(&stale);

        let parsed = parse_launchd_definition(&plist);

        assert_eq!(
            parsed.exe,
            PathBuf::from("/old/paceflow/target/debug/paceflow")
        );
        let fresh = expected_definition_with_exe(
            ScheduleBackendKind::MacOsLaunchd,
            PathBuf::from("/Users/me/.cargo/bin/paceflow"),
        );
        assert_ne!(parsed, fresh);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn parse_systemd_definition_round_trips_through_rendered_service() {
        let original = expected_definition_with_exe(ScheduleBackendKind::LinuxSystemd, fake_exe());

        let service = systemd_service(&original);
        let parsed = parse_systemd_definition(&service);

        assert_eq!(parsed, original);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn parse_systemd_definition_detects_stale_exe_so_install_can_update() {
        let stale = expected_definition_with_exe(
            ScheduleBackendKind::LinuxSystemd,
            PathBuf::from("/old/paceflow/target/debug/paceflow"),
        );
        let service = systemd_service(&stale);

        let parsed = parse_systemd_definition(&service);

        assert_eq!(
            parsed.exe,
            PathBuf::from("/old/paceflow/target/debug/paceflow")
        );
        let fresh = expected_definition_with_exe(
            ScheduleBackendKind::LinuxSystemd,
            PathBuf::from("/home/me/.cargo/bin/paceflow"),
        );
        assert_ne!(parsed, fresh);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn shell_tokenize_handles_quoted_paths_and_bare_args() {
        let tokens = shell_tokenize("'/tmp/paceflow test/bin/paceflow' sync schedule run");
        assert_eq!(
            tokens,
            vec![
                "/tmp/paceflow test/bin/paceflow".to_string(),
                "sync".to_string(),
                "schedule".to_string(),
                "run".to_string(),
            ]
        );
    }

    #[cfg(any(target_os = "windows", test))]
    #[test]
    fn windows_task_xml_bytes_are_utf16_le_with_bom() {
        let definition = expected_definition_with_exe(
            ScheduleBackendKind::WindowsTaskScheduler,
            PathBuf::from(r"C:\Program Files\Paceflow\paceflow.exe"),
        );

        let bytes = windows_task_xml_bytes(&definition);

        assert!(
            bytes.starts_with(&[0xFF, 0xFE]),
            "expected UTF-16 LE BOM, got {:?}",
            bytes.get(..2)
        );
        assert!(
            (bytes.len() - 2).is_multiple_of(2),
            "UTF-16 payload must be an even number of bytes"
        );

        let payload = &bytes[2..];
        let units: Vec<u16> = payload
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        let decoded = String::from_utf16(&units).expect("payload should be valid UTF-16");

        assert!(
            decoded.starts_with(r#"<?xml version="1.0" encoding="UTF-16"?>"#),
            "prolog must declare encoding=\"UTF-16\" to match the file bytes: {decoded}"
        );
        assert!(decoded.contains("<Command>powershell.exe</Command>"));
        assert!(
            decoded.contains("-WindowStyle Hidden"),
            "decoded XML missing hidden window arguments: {decoded}"
        );
    }

    #[cfg(any(target_os = "windows", test))]
    #[test]
    fn decode_schtasks_xml_handles_utf16_le_bom_and_falls_back_to_utf8() {
        let original = "<Task>PACEFLOW_MANAGED_SYNC_SCHEDULE</Task>";
        let utf16_bytes = encode_utf16_le_with_bom(original);

        let decoded = decode_schtasks_xml(&utf16_bytes);

        assert_eq!(decoded, original);
        assert!(decoded.contains("PACEFLOW_MANAGED_SYNC_SCHEDULE"));

        let utf8_bytes = original.as_bytes();
        let decoded_utf8 = decode_schtasks_xml(utf8_bytes);
        assert_eq!(decoded_utf8, original);
    }

    #[cfg(any(target_os = "windows", test))]
    #[test]
    fn parse_windows_task_definition_round_trips_through_utf16_query_output() {
        let original = expected_definition_with_exe(
            ScheduleBackendKind::WindowsTaskScheduler,
            PathBuf::from(r"C:\Program Files\Paceflow\paceflow.exe"),
        );
        let utf16_bytes = windows_task_xml_bytes(&original);

        let xml = decode_schtasks_xml(&utf16_bytes);

        assert!(xml.contains("PACEFLOW_MANAGED_SYNC_SCHEDULE"));
        let parsed = parse_windows_task_definition(&xml);
        assert_eq!(parsed, original);
    }

    #[cfg(any(target_os = "windows", test))]
    #[test]
    fn parse_windows_task_definition_detects_stale_command_so_install_can_update() {
        let stale = expected_definition_with_exe(
            ScheduleBackendKind::WindowsTaskScheduler,
            PathBuf::from(r"C:\old\paceflow\target\debug\paceflow.exe"),
        );
        let xml = windows_task_xml(&stale);

        let parsed = parse_windows_task_definition(&xml);

        assert_eq!(
            parsed.exe,
            PathBuf::from(r"C:\old\paceflow\target\debug\paceflow.exe")
        );
        assert_eq!(
            parsed.args,
            vec![
                "sync".to_string(),
                "schedule".to_string(),
                "run".to_string()
            ]
        );
        assert_eq!(parsed.interval_seconds, SCHEDULE_INTERVAL_SECONDS);
    }

    #[derive(Default)]
    struct FakeJobRunner {
        calls: Vec<&'static str>,
    }

    impl ScheduledJobRunner for FakeJobRunner {
        fn ingest(&mut self) -> Result<()> {
            self.calls.push("ingest");
            Ok(())
        }

        fn push_all_projects(&mut self) -> Result<()> {
            self.calls.push("push_all_projects");
            Ok(())
        }
    }

    #[test]
    fn scheduled_run_takes_lock_calls_ingest_then_push_and_writes_log() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let mut runner = FakeJobRunner::default();

        run_scheduled_sync_with_runner(&mut runner)?;

        assert_eq!(runner.calls, vec!["ingest", "push_all_projects"]);
        let log = fs::read_to_string(tempdir.path().join(".paceflow/logs/sync-schedule.log"))?;
        assert!(log.contains("Started scheduled sync"));
        assert!(log.contains("Finished scheduled sync"));
        assert!(!tempdir.path().join(".paceflow/sync_schedule.lock").exists());
        Ok(())
    }

    #[test]
    fn scheduled_run_skips_when_lock_is_already_held() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let app_dir = tempdir.path().join(".paceflow");
        fs::create_dir_all(&app_dir)?;
        fs::write(app_dir.join("sync_schedule.lock"), "locked")?;
        let mut runner = FakeJobRunner::default();

        run_scheduled_sync_with_runner(&mut runner)?;

        assert!(runner.calls.is_empty());
        let log = fs::read_to_string(tempdir.path().join(".paceflow/logs/sync-schedule.log"))?;
        assert!(log.contains("already running"));
        Ok(())
    }
}
