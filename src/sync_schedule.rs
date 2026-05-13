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

pub fn current_backend_with_home(home: PathBuf) -> Box<dyn ScheduleBackend> {
    #[cfg(target_os = "macos")]
    {
        Box::new(LaunchdBackend::new(home))
    }

    #[cfg(target_os = "windows")]
    {
        return Box::new(WindowsTaskBackend::new());
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if systemd_user_available() {
            Box::new(SystemdBackend::new(home))
        } else {
            Box::new(CronBackend::new())
        }
    }

    #[cfg(not(any(unix, target_os = "windows")))]
    {
        Box::new(CronBackend::new())
    }
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
        state_from_file(&self.plist_path(), ScheduleBackendKind::MacOsLaunchd)
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        let path = self.plist_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, launchd_plist(expected))?;
        let _ = Command::new("launchctl").arg("unload").arg(&path).status();
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
        let _ = Command::new("launchctl").arg("unload").arg(&path).status();
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
            state_from_file(&self.service_path(), ScheduleBackendKind::LinuxSystemd)?;
        let timer_state = state_from_file(&self.timer_path(), ScheduleBackendKind::LinuxSystemd)?;
        match (service_state, timer_state) {
            (ScheduleState::Missing, ScheduleState::Missing) => Ok(ScheduleState::Missing),
            (ScheduleState::NonPaceflowArtifact, _) | (_, ScheduleState::NonPaceflowArtifact) => {
                Ok(ScheduleState::NonPaceflowArtifact)
            }
            (ScheduleState::Installed(service), ScheduleState::Installed(timer))
                if service == expected_definition(ScheduleBackendKind::LinuxSystemd)?
                    && timer == expected_definition(ScheduleBackendKind::LinuxSystemd)? =>
            {
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
        if crontab.contains("sync schedule run") && crontab.contains("0 */6 * * *") {
            Ok(ScheduleState::Installed(expected_definition(
                ScheduleBackendKind::LinuxCron,
            )?))
        } else {
            Ok(ScheduleState::Installed(ScheduleDefinition {
                backend: ScheduleBackendKind::LinuxCron,
                exe: PathBuf::from("stale"),
                args: Vec::new(),
                interval_seconds: 0,
            }))
        }
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
        let xml = String::from_utf8_lossy(&output.stdout);
        if !xml.contains("PACEFLOW_MANAGED_SYNC_SCHEDULE") {
            return Ok(ScheduleState::NonPaceflowArtifact);
        }
        if xml.contains("sync schedule run") {
            Ok(ScheduleState::Installed(expected_definition(
                ScheduleBackendKind::WindowsTaskScheduler,
            )?))
        } else {
            Ok(ScheduleState::Installed(ScheduleDefinition {
                backend: ScheduleBackendKind::WindowsTaskScheduler,
                exe: PathBuf::from("stale"),
                args: Vec::new(),
                interval_seconds: 0,
            }))
        }
    }

    fn install(&mut self, expected: &ScheduleDefinition) -> Result<()> {
        let xml_path = env::temp_dir().join("paceflow-sync-task.xml");
        fs::write(&xml_path, windows_task_xml(expected))?;
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
fn state_from_file(path: &Path, backend: ScheduleBackendKind) -> Result<ScheduleState> {
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
    if contents.contains("sync schedule run") && contents.contains("21600") {
        Ok(ScheduleState::Installed(expected_definition(backend)?))
    } else {
        Ok(ScheduleState::Installed(ScheduleDefinition {
            backend,
            exe: PathBuf::from("stale"),
            args: Vec::new(),
            interval_seconds: 0,
        }))
    }
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

#[cfg(target_os = "windows")]
fn windows_task_xml(expected: &ScheduleDefinition) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
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
        escape_xml(&expected.exe.to_string_lossy()),
        escape_xml(&expected.args.join(" ")),
        shell_quote_command(expected),
        expected.interval_seconds
    )
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
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(target_os = "windows")]
fn shell_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\\\""))
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

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
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::tempdir;

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

    #[derive(Debug)]
    struct FakeBackend {
        kind: ScheduleBackendKind,
        state: ScheduleState,
        installs: Vec<ScheduleDefinition>,
        uninstall_count: usize,
    }

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

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_task_xml_uses_absolute_executable_command() {
        let definition = expected_definition_with_exe(
            ScheduleBackendKind::WindowsTaskScheduler,
            PathBuf::from(r"C:\Program Files\Paceflow\paceflow.exe"),
        );

        let xml = windows_task_xml(&definition);

        assert!(xml.contains(r"<Command>C:\Program Files\Paceflow\paceflow.exe</Command>"));
        assert!(xml.contains("<Arguments>sync schedule run</Arguments>"));
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
