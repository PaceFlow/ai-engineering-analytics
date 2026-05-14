# `paceflow` Install Notes

`paceflow` is a single-file CLI binary. Download the archive for your platform, extract it, and run:

```bash
paceflow --help
paceflow ingest
```

## Install with Cargo

If you have the Rust toolchain (or `cargo-binstall`) on your machine, this is the fastest path.

### `cargo binstall` (prebuilt binary, no compilation)

```bash
cargo binstall paceflow
paceflow --help
```

`cargo binstall` downloads the matching prebuilt release artifact for your target triple and drops `paceflow` in `~/.cargo/bin`. No Rust compilation required. Install `cargo-binstall` itself with `cargo install cargo-binstall` or grab its prebuilt binary from [cargo-bins/cargo-binstall](https://github.com/cargo-bins/cargo-binstall#installation).

### `cargo install` (compiles from source)

```bash
cargo install --locked paceflow
paceflow --help
```

Builds from crates.io with your local toolchain. Slower than `binstall` (rusqlite-bundled + reqwest + tokio take a few minutes), but works on any target where Rust compiles. If install fails, try `rustup update` first.

Both commands install to `~/.cargo/bin/paceflow`, which rustup adds to `PATH` by default. See [Add `paceflow` To `PATH`](#add-paceflow-to-path) if it isn't.

## Prebuilt Releases

Download a release from [GitHub Releases](https://github.com/PaceFlow/ai-engineering-analytics/releases).

Supported release targets:

| Platform | Asset |
| --- | --- |
| Windows x86_64 | `paceflow-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (glibc) | `paceflow-x86_64-unknown-linux-gnu.tar.gz` |
| macOS Apple Silicon | `paceflow-aarch64-apple-darwin.tar.gz` |

Windows PowerShell example:

```powershell
$version = "v0.2.0"
$asset = "paceflow-x86_64-pc-windows-msvc.zip"
Invoke-WebRequest `
  -Uri "https://github.com/PaceFlow/ai-engineering-analytics/releases/download/$version/$asset" `
  -OutFile $asset
Expand-Archive .\$asset -DestinationPath .\paceflow
.\paceflow\paceflow.exe --help
```

macOS/Linux example:

```bash
version="v0.2.0"
asset="paceflow-x86_64-unknown-linux-gnu.tar.gz"
curl -L "https://github.com/PaceFlow/ai-engineering-analytics/releases/download/${version}/${asset}" -o "${asset}"
tar -xzf "${asset}"
./paceflow-x86_64-unknown-linux-gnu/paceflow --help
```

For macOS Apple Silicon, use `paceflow-aarch64-apple-darwin.tar.gz` as the asset name.

## Add `paceflow` To `PATH`

Git hooks and team setup scripts call `paceflow` by name, so the binary must be available on `PATH`.

macOS/Linux, for a downloaded release:

```bash
mkdir -p ~/.local/bin
cp ./paceflow-x86_64-unknown-linux-gnu/paceflow ~/.local/bin/paceflow
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
exec zsh
paceflow --help
```

For macOS Apple Silicon, replace `paceflow-x86_64-unknown-linux-gnu` with `paceflow-aarch64-apple-darwin`. If you use Bash, append the `PATH` line to `~/.bashrc` instead of `~/.zshrc`.

macOS/Linux, for a local development build:

```bash
cargo install --path .
paceflow --help
paceflow --version           # paceflow 0.2.0 (<git-sha> clean, <commit-time>)
```

This builds in release mode and copies the binary to `~/.cargo/bin/paceflow`, which is on `PATH` by default for rustup installs. Re-run the command (add `--force` after the first time) to pick up changes. `paceflow --version` embeds the git commit SHA and a `clean`/`dirty` flag from `build.rs`, so you can confirm which checkout you're running:

```bash
paceflow --version                          # what's on PATH
git rev-parse --short=12 origin/main        # what main currently points at
git fetch origin main && git diff --stat HEAD origin/main   # check if you're behind main
```

If the SHA in `paceflow --version` matches `git rev-parse --short=12 origin/main` and shows `clean`, you're running an exact build of `origin/main`.

If `~/.cargo/bin` is not on `PATH`, add it:

```bash
echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zshrc
exec zsh
```

Windows PowerShell, for a local development build:

```powershell
cargo install --path .
paceflow --help
paceflow --version           # paceflow 0.2.0 (<git-sha> clean, <commit-time>)
```

This installs `paceflow.exe` to `%USERPROFILE%\.cargo\bin\`, which rustup adds to the user `Path`. Re-run with `--force` after the first install to pick up changes. `paceflow --version` embeds the git commit SHA and a `clean`/`dirty` flag from `build.rs`, so you can confirm which checkout you're running:

```powershell
paceflow --version                          # what's on PATH
git rev-parse --short=12 origin/main        # what main currently points at
git fetch origin main; git diff --stat HEAD origin/main   # check if you're behind main
(Get-Command paceflow).Source               # which paceflow.exe resolves on PATH
```

If the SHA in `paceflow --version` matches `git rev-parse --short=12 origin/main` and shows `clean`, you're running an exact build of `origin/main`.

If `cargo install` reports the binary is not on `PATH`, add `%USERPROFILE%\.cargo\bin` for the current user and open a new PowerShell window:

```powershell
[Environment]::SetEnvironmentVariable(
  "Path",
  [Environment]::GetEnvironmentVariable("Path", "User") + ";$env:USERPROFILE\.cargo\bin",
  "User"
)
```

Windows PowerShell, for a downloaded release:

```powershell
$installDir = "$env:USERPROFILE\bin\paceflow"
New-Item -ItemType Directory -Force -Path $installDir | Out-Null
Copy-Item .\paceflow\paceflow.exe $installDir
[Environment]::SetEnvironmentVariable(
  "Path",
  [Environment]::GetEnvironmentVariable("Path", "User") + ";$installDir",
  "User"
)
```

Open a new PowerShell window, then verify:

```powershell
paceflow --help
```

## Build From Source

```bash
git clone https://github.com/PaceFlow/ai-engineering-analytics.git
cd ai-engineering-analytics
cargo install --path . --force
```

## macOS Gatekeeper

If Gatekeeper blocks `paceflow`, go to `System Settings > Privacy & Security` and click `Open Anyway`, then rerun the binary.

Fresh extractions can inherit quarantine from the downloaded archive. If needed, clear quarantine on the extracted folder:

```bash
xattr -dr com.apple.quarantine paceflow-aarch64-apple-darwin
./paceflow-aarch64-apple-darwin/paceflow --help
```

## Requirements

- Git must be installed and available on `PATH`
- `paceflow` reads local Claude Code sessions from `~/.claude/projects/*/*.jsonl`
- `paceflow` reads local Codex sessions from `~/.codex/sessions`
- `paceflow` reads local Cursor state/history from the OS config directory under `Cursor/User`
- `paceflow` reads local OpenCode history from `~/.local/share/opencode/opencode.db` and `~/.local/share/opencode/storage/session_diff`
- GitHub PR sync requires `paceflow github token` or `PACEFLOW_GITHUB_TOKEN`

## Optional Overrides

- `PACEFLOW_HOME` changes the base directory for Paceflow's local analytics state
- `PACEFLOW_CURSOR_STATE_PATH` points to a custom Cursor `state.vscdb`
- `PACEFLOW_CURSOR_HISTORY_PATH` points to a custom Cursor history directory
- `PACEFLOW_OPENCODE_DB_PATH` points to a custom OpenCode database
- `PACEFLOW_GITHUB_TOKEN` overrides the saved GitHub token for PR sync

Local analytics state lives under `~/.paceflow/paceflow.db` by default. The source assistant and git data remains in its original location.
