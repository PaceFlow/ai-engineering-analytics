# `paceflow` Install Notes

`paceflow` is a single-file CLI binary. Download the archive for your platform, extract it, and run:

```bash
paceflow --help
paceflow ingest
```

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
cargo build
export PATH="$(pwd)/target/debug:$PATH"
paceflow --help
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
