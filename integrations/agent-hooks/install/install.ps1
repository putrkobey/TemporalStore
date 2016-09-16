param(
  [ValidateSet("codex", "claude", "both", "generic")]
  [string]$Agent = "codex",
  [ValidateSet("dry-run", "wsl", "native", "remote", "docker")]
  [string]$Mode = "dry-run",
  [string]$Dest = "$env:USERPROFILE\plugins\temporalstore-agent-hooks",
  [string]$Node = $env:CODEX_NODE_PATH,
  [string]$CodexBin = "codex",
  [string]$ClaudeSettings = "$env:USERPROFILE\.claude\settings.json",
  [string]$Endpoint = "http://127.0.0.1:18080",
  [string]$Repo = "/opt/github-services/TemporalStore",
  [string]$WslRepo = "/opt/github-services/TemporalStore",
  [string]$WslDistro = $env:TEMPORALSTORE_WSL_DISTRO,
  [switch]$SkipCodexAdd
)

if (-not $Node) { $Node = "node" }

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$PluginSource = Join-Path $Root "codex\plugin"
New-Item -ItemType Directory -Force $Dest | Out-Null
Copy-Item -Path (Join-Path $PluginSource "*") -Destination $Dest -Recurse -Force

$Template = Join-Path $Dest "hooks\hooks.template.json"
$Hooks = Join-Path $Dest "hooks\hooks.json"
$PluginRoot = $Dest.Replace("\", "/")
$NodeForJson = $Node.Replace("\", "/")
(Get-Content -Raw $Template).Replace("__NODE__", $NodeForJson).Replace("__PLUGIN_ROOT__", $PluginRoot) | Set-Content -Path $Hooks -Encoding ASCII

@"
TEMPORALSTORE_AGENT_MODE=$Mode
TEMPORALSTORE_AGENT_ENDPOINT=$Endpoint
TEMPORALSTORE_REPO=$Repo
TEMPORALSTORE_WSL_REPO=$WslRepo
TEMPORALSTORE_AGENT_PROJECT=TemporalStore
TEMPORALSTORE_AGENT_NAME=$Agent
