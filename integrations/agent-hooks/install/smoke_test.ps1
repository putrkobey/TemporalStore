param(
  [string]$Mode = $env:TEMPORALSTORE_AGENT_MODE
)

if (-not $Mode) {
  $Mode = "dry-run"
}

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Node = $env:CODEX_NODE_PATH
if (-not $Node) {
  $Node = "node"
}

$Marker = "TEMPORALSTORE_AGENT_HOOK_SMOKE_$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
