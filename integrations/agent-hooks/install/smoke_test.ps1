param(
  [string]$Mode = $env:TEMPORALSTORE_AGENT_MODE
)

if (-not $Mode) {
  $Mode = "dry-run"
}
