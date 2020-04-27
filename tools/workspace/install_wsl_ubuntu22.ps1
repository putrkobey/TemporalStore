$ErrorActionPreference = "Continue"

$LogPath = Join-Path $PSScriptRoot "wsl_install_ubuntu22.log"
Start-Transcript -Path $LogPath -Append

Write-Host "=== TemporalStore WSL2 Ubuntu 22.04 setup ==="
Write-Host "Log: $LogPath"
Write-Host "Started: $(Get-Date -Format o)"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
Write-Host "Administrator: $isAdmin"

Write-Host ""
Write-Host "Step 1: Enable WSL optional component"
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
Write-Host "WSL feature exit code: $LASTEXITCODE"

