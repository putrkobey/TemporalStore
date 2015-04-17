param(
  [Parameter(ValueFromRemainingArguments=$true)]
  [string[]]$HookArgs
)

$Distro = if ($env:MATRIXARK_WSL_DISTRO) { $env:MATRIXARK_WSL_DISTRO } else { "Ubuntu2204LocalUser" }
$Repo = if ($env:MATRIXARK_REPO_ROOT) { $env:MATRIXARK_REPO_ROOT } else { "<repo>" }

$stdin = [Console]::In.ReadToEnd()
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stdin))
$quotedArgs = ($HookArgs | ForEach-Object { "'" + ($_ -replace "'", "'\''") + "'" }) -join " "

