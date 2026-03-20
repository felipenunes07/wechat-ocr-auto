$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $dir "wechat_status_dashboard.py"
$py = Join-Path $dir ".venv\\Scripts\\python.exe"
$runtime = Join-Path $dir ".runtime"
$logOut = Join-Path $runtime "dashboard.out.log"
$logErr = Join-Path $runtime "dashboard.err.log"

if (!(Test-Path $script)) {
  throw "Script do painel nao encontrado: $script"
}

if (!(Test-Path $runtime)) {
  New-Item -ItemType Directory -Path $runtime | Out-Null
}

if (!(Test-Path $py)) {
  $py = "python"
}

try {
  $arguments = "-X utf8 `"$script`""
  Start-Process -FilePath $py -ArgumentList $arguments -WorkingDirectory $dir -WindowStyle Hidden -RedirectStandardOutput $logOut -RedirectStandardError $logErr
  Write-Output "PAINEL_ABERTO"
  Write-Output "LOG_ERR=$logErr"
} catch {
  throw "Falha ao abrir painel local. Erro: $($_.Exception.Message)"
}
