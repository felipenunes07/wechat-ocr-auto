$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $dir "wechat_receipt_daemon.py"
$db = Join-Path $dir "wechat_receipt_state.db"
$excel = Join-Path $dir "pagamentos_wechat.xlsx"
$sinkConfigPath = Join-Path $dir "sink_config.json"
$logOut = Join-Path $dir "wechat_receipt.out.log"
$logErr = Join-Path $dir "wechat_receipt.err.log"
$log = $logOut
$pidf = Join-Path $dir "wechat_receipt.pid"

$py = Join-Path $dir ".venv\\Scripts\\python.exe"
if (!(Test-Path $py)) {
  throw "Python da venv nao encontrado: $py"
}

$pyVersion = (& $py -X utf8 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')").Trim()
if ($LASTEXITCODE -ne 0) {
  throw "Falha ao consultar a versao do Python da venv."
}
if ($pyVersion -ne "3.12") {
  throw "Este projeto exige Python 3.12 na .venv. Versao atual detectada: $pyVersion"
}

function Get-WeChatFileStorageRoot {
  $doc = [Environment]::GetFolderPath("MyDocuments")
  $wechatFiles = Join-Path $doc "WeChat Files"
  $xwechatFiles = Join-Path $doc "xwechat_files"

  if (Test-Path $xwechatFiles) {
    $accounts = Get-ChildItem -Path $xwechatFiles -Directory -ErrorAction SilentlyContinue | Where-Object {
      $_.Name -like "wxid_*" -and (Test-Path (Join-Path $_.FullName "msg\attach"))
    } | Sort-Object LastWriteTime -Descending

    if ($accounts -and $accounts.Count -gt 0) {
      return $accounts[0].FullName
    }
  }

  if (Test-Path $wechatFiles) {
    # Pick the most recently changed account folder that has FileStorage.
    $accounts = Get-ChildItem -Path $wechatFiles -Directory -ErrorAction SilentlyContinue | Where-Object {
      Test-Path (Join-Path $_.FullName "FileStorage")
    } | Sort-Object LastWriteTime -Descending

    if ($accounts -and $accounts.Count -gt 0) {
      return (Join-Path $accounts[0].FullName "FileStorage")
    }
  }

  return $null
}

function Convert-ToCliArg([string]$value) {
  if ($null -eq $value) { return '""' }
  if ($value -notmatch '[\s"]') { return $value }
  return '"' + ($value -replace '(\\*)"', '$1$1\"') + '"'
}

function Invoke-EnvironmentHealthCheck {
  $healthScript = @'
import importlib
modules = [
    ("pywxdump", "pywxdump"),
    ("pywinauto", "pywinauto"),
    ("watchdog", "watchdog"),
    ("rapidocr_onnxruntime", "rapidocr_onnxruntime"),
    ("PIL", "PIL"),
]
errors = []
for label, module_name in modules:
    try:
        importlib.import_module(module_name)
    except Exception as exc:
        errors.append(f"{label}:{type(exc).__name__}:{exc}")
if errors:
    print("ERROR:" + " | ".join(errors))
    raise SystemExit(1)
print("OK:env_health")
'@
  $output = $healthScript | & $py -X utf8 -
  if ($LASTEXITCODE -ne 0) {
    $message = (($output | Out-String).Trim())
    if ([string]::IsNullOrWhiteSpace($message)) { $message = "env_health_failed" }
    throw "Health-check do ambiente falhou: $message"
  }
}

$watch = Get-WeChatFileStorageRoot

if (!(Test-Path $script)) { throw "Script nao encontrado: $script" }
if (!$watch -or !(Test-Path $watch)) {
  throw "Pasta WeChat nao encontrada. Verifique se existe 'Documentos\WeChat Files\<wxid>\FileStorage' ou 'Documentos\xwechat_files\<wxid>'."
}

$sinkMode = "excel"
$gsheetRef = ""
$gsheetWorksheet = ""
$gsheetReviewWorksheet = ""
$recentFilesHours = 0
$originalWaitSeconds = 90
$tempCorrelationSeconds = 30
$thumbCandidatesEnabled = $false
$manualOrderGuardEnabled = $true
$manualBurstGapSeconds = 2
$manualBurstMaxSeconds = 8
$resolutionMode = "db-first"
$verificationColumnName = "STATUS_VERIFICACAO"
$uiForceDownloadEnabled = $false
$uiForceDelaySeconds = 15
$uiForceScope = "mapped-groups"
$uiFocusPolicy = "immediate"
$uiBatchMode = "group-sequential"
$uiItemTimeoutSeconds = 5
$uiRetryBackoffSeconds = "5,10,20,40"
$uiWindowBackends = "win32,uia"
$uiWindowClasses = "WeChatMainWndForPC,mmui::MainWindow,Qt51514QWindowIcon,Base_PowerMessageWindow,Chrome_WidgetWin_0"
$sheetOrderScope = "per_talker"
$sheetMaterializationOrder = "desc"
$sheetCommitOrder = "asc"
$dbMergePath = Join-Path $dir ".runtime\\wechat_merge.db"
$googleCredentialsPath = Join-Path $dir "google_service_account.json"
$effectiveSinkMode = "excel"
if (Test-Path $sinkConfigPath) {
  $sinkConfig = Get-Content $sinkConfigPath -Raw | ConvertFrom-Json
  if ($sinkConfig.sink_mode) { $sinkMode = [string]$sinkConfig.sink_mode }
  if ($sinkConfig.spreadsheet_url) { $gsheetRef = [string]$sinkConfig.spreadsheet_url }
  if ($sinkConfig.spreadsheet_id -and [string]::IsNullOrWhiteSpace($gsheetRef)) { $gsheetRef = [string]$sinkConfig.spreadsheet_id }
  if ($sinkConfig.worksheet) { $gsheetWorksheet = [string]$sinkConfig.worksheet }
  if ($null -ne $sinkConfig.review_worksheet) { $gsheetReviewWorksheet = [string]$sinkConfig.review_worksheet }
  if ($sinkConfig.recent_files_hours) { $recentFilesHours = [int]$sinkConfig.recent_files_hours }
  if ($sinkConfig.original_wait_seconds) { $originalWaitSeconds = [int]$sinkConfig.original_wait_seconds }
  if ($sinkConfig.temp_correlation_seconds) { $tempCorrelationSeconds = [int]$sinkConfig.temp_correlation_seconds }
  if ($null -ne $sinkConfig.thumb_candidates_enabled) { $thumbCandidatesEnabled = [bool]$sinkConfig.thumb_candidates_enabled }
  if ($null -ne $sinkConfig.manual_order_guard_enabled) { $manualOrderGuardEnabled = [bool]$sinkConfig.manual_order_guard_enabled }
  if ($sinkConfig.manual_burst_gap_seconds) { $manualBurstGapSeconds = [int]$sinkConfig.manual_burst_gap_seconds }
  if ($sinkConfig.manual_burst_max_seconds) { $manualBurstMaxSeconds = [int]$sinkConfig.manual_burst_max_seconds }
  if ($sinkConfig.resolution_mode) { $resolutionMode = [string]$sinkConfig.resolution_mode }
  if ($sinkConfig.verification_column_name) { $verificationColumnName = [string]$sinkConfig.verification_column_name }
  if ($null -ne $sinkConfig.ui_force_download_enabled) { $uiForceDownloadEnabled = [bool]$sinkConfig.ui_force_download_enabled }
  if ($sinkConfig.ui_force_delay_seconds) { $uiForceDelaySeconds = [int]$sinkConfig.ui_force_delay_seconds }
  if ($sinkConfig.ui_force_scope) { $uiForceScope = [string]$sinkConfig.ui_force_scope }
  if ($sinkConfig.ui_focus_policy) { $uiFocusPolicy = [string]$sinkConfig.ui_focus_policy }
  if ($sinkConfig.ui_batch_mode) { $uiBatchMode = [string]$sinkConfig.ui_batch_mode }
  if ($sinkConfig.ui_item_timeout_seconds) { $uiItemTimeoutSeconds = [int]$sinkConfig.ui_item_timeout_seconds }
  if ($sinkConfig.ui_retry_backoff_seconds) { $uiRetryBackoffSeconds = (($sinkConfig.ui_retry_backoff_seconds | ForEach-Object { [string]$_ }) -join ",") }
  if ($sinkConfig.ui_window_backends) { $uiWindowBackends = (($sinkConfig.ui_window_backends | ForEach-Object { [string]$_ }) -join ",") }
  if ($sinkConfig.ui_window_classes) { $uiWindowClasses = (($sinkConfig.ui_window_classes | ForEach-Object { [string]$_ }) -join ",") }
  if ($sinkConfig.sheet_order_scope) { $sheetOrderScope = [string]$sinkConfig.sheet_order_scope }
  if ($sinkConfig.sheet_materialization_order) { $sheetMaterializationOrder = [string]$sinkConfig.sheet_materialization_order }
  if ($sinkConfig.sheet_commit_order) { $sheetCommitOrder = [string]$sinkConfig.sheet_commit_order }
  if ($sinkConfig.db_merge_path) { $dbMergePath = [string]$sinkConfig.db_merge_path }
  if ($sinkConfig.google_credentials_path) {
    $googleCredentialsPath = [string]$sinkConfig.google_credentials_path
    if (-not [System.IO.Path]::IsPathRooted($googleCredentialsPath)) {
      $googleCredentialsPath = Join-Path $dir $googleCredentialsPath
    }
  }
}
if (-not [System.IO.Path]::IsPathRooted($dbMergePath)) {
  $dbMergePath = Join-Path $dir $dbMergePath
}

if (-not $uiForceDownloadEnabled -and -not $manualOrderGuardEnabled) {
  throw "Modo manual exige manual_order_guard_enabled=true em sink_config.json."
}
if (-not $uiForceDownloadEnabled) {
  $manualOrderGuardEnabled = $true
}

Invoke-EnvironmentHealthCheck

# Atualiza automaticamente o mapa hash->nome de grupo antes de iniciar.
$mapUpdater = Join-Path $dir "refresh_group_map.py"
$dbResolverReady = $true
if (Test-Path $mapUpdater) {
  $runtimeDir = Join-Path $dir ".runtime"
  if (!(Test-Path $runtimeDir)) { New-Item -ItemType Directory -Path $runtimeDir | Out-Null }
  $mapStdOut = Join-Path $runtimeDir "refresh_group_map.stdout.log"
  $mapStdErr = Join-Path $runtimeDir "refresh_group_map.stderr.log"
  Remove-Item -LiteralPath $mapStdOut, $mapStdErr -ErrorAction SilentlyContinue
  $mapProc = Start-Process -FilePath $py -ArgumentList @("-X", "utf8", ('"' + $mapUpdater + '"')) -WorkingDirectory $dir -NoNewWindow -RedirectStandardOutput $mapStdOut -RedirectStandardError $mapStdErr -Wait -PassThru
  $mapExitCode = $mapProc.ExitCode
  $mapOutput = @()
  if (Test-Path $mapStdOut) { $mapOutput += Get-Content -Path $mapStdOut -ErrorAction SilentlyContinue }
  if (Test-Path $mapStdErr) { $mapOutput += Get-Content -Path $mapStdErr -ErrorAction SilentlyContinue }
  if ($mapOutput) {
    $mapOutput | ForEach-Object { Write-Output $_ }
  }
  if ($mapExitCode -ne 0) {
    $dbResolverReady = $false
    if ($resolutionMode -eq "db-first" -and -not $uiForceDownloadEnabled) {
      Write-Warning "refresh_group_map.py falhou no WeChat 4 atual. Iniciando fallback seguro em path-only viewer-only; nomes automaticos de grupo ficam indisponiveis por enquanto."
      $resolutionMode = "path-only"
    } elseif ($resolutionMode -eq "db-first") {
      throw "refresh_group_map.py falhou em modo db-first (exit=$mapExitCode). Inicializacao abortada."
    }
    if ($resolutionMode -ne "path-only") {
      Write-Warning "refresh_group_map.py falhou (exit=$mapExitCode)."
    }
  }
} elseif ($resolutionMode -eq "db-first") {
  throw "refresh_group_map.py nao encontrado e resolution_mode=db-first exige merge DB valido."
}

if (Test-Path $pidf) {
  $oldPid = (Get-Content $pidf -ErrorAction SilentlyContinue | Select-Object -First 1)
  if ($oldPid) {
    $pOld = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
    if ($pOld) {
      Write-Output "JA_EM_EXECUCAO PID=$oldPid"
      exit 0
    }
  }
}

$arguments = @(
  "-X", "utf8",
  "-u", $script,
  "--watch-root", $watch,
  "--db-path", $db,
  "--reconcile-seconds", "90",
  "--recent-files-hours", "$recentFilesHours",
  "--min-confidence", "0.55",
  "--resolution-mode", $resolutionMode,
  "--db-merge-path", $dbMergePath,
  "--original-wait-seconds", "$originalWaitSeconds",
  "--temp-correlation-seconds", "$tempCorrelationSeconds",
  "--thumb-candidates-enabled", ($(if ($thumbCandidatesEnabled) { "true" } else { "false" })),
  "--manual-order-guard-enabled", ($(if ($manualOrderGuardEnabled) { "true" } else { "false" })),
  "--manual-burst-gap-seconds", "$manualBurstGapSeconds",
  "--manual-burst-max-seconds", "$manualBurstMaxSeconds",
  "--verification-column-name", $verificationColumnName,
  "--ui-force-download-enabled", ($(if ($uiForceDownloadEnabled) { "true" } else { "false" })),
  "--ui-force-delay-seconds", "$uiForceDelaySeconds",
  "--ui-force-scope", $uiForceScope,
  "--ui-focus-policy", $uiFocusPolicy,
  "--ui-batch-mode", $uiBatchMode,
  "--ui-item-timeout-seconds", "$uiItemTimeoutSeconds",
  "--ui-retry-backoff-seconds", $uiRetryBackoffSeconds,
  "--ui-window-backends", $uiWindowBackends,
  "--ui-window-classes", $uiWindowClasses,
  "--sheet-order-scope", $sheetOrderScope,
  "--sheet-materialization-order", $sheetMaterializationOrder,
  "--sheet-commit-order", $sheetCommitOrder,
  "--client-map-path", (Join-Path $dir "clientes_grupos.json")
)

if ($sinkMode -eq "google-sheets") {
  if ([string]::IsNullOrWhiteSpace($gsheetRef)) {
    Write-Warning "Google Sheets configurado sem spreadsheet_url/spreadsheet_id. Fallback para Excel local."
    $effectiveSinkMode = "excel"
  } elseif (!(Test-Path $googleCredentialsPath)) {
    Write-Warning "Credencial Google nao encontrada: $googleCredentialsPath. Fallback para Excel local."
    $effectiveSinkMode = "excel"
  } else {
    $effectiveSinkMode = "google-sheets"
  }
}

if ($effectiveSinkMode -eq "google-sheets") {
  $arguments += @("--sink-mode", "google-sheets", "--gsheet-ref", $gsheetRef, "--google-credentials-path", $googleCredentialsPath)
  if (-not [string]::IsNullOrWhiteSpace($gsheetWorksheet)) {
    $arguments += @("--gsheet-worksheet", $gsheetWorksheet)
  }
  if (-not [string]::IsNullOrWhiteSpace($gsheetReviewWorksheet)) {
    $arguments += @("--gsheet-review-worksheet", $gsheetReviewWorksheet)
  }
} else {
  $arguments += @("--sink-mode", "excel", "--excel-path", $excel)
}

$argumentLine = ($arguments | ForEach-Object { Convert-ToCliArg ([string]$_) }) -join " "
$p = $null
try {
  $p = Start-Process -FilePath $py -ArgumentList $argumentLine -WorkingDirectory $dir -WindowStyle Hidden -RedirectStandardOutput $logOut -RedirectStandardError $logErr -PassThru
} catch {
  Write-Warning "Falha ao iniciar processo: $($_.Exception.Message)"
  $p = $null
}
if ($p) {
  Start-Sleep -Seconds 2
  $p.Refresh()
  if ($p.HasExited) {
    Write-Output "FALHOU_INICIAR. O processo encerrou logo apos iniciar."
    Write-Output "EXIT_CODE=$($p.ExitCode)"
    if (Test-Path $logOut) {
      Write-Output "----- ULTIMAS LINHAS OUT -----"
      Get-Content -Path $logOut -Tail 40 -ErrorAction SilentlyContinue
    }
    if (Test-Path $logErr) {
      Write-Output "----- ULTIMAS LINHAS ERR -----"
      Get-Content -Path $logErr -Tail 40 -ErrorAction SilentlyContinue
    }
    exit 1
  }

  $p.Id | Set-Content -Path $pidf -Encoding ascii
  Write-Output "INICIADO PID=$($p.Id)"
  Write-Output "LOG=$log"
  if ($effectiveSinkMode -eq "google-sheets") {
    Write-Output "DESTINO=GOOGLE_SHEETS"
    Write-Output "PLANILHA=$gsheetRef"
  } else {
    if ($sinkMode -eq "google-sheets") {
      Write-Output "DESTINO_FALLBACK=EXCEL (credencial/config Google indisponivel neste PC)"
    }
    Write-Output "EXCEL=$excel"
  }
  Write-Output "RESOLUTION_MODE=$resolutionMode"
  Write-Output "THUMB_CANDIDATES_ENABLED=$thumbCandidatesEnabled"
  Write-Output "MANUAL_ORDER_GUARD_ENABLED=$manualOrderGuardEnabled"
  Write-Output "MANUAL_BURST_GAP_SECONDS=$manualBurstGapSeconds"
  Write-Output "MANUAL_BURST_MAX_SECONDS=$manualBurstMaxSeconds"
  Write-Output "DB_MERGE_PATH=$dbMergePath"
  Write-Output "UI_FORCE_DOWNLOAD_ENABLED=$uiForceDownloadEnabled"
  Write-Output "UI_FORCE_DELAY_SECONDS=$uiForceDelaySeconds"
  Write-Output "UI_WINDOW_BACKENDS=$uiWindowBackends"
  Write-Output "UI_WINDOW_CLASSES=$uiWindowClasses"
  Write-Output "SHEET_ORDER_SCOPE=$sheetOrderScope"
  Write-Output "SHEET_MATERIALIZATION_ORDER=$sheetMaterializationOrder"
  Write-Output "SHEET_COMMIT_ORDER=$sheetCommitOrder"
} else {
  Write-Output "FALHOU_INICIAR. Veja log: $log"
  exit 1
}

