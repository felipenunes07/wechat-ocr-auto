@echo off
setlocal
set "DIR=%~dp0"
set "PS1=%DIR%PARAR_WECHAT_OCR.ps1"

if not exist "%PS1%" (
  echo Script nao encontrado: "%PS1%"
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
if errorlevel 1 (
  echo.
  echo Falha ao parar WeChat OCR.
  pause
  exit /b 1
)

echo.
echo WeChat OCR parado. Esta janela pode ser fechada.
timeout /t 3 >nul
exit /b 0
