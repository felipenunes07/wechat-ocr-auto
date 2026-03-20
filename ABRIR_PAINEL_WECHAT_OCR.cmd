@echo off
setlocal
set "DIR=%~dp0"
set "SCRIPT=%DIR%wechat_status_dashboard.py"
set "PY=%DIR%.venv\Scripts\python.exe"

if not exist "%SCRIPT%" (
  echo Script do painel nao encontrado: "%SCRIPT%"
  pause
  exit /b 1
)

if exist "%PY%" (
  start "" "%PY%" -X utf8 "%SCRIPT%"
  exit /b 0
)

where python >nul 2>nul
if errorlevel 1 (
  echo Python nao encontrado.
  pause
  exit /b 1
)

start "" python -X utf8 "%SCRIPT%"
exit /b 0
