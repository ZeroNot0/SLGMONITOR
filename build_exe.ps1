$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install pyinstaller

pyinstaller --noconfirm --clean --onefile `
  --name "SLGMonitor" `
  --add-data "frontend;frontend" `
  --add-data "mapping;mapping" `
  --add-data "labels;labels" `
  --add-data "deploy;deploy" `
  --add-data "backend;backend" `
  --add-data "scripts;scripts" `
  --add-data "pipeline\steps;pipeline\steps" `
  --add-data "request;request" `
  app\app_launcher.py

Write-Host "Build complete. EXE at dist\SLGMonitor.exe"