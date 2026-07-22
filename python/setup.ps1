#Requires -Version 5.1
<#
.SYNOPSIS
    Create .venv (Python 3.12) and install requirements.txt.

.EXAMPLE
    .\setup.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Resolve-Python312 {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        $exe = & py -3.12 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $exe) {
            return $exe.Trim()
        }
    }

    foreach ($name in @("python3.12", "python")) {
        if (Get-Command $name -ErrorAction SilentlyContinue) {
            $ver = & $name -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
            if ($ver -eq "3.12") {
                return (Get-Command $name).Source
            }
        }
    }

    throw @"
Python 3.12 not found.
Install Python 3.12 first (see README), then run this script again.
Expected: py -3.12 --version
"@
}

$python = Resolve-Python312
$venvDir = Join-Path $PSScriptRoot ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$requirements = Join-Path $PSScriptRoot "requirements.txt"

if (-not (Test-Path $requirements)) {
    throw "requirements.txt not found: $requirements"
}

Write-Host "=== Python environment setup ===" -ForegroundColor Cyan
Write-Host "System Python: $python"
& $python -c "import sys; print(f'Version: {sys.version}')"

if (-not (Test-Path $venvPython)) {
    Write-Host "`n[1/2] Creating virtual environment (.venv) ..." -ForegroundColor Yellow
    & $python -m venv $venvDir
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create .venv"
    }
}
else {
    Write-Host "`n[1/2] .venv already exists — skip create" -ForegroundColor DarkGray
}

Write-Host "`n[2/2] Installing packages from requirements.txt ..." -ForegroundColor Yellow
& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    throw "pip upgrade failed"
}
& $venvPython -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw "pip install -r requirements.txt failed"
}

Write-Host "`nSetup complete." -ForegroundColor Green
Write-Host "Next:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  # or run tests: .\start.ps1"
Write-Host ""
Write-Host "Note: .venv is local only (not committed to git)."
