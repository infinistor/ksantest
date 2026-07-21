#Requires -Version 5.1
<#
.SYNOPSIS
    Run one test method with pytest (Python 3.12). No HTML report.
    Same shape as Java start-function: config, class, method.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    e.g. 11.151 → 11.151.ini

.EXAMPLE
    .\start-function.ps1 awstests ACL test_bucket_permission_alt_user_read_acp
    .\start-function.ps1 11.151 test_backend test_put_object_replication
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Config,

    [Parameter(Mandatory = $true, Position = 1)]
    [string]$TestClass,

    [Parameter(Mandatory = $true, Position = 2)]
    [string]$TestMethod
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

function Resolve-Python312 {
    $venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $ver = & $venvPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
        if ($ver -eq "3.12") {
            return $venvPython
        }
        Write-Warning ".venv exists but is Python $ver (need 3.12). Falling back to py -3.12."
    }

    if (Get-Command py -ErrorAction SilentlyContinue) {
        $exe = & py -3.12 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $exe) {
            return $exe.Trim()
        }
    }

    throw @"
Python 3.12 not found.
  py -3.12 -m venv .venv
  .\.venv\Scripts\Activate.ps1
  python -m pip install -r requirements.txt
"@
}

function Resolve-ConfigPath {
    param([string]$Name)
    # Always append .ini — do not use HasExtension (e.g. "11.151" looks like it has one).
    $baseName = $Name
    if ($baseName.EndsWith(".ini", [StringComparison]::OrdinalIgnoreCase)) {
        $baseName = $baseName.Substring(0, $baseName.Length - 4)
    }
    return (Join-Path $PSScriptRoot "$baseName.ini")
}

$configPath = Resolve-ConfigPath -Name $Config
if (-not (Test-Path $configPath)) {
    throw "Config file not found: $configPath"
}

$python = Resolve-Python312
$resolveScript = Join-Path $PSScriptRoot "scripts\resolve_test_target.py"
$pytestTarget = & $python $resolveScript $TestClass $TestMethod
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($pytestTarget)) {
    throw "Failed to resolve test target from class='$TestClass' method='$TestMethod'"
}
$pytestTarget = $pytestTarget.Trim()

$env:S3TESTS_INI = $configPath

Write-Host "Python : $python"
Write-Host "Config : $configPath"
Write-Host "Class  : $TestClass"
Write-Host "Method : $TestMethod"
Write-Host "Target : $pytestTarget"
& $python -c "import sys; print(f'Version: {sys.version}')"

Write-Host "`n=== Running pytest ===" -ForegroundColor Cyan
& $python -m pytest -v $pytestTarget
exit $LASTEXITCODE
