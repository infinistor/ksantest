#Requires -Version 5.1
<#
.SYNOPSIS
    Run all S3 tests and generate HTML report (Python 3.12).

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    Default: config → config.ini

.PARAMETER NoOpen
    Do not open Result_python.html after generation.

.EXAMPLE
    .\start.ps1
    .\start.ps1 awstests
    .\start.ps1 11.151
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Config = "config",

    [switch]$NoOpen
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
Install Python 3.12, then either:
  py -3.12 -m venv .venv
  .\.venv\Scripts\Activate.ps1
  python -m pip install -r requirements.txt
or ensure 'py -3.12' works on PATH.
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
$env:S3TESTS_INI = $configPath

$ResultsDir = Join-Path $PSScriptRoot "results"
$XunitDir = Join-Path (Split-Path $PSScriptRoot -Parent) "xunit-to-html"
$JunitXml = Join-Path $ResultsDir "junit.xml"
$MergedXml = Join-Path $XunitDir "Result_python.xml"
$ReportHtml = Join-Path $XunitDir "Result_python.html"
$SaxonJar = Join-Path $XunitDir "saxon9he.jar"
$XslFile = Join-Path $XunitDir "xunit_to_html.xsl"

if (-not (Test-Path $SaxonJar)) {
    throw "Saxon not found: $SaxonJar"
}
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found. Java 8+ is required for HTML report generation."
}

New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
Remove-Item "$ResultsDir\*.xml", $ReportHtml, $MergedXml -ErrorAction SilentlyContinue

Write-Host "Python : $python"
Write-Host "Config : $configPath"
& $python -c "import sys; print(f'Version: {sys.version}')"

Write-Host "`n=== Running pytest (parallel) ===" -ForegroundColor Cyan
& $python -m pytest -v -n auto "--junitxml=$JunitXml" s3tests\tests
$pytestExit = $LASTEXITCODE

if (-not (Test-Path $JunitXml)) {
    throw "pytest failed with exit code $pytestExit (no JUnit XML produced)"
}

Copy-Item $JunitXml $MergedXml -Force

Write-Host "`n=== Generating HTML report ===" -ForegroundColor Cyan
Push-Location $XunitDir
try {
    & java -jar $SaxonJar "-o:$ReportHtml" "-s:$MergedXml" "-xsl:$XslFile"
    if ($LASTEXITCODE -ne 0) {
        throw "Saxon transformation failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}

Write-Host "`nReport: $ReportHtml" -ForegroundColor Green
if (-not $NoOpen) {
    Start-Process $ReportHtml
}

exit $pytestExit
