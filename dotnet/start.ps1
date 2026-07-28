#Requires -Version 5.1
<#
.SYNOPSIS
    Run all S3 tests and generate HTML report (.NET 9 / xUnit).

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    Default: config -> config.ini

.PARAMETER NoOpen
    Do not open Result_dotnet.html after generation.

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

# Always append .ini - do not use HasExtension (e.g. "11.151" looks like it has one).
if (-not $Config.EndsWith(".ini", [StringComparison]::OrdinalIgnoreCase)) {
    $Config = "$Config.ini"
}
$ConfigPath = Join-Path $PSScriptRoot $Config
if (-not (Test-Path $ConfigPath)) {
    $available = (Get-ChildItem $PSScriptRoot -Filter *.ini | ForEach-Object { $_.BaseName }) -join ", "
    throw "Config not found: $ConfigPath`nAvailable: $available"
}

$RepoRoot = Split-Path $PSScriptRoot -Parent
$XunitDir = Join-Path $RepoRoot "xunit-to-html"
$ResultsDir = Join-Path $PSScriptRoot "TestResults"
$JunitXml = Join-Path $ResultsDir "junit.xml"
$MergedXml = Join-Path $XunitDir "Result_dotnet.xml"
$ReportHtml = Join-Path $XunitDir "Result_dotnet.html"
$SaxonJar = Join-Path $XunitDir "saxon9he.jar"
$XslFile = Join-Path $XunitDir "xunit_to_html.xsl"

if (-not (Test-Path $SaxonJar)) {
    throw "Saxon not found: $SaxonJar"
}
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found. Java 8+ is required for HTML report generation."
}

New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
Remove-Item $JunitXml, $MergedXml, $ReportHtml -ErrorAction SilentlyContinue

$env:S3TESTS_INI = $ConfigPath

Write-Host "Config : $ConfigPath"
dotnet --version

Write-Host "`n=== Building ===" -ForegroundColor Cyan
dotnet build s3tests.csproj -clp:ErrorsOnly
if ($LASTEXITCODE -ne 0) {
    throw "Build failed - fix the compile errors above before running tests."
}

Write-Host "`n=== Running dotnet test ===" -ForegroundColor Cyan
# JunitXml.TestLogger resolves LogFilePath relative to the project dir, not --results-directory.
dotnet test s3tests.csproj --no-build --logger "junit;LogFilePath=$JunitXml"
$testExit = $LASTEXITCODE

if (-not (Test-Path $JunitXml)) {
    throw "dotnet test produced no JUnit XML (exit code $testExit)"
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

exit $testExit
