#Requires -Version 5.1
<#
.SYNOPSIS
    Run all Go S3 tests and generate HTML report.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    Default: config → config.ini

.PARAMETER NoOpen
    Do not open Result_go.html after generation.

.PARAMETER Parallel
    Max concurrent test classes (go test -parallel). Default: 4.

.EXAMPLE
    .\start.ps1
    .\start.ps1 awstests
    .\start.ps1 11.151 -NoOpen
    .\start.ps1 awstests -Parallel 8
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Config = "config",

    [switch]$NoOpen,

    [ValidateRange(1, 64)]
    [int]$Parallel = 4
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

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
if (-not (Test-Path $configPath -PathType Leaf)) {
    throw "Config file not found: $configPath"
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "Go was not found on PATH. Go 1.25 or later is required."
}

$env:S3TESTS_INI = $configPath
$JsonPath = Join-Path $PSScriptRoot "test-results.json"
$XunitDir = Join-Path (Split-Path $PSScriptRoot -Parent) "xunit-to-html"
$XmlPath = Join-Path $XunitDir "Result_go.xml"
$ReportPath = Join-Path $XunitDir "Result_go.html"
$SaxonJar = Join-Path $XunitDir "saxon9he.jar"
$XslFile = Join-Path $XunitDir "xunit_to_html.xsl"

if (-not (Test-Path $SaxonJar)) { throw "Saxon not found: $SaxonJar" }
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw "Java not found. Java 8+ is required for HTML report generation." }
Remove-Item $JsonPath, $XmlPath, $ReportPath -ErrorAction SilentlyContinue

Write-Host "Config : $configPath"
Write-Host "Parallel: $Parallel classes"
& go version

Write-Host "`n=== Running Go tests ===" -ForegroundColor Cyan
& go test -parallel $Parallel -json -count=1 . | Tee-Object -FilePath $JsonPath
$testExit = $LASTEXITCODE

if (-not (Test-Path $JsonPath -PathType Leaf)) {
    throw "go test failed with exit code $testExit (no JSON output produced)"
}

Write-Host "`n=== Generating HTML report ===" -ForegroundColor Cyan
Get-Content $JsonPath | & go run ./cmd/junit-report "-output=$XmlPath"
if ($LASTEXITCODE -ne 0) {
    throw "JUnit XML generation failed with exit code $LASTEXITCODE"
}

Push-Location $XunitDir
try {
    & java -jar $SaxonJar "-o:$ReportPath" "-s:$XmlPath" "-xsl:$XslFile"
    if ($LASTEXITCODE -ne 0) { throw "Saxon transformation failed with exit code $LASTEXITCODE" }
}
finally { Pop-Location }

Write-Host "`nReport: $ReportPath" -ForegroundColor Green
if (-not $NoOpen -and (Test-Path $ReportPath -PathType Leaf)) {
    Start-Process $ReportPath
}

exit $testExit
