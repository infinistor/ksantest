#Requires -Version 5.1
<#
.SYNOPSIS
    Run all tests in one Go test class. No HTML report.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    e.g. 11.151 -> 11.151.ini

.PARAMETER TestClass
    Test class name, e.g. PutBucket, Post, Multipart

.EXAMPLE
    .\start-class.ps1 awstests Post
    .\start-class.ps1 11.151 Multipart
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Config,

    [Parameter(Mandatory = $true, Position = 1)]
    [string]$TestClass
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Resolve-ConfigPath {
    param([string]$Name)
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

$resolved = & go run ./cmd/resolve-test $TestClass
if ($LASTEXITCODE -ne 0 -or -not $resolved) {
    throw "Failed to resolve Go tests from class='$TestClass'"
}
$testNames = ([string]($resolved | Select-Object -Last 1)).Trim()
if ([string]::IsNullOrWhiteSpace($testNames)) {
    throw "Failed to resolve Go tests from class='$TestClass'"
}

$env:S3TESTS_INI = $configPath
Write-Host "Config : $configPath"
Write-Host "Class  : $TestClass"
Write-Host "Target : $testNames"
& go version

$runPattern = "^($testNames)$"
Write-Host "Filter : $runPattern"
& go test -v -count=1 -run $runPattern .
exit $LASTEXITCODE
