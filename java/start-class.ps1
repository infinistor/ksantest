#Requires -Version 5.1
<#
.SYNOPSIS
    Run all tests in one test class with JUnit. No HTML report.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    Default base name examples: config, awstests, 11.151

.PARAMETER TestClass
    Test class name, e.g. ACL, Post, CopyObject

.EXAMPLE
    .\start-class.ps1 awstests Post
    .\start-class.ps1 11.151 ACL
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

if (-not $Config.EndsWith(".ini", [StringComparison]::OrdinalIgnoreCase)) {
    $Config = "$Config.ini"
}
if (-not (Test-Path $Config)) {
    throw "Config not found: $Config"
}

Write-Host "Config : $Config"
Write-Host "Class  : $TestClass"
Write-Host "Filter : -Dtest=$TestClass"

Write-Host "`n=== Running mvn test ===" -ForegroundColor Cyan
mvn test "-Ds3tests.ini=$Config" "-Dtest=$TestClass"
exit $LASTEXITCODE
