#Requires -Version 5.1
<#
.SYNOPSIS
    Run one test method with xUnit (.NET 9). No HTML report.
    Same shape as Java start-function: config, class, method.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    e.g. 11.151 -> 11.151.ini

.PARAMETER TestClass
    Test class name, e.g. ACL, Backend, CopyObject. (SSE-C class is named sseC)

.PARAMETER TestMethod
    Test method name, e.g. TestPutObjectReplication

.EXAMPLE
    .\start-function.ps1 awstests ACL TestBucketPermissionAltUserReadAcp
    .\start-function.ps1 11.151 Backend TestPutObjectReplication
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

# Always append .ini - do not use HasExtension (e.g. "11.151" looks like it has one).
if (-not $Config.EndsWith(".ini", [StringComparison]::OrdinalIgnoreCase)) {
    $Config = "$Config.ini"
}
$ConfigPath = Join-Path $PSScriptRoot $Config
if (-not (Test-Path $ConfigPath)) {
    $available = (Get-ChildItem $PSScriptRoot -Filter *.ini | ForEach-Object { $_.BaseName }) -join ", "
    throw "Config not found: $ConfigPath`nAvailable: $available"
}

$env:S3TESTS_INI = $ConfigPath
# FullyQualifiedName 완전일치 (~ 부분일치면 TestCopyObject가 TestCopyObjectReplication 등까지 잡힘)
$Filter = "FullyQualifiedName=s3tests.Test.$TestClass.$TestMethod"

Write-Host "Config : $ConfigPath"
Write-Host "Class  : $TestClass"
Write-Host "Method : $TestMethod"
Write-Host "Filter : $Filter"

Write-Host "`n=== Building ===" -ForegroundColor Cyan
dotnet build s3tests.csproj -clp:ErrorsOnly
if ($LASTEXITCODE -ne 0) {
    throw "Build failed - fix the compile errors above before running tests."
}

Write-Host "`n=== Running dotnet test ===" -ForegroundColor Cyan
dotnet test s3tests.csproj --no-build --filter $Filter --logger "console;verbosity=detailed"
exit $LASTEXITCODE
