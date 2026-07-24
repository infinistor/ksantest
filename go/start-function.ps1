#Requires -Version 5.1
<#
.SYNOPSIS
    Run one Go test method. No HTML report.
    Same shape as Java/Python start-function: config, class, method.

.PARAMETER Config
    INI config base name (without .ini). Always resolved as <name>.ini under this script directory.
    e.g. 11.151 → 11.151.ini

.EXAMPLE
    .\start-function.ps1 config PutBucket test_bucket_create_naming_bad_ip
    .\start-function.ps1 awstests PutBucket testBucketCreateNamingGoodLong60
    .\start-function.ps1 11.151 Multipart testPutObjectOverwriteMultipartUpload
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

$resolved = & go run ./cmd/resolve-test $TestClass $TestMethod
if ($LASTEXITCODE -ne 0 -or -not $resolved) {
    throw "Failed to resolve Go test from class='$TestClass' method='$TestMethod'"
}
$testName = ([string]($resolved | Select-Object -Last 1)).Trim()
if ([string]::IsNullOrWhiteSpace($testName)) {
    throw "Failed to resolve Go test from class='$TestClass' method='$TestMethod'"
}

$env:S3TESTS_INI = $configPath
Write-Host "Config : $configPath"
Write-Host "Class  : $TestClass"
Write-Host "Method : $TestMethod"
Write-Host "Target : $testName"
& go version

& go test -v -count=1 -run "^$testName$" .
exit $LASTEXITCODE
