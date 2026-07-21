#Requires -Version 5.1
param(
    [Parameter(Position = 0)]
    [string]$Config = "config",

    [switch]$NoOpen
)

Set-Location $PSScriptRoot

if (-not $Config.EndsWith(".ini", [StringComparison]::OrdinalIgnoreCase)) {
    $Config = "$Config.ini"
}
if (-not (Test-Path $Config)) {
    throw "Config not found: $Config"
}

$RepoRoot = Split-Path $PSScriptRoot -Parent
$MergeScript = Join-Path $RepoRoot "scripts\merge_junit_results.py"
$XunitDir = Join-Path $RepoRoot "xunit-to-html"

Remove-Item "$XunitDir\Result_java.html", "$XunitDir\Result_java.xml" -ErrorAction SilentlyContinue

mvn clean
mvn test surefire-report:report "-Ds3tests.ini=$Config"
$mvnExit = $LASTEXITCODE

# PowerShell '>' is UTF-16; use cmd for UTF-8 XML
cmd /c "python `"$MergeScript`" target\results\*.xml > `"$XunitDir\Result_java.xml`""
if (-not (Test-Path "$XunitDir\Result_java.xml") -or (Get-Item "$XunitDir\Result_java.xml").Length -eq 0) {
    throw "Failed to create Result_java.xml"
}

Push-Location $XunitDir
try {
    java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
    if (-not $NoOpen) { Start-Process "Result_java.html" }
}
finally {
    Pop-Location
}

exit $mvnExit
