#Requires -Version 7.4
# 빌드 후 서버 배포용 묶음을 만든다.
#   dist\ReplicationTest-<커밋 수>-<짧은 해시>.tar.gz
#     └── ReplicationTest-<커밋 수>-<짧은 해시>/
#         ├── replication-usage-test.jar
#         ├── run.sh              (755)
#         └── config.ini.example
# Windows tar.exe 는 실행 권한을 남기지 못해서 .NET TarWriter 로 권한을 직접 넣는다.
# mvn 과 git 이 PATH 에 있어야 한다.
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$count = (git rev-list --count HEAD).Trim()
$hash = (git rev-parse --short HEAD).Trim()
$name = "ReplicationTest-$count-$hash"

# 커밋되지 않은 변경이 있으면 이름의 해시와 내용이 다를 수 있다. 막지는 않고 알린다.
if (git status --porcelain -- .) {
	Write-Warning "이 폴더에 커밋되지 않은 변경이 있습니다. $hash 와 내용이 다를 수 있습니다."
}

Write-Host '빌드: mvn -q clean package'
mvn -q clean package
if ($LASTEXITCODE -ne 0) {
	Write-Error '빌드 실패. 묶음을 만들지 않습니다.' -ErrorAction Continue
	exit 1
}

$jar = Join-Path $PSScriptRoot 'target\replication-usage-test.jar'
if (-not (Test-Path $jar)) {
	Write-Error "빌드 결과 $jar 가 없습니다." -ErrorAction Continue
	exit 1
}

# 리눅스에서 $'\r' 오류가 나지 않도록 CR 을 지운 바이트를 돌려준다.
function Get-LfBytes([string]$path) {
	$text = [IO.File]::ReadAllText((Join-Path $PSScriptRoot $path)) -replace "`r", ''
	return [Text.UTF8Encoding]::new($false).GetBytes($text)
}

$mode755 = [IO.UnixFileMode]493  # rwxr-xr-x
$mode644 = [IO.UnixFileMode]420  # rw-r--r--
$now = [DateTimeOffset]::Now

function Add-TarEntry($writer, [string]$path, [Formats.Tar.TarEntryType]$type, $mode, [IO.Stream]$data) {
	$entry = [Formats.Tar.PaxTarEntry]::new($type, $path)
	$entry.Mode = $mode
	$entry.ModificationTime = $now
	$entry.Uid = 0
	$entry.Gid = 0
	if ($data) { $entry.DataStream = $data }
	$writer.WriteEntry($entry)
}

New-Item -ItemType Directory -Force (Join-Path $PSScriptRoot 'dist') | Out-Null
$out = Join-Path $PSScriptRoot "dist\$name.tar.gz"

$file = [IO.File]::Create($out)
$gzip = [IO.Compression.GZipStream]::new($file, [IO.Compression.CompressionLevel]::Optimal)
$tar = [Formats.Tar.TarWriter]::new($gzip, [Formats.Tar.TarEntryFormat]::Pax, $false)
$jarStream = [IO.File]::OpenRead($jar)
try {
	Add-TarEntry $tar "$name/" Directory $mode755 $null
	Add-TarEntry $tar "$name/replication-usage-test.jar" RegularFile $mode644 $jarStream
	Add-TarEntry $tar "$name/run.sh" RegularFile $mode755 ([IO.MemoryStream]::new((Get-LfBytes 'run.sh')))
	Add-TarEntry $tar "$name/config.ini.example" RegularFile $mode644 ([IO.MemoryStream]::new((Get-LfBytes 'config.ini.example')))
}
finally {
	$tar.Dispose()
	$gzip.Dispose()
	$file.Dispose()
	$jarStream.Dispose()
}

Write-Host "완료: dist\$name.tar.gz"
tar -tvzf $out
