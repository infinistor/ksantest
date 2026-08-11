# S3 compatibility test for .NET

아마존 S3 API를 사용해 S3 호환 구현의 기능을 점검하는 테스트입니다.
[xunit-to-html](https://github.com/Zir0-93/xunit-to-html)로 HTML 리포트를 생성합니다.

## 구동환경

- **.NET 9** 이상 (`s3tests.csproj` `TargetFramework` 참고)
- HTML 리포트용: Java 8+, `xunit-to-html/saxon9he.jar`

## 환경 구성 (Windows)

Windows에서 `start.ps1` / `start-function.ps1` 실행이 막히면, PowerShell을 한 번 열어 실행 정책을 설정합니다.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### .NET SDK 설치

- 다운로드: [.NET 9](https://dotnet.microsoft.com/download/dotnet/9.0)

Linux(예: RHEL/CentOS 계열) 패키지 설치 예:

```bash
sudo rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
sudo yum install -y dotnet-sdk-9.0
```

배포판별 최신 안내는 [Microsoft 문서](https://learn.microsoft.com/dotnet/core/install/linux)를 참고하세요.

## 설정 파일

우선순위:

1. 환경 변수 `S3TESTS_INI` (스크립트가 설정)
2. 기본값: `MainConfig.STR_DEF_FILENAME` (현재 `config.ini`)

표준 섹션: `[S3]`, `[Fixtures]`, `[Main User]`, `[Alt User]`, `[Backend User]`

스크립트 기본값은 `config.ini`입니다. (`.\start.ps1` → `config.ini`)

## How to Build

```powershell
dotnet build s3tests.csproj -c Release
```

## 테스트 실행

### 스크립트 (권장)

```powershell
cd dotnet
.\start.ps1
.\start.ps1 awstests
.\start.ps1 11.151 -NoOpen
```

Linux:

```bash
cd dotnet
./start.sh
./start.sh awstests
./start.sh 11.151
```

리포트: `../xunit-to-html/Result_dotnet.html`

### 단일 테스트 (클래스/메서드)

HTML 리포트 없이 콘솔로만 실행합니다.

```powershell
.\start-function.ps1 11.151 ACL TestBucketPermissionAltUserReadAcp
```

```bash
./start-function.sh 11.151 ACL TestBucketPermissionAltUserReadAcp
```

### dotnet으로 직접 실행

#### Windows

```powershell
cd dotnet
$env:S3TESTS_INI = "$PWD\config.ini"
dotnet build s3tests.csproj -clp:ErrorsOnly
dotnet test s3tests.csproj --no-build --logger "junit;LogFilePath=$PWD\TestResults\junit.xml"
Copy-Item TestResults\junit.xml ..\xunit-to-html\Result_dotnet.xml -Force
cd ..\xunit-to-html
java -jar saxon9he.jar -o:Result_dotnet.html -s:Result_dotnet.xml -xsl:xunit_to_html.xsl
```

#### Linux

```bash
cd dotnet
export S3TESTS_INI="$PWD/config.ini"
dotnet build s3tests.csproj -clp:ErrorsOnly
dotnet test s3tests.csproj --no-build --logger "junit;LogFilePath=$PWD/TestResults/junit.xml"
cp TestResults/junit.xml ../xunit-to-html/Result_dotnet.xml
cd ../xunit-to-html
java -jar saxon9he.jar -o:Result_dotnet.html -s:Result_dotnet.xml -xsl:xunit_to_html.xsl
```

## 테스트 결과 레포트

- 도구: [xunit-to-html](https://github.com/Zir0-93/xunit-to-html) (저장소 루트의 Git submodule)
- 결과 파일: `../xunit-to-html/Result_dotnet.html`
- **사전 준비**
  - submodule 초기화: 일반 `git pull`만으로는 `xunit-to-html` 내용이 받아지지 않습니다.

    ```powershell
    git submodule update --init --recursive
    ```

    pull 때 함께 받으려면 `git pull --recurse-submodules`, 또는 pull 후 위 명령을 다시 실행합니다.
  - `xunit-to-html/saxon9he.jar` 필요
