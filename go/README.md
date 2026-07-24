# S3 compatibility test for Go

Java `testV2`와 Python 포트를 기준으로 AWS SDK for Go v2로 작성한 S3 호환성 테스트 스위트입니다. Java/Python과 동일한 INI 및 `xunit-to-html` 리포트 흐름을 사용합니다.

## 구동환경

- Go 1.25 이상
- HTML 리포트용 Java 8 이상
- 저장소의 `xunit-to-html/saxon9he.jar`

## 설정

`config.ini`에서 서버 주소와 자격 증명을 설정합니다. S3 호환 서버는 `URL`과 포트를 입력하며 path-style 요청을 사용합니다. AWS S3는 `URL`을 비워 둡니다. Go SDK v2는 Signature V4만 지원하므로 `SignatureVersion = 4`로 설정해야 합니다.

## 테스트 실행

### 전체 실행 및 HTML 리포트

Windows PowerShell:

```powershell
cd go
.\start.ps1
.\start.ps1 awstests
.\start.ps1 awstests -NoOpen
.\start.ps1 awstests -Parallel 8
```

설정 인자는 `.ini` 없이 기본 이름만 넘깁니다 (`config` → `config.ini`, `11.151` → `11.151.ini`).

```powershell
.\start.ps1 11.151
```

Linux/macOS:

```sh
cd go
./start.sh
./start.sh awstests
```

기본적으로 최대 4개 테스트 클래스를 병렬 실행합니다. `PutBucket`과 `ListBuckets`는 bucket 이름 및 전체 목록을 검증하므로 순차 실행을 유지합니다. Linux/macOS에서는 `GO_TEST_PARALLEL`로 동시성을 변경합니다.

```bash
GO_TEST_PARALLEL=8 ./start.sh awstests
```

결과 파일:

- Go 원본 이벤트: `go/test-results.json`
- JUnit XML: `xunit-to-html/Result_go.xml`
- HTML: `xunit-to-html/Result_go.html`

`-NoOpen`을 지정하지 않으면 PowerShell 스크립트가 HTML을 자동으로 엽니다.

## 단일 테스트 실행

Python 및 Java 실행 스크립트와 동일하게 설정, 테스트 클래스, 테스트 메서드 순서로 지정합니다. 설정은 `.ini` 없이 기본 이름만 넘기고, 메서드는 snake_case와 Java camelCase를 모두 허용합니다.

```powershell
.\start-function.ps1 config PutBucket test_bucket_create_naming_bad_ip
.\start-function.ps1 config PutBucket testBucketCreateNamingGoodLong60
.\start-function.ps1 11.151 Multipart testPutObjectOverwriteMultipartUpload
```

```bash
./start-function.sh config PutBucket test_bucket_create_naming_bad_ip
./start-function.sh awstests Versioning testVersioningObjMixPutAndMultipart
```

Go에서는 관련 시나리오를 table-driven subtest로 묶습니다. 위 명령은 내부적으로 `TestPutBucket/test_bucket_create_naming_bad_ip`와 같은 `go test -run` 대상으로 변환됩니다.

## HTML 리포트 준비

`xunit-to-html`은 저장소 루트의 Git submodule입니다.

```powershell
git submodule update --init --recursive
```

일반 `git pull` 이후 submodule이 비어 있다면 위 명령을 다시 실행합니다.

## 프로젝트 구조

```text
go/
├── *_test.go                # 클래스별 Go 테스트
├── internal/testconfig/     # INI 설정
├── cmd/resolve-test/        # 클래스·메서드 대상 해석
├── cmd/junit-report/        # go test JSON → JUnit XML
├── start.ps1 / start.sh
├── start-function.ps1 / start-function.sh
└── MIGRATION.md
```

## Java/Python 결과와 비교

동일한 INI로 세 스위트를 실행한 뒤 공용 HTML 리포트를 비교합니다.

- Java: `java/start.ps1` → `xunit-to-html/Result_java.html`
- Python: `python/start.ps1` → `xunit-to-html/Result_python.html`
- Go: `go/start.ps1` → `xunit-to-html/Result_go.html`
