# S3 compatibility test for Python

Java `testV2` 코드를 기준으로 포팅한 S3 호환성 테스트 스위트입니다.

## 구동환경

- **Python 3.12.12** (`.python-version` 참고)
- boto3, pytest (버전은 `requirements.txt`에 고정)

## 환경 구성

Windows에서 `Activate.ps1` / `start.ps1` 실행이 막히면, PowerShell을 한 번 열어 실행 정책을 설정합니다.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

```powershell
cd python
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux:

```bash
cd python
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 설정 파일

Java와 동일한 INI 형식을 사용합니다.

우선순위:

1. `S3TESTS_INI` 환경변수
2. `config.ini` (현재 디렉터리)

표준 섹션: `[S3]`, `[Fixtures]`, `[Main User]`, `[Alt User]`, `[Backend User]`

## 테스트 실행

```powershell
$env:S3TESTS_INI="config.ini"
pytest -v s3tests/tests
```

단일 모듈:

```powershell
pytest -v s3tests/tests/test_put_object.py
```

태그 필터:

```powershell
pytest -v -m "tag('PUT')" s3tests/tests
```

## HTML 리포트

Java와 동일한 xunit-to-html 파이프라인을 사용합니다.

```powershell
.\start.ps1
.\start.ps1 -Config config.ini
.\start.bat config.ini
```

또는 수동:

```powershell
$env:S3TESTS_INI="config.ini"
pytest -v --junitxml=results/junit.xml s3tests/tests
copy results\junit.xml ..\xunit-to-html\Result_python.xml
cd ..\xunit-to-html
java -jar saxon9he.jar -o:Result_python.html -s:Result_python.xml -xsl:xunit_to_html.xsl
```

**사전 준비**: `xunit-to-html/saxon9he.jar` 필요 ([xunit-to-html](https://github.com/Zir0-93/xunit-to-html))

## 프로젝트 구조

```
python/
├── s3tests/
│   ├── config.py          # S3Config (INI)
│   ├── test_base.py       # S3TestBase (Java TestBase)
│   ├── data/              # 상수, UserData 등
│   ├── utils/             # Utils, CheckSum, NetUtils
│   ├── auth/              # SigV2/V4 (Post, CSE)
│   ├── ksan/              # KsanClient
│   └── tests/             # 38개 pytest 모듈 (~1100 테스트)
├── config.ini
├── requirements.txt
├── pytest.ini
├── start.bat / start.sh
└── MIGRATION.md
```

## Java 결과와 비교

동일한 `config.ini`로 Java와 Python 테스트를 각각 실행한 뒤 HTML 리포트를 비교합니다.

- Java: `java/start.bat` → `xunit-to-html/Result_java.html`
- Python: `python/start.bat` → `xunit-to-html/Result_python.html`
