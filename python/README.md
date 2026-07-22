# S3 compatibility test for Python

Java `testV2` 코드를 기준으로 포팅한 S3 호환성 테스트 스위트입니다.

## 구동환경

- **Python 3.12.x** (`pyproject.toml`의 `requires-python`: `>=3.12,<3.13`)
- boto3, pytest (버전은 `requirements.txt`에 고정)

## 환경 구성

처음 한 번만 하면 됩니다. Git에는 `.venv`(가상환경 폴더)가 **포함되지 않으므로**, 각자 PC에서 만들어 패키지를 설치해야 합니다.

흐름 요약:

1. PC에 **Python 3.12** 설치
2. (Windows) PowerShell 실행 정책 허용
3. **`setup.ps1` / `setup.sh` 실행** → `.venv` 생성 + `requirements.txt` 패키지 설치
4. `config.ini` 준비 후 `start.ps1` / `start.sh`로 테스트

### 1. Python 3.12 설치

PC에 Python 3.12가 없으면 먼저 설치합니다. 설치 후 터미널을 **새로 연 다음** 버전을 확인합니다.

#### Windows

[python.org](https://www.python.org/downloads/release/python-31212/)에서 **Windows installer (64-bit)** 를 받거나, winget으로 설치합니다.

```powershell
winget install Python.Python.3.12
```

설치 시 **Add python.exe to PATH** 를 체크합니다.

```powershell
py -3.12 --version
```

`Python 3.12.x`처럼 나오면 성공입니다.

#### Linux (Ubuntu/Debian 예)

```bash
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3.12-dev
python3.12 --version
```

#### Linux (Rocky Linux 10.1)

Rocky Linux 10의 기본 Python은 3.12입니다.

```bash
sudo dnf install -y python3 python3-pip python3-devel
python3 --version
```

`3.12.x`이면 아래 setup에서 `python3`를 사용합니다.

### 2. PowerShell 실행 정책 (Windows)

`setup.ps1` / `start.ps1` 실행이 막히면 PowerShell에서 **한 번** 설정합니다.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3. 가상환경 구성 (권장: setup 스크립트)

저장소의 `python` 폴더로 이동한 뒤 setup 스크립트를 실행합니다.
이 스크립트가 하는 일:

- Python 3.12로 **`.venv` 폴더 생성** (없으면)
- `.venv` 안의 pip로 **`requirements.txt` 패키지 설치**

#### Windows

```powershell
cd python
.\setup.ps1
```

성공 시 `Setup complete.` 가 출력됩니다. 이후 테스트는:

```powershell
.\start.ps1
```

직접 pytest를 쓰려면 먼저 가상환경을 켭니다. 프롬프트 앞에 `(.venv)` 가 보이면 활성화된 상태입니다.

```powershell
.\.venv\Scripts\Activate.ps1
pytest -v s3tests/tests
```

#### Linux (Ubuntu / Rocky Linux 10.1 동일)

Ubuntu와 Rocky 10.1 모두 같은 명령을 사용합니다. (`setup.sh`가 `python3.12` 또는 시스템 `python3`(3.12)를 자동 선택)

```bash
cd python
chmod +x setup.sh start.sh
./setup.sh
```

성공 시 `Setup complete.` 가 출력됩니다. 이후 테스트는:

```bash
./start.sh
```

직접 pytest를 쓰려면:

```bash
source .venv/bin/activate
pytest -v s3tests/tests
```

#### setup이 하는 일을 수동으로 하려면

스크립트 없이 동일하게 구성할 때의 예시입니다.
**미리 만들어 둔 `.venv`가 Git에 있는 것이 아니라**, 아래 첫 줄이 그 폴더를 새로 만듭니다.

Windows:

```powershell
cd python
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Linux:

```bash
cd python
python3.12 -m venv .venv   # Rocky 10.1은 python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

- `requirements.txt`: Git에 있는 **패키지 목록**
- `.venv`: 로컬에만 생기는 **가상환경 폴더** (커밋하지 않음)

## 설정 파일

Java와 동일한 INI 형식을 사용합니다.

우선순위:

1. `S3TESTS_INI` 환경변수
2. `config.ini` (현재 디렉터리)

표준 섹션: `[S3]`, `[Fixtures]`, `[Main User]`, `[Alt User]`, `[Backend User]`

## 테스트 실행

가상환경 활성화 후:

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
.\start.ps1 config -NoOpen
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
├── requirements.txt       # 설치할 패키지 목록 (Git에 포함)
├── setup.ps1 / setup.sh   # .venv 생성 + pip install
├── pytest.ini
├── start.ps1 / start.sh
└── MIGRATION.md
```

## Java 결과와 비교

동일한 `config.ini`로 Java와 Python 테스트를 각각 실행한 뒤 HTML 리포트를 비교합니다.

- Java: `java/start.ps1` → `xunit-to-html/Result_java.html`
- Python: `python/start.ps1` → `xunit-to-html/Result_python.html`
