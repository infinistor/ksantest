# S3 compatibility test for Java

아마존 S3 API를 사용해 S3 호환 구현의 기능을 점검하는 테스트입니다.
[xunit-to-html](https://github.com/Zir0-93/xunit-to-html)로 HTML 리포트를 생성합니다.

## 구동환경

- **Java 21** 이상 (`pom.xml` `release` 참고)
- **Maven 3.9.5** 이상
- HTML 리포트용: Python 3 (`scripts/merge_junit_results.py`), `xunit-to-html/saxon9he.jar`

## 환경 구성 (Windows)

Windows에서 `start.ps1` / `build.ps1` 실행이 막히면, PowerShell을 한 번 열어 실행 정책을 설정합니다.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 설정 파일

우선순위:

1. Maven: `-Ds3tests.ini=<파일>` / JAR: `-f` / `--config`
2. 기본값: `S3Config.STR_FILENAME` (소스에 정의)

표준 섹션: `[S3]`, `[Fixtures]`, `[Main User]`, `[Alt User]`, `[Backend User]`

스크립트 기본값은 `config.ini`입니다.

## How to Build

테스트는 건너뛰고 패키징만 할 때:

```powershell
mvn clean package -DskipTests
```

또는:

```powershell
.\build.ps1
```

## 테스트 실행

### 스크립트 (권장)

```powershell
cd java
.\start.ps1
.\start.ps1 -Config config.ini
.\start.ps1 config -NoOpen
```

Linux:

```bash
cd java
./start.sh
./start.sh config
```

리포트: `../xunit-to-html/Result_java.html`

### mvn으로 직접 실행

#### Windows

```powershell
cd java
mvn clean
mvn test surefire-report:report "-Ds3tests.ini=config.ini"
python ..\scripts\merge_junit_results.py .\target\results\*.xml > ..\xunit-to-html\Result_java.xml
cd ..\xunit-to-html
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
```

#### Linux

```bash
cd java
mvn clean
mvn test surefire-report:report "-Ds3tests.ini=config.ini"
python ../scripts/merge_junit_results.py ./target/results/*.xml > ../xunit-to-html/Result_java.xml
cd ../xunit-to-html
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
```

### 빌드한 JAR로 실행

리포트는 생성되지 않고, 결과가 콘솔에 출력됩니다.

```powershell
java -jar target\s3tests_java-1.0.0-jar-with-dependencies.jar -f config.ini
```

```bash
java -jar target/s3tests_java-1.0.0-jar-with-dependencies.jar -f config.ini
```

클래스/메서드 지정 예:

```powershell
java -jar target\s3tests_java-1.0.0-jar-with-dependencies.jar -f config.ini -c PutObject -m testPutObject
```

## 테스트 결과 레포트

- 도구: [xunit-to-html](https://github.com/Zir0-93/xunit-to-html) (저장소 루트의 Git submodule)
- 결과 파일: `../xunit-to-html/Result_java.html`
- **사전 준비**
  - submodule 초기화: 일반 `git pull`만으로는 `xunit-to-html` 내용이 받아지지 않습니다.

    ```powershell
    git submodule update --init --recursive
    ```

    pull 때 함께 받으려면 `git pull --recurse-submodules`, 또는 pull 후 위 명령을 다시 실행합니다.
  - `xunit-to-html/saxon9he.jar` 필요

## Python 결과와 비교

동일한 `config.ini`로 Java와 Python 테스트를 각각 실행한 뒤 HTML 리포트를 비교합니다.

- Java: `java/start.ps1` → `xunit-to-html/Result_java.html`
- Python: `python/start.ps1` → `xunit-to-html/Result_python.html`
