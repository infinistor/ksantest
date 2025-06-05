# S3 compatibility test for .Net Core

이 테스트는 아마존에서 제공하는 S3 Api를 사용하여 S3 호환 프로그램에 대한 기능점검 프로그램입니다.

별도의 유틸을 이용하여 보기 쉽게 결과물을 출력하는 기능을 포함하고 있습니다.

## 구동환경

*  .Net Core : 7 이상

## How to Build

### dotnet 설치

#### Window 10
- 설치파일 링크 : [Link](https://dotnet.microsoft.com/en-us/download/dotnet/3.1)

#### CentOS 7
```
sudo rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
sudo yum install -y dotnet-sdk-3.1
```

### Build
``` shell
dotnet build -c Release
```

## 테스트 방법

### 테스트 실행
``` ps1
#설정파일 경로
SET S3TESTS_INI=sample.ini
dotnet test s3tests.dll --test-adapter-path:. --nologo --logger "junit;"
```
### 레포트 출력
```ps1
COPY TestResults\TestResults.xml ..\..\..\..\..\xunit-to-html\result_netcore.xml
cd ..\..\..\..\..\xunit-to-html\
java -jar saxon9he.jar -o:result_netcore.html -s:result_netcore.xml -xsl:xunit_to_html.xsl
start result_netcore.html
```
## 테스트 결과 레포트 확인

- 테스트 결과 레포트는 [링크](https://github.com/Zir0-93/xunit-to-html)를 사용하여 작성했습니다.
- 테스트 결과는 **../xunit-to-html-master/result_netcore.html**로 확인 가능합니다.