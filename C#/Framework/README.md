# S3 compatibility test for .Net Framework

이 테스트는 아마존에서 제공하는 S3 Api를 사용하여 S3 호환 프로그램에 대한 기능점검 프로그램입니다.

별도의 유틸을 이용하여 보기 쉽게 결과물을 출력하는 기능을 포함하고 있습니다.

## 구동환경

*  .Net Framework : 4.7.2 이상
*  OS : Window 10 이상

## How to Build

### Visual Studio 설치
- Window 10에서만 동작합니다.
- Visual Studio : [Link](https://visualstudio.microsoft.com/ko/)
- dotnet : [Link](https://dotnet.microsoft.com/en-us/download/dotnet/3.1)
- Build 명령 : Ctrl + Shift + B

## 테스트 방법

### 테스트 실행
``` ps1
#설정파일 경로
SET S3TESTS_INI=sample.ini
dotnet test s3tests2.dll --test-adapter-path:. --nologo --logger "junit;"
```

### 레포트 출력
```ps1
COPY TestResults\TestResults.xml ..\..\..\..\..\xunit-to-html\result_netframework.xml
cd ..\..\..\..\..\xunit-to-html\
java -jar saxon9he.jar -o:result_netframework.html -s:result_netframework.xml -xsl:xunit_to_html.xsl
start result_netframework.html
```

## 테스트 결과 레포트 확인

- 테스트 결과 레포트는 [링크](https://github.com/Zir0-93/xunit-to-html)를 사용하여 작성했습니다.
- 테스트 결과는 **../xunit-to-html-master/result_netframework.html**로 확인 가능합니다.