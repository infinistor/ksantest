# S3 compatibility test for Java

이 테스트는 아마존에서 제공하는 S3 Api를 사용하여 S3 호환 프로그램에 대한 기능점검 프로그램입니다.

별도의 유틸을 이용하여 보기 쉽게 결과물을 출력하는 기능을 포함하고 있습니다.

## 구동환경

*  Java : 11 이상
*  maven : 3.6.3 이상
*  OS : Window 10, Centos 7.5 이상

## How to Build

- 빌드하기 위해서 테스트 과정을 생략하는 옵션을 추가해야 합니다.

``` shell
mvn clean package -DskipTests
```

## 테스트 방법

### mvn으로 테스트할 경우
#### Window

``` bat
@REM 설정파일 경로
SET S3TESTS_INI=sample.ini
call mvn clean
call mvn test surefire-report:report
.\junit-merger.exe target/surefire-reports > xunit-to-html-master\Result_java.xml
cd xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
start Result_java.html
```

#### Linux

``` bash
#설정파일 경로
export S3TESTS_INI=sample.ini
mvn clean
mvn test surefire-report:report
python ./merge_junit_results.py ./target/surefire-reports/*.xml > ../xunit-to-html-master/Result_java.xml
cd ../xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
```

### 빌드한 경우

- 빌드한 프로그램으로 테스트 할 경우 레포트는 생성할 수 없습니다.
- 테스트 결과가 콘솔창에 출력됩니다.

#### Windows
``` shell
SET S3TESTS_INI=sample.ini
java -jar s3tests-java
```

#### Linux
``` shell
export S3TESTS_INI=sample.ini
./s3tests-java
```

## 테스트 결과 레포트

- 테스트 결과 레포트는 [링크](https://github.com/Zir0-93/xunit-to-html)를 사용하여 작성했습니다.
- 테스트 결과는 **../xunit-to-html-master/Result_java.html**로 확인 가능합니다.
- 테스트 결과 예제 : [kjw_s3tests_0001](xunit-to-html-master/kjw_s3tests_0001.PNG "kjw_s3tests_0001.PNG")
