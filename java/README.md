# S3 compatibility test for Java

이 테스트는 아마존에서 제공하는 S3 Api를 사용하여 S3 호환 프로그램에 대한 기능점검 프로그램입니다.

별도의 유틸을 이용하여 보기 쉽게 결과물을 출력하는 기능을 포함하고 있습니다.

## 구동환경

*  Java : 11 이상
*  maven : 3.6.3 이상
*  OS : Window 10, Centos 7.5 이상

## 테스트 방법

## Window

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

## Linux

``` bash
#설정파일 경로
export S3TESTS_INI=sample.ini
mvn clean
mvn test surefire-report:report
python ./merge_junit_results.py ./target/surefire-reports/*.xml > ../xunit-to-html-master/Result_java.xml
cd ../xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
```

## 테스트 결과 레포트

- 테스트 결과 레포트는 [링크](https://github.com/Zir0-93/xunit-to-html)를 사용하여 작성했습니다.
- 테스트 결과는 **../xunit-to-html-master/Result_java.html**로 확인 가능합니다.
- 테스트 결과 예제 : [kjw_s3tests_0001](xunit-to-html-master/kjw_s3tests_0001.PNG "kjw_s3tests_0001.PNG")
