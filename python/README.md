# S3 compatibility test for Python

- 이 테스트는 [ceph/s3-tests](https://github.com/ceph/s3-tests)를 복제하여 작성한 프로그램입니다.
- 아마존에서 제공하는 S3 Api를 사용하여 S3 호환 프로그램에 대한 기능점검 프로그램입니다.
- 별도의 유틸을 이용하여 보기 쉽게 결과물을 출력하는 기능을 포함하고 있습니다.

## 원본에서 변경점

* 함수에서 클래스로 변경
* AWS S3에서 지원하지 않는 기능 테스트 삭제 및 주석처리

## 구동환경

*  python : 3.4.3
*  OS : Centos 7.5
*  python-virtualenv

## 테스트 환경 구성

### Centos7에 virtualenv 설치

``` bash
# pip 설치
sudo yum install epel-release
yum -y update
yum -y install python-pip
# virtualenv 설치
sudo pip install virtualenv
# python module 설치
./bootstrap
# virtualenv 환경 설정
mkdir virtualenv
virtualenv virtualenv
source ./virtualenv/bin/activate
```

## 테스트 

``` bash
S3TEST_CONF=sample.conf \
./virtualenv/bin/nosetests \
-v --with-xunit --xunit-file=../xunit-to-html-master/Result_file.xml \
--nologcapture -a '!fails_on_aws' --with-id --id-file=mylist \
--failure-detail s3tests_boto3.functional.test_s3 || true
```

## 테스트 결과 레포트

``` bash
cd ..xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
```

테스트 결과 레포트는 [링크](https://github.com/Zir0-93/xunit-to-html)를 사용하여 작성했습니다.

테스트 결과는 **./xunit-to-html-master/Result_java.html**로 확인 가능합니다.

![kjw_s3tests_0001](xunit-to-html-master/kjw_s3tests_0001.PNG "kjw_s3tests_0001.PNG")