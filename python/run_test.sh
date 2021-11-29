#!/bin/bash
#Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
#KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
#the GNU General Public License as published by the Free Software Foundation, either version 
#3 of the License.  See LICENSE for details

#본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
#KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
#KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.

## Check the arguments when execute the script
if [ "$#" -eq 0 ]
then
	echo "
	Error:	Arguments missing!
	Usage:	./[script] [arg1]
		arg1 'a' : run all tests
			   e.g., $ $0 a
		     'f' : run failed tests again
			   e.g., $ $0 f
		     'l' : show tests list with test-id
			   e.g., $ $0 l
		     'n' : run test using test-id
			   e.g., $ $0 n 97
		     'c' : run tests with specific class
			   e.g., $ $0 c SSE_C
		     'w' : run test with specific att. 'major' & 'minor'
			   e.g., $ $0 w \"ListObjectsV2\" \"Delimiter and Prefix\"
		     't' : run tests with specific att. 'minor'
			   e.g., $ $0 t \"Multi thread\"
	"
	exit 1
fi

## Common settings
vConfig="./sample.conf"
vResult_file="./Report/result_"$vMajor"_"$vMinor"_"

## Execute nosetests & generate the report with html form
vDate=`date +%Y%m%d%H%M`
vResult_file+=$vDate
vResult_file+=".html"

##
case "$1" in
	"a")
	echo "####  Run all tests  #########################"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws' --with-id --id-file=mylist --with-html --html-file=$vResult_file s3tests_boto3.functional.test_s3`
	;;
	"f")
	echo "####  Run failed tests  ######################"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --failed --nologcapture -a '!fails_on_aws' --with-id --id-file=mylist --with-html --html-file=$vResult_file s3tests_boto3.functional.test_s3`
	;;
	"l")
	echo "####  Test list with test-id  ################"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws' --collect-only --with-id --id-file=mylist s3tests_boto3.functional.test_s3`
	;;
	"n")
	echo "####  Run tests using test-id  ###############"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws' --with-id --id-file=mylist --with-html --html-file=$vResult_file $2`
	;;
	"c")
	echo "####  Run tests with specific class ##########"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws' --with-id --id-file=mylist --with-html --html-file=$vResult_file s3tests_boto3.functional.test_s3:$2`
	;;
	"w")
	echo "####  Run test with specific attr. 'major' & 'minor' ##########"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws','major='"$2",'minor='"$3" --with-id --id-file=mylist --with-html --html-file=$vResult_file s3tests_boto3.functional.test_s3`
	;;
	"t")
	echo "####  Run tests with specific attr. ##########"
	result=`S3TEST_CONF=$vConfig ./virtualenv/bin/nosetests -v --nologcapture -a '!fails_on_aws','minor='"$2" --with-id --id-file=mylist --with-html --html-file=$vResult_file s3tests_boto3.functional.test_s3`
esac
