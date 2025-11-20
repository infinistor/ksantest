#!/bin/bash

if [ -z "$3" ]; then
	echo "Usage: start-function.sh <config-file> <test-class> <test-method>"
	exit 1
fi

clear
export S3TESTS_INI="$1"
rm -f xunit-to-html-master/Result_java.html
rm -f xunit-to-html-master/Result_java.xml
mvn clean
mvn test surefire-report:report -Dtest="$2#$3"
./junit-merger.exe target/results > xunit-to-html-master/Result_java.xml
cd xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
xdg-open Result_java.html 2>/dev/null || open Result_java.html 2>/dev/null || echo "Please open Result_java.html manually"

