if "%3"=="" (
	echo "Usage: start-function.bat <test-suite> <test-class> <test-method>"
	exit /b
)

cls
SET S3TESTS_INI=%1.ini
del xunit-to-html-master\Result_java.html
del xunit-to-html-master\Result_java.xml
call mvn clean
call mvn test surefire-report:report -Dtest=%2#%3
.\junit-merger.exe target/results > xunit-to-html-master\Result_java.xml
cd xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
start Result_java.html