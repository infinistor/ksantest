if "%1" NEQ "" (
	SET INI_FILE=%1.ini
) else (
	SET INI_FILE=config.ini
)

cls
del xunit-to-html-master\Result_java.html
del xunit-to-html-master\Result_java.xml
call mvn clean
call mvn test surefire-report:report -Ds3tests.ini=%INI_FILE%
python merge_junit_results.py target\results\*.xml > xunit-to-html-master\Result_java.xml
cd xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl
start Result_java.html
