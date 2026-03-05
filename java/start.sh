clear

if [ "$1" != "" ]; then
    INI_FILE="$1.ini"
else
    INI_FILE="config.ini"
fi

mvn clean
mvn test surefire-report:report "-Ds3tests.ini=$INI_FILE"
python ./merge_junit_results.py ./target/results/*.xml > xunit-to-html-master/Result_java.xml
cd xunit-to-html-master
java -jar saxon9he.jar -o:Result_java.html -s:Result_java.xml -xsl:xunit_to_html.xsl