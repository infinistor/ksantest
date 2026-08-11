#!/bin/bash
clear

cd "$(dirname "$0")" || exit 1

if [ "$1" != "" ]; then
    INI_FILE="$1.ini"
else
    INI_FILE="config.ini"
fi

if [ ! -f "$INI_FILE" ]; then
    echo "Config not found: $INI_FILE"
    exit 1
fi

export S3TESTS_INI="$PWD/$INI_FILE"

rm -f TestResults/junit.xml ../xunit-to-html/Result_dotnet.xml ../xunit-to-html/Result_dotnet.html

dotnet build s3tests.csproj -clp:ErrorsOnly
if [ $? -ne 0 ]; then
    echo "Build failed - fix the compile errors above before running tests."
    exit 1
fi

mkdir -p TestResults
# JunitXml.TestLogger resolves LogFilePath relative to the project dir, not --results-directory.
dotnet test s3tests.csproj --no-build --logger "junit;LogFilePath=$PWD/TestResults/junit.xml"
TEST_EXIT=$?

if [ ! -f TestResults/junit.xml ]; then
    echo "dotnet test produced no JUnit XML (exit code $TEST_EXIT)"
    exit $TEST_EXIT
fi

cp TestResults/junit.xml ../xunit-to-html/Result_dotnet.xml
cd ../xunit-to-html || exit 1
java -jar saxon9he.jar -o:Result_dotnet.html -s:Result_dotnet.xml -xsl:xunit_to_html.xsl

exit $TEST_EXIT
