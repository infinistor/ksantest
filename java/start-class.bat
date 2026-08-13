if "%2"=="" (
	echo Usage: start-class.bat ^<test-suite^> ^<test-class^>
	echo   start-class.bat awstests Post
	exit /b 1
)

cls
SET S3TESTS_INI=%1.ini

echo Config : %S3TESTS_INI%
echo Class  : %2
echo Filter : -Dtest=%2

call mvn test "-Ds3tests.ini=%S3TESTS_INI%" "-Dtest=%2"
exit /b %ERRORLEVEL%
