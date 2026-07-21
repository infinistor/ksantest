@echo off
setlocal EnableExtensions

cd /d "%~dp0"

REM Prefer local .venv (3.12), else Windows py launcher -3.12
set "PYTHON="
if exist ".venv\Scripts\python.exe" (
    for /f "delims=" %%i in ('.venv\Scripts\python.exe -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set "VENV_VER=%%i"
    if "%VENV_VER%"=="3.12" set "PYTHON=.venv\Scripts\python.exe"
)
if not defined PYTHON (
    where py >nul 2>&1
    if errorlevel 1 (
        echo Python launcher 'py' not found. Install Python 3.12 or create .venv with 3.12.
        exit /b 1
    )
    for /f "delims=" %%i in ('py -3.12 -c "import sys; print(sys.executable)" 2^>nul') do set "PYTHON=%%i"
)
if not defined PYTHON (
    echo Python 3.12 not found. Install it or run: py -3.12 -m venv .venv
    exit /b 1
)

if "%1" NEQ "" (
    echo %1 | findstr /i "\.ini$" >nul
    if errorlevel 1 (
        SET INI_FILE=%1.ini
    ) else (
        SET INI_FILE=%1
    )
) else (
    SET INI_FILE=config.ini
)

set S3TESTS_INI=%INI_FILE%
if not exist results mkdir results
del /q results\*.xml 2>nul
del ..\xunit-to-html\Result_python.html 2>nul
del ..\xunit-to-html\Result_python.xml 2>nul

echo Python : %PYTHON%
echo Config : %S3TESTS_INI%
"%PYTHON%" -c "import sys; print('Version:', sys.version)"

"%PYTHON%" -m pytest -v --junitxml=results\junit.xml s3tests\tests
set PYTEST_EXIT=%ERRORLEVEL%
if not exist results\junit.xml (
    echo pytest produced no JUnit XML
    exit /b %PYTEST_EXIT%
)

copy /y results\junit.xml ..\xunit-to-html\Result_python.xml >nul
cd ..\xunit-to-html
java -jar saxon9he.jar -o:Result_python.html -s:Result_python.xml -xsl:xunit_to_html.xsl
start Result_python.html
exit /b %PYTEST_EXIT%
