@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

if "%3"=="" (
    echo Usage: start-function.bat ^<config^> ^<test-class^> ^<test-method^>
    echo   start-function.bat awstests ACL test_bucket_permission_alt_user_read_acp
    echo   start-function.bat awstests TestACL test_private_bucket_and_object
    echo   start-function.bat awstests PutBucket test_bucket_create_naming_good_starts_alpha
    exit /b 1
)

REM Prefer local .venv (3.12), else Windows py launcher -3.12
set "PYTHON="
if exist ".venv\Scripts\python.exe" (
    for /f "delims=" %%i in ('.venv\Scripts\python.exe -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set "VENV_VER=%%i"
    if "!VENV_VER!"=="3.12" set "PYTHON=.venv\Scripts\python.exe"
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

set CONFIG=%1
echo %CONFIG% | findstr /i "\.ini$" >nul
if errorlevel 1 (
    set INI_FILE=%CONFIG%.ini
) else (
    set INI_FILE=%CONFIG%
)

set CLASS_ARG=%2
set METHOD_ARG=%3

for /f "delims=" %%i in ('"%PYTHON%" scripts\resolve_test_target.py %CLASS_ARG% %METHOD_ARG%') do set "TEST_TARGET=%%i"
if not defined TEST_TARGET (
    echo Failed to resolve test target from class=%CLASS_ARG% method=%METHOD_ARG%
    exit /b 1
)

set S3TESTS_INI=%INI_FILE%

echo Python : %PYTHON%
echo Config : %S3TESTS_INI%
echo Class  : %CLASS_ARG%
echo Method : %METHOD_ARG%
echo Target : %TEST_TARGET%
"%PYTHON%" -c "import sys; print('Version:', sys.version)"

"%PYTHON%" -m pytest -v %TEST_TARGET%
exit /b %ERRORLEVEL%
