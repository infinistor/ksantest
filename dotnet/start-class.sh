#!/bin/bash

if [ -z "$2" ]; then
	echo "Usage: start-class.sh <config-file> <test-class>"
	exit 1
fi

clear
cd "$(dirname "$0")" || exit 1

INI_FILE="$1.ini"
if [ ! -f "$INI_FILE" ]; then
    echo "Config not found: $INI_FILE"
    exit 1
fi

export S3TESTS_INI="$PWD/$INI_FILE"

echo "Config : $S3TESTS_INI"
echo "Class  : $2"
echo "Filter : FullyQualifiedName~s3tests.Test.$2."

dotnet build s3tests.csproj -clp:ErrorsOnly
if [ $? -ne 0 ]; then
    echo "Build failed - fix the compile errors above before running tests."
    exit 1
fi

dotnet test s3tests.csproj --no-build --filter "FullyQualifiedName~s3tests.Test.$2." --logger "console;verbosity=detailed"
exit $?
