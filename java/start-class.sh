#!/bin/bash

if [ -z "$2" ]; then
	echo "Usage: start-class.sh <config-file> <test-class>"
	echo "  ./start-class.sh awstests Post"
	exit 1
fi

clear
cd "$(dirname "$0")" || exit 1

INI_FILE="$1"
if [[ "${INI_FILE}" != *.ini ]]; then
	INI_FILE="${INI_FILE}.ini"
fi
if [ ! -f "$INI_FILE" ]; then
	echo "Config not found: $INI_FILE"
	exit 1
fi

export S3TESTS_INI="$INI_FILE"

echo "Config : $S3TESTS_INI"
echo "Class  : $2"
echo "Filter : -Dtest=$2"

mvn test "-Ds3tests.ini=$INI_FILE" "-Dtest=$2"
exit $?
