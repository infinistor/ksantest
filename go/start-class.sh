#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ $# -lt 2 ]]; then
  echo "Usage: start-class.sh <config> <test-class>" >&2
  echo "  config is the INI base name without .ini (e.g. config, awstests, 11.151)" >&2
  echo "  ./start-class.sh awstests Post" >&2
  exit 1
fi

INI_FILE="$1"
if [[ "${INI_FILE}" != *.ini ]]; then
  INI_FILE="${INI_FILE}.ini"
fi
if [[ "${INI_FILE}" != /* ]]; then
  INI_FILE="${PWD}/${INI_FILE}"
fi
if [[ ! -f "${INI_FILE}" ]]; then
  echo "Config file not found: ${INI_FILE}" >&2
  exit 1
fi

TEST_NAMES="$(go run ./cmd/resolve-test "$2")"
export S3TESTS_INI="${INI_FILE}"

echo "Config : ${S3TESTS_INI}"
echo "Class  : $2"
echo "Target : ${TEST_NAMES}"
go version

go test -v -count=1 -run "^(${TEST_NAMES})$" .
