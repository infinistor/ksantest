#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Config base name without .ini (e.g. config, awstests, 11.151). .ini is appended when missing.
INI_FILE="${1:-config}"
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
if ! command -v go >/dev/null 2>&1; then
  echo "Go was not found on PATH. Go 1.25 or later is required." >&2
  exit 1
fi

export S3TESTS_INI="${INI_FILE}"
GO_TEST_PARALLEL="${GO_TEST_PARALLEL:-4}"
XUNIT_DIR="$(cd .. && pwd)/xunit-to-html"
XML_PATH="${XUNIT_DIR}/Result_go.xml"
HTML_PATH="${XUNIT_DIR}/Result_go.html"
SAXON_JAR="${XUNIT_DIR}/saxon9he.jar"
XSL_FILE="${XUNIT_DIR}/xunit_to_html.xsl"

[[ -f "${SAXON_JAR}" ]] || { echo "Saxon not found: ${SAXON_JAR}" >&2; exit 1; }
command -v java >/dev/null 2>&1 || { echo "Java not found. Java 8+ is required for HTML report generation." >&2; exit 1; }
rm -f test-results.json "${XML_PATH}" "${HTML_PATH}"

echo "Config : ${S3TESTS_INI}"
echo "Parallel: ${GO_TEST_PARALLEL} classes"
go version
echo
echo "=== Running Go tests ==="
set +e
go test -parallel "${GO_TEST_PARALLEL}" -json -count=1 . | tee test-results.json
TEST_EXIT=${PIPESTATUS[0]}
set -e

if [[ ! -f test-results.json ]]; then
  echo "go test produced no JSON output" >&2
  exit "${TEST_EXIT}"
fi

echo
echo "=== Generating HTML report ==="
go run ./cmd/junit-report -output "${XML_PATH}" < test-results.json
(
  cd "${XUNIT_DIR}"
  java -jar "${SAXON_JAR}" -o:Result_go.html -s:Result_go.xml -xsl:"${XSL_FILE}"
)
echo "Report: ${HTML_PATH}"

if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "${HTML_PATH}" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "${HTML_PATH}" >/dev/null 2>&1 || true
fi

exit "${TEST_EXIT}"
