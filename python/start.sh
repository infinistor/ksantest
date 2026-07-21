#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

resolve_python312() {
  if [[ -x .venv/bin/python ]]; then
    local ver
    ver="$(.venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    if [[ "${ver}" == "3.12" ]]; then
      echo .venv/bin/python
      return 0
    fi
    echo ".venv exists but is Python ${ver} (need 3.12); trying python3.12" >&2
  fi
  if command -v python3.12 >/dev/null 2>&1; then
    command -v python3.12
    return 0
  fi
  echo "Python 3.12 not found. Create a venv: python3.12 -m venv .venv" >&2
  return 1
}

PYTHON="$(resolve_python312)"

INI_FILE="${1:-config.ini}"
if [[ "${INI_FILE}" != *.ini ]]; then
  INI_FILE="${INI_FILE}.ini"
fi
export S3TESTS_INI="${INI_FILE}"

mkdir -p results
rm -f results/*.xml ../xunit-to-html/Result_python.html ../xunit-to-html/Result_python.xml

echo "Python : ${PYTHON}"
echo "Config : ${S3TESTS_INI}"
"${PYTHON}" -c 'import sys; print("Version:", sys.version)'

set +e
"${PYTHON}" -m pytest -v --junitxml=results/junit.xml s3tests/tests
PYTEST_EXIT=$?
set -e

if [[ ! -f results/junit.xml ]]; then
  echo "pytest produced no JUnit XML" >&2
  exit "${PYTEST_EXIT}"
fi

cp results/junit.xml ../xunit-to-html/Result_python.xml

(
  cd ../xunit-to-html
  java -jar saxon9he.jar -o:Result_python.html -s:Result_python.xml -xsl:xunit_to_html.xsl
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open Result_python.html
  elif command -v open >/dev/null 2>&1; then
    open Result_python.html
  else
    echo "Report: $(pwd)/Result_python.html"
  fi
)

exit "${PYTEST_EXIT}"
