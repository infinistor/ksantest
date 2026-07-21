#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ $# -lt 3 ]]; then
  echo "Usage: start-function.sh <config> <test-class> <test-method>"
  echo "  ./start-function.sh awstests ACL test_bucket_permission_alt_user_read_acp"
  echo "  ./start-function.sh awstests TestACL test_private_bucket_and_object"
  exit 1
fi

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

INI_FILE="$1"
if [[ "${INI_FILE}" != *.ini ]]; then
  INI_FILE="${INI_FILE}.ini"
fi
export S3TESTS_INI="${INI_FILE}"

CLASS_ARG="$2"
METHOD_ARG="$3"
TEST_TARGET="$("${PYTHON}" scripts/resolve_test_target.py "${CLASS_ARG}" "${METHOD_ARG}")"

echo "Python : ${PYTHON}"
echo "Config : ${S3TESTS_INI}"
echo "Class  : ${CLASS_ARG}"
echo "Method : ${METHOD_ARG}"
echo "Target : ${TEST_TARGET}"
"${PYTHON}" -c 'import sys; print("Version:", sys.version)'

"${PYTHON}" -m pytest -v "${TEST_TARGET}"
