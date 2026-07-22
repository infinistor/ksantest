#!/usr/bin/env bash
# Create .venv (Python 3.12) and install requirements.txt.
# Works on Ubuntu/Debian and Rocky Linux 10.1 (system python3 == 3.12).
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh

set -euo pipefail
cd "$(dirname "$0")"

hint_install_python() {
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    case "${ID:-}" in
      rocky|rhel|centos|almalinux|fedora)
        echo "Rocky/RHEL 예:" >&2
        echo "  sudo dnf install -y python3 python3-pip python3-devel" >&2
        ;;
      ubuntu|debian)
        echo "Ubuntu/Debian 예:" >&2
        echo "  sudo apt install -y python3.12 python3.12-venv python3.12-dev" >&2
        ;;
    esac
  fi
}

resolve_python312() {
  # Prefer versioned binary when present (Ubuntu/deadsnakes).
  if command -v python3.12 >/dev/null 2>&1; then
    command -v python3.12
    return 0
  fi

  # Rocky Linux 10.1: system python3 is 3.12.
  if command -v python3 >/dev/null 2>&1; then
    local ver
    ver="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)"
    if [[ "$ver" == "3.12" ]]; then
      command -v python3
      return 0
    fi
    echo "Found python3 but version is ${ver:-unknown} (need 3.12)." >&2
  fi

  echo "Python 3.12 not found." >&2
  echo "Install Python 3.12 first (see README), then run this script again." >&2
  hint_install_python
  exit 1
}

PYTHON="$(resolve_python312)"
VENV_DIR=".venv"
VENV_PYTHON="$VENV_DIR/bin/python"
REQUIREMENTS="requirements.txt"

if [[ ! -f "$REQUIREMENTS" ]]; then
  echo "requirements.txt not found: $REQUIREMENTS" >&2
  exit 1
fi

echo "=== Python environment setup ==="
echo "System Python: $PYTHON"
"$PYTHON" -c 'import sys; print(f"Version: {sys.version}")'

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo
  echo "[1/2] Creating virtual environment (.venv) ..."
  if ! "$PYTHON" -m venv "$VENV_DIR"; then
    echo "Failed to create .venv (venv module missing?)." >&2
    hint_install_python
    exit 1
  fi
else
  echo
  echo "[1/2] .venv already exists — skip create"
fi

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "venv python not found: $VENV_PYTHON" >&2
  exit 1
fi

VENV_VER="$("$VENV_PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$VENV_VER" != "3.12" ]]; then
  echo ".venv is Python $VENV_VER (need 3.12). Remove .venv and re-run ./setup.sh" >&2
  exit 1
fi

echo
echo "[2/2] Installing packages from requirements.txt ..."
if ! "$VENV_PYTHON" -m pip --version >/dev/null 2>&1; then
  echo "pip missing in .venv; bootstrapping with ensurepip ..."
  "$VENV_PYTHON" -m ensurepip --upgrade || true
fi
"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PYTHON" -m pip install -r "$REQUIREMENTS"

echo
echo "Setup complete."
echo "Next:"
echo "  source .venv/bin/activate"
echo "  # or run tests: ./start.sh"
echo
echo "Note: .venv is local only (not committed to git)."
