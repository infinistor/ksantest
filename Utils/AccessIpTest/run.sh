#!/bin/bash
# cron·Jenkins 진입점. 이전 실행이 끝나지 않았으면 이번 회차는 건너뛴다.
cd "$(dirname "$0")" || exit 2
PYTHON=python3
[ -x .venv/bin/python ] && PYTHON=.venv/bin/python   # 패키지를 venv 에 설치한 경우
exec flock -n /tmp/ksan-accessip-test.lock "$PYTHON" run.py "$@"
