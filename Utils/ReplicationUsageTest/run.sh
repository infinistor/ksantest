#!/bin/bash
# cron 진입점. 이전 실행이 끝나지 않았으면 이번 회차는 건너뛴다.
cd "$(dirname "$0")" || exit 2
exec flock -n /tmp/ksan-replication-usage-test.lock python3 run.py "$@"
