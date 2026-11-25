#!/bin/bash
# cron 진입점. 이전 실행이 끝나지 않았으면 이번 회차는 건너뛴다.
# cron 은 PATH 가 짧으니 java 가 /usr/bin 에 없으면 JAVA=/경로/bin/java 로 준다.
cd "$(dirname "$0")" || exit 2
exec flock -n /tmp/ksan-replication-usage-test.lock "${JAVA:-java}" -jar replication-usage-test.jar "$@"
