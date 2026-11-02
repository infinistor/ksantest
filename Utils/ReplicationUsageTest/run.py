"""복제(local → local) usage 주기 테스트 실행 진입점.

    python3 run.py                 # 등록된 케이스 전부
    python3 run.py --tc TC01       # 지정 케이스만 (여러 번 줄 수 있음)
    python3 run.py --list          # 케이스 목록
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime

from cases import CASES
from common import ConfigError, PortalClient, Settings, format_size, logger, make_s3_client

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "config.ini")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
SUMMARY_FILE = os.path.join(RESULTS_DIR, "summary.csv")
SUMMARY_HEADER = ["run_time", "case", "result", "failed_step", "replication_sec",
                  "src_bucket", "src_count", "src_size", "dst_bucket", "dst_count", "dst_size", "message"]
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(message)s"

EXIT_OK, EXIT_FAIL, EXIT_CONFIG = 0, 1, 2


def setup_logging(log_path: str) -> None:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def append_summary(run_time: str, results) -> None:
    new_file = not os.path.exists(SUMMARY_FILE)
    with open(SUMMARY_FILE, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(SUMMARY_HEADER)
        for r in results:
            s, d = r.src_usage, r.dst_usage
            w.writerow([run_time, r.case_id, r.result, r.failed_step,
                        "" if r.replication_sec is None else r.replication_sec,
                        r.src_bucket, s.file_count if s else "", s.used_size if s else "",
                        r.dst_bucket, d.file_count if d else "", d.used_size if d else "", r.message])


def main() -> int:
    parser = argparse.ArgumentParser(description="복제(local → local) usage 주기 테스트")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="설정 파일 (기본 config.ini)")
    parser.add_argument("--tc", action="append", metavar="ID", help="실행할 케이스 ID (예: TC01)")
    parser.add_argument("--list", action="store_true", help="케이스 목록만 출력")
    args = parser.parse_args()

    if args.list:
        for case in CASES:
            print(f"{case.CASE_ID}  {case.TITLE}")
        return EXIT_OK

    selected = CASES
    if args.tc:
        wanted = {t.upper() for t in args.tc}
        selected = [c for c in CASES if c.CASE_ID in wanted]
        unknown = wanted - {c.CASE_ID for c in selected}
        if unknown:
            print(f"없는 케이스: {', '.join(sorted(unknown))}", file=sys.stderr)
            return EXIT_CONFIG

    try:
        settings = Settings.load(args.config)
    except ConfigError as e:
        print(f"설정 오류: {e}", file=sys.stderr)
        return EXIT_CONFIG

    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_time = datetime.now().strftime("%Y%m%d%H%M%S")
    setup_logging(os.path.join(RESULTS_DIR, f"{run_time}.log"))
    logger.info("실행 시작 %s: s3=%s portal=%s 파일 %d개 크기 %s", run_time, settings.s3_url,
                settings.portal_url, settings.file_count, ",".join(format_size(s) for s in settings.file_sizes))

    s3 = make_s3_client(settings)
    portal = PortalClient(settings)
    results = [case(settings, s3, portal).execute() for case in selected]
    append_summary(run_time, results)

    failed = [r for r in results if r.result != "PASS"]
    logger.info("실행 종료: %d개 중 통과 %d, 실패 %d", len(results), len(results) - len(failed), len(failed))
    return EXIT_FAIL if failed else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
