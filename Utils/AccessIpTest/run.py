"""Access IP 테스트 실행 진입점.

    python3 run.py                  # 사용자 단위 + 버킷 단위 전부
    python3 run.py --mode user      # 사용자 단위(ALL)만
    python3 run.py --mode bucket    # 버킷 단위만
    python3 run.py --case IP05      # 지정 IP 케이스만 (여러 번 줄 수 있음)
    python3 run.py --list           # 감지한 클라이언트 IP 와 케이스 목록만 출력
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime

from cases import MODES, USERS, Context, ScenarioResult, TestUser
from common import (BUCKET_PREFIX, USER_PREFIX, ConfigError, PortalClient, PortalError, Settings,
                    build_ip_cases, detect_client_ip, logger, make_s3_client, wait_user_ready)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "config.ini")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
SUMMARY_FILE = os.path.join(RESULTS_DIR, "summary.csv")
SUMMARY_HEADER = ["run_time", "client_ip", "mode", "case", "ip", "allowed", "scenario", "result", "message"]
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(message)s"

EXIT_OK, EXIT_FAIL, EXIT_CONFIG = 0, 1, 2


def setup_logging(log_path: str) -> None:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def append_summary(run_time: str, client_ip: str, results: list[ScenarioResult]) -> None:
    new_file = not os.path.exists(SUMMARY_FILE)
    with open(SUMMARY_FILE, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(SUMMARY_HEADER)
        for r in results:
            w.writerow([run_time, client_ip, r.mode, r.case_id, r.ip, r.allowed, r.scenario, r.result,
                        "; ".join(r.failures)])


def prepare(settings: Settings, portal: PortalClient, run_time: str, need_buckets: bool,
            created: dict) -> Context:
    """사용자 둘(과 버킷 넷)을 만든다. 만든 것은 created 에 바로 기록해 실패해도 정리되게 한다."""
    users = {}
    for key in USERS:
        name = f"{USER_PREFIX}-{key}-{run_time}"
        info = portal.create_user(name)
        created["users"].append((name, info["Id"]))
        s3 = make_s3_client(settings, info["AccessKey"], info["SecretKey"])
        users[key] = TestUser(key, name, info["Id"], s3)
        logger.info("사용자 생성: %s (%s)", name, info["Id"])
    for user in users.values():
        wait_user_ready(user.s3, user.name)

    ctx = Context(portal, users, delay=settings.delay)
    if need_buckets:
        for label, owner in (("A", "main"), ("B", "main"), ("C", "alt"), ("D", "alt")):
            bucket = f"{BUCKET_PREFIX}-{run_time}-{label.lower()}"
            users[owner].s3.create_bucket(Bucket=bucket)
            created["buckets"].append((users[owner].s3, bucket))
            ctx.buckets[label] = bucket
        logger.info("버킷 생성: %s", ", ".join(f"{k}={v}" for k, v in ctx.buckets.items()))
    return ctx


def cleanup(portal: PortalClient, created: dict) -> bool:
    """정책 → 버킷 → 사용자 순서로 정리한다. 하나가 실패해도 나머지는 계속한다."""
    ok = True
    for name, user_id in created["users"]:
        try:
            portal.clear_access_ips(user_id)
        except PortalError as e:
            ok = False
            logger.error("정리 실패: %s Access IP 해제: %s", name, e)
    for s3, bucket in created["buckets"]:
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception as e:
            ok = False
            logger.error("정리 실패: 버킷 %s 삭제: %s", bucket, e)
    for name, user_id in created["users"]:
        try:
            portal.delete_user(user_id)
        except PortalError as e:
            ok = False
            logger.error("정리 실패: 사용자 %s 삭제: %s", name, e)
    if ok:
        logger.info("정리 완료: 사용자 %d, 버킷 %d", len(created["users"]), len(created["buckets"]))
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="KSAN S3 Access IP 테스트")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="설정 파일 (기본 config.ini)")
    parser.add_argument("--mode", choices=["all", *MODES], default="all", help="실행할 단위 (기본 all)")
    parser.add_argument("--case", action="append", metavar="ID", help="실행할 IP 케이스 ID (예: IP05)")
    parser.add_argument("--list", action="store_true", help="클라이언트 IP 와 케이스 목록만 출력")
    args = parser.parse_args()

    try:
        settings = Settings.load(args.config)
        client_ip = settings.client_ip or detect_client_ip(settings.s3_url)
    except (ConfigError, OSError) as e:
        print(f"설정 오류: {e}", file=sys.stderr)
        return EXIT_CONFIG

    ip_cases, skipped = build_ip_cases(client_ip)
    if args.case:
        wanted = {c.upper() for c in args.case}
        unknown = wanted - {c.case_id for c in ip_cases}
        if unknown:
            print(f"없는 케이스: {', '.join(sorted(unknown))}", file=sys.stderr)
            return EXIT_CONFIG
        ip_cases = [c for c in ip_cases if c.case_id in wanted]

    if args.list:
        print(f"클라이언트 IP: {client_ip}{' (설정값)' if settings.client_ip else ' (자동 감지)'}")
        for c in ip_cases:
            print(f"{c.case_id}  {c.ip:<32} {'허용' if c.allowed else '차단'}  {c.title}")
        for s in skipped:
            print(f"건너뜀  {s}")
        return EXIT_OK

    modes = list(MODES) if args.mode == "all" else [args.mode]
    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_time = datetime.now().strftime("%Y%m%d%H%M%S")
    setup_logging(os.path.join(RESULTS_DIR, f"{run_time}.log"))
    logger.info("실행 시작 %s: portal=%s s3=%s client_ip=%s 단위=%s 케이스 %d개 delay=%.1f초",
                run_time, settings.portal_url, settings.s3_url, client_ip, ",".join(modes),
                len(ip_cases), settings.delay)
    for s in skipped:
        logger.warning("건너뜀 %s", s)

    portal = PortalClient(settings)
    created = {"users": [], "buckets": []}
    results: list[ScenarioResult] = []
    aborted = False
    try:
        portal.check()
        ctx = prepare(settings, portal, run_time, "bucket" in modes, created)
        for mode in modes:
            for case in ip_cases:
                logger.info("---- %s %s %s (%s, %s) ----", mode, case.case_id, case.ip, case.title,
                            "허용" if case.allowed else "차단")
                try:
                    results.extend(MODES[mode](ctx, case))
                except Exception as e:  # 포털 오류 등: 이 케이스만 실패로 기록하고 정책을 비운 뒤 계속
                    logger.error("[%s %s] 실행 오류: %s", mode.upper(), case.case_id, e)
                    results.append(ScenarioResult(mode, case.case_id, case.ip, case.allowed, "-", "ERROR",
                                                  [str(e)]))
                    for user in ctx.users.values():
                        portal.clear_access_ips(user.user_id)
    except Exception as e:
        aborted = True
        logger.error("준비 또는 실행 중 중단: %s", e)
    finally:
        cleaned = cleanup(portal, created)

    append_summary(run_time, client_ip, results)
    failed = [r for r in results if r.result != "PASS"]
    logger.info("실행 종료: 시나리오 %d개 중 통과 %d, 실패 %d%s%s", len(results), len(results) - len(failed),
                len(failed), ", 중단됨" if aborted else "", "" if cleaned else ", 정리 실패 있음")
    for r in failed:
        logger.info("  실패 %s %s %s %s: %s", r.mode, r.case_id, r.ip, r.scenario, "; ".join(r.failures))
    return EXIT_FAIL if (failed or aborted or not cleaned) else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
