"""복제 usage 테스트 실행 진입점.

    python3 run.py                       # 같은 시스템(local) + 다른 리전(remote) 전부
    python3 run.py --target local        # 같은 시스템만
    python3 run.py --target remote       # 포털에 등록된 다른 리전만
    python3 run.py --region kr-ksan-1    # remote 를 이 리전으로만 (여러 번 줄 수 있음)
    python3 run.py --tc TC01             # 지정 케이스만 (여러 번 줄 수 있음)
    python3 run.py --list                # 케이스 목록
    python3 run.py --check               # 설정·포털·리전·키 점검만

원본 S3 주소는 포털의 기본 리전(GET /api/v2/Regions/Default), 원격 대상은 포털에 등록된 나머지 리전에서 가져온다.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime

from cases import CASES
from common import (CaseFailed, ConfigError, PortalClient, Region, Settings, Target, format_size, logger,
                    make_s3_client)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "config.ini")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
SUMMARY_FILE = os.path.join(RESULTS_DIR, "summary.csv")
SUMMARY_HEADER = ["run_time", "case", "result", "failed_step", "replication_sec",
                  "src_bucket", "src_count", "src_size", "dst_bucket", "dst_count", "dst_size", "message"]
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(message)s"

EXIT_OK, EXIT_FAIL, EXIT_CONFIG = 0, 1, 2


def setup_logging(log_path: str | None) -> None:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_path:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    for handler in handlers:
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


def build_targets(settings: Settings, portal: PortalClient, which: str, regions_wanted: list[str] | None):
    """(원본 리전, 원본 S3 클라이언트, 대상 목록) 을 만든다."""
    home = portal.default_region()
    s3 = make_s3_client(home.endpoint(settings.use_https), settings.access_key, settings.secret_key)
    targets: list[Target] = []
    if which in ("all", "local"):
        targets.append(Target("local", s3))
    if which in ("all", "remote"):
        others = [r for r in portal.regions() if r.name != home.name]
        if regions_wanted:
            names = {r.name for r in others}
            unknown = set(regions_wanted) - names
            if unknown:
                raise ConfigError(f"포털에 없는(또는 원본) 리전: {', '.join(sorted(unknown))} "
                                  f"(다른 리전: {', '.join(sorted(names)) or '없음'})")
            others = [r for r in others if r.name in regions_wanted]
        if not others and which == "remote":
            raise ConfigError("포털에 원본 외의 리전이 등록돼 있지 않음")
        for i, region in enumerate(others, start=1):
            dst = make_s3_client(region.endpoint(settings.use_https), region.access_key, region.secret_key)
            # 주소가 같아도 포트가 다르면 앞단 haproxy 가 다른 시스템으로 보낼 수 있어 둘 다 같아야 같은 시스템으로 본다
            same = (region.address, region.port) == (home.address, home.port)
            targets.append(Target("remote", dst, region, i, same_portal=same))
    return home, s3, targets


def describe_region(region: Region, https: bool) -> str:
    return f"{region.name} {region.endpoint(https)}"


def check(settings: Settings, portal: PortalClient, targets_arg: str, regions_wanted) -> int:
    """설정·포털·리전·키만 점검한다. 버킷은 만들지 않는다."""
    ok = True
    try:
        home, s3, targets = build_targets(settings, portal, targets_arg, regions_wanted)
    except (CaseFailed, ConfigError) as e:
        print(f"[FAIL] {e}")
        return EXIT_CONFIG
    print(f"[OK]   포털 {settings.portal_url}, 원본 리전 {describe_region(home, settings.use_https)}")
    for label, client in [("원본 사용자 키", s3)] + [
            (f"리전 {t.region.name} 키({t.region.endpoint(settings.use_https)})", t.s3)
            for t in targets if t.region]:
        try:
            n = len(client.list_buckets().get("Buckets", []))
            print(f"[OK]   {label}: ListBuckets 성공(버킷 {n}개)")
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"[FAIL] {label}: ListBuckets 실패 {e}")
    for t in targets:
        if t.region:
            usage = "같은 포털, 대상 usage 비교" if t.same_portal else "다른 시스템, 대상 usage 건너뜀"
            print(f"[INFO] remote 대상 {t.region.name}: {usage}")
    if not any(t.region for t in targets) and targets_arg != "local":
        print("[INFO] 원본 외 리전이 없어 remote 케이스는 돌지 않음")
    return EXIT_OK if ok else EXIT_CONFIG


def main() -> int:
    parser = argparse.ArgumentParser(description="복제(local·remote) usage 주기 테스트")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="설정 파일 (기본 config.ini)")
    parser.add_argument("--target", choices=["all", "local", "remote"], default="all",
                        help="복제 대상 (기본 all)")
    parser.add_argument("--region", action="append", metavar="NAME", help="remote 대상 리전 이름")
    parser.add_argument("--tc", action="append", metavar="ID", help="실행할 케이스 ID (예: TC01)")
    parser.add_argument("--list", action="store_true", help="케이스 목록만 출력")
    parser.add_argument("--check", action="store_true", help="설정·포털·리전·키 점검만")
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
    portal = PortalClient(settings)

    if args.check:
        return check(settings, portal, args.target, args.region)

    try:
        home, s3, targets = build_targets(settings, portal, args.target, args.region)
    except (CaseFailed, ConfigError) as e:
        print(f"설정 오류: {e}", file=sys.stderr)
        return EXIT_CONFIG

    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_time = datetime.now().strftime("%Y%m%d%H%M%S")
    setup_logging(os.path.join(RESULTS_DIR, f"{run_time}.log"))
    logger.info("실행 시작 %s: portal=%s 원본=%s 대상=%s 파일 %d개 크기 %s", run_time, settings.portal_url,
                describe_region(home, settings.use_https),
                ", ".join("local" if t.region is None else t.region.name for t in targets),
                settings.file_count, ",".join(format_size(s) for s in settings.file_sizes))

    results = [case(settings, s3, portal, target).execute() for target in targets for case in selected]
    append_summary(run_time, results)

    failed = [r for r in results if r.result != "PASS"]
    logger.info("실행 종료: %d개 중 통과 %d, 실패 %d", len(results), len(results) - len(failed), len(failed))
    for r in failed:
        logger.info("  실패 %s %s: %s", r.case_id, r.failed_step, r.message[:300])
    return EXIT_FAIL if failed else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
