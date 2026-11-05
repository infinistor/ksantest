"""Access IP 시나리오: 사용자 단위(BucketName=ALL)와 버킷 단위.

GW 판정 규칙(ksan GWUtils.checkIPs):
- ALL 정책이 있으면 ListBuckets 를 포함한 모든 요청이 범위 안이어야 통과
- 버킷 정책만 있으면 그 버킷 요청만 검사하고 ListBuckets·다른 버킷은 영향 없음
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from common import (BUCKET_ALL, IpCase, PortalClient, logger, probe_list_buckets,
                    probe_list_objects)

USERS = ("main", "alt")


@dataclass
class TestUser:
    key: str          # "main" / "alt"
    name: str
    user_id: str
    s3: object


@dataclass
class Context:
    portal: PortalClient
    users: dict[str, TestUser]
    buckets: dict[str, str] = field(default_factory=dict)  # "A","B" → main, "C","D" → alt
    delay: float = 1.0


BUCKET_OWNER = {"A": "main", "B": "main", "C": "alt", "D": "alt"}


@dataclass
class ScenarioResult:
    mode: str
    case_id: str
    ip: str
    allowed: bool
    scenario: str
    result: str = "PASS"
    failures: list[str] = field(default_factory=list)


class Recorder:
    """판정을 한 줄씩 로그로 남기고 불일치를 모은다."""

    def __init__(self, result: ScenarioResult):
        self.result = result
        self.prefix = f"[{result.mode.upper()} {result.case_id} {result.scenario}]"

    def check(self, step: str, what: str, actual: tuple[bool, str], expected: bool) -> None:
        ok, code = actual
        detail = f" ({code})" if code else ""
        line = f"{self.prefix} {step}: {what} 허용={ok}{detail} 기대={expected}"
        if ok == expected:
            logger.info("%s OK", line)
        else:
            logger.error("%s 불일치", line)
            self.result.result = "FAIL"
            self.result.failures.append(f"{step} {what} 허용={ok}{detail} 기대={expected}")


# ── 사용자 단위 ───────────────────────────────────────────────────────
def _check_list_buckets(ctx: Context, rec: Recorder, step: str, expected: dict[str, bool]) -> None:
    for key in USERS:
        user = ctx.users[key]
        rec.check(step, f"{key} ListBuckets", probe_list_buckets(user.s3), expected[key])


def run_user_mode(ctx: Context, case: IpCase) -> list[ScenarioResult]:
    """사용자 단위: main 만 / alt 만 / 둘 다 ALL 정책을 걸고 ListBuckets 로 확인한다."""
    scenarios = [("main-only", ("main",)), ("alt-only", ("alt",)), ("both", USERS)]
    results = []
    for name, targets in scenarios:
        result = ScenarioResult("user", case.case_id, case.ip, case.allowed, name)
        rec = Recorder(result)
        _check_list_buckets(ctx, rec, "설정 전", {"main": True, "alt": True})

        for key in targets:
            ctx.portal.set_access_ip(ctx.users[key].user_id, case.ip, BUCKET_ALL)
        time.sleep(ctx.delay)
        _check_list_buckets(ctx, rec, "설정 후",
                            {k: (case.allowed if k in targets else True) for k in USERS})

        for key in targets:
            ctx.portal.clear_access_ips(ctx.users[key].user_id)
        time.sleep(ctx.delay)
        _check_list_buckets(ctx, rec, "해제 후", {"main": True, "alt": True})
        results.append(result)
    return results


# ── 버킷 단위 ─────────────────────────────────────────────────────────
def _check_buckets(ctx: Context, rec: Recorder, step: str, expected: dict[str, bool]) -> None:
    # 버킷 정책은 ListBuckets 에 영향이 없어야 한다
    for key in USERS:
        rec.check(step, f"{key} ListBuckets", probe_list_buckets(ctx.users[key].s3), True)
    for label in ("A", "B", "C", "D"):
        user = ctx.users[BUCKET_OWNER[label]]
        rec.check(step, f"{user.key} ListObjects {label}",
                  probe_list_objects(user.s3, ctx.buckets[label]), expected[label])


def run_bucket_mode(ctx: Context, case: IpCase) -> list[ScenarioResult]:
    """버킷 단위: A(main) / C(alt) / A·C 에 정책을 걸고, 건 버킷만 판정이 바뀌는지 확인한다."""
    all_open = {label: True for label in BUCKET_OWNER}
    scenarios = [("bucket-A", ("A",)), ("bucket-C", ("C",)), ("bucket-A+C", ("A", "C"))]
    results = []
    for name, targets in scenarios:
        result = ScenarioResult("bucket", case.case_id, case.ip, case.allowed, name)
        rec = Recorder(result)
        _check_buckets(ctx, rec, "설정 전", all_open)

        for label in targets:
            owner = ctx.users[BUCKET_OWNER[label]]
            ctx.portal.set_access_ip(owner.user_id, case.ip, ctx.buckets[label])
        time.sleep(ctx.delay)
        _check_buckets(ctx, rec, "설정 후",
                       {label: (case.allowed if label in targets else True) for label in BUCKET_OWNER})

        for key in {BUCKET_OWNER[label] for label in targets}:
            ctx.portal.clear_access_ips(ctx.users[key].user_id)
        time.sleep(ctx.delay)
        _check_buckets(ctx, rec, "해제 후", all_open)
        results.append(result)
    return results


MODES = {"user": run_user_mode, "bucket": run_bucket_mode}
