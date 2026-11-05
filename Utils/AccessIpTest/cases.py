"""Access IP 시나리오: 사용자 단위(BucketName=ALL)와 버킷 단위.

GW 판정 규칙(ksan GWUtils.checkIPs):
- ALL 정책이 있으면 ListBuckets 를 포함한 모든 요청이 범위 안이어야 통과
- 버킷 정책만 있으면 그 버킷 요청만 검사하고 ListBuckets·다른 버킷은 영향 없음
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from common import (BUCKET_ALL, XFF_OUTSIDE_IP, IpCase, PortalClient, inside_ip, logger,
                    probe_list_buckets, probe_list_buckets_with_xff, probe_list_objects)

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


# ── XFF 우회 ──────────────────────────────────────────────────────────
# 올바른 동작: 클라이언트가 보낸 X-Forwarded-For 헤더는 Access IP 판정을 바꾸면 안 된다.
#   - 실제 접속 출발지가 허용 범위 밖이면, XFF 에 허용 IP 를 넣어도 차단돼야 한다.
#   - 실제 접속 출발지가 허용 범위 안이면, XFF 에 아무 주소나 넣어도 허용돼야 한다.
# 현재 GWUtils.checkIPs 는 XFF 의 주소까지 함께 검사해 하나라도 맞으면 통과시키므로,
# 아래 "XFF=범위 안" 단계가 allowed=False 케이스에서 통과(=우회)하여 FAIL 로 드러난다.
# 신뢰 프록시만 XFF 를 쓰도록 고치면 모두 PASS 가 된다.
def run_xff_mode(ctx: Context, case: IpCase) -> list[ScenarioResult]:
    """main 사용자에 ALL 정책을 걸고, XFF 헤더가 판정을 바꾸지 못하는지(=무시되는지) 확인한다."""
    user = ctx.users["main"]
    result = ScenarioResult("xff", case.case_id, case.ip, case.allowed, "all-policy")
    rec = Recorder(result)

    rec.check("설정 전", "ListBuckets", probe_list_buckets(user.s3), True)

    ctx.portal.set_access_ip(user.user_id, case.ip, BUCKET_ALL)
    time.sleep(ctx.delay)
    # 기준선: XFF 없이 실제 출발지로만 판정 → 케이스의 기대값과 같아야 한다.
    rec.check("설정 후", "XFF 없음", probe_list_buckets(user.s3), case.allowed)
    # 핵심: XFF 가 판정을 바꾸면 안 되므로, 기대값은 세 경우 모두 case.allowed 로 같다.
    rec.check("설정 후", f"XFF=범위안({inside_ip(case.ip)})",
              probe_list_buckets_with_xff(user.s3, inside_ip(case.ip)), case.allowed)
    rec.check("설정 후", f"XFF=범위밖({XFF_OUTSIDE_IP})",
              probe_list_buckets_with_xff(user.s3, XFF_OUTSIDE_IP), case.allowed)

    ctx.portal.clear_access_ips(user.user_id)
    time.sleep(ctx.delay)
    rec.check("해제 후", "ListBuckets", probe_list_buckets(user.s3), True)
    return [result]


MODES = {"user": run_user_mode, "bucket": run_bucket_mode, "xff": run_xff_mode}
