"""버킷 QoS 시나리오: 포털에서 건 초당 요청 한도를 GW 가 지키는지 확인한다.

GW 판정 규칙(ksan `GWHandler.enforceQoS`, ifs 모드도 같다):
- 포털이 Valkey 에 쓴 값 가운데 Bucket 값만 쓴다. 나머지 네 값(ListObject 등)은 적용하지 않는다.
- 버킷마다 1초 창에 Bucket 개까지 통과시키고 나머지는 503 SlowDown 으로 돌려준다.
  창은 한도를 다 채운 뒤 1초가 지나 처음 온 요청에서 다시 열린다.
- 같은 버킷으로 오는 요청은 종류와 상관없이 한 창을 함께 쓴다. 버킷 이름이 없는 요청(ListBuckets)은 제한하지 않는다.
- 0 이거나 값이 없으면 제한하지 않는다. 포털에서 바꾼 값은 1초 안에 반영된다.

재는 방법: 한도보다 훨씬 많은 요청을 일정한 간격으로 N초 동안 보내고 통과한 개수를 센다.
창이 N번 열리므로 기대값은 `한도 × N` 이고, 허용 오차(QOS_TOLERANCE) 안이면 통과로 본다.
"""
from __future__ import annotations

import threading
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field

from cases import Context, ScenarioResult
from common import (QOS_BASELINE_SEC, QOS_LIMIT, QOS_LIMIT_CHANGED, QOS_LOAD_FACTOR, QOS_MEASURE_SEC,
                    QOS_MIN_LOAD_FACTOR, QOS_SET_RETRY_POLL_SEC, QOS_SET_RETRY_SEC, QOS_SETTLE_SEC,
                    QOS_STOP_GUARD_SEC, QOS_TOLERANCE, QOS_WORKERS, logger)
from portal import PortalNotFound

MODE = "qos"
KEY_PREFIX = "qos-object"
OBJECT_KEY = KEY_PREFIX + "-read"    # Get·Head 가 읽는 객체. 측정 중에는 고치지 않는다.
PUT_KEY = KEY_PREFIX + "-write"      # Put 이 덮어쓰는 객체.
# 읽는 키와 쓰는 키를 나눈 이유: 같은 키를 덮어쓰는 동안 읽으면 InfiniStor GW 가 404·500 을 돌려주는 일이 있어
# (2026-11-18 227·228) QoS 와 무관한 실패가 섞인다. 그 동작은 이 테스트의 검사 대상이 아니다.
OBJECT_BODY = b"ksantest qos"
HTTP_SLOW_DOWN = 503


# ── 요청 ──────────────────────────────────────────────────────────────
def _list(s3, bucket: str) -> None:
    s3.list_objects_v2(Bucket=bucket, MaxKeys=1)


def _put(s3, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key=PUT_KEY, Body=OBJECT_BODY)


def _get(s3, bucket: str) -> None:
    s3.get_object(Bucket=bucket, Key=OBJECT_KEY)["Body"].read()


def _head(s3, bucket: str) -> None:
    s3.head_object(Bucket=bucket, Key=OBJECT_KEY)


LIST_ONLY = (("List", _list),)
MIXED = (("Put", _put), ("Get", _get), ("Head", _head), ("List", _list))


def _failure(e: Exception) -> tuple[int | None, str]:
    """(HTTP 상태, 오류 코드). 응답을 받지 못한 오류는 상태가 None 이다."""
    response = getattr(e, "response", None)
    if isinstance(response, dict):
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return status, str(response.get("Error", {}).get("Code", "")) or type(e).__name__
    return None, type(e).__name__


# ── 부하 ──────────────────────────────────────────────────────────────
@dataclass
class LoadResult:
    sent: int = 0
    passed: int = 0
    slowed: int = 0
    errors: Counter = field(default_factory=Counter)        # 503 이 아닌 실패: "요청 상태 코드" → 건수
    error_traces: dict = field(default_factory=dict)        # 같은 키 → 처음 난 것의 traceback
    passed_by_op: Counter = field(default_factory=Counter)
    passed_by_second: Counter = field(default_factory=Counter)  # 보낸 시각(시작부터 몇 초째) → 통과 건수
    sent_by_second: Counter = field(default_factory=Counter)    # 보낸 시각 → 보낸 건수


class _Load:
    """한 버킷에 초당 rate 개씩, seconds 초 동안 보낸다. 요청 k 는 시작 + k/rate 에 보낸다."""

    def __init__(self, s3, bucket: str, ops, rate: float, seconds: float):
        self.s3, self.bucket, self.ops = s3, bucket, ops
        self.rate = rate
        # 끝나는 시각 바로 앞에서 보낸 요청이 늦게 도착해 창을 하나 더 열지 않게 조금 일찍 멈춘다
        self.send_seconds = seconds - QOS_STOP_GUARD_SEC
        self.result = LoadResult()
        self._lock = threading.Lock()
        self._next = 0
        self._start = 0.0

    def start(self, start: float) -> list[threading.Thread]:
        self._start = start
        threads = [threading.Thread(target=self._work, daemon=True) for _ in range(QOS_WORKERS)]
        for t in threads:
            t.start()
        return threads

    def _work(self) -> None:
        while True:
            with self._lock:
                k = self._next
                offset = k / self.rate
                if offset >= self.send_seconds:
                    return
                self._next += 1
            wait = self._start + offset - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            name, call = self.ops[k % len(self.ops)]
            sent_at = time.monotonic() - self._start
            if sent_at >= self.send_seconds:
                # 서버가 느려 일정보다 밀렸다. 측정 시간 밖으로 보내면 창이 더 열려 통과 수가 늘어난다.
                return
            try:
                call(self.s3, self.bucket)
                failure = trace = None
            except Exception as e:  # 503 과 그 밖의 실패를 아래에서 나눈다
                failure = _failure(e)
                trace = None if failure[0] == HTTP_SLOW_DOWN else traceback.format_exc()
            with self._lock:
                r = self.result
                r.sent += 1
                r.sent_by_second[int(sent_at)] += 1
                if failure is None:
                    r.passed += 1
                    r.passed_by_op[name] += 1
                    r.passed_by_second[int(sent_at)] += 1
                elif failure[0] == HTTP_SLOW_DOWN:
                    r.slowed += 1
                else:
                    key = f"{name} {failure[0]} {failure[1]}"
                    r.errors[key] += 1
                    r.error_traces.setdefault(key, trace)


def run_loads(loads: list[_Load]) -> None:
    """여러 버킷의 부하를 같은 시각에 시작해 모두 끝날 때까지 기다린다."""
    start = time.monotonic() + 0.1
    threads = [t for load in loads for t in load.start(start)]
    for t in threads:
        t.join()


# ── 판정 ──────────────────────────────────────────────────────────────
class Judge:
    def __init__(self, result: ScenarioResult):
        self.result = result
        self.prefix = f"[QOS {result.case_id} {result.scenario}]"

    def _mark(self, verdict: str, message: str) -> None:
        logger.error("%s %s", self.prefix, message)
        # 측정 불가(ERROR)보다 FAIL 을 우선한다
        if self.result.result != "FAIL":
            self.result.result = verdict
        self.result.failures.append(message)

    def _errors(self, what: str, r: LoadResult) -> None:
        if r.errors:
            detail = ", ".join(f"{code} {n}건" for code, n in sorted(r.errors.items()))
            self._mark("FAIL", f"{what}: 503 이 아닌 실패가 있음 ({detail})")
            for code, trace in sorted(r.error_traces.items()):
                logger.error("%s %s 의 첫 오류:\n%s", self.prefix, code, trace)

    def unlimited(self, what: str, r: LoadResult) -> None:
        """한도가 없으면 503 이 하나도 없어야 한다."""
        line = f"{what}: 보냄 {r.sent} 통과 {r.passed} 503 {r.slowed} 기대=503 없음"
        self._errors(what, r)
        if r.sent == 0:
            self._mark("ERROR", f"{line} 측정 불가(보낸 요청 없음)")
        elif r.slowed:
            self._mark("FAIL", f"{line} 불일치")
        else:
            logger.info("%s %s OK", self.prefix, line)

    def limited(self, what: str, r: LoadResult, limit: int, seconds: float) -> None:
        """통과 수가 한도 × 측정 시간의 허용 오차 안이어야 한다."""
        expected = limit * seconds
        low, high = expected * (1 - QOS_TOLERANCE), expected * (1 + QOS_TOLERANCE)
        per_second = " ".join(str(r.passed_by_second.get(i, 0)) for i in range(int(seconds)))
        line = (f"{what}: 한도 {limit}/초 × {seconds:g}초, 보냄 {r.sent} 통과 {r.passed} 503 {r.slowed} "
                f"기대={expected:g} (허용 {low:g}~{high:g}), 초별 통과 [{per_second}]")
        self._errors(what, r)
        # 1초 창마다 한도를 넘길 만큼 보냈어야 판정할 수 있다. 서버가 멈칫해 요청이 밀린 초가 있으면
        # 그 창은 덜 채워지므로 통과 수가 적게 나와도 GW 의 한도 탓이 아니다.
        send_seconds = seconds - QOS_STOP_GUARD_SEC
        starved = [i for i in range(int(seconds))
                   if r.sent_by_second.get(i, 0) < limit * QOS_MIN_LOAD_FACTOR * min(1.0, send_seconds - i)]
        if starved:
            sent = " ".join(str(r.sent_by_second.get(i, 0)) for i in range(int(seconds)))
            self._mark("ERROR", f"{line} 측정 불가({', '.join(str(i) for i in starved)}초째에 보낸 요청이 "
                                f"한도의 {QOS_MIN_LOAD_FACTOR}배보다 적음, 초별 보냄 [{sent}])")
        elif not low <= r.passed <= high:
            self._mark("FAIL", f"{line} 불일치")
        else:
            logger.info("%s %s OK", self.prefix, line)


# ── 시나리오 ──────────────────────────────────────────────────────────
def _set_qos(ctx: Context, bucket: str, limit: int) -> None:
    """한도를 건다. 방금 만든 버킷이 포털에 아직 보이지 않으면 잠깐 다시 시도한다."""
    deadline = time.monotonic() + QOS_SET_RETRY_SEC
    while True:
        try:
            ctx.portal.set_qos(bucket, limit)
            return
        except PortalNotFound:
            if time.monotonic() > deadline:
                raise
            time.sleep(QOS_SET_RETRY_POLL_SEC)


def _delete_objects(s3, bucket: str) -> None:
    """테스트 객체 둘을 지운다. 버전 관리가 켜진 버킷이면 버전과 삭제 표시까지 지운다."""
    try:
        listed = s3.list_object_versions(Bucket=bucket, Prefix=KEY_PREFIX)
        entries = listed.get("Versions", []) + listed.get("DeleteMarkers", [])
    except Exception:
        entries = []
    for entry in entries:
        s3.delete_object(Bucket=bucket, Key=entry["Key"], VersionId=entry["VersionId"])
    if not entries:
        for key in (OBJECT_KEY, PUT_KEY):
            s3.delete_object(Bucket=bucket, Key=key)


def run_qos_mode(ctx: Context) -> list[ScenarioResult]:
    """버킷 A 에 한도를 걸고 재며, 버킷 B 는 한도가 없는 비교 대상으로 쓴다. 둘 다 main 사용자의 버킷이다."""
    s3 = ctx.users["main"].s3
    limited_bucket, other_bucket = ctx.buckets["A"], ctx.buckets["B"]
    results: list[ScenarioResult] = []

    def scenario(case_id: str, name: str, limit: int) -> Judge:
        result = ScenarioResult(MODE, case_id, f"Bucket={limit}", True, name)
        results.append(result)
        logger.info("---- qos %s %s (Bucket=%d) ----", case_id, name, limit)
        return Judge(result)

    def load(bucket: str, ops, limit: int, seconds: float) -> _Load:
        return _Load(s3, bucket, ops, QOS_LOAD_FACTOR * limit, seconds)

    def measure(loads: list[_Load]) -> None:
        # 앞 측정의 1초 창이 끝나고, 포털에서 바꾼 한도가 GW 에 반영될 때까지 기다린다
        time.sleep(QOS_SETTLE_SEC)
        run_loads(loads)

    try:
        # 혼합 시나리오의 Get·Head 가 읽을 객체. 한도를 걸기 전에 올린다.
        s3.put_object(Bucket=limited_bucket, Key=OBJECT_KEY, Body=OBJECT_BODY)

        judge = scenario("Q1", "no-limit", 0)
        a = load(limited_bucket, LIST_ONLY, QOS_LIMIT, QOS_BASELINE_SEC)
        measure([a])
        judge.unlimited("A ListObjects", a.result)

        judge = scenario("Q2", "limit", QOS_LIMIT)
        _set_qos(ctx, limited_bucket, QOS_LIMIT)
        a = load(limited_bucket, LIST_ONLY, QOS_LIMIT, QOS_MEASURE_SEC)
        measure([a])
        judge.limited("A ListObjects", a.result, QOS_LIMIT, QOS_MEASURE_SEC)

        judge = scenario("Q3", "other-bucket", QOS_LIMIT)
        a = load(limited_bucket, LIST_ONLY, QOS_LIMIT, QOS_MEASURE_SEC)
        b = load(other_bucket, LIST_ONLY, QOS_LIMIT, QOS_MEASURE_SEC)
        measure([a, b])
        judge.limited("A ListObjects", a.result, QOS_LIMIT, QOS_MEASURE_SEC)
        judge.unlimited("B ListObjects", b.result)

        judge = scenario("Q4", "limit-changed", QOS_LIMIT_CHANGED)
        _set_qos(ctx, limited_bucket, QOS_LIMIT_CHANGED)
        a = load(limited_bucket, LIST_ONLY, QOS_LIMIT_CHANGED, QOS_MEASURE_SEC)
        measure([a])
        judge.limited("A ListObjects", a.result, QOS_LIMIT_CHANGED, QOS_MEASURE_SEC)

        judge = scenario("Q5", "mixed-requests", QOS_LIMIT)
        _set_qos(ctx, limited_bucket, QOS_LIMIT)
        a = load(limited_bucket, MIXED, QOS_LIMIT, QOS_MEASURE_SEC)
        measure([a])
        by_op = ", ".join(f"{name} {a.result.passed_by_op.get(name, 0)}" for name, _ in MIXED)
        judge.limited(f"A Put·Get·Head·List 합계(통과: {by_op})", a.result, QOS_LIMIT, QOS_MEASURE_SEC)

        judge = scenario("Q6", "limit-cleared", 0)
        ctx.portal.clear_qos(limited_bucket)
        a = load(limited_bucket, LIST_ONLY, QOS_LIMIT, QOS_BASELINE_SEC)
        measure([a])
        judge.unlimited("A ListObjects", a.result)
    except Exception as e:  # 포털·S3 오류: 진행 중이던 시나리오를 오류로 남기고 나머지는 건너뛴다
        logger.error("[QOS] 실행 오류: %s", e)
        if results and results[-1].result == "PASS":
            results[-1].result = "ERROR"
            results[-1].failures.append(str(e))
        else:
            results.append(ScenarioResult(MODE, "-", "", True, "-", "ERROR", [str(e)]))
    finally:
        # 한도가 남아 있으면 정리 요청(객체·버킷 삭제)이 503 으로 막힐 수 있어 먼저 푼다.
        # 버킷을 지워도 포털은 QoS 값을 바로 지우지 않으므로, 여기서 풀어 두지 않으면 값이 남는다.
        try:
            ctx.portal.clear_qos(limited_bucket)
            time.sleep(QOS_SETTLE_SEC)
        except Exception as e:
            logger.error("정리 실패: %s QoS 해제: %s", limited_bucket, e)
        try:
            _delete_objects(s3, limited_bucket)
        except Exception as e:
            logger.error("정리 실패: %s 의 테스트 객체 삭제: %s", limited_bucket, e)
    return results
