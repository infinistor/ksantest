"""TC19 룰별 조건 다른 다중 대상: 원본 하나에 조건이 다른 룰 3개를 걸고, 객체마다 맞는 룰의 대상으로만 가는지 본다.

| 룰 | Priority | Filter | 삭제 마커 복제 | 대상 |
|---|---|---|---|---|
| 1 | 1 | Prefix a/ | 켬 | dst |
| 2 | 2 | Prefix b/ | 끔 | dst2 |
| 3 | 3 | 빈 Filter(전체) | 끔 | dst3 |

a/·b/·c/ 키를 PUT 한 뒤 접두어마다 절반을 버전 없이 지워 삭제 마커를 만든다. 기대값은 다음과 같다.

- dst: a/ 버전 전부 + a/ 삭제 마커
- dst2: b/ 버전 전부, 삭제 마커 없음
- dst3: a/·b/·c/ 버전 전부, 삭제 마커 없음(a/ 는 룰 1 에 맞아도 룰 3 의 삭제 마커 설정을 따른다)

대상 버킷이 서로 다르면 객체가 맞는 룰 모두에 복제된다(Priority 는 대상이 같은 룰끼리만 고른다).
룰 하나만 적용하고 멈추거나, 다른 룰의 삭제 마커 설정을 가져다 쓰면 실패한다.
"""
from __future__ import annotations

from cases.base import ReplicationCase
from cases.tc06_delete_marker_off import put_then_delete

PREFIXES = ("a/", "b/", "c/")
# 대상 순서대로 (Filter, 삭제 마커 복제, 복제되는 접두어. None 이면 전부)
RULES: list[tuple[dict, bool, str | None]] = [
    ({"Prefix": "a/"}, True, "a/"),
    ({"Prefix": "b/"}, False, "b/"),
    ({}, False, None),
]


class Tc19MultiRuleMixed(ReplicationCase):
    CASE_ID = "TC19"
    TITLE = "룰별 조건 다른 다중 대상(룰 3개)"
    OPERATION = "a/·b/·c/ PUT 후 삭제(마커 생성)"
    DESTINATIONS = len(RULES)
    HAS_NEGATIVE = True

    def rule_spec(self, index: int) -> tuple[dict, bool]:
        rule_filter, marker, _ = RULES[index]
        return rule_filter, marker

    def replicates_to(self, index: int, key: str, tags: dict[str, str]) -> bool:
        prefix = RULES[index][2]
        return prefix is None or key.startswith(prefix)

    def operate(self, detail: list) -> None:
        parts: list[str] = []
        for prefix in PREFIXES:
            put_then_delete(self, parts, prefix)
        detail.append(", ".join(parts))
