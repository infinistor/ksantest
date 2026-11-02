"""TC03 Prefix 필터: 접두사가 맞는 키만 복제되고 나머지는 대상에 오지 않는다."""
from __future__ import annotations

from cases.base import ReplicationCase

PREFIX_IN = "in/"
PREFIX_OUT = "out/"


class Tc03Prefix(ReplicationCase):
    CASE_ID = "TC03"
    TITLE = "Prefix 필터 복제"
    OPERATION = f"{PREFIX_IN}·{PREFIX_OUT} 키 PUT"
    RULE_FILTER = {"Prefix": PREFIX_IN}
    HAS_NEGATIVE = True

    def replicates(self, key: str, tags: dict[str, str]) -> bool:
        return key.startswith(PREFIX_IN)

    def operate(self, detail: list) -> None:
        for prefix in (PREFIX_IN, PREFIX_OUT):
            for key, size in self.sized_keys(prefix):
                self.put(key, size)
