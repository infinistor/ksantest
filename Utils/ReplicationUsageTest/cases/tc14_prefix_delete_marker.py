"""TC14 Prefix + 삭제 마커 복제: 접두사가 맞는 키의 삭제 마커만 대상에 생기고, 나머지 키의 마커는 오지 않는다."""
from __future__ import annotations

from cases.base import ReplicationCase
from cases.tc03_prefix import PREFIX_IN, PREFIX_OUT
from cases.tc06_delete_marker_off import put_then_delete


class Tc14PrefixDeleteMarker(ReplicationCase):
    CASE_ID = "TC14"
    TITLE = "Prefix 필터 + 삭제 마커 복제"
    OPERATION = f"{PREFIX_IN}·{PREFIX_OUT} PUT 후 삭제(마커 생성)"
    RULE_FILTER = {"Prefix": PREFIX_IN}
    DELETE_MARKER_REPLICATION = True
    HAS_NEGATIVE = True

    def replicates(self, key: str, tags: dict[str, str]) -> bool:
        return key.startswith(PREFIX_IN)

    def operate(self, detail: list) -> None:
        for prefix in (PREFIX_IN, PREFIX_OUT):
            put_then_delete(self, detail, prefix)
