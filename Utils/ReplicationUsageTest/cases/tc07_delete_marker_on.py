"""TC07 삭제 마커 복제 켬: 원본에 생긴 삭제 마커가 같은 versionId 로 대상에 생긴다. 마커는 usage 에 세지 않는다."""
from __future__ import annotations

from cases.base import ReplicationCase
from cases.tc06_delete_marker_off import put_then_delete


class Tc07DeleteMarkerOn(ReplicationCase):
    CASE_ID = "TC07"
    TITLE = "삭제 마커 복제 켬"
    OPERATION = "PUT 후 삭제(마커 생성)"
    DELETE_MARKER_REPLICATION = True

    def operate(self, detail: list) -> None:
        put_then_delete(self, detail)
