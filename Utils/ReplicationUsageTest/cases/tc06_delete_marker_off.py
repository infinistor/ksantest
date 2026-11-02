"""TC06 삭제 마커 복제 끔: 원본에 생긴 삭제 마커가 대상으로 가지 않는다. 마커는 usage 에 세지 않는다."""
from __future__ import annotations

from cases.base import ReplicationCase


def put_then_delete(case: ReplicationCase, detail: list, prefix: str = "") -> None:
    """전부 PUT 한 뒤 절반을 버전 없이 지운다(그 절반의 앞쪽은 DeleteObject, 뒤쪽은 DeleteObjects)."""
    keys = []
    for key, size in case.sized_keys(prefix):
        case.put(key, size)
        keys.append(key)
    targets = keys[: max(1, len(keys) // 2)]
    half = (len(targets) + 1) // 2
    singles, batch = targets[:half], targets[half:]
    for key in singles:
        case.s3.delete_object(Bucket=case.src, Key=key)
    if batch:
        case.s3.delete_objects(Bucket=case.src, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
    detail.append(f"{prefix}PUT {len(keys)}개, 삭제 마커 {len(targets)}개"
                  f"(DeleteObject {len(singles)}, DeleteObjects {len(batch)})")


class Tc06DeleteMarkerOff(ReplicationCase):
    CASE_ID = "TC06"
    TITLE = "삭제 마커 복제 끔"
    OPERATION = "PUT 후 삭제(마커 생성)"
    DELETE_MARKER_REPLICATION = False
    HAS_NEGATIVE = True

    def operate(self, detail: list) -> None:
        put_then_delete(self, detail)
