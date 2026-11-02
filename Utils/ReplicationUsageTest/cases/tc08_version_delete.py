"""TC08 버전 지정 삭제: 원본에서 versionId 로 지운 버전은 대상에서 지워지지 않는다(복제 안 함).
usage 는 원본만 줄고 대상은 그대로여야 한다."""
from __future__ import annotations

from cases.base import ReplicationCase


class Tc08VersionDelete(ReplicationCase):
    CASE_ID = "TC08"
    TITLE = "버전 지정 삭제"
    OPERATION = "2번 PUT, 복제 후 이전 버전 삭제"
    DELETE_MARKER_REPLICATION = True  # 마커 복제를 켜도 버전 지정 삭제는 복제되지 않아야 한다
    HAS_NEGATIVE = True

    def operate(self, detail: list) -> None:
        old_versions = []
        for key, size in self.sized_keys():
            old_versions.append((key, self.put(key, size)))
            self.put(key, size)
        waited = self.wait_replicated()
        for key, version_id in old_versions:
            self.delete_version(key, version_id)
        detail.append(f"PUT {len(old_versions) * 2}개, 1차 복제 {waited:.1f}초, 이전 버전 {len(old_versions)}개 삭제")
