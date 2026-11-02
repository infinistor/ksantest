"""TC02 여러 버전: 같은 키를 여러 번 덮어 PUT 하면 모든 버전이 같은 versionId 로 복제되고 usage 는 버전 수만큼 센다."""
from __future__ import annotations

from cases.base import ReplicationCase

VERSIONS_PER_KEY = 3


class Tc02Versions(ReplicationCase):
    CASE_ID = "TC02"
    TITLE = "여러 버전 복제"
    OPERATION = f"같은 키 {VERSIONS_PER_KEY}번 PUT"

    def operate(self, detail: list) -> None:
        for _ in range(VERSIONS_PER_KEY):
            for key, size in self.sized_keys():
                self.put(key, size)
