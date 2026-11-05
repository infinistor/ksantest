"""TC01 기본 복제 테스트: 새 버킷 쌍에 복제를 걸고 업로드한 파일과 bucket usage 가 같아지는지 본다."""
from __future__ import annotations

from cases.base import ReplicationCase
from common import format_size


class Tc01Basic(ReplicationCase):
    CASE_ID = "TC01"
    TITLE = "기본 복제 테스트"
    OPERATION = "파일 업로드"

    def operate(self, detail: list) -> None:
        keys = self.sized_keys()
        for key, size in keys:
            self.put(key, size)
        detail.append(f"{len(keys)}개, {format_size(sum(size for _, size in keys))}")
