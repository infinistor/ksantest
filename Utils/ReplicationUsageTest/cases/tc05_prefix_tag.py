"""TC05 Prefix + Tag 필터: 접두사와 태그가 모두 맞는 객체만 복제된다."""
from __future__ import annotations

from cases.base import ReplicationCase
from cases.tc03_prefix import PREFIX_IN, PREFIX_OUT
from cases.tc04_tag import FILTER_TAG, filter_tags, has_filter_tag


class Tc05PrefixTag(ReplicationCase):
    CASE_ID = "TC05"
    TITLE = "Prefix + Tag 필터 복제"
    OPERATION = "접두사·태그 4조합 PUT"
    RULE_FILTER = {"And": {"Prefix": PREFIX_IN, "Tags": [FILTER_TAG]}}
    HAS_NEGATIVE = True

    def replicates(self, key: str, tags: dict[str, str]) -> bool:
        return key.startswith(PREFIX_IN) and has_filter_tag(tags)

    def operate(self, detail: list) -> None:
        for prefix in (PREFIX_IN, PREFIX_OUT):
            for i, (key, size) in enumerate(self.sized_keys(prefix)):
                self.put(key, size, filter_tags(i))
