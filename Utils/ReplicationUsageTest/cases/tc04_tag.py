"""TC04 Tag 필터: 필터 태그가 붙은 객체만 복제된다."""
from __future__ import annotations

from cases.base import ReplicationCase

FILTER_TAG = {"Key": "Replication", "Value": "True"}


def has_filter_tag(tags: dict[str, str]) -> bool:
    return tags.get(FILTER_TAG["Key"]) == FILTER_TAG["Value"]


def filter_tags(index: int) -> dict[str, str]:
    """짝수 번째에만 필터 태그를 붙인다."""
    return {FILTER_TAG["Key"]: FILTER_TAG["Value"]} if index % 2 == 0 else {}


class Tc04Tag(ReplicationCase):
    CASE_ID = "TC04"
    TITLE = "Tag 필터 복제"
    OPERATION = "태그 있는·없는 객체 PUT"
    RULE_FILTER = {"Tag": FILTER_TAG}
    HAS_NEGATIVE = True

    def replicates(self, key: str, tags: dict[str, str]) -> bool:
        return has_filter_tag(tags)

    def operate(self, detail: list) -> None:
        for i, (key, size) in enumerate(self.sized_keys()):
            self.put(key, size, filter_tags(i))
