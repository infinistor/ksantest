"""TC12 태그 변경: 복제가 끝난 뒤 바꾼 태그(PutObjectTagging·DeleteObjectTagging)가 대상에 반영되고,
그 복제가 usage 를 두 번 세지 않는다."""
from __future__ import annotations

from cases.base import SHOWN_ITEMS, SOURCE_TAG, ReplicationCase, get_tags
from common import REPLICATION_POLL_SEC, REPLICATION_TIMEOUT_SEC, CaseFailed, wait_until

CHANGED_TAG = {"changed": "true"}


class Tc12Tagging(ReplicationCase):
    CASE_ID = "TC12"
    TITLE = "태그 변경 복제"
    OPERATION = "복제 후 태그 변경·삭제"

    def operate(self, detail: list) -> None:
        versions = [(key, self.put(key, size)) for key, size in self.sized_keys()]
        waited = self.wait_replicated()

        for i, (key, version_id) in enumerate(versions):
            if i % 2 == 0:
                tags = {SOURCE_TAG: key, **CHANGED_TAG}
                self.s3.put_object_tagging(Bucket=self.src, Key=key, VersionId=version_id,
                                           Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]})
            else:
                tags = {}
                self.s3.delete_object_tagging(Bucket=self.src, Key=key, VersionId=version_id)
            self.src_expected[(key, version_id)]["Tags"] = tags
            self.dst_expected[(key, version_id)]["Tags"] = dict(tags)

        def tags_replicated():
            pending = [k for k, vid in versions
                       if get_tags(self.s3, self.dst, k, vid) != self.dst_expected[(k, vid)]["Tags"]]
            return not pending, pending

        done, pending, elapsed = wait_until(tags_replicated, REPLICATION_TIMEOUT_SEC, REPLICATION_POLL_SEC)
        if not done:
            raise CaseFailed(f"{REPLICATION_TIMEOUT_SEC}초 안에 태그가 대상에 반영되지 않은 키 "
                             f"{len(pending)}개: {pending[:SHOWN_ITEMS]}")
        detail.append(f"PUT {len(versions)}개, 1차 복제 {waited:.1f}초, 태그 변경 {(len(versions) + 1) // 2}개·"
                      f"삭제 {len(versions) // 2}개, 태그 반영 {elapsed:.1f}초")
