"""TC09 CopyObject: 같은 버킷 안 복사와 다른 버킷에서 가져온 복사가 모두 복제된다."""
from __future__ import annotations

from cases.base import META_KEY, SOURCE_TAG, ReplicationCase, tagging_header
from common import CaseFailed, random_body

COPY_SUFFIX = "-copy"
AUX_PREFIX = "from-aux/"


class Tc09Copy(ReplicationCase):
    CASE_ID = "TC09"
    TITLE = "CopyObject 복제"
    OPERATION = "같은 버킷·다른 버킷 Copy"

    def copy(self, source_bucket: str, source_key: str, key: str, source_info: dict) -> None:
        r = self.s3.copy_object(Bucket=self.src, Key=key, CopySource={"Bucket": source_bucket, "Key": source_key},
                                MetadataDirective="COPY", TaggingDirective="COPY")
        etag = r["CopyObjectResult"]["ETag"].strip('"')
        if etag != source_info["ETag"]:
            raise CaseFailed(f"{key} Copy ETag 가 원본과 다름: {etag} != {source_info['ETag']}")
        self.record(key, r.get("VersionId"), dict(source_info))

    def operate(self, detail: list) -> None:
        aux = self.new_extra_bucket("aux")  # 복제 룰이 없는 원천 버킷
        keys = self.sized_keys()
        for key, size in keys:
            version_id = self.put(key, size)
            self.copy(self.src, key, key + COPY_SUFFIX, self.src_expected[(key, version_id)])
        for key, size in keys:
            body, md5 = random_body(size)
            meta, tags = {META_KEY: key}, {SOURCE_TAG: key}
            self.s3.put_object(Bucket=aux, Key=key, Body=body, Metadata=meta, Tagging=tagging_header(tags))
            self.copy(aux, key, AUX_PREFIX + key, {"Size": size, "ETag": md5, "Metadata": meta, "Tags": tags})
        detail.append(f"PUT {len(keys)}개, 같은 버킷 Copy {len(keys)}개, {aux} 에서 Copy {len(keys)}개")
