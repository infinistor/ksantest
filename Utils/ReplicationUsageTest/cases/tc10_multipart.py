"""TC10 Multipart: 멀티파트로 올린 객체가 크기·ETag(-N)·메타데이터·태그 그대로 복제된다."""
from __future__ import annotations

import hashlib
import os

from cases.base import META_KEY, SOURCE_TAG, ReplicationCase, tagging_header
from common import CaseFailed

MIB = 1024 ** 2
PART_SIZES = [5 * MIB, 5 * MIB, 1 * MIB]  # 마지막 파트만 5 MiB 미만 허용
MULTIPART_OBJECTS = 2


class Tc10Multipart(ReplicationCase):
    CASE_ID = "TC10"
    TITLE = "Multipart 복제"
    OPERATION = "Multipart 업로드"

    def upload(self, key: str) -> None:
        meta, tags = {META_KEY: key}, {SOURCE_TAG: key}
        upload_id = self.s3.create_multipart_upload(Bucket=self.src, Key=key, Metadata=meta,
                                                    Tagging=tagging_header(tags))["UploadId"]
        parts, digests = [], []
        try:
            for number, size in enumerate(PART_SIZES, start=1):
                body = os.urandom(size)
                digests.append(hashlib.md5(body).digest())
                etag = self.s3.upload_part(Bucket=self.src, Key=key, UploadId=upload_id,
                                           PartNumber=number, Body=body)["ETag"]
                parts.append({"PartNumber": number, "ETag": etag})
            r = self.s3.complete_multipart_upload(Bucket=self.src, Key=key, UploadId=upload_id,
                                                  MultipartUpload={"Parts": parts})
        except Exception:
            self.s3.abort_multipart_upload(Bucket=self.src, Key=key, UploadId=upload_id)
            raise
        want = f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(PART_SIZES)}"
        etag = r["ETag"].strip('"')
        if etag != want:
            raise CaseFailed(f"{key} Complete ETag 가 기대와 다름: {etag} != {want}")
        self.record(key, r.get("VersionId"),
                    {"Size": sum(PART_SIZES), "ETag": etag, "Metadata": meta, "Tags": tags})

    def operate(self, detail: list) -> None:
        for i in range(MULTIPART_OBJECTS):
            self.upload(f"multipart-{i:05d}")
        detail.append(f"{MULTIPART_OBJECTS}개 × 파트 {len(PART_SIZES)}개({sum(PART_SIZES) // MIB} MiB)")
