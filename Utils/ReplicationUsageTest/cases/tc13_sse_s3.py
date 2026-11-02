"""TC13 SSE-S3: 원본·대상 버킷에 기본 암호화(AES256)를 걸고 올린 객체가 그대로 복제되고 대상에서도 암호화돼 있다."""
from __future__ import annotations

from cases.base import SHOWN_ITEMS, ReplicationCase
from common import CaseFailed

SSE_RULE = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}


class Tc13SseS3(ReplicationCase):
    CASE_ID = "TC13"
    TITLE = "SSE-S3 버킷 복제"
    OPERATION = "기본 암호화 설정 후 PUT"

    def operate(self, detail: list) -> None:
        for bucket in (self.src, self.dst):
            self.s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration=SSE_RULE)
        for key, size in self.sized_keys():
            self.put(key, size)

    def compare_versions(self) -> None:
        super().compare_versions()
        plain = []
        for bucket, expected in ((self.src, self.src_expected), (self.dst, self.dst_expected)):
            for key, version_id in expected:
                sse = self.s3.head_object(Bucket=bucket, Key=key, VersionId=version_id).get("ServerSideEncryption")
                if sse != "AES256":
                    plain.append(f"{bucket}/{key} {sse}")
        if plain:
            raise CaseFailed(f"암호화(AES256)가 아닌 버전 {len(plain)}개: {plain[:SHOWN_ITEMS]}")
