"""SSE-S3 기본 암호화 조합 케이스.

- TC13: 원본·대상 둘 다 기본 암호화(AES256)
- TC15: 원본만 기본 암호화 → 원본 객체가 암호화돼 있으므로 복제본도 암호화돼야 한다
- TC16: 대상만 기본 암호화 → 평문 원본이 대상 버킷 기본 암호화로 암호화돼야 한다
"""
from __future__ import annotations

from cases.base import SHOWN_ITEMS, ReplicationCase
from common import CaseFailed

SSE_ALGORITHM = "AES256"
SSE_RULE = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM}}]}


class Tc13SseS3(ReplicationCase):
    CASE_ID = "TC13"
    TITLE = "SSE-S3 버킷 복제(원본·대상)"
    OPERATION = "기본 암호화 설정 후 PUT"
    SRC_ENCRYPTED = True   # 원본 버킷에 기본 암호화를 거는가
    DST_ENCRYPTED = True   # 대상 버킷에 기본 암호화를 거는가

    def operate(self, detail: list) -> None:
        for bucket, on in ((self.src, self.SRC_ENCRYPTED), (self.dst, self.DST_ENCRYPTED)):
            if on:
                self.client_for(bucket).put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration=SSE_RULE)
        for key, size in self.sized_keys():
            self.put(key, size)
        detail.append(f"원본 암호화 {self.SRC_ENCRYPTED}, 대상 암호화 {self.DST_ENCRYPTED}")

    def compare_versions(self) -> None:
        super().compare_versions()
        # 원본 객체는 원본 버킷 설정을 따르고, 복제본은 원본이 암호화됐거나 대상 버킷이 암호화면 암호화
        want = {self.src: SSE_ALGORITHM if self.SRC_ENCRYPTED else None,
                self.dst: SSE_ALGORITHM if (self.SRC_ENCRYPTED or self.DST_ENCRYPTED) else None}
        wrong = []
        for bucket, expected in ((self.src, self.src_expected), (self.dst, self.dst_expected)):
            client = self.client_for(bucket)
            for key, version_id in expected:
                sse = client.head_object(Bucket=bucket, Key=key, VersionId=version_id).get("ServerSideEncryption")
                if sse != want[bucket]:
                    wrong.append(f"{bucket}/{key} 기대 {want[bucket]} != {sse}")
        if wrong:
            raise CaseFailed(f"암호화 상태가 다른 버전 {len(wrong)}개: {wrong[:SHOWN_ITEMS]}")


class Tc15SseSourceOnly(Tc13SseS3):
    CASE_ID = "TC15"
    TITLE = "SSE-S3 버킷 복제(원본만)"
    SRC_ENCRYPTED = True
    DST_ENCRYPTED = False


class Tc16SseTargetOnly(Tc13SseS3):
    CASE_ID = "TC16"
    TITLE = "SSE-S3 버킷 복제(대상만)"
    SRC_ENCRYPTED = False
    DST_ENCRYPTED = True
