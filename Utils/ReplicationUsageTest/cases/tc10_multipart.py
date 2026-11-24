"""TC10 Multipart: 멀티파트로 올린 객체가 크기·내용·메타데이터·태그 그대로 복제된다.

원본은 Complete ETag 가 md5(파트 md5 연결)-N 인지 본다.
대상 ETag 는 비교하지 않는다. S3Backend 복제기는 원본 파트 경계가 아니라 PartSize(기본 1GB) 단위로
다시 잘라 올리므로 파트 수가 달라지면 ETag 가 달라진다(AWS 는 원본 파트 구성을 유지한다).
대신 대상 객체를 내려받아 전체 MD5 를 원본과 비교한다.
"""
from __future__ import annotations

import hashlib
import os

from cases.base import META_KEY, SOURCE_TAG, ReplicationCase, tagging_header
from common import CaseFailed

MIB = 1024 ** 2
PART_SIZES = [5 * MIB, 5 * MIB, 1 * MIB]  # 마지막 파트만 5 MiB 미만 허용
MULTIPART_OBJECTS = 2
CHUNK_SIZE = 1 * MIB  # 대상 내용 MD5 를 계산할 때 읽는 단위


class Tc10Multipart(ReplicationCase):
    CASE_ID = "TC10"
    TITLE = "Multipart 복제"
    OPERATION = "Multipart 업로드"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_md5: dict[tuple[str, str], str] = {}  # 대상 (키, 버전) → 원본 전체 MD5

    def upload(self, key: str) -> None:
        meta, tags = {META_KEY: key}, {SOURCE_TAG: key}
        upload_id = self.s3.create_multipart_upload(Bucket=self.src, Key=key, Metadata=meta,
                                                    Tagging=tagging_header(tags))["UploadId"]
        parts, digests, whole = [], [], hashlib.md5()
        try:
            for number, size in enumerate(PART_SIZES, start=1):
                body = os.urandom(size)
                digests.append(hashlib.md5(body).digest())
                whole.update(body)
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
        version_id = self.record(key, r.get("VersionId"),
                                 {"Size": sum(PART_SIZES), "ETag": etag, "Metadata": meta, "Tags": tags})
        # 대상은 ETag 대신 내용 MD5 로 본다
        if (key, version_id) in self.dst_expected:
            self.dst_expected[(key, version_id)].pop("ETag", None)
            self.content_md5[(key, version_id)] = whole.hexdigest()

    def compare_versions(self) -> None:
        super().compare_versions()
        diffs = []
        for (key, vid), want in self.content_md5.items():
            body = self.dst_s3.get_object(Bucket=self.dst, Key=key, VersionId=vid)["Body"]
            got = hashlib.md5()
            try:
                for chunk in iter(lambda: body.read(CHUNK_SIZE), b""):
                    got.update(chunk)
            finally:
                body.close()
            if got.hexdigest() != want:
                diffs.append(f"{self.dst}/{key}?versionId={vid} 내용 MD5: 기대 {want} != {got.hexdigest()}")
        if diffs:
            raise CaseFailed(f"대상 내용 불일치 {len(diffs)}건: {diffs}")

    def operate(self, detail: list) -> None:
        for i in range(MULTIPART_OBJECTS):
            self.upload(f"multipart-{i:05d}")
        detail.append(f"{MULTIPART_OBJECTS}개 × 파트 {len(PART_SIZES)}개({sum(PART_SIZES) // MIB} MiB)")
