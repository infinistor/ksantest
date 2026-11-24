"""TC17 Multipart 체크섬: 추가 체크섬을 붙여 멀티파트로 올린 객체가 체크섬 값 그대로 복제된다.

COMPOSITE(CRC32·CRC32C·SHA1·SHA256)와 FULL_OBJECT(CRC32·CRC32C·CRC64NVME)를 모두 올린다.
S3Backend 복제기는 원본 HeadObject 의 헤더(체크섬 포함)를 대상 멀티파트 시작 요청에 그대로 복사하고,
파트는 PartSize(기본 1GB) 단위로 다시 잘라 올린다. 대상 GW 가 시작 요청의 체크섬(객체 전체 값)을
파트 값과 비교하면 파트 업로드가 400 으로 거부돼 복제가 되지 않는다.

원본은 Complete 응답의 체크섬이 계산값(COMPOSITE 는 base64(H(파트 체크섬 연결))-N)과 같은지 본다.
대상은 복제됐는지와 내려받은 내용 MD5 만 본다. 체크섬은 보지 않는다(AWS 는 원본 값을 유지하지만
ksan 은 Backend HEAD 가 체크섬 헤더를 주지 않아 복제본에 체크섬이 남지 않는다). ETag 는 TC10 과 같은 이유로 보지 않는다.
"""
from __future__ import annotations

import base64
import hashlib
import os

from cases.base import META_KEY, SOURCE_TAG, tagging_header
from cases.tc10_multipart import PART_SIZES, Tc10Multipart
from cases.tc11_checksum import ALGORITHMS
from common import CaseFailed

# (알고리즘, 체크섬 타입). CRC64NVME 는 FULL_OBJECT 만, SHA 계열은 COMPOSITE 만 된다
COMBINATIONS = [
    ("CRC32", "COMPOSITE"),
    ("CRC32C", "COMPOSITE"),
    ("SHA1", "COMPOSITE"),
    ("SHA256", "COMPOSITE"),
    ("CRC32", "FULL_OBJECT"),
    ("CRC32C", "FULL_OBJECT"),
    ("CRC64NVME", "FULL_OBJECT"),
]


class Tc17MultipartChecksum(Tc10Multipart):
    CASE_ID = "TC17"
    TITLE = "Multipart 체크섬 복제"
    OPERATION = "체크섬 Multipart 업로드"
    COMPARE_CHECKSUM = True

    def upload_with_checksum(self, key: str, algorithm: str, checksum_type: str) -> None:
        func = ALGORITHMS[algorithm]
        param = f"Checksum{algorithm}"
        meta, tags = {META_KEY: key}, {SOURCE_TAG: key}
        upload_id = self.s3.create_multipart_upload(
            Bucket=self.src, Key=key, Metadata=meta, Tagging=tagging_header(tags),
            ChecksumAlgorithm=algorithm, ChecksumType=checksum_type)["UploadId"]
        parts, md5s, part_sums, whole = [], [], [], bytearray()
        try:
            for number, size in enumerate(PART_SIZES, start=1):
                body = os.urandom(size)
                md5s.append(hashlib.md5(body).digest())
                part_sums.append(func(body))
                whole += body
                # 151 에는 awscrt 가 없어 값을 직접 계산해 보낸다(TC11 과 같음)
                value = base64.b64encode(part_sums[-1]).decode()
                r = self.s3.upload_part(Bucket=self.src, Key=key, UploadId=upload_id, PartNumber=number,
                                        Body=body, **{param: value})
                parts.append({"PartNumber": number, "ETag": r["ETag"], param: value})
            r = self.s3.complete_multipart_upload(Bucket=self.src, Key=key, UploadId=upload_id,
                                                  ChecksumType=checksum_type, MultipartUpload={"Parts": parts})
        except Exception:
            self.s3.abort_multipart_upload(Bucket=self.src, Key=key, UploadId=upload_id)
            raise

        if checksum_type == "COMPOSITE":
            want = f"{base64.b64encode(func(b''.join(part_sums))).decode()}-{len(PART_SIZES)}"
        else:
            want = base64.b64encode(func(bytes(whole))).decode()
        got = r.get(param)
        if got != want:
            raise CaseFailed(f"{key} Complete 응답 {param} 가 계산값과 다름: {got} != {want}")
        etag = r["ETag"].strip('"')
        want_etag = f"{hashlib.md5(b''.join(md5s)).hexdigest()}-{len(PART_SIZES)}"
        if etag != want_etag:
            raise CaseFailed(f"{key} Complete ETag 가 기대와 다름: {etag} != {want_etag}")

        version_id = self.record(key, r.get("VersionId"),
                                 {"Size": sum(PART_SIZES), "ETag": etag, "Metadata": meta, "Tags": tags}, r)
        if (key, version_id) in self.dst_expected:
            self.dst_expected[(key, version_id)].pop("ETag", None)
            self.dst_expected[(key, version_id)].pop("Checksum", None)
            self.content_md5[(key, version_id)] = hashlib.md5(whole).hexdigest()

    def operate(self, detail: list) -> None:
        for algorithm, checksum_type in COMBINATIONS:
            self.upload_with_checksum(f"multipart-{algorithm.lower()}-{checksum_type.lower()}",
                                      algorithm, checksum_type)
        detail.append(f"{len(COMBINATIONS)}개 × 파트 {len(PART_SIZES)}개({sum(PART_SIZES) // 1024 ** 2} MiB): "
                      + ", ".join(f"{a} {t}" for a, t in COMBINATIONS))
