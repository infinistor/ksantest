"""TC02 부터 쓰는 공통 틀: 버전 단위로 기대값을 기록하고, 대상 버전 목록·내용·usage 를 비교한다.

하위 케이스는 룰(RULE_FILTER·DELETE_MARKER_REPLICATION)과 operate() 만 정한다.
operate() 에서 self.put()/self.copy()/... 로 작업하면 원본·대상 기대값이 함께 기록된다.
"""
from __future__ import annotations

import hashlib
import logging
import time
from urllib.parse import urlencode

from common import (
    REPLICATION_POLL_SEC, REPLICATION_TIMEOUT_SEC, USAGE_POLL_SEC, USAGE_TIMEOUT_SEC,
    Case, CaseFailed, Usage, empty_and_delete_bucket, format_size, random_body, wait_until,
)

logger = logging.getLogger("rtest")

RULE_ID = "rtest-rule"
SHOWN_ITEMS = 20  # 실패 메시지에 보여 줄 항목 수
# 대상에 "없어야 할" 것(필터 제외·마커·버전 지정 삭제)이 늦게 도착하는지 보려고 복제 완료 뒤 더 기다리는 시간
NEGATIVE_SETTLE_SEC = 20
META_KEY = "test"  # 모든 PUT 에 붙이는 사용자 메타데이터(x-amz-meta-test)
SOURCE_TAG = "source"  # 모든 PUT 에 붙이는 기본 태그(값은 키)

VersionId = tuple[str, str]  # (Key, VersionId)


def tagging_header(tags: dict[str, str]) -> str:
    return urlencode(tags)


def list_versions(s3, bucket: str) -> tuple[dict[VersionId, dict], set[VersionId]]:
    """버킷의 (파일 버전 {(키, 버전): {Size, ETag}}, 삭제 마커 {(키, 버전)}) 를 돌려준다."""
    files, markers = {}, set()
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        for v in page.get("Versions", []):
            files[(v["Key"], v["VersionId"])] = {"Size": v["Size"], "ETag": v["ETag"].strip('"')}
        for m in page.get("DeleteMarkers", []):
            markers.add((m["Key"], m["VersionId"]))
    return files, markers


def get_tags(s3, bucket: str, key: str, version_id: str) -> dict[str, str]:
    tag_set = s3.get_object_tagging(Bucket=bucket, Key=key, VersionId=version_id).get("TagSet", [])
    return {t["Key"]: t["Value"] for t in tag_set}


def usage_of(versions: dict[VersionId, dict]) -> Usage:
    """파일 버전만 센다. 삭제 마커는 usage 에 반영되지 않는다."""
    return Usage(len(versions), sum(v["Size"] for v in versions.values()))


def short(items) -> list:
    return sorted(items)[:SHOWN_ITEMS]


class ReplicationCase(Case):
    STEPS = [
        "신규 버킷 생성",
        "버저닝 설정",
        "룰 설정",
        "작업",  # 하위 클래스가 OPERATION 으로 이름을 바꾼다
        "복제 완료까지 대기",
        "버전 비교",
        "bucket usage 비교(portal api)",
    ]
    OPERATION = "작업"
    RULE_FILTER: dict = {"Prefix": ""}
    DELETE_MARKER_REPLICATION = False
    # 대상에 없어야 할 것이 있는 케이스면 True (복제 완료 뒤 NEGATIVE_SETTLE_SEC 더 보고 다시 확인)
    HAS_NEGATIVE = False
    COMPARE_CHECKSUM = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.STEPS = list(type(self).STEPS)
        self.STEPS[3] = self.OPERATION
        self.src = self.dst = ""
        self.extra_buckets: list[str] = []
        # 기대값: (키, 버전) → {Size, ETag, Metadata, Tags[, Checksum]}
        self.src_expected: dict[VersionId, dict] = {}
        self.dst_expected: dict[VersionId, dict] = {}

    # ── 하위 클래스가 정하는 부분 ─────────────────────────────────────
    def operate(self, detail: list) -> None:
        raise NotImplementedError

    def replicates(self, key: str, tags: dict[str, str]) -> bool:
        """룰 필터에 맞는 객체인지. 필터 케이스가 바꾼다."""
        return True

    # ── 작업 도우미 (원본에 작업하고 기대값을 기록) ───────────────────
    def put(self, key: str, size: int, tags: dict[str, str] | None = None, body: bytes | None = None,
            **extra) -> str:
        if body is None:
            body, md5 = random_body(size)
        else:
            md5 = hashlib.md5(body).hexdigest()
        tags = {SOURCE_TAG: key, **(tags or {})}
        meta = {META_KEY: key}
        r = self.s3.put_object(Bucket=self.src, Key=key, Body=body, Metadata=meta,
                               Tagging=tagging_header(tags), **extra)
        etag = r["ETag"].strip('"')
        if etag != md5:
            raise CaseFailed(f"{key} 업로드 ETag 가 MD5 와 다름: {etag} != {md5}")
        info = {"Size": size, "ETag": etag, "Metadata": meta, "Tags": tags}
        return self.record(key, r["VersionId"], info, r)

    def record(self, key: str, version_id: str | None, info: dict, response: dict | None = None) -> str:
        if not version_id:
            raise CaseFailed(f"{key} 응답에 VersionId 가 없음(버저닝이 안 걸림)")
        if self.COMPARE_CHECKSUM and response is not None:
            info["Checksum"] = {k: v for k, v in response.items() if k.startswith("Checksum") and k != "ChecksumType"}
        self.src_expected[(key, version_id)] = info
        if self.replicates(key, info["Tags"]):
            self.dst_expected[(key, version_id)] = dict(info)
        return version_id

    def delete_version(self, key: str, version_id: str) -> None:
        """원본에서 버전을 지정해 지운다. 이 삭제는 복제되지 않으므로 대상 기대값은 그대로 둔다."""
        self.s3.delete_object(Bucket=self.src, Key=key, VersionId=version_id)
        self.src_expected.pop((key, version_id), None)

    def new_extra_bucket(self, kind: str) -> str:
        bucket = self.bucket_name(kind)
        self.s3.create_bucket(Bucket=bucket)
        self.extra_buckets.append(bucket)
        return bucket

    # ── 단계 ─────────────────────────────────────────────────────────
    def run(self) -> None:
        self.src = self.result.src_bucket = self.bucket_name("src")
        self.dst = self.result.dst_bucket = self.bucket_name("dst")

        with self.step(1, [f"{self.src}, {self.dst}"]):
            self.s3.create_bucket(Bucket=self.src)
            self.s3.create_bucket(Bucket=self.dst)

        with self.step(2):
            for bucket in (self.src, self.dst):
                self.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
                status = self.s3.get_bucket_versioning(Bucket=bucket).get("Status")
                if status != "Enabled":
                    raise CaseFailed(f"{bucket} 버저닝 상태가 Enabled 가 아님: {status}")

        with self.step(3):
            status = "Enabled" if self.DELETE_MARKER_REPLICATION else "Disabled"
            # 리전 없는 ARN = 같은 시스템(local) 안의 대상 버킷
            self.s3.put_bucket_replication(Bucket=self.src, ReplicationConfiguration={
                "Role": "",
                "Rules": [{
                    "ID": RULE_ID,
                    "Priority": 1,
                    "Status": "Enabled",
                    "Filter": self.RULE_FILTER,
                    "Destination": {"Bucket": f"arn:aws:s3:::{self.dst}"},
                    "DeleteMarkerReplication": {"Status": status},
                }],
            })
            rules = self.s3.get_bucket_replication(Bucket=self.src)["ReplicationConfiguration"]["Rules"]
            if not any(r.get("ID") == RULE_ID for r in rules):
                raise CaseFailed(f"설정한 복제 룰이 조회되지 않음: {rules}")

        operate_detail: list[str] = []
        with self.step(4, operate_detail):
            self.operate(operate_detail)
            if not operate_detail:
                operate_detail.append(f"원본 {usage_of(self.src_expected)}, 대상 기대 {usage_of(self.dst_expected)}")

        wait_detail: list[str] = []
        with self.step(5, wait_detail):
            elapsed = self.wait_replicated()
            self.result.replication_sec = round(elapsed, 1)
            wait_detail.append(f"복제 {elapsed:.1f}초")
            if self.HAS_NEGATIVE:
                time.sleep(NEGATIVE_SETTLE_SEC)
                done, diff = self.dst_matches()
                if not done:
                    raise CaseFailed(f"복제 완료 {NEGATIVE_SETTLE_SEC}초 뒤 대상이 달라짐: {diff}")
                wait_detail.append(f"이후 {NEGATIVE_SETTLE_SEC}초 변화 없음")

        with self.step(6):
            self.compare_versions()

        usage_detail: list[str] = []
        with self.step(7, usage_detail):
            self.compare_usage(usage_detail)

    def expected_markers(self, src_markers: set[VersionId]) -> set[VersionId]:
        if not self.DELETE_MARKER_REPLICATION:
            return set()
        return {(k, v) for k, v in src_markers if self.replicates(k, {})}

    def dst_matches(self) -> tuple[bool, str]:
        _, src_markers = list_versions(self.s3, self.src)
        files, markers = list_versions(self.s3, self.dst)
        want_markers = self.expected_markers(src_markers)
        missing = set(self.dst_expected) - set(files)
        extra = set(files) - set(self.dst_expected)
        missing_m = want_markers - markers
        extra_m = markers - want_markers
        if not (missing or extra or missing_m or extra_m):
            return True, ""
        parts = []
        if missing:
            parts.append(f"빠진 버전 {len(missing)}개 {short(missing)}")
        if extra:
            parts.append(f"남는 버전 {len(extra)}개 {short(extra)}")
        if missing_m:
            parts.append(f"빠진 삭제 마커 {len(missing_m)}개 {short(missing_m)}")
        if extra_m:
            parts.append(f"남는 삭제 마커 {len(extra_m)}개 {short(extra_m)}")
        return False, " / ".join(parts)

    def wait_replicated(self) -> float:
        done, diff, elapsed = wait_until(self.dst_matches, REPLICATION_TIMEOUT_SEC, REPLICATION_POLL_SEC)
        if not done:
            raise CaseFailed(f"{REPLICATION_TIMEOUT_SEC}초 안에 대상이 기대와 같아지지 않음: {diff}")
        return elapsed

    def describe(self, bucket: str, key: str, version_id: str) -> dict:
        kwargs = {"ChecksumMode": "ENABLED"} if self.COMPARE_CHECKSUM else {}
        h = self.s3.head_object(Bucket=bucket, Key=key, VersionId=version_id, **kwargs)
        info = {"Size": h["ContentLength"], "ETag": h["ETag"].strip('"'),
                "Metadata": h.get("Metadata", {}), "Tags": get_tags(self.s3, bucket, key, version_id)}
        if self.COMPARE_CHECKSUM:
            info["Checksum"] = {k: v for k, v in h.items() if k.startswith("Checksum") and k != "ChecksumType"}
        return info

    def compare_versions(self) -> None:
        src_files, _ = list_versions(self.s3, self.src)
        diffs = []
        if set(src_files) != set(self.src_expected):
            diffs.append(f"원본 버전 목록이 기대와 다름: 빠짐 {short(set(self.src_expected) - set(src_files))}, "
                         f"남음 {short(set(src_files) - set(self.src_expected))}")
        for bucket, expected in ((self.src, self.src_expected), (self.dst, self.dst_expected)):
            for (key, vid), want in expected.items():
                got = self.describe(bucket, key, vid)
                for field in want:
                    if got.get(field) != want[field]:
                        diffs.append(f"{bucket}/{key}?versionId={vid} {field}: 기대 {want[field]} != {got.get(field)}")
        if diffs:
            raise CaseFailed(f"버전 불일치 {len(diffs)}건: {diffs[:SHOWN_ITEMS]}")

    def compare_usage(self, detail: list) -> None:
        want_src, want_dst = usage_of(self.src_expected), usage_of(self.dst_expected)

        def usage_matched():
            s, d = self.portal.bucket_usage(self.src), self.portal.bucket_usage(self.dst)
            self.result.src_usage, self.result.dst_usage = s, d
            return s == want_src and d == want_dst, (s, d)

        done, (s, d), elapsed = wait_until(usage_matched, USAGE_TIMEOUT_SEC, USAGE_POLL_SEC)
        if not done:
            raise CaseFailed(f"{USAGE_TIMEOUT_SEC}초 안에 usage 가 맞지 않음: "
                             f"src 기대 {want_src} 실제 {s} / dst 기대 {want_dst} 실제 {d}")
        same = "src=dst " if want_src == want_dst else ""
        detail.append(f"{same}src {want_src}, dst {want_dst}, 수렴 {elapsed:.1f}초")

    def cleanup(self) -> None:
        if self.src:
            # 복제 룰을 먼저 지워야 원본 삭제가 대상에 다시 복제되지 않는다
            try:
                self.s3.delete_bucket_replication(Bucket=self.src)
            except Exception as e:  # noqa: BLE001
                logger.warning("복제 룰 삭제 실패: %s (%s)", self.src, e)
        for bucket in [self.src, self.dst, *self.extra_buckets]:
            if bucket:
                empty_and_delete_bucket(self.s3, bucket)

    # ── 공용 계산 ────────────────────────────────────────────────────
    def sized_keys(self, prefix: str = "") -> list[tuple[str, int]]:
        """file_count·file_sizes 대로 (키, 크기) 목록을 만든다."""
        return [(f"{prefix}obj-{i:05d}-{format_size(size)}", size)
                for i, size in enumerate(self.settings.object_sizes())]
