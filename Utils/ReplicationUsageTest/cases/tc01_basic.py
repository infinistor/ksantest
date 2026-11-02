"""TC01 기본 복제 테스트: 새 버킷 쌍에 local 복제를 걸고 업로드한 파일과 bucket usage 가 같아지는지 본다."""
from __future__ import annotations

import logging

from common import (
    REPLICATION_POLL_SEC, REPLICATION_TIMEOUT_SEC, USAGE_POLL_SEC, USAGE_TIMEOUT_SEC, KEY_PREFIX,
    Case, CaseFailed, Usage, empty_and_delete_bucket, format_size, list_objects, new_bucket_name,
    random_body, wait_until,
)

logger = logging.getLogger("rtest")

RULE_ID = "rtest-rule"
MISSING_KEYS_SHOWN = 20  # 실패 메시지에 보여 줄 키 수


class Tc01Basic(Case):
    CASE_ID = "TC01"
    TITLE = "기본 복제 테스트"
    STEPS = [
        "신규 버킷 생성",
        "버저닝 설정",
        "룰 설정",
        "파일 업로드",
        "복제 완료까지 대기",
        "파일 비교",
        "bucket usage 비교(portal api)",
    ]

    def run(self) -> None:
        src = self.result.src_bucket = new_bucket_name("src")
        dst = self.result.dst_bucket = new_bucket_name("dst")
        expected: dict[str, dict] = {}

        with self.step(1, [f"{src}, {dst}"]):
            self.s3.create_bucket(Bucket=src)
            self.s3.create_bucket(Bucket=dst)

        with self.step(2):
            for bucket in (src, dst):
                self.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
                status = self.s3.get_bucket_versioning(Bucket=bucket).get("Status")
                if status != "Enabled":
                    raise CaseFailed(f"{bucket} 버저닝 상태가 Enabled 가 아님: {status}")

        with self.step(3):
            # 리전 없는 ARN = 같은 시스템(local) 안의 대상 버킷
            self.s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={
                "Role": "",
                "Rules": [{
                    "ID": RULE_ID,
                    "Priority": 1,
                    "Status": "Enabled",
                    "Filter": {"Prefix": ""},
                    "Destination": {"Bucket": f"arn:aws:s3:::{dst}"},
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                }],
            })
            rules = self.s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"]
            if not any(r.get("ID") == RULE_ID for r in rules):
                raise CaseFailed(f"설정한 복제 룰이 조회되지 않음: {rules}")

        sizes = self.settings.object_sizes()
        upload_detail = [f"{len(sizes)}개, {format_size(sum(sizes))}"]
        with self.step(4, upload_detail):
            for i, size in enumerate(sizes):
                key = f"{KEY_PREFIX}-{i:05d}-{format_size(size)}"
                body, md5 = random_body(size)
                etag = self.s3.put_object(Bucket=src, Key=key, Body=body)["ETag"].strip('"')
                if etag != md5:
                    raise CaseFailed(f"{key} 업로드 ETag 가 MD5 와 다름: {etag} != {md5}")
                expected[key] = {"Size": size, "ETag": md5}

        wait_detail: list[str] = []
        with self.step(5, wait_detail):
            def replicated():
                got = list_objects(self.s3, dst)
                missing = [k for k in expected if k not in got]
                return not missing, missing

            done, missing, elapsed = wait_until(replicated, REPLICATION_TIMEOUT_SEC, REPLICATION_POLL_SEC)
            if not done:
                raise CaseFailed(f"{REPLICATION_TIMEOUT_SEC}초 안에 복제되지 않은 키 {len(missing)}개: "
                                 f"{missing[:MISSING_KEYS_SHOWN]}")
            self.result.replication_sec = round(elapsed, 1)
            wait_detail.append(f"복제 {elapsed:.1f}초")

        with self.step(6):
            src_objects = list_objects(self.s3, src)
            dst_objects = list_objects(self.s3, dst)
            extra = sorted(set(dst_objects) - set(expected))
            if extra:
                raise CaseFailed(f"대상 버킷에 업로드하지 않은 키 {len(extra)}개: {extra[:MISSING_KEYS_SHOWN]}")
            diffs = []
            for key, exp in expected.items():
                s = self.s3.head_object(Bucket=src, Key=key)
                d = self.s3.head_object(Bucket=dst, Key=key)
                s_info = (s["ContentLength"], s["ETag"].strip('"'))
                d_info = (d["ContentLength"], d["ETag"].strip('"'))
                if s_info != (exp["Size"], exp["ETag"]) or d_info != s_info:
                    diffs.append(f"{key}: 기대 {exp['Size']}/{exp['ETag']}, src {s_info}, dst {d_info}")
            if len(src_objects) != len(expected):
                diffs.append(f"원본 객체 수 {len(src_objects)} != 업로드 {len(expected)}")
            if diffs:
                raise CaseFailed(f"파일 불일치 {len(diffs)}건: {diffs[:MISSING_KEYS_SHOWN]}")

        want = Usage(len(expected), sum(v["Size"] for v in expected.values()))
        usage_detail: list[str] = []
        with self.step(7, usage_detail):
            def usage_matched():
                s, d = self.portal.bucket_usage(src), self.portal.bucket_usage(dst)
                self.result.src_usage, self.result.dst_usage = s, d
                return s == want and d == want, (s, d)

            done, (s, d), elapsed = wait_until(usage_matched, USAGE_TIMEOUT_SEC, USAGE_POLL_SEC)
            if not done:
                raise CaseFailed(f"{USAGE_TIMEOUT_SEC}초 안에 usage 가 맞지 않음: "
                                 f"기대 {want} / src {s} / dst {d}")
            usage_detail.append(f"{want}, 수렴 {elapsed:.1f}초")

    def cleanup(self) -> None:
        src, dst = self.result.src_bucket, self.result.dst_bucket
        if src:
            # 복제 룰을 먼저 지워야 원본 삭제가 대상에 다시 복제되지 않는다
            try:
                self.s3.delete_bucket_replication(Bucket=src)
            except Exception as e:  # noqa: BLE001
                logger.warning("복제 룰 삭제 실패: %s (%s)", src, e)
            empty_and_delete_bucket(self.s3, src)
        if dst:
            empty_and_delete_bucket(self.s3, dst)
