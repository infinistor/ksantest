"""복제 usage 테스트의 공용 부분: 설정, S3·포털 클라이언트, 단계 기록, 대기·정리 함수."""
from __future__ import annotations

import configparser
import hashlib
import logging
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime

import boto3
import requests
import urllib3
from botocore.config import Config as BotoConfig

# ── 기본값 ────────────────────────────────────────────────────────────
DEFAULT_FILE_COUNT = 10
DEFAULT_FILE_SIZES = "0,1K,64K,1M"
MAX_FILE_SIZE = 5 * 1024 ** 3  # GW MaxPutSize(5 GiB). 단일 PUT 상한

BUCKET_PREFIX = "rtest"
KEY_PREFIX = "obj"
S3_REGION = "us-east-1"
REQUEST_TIMEOUT_SEC = 60
VERIFY_TLS = False  # 테스트 서버는 자체 서명 인증서

REPLICATION_TIMEOUT_SEC = 300
REPLICATION_POLL_SEC = 5
USAGE_TIMEOUT_SEC = 300
USAGE_POLL_SEC = 10
KEEP_BUCKETS_ON_FAIL = True

PORTAL_BUCKET_PATH = "/api/v2/S3Api/Buckets/{bucket}"
PORTAL_AUTH_SCHEME = "Bearer"

SIZE_UNITS = {"": 1, "B": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}

logger = logging.getLogger("rtest")

if not VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ConfigError(Exception):
    pass


class CaseFailed(Exception):
    """케이스 판정 실패. 단계 번호와 이름은 Case.step 이 채운다."""


# ── 설정 ──────────────────────────────────────────────────────────────
def parse_size(text: str) -> int:
    m = re.fullmatch(r"\s*(\d+)\s*([BKMG]?)\s*", text.upper())
    if not m:
        raise ConfigError(f"file_sizes 의 크기 형식이 잘못됨: {text!r} (예: 0, 512B, 64K, 1M, 1G)")
    size = int(m.group(1)) * SIZE_UNITS[m.group(2)]
    if size > MAX_FILE_SIZE:
        raise ConfigError(f"file_sizes 의 {text!r} 가 단일 PUT 상한 5G 를 넘음")
    return size


def format_size(size: int) -> str:
    for unit in ("G", "M", "K"):
        if size >= SIZE_UNITS[unit] and size % SIZE_UNITS[unit] == 0:
            return f"{size // SIZE_UNITS[unit]}{unit}"
    return f"{size}B"


@dataclass
class Settings:
    portal_url: str
    api_key: str
    s3_url: str
    access_key: str
    secret_key: str
    file_count: int
    file_sizes: list[int] = field(default_factory=list)

    @classmethod
    def load(cls, path: str) -> "Settings":
        if not os.path.isfile(path):
            raise ConfigError(f"설정 파일이 없음: {path} (config.ini.example 을 복사해 채운다)")
        cp = configparser.ConfigParser()
        cp.read(path, encoding="utf-8")

        def get(section: str, key: str, required: bool = True) -> str:
            value = cp.get(section, key, fallback="").strip()
            if required and not value:
                raise ConfigError(f"[{section}] {key} 값이 비어 있음")
            return value

        count_text = get("test", "file_count", required=False)
        try:
            file_count = int(count_text) if count_text else DEFAULT_FILE_COUNT
        except ValueError:
            raise ConfigError(f"[test] file_count 가 정수가 아님: {count_text!r}") from None
        if file_count < 1:
            raise ConfigError("[test] file_count 는 1 이상이어야 함")
        sizes_text = get("test", "file_sizes", required=False) or DEFAULT_FILE_SIZES
        file_sizes = [parse_size(s) for s in sizes_text.split(",") if s.strip()]
        if not file_sizes:
            raise ConfigError("[test] file_sizes 에 크기가 없음")

        return cls(
            portal_url=get("portal", "url").rstrip("/"),
            api_key=get("portal", "api_key"),
            s3_url=get("s3", "url").rstrip("/"),
            access_key=get("s3", "access_key"),
            secret_key=get("s3", "secret_key"),
            file_count=file_count,
            file_sizes=file_sizes,
        )

    def object_sizes(self) -> list[int]:
        """file_sizes 를 file_count 만큼 돌아가며 배정한 크기 목록."""
        return [self.file_sizes[i % len(self.file_sizes)] for i in range(self.file_count)]


# ── 클라이언트 ────────────────────────────────────────────────────────
def make_s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=S3_REGION,
        verify=VERIFY_TLS,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=REQUEST_TIMEOUT_SEC,
            read_timeout=REQUEST_TIMEOUT_SEC,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


@dataclass
class Usage:
    file_count: int
    used_size: int

    def __str__(self) -> str:
        return f"count={self.file_count}, size={self.used_size}"


class PortalClient:
    def __init__(self, settings: Settings):
        self.base_url = settings.portal_url
        self.session = requests.Session()
        self.session.verify = VERIFY_TLS
        self.session.headers.update({
            "Authorization": f"{PORTAL_AUTH_SCHEME} {settings.api_key}",
            "AccessKey": settings.access_key,
            "SecretKey": settings.secret_key,
        })

    def bucket_usage(self, bucket: str) -> Usage:
        url = self.base_url + PORTAL_BUCKET_PATH.format(bucket=bucket)
        r = self.session.get(url, timeout=REQUEST_TIMEOUT_SEC)
        if r.status_code != 200:
            raise CaseFailed(f"포털 버킷 조회 실패: GET {url} → {r.status_code} {r.text[:300]}")
        body = r.json()
        # ResponseData 봉투({"Data": {...}})로 오거나 버킷 상세가 바로 온다
        data = _get_ci(body, "Data") if _get_ci(body, "Data") is not None else body
        if not isinstance(data, dict) or _get_ci(data, "FileCount") is None:
            raise CaseFailed(f"포털 응답에 FileCount 가 없음: {r.text[:300]}")
        return Usage(int(_get_ci(data, "FileCount") or 0), int(_get_ci(data, "UsedSize") or 0))


def _get_ci(d: dict, key: str):
    """대소문자 구분 없이 dict 값을 꺼낸다(PascalCase·camelCase 응답 모두 대응)."""
    lower = key.lower()
    for k, v in d.items():
        if k.lower() == lower:
            return v
    return None


# ── 케이스 실행 틀 ────────────────────────────────────────────────────
@dataclass
class CaseResult:
    case_id: str
    result: str = "PASS"  # PASS / FAIL / ERROR
    failed_step: str = ""
    message: str = ""
    replication_sec: float | None = None
    src_bucket: str = ""
    dst_bucket: str = ""
    src_usage: Usage | None = None
    dst_usage: Usage | None = None


class Case:
    """케이스 하나의 실행 문맥. 하위 클래스가 CASE_ID·TITLE·STEPS·run() 을 정한다."""

    CASE_ID = ""
    TITLE = ""
    STEPS: list[str] = []

    def __init__(self, settings: Settings, s3, portal: PortalClient):
        self.settings = settings
        self.s3 = s3
        self.portal = portal
        self.result = CaseResult(self.CASE_ID)

    def run(self) -> None:
        raise NotImplementedError

    def bucket_name(self, kind: str) -> str:
        """케이스마다 다른 버킷 이름(같은 초에 여러 케이스가 돌아도 겹치지 않게 케이스 ID 를 넣는다)."""
        return new_bucket_name(f"{self.CASE_ID.lower()}-{kind}")

    def cleanup(self) -> None:
        pass

    @contextmanager
    def step(self, index: int, detail_holder: list | None = None):
        """단계 하나를 감싸 '[TC01][4/7] 파일 업로드 ... OK (2.1초)' 로 기록한다.
        detail_holder 에 문자열을 넣으면 OK 뒤 괄호에 함께 찍는다."""
        name = self.STEPS[index - 1]
        tag = f"[{self.CASE_ID}][{index}/{len(self.STEPS)}] {name}"
        started = time.monotonic()
        try:
            yield
        except CaseFailed as e:
            self.result.failed_step = f"{index}/{len(self.STEPS)} {name}"
            logger.error("%s ... FAIL (%.1f초) %s", tag, time.monotonic() - started, e)
            raise
        except Exception:
            self.result.failed_step = f"{index}/{len(self.STEPS)} {name}"
            logger.exception("%s ... ERROR (%.1f초)", tag, time.monotonic() - started)
            raise
        detail = f", {detail_holder[0]}" if detail_holder else ""
        logger.info("%s ... OK (%.1f초%s)", tag, time.monotonic() - started, detail)

    def execute(self) -> CaseResult:
        logger.info("[%s] %s 시작", self.CASE_ID, self.TITLE)
        try:
            self.run()
        except CaseFailed as e:
            self.result.result, self.result.message = "FAIL", str(e)
        except Exception as e:  # noqa: BLE001 - 예상 못 한 오류도 결과로 남긴다
            self.result.result, self.result.message = "ERROR", f"{type(e).__name__}: {e}"
        if self.result.result == "PASS" or not KEEP_BUCKETS_ON_FAIL:
            self.cleanup()
        else:
            logger.warning("[%s] 실패하여 버킷을 남김: %s, %s",
                           self.CASE_ID, self.result.src_bucket, self.result.dst_bucket)
        logger.info("[%s] 결과 %s %s", self.CASE_ID, self.result.result, self.result.message)
        return self.result


# ── S3 보조 함수 ──────────────────────────────────────────────────────
def new_bucket_name(kind: str) -> str:
    return f"{BUCKET_PREFIX}-{kind}-{datetime.now():%Y%m%d%H%M%S}"


def random_body(size: int) -> tuple[bytes, str]:
    body = os.urandom(size)
    return body, hashlib.md5(body).hexdigest()


def list_objects(s3, bucket: str) -> dict[str, dict]:
    """버킷의 현재 객체를 {키: {Size, ETag}} 로 돌려준다."""
    objects = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = {"Size": obj["Size"], "ETag": obj["ETag"].strip('"')}
    return objects


def wait_until(check, timeout_sec: float, poll_sec: float):
    """check() 가 (완료 여부, 상태) 를 돌려줄 때 완료될 때까지 기다린다.
    (완료 여부, 마지막 상태, 걸린 초) 를 돌려준다."""
    started = time.monotonic()
    while True:
        done, state = check()
        elapsed = time.monotonic() - started
        if done or elapsed >= timeout_sec:
            return done, state, elapsed
        time.sleep(poll_sec)


def empty_and_delete_bucket(s3, bucket: str) -> None:
    """모든 버전·삭제 마커를 지우고 버킷을 삭제한다. 실패는 경고만 남긴다."""
    try:
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
            targets = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                       for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            for i in range(0, len(targets), 1000):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": targets[i:i + 1000], "Quiet": True})
        s3.delete_bucket(Bucket=bucket)
        logger.info("버킷 삭제: %s", bucket)
    except Exception as e:  # noqa: BLE001
        logger.warning("버킷 삭제 실패: %s (%s)", bucket, e)
