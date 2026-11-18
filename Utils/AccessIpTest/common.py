"""Access IP 테스트의 공용 부분: 설정, S3 클라이언트, 클라이언트 IP 감지, IP 케이스 생성.

포털 호출은 대상별로 달라 portal.py 에 있다.
"""
from __future__ import annotations

import configparser
import ipaddress
import logging
import os
import socket
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
import urllib3
from botocore.config import Config as BotoConfig

# ── 기본값 ────────────────────────────────────────────────────────────
DEFAULT_DELAY_SEC = 1.0          # 정책 설정·해제 후 GW 반영 대기
USER_PREFIX = "accessip"         # 사용자 이름: accessip-main-<시각>, accessip-alt-<시각>
BUCKET_PREFIX = "accessip-test"  # 버킷 이름: accessip-test-<시각>-a ~ d
BUCKET_ALL = "ALL"               # 사용자의 모든 버킷(=사용자 단위) 정책에 쓰는 예약 이름

S3_REGION = "us-east-1"
S3_CONNECT_TIMEOUT_SEC = 10      # 차단 시 빨리 실패하도록 짧게, 재시도 없음
S3_READ_TIMEOUT_SEC = 30
S3_MAX_POOL_CONNECTIONS = 64     # QoS 부하(버킷 둘 × QOS_WORKERS)가 한 클라이언트를 함께 쓴다
PORTAL_TIMEOUT_SEC = 30
VERIFY_TLS = False               # 테스트 서버는 자체 서명 인증서
USER_READY_TIMEOUT_SEC = 60      # 새 사용자 키가 GW 에 반영될 때까지 기다리는 상한
USER_READY_POLL_SEC = 2

# 원본 testlist 의 범위 케이스 끝값(.1-.44 / .46-.100 처럼 쓰던 값)
RANGE_LOW = 1
RANGE_MID = 100
RANGE_HIGH = 254
NEAR_SPAN = 5                    # ".40-.50" 처럼 클라이언트 앞뒤로 넓힌 범위의 폭

# XFF 우회 테스트에서 "허용 범위 밖" 으로 쓰는 주소(테스트 대역과 겹치지 않는 사설 IP)
XFF_OUTSIDE_IP = "10.254.254.254"

# 테스트 대상. 포털 API 가 다르다 (portal.py). ifs 는 InfiniStor 다.
PORTAL_TYPES = ("ksan", "ifs")
DEFAULT_PORTAL_TYPE = "ksan"
# 예전 이름. 이 이름으로 적힌 설정 파일과 --target 도 그대로 받는다.
PORTAL_TYPE_ALIASES = {"s3backend": "ifs"}
IFS_SECTION = "ifs"
IFS_SECTION_OLD = "s3backend"

# InfiniStor 전용 기본값 ([ifs] 섹션에서 바꾼다). 비밀번호는 기본값이 없다.
DEFAULT_IFS_VOLUME_NAME = "accessipvol"
DEFAULT_IFS_VOLUME_SIZE = "100GB"
DEFAULT_IFS_USER_SIZE = "50GB"

# ── QoS (--mode qos) ──────────────────────────────────────────────────
QOS_LIMIT = 20                   # 버킷에 거는 초당 요청 한도
QOS_LIMIT_CHANGED = 40           # 한도 변경 시나리오에서 바꾸는 값
QOS_MEASURE_SEC = 10             # 한도를 재는 시간. 1초 창이 이만큼 열린다
QOS_BASELINE_SEC = 3             # 한도가 없을 때 503 이 없는지 보는 시간
QOS_TOLERANCE = 0.10             # 통과 수의 허용 오차 (한도 × 시간 기준)
QOS_LOAD_FACTOR = 5              # 보내는 속도 = 한도의 몇 배
QOS_MIN_LOAD_FACTOR = 2          # 실제로 보낸 요청이 한도의 이 배수보다 적으면 측정 불가
QOS_WORKERS = 16                 # 버킷 하나에 동시에 보내는 스레드 수
QOS_STOP_GUARD_SEC = 0.2         # 측정 시간이 끝나기 이만큼 전에 보내기를 멈춘다
QOS_SETTLE_SEC = 2.0             # 측정 전 대기: 앞 측정의 1초 창이 끝나고 바꾼 한도가 반영될 시간
QOS_SET_RETRY_SEC = 10           # 방금 만든 버킷이 포털에 보일 때까지 한도 설정을 다시 시도하는 상한
QOS_SET_RETRY_POLL_SEC = 1

logger = logging.getLogger("accessip")

if not VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ConfigError(Exception):
    pass


class PortalError(Exception):
    pass


# ── 설정 ──────────────────────────────────────────────────────────────
@dataclass
class Settings:
    portal_url: str
    api_key: str
    s3_url: str
    client_ip: str   # 비어 있으면 자동 감지
    delay: float
    portal_type: str = DEFAULT_PORTAL_TYPE
    # 아래는 portal_type 이 ifs 일 때만 쓴다
    volume_name: str = ""
    volume_size: str = ""
    volume_password: str = ""
    user_password: str = ""
    user_size: str = ""

    @classmethod
    def load(cls, path: str, target: str | None = None) -> "Settings":
        """target 을 주면 [portal] type 대신 그 값을 쓴다(--target)."""
        if not os.path.isfile(path):
            raise ConfigError(f"설정 파일이 없음: {path} (config.ini.example 을 복사해 채운다)")
        cp = configparser.ConfigParser()
        cp.read(path, encoding="utf-8")

        def get(section: str, key: str, required: bool = True) -> str:
            value = cp.get(section, key, fallback="").strip()
            if required and not value:
                raise ConfigError(f"[{section}] {key} 값이 비어 있음")
            return value

        client_ip = get("test", "client_ip", required=False)
        if client_ip:
            try:
                ipaddress.IPv4Address(client_ip)
            except ValueError:
                raise ConfigError(f"[test] client_ip 가 IPv4 주소가 아님: {client_ip!r}") from None

        delay_text = get("test", "delay", required=False)
        try:
            delay = float(delay_text) if delay_text else DEFAULT_DELAY_SEC
        except ValueError:
            raise ConfigError(f"[test] delay 가 숫자가 아님: {delay_text!r}") from None
        if delay < 0:
            raise ConfigError("[test] delay 는 0 이상이어야 함")

        portal_type = (target or get("portal", "type", required=False) or DEFAULT_PORTAL_TYPE).lower()
        portal_type = PORTAL_TYPE_ALIASES.get(portal_type, portal_type)
        if portal_type not in PORTAL_TYPES:
            raise ConfigError(f"대상이 {'·'.join(PORTAL_TYPES)} 중 하나가 아님: {portal_type!r}")

        settings = cls(
            portal_url=get("portal", "url").rstrip("/"),
            api_key=get("portal", "api_key"),
            s3_url=get("s3", "url").rstrip("/"),
            client_ip=client_ip,
            delay=delay,
            portal_type=portal_type,
        )
        if portal_type == "ifs":
            # 예전 이름의 섹션만 있는 설정 파일은 그 섹션을 읽는다
            section = IFS_SECTION_OLD if cp.has_section(IFS_SECTION_OLD) and not cp.has_section(IFS_SECTION) \
                else IFS_SECTION
            settings.volume_name = get(section, "volume_name", required=False) or DEFAULT_IFS_VOLUME_NAME
            settings.volume_size = get(section, "volume_size", required=False) or DEFAULT_IFS_VOLUME_SIZE
            settings.user_size = get(section, "user_size", required=False) or DEFAULT_IFS_USER_SIZE
            settings.volume_password = get(section, "volume_password")
            settings.user_password = get(section, "user_password")
        return settings


# ── 클라이언트 IP·케이스 ──────────────────────────────────────────────
def detect_client_ip(s3_url: str) -> str:
    """S3 주소로 나가는 경로의 출발지 IP. UDP connect 라 실제 패킷은 나가지 않는다."""
    parsed = urlparse(s3_url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((parsed.hostname, port))
        return sock.getsockname()[0]


@dataclass
class IpCase:
    case_id: str
    ip: str          # 포털에 등록할 값(단일, CIDR, 범위)
    allowed: bool    # 클라이언트 IP 가 이 범위에 들어가는가
    title: str


def build_ip_cases(client_ip: str) -> tuple[list[IpCase], list[str]]:
    """원본 testlist 10개와 같은 모양의 케이스를 클라이언트 IP 기준으로 만든다.

    클라이언트 끝자리 때문에 만들 수 없는 케이스는 건너뛰고 (케이스 ID, 이유) 를 돌려준다.
    """
    a, b, c, d = (int(x) for x in client_ip.split("."))
    net = f"{a}.{b}.{c}"
    other_net = f"{a}.{b}.{(c + 1) % 256}"

    def host(n: int) -> str:
        return f"{net}.{n}"

    def rng(lo: int, hi: int) -> str:
        return f"{host(lo)}-{host(hi)}"

    cases: list[IpCase] = []
    skipped: list[str] = []

    def add(case_id: str, ok: bool, ip: str, allowed: bool, title: str, reason: str = "") -> None:
        if ok:
            cases.append(IpCase(case_id, ip, allowed, title))
        else:
            skipped.append(f"{case_id} {title}: {reason}")

    add("IP01", True, f"{other_net}.1/24", False, "다른 /24")
    add("IP02", True, f"{net}.1/24", True, "자기 /24")
    add("IP03", d - 1 >= RANGE_LOW, host(d - 1), False, "단일 IP, 클라이언트 -1", "앞 주소 없음")
    add("IP04", d + 1 <= RANGE_HIGH, host(d + 1), False, "단일 IP, 클라이언트 +1", "뒤 주소 없음")
    add("IP05", True, client_ip, True, "단일 IP, 클라이언트")
    add("IP06", d - 1 >= RANGE_LOW, rng(RANGE_LOW, d - 1), False, "범위, 바로 앞까지", "앞 주소 없음")
    mid_end = RANGE_MID if d + 1 <= RANGE_MID else RANGE_HIGH
    add("IP07", d + 1 <= RANGE_HIGH, rng(d + 1, mid_end), False, "범위, 바로 뒤부터", "뒤 주소 없음")
    add("IP08", True, rng(max(RANGE_LOW, d - 1), min(RANGE_HIGH, d + 1)), True, "범위, 앞뒤 1")
    add("IP09", True, rng(max(RANGE_LOW, d - NEAR_SPAN), min(RANGE_HIGH, d + NEAR_SPAN)), True,
        f"범위, 앞뒤 {NEAR_SPAN}")
    add("IP10", True, rng(RANGE_LOW, RANGE_HIGH), True, "범위, 전체")
    return cases, skipped


def inside_ip(policy_ip: str) -> str:
    """포털에 등록한 Access IP 값(단일/CIDR/범위) 안에 드는 주소 하나를 돌려준다.

    XFF 우회 테스트에서 "허용 범위 안" 주소를 헤더에 넣어보는 용도.
    """
    if "/" in policy_ip:
        net = ipaddress.ip_network(policy_ip, strict=False)
        host = net.network_address + (1 if net.num_addresses > 1 else 0)
        return str(host)
    if "-" in policy_ip:
        low, _, _ = policy_ip.partition("-")
        return low.strip()
    return policy_ip.strip()


# ── S3 ────────────────────────────────────────────────────────────────
def make_s3_client(settings: Settings, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=S3_REGION,
        verify=VERIFY_TLS,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=S3_CONNECT_TIMEOUT_SEC,
            read_timeout=S3_READ_TIMEOUT_SEC,
            retries={"max_attempts": 1, "mode": "standard"},
            max_pool_connections=S3_MAX_POOL_CONNECTIONS,
            # 응답 체크섬 검증을 켜 두면 botocore 가 GetObject 응답 본문을 검증용 스트림으로 감싼다.
            # 오류 응답에 체크섬 헤더가 붙어 오면 그 상태로 오류 본문을 읽다가 AttributeError·TypeError 로
            # 끝나 실제 상태 코드를 알 수 없게 된다(2026-11-18 InfiniStor 228). 필요한 경우에만 검증한다.
            response_checksum_validation="when_required",
        ),
    )


def _error_code(e: Exception) -> str:
    response = getattr(e, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code", "")) or type(e).__name__
    return type(e).__name__


def probe_list_buckets(s3) -> tuple[bool, str]:
    """ListBuckets 성공 여부와 실패 시 오류 코드."""
    try:
        s3.list_buckets()
        return True, ""
    except Exception as e:  # 차단은 AccessDenied 또는 연결 오류로 나타난다
        return False, _error_code(e)


def probe_list_buckets_with_xff(s3, xff_value: str) -> tuple[bool, str]:
    """ListBuckets 요청에 X-Forwarded-For 헤더를 붙여 성공 여부와 실패 코드를 돌려준다.

    boto3 서명이 끝난 뒤(before-send) 헤더를 넣어, 클라이언트가 임의로 XFF 를 보내는 상황을 흉내 낸다.
    """
    def add_header(request, **kwargs):
        request.headers["X-Forwarded-For"] = xff_value

    s3.meta.events.register("before-send.s3", add_header)
    try:
        return probe_list_buckets(s3)
    finally:
        s3.meta.events.unregister("before-send.s3", add_header)


def probe_list_objects(s3, bucket: str) -> tuple[bool, str]:
    """ListObjects 성공 여부와 실패 시 오류 코드."""
    try:
        s3.list_objects(Bucket=bucket)
        return True, ""
    except Exception as e:
        return False, _error_code(e)


def wait_user_ready(s3, name: str) -> None:
    """새 사용자 키가 GW 에 반영돼 ListBuckets 가 될 때까지 기다린다."""
    deadline = time.monotonic() + USER_READY_TIMEOUT_SEC
    while True:
        ok, code = probe_list_buckets(s3)
        if ok:
            return
        if time.monotonic() > deadline:
            raise PortalError(f"사용자 {name} 가 {USER_READY_TIMEOUT_SEC}초 안에 GW 에서 쓸 수 있게 되지 않음 ({code})")
        time.sleep(USER_READY_POLL_SEC)
