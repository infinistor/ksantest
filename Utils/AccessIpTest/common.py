"""Access IP 테스트의 공용 부분: 설정, 포털·S3 클라이언트, 클라이언트 IP 감지, IP 케이스 생성."""
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
import requests
import urllib3
from botocore.config import Config as BotoConfig

# ── 기본값 ────────────────────────────────────────────────────────────
DEFAULT_DELAY_SEC = 1.0          # 정책 설정·해제 후 GW 반영 대기
USER_PREFIX = "accessip"         # 사용자 이름: accessip-main-<시각>, accessip-alt-<시각>
BUCKET_PREFIX = "accessip-test"  # 버킷 이름: accessip-test-<시각>-a ~ d
EMAIL_DOMAIN = "pspace.co.kr"
USER_DESCRIPTION = "ksantest AccessIpTest user"
ACCESS_IP_DESCRIPTION = "ksantest AccessIpTest"
BUCKET_ALL = "ALL"               # 사용자의 모든 버킷(=사용자 단위) 정책에 쓰는 예약 이름

S3_REGION = "us-east-1"
S3_CONNECT_TIMEOUT_SEC = 10      # 차단 시 빨리 실패하도록 짧게, 재시도 없음
S3_READ_TIMEOUT_SEC = 30
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

KSAN_USERS_API = "/api/v2/KsanUsers"
ACCESS_IPS_API = "/api/v2/S3/AccessIps"

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

        return cls(
            portal_url=get("portal", "url").rstrip("/"),
            api_key=get("portal", "api_key"),
            s3_url=get("s3", "url").rstrip("/"),
            client_ip=client_ip,
            delay=delay,
        )


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


# ── 포털 ──────────────────────────────────────────────────────────────
class PortalClient:
    def __init__(self, settings: Settings):
        self.base = settings.portal_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": settings.api_key, "Accept": "application/json"})
        self.session.verify = VERIFY_TLS

    def _call(self, method: str, path: str, *, params=None, body=None):
        try:
            r = self.session.request(method, self.base + path, params=params, json=body,
                                     timeout=PORTAL_TIMEOUT_SEC)
        except requests.RequestException as e:
            raise PortalError(f"{method} {path} 요청 실패: {e}") from e
        if r.status_code != 200:
            raise PortalError(f"{method} {path} → HTTP {r.status_code}: {r.text.strip()[:300]}")
        if not r.text.strip():
            return None
        try:
            return r.json()
        except ValueError as e:
            raise PortalError(f"{method} {path} 응답이 JSON 이 아님: {r.text[:200]}") from e

    def check(self) -> None:
        data = self._call("GET", KSAN_USERS_API, params={"skip": 0, "countPerPage": 1})
        if not isinstance(data, list):
            raise PortalError(f"GET {KSAN_USERS_API} 응답이 목록이 아님")

    def create_user(self, name: str) -> dict:
        data = self._call("POST", KSAN_USERS_API, body={
            "Name": name, "Email": f"{name}@{EMAIL_DOMAIN}", "Description": USER_DESCRIPTION})
        if not isinstance(data, dict) or not data.get("Id") or not data.get("AccessKey"):
            raise PortalError(f"사용자 생성({name}) 응답에 Id·AccessKey 가 없음: {data}")
        return data

    def delete_user(self, user_id: str) -> None:
        self._call("DELETE", f"{KSAN_USERS_API}/{user_id}")

    def set_access_ip(self, user_id: str, ip: str, bucket: str) -> None:
        data = self._call("POST", ACCESS_IPS_API, body={
            "UserId": user_id, "IpAddress": ip, "BucketName": bucket, "Description": ACCESS_IP_DESCRIPTION})
        if not isinstance(data, dict) or not data.get("AddressId"):
            raise PortalError(f"Access IP 등록({ip}, {bucket}) 응답에 AddressId 가 없음: {data}")

    def clear_access_ips(self, user_id: str) -> None:
        self._call("DELETE", ACCESS_IPS_API, params={"userId": user_id})

    def list_access_ips(self, user_id: str) -> list:
        return self._call("GET", ACCESS_IPS_API, params={"userId": user_id}) or []


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
