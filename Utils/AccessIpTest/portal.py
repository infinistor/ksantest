"""포털 호출. 대상(KSAN / S3Backend)마다 API 가 달라 클래스를 나눈다.

두 클래스는 같은 메서드를 가지므로 시나리오(cases.py)와 실행부(run.py)는 대상을 구분하지 않는다.
- prepare()                      테스트 전에 한 번. S3Backend 는 볼륨을 준비한다.
- create_user(name)              사용자를 만들고 PortalUser(식별자, S3 키)를 돌려준다.
- delete_user(user_id)
- set_access_ip(user_id, ip, bucket)
- clear_access_ips(user_id)      그 사용자의 Access IP 를 모두 지운다.

user_id 는 포털이 사용자를 가리키는 값이다. KSAN 은 사용자 UUID, S3Backend 는 사용자 이름이다.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import quote

import requests

from common import PORTAL_TIMEOUT_SEC, VERIFY_TLS, PortalError, Settings, logger

# ── 기본값 ────────────────────────────────────────────────────────────
EMAIL_DOMAIN = "pspace.co.kr"
USER_DESCRIPTION = "ksantest AccessIpTest user"
ACCESS_IP_DESCRIPTION = "ksantest AccessIpTest"

# KSAN
KSAN_USERS_API = "/api/v2/KsanUsers"
KSAN_ACCESS_IPS_API = "/api/v2/S3/AccessIps"

# S3Backend
S3B_HEALTH_API = "/api/v1/Health"
S3B_VOLUMES_API = "/api/v1/Volumes"
S3B_USERS_API = "/api/v1/SystemUsers"
S3B_ACCESS_IPS_API = "/api/v1/S3/AccessIps"
S3B_VOLUME_STATUS_ONLINE = 256
S3B_VOLUME_STATUS_OFFLINE = 257
S3B_VOLUME_STATUS_NAMES = {"processing": 0, "online": S3B_VOLUME_STATUS_ONLINE,
                           "offline": S3B_VOLUME_STATUS_OFFLINE}
S3B_DISK_SIZE_UNITS = {"TB": 0, "GB": 1, "MB": 2}   # 포털의 DiskSizeUnit 코드
S3B_VOLUME_READY_TIMEOUT_SEC = 120   # 볼륨을 시작한 뒤 Online 이 될 때까지 기다리는 상한
S3B_VOLUME_READY_POLL_SEC = 2


@dataclass
class PortalUser:
    user_id: str
    access_key: str
    secret_key: str


class BasePortal:
    def __init__(self, settings: Settings):
        self.base = settings.portal_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": settings.api_key, "Accept": "application/json"})
        self.session.verify = VERIFY_TLS

    def _is_ok(self, response: requests.Response) -> bool:
        return response.status_code == 200

    def _call(self, method: str, path: str, *, params=None, body=None):
        try:
            r = self.session.request(method, self.base + path, params=params, json=body,
                                     timeout=PORTAL_TIMEOUT_SEC)
        except requests.RequestException as e:
            raise PortalError(f"{method} {path} 요청 실패: {e}") from e
        if not self._is_ok(r):
            raise PortalError(f"{method} {path} → HTTP {r.status_code}: {r.text.strip()[:300]}")
        if not r.text.strip():
            return None
        try:
            return r.json()
        except ValueError as e:
            raise PortalError(f"{method} {path} 응답이 JSON 이 아님: {r.text[:200]}") from e

    def prepare(self) -> None:
        """테스트 전에 한 번 부른다. 준비할 것이 없는 대상은 아무것도 하지 않는다."""


# ── KSAN ──────────────────────────────────────────────────────────────
class KsanPortal(BasePortal):
    def check(self) -> None:
        data = self._call("GET", KSAN_USERS_API, params={"skip": 0, "countPerPage": 1})
        if not isinstance(data, list):
            raise PortalError(f"GET {KSAN_USERS_API} 응답이 목록이 아님")

    def create_user(self, name: str) -> PortalUser:
        data = self._call("POST", KSAN_USERS_API, body={
            "Name": name, "Email": f"{name}@{EMAIL_DOMAIN}", "Description": USER_DESCRIPTION})
        if (not isinstance(data, dict) or not data.get("Id") or not data.get("AccessKey")
                or not data.get("SecretKey")):
            raise PortalError(f"사용자 생성({name}) 응답에 Id·AccessKey·SecretKey 가 없음")
        return PortalUser(str(data["Id"]), str(data["AccessKey"]), str(data["SecretKey"]))

    def delete_user(self, user_id: str) -> None:
        self._call("DELETE", f"{KSAN_USERS_API}/{user_id}")

    def set_access_ip(self, user_id: str, ip: str, bucket: str) -> None:
        data = self._call("POST", KSAN_ACCESS_IPS_API, body={
            "UserId": user_id, "IpAddress": ip, "BucketName": bucket, "Description": ACCESS_IP_DESCRIPTION})
        if not isinstance(data, dict) or not data.get("AddressId"):
            raise PortalError(f"Access IP 등록({ip}, {bucket}) 응답에 AddressId 가 없음: {data}")

    def clear_access_ips(self, user_id: str) -> None:
        self._call("DELETE", KSAN_ACCESS_IPS_API, params={"userId": user_id})


# ── S3Backend ─────────────────────────────────────────────────────────
def _result_ok(value) -> bool:
    """응답의 Result 가 성공인가. 포털 버전에 따라 숫자 1 또는 문자열로 온다."""
    return value == 1 or str(value).lower() in {"1", "success", "ok", "true"}


def _parse_size(size: str) -> tuple[int, int]:
    """"100GB" → (QuotaSize, DiskSizeUnit)."""
    text = size.strip().upper()
    unit = text[-2:]
    if unit not in S3B_DISK_SIZE_UNITS or not text[:-2].isdigit():
        raise PortalError(f"크기는 숫자 뒤에 MB·GB·TB 를 붙인다: {size!r}")
    return int(text[:-2]), S3B_DISK_SIZE_UNITS[unit]


def _volume_status(value) -> int:
    """볼륨 Status 를 숫자 코드로 바꾼다. 포털 버전에 따라 숫자 또는 이름으로 온다."""
    if isinstance(value, bool):
        return -1
    if isinstance(value, int):
        return value
    text = str(value).strip().lower()
    return int(text) if text.isdigit() else S3B_VOLUME_STATUS_NAMES.get(text, -1)


class S3BackendPortal(BasePortal):
    """볼륨에 사용자를 마운트해야 S3 키가 나온다. Access IP 는 (볼륨, 사용자 이름) 으로 건다."""

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.volume = settings.volume_name
        self.volume_size = settings.volume_size
        self.volume_password = settings.volume_password
        self.user_password = settings.user_password
        self.user_size = settings.user_size

    def _is_ok(self, response: requests.Response) -> bool:
        return response.ok

    def _call_ok(self, method: str, path: str, action: str, *, params=None, body=None) -> dict:
        data = self._call(method, path, params=params, body=body)
        if not isinstance(data, dict) or not _result_ok(data.get("Result")):
            detail = data if not isinstance(data, dict) else (
                f"Result={data.get('Result')} Code={data.get('Code')} Message={data.get('Message')}")
            raise PortalError(f"{action} 실패: {detail}")
        return data

    def check(self) -> None:
        data = self._call("GET", S3B_HEALTH_API)
        if not isinstance(data, dict):
            raise PortalError(f"GET {S3B_HEALTH_API} 응답이 객체가 아님")
        result = data.get("Result")
        if isinstance(result, int) and not isinstance(result, bool) and result < 0:
            raise PortalError(f"GET {S3B_HEALTH_API} Result={result}")

    # 볼륨
    def _get_volume(self) -> dict | None:
        data = self._call("GET", f"{S3B_VOLUMES_API}/{quote(self.volume)}")
        if not isinstance(data, dict):
            return None
        result, volume = data.get("Result"), data.get("Data")
        if (isinstance(result, int) and result < 0) or not isinstance(volume, dict):
            return None
        return volume

    def _start_volume(self) -> None:
        self._call_ok("POST", f"{S3B_VOLUMES_API}/{quote(self.volume)}/Start", f"볼륨 시작({self.volume})",
                      body={})

    def _wait_volume_online(self) -> None:
        deadline = time.monotonic() + S3B_VOLUME_READY_TIMEOUT_SEC
        while True:
            volume = self._get_volume()
            status = volume.get("Status") if volume else None
            if volume and _volume_status(status) == S3B_VOLUME_STATUS_ONLINE:
                return
            if time.monotonic() > deadline:
                raise PortalError(f"볼륨 {self.volume} 이 {S3B_VOLUME_READY_TIMEOUT_SEC}초 안에 Online 이 되지 않음 "
                                  f"(Status={status})")
            time.sleep(S3B_VOLUME_READY_POLL_SEC)

    def prepare(self) -> None:
        """볼륨이 없으면 만들어 시작하고, 꺼져 있으면 시작한다. 테스트가 끝나도 볼륨은 지우지 않는다."""
        volume = self._get_volume()
        if volume is None:
            quota, unit = _parse_size(self.volume_size)
            self._call_ok("POST", S3B_VOLUMES_API, f"볼륨 생성({self.volume})", body={
                "Name": self.volume, "ReplicationType": 1, "QuotaSize": quota, "DiskSizeUnit": unit,
                "Permission": 1, "Password": self.volume_password, "ConfirmPassword": self.volume_password,
                "SecurityLevel": 0, "Remark": None})
            self._start_volume()
            logger.info("볼륨 생성·시작: %s (%s)", self.volume, self.volume_size)
        elif _volume_status(volume.get("Status")) == S3B_VOLUME_STATUS_OFFLINE:
            self._start_volume()
            logger.info("볼륨 시작: %s", self.volume)
        else:
            logger.info("볼륨 사용: %s (Status=%s)", self.volume, volume.get("Status"))
            return
        self._wait_volume_online()

    # 사용자
    def create_user(self, name: str) -> PortalUser:
        quota, unit = _parse_size(self.user_size)
        self._call_ok("POST", S3B_USERS_API, f"사용자 생성({name})", body={
            "UserId": name, "UserName": name, "Password": self.user_password,
            "ConfirmPassword": self.user_password, "Department": "", "Email": None, "Remark": None,
            "Notification": False, "VolumeName": self.volume, "QuotaSize": quota, "DiskSizeUnit": unit})
        try:
            data = self._call_ok("GET", f"{S3B_VOLUMES_API}/{quote(self.volume)}/Users/{quote(name)}/Tenant",
                                 f"S3 키 조회({self.volume}, {name})").get("Data")
            if not isinstance(data, dict) or not data.get("AccessKey") or not data.get("AccessSecret"):
                raise PortalError(f"S3 키 조회({self.volume}, {name}) 응답에 AccessKey·AccessSecret 이 없음")
        except PortalError:
            # 만든 사용자를 호출한 쪽이 알 수 없으므로 여기서 지운다
            try:
                self.delete_user(name)
            except PortalError as e:
                logger.error("정리 실패: 사용자 %s 삭제: %s", name, e)
            raise
        return PortalUser(name, str(data["AccessKey"]), str(data["AccessSecret"]))

    def delete_user(self, user_id: str) -> None:
        self._call_ok("DELETE", f"{S3B_USERS_API}/{quote(user_id)}", f"사용자 삭제({user_id})")

    # Access IP
    def set_access_ip(self, user_id: str, ip: str, bucket: str) -> None:
        self._call_ok("POST", S3B_ACCESS_IPS_API, f"Access IP 등록({user_id}, {ip}, {bucket})", body={
            "userId": user_id, "tenantId": self.volume, "ipAddress": ip, "bucketName": bucket})

    def clear_access_ips(self, user_id: str) -> None:
        self._call_ok("DELETE", S3B_ACCESS_IPS_API, f"Access IP 해제({user_id})",
                      params={"TenantId": self.volume, "UserId": user_id})


PORTALS = {"ksan": KsanPortal, "s3backend": S3BackendPortal}


def make_portal(settings: Settings) -> BasePortal:
    return PORTALS[settings.portal_type](settings)
