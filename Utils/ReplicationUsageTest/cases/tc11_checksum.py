"""TC11 체크섬: 추가 체크섬(CRC32·CRC32C·CRC64NVME·SHA1·SHA256)을 붙여 올린 객체의 체크섬 값이 대상에서도 같다.

151 에는 awscrt 가 없어 botocore 가 CRC32C·CRC64NVME 를 계산하지 못하므로 값을 직접 계산해 헤더로 보낸다.
"""
from __future__ import annotations

import base64
import hashlib
import os
import zlib

from cases.base import ReplicationCase
from common import CaseFailed


def _crc_table(poly: int) -> list[int]:
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        table.append(c)
    return table


_CRC32C_TABLE = _crc_table(0x82F63B78)
_CRC64NVME_TABLE = _crc_table(0x9A6C9329AC4BC9B5)


def _reflected_crc(data: bytes, table: list[int], mask: int) -> int:
    c = mask
    for b in data:
        c = table[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ mask


def crc32c(data: bytes) -> bytes:
    return _reflected_crc(data, _CRC32C_TABLE, 0xFFFFFFFF).to_bytes(4, "big")


def crc64nvme(data: bytes) -> bytes:
    return _reflected_crc(data, _CRC64NVME_TABLE, 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")


ALGORITHMS = {
    "CRC32": lambda d: zlib.crc32(d).to_bytes(4, "big"),
    "CRC32C": crc32c,
    "CRC64NVME": crc64nvme,
    "SHA1": lambda d: hashlib.sha1(d).digest(),
    "SHA256": lambda d: hashlib.sha256(d).digest(),
}


class Tc11Checksum(ReplicationCase):
    CASE_ID = "TC11"
    TITLE = "체크섬 복제"
    OPERATION = "추가 체크섬 5종 PUT"
    COMPARE_CHECKSUM = True

    def operate(self, detail: list) -> None:
        sizes = self.settings.object_sizes()
        for i, (name, func) in enumerate(ALGORITHMS.items()):
            size = sizes[i % len(sizes)]
            body = os.urandom(size)
            value = base64.b64encode(func(body)).decode()
            param = f"Checksum{name}"
            key = f"checksum-{name.lower()}"
            version_id = self.put(key, size, body=body, **{param: value})
            got = self.src_expected[(key, version_id)]["Checksum"].get(param)
            if got != value:
                raise CaseFailed(f"{key} PUT 응답 {param} 가 계산값과 다름: {got} != {value}")
        detail.append(", ".join(ALGORITHMS))
