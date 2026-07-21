"""CRC combine helpers ported from Java Utility/CrcCombine."""

from __future__ import annotations

import struct
from typing import List

from botocore.compat import HAS_CRT

if HAS_CRT:
    import _awscrt
else:
    _awscrt = None

GF2_DIM = 64
POLYNOMIAL_64 = 0x9A6C9329AC4BC9B5


def _gf2_matrix_times(mat: List[int], vec: int) -> int:
    total = 0
    idx = 0
    while vec:
        if vec & 1:
            total ^= mat[idx]
        vec >>= 1
        idx += 1
    return total


def _gf2_matrix_square(square: List[int], mat: List[int]) -> None:
    for n in range(GF2_DIM):
        square[n] = _gf2_matrix_times(mat, mat[n])


def _crc64_combine(crc1: int, crc2: int, len2: int) -> int:
    if len2 == 0:
        return crc1

    even = [0] * GF2_DIM
    odd = [0] * GF2_DIM

    odd[0] = POLYNOMIAL_64
    row = 1
    for n in range(1, GF2_DIM):
        odd[n] = row
        row <<= 1

    _gf2_matrix_square(even, odd)
    _gf2_matrix_square(odd, even)

    while True:
        _gf2_matrix_square(even, odd)
        if len2 & 1:
            crc1 = _gf2_matrix_times(even, crc1)
        len2 >>= 1
        if len2 == 0:
            break

        _gf2_matrix_square(odd, even)
        if len2 & 1:
            crc1 = _gf2_matrix_times(odd, crc1)
        len2 >>= 1
        if len2 == 0:
            break

    return crc1 ^ crc2


def _byte_to_int(data: bytes) -> int:
    buffer = bytearray(8)
    buffer[8 - len(data) :] = data
    return struct.unpack(">Q", buffer)[0]


def _byte_to_long(data: bytes) -> int:
    return struct.unpack(">Q", data)[0]


def _get_checksum_bytes(value: int) -> bytes:
    return struct.pack(">Q", value & 0xFFFFFFFFFFFFFFFF)[4:8]


def _long_to_bytes(value: int) -> bytes:
    return struct.pack(">Q", value & 0xFFFFFFFFFFFFFFFF)


def _combine_crc32(crc1: int, crc2: int, len2: int) -> int:
    if _awscrt is not None and hasattr(_awscrt, "checksums_crc32_combine"):
        return _awscrt.checksums_crc32_combine(crc1, crc2, len2)
    try:
        from awscrt.checksums import combine_crc32

        return combine_crc32(crc1, crc2, len2)
    except (ImportError, AttributeError) as exc:
        raise ImportError(
            "CRC32 combine requires awscrt with combine_crc32 support"
        ) from exc


def _combine_crc32c(crc1: int, crc2: int, len2: int) -> int:
    if _awscrt is not None and hasattr(_awscrt, "checksums_crc32c_combine"):
        return _awscrt.checksums_crc32c_combine(crc1, crc2, len2)
    try:
        from awscrt.checksums import combine_crc32c

        return combine_crc32c(crc1, crc2, len2)
    except (ImportError, AttributeError) as exc:
        raise ImportError(
            "CRC32C combine requires awscrt with combine_crc32c support"
        ) from exc


def _combine_crc64nvme(crc1: int, crc2: int, len2: int) -> int:
    if _awscrt is not None and hasattr(_awscrt, "checksums_crc64nvme_combine"):
        return _awscrt.checksums_crc64nvme_combine(crc1, crc2, len2)
    try:
        from awscrt.checksums import combine_crc64nvme

        return combine_crc64nvme(crc1, crc2, len2)
    except (ImportError, AttributeError):
        return _crc64_combine(crc1, crc2, len2)


def combine_bytes(crc1: bytes, crc2: bytes, original_length_of_crc2: int, algorithm: str) -> bytes:
    algorithm = algorithm.upper()
    if algorithm == "CRC64NVME":
        combined = _combine_crc64nvme(_byte_to_long(crc1), _byte_to_long(crc2), original_length_of_crc2)
        return _long_to_bytes(combined)

    crc1_value = _byte_to_int(crc1)
    crc2_value = _byte_to_int(crc2)
    if algorithm == "CRC32":
        combined = _combine_crc32(crc1_value, crc2_value, original_length_of_crc2)
    elif algorithm == "CRC32C":
        combined = _combine_crc32c(crc1_value, crc2_value, original_length_of_crc2)
    else:
        raise ValueError(f"Invalid type: {algorithm}")
    return _get_checksum_bytes(combined)
