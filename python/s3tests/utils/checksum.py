"""Checksum helpers ported from Java Utility/CheckSum."""

from __future__ import annotations

import base64
from hashlib import md5 as md5_hash, sha1 as sha1_hash, sha256 as sha256_hash, sha512 as sha512_hash
from typing import Any, Dict, List, Optional

from botocore.compat import HAS_CRT, has_minimum_crt_version
from botocore.httpchecksum import Crc32Checksum, Sha1Checksum, Sha256Checksum

if HAS_CRT:
    from awscrt import checksums as crt_checksums
else:
    crt_checksums = None

# Wire names match botocore/AWS (Java enum CRC64_NVME serializes as CRC64NVME).
FULL_OBJECT_ALGORITHMS = ["CRC32", "CRC32C", "CRC64NVME"]

COMPOSITE_ALGORITHMS = [
    "CRC32",
    "CRC32C",
    "SHA1",
    "SHA256",
    "MD5",
    "SHA512",
    "XXHASH64",
    "XXHASH3",
    "XXHASH128",
]

ALL_ALGORITHMS = [
    "CRC32",
    "CRC32C",
    "CRC64NVME",
    "SHA1",
    "SHA256",
    "MD5",
    "SHA512",
    "XXHASH64",
    "XXHASH3",
    "XXHASH128",
]

_RESPONSE_FIELD = {
    "CRC32": "ChecksumCRC32",
    "CRC32C": "ChecksumCRC32C",
    "CRC64NVME": "ChecksumCRC64NVME",
    "SHA1": "ChecksumSHA1",
    "SHA256": "ChecksumSHA256",
    "MD5": "ChecksumMD5",
    "SHA512": "ChecksumSHA512",
    "XXHASH64": "ChecksumXXHASH64",
    "XXHASH3": "ChecksumXXHASH3",
    "XXHASH128": "ChecksumXXHASH128",
}

_REQUEST_FIELD = dict(_RESPONSE_FIELD)

_ALIASES = {
    "CRC64_NVME": "CRC64NVME",
    "CRC32_C": "CRC32C",
}


def _normalize_algorithm(algorithm: str) -> str:
    value = algorithm.upper()
    return _ALIASES.get(value, value)


class _HashlibChecksum:
    def __init__(self, factory):
        self._checksum = factory()

    def update(self, chunk: bytes) -> None:
        self._checksum.update(chunk)

    def digest(self) -> bytes:
        return self._checksum.digest()

    def b64digest(self) -> str:
        return base64.b64encode(self.digest()).decode("ascii")

    def handle(self, body: bytes) -> str:
        self.update(body)
        return self.b64digest()


class _CrtChecksum:
    def __init__(self, func, initial=0, width=4):
        self._func = func
        self._initial = initial
        self._width = width
        self._value = initial

    def update(self, chunk: bytes) -> None:
        self._value = self._func(chunk, self._value)

    def digest(self) -> bytes:
        mask = (1 << (self._width * 8)) - 1
        return int(self._value & mask).to_bytes(self._width, byteorder="big")

    def b64digest(self) -> str:
        return base64.b64encode(self.digest()).decode("ascii")

    def handle(self, body: bytes) -> str:
        self.update(body)
        return self.b64digest()


def _xxhash_digest(data: bytes, bits: int, seed: int = 0) -> bytes:
    try:
        import xxhash
    except ImportError as exc:
        raise ImportError(
            "XXHASH algorithms require the xxhash package for local checksum calculation"
        ) from exc

    if bits == 64:
        return xxhash.xxh64(data, seed=seed).digest()
    if bits == 128:
        return xxhash.xxh128(data, seed=seed).digest()
    if bits == 3:
        return xxhash.xxh3_64(data, seed=seed).digest()
    raise ValueError(f"Unsupported xxhash width: {bits}")


def calculate_checksum(algorithm: str, content: str) -> str:
    data = content.encode("utf-8")
    algorithm = _normalize_algorithm(algorithm)

    if algorithm == "CRC32":
        return Crc32Checksum().handle(data)
    if algorithm == "SHA1":
        return Sha1Checksum().handle(data)
    if algorithm == "SHA256":
        return Sha256Checksum().handle(data)
    if algorithm == "MD5":
        return _HashlibChecksum(md5_hash).handle(data)
    if algorithm == "SHA512":
        return _HashlibChecksum(sha512_hash).handle(data)

    if crt_checksums is not None:
        if algorithm == "CRC32C":
            return _CrtChecksum(crt_checksums.crc32c).handle(data)
        if algorithm == "CRC64NVME":
            return _CrtChecksum(crt_checksums.crc64nvme, width=8).handle(data)
        if has_minimum_crt_version((0, 31, 2)) and hasattr(crt_checksums, "XXHash"):
            if algorithm == "XXHASH64":
                return base64.b64encode(crt_checksums.XXHash.compute_xxhash64(data)).decode("ascii")
            if algorithm == "XXHASH3":
                return base64.b64encode(crt_checksums.XXHash.compute_xxhash3_64(data)).decode("ascii")
            if algorithm == "XXHASH128":
                return base64.b64encode(crt_checksums.XXHash.compute_xxhash3_128(data)).decode("ascii")

    if algorithm == "XXHASH64":
        return base64.b64encode(_xxhash_digest(data, 64)).decode("ascii")
    if algorithm == "XXHASH3":
        return base64.b64encode(_xxhash_digest(data, 3)).decode("ascii")
    if algorithm == "XXHASH128":
        return base64.b64encode(_xxhash_digest(data, 128)).decode("ascii")

    if algorithm == "CRC32C":
        raise ImportError("CRC32C requires botocore[crt] (awscrt) for local checksum calculation")
    if algorithm == "CRC64NVME":
        raise ImportError("CRC64NVME requires botocore[crt] (awscrt) for local checksum calculation")

    raise ValueError(f"Unsupported checksum algorithm: {algorithm}")


def get_checksum(response: Dict[str, Any], algorithm: str) -> Optional[str]:
    field = _RESPONSE_FIELD.get(_normalize_algorithm(algorithm))
    if not field:
        return None
    return response.get(field)


def set_checksum(params: Dict[str, Any], algorithm: str, value: str) -> None:
    field = _REQUEST_FIELD.get(_normalize_algorithm(algorithm))
    if not field:
        raise ValueError(f"Unsupported checksum algorithm: {algorithm}")
    params[field] = value


def _requires_precomputed_checksum(algorithm: str) -> bool:
    algorithm = _normalize_algorithm(algorithm)
    if algorithm == "MD5":
        return True
    if algorithm in {"XXHASH64", "XXHASH3", "XXHASH128"}:
        return not (HAS_CRT and has_minimum_crt_version((0, 31, 2)))
    return False


def apply_checksum(params: Dict[str, Any], algorithm: str, content: str) -> None:
    algorithm = _normalize_algorithm(algorithm)
    if _requires_precomputed_checksum(algorithm):
        set_checksum(params, algorithm, calculate_checksum(algorithm, content))
    else:
        params["ChecksumAlgorithm"] = algorithm


def apply_put_checksum_params(params: Dict[str, Any], algorithm: str, content: str) -> Dict[str, Any]:
    apply_checksum(params, algorithm, content)
    return params


def checksum_compare_part(algorithm: str, content: str, response: Dict[str, Any]) -> None:
    """Compare part-level checksum (UploadPart). Java does not assert ChecksumType here."""
    algorithm = _normalize_algorithm(algorithm)
    expected = calculate_checksum(algorithm, content)
    actual = get_checksum(response, algorithm)
    assert expected == actual


def checksum_compare(algorithm: str, content: str, response: Dict[str, Any]) -> None:
    """Compare object-level checksum (PutObject). Java asserts FULL_OBJECT except MD5."""
    checksum_compare_part(algorithm, content, response)
    algorithm = _normalize_algorithm(algorithm)
    if algorithm != "MD5":
        assert response.get("ChecksumType") == "FULL_OBJECT"


def _calculate_checksum_bytes_from_byte_chunks(algorithm: str, chunks: List[bytes]) -> bytes:
    algorithm = _normalize_algorithm(algorithm)
    if not chunks:
        raise ValueError("checksum chunks must not be empty")

    if algorithm == "CRC32":
        checker = Crc32Checksum()
        for chunk in chunks:
            checker.update(chunk)
        return checker.digest()
    if algorithm == "SHA1":
        checker = Sha1Checksum()
        for chunk in chunks:
            checker.update(chunk)
        return checker.digest()
    if algorithm == "SHA256":
        checker = Sha256Checksum()
        for chunk in chunks:
            checker.update(chunk)
        return checker.digest()
    if algorithm == "MD5":
        hasher = _HashlibChecksum(md5_hash)
        for chunk in chunks:
            hasher.update(chunk)
        return hasher.digest()
    if algorithm == "SHA512":
        hasher = _HashlibChecksum(sha512_hash)
        for chunk in chunks:
            hasher.update(chunk)
        return hasher.digest()

    if crt_checksums is not None:
        if algorithm == "CRC32C":
            value = 0
            for chunk in chunks:
                value = crt_checksums.crc32c(chunk, value)
            return int(value & 0xFFFFFFFF).to_bytes(4, byteorder="big")
        if algorithm == "CRC64NVME":
            value = 0
            for chunk in chunks:
                value = crt_checksums.crc64nvme(chunk, value)
            return int(value & 0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="big")

    if algorithm in {"XXHASH64", "XXHASH3", "XXHASH128"}:
        combined = b"".join(chunks)
        if algorithm == "XXHASH64":
            return _xxhash_digest(combined, 64)
        if algorithm == "XXHASH3":
            return _xxhash_digest(combined, 3)
        return _xxhash_digest(combined, 128)

    if algorithm == "CRC32C":
        raise ImportError("CRC32C requires botocore[crt] (awscrt) for local checksum calculation")
    if algorithm == "CRC64NVME":
        raise ImportError("CRC64NVME requires botocore[crt] (awscrt) for local checksum calculation")

    raise ValueError(f"Unsupported checksum algorithm: {algorithm}")


def calculate_checksum_by_base64(algorithm: str, base64_checksums: List[str]) -> str:
    chunks = [base64.b64decode(value) for value in base64_checksums]
    digest = _calculate_checksum_bytes_from_byte_chunks(algorithm, chunks)
    return base64.b64encode(digest).decode("ascii") + f"-{len(base64_checksums)}"


def combine_checksum_by_base64(algorithm: str, part_size: int, base64_checksums: List[str]) -> str:
    from s3tests.utils import crc_combine

    algorithm = _normalize_algorithm(algorithm)
    if not base64_checksums:
        raise ValueError("checksum list must not be empty")

    combined = base64.b64decode(base64_checksums[0])
    for value in base64_checksums[1:]:
        combined = crc_combine.combine_bytes(
            combined,
            base64.b64decode(value),
            part_size,
            algorithm,
        )
    return base64.b64encode(combined).decode("ascii")


def checksum_compare_multipart(
    algorithm: str,
    upload_data: Any,
    complete_response: Dict[str, Any],
    *,
    source_checksum: Optional[str] = None,
) -> None:
    algorithm = _normalize_algorithm(algorithm)
    part_checksums = []
    for part in upload_data.parts:
        value = get_checksum(part, algorithm)
        assert value is not None, f"missing part checksum for {algorithm}"
        part_checksums.append(value)

    checksum_type = complete_response.get("ChecksumType")
    if checksum_type == "COMPOSITE":
        expected = calculate_checksum_by_base64(algorithm, part_checksums)
    else:
        try:
            expected = combine_checksum_by_base64(algorithm, upload_data.part_size, part_checksums)
        except ImportError:
            body = upload_data.get_body()
            if body:
                expected = calculate_checksum(algorithm, body)
            elif source_checksum is not None:
                expected = source_checksum
            else:
                raise

    actual = get_checksum(complete_response, algorithm)
    assert expected == actual, f"{algorithm} checksum compare failed"
