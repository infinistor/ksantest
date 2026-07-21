"""AWS Signature Version 4 base helpers ported from Java AWS4SignerBase."""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import quote, urlparse

from s3tests.utils import net_utils

EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
SCHEME = "AWS4"
ALGORITHM = "HMAC-SHA256"
TERMINATOR = "aws4_request"
X_AMZ_CONTENT_SHA256 = "X-Amz-Content-SHA256"
ISO8601_BASIC_FORMAT = "%Y%m%dT%H%M%SZ"
DATE_STRING_FORMAT = "%Y%m%d"


def get_amz_date(dt: Optional[datetime] = None) -> str:
    value = dt or datetime.now(timezone.utc)
    return value.strftime(ISO8601_BASIC_FORMAT)


def get_post_policy_signature(secret_key: str, date_stamp: str, region_name: str, policy_base64: str) -> str:
    k_secret = (SCHEME + secret_key).encode("utf-8")
    k_date = _sign(date_stamp, k_secret)
    k_region = _sign(region_name, k_date)
    k_service = _sign("s3", k_region)
    k_signing = _sign(TERMINATOR, k_service)
    return _to_hex(_sign(policy_base64, k_signing))


def hash_text(text: str) -> bytes:
    return hashlib.sha256(text.encode("utf-8")).digest()


def hash_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _to_hex(data: bytes) -> str:
    return data.hex()


def _sign(data: str, key: bytes) -> bytes:
    return hmac.new(key, data.encode("utf-8"), hashlib.sha256).digest()


def canonicalize_header_names(headers: Dict[str, str]) -> str:
    return ";".join(sorted((k.lower() for k in headers.keys()), key=str.lower))


def canonicalized_header_string(headers: Dict[str, str]) -> str:
    if not headers:
        return ""
    lines = []
    for key in sorted(headers.keys(), key=str.lower):
        value = " ".join(headers[key].split())
        lines.append(f"{key.lower()}:{value}")
    return "\n".join(lines) + "\n"


def canonicalized_resource_path(endpoint: str) -> str:
    path = urlparse(endpoint).path or "/"
    encoded = net_utils.url_encode(path, keep_path_slash=True)
    return encoded if encoded.startswith("/") else f"/{encoded}"


def format_host_header(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    host = parsed.hostname or ""
    if parsed.port and parsed.port not in (80, 443):
        return f"{host}:{parsed.port}"
    return host


def canonicalized_query_string(parameters: Optional[Dict[str, str]]) -> str:
    if not parameters:
        return ""
    items = []
    for key in sorted(parameters.keys()):
        items.append(
            f"{net_utils.url_encode(key, keep_path_slash=False)}="
            f"{net_utils.url_encode(parameters[key], keep_path_slash=False)}"
        )
    return "&".join(items)


def canonical_request(
    endpoint: str,
    http_method: str,
    query_parameters: str,
    canonicalized_header_names: str,
    canonicalized_headers: str,
    body_hash: str,
) -> str:
    return "\n".join(
        [
            http_method,
            canonicalized_resource_path(endpoint),
            query_parameters,
            canonicalized_headers,
            canonicalized_header_names,
            body_hash,
        ]
    )


def string_to_sign(scheme: str, algorithm: str, date_time: str, scope: str, canonical_request_text: str) -> str:
    return "\n".join(
        [
            f"{scheme}-{algorithm}",
            date_time,
            scope,
            _to_hex(hash_text(canonical_request_text)),
        ]
    )


def signing_key(secret_key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
    k_secret = (SCHEME + secret_key).encode("utf-8")
    k_date = _sign(date_stamp, k_secret)
    k_region = _sign(region_name, k_date)
    k_service = _sign(service_name, k_region)
    return _sign(TERMINATOR, k_service)
