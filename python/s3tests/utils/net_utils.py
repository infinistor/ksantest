"""Network URL and HTTP helpers ported from Java NetUtils."""

from __future__ import annotations

import io
import ssl
import urllib.error
import urllib.request
from typing import Dict, Optional
from urllib.parse import quote

from s3tests.auth.aws4_signer_chunked import AWS4SignerForChunkedUpload
from s3tests.data.form_file import FormFile
from s3tests.data.http_result import HttpResult
from s3tests.data import main_data as md

USER_DATA_BLOCK_SIZE = 64 * 1024
_LINE_FEED = "\r\n"


def create_region_http(region: str) -> str:
    return f"http://s3.{region}.amazonaws.com"


def create_region_https(region: str) -> str:
    return f"https://s3.{region}.amazonaws.com"


def create_url_to_http(address: str, port: int) -> str:
    url = f"{md.HTTP}{address}".rstrip("/")
    return append_port_if_non_default(url, port, 80)


def create_url_to_https(address: str, port: int) -> str:
    url = f"{md.HTTPS}{address}".rstrip("/")
    return append_port_if_non_default(url, port, 443)


def append_port_if_non_default(url: str, port: int, default_port: int) -> str:
    if port <= 0 or port == default_port:
        return url
    return f"{url}:{port}"


def format_authority(protocol: str, address: str, port: int) -> str:
    if port > 0 and port not in (80, 443):
        return f"{protocol}{address}:{port}"
    return f"{protocol}{address}"


def get_endpoint(protocol: str, address: str, port: int, bucket_name: str, key: str | None = None) -> str:
    base = format_authority(protocol, address, port)
    if key:
        return f"{base}/{bucket_name}/{key}"
    return f"{base}/{bucket_name}"


def get_endpoint_aws(protocol: str, region_name: str, bucket_name: str, key: str | None = None) -> str:
    if key:
        return f"{protocol}{bucket_name}.s3-{region_name}.amazonaws.com/{key}"
    return f"{protocol}{bucket_name}.s3-{region_name}.amazonaws.com"


def url_encode(value: str, keep_path_slash: bool = False) -> str:
    encoded = quote(value, safe="-_.~")
    if keep_path_slash:
        encoded = encoded.replace("%2F", "/")
    return encoded


def post_upload(send_url: str, headers: Dict[str, str], file_data: FormFile) -> HttpResult:
    result = HttpResult()
    try:
        boundary = f"{int(__import__('time').time() * 1000):x}"
        body = io.BytesIO()
        for key, value in headers.items():
            body.write(f"--{boundary}{_LINE_FEED}".encode("utf-8"))
            body.write(f'Content-Disposition: form-data; name="{key}"{_LINE_FEED}'.encode("utf-8"))
            body.write(_LINE_FEED.encode("utf-8"))
            body.write(f"{value}{_LINE_FEED}".encode("utf-8"))
        body.write(f"--{boundary}{_LINE_FEED}".encode("utf-8"))
        body.write(
            f'Content-Disposition: form-data; name="file"; filename="{file_data.name}"{_LINE_FEED}'.encode("utf-8")
        )
        body.write(f"Content-Type: {file_data.content_type}{_LINE_FEED}".encode("utf-8"))
        body.write(_LINE_FEED.encode("utf-8"))
        body.write(file_data.body.encode("utf-8"))
        body.write(_LINE_FEED.encode("utf-8"))
        body.write(f"--{boundary}--{_LINE_FEED}".encode("utf-8"))

        request = urllib.request.Request(
            send_url,
            data=body.getvalue(),
            method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=context, timeout=300) as response:
            result.status_code = response.status
            result.url = response.geturl()
    except urllib.error.HTTPError as exc:
        result.status_code = exc.code
        result.url = send_url
        result.message = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        result.message = str(exc)
    return result


def put_upload(endpoint: str, http_method: str, headers: Dict[str, str], request_body: str | None) -> HttpResult:
    try:
        data = request_body.encode("utf-8") if request_body is not None else None
        request = urllib.request.Request(endpoint, data=data, method=http_method)
        for key, value in headers.items():
            request.add_header(key, value)
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=context, timeout=300) as response:
            content = response.read().decode("utf-8", errors="replace")
            return HttpResult(status_code=response.status, message=content, url=response.geturl())
    except urllib.error.HTTPError as exc:
        return HttpResult(status_code=exc.code, message=exc.read().decode("utf-8", errors="replace"), url=endpoint)
    except Exception as exc:
        return HttpResult(message=str(exc), url=endpoint)


def put_upload_chunked(
    endpoint: str,
    http_method: str,
    headers: Dict[str, str],
    signer: AWS4SignerForChunkedUpload,
    request_body: str,
) -> HttpResult:
    try:
        buffer = bytearray(USER_DATA_BLOCK_SIZE)
        payload = io.BytesIO()
        data = request_body.encode("utf-8")
        offset = 0
        while offset < len(data):
            chunk_len = min(USER_DATA_BLOCK_SIZE, len(data) - offset)
            chunk = data[offset : offset + chunk_len]
            payload.write(signer.construct_signed_chunk(chunk_len, chunk))
            offset += chunk_len
        payload.write(signer.construct_signed_chunk(0, bytes(buffer)))

        request = urllib.request.Request(endpoint, data=payload.getvalue(), method=http_method)
        for key, value in headers.items():
            request.add_header(key, value)
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=context, timeout=300) as response:
            content = response.read().decode("utf-8", errors="replace")
            return HttpResult(status_code=response.status, message=content, url=response.geturl())
    except urllib.error.HTTPError as exc:
        return HttpResult(status_code=exc.code, message=exc.read().decode("utf-8", errors="replace"), url=endpoint)
    except Exception as exc:
        return HttpResult(message=str(exc), url=endpoint)


def cors_request(
    url: str,
    method: str,
    headers: Dict[str, str],
) -> tuple[int, Dict[str, str]]:
    request = urllib.request.Request(url, method=method)
    for key, value in headers.items():
        request.add_header(key, value)
    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(request, context=context, timeout=15) as response:
            return response.status, {k.lower(): v for k, v in response.headers.items()}
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}
