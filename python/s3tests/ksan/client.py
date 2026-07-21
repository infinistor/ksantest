"""KSAN admin API client ported from Java KsanClient."""

from __future__ import annotations

import urllib.request
import ssl
from typing import Dict
from urllib.parse import urlencode

from s3tests.auth import aws4_signer_base as base
from s3tests.auth.aws4_signer_auth_header import AWS4SignerForAuthorizationHeader


class KsanClient:
    METHOD_DELETE = "DELETE"
    METHOD_POST = "POST"
    METHOD_GET = "GET"
    METHOD_PUT = "PUT"
    HEADER_CONTENT_LENGTH = "content-length"
    HEADER_CONTENT_TYPE = "content-type"
    DEFAULT_CONTENT_TYPE = "text/plain"

    def __init__(self, host: str, port: int, access_key: str, secret_key: str) -> None:
        self.host = host
        self.port = port
        self.access_key = access_key
        self.secret_key = secret_key
        self._ssl_context = ssl._create_unverified_context()

    def delete_bucket_tag_index(self, bucket_name: str) -> None:
        query = "tag-index"
        url = f"http://{self.host}:{self.port}/{bucket_name}/?{query}"
        headers: Dict[str, str] = {
            base.X_AMZ_CONTENT_SHA256: base.EMPTY_BODY_SHA256,
            self.HEADER_CONTENT_LENGTH: "0",
            self.HEADER_CONTENT_TYPE: self.DEFAULT_CONTENT_TYPE,
        }
        signer = AWS4SignerForAuthorizationHeader(url, self.METHOD_DELETE, "s3", "us-west-2")
        headers["Authorization"] = signer.compute_signature(headers, {query: ""}, base.EMPTY_BODY_SHA256, self.access_key, self.secret_key)
        self._send_request(self.METHOD_DELETE, url, headers, None)

    def _send_request(self, method: str, url: str, headers: Dict[str, str], body: str | None) -> None:
        data = body.encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        for key, value in headers.items():
            request.add_header(key, value)
        with urllib.request.urlopen(request, context=self._ssl_context) as response:
            if response.status not in (200, 204):
                raise RuntimeError(f"Request failed with response code: {response.status}")
