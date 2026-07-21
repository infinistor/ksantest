"""AWS SigV4 chunked upload signer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from s3tests.auth import aws4_signer_base as base

STREAMING_BODY_SHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
_CLRF = "\r\n"
_CHUNK_STRING_TO_SIGN_PREFIX = "AWS4-HMAC-SHA256-PAYLOAD"
_CHUNK_SIGNATURE_HEADER = ";chunk-signature="
_SIGNATURE_LENGTH = 64


class AWS4SignerForChunkedUpload:
    STREAMING_BODY_SHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

    def __init__(self, endpoint_url: str, http_method: str, service_name: str, region_name: str) -> None:
        self.endpoint_url = endpoint_url
        self.http_method = http_method
        self.service_name = service_name
        self.region_name = region_name or "us-east-1"
        self.last_computed_signature = ""
        self.date_time_stamp = ""
        self.scope = ""
        self.signing_key: bytes = b""

    def compute_signature(
        self,
        headers: Dict[str, str],
        query_parameters: Optional[Dict[str, str]],
        body_hash: str,
        access_key: str,
        secret_key: str,
    ) -> str:
        now = datetime.now(timezone.utc)
        self.date_time_stamp = now.strftime(base.ISO8601_BASIC_FORMAT)
        date_stamp = now.strftime(base.DATE_STRING_FORMAT)

        # Java mutates the caller's header map in-place (x-amz-date, Host).
        headers["x-amz-date"] = self.date_time_stamp
        headers["Host"] = base.format_host_header(self.endpoint_url)

        canonicalized_header_names = base.canonicalize_header_names(headers)
        canonicalized_headers = base.canonicalized_header_string(headers)
        canonicalized_query_parameters = base.canonicalized_query_string(query_parameters)
        canonical_request_text = base.canonical_request(
            self.endpoint_url,
            self.http_method,
            canonicalized_query_parameters,
            canonicalized_header_names,
            canonicalized_headers,
            body_hash,
        )

        self.scope = f"{date_stamp}/{self.region_name}/{self.service_name}/{base.TERMINATOR}"
        string_to_sign = base.string_to_sign(
            base.SCHEME,
            base.ALGORITHM,
            self.date_time_stamp,
            self.scope,
            canonical_request_text,
        )
        self.signing_key = base.signing_key(secret_key, date_stamp, self.region_name, self.service_name)
        signature = base._to_hex(base._sign(string_to_sign, self.signing_key))
        self.last_computed_signature = signature

        credentials = f"Credential={access_key}/{self.scope}"
        signed_headers = f"SignedHeaders={canonicalized_header_names}"
        signature_header = f"Signature={signature}"
        return f"{base.SCHEME}-{base.ALGORITHM} {credentials}, {signed_headers}, {signature_header}"

    @staticmethod
    def calculate_chunked_content_length(original_length: int, chunk_size: int) -> int:
        if original_length <= 0:
            raise ValueError("Nonnegative content length expected.")
        max_size_chunks = original_length // chunk_size
        remaining_bytes = original_length % chunk_size
        total = max_size_chunks * _calculate_chunk_header_length(chunk_size)
        if remaining_bytes > 0:
            total += _calculate_chunk_header_length(remaining_bytes)
        total += _calculate_chunk_header_length(0)
        return total

    def construct_signed_chunk(self, user_data_len: int, user_data: bytes) -> bytes:
        if user_data_len == 0:
            data_to_chunk = b""
        elif user_data_len < len(user_data):
            data_to_chunk = user_data[:user_data_len]
        else:
            data_to_chunk = user_data

        non_sign_extension = ""
        chunk_string_to_sign = "\n".join(
            [
                _CHUNK_STRING_TO_SIGN_PREFIX,
                self.date_time_stamp,
                self.scope,
                self.last_computed_signature,
                base._to_hex(base.hash_text(non_sign_extension)),
                base._to_hex(base.hash_bytes(data_to_chunk)),
            ]
        )
        chunk_signature = base._to_hex(base._sign(chunk_string_to_sign, self.signing_key))
        self.last_computed_signature = chunk_signature

        header = (
            f"{len(data_to_chunk):x}{non_sign_extension}{_CHUNK_SIGNATURE_HEADER}{chunk_signature}{_CLRF}"
        ).encode("utf-8")
        trailer = _CLRF.encode("utf-8")
        return header + data_to_chunk + trailer


def _calculate_chunk_header_length(chunk_data_size: int) -> int:
    return (
        len(f"{chunk_data_size:x}")
        + len(_CHUNK_SIGNATURE_HEADER)
        + _SIGNATURE_LENGTH
        + len(_CLRF)
        + chunk_data_size
        + len(_CLRF)
    )
