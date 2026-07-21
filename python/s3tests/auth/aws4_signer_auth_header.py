"""AWS SigV4 authorization header signer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from s3tests.auth import aws4_signer_base as base


class AWS4SignerForAuthorizationHeader:
    def __init__(self, endpoint_url: str, http_method: str, service_name: str, region_name: str) -> None:
        self.endpoint_url = endpoint_url
        self.http_method = http_method
        self.service_name = service_name
        self.region_name = region_name or "us-east-1"

    def compute_signature(
        self,
        headers: Dict[str, str],
        query_parameters: Optional[Dict[str, str]],
        body_hash: str,
        access_key: str,
        secret_key: str,
    ) -> str:
        now = datetime.now(timezone.utc)
        date_time_stamp = now.strftime(base.ISO8601_BASIC_FORMAT)
        date_stamp = now.strftime(base.DATE_STRING_FORMAT)

        # Java mutates the caller's header map in-place (x-amz-date, Host).
        headers["x-amz-date"] = date_time_stamp
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

        scope = f"{date_stamp}/{self.region_name}/{self.service_name}/{base.TERMINATOR}"
        string_to_sign = base.string_to_sign(
            base.SCHEME,
            base.ALGORITHM,
            date_time_stamp,
            scope,
            canonical_request_text,
        )
        signature = base._to_hex(
            base._sign(string_to_sign, base.signing_key(secret_key, date_stamp, self.region_name, self.service_name))
        )

        credentials = f"Credential={access_key}/{scope}"
        signed_headers = f"SignedHeaders={canonicalized_header_names}"
        signature_header = f"Signature={signature}"
        return f"{base.SCHEME}-{base.ALGORITHM} {credentials}, {signed_headers}, {signature_header}"
