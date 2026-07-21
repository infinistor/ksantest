"""AWS Signature Version 2 helpers ported from Java AWS2SignerBase."""

from __future__ import annotations

import base64
import hmac
import hashlib


def get_base64_encoded_sha1_hash(policy: str, secret_key: str) -> str:
    digest = hmac.new(secret_key.encode("utf-8"), policy.encode("utf-8"), hashlib.sha1).digest()
    return base64.b64encode(digest).decode("ascii")
