"""Utility helpers ported from Java Utils."""

from __future__ import annotations

import base64
import hashlib
import random
import string
import time
from datetime import datetime, timedelta, timezone
from typing import List

TEXT = string.ascii_lowercase + string.digits
TEXT_LONG = string.ascii_letters + string.digits
BUCKET_MAX_LENGTH = 63

_rand = random.Random(int(time.time() * 1000))


def random_text(length: int) -> str:
    return "".join(_rand.choice(TEXT) for _ in range(length))


def random_text_to_long(length: int) -> str:
    return "".join(_rand.choice(TEXT_LONG) for _ in range(length))


def generate_random_string(size: int, part_size: int) -> List[str]:
    result: List[str] = []
    remain = size
    while remain > 0:
        now_part = part_size if remain > part_size else remain
        result.append(random_text_to_long(now_part))
        remain -= now_part
    return result


def random_object_name(length: int) -> str:
    max_length = 200
    name = []
    index = 0
    while sum(len(part) for part in name) <= length:
        if index + max_length < length:
            name.append(random_text_to_long(max_length))
            index += max_length
        else:
            name.append(random_text_to_long(length - index))
            index = length
        name.append("/")
    combined = "".join(name)
    return combined[:length]


def get_md5(value: str) -> str:
    digest = hashlib.md5(value.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def get_utc_time_string(expire_seconds: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(seconds=expire_seconds)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def get_new_bucket_name(prefix: str) -> str:
    bucket_name = prefix + random_text(BUCKET_MAX_LENGTH)
    return bucket_name[: BUCKET_MAX_LENGTH - 1]


def make_arn_resource(path: str) -> str:
    return f"arn:aws:s3:::{path}"
