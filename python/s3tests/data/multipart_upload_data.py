"""Multipart upload state ported from Java MultipartUploadV2Data."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from s3tests.data import main_data as md
from s3tests.utils import checksum


class MultipartUploadV2Data:
    def __init__(self, part_size: int = 5 * md.MB) -> None:
        self.upload_id = ""
        self.parts: List[Dict[str, Any]] = []
        self.body = ""
        self.part_size = part_size

    def next_part_number(self) -> int:
        return len(self.parts) + 1

    def get_body(self) -> str:
        return self.body

    def add_part(self, etag: str, part_number: Optional[int] = None) -> None:
        if part_number is None:
            part_number = self.next_part_number()
        self.parts.append({"ETag": etag, "PartNumber": part_number})

    def add_part_with_checksum(self, algorithm: str, part_response: Dict[str, Any]) -> None:
        part_number = self.next_part_number()
        part: Dict[str, Any] = {"PartNumber": part_number, "ETag": part_response["ETag"]}
        value = checksum.get_checksum(part_response, algorithm)
        if value is not None:
            checksum.set_checksum(part, algorithm, value)
        self.parts.append(part)

    def add_part_from_copy(self, algorithm: str, part_response: Dict[str, Any]) -> None:
        copy_result = part_response["CopyPartResult"]
        part_number = self.next_part_number()
        part: Dict[str, Any] = {"PartNumber": part_number, "ETag": copy_result["ETag"]}
        value = checksum.get_checksum(copy_result, algorithm)
        if value is not None:
            checksum.set_checksum(part, algorithm, value)
        self.parts.append(part)

    def append_body(self, data: str) -> None:
        self.body += data

    def completed_multipart_upload(self) -> Dict[str, Any]:
        return {"Parts": self.parts}


MultipartUploadData = MultipartUploadV2Data
