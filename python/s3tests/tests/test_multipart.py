"""Multipart tests ported from Java testV2/Multipart.java."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.data.multipart_upload_data import MultipartUploadData
from s3tests.test_base import AsyncS3Client, S3TestBase
from s3tests.utils import checksum, utils

DEFAULT_PART_SIZE = 5 * md.MB
PAST_DATE = datetime(1994, 9, 29, 19, 43, 31, tzinfo=timezone.utc)
FUTURE_DATE = datetime(2100, 9, 29, 19, 43, 31, tzinfo=timezone.utc)

_CHECKSUM_TEST_CONFIGS: List[Tuple[str, str, str, List[str]]] = [
    ("when_required", "when_required", "FULL_OBJECT", checksum.FULL_OBJECT_ALGORITHMS),
    ("when_required", "when_supported", "FULL_OBJECT", checksum.FULL_OBJECT_ALGORITHMS),
    ("when_supported", "when_required", "FULL_OBJECT", checksum.FULL_OBJECT_ALGORITHMS),
    ("when_supported", "when_supported", "FULL_OBJECT", checksum.FULL_OBJECT_ALGORITHMS),
    ("when_required", "when_required", "COMPOSITE", checksum.COMPOSITE_ALGORITHMS),
    ("when_required", "when_supported", "COMPOSITE", checksum.COMPOSITE_ALGORITHMS),
    ("when_supported", "when_required", "COMPOSITE", checksum.COMPOSITE_ALGORITHMS),
    ("when_supported", "when_supported", "COMPOSITE", checksum.COMPOSITE_ALGORITHMS),
]

_UNSUPPORTED_FULL_OBJECT_CHECKSUMS = [
    "SHA1",
    "SHA256",
    "MD5",
    "SHA512",
    "XXHASH64",
    "XXHASH3",
    "XXHASH128",
]

_UNSUPPORTED_COMPOSITE_CHECKSUMS = ["CRC64NVME"]


class TestMultipart(S3TestBase):
    def _client_call(self, client: Any, operation: str, **kwargs: Any) -> Dict[str, Any]:
        if isinstance(client, AsyncS3Client):
            if operation == "upload_part_copy":
                return client._executor.submit(client._client.upload_part_copy, **kwargs).result()
            async_method = getattr(client, operation, None)
            if async_method is not None:
                return async_method(**kwargs).join()
            return getattr(client._client, operation)(**kwargs)
        return getattr(client, operation)(**kwargs)

    def _multipart_upload_checksum_async(
        self,
        async_client: AsyncS3Client,
        bucket_name: str,
        key: str,
        checksum_type: str,
        checksum_algorithm: str,
    ) -> None:
        size = 10 * md.MB
        part_size = 5 * md.MB
        upload_data = MultipartUploadData()
        create_response = async_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            ChecksumType=checksum_type,
            ChecksumAlgorithm=checksum_algorithm,
        ).join()
        upload_data.upload_id = create_response["UploadId"]
        for part in utils.generate_random_string(size, part_size):
            upload_data.append_body(part)
            part_params = checksum.apply_put_checksum_params(
                {
                    "Bucket": bucket_name,
                    "Key": key,
                    "UploadId": upload_data.upload_id,
                    "PartNumber": upload_data.next_part_number(),
                    "Body": part.encode("utf-8"),
                },
                checksum_algorithm,
                part,
            )
            part_response = async_client.upload_part(**part_params).join()
            checksum.checksum_compare_part(checksum_algorithm, part, part_response)
            upload_data.add_part_with_checksum(checksum_algorithm, part_response)
        complete_response = async_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            ChecksumType=checksum_type,
            MultipartUpload={"Parts": upload_data.parts},
        ).join()
        assert complete_response.get("ChecksumType") == checksum_type
        checksum.checksum_compare_multipart(checksum_algorithm, upload_data, complete_response)

    def _multipart_copy_checksum(
        self,
        client: Any,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
        checksum_algorithm: str,
    ) -> None:
        size = 10 * md.MB
        part_size = 5 * md.MB
        upload_data = MultipartUploadData()
        source_head = self._client_call(
            client,
            "head_object",
            Bucket=source_bucket,
            Key=source_key,
            ChecksumMode="ENABLED",
        )
        source_checksum = checksum.get_checksum(source_head, checksum_algorithm)
        create_response = self._client_call(
            client,
            "create_multipart_upload",
            Bucket=target_bucket,
            Key=target_key,
            ChecksumAlgorithm=checksum_algorithm,
        )
        upload_data.upload_id = create_response["UploadId"]
        index = 0
        while index < size:
            start = index
            end = min(index + part_size, size) - 1
            part_number = upload_data.next_part_number()
            part_response = self._client_call(
                client,
                "upload_part_copy",
                CopySource={"Bucket": source_bucket, "Key": source_key},
                Bucket=target_bucket,
                Key=target_key,
                UploadId=upload_data.upload_id,
                PartNumber=part_number,
                CopySourceRange=f"bytes={start}-{end}",
            )
            upload_data.add_part_from_copy(checksum_algorithm, part_response)
            index = end + 1
        complete_response = self._client_call(
            client,
            "complete_multipart_upload",
            Bucket=target_bucket,
            Key=target_key,
            UploadId=upload_data.upload_id,
            MultipartUpload={"Parts": upload_data.parts},
        )
        checksum.checksum_compare_multipart(
            checksum_algorithm,
            upload_data,
            complete_response,
            source_checksum=source_checksum,
        )

    def _upload_part_copy_with_request_headers(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        upload_id: str,
        part_number: int,
        copy_source: Dict[str, str],
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        def _inject_headers(request, **kwargs):
            for header_name, header_value in headers.items():
                request.headers[header_name] = header_value

        client.meta.events.register("before-sign.s3.UploadPartCopy", _inject_headers)
        try:
            return client.upload_part_copy(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=copy_source,
            )
        finally:
            client.meta.events.unregister("before-sign.s3.UploadPartCopy", _inject_headers)

    def _assert_precondition_failed(self, func) -> None:
        with pytest.raises(ClientError) as exc_info:
            func()
        error = exc_info.value.response.get("Error", {})
        status = exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
        assert status in (412, 200), f"statusCode: {status}"
        assert error.get("Code") == md.PRECONDITION_FAILED

    @pytest.mark.tag("ERROR")
    def test_multipart_upload_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        key = "testMultipartUploadEmpty"
        size = 0

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
            ),
            400,
            md.MALFORMED_XML,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)

    @pytest.mark.tag("Check")
    def test_multipart_upload_small(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        key = "testMultipartUploadSmall"
        size = 1

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ContentLength"] == size

    @pytest.mark.tag("Copy")
    def test_multipart_copy_small(self):
        source_key = "foo"
        target_key = "testMultipartCopySmall"
        size = 1

        client = self.get_client()
        source_bucket_name = self.create_key_with_random_content(client, source_key, 0, test_id=3)
        target_bucket_name = self.create_bucket(client, 3)

        upload_data = self.multipart_copy(client, source_bucket_name, source_key, target_bucket_name, target_key, size)
        client.complete_multipart_upload(
            Bucket=target_bucket_name,
            Key=target_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        response = client.head_object(Bucket=target_bucket_name, Key=target_key)
        assert response["ContentLength"] == size

    @pytest.mark.tag("ERROR")
    def test_multipart_copy_invalid_range(self):
        client = self.get_client()
        source_key = "source"
        bucket_name = self.create_key_with_random_content(client, source_key, 5, test_id=4)

        target_key = "testMultipartCopyInvalidRange"
        response = client.create_multipart_upload(Bucket=bucket_name, Key=target_key)
        upload_id = response["UploadId"]

        with pytest.raises(ClientError) as exc_info:
            client.upload_part_copy(
                CopySource={"Bucket": bucket_name, "Key": source_key},
                Bucket=bucket_name,
                Key=target_key,
                UploadId=upload_id,
                PartNumber=1,
                CopySourceRange="bytes=0-21",
            )
        status = exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
        assert status in (400, 416)
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_ARGUMENT

        client.abort_multipart_upload(Bucket=bucket_name, Key=target_key, UploadId=upload_id)

    @pytest.mark.tag("Range")
    def test_multipart_copy_without_range(self):
        client = self.get_client()
        source_key = "source"
        source_bucket_name = self.create_key_with_random_content(client, source_key, 10, test_id=5)
        target_bucket_name = self.create_bucket(client, 5)
        target_key = "testMultipartCopyWithoutRange"

        init_response = client.create_multipart_upload(Bucket=target_bucket_name, Key=target_key)
        upload_id = init_response["UploadId"]
        parts: List[Dict[str, Any]] = []

        copy_response = client.upload_part_copy(
            CopySource={"Bucket": source_bucket_name, "Key": source_key},
            Bucket=target_bucket_name,
            Key=target_key,
            UploadId=upload_id,
            PartNumber=1,
            CopySourceRange="bytes=0-9",
        )
        parts.append({"PartNumber": 1, "ETag": copy_response["CopyPartResult"]["ETag"]})
        client.complete_multipart_upload(
            Bucket=target_bucket_name,
            Key=target_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        response = client.head_object(Bucket=target_bucket_name, Key=target_key)
        assert response["ContentLength"] == 10

    @pytest.mark.tag("SpecialNames")
    def test_multipart_copy_special_names(self):
        source_keys = [" ", "_", "__", "?versionId"]
        target_key = "testMultipartCopySpecialNames"
        size = 10 * md.MB
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 6)
        target_bucket_name = self.create_bucket(client, 6)

        for source_key in source_keys:
            self.create_key_with_random_content(client, source_key, size, source_bucket_name)
            upload_data = self.multipart_copy(client, source_bucket_name, source_key, target_bucket_name, target_key, size)
            client.complete_multipart_upload(
                Bucket=target_bucket_name,
                Key=target_key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
            )
            response = client.head_object(Bucket=target_bucket_name, Key=target_key)
            assert response["ContentLength"] == size
            self.check_copy_content_using_range(source_bucket_name, source_key, target_bucket_name, target_key, md.MB)

    @pytest.mark.tag("Put")
    def test_multipart_upload(self):
        key = "testMultipartUpload"
        size = 50 * md.MB
        metadata = {"foo": "bar"}
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size, metadata=metadata)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        get_response = client.head_object(Bucket=bucket_name, Key=key)
        assert get_response["Metadata"] == metadata

        body = upload_data.get_body()
        self.check_content_using_range(bucket_name, key, body, md.MB)
        self.check_content_using_range(bucket_name, key, body, 10 * md.MB)
        self.check_content_using_random_range(bucket_name, key, body, 100)

    @pytest.mark.tag("Copy")
    def test_multipart_copy_versioned(self):
        target_key = "testMultipartCopyVersioned"
        source_key = "foo"
        size = 15 * md.MB
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 8)
        target_bucket_name = self.create_bucket(client, 8)

        self.check_configure_versioning_retry(source_bucket_name, "Enabled")

        self.create_key_with_random_content(client, source_key, size, source_bucket_name)
        self.create_key_with_random_content(client, source_key, size, source_bucket_name)
        self.create_key_with_random_content(client, source_key, size, source_bucket_name)

        list_response = client.list_object_versions(Bucket=source_bucket_name)
        version_ids = [version["VersionId"] for version in list_response.get("Versions", [])]

        for version_id in version_ids:
            upload_data = self.multipart_copy(
                client,
                source_bucket_name,
                source_key,
                target_bucket_name,
                target_key,
                size,
                version_id=version_id,
            )
            client.complete_multipart_upload(
                Bucket=target_bucket_name,
                Key=target_key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
            )
            response = client.head_object(Bucket=target_bucket_name, Key=target_key)
            assert response["ContentLength"] == size
            self.check_copy_content(source_bucket_name, source_key, target_bucket_name, target_key, version_id)

    @pytest.mark.tag("Duplicate")
    def test_multipart_upload_resend_part(self):
        key = "testMultipartUploadResendPart"
        size = 50 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)

        self.check_upload_multipart_resend(bucket_name, key, size, [0])
        self.check_upload_multipart_resend(bucket_name, key, size, [1])
        self.check_upload_multipart_resend(bucket_name, key, size, [2])
        self.check_upload_multipart_resend(bucket_name, key, size, [1, 2])
        self.check_upload_multipart_resend(bucket_name, key, size, [0, 1, 2, 3, 4, 5])

    @pytest.mark.tag("Put")
    def test_multipart_upload_multiple_sizes(self):
        key = "testMultipartUploadMultipleSizes"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)

        size_list = [
            5 * md.MB,
            5 * md.MB + 100 * md.KB,
            5 * md.MB + 600 * md.KB,
            10 * md.MB,
            10 * md.MB + 100 * md.KB,
            10 * md.MB + 600 * md.KB,
        ]

        for size in size_list:
            upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
            client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
            )

    @pytest.mark.tag("Copy")
    def test_multipart_copy_multiple_sizes(self):
        source_key = "source"
        target_key = "testMultipartCopyMultipleSizes"
        client = self.get_client()
        source_bucket_name = self.create_key_with_random_content(client, source_key, 12 * md.MB, test_id=11)
        target_bucket_name = self.create_bucket(client, 11)

        size_list = [
            5 * md.MB,
            5 * md.MB + 100 * md.KB,
            5 * md.MB + 600 * md.KB,
            10 * md.MB,
            10 * md.MB + 100 * md.KB,
            10 * md.MB + 600 * md.KB,
        ]

        for size in size_list:
            upload_data = self.multipart_copy(client, source_bucket_name, source_key, target_bucket_name, target_key, size)
            client.complete_multipart_upload(
                Bucket=target_bucket_name,
                Key=target_key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
            )
            self.check_copy_content_using_range(source_bucket_name, source_key, target_bucket_name, target_key)

    @pytest.mark.tag("ERROR")
    def test_multipart_upload_size_too_small(self):
        key = "testMultipartUploadSizeTooSmall"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        content = utils.random_text_to_long(10 * md.KB)

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]
        parts: List[Dict[str, Any]] = []

        for i in range(10):
            part_number = i + 1
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=content.encode("utf-8"),
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})

        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            ),
            400,
            md.ENTITY_TOO_SMALL,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    @pytest.mark.tag("Check")
    def test_multipart_upload_contents(self):
        bucket_name = self.create_bucket(13)
        self.do_test_multipart_upload_contents(bucket_name, "testMultipartUploadContents", 3)

    @pytest.mark.tag("OverWrite")
    def test_multipart_upload_overwrite_existing_object(self):
        key = "testMultipartUploadOverwriteExistingObject"
        part_count = 2
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        content = utils.random_text_to_long(5 * md.MB)

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]
        parts: List[Dict[str, Any]] = []
        total_content = ""

        for i in range(part_count):
            part_number = i + 1
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=content.encode("utf-8"),
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})
            total_content += content

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert body == total_content, md.NOT_MATCHED

    @pytest.mark.tag("OverWrite")
    def test_put_object_overwrite_multipart_upload(self):
        key = "testPutObjectOverwriteMultipartUpload"
        multipart_size = 10 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 48)
        content = utils.random_text_to_long(1 * md.MB)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, multipart_size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["ContentLength"] == len(content)

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert len(body) == len(content)
        assert body == content, md.NOT_MATCHED

        self.check_content_using_range(bucket_name, key, content, md.KB)

    @pytest.mark.tag("Cancel")
    def test_abort_multipart_upload(self):
        key = "testAbortMultipartUpload"
        size = 10 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)

        head_response = client.list_objects_v2(Bucket=bucket_name)
        assert head_response["KeyCount"] == 0
        assert self.get_bytes_used(head_response) == 0

    @pytest.mark.tag("ERROR")
    def test_abort_multipart_upload_not_found(self):
        key = "testAbortMultipartUploadNotFound"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"")

        self.assert_client_error(
            lambda: client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId="nonexistent"),
            404,
            md.NO_SUCH_UPLOAD,
        )

    @pytest.mark.tag("List")
    def test_list_multipart_upload(self):
        key = "testListMultipartUpload"
        key2 = "testListMultipartUpload2"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)

        upload_data1 = self.setup_multipart_upload(client, bucket_name, key, 5 * md.MB)
        upload_data2 = self.setup_multipart_upload(client, bucket_name, key, 6 * md.MB)
        upload_data3 = self.setup_multipart_upload(client, bucket_name, key2, 5 * md.MB)

        upload_ids = [upload_data1.upload_id, upload_data2.upload_id, upload_data3.upload_id]

        response = client.list_multipart_uploads(Bucket=bucket_name)
        get_upload_ids = [upload["UploadId"] for upload in response.get("Uploads", [])]

        for upload_id in upload_ids:
            assert upload_id in get_upload_ids

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_ids[0])
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_ids[1])
        client.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload_ids[2])

    @pytest.mark.tag("ERROR")
    def test_multipart_upload_missing_part(self):
        key = "testMultipartUploadMissingPart"
        body = "test"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]

        parts: List[Dict[str, Any]] = []
        part_response = client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=body.encode("utf-8"),
        )
        parts.append({"PartNumber": 9999, "ETag": part_response["ETag"]})

        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            ),
            400,
            md.INVALID_PART,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    @pytest.mark.tag("ERROR")
    def test_multipart_upload_incorrect_etag(self):
        key = "testMultipartUploadIncorrectEtag"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]

        parts: List[Dict[str, Any]] = []
        client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=b"test",
        )
        parts.append({"PartNumber": 1, "ETag": "ffffffffffffffffffffffffffffffff"})

        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            ),
            400,
            md.INVALID_PART,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    @pytest.mark.tag("Overwrite")
    def test_atomic_multipart_upload_write(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        key = "testAtomicMultipartUploadWrite"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert body == "bar"

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert body == "bar"

    @pytest.mark.tag("List")
    def test_multipart_upload_list(self):
        key = "testMultipartUploadList"
        size = 50 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)

        response = client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)
        self.parts_etag_compare(upload_data.parts, response["Parts"])

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)

    @pytest.mark.tag("Cancel")
    def test_abort_multipart_upload_list(self):
        key = "testAbortMultipartUploadList"
        size = 10 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)

        list_response = client.list_multipart_uploads(Bucket=bucket_name)
        assert len(list_response.get("Uploads", [])) == 0

    @pytest.mark.tag("Copy")
    def test_multipart_copy_many(self):
        source_key = "testMultipartCopyMany"
        size = 10 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        body = ""

        upload_data = self.setup_multipart_upload(client, bucket_name, source_key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        body += upload_data.body
        self.check_content_using_range(bucket_name, source_key, body, md.MB)

        target_key1 = "testMultipartCopyMany1"
        upload_data2 = self.multipart_copy(client, bucket_name, source_key, bucket_name, target_key1, size)
        copy_data1 = self.multipart_upload(client, bucket_name, target_key1, size, DEFAULT_PART_SIZE, upload_data2)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key1,
            UploadId=copy_data1.upload_id,
            MultipartUpload=copy_data1.completed_multipart_upload(),
        )
        body += copy_data1.body
        self.check_content_using_range(bucket_name, target_key1, body, md.MB)

        target_key2 = "testMultipartCopyMany2"
        upload_data3 = self.multipart_copy(client, bucket_name, target_key1, bucket_name, target_key2, size * 2)
        copy_data2 = self.multipart_upload(client, bucket_name, target_key2, size, DEFAULT_PART_SIZE, upload_data3)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key2,
            UploadId=copy_data2.upload_id,
            MultipartUpload=copy_data2.completed_multipart_upload(),
        )
        body += copy_data2.body
        self.check_content_using_range(bucket_name, target_key2, body, md.MB)

    @pytest.mark.tag("List")
    def test_multipart_list_parts(self):
        key = "testMultipartListParts"
        size = 50 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size, part_size=1 * md.MB)

        index = 0
        while True:
            part_number = index
            response = client.list_parts(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MaxParts=10,
                PartNumberMarker=part_number,
            )
            assert len(response["Parts"]) == 10
            self.parts_etag_compare(upload_data.parts[index : index + 10], response["Parts"])
            if response.get("IsTruncated"):
                index = response["NextPartNumberMarker"]
            else:
                break

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_data.upload_id)

    @pytest.mark.tag("checksum")
    def test_multipart_upload_checksum_use_chunk_encoding(self):
        bucket_name = self.create_bucket(25)

        for request_option, response_option, checksum_type, algorithms in _CHECKSUM_TEST_CONFIGS:
            client = self.get_client_with_checksum(True, request_option, response_option)
            async_client = self.get_async_client(True, request_option, response_option)

            for checksum_algorithm in algorithms:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{checksum_type.lower()}/{checksum_algorithm}"
                async_key = f"{prefix}/async/{checksum_type.lower()}/{checksum_algorithm}"

                self.multipart_upload_checksum(client, bucket_name, key, checksum_type, checksum_algorithm)
                self._multipart_upload_checksum_async(async_client, bucket_name, async_key, checksum_type, checksum_algorithm)

    @pytest.mark.tag("checksum")
    def test_multipart_upload_checksum(self):
        bucket_name = self.create_bucket(26)

        for request_option, response_option, checksum_type, algorithms in _CHECKSUM_TEST_CONFIGS:
            client = self.get_client_with_checksum(False, request_option, response_option)
            async_client = self.get_async_client(False, request_option, response_option)

            for checksum_algorithm in algorithms:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{checksum_type.lower()}/{checksum_algorithm}"
                async_key = f"{prefix}/async/{checksum_type.lower()}/{checksum_algorithm}"

                self.multipart_upload_checksum(client, bucket_name, key, checksum_type, checksum_algorithm)
                self._multipart_upload_checksum_async(async_client, bucket_name, async_key, checksum_type, checksum_algorithm)

    @pytest.mark.tag("checksum-failure")
    def test_multipart_upload_checksum_failure(self):
        bucket_name = self.create_bucket(27)

        failure_configs = [
            ("when_required", "when_required", "FULL_OBJECT", _UNSUPPORTED_FULL_OBJECT_CHECKSUMS),
            ("when_required", "when_supported", "FULL_OBJECT", _UNSUPPORTED_FULL_OBJECT_CHECKSUMS),
            ("when_supported", "when_required", "FULL_OBJECT", _UNSUPPORTED_FULL_OBJECT_CHECKSUMS),
            ("when_supported", "when_supported", "FULL_OBJECT", _UNSUPPORTED_FULL_OBJECT_CHECKSUMS),
            ("when_required", "when_required", "COMPOSITE", _UNSUPPORTED_COMPOSITE_CHECKSUMS),
            ("when_required", "when_supported", "COMPOSITE", _UNSUPPORTED_COMPOSITE_CHECKSUMS),
            ("when_supported", "when_required", "COMPOSITE", _UNSUPPORTED_COMPOSITE_CHECKSUMS),
            ("when_supported", "when_supported", "COMPOSITE", _UNSUPPORTED_COMPOSITE_CHECKSUMS),
        ]

        for request_option, response_option, checksum_type, algorithms in failure_configs:
            client = self.get_client_with_checksum(True, request_option, response_option)
            async_client = self.get_async_client(True, request_option, response_option)

            for checksum_algorithm in algorithms:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{checksum_type.lower()}/{checksum_algorithm}"
                async_key = f"{prefix}/async/{checksum_type.lower()}/{checksum_algorithm}"

                self.assert_client_error(
                    lambda c=client, k=key, ct=checksum_type, ca=checksum_algorithm: self.multipart_upload_checksum(c, bucket_name, k, ct, ca),
                    400,
                    md.INVALID_REQUEST,
                )

                with pytest.raises(ClientError) as exc_info:
                    self._multipart_upload_checksum_async(async_client, bucket_name, async_key, checksum_type, checksum_algorithm)
                assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
                assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("checksum")
    def test_multipart_copy_checksum(self):
        bucket_name = self.create_bucket(28)

        for request_option, response_option, checksum_type, algorithms in _CHECKSUM_TEST_CONFIGS:
            client = self.get_client_with_checksum(True, request_option, response_option)
            async_client = self.get_async_client(True, request_option, response_option)

            for checksum_algorithm in algorithms:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{checksum_type.lower()}/{checksum_algorithm}"
                async_key = f"{prefix}/async/{checksum_type.lower()}/{checksum_algorithm}"

                self.multipart_upload_checksum(client, bucket_name, key, checksum_type, checksum_algorithm)
                self._multipart_copy_checksum(client, bucket_name, key, bucket_name, key, checksum_algorithm)
                self._multipart_upload_checksum_async(async_client, bucket_name, async_key, checksum_type, checksum_algorithm)
                self._multipart_copy_checksum(async_client, bucket_name, async_key, bucket_name, async_key, checksum_algorithm)

    @pytest.mark.tag("checksum")
    def test_create_multipart_upload_empty_checksum_algorithm(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        key = "testcreateMultipartUploadEmptyChecksumAlgorithm"
        checksum_type = "FULL_OBJECT"

        self.assert_client_error(
            lambda: client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                ChecksumType=checksum_type,
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("checksum")
    def test_create_multipart_upload_empty_checksum_type(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        key = "testcreateMultipartUploadEmptyChecksumType"
        size = 10 * md.MB
        part_size = 5 * md.MB
        checksum_type = "COMPOSITE"
        checksum_algorithm = "CRC32"
        upload_data = MultipartUploadData()

        create_response = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            ChecksumAlgorithm=checksum_algorithm,
        )
        upload_data.upload_id = create_response["UploadId"]

        parts = utils.generate_random_string(size, part_size)
        for part in parts:
            upload_data.append_body(part)
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                PartNumber=upload_data.next_part_number(),
                Body=part.encode("utf-8"),
                ChecksumAlgorithm=checksum_algorithm,
            )
            checksum.checksum_compare_part(checksum_algorithm, part, part_response)
            upload_data.add_part_with_checksum(checksum_algorithm, part_response)

        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            ChecksumType=checksum_type,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        checksum.checksum_compare_multipart(checksum_algorithm, upload_data, complete_response)

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        source = "testUploadPartCopyIfMatchGoodSource"
        target = "testUploadPartCopyIfMatchGoodTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        part_response = client.upload_part_copy(
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfMatch=etag,
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            PartNumber=1,
        )

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_response["CopyPartResult"]["ETag"]}]},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 32)
        source = "testUploadPartCopyIfMatchFailedSource"
        target = "testUploadPartCopyIfMatchFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: client.upload_part_copy(
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfMatch="ABC",
                Bucket=bucket_name,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
            ),
            412,
            md.PRECONDITION_FAILED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        source = "testUploadPartCopyIfNoneMatchGoodSource"
        target = "testUploadPartCopyIfNoneMatchGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        part_response = client.upload_part_copy(
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfNoneMatch="ABC",
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            PartNumber=1,
        )

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_response["CopyPartResult"]["ETag"]}]},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 34)
        source = "testUploadPartCopyIfNoneMatchFailedSource"
        target = "testUploadPartCopyIfNoneMatchFailedTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: client.upload_part_copy(
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfNoneMatch=etag,
                Bucket=bucket_name,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
            ),
            412,
            md.PRECONDITION_FAILED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_upload_part_copy_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 35)
        source = "testUploadPartCopyIfMatchAndIfNoneMatchSource"
        target = "testUploadPartCopyIfMatchAndIfNoneMatchTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: self._upload_part_copy_with_request_headers(
                client,
                bucket_name,
                target,
                upload_id,
                1,
                {"Bucket": bucket_name, "Key": source},
                {"If-Match": "ABC", "If-None-Match": "DEF"},
            ),
            501,
            md.NOT_IMPLEMENTED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_upload_part_copy_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 36)
        source = "testUploadPartCopyIfMatchAndIfNoneMatchAnySource"
        target = "testUploadPartCopyIfMatchAndIfNoneMatchAnyTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: self._upload_part_copy_with_request_headers(
                client,
                bucket_name,
                target,
                upload_id,
                1,
                {"Bucket": bucket_name, "Key": source},
                {"If-Match": "ABC", "If-None-Match": "*"},
            ),
            501,
            md.NOT_IMPLEMENTED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 37)
        source = "testUploadPartCopyIfModifiedSinceGoodSource"
        target = "testUploadPartCopyIfModifiedSinceGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        part_response = client.upload_part_copy(
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfModifiedSince=PAST_DATE,
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            PartNumber=1,
        )

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_response["CopyPartResult"]["ETag"]}]},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 38)
        source = "testUploadPartCopyIfModifiedSinceFailedSource"
        target = "testUploadPartCopyIfModifiedSinceFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        last_modified = client.head_object(Bucket=bucket_name, Key=source)["LastModified"]
        after = last_modified + timedelta(seconds=1)

        self.delay(1000)

        self.assert_client_error(
            lambda: client.upload_part_copy(
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfModifiedSince=after,
                Bucket=bucket_name,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
            ),
            412,
            md.PRECONDITION_FAILED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_unmodified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 39)
        source = "testUploadPartCopyIfUnmodifiedSinceGoodSource"
        target = "testUploadPartCopyIfUnmodifiedSinceGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        part_response = client.upload_part_copy(
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfUnmodifiedSince=FUTURE_DATE,
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            PartNumber=1,
        )

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_response["CopyPartResult"]["ETag"]}]},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_upload_part_copy_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 40)
        source = "testUploadPartCopyIfUnmodifiedSinceFailedSource"
        target = "testUploadPartCopyIfUnmodifiedSinceFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: client.upload_part_copy(
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfUnmodifiedSince=PAST_DATE,
                Bucket=bucket_name,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
            ),
            412,
            md.PRECONDITION_FAILED,
        )
        client.abort_multipart_upload(Bucket=bucket_name, Key=target, UploadId=upload_id)

    @pytest.mark.tag("IfMatch")
    def test_complete_multipart_upload_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 41)
        key = "testCompleteMultipartUploadIfMatchGood"
        size = 5 * md.MB

        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"old")["ETag"]

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
            IfMatch=etag,
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == upload_data.get_body()

    @pytest.mark.tag("IfMatch")
    def test_complete_multipart_upload_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 42)
        key = "testCompleteMultipartUploadIfMatchFailed"
        size = 5 * md.MB

        client.put_object(Bucket=bucket_name, Key=key, Body=b"old")

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        self._assert_precondition_failed(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
                IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            )
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfNoneMatch")
    def test_complete_multipart_upload_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 43)
        key = "testCompleteMultipartUploadIfNoneMatchGood"
        size = 5 * md.MB

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
            IfNoneMatch="*",
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == upload_data.get_body()

    @pytest.mark.tag("IfNoneMatch")
    def test_complete_multipart_upload_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 44)
        key = "testCompleteMultipartUploadIfNoneMatchFailed"
        size = 5 * md.MB

        client.put_object(Bucket=bucket_name, Key=key, Body=b"old")

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        self._assert_precondition_failed(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
                IfNoneMatch="*",
            )
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_complete_multipart_upload_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 45)
        key = "testCompleteMultipartUploadIfMatchAndIfNoneMatch"
        size = 5 * md.MB

        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"old")["ETag"]

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
                IfMatch=etag,
                IfNoneMatch=etag,
            ),
            501,
            md.NOT_IMPLEMENTED,
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_complete_multipart_upload_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 46)
        key = "testCompleteMultipartUploadIfMatchAndIfNoneMatchAny"
        size = 5 * md.MB

        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"old")["ETag"]

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        self.assert_client_error(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                MultipartUpload=upload_data.completed_multipart_upload(),
                IfMatch=etag,
                IfNoneMatch="*",
            ),
            501,
            md.NOT_IMPLEMENTED,
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("Cancel")
    def test_multipart_upload_abort_during_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 47)
        key = "testMultipartUploadAbortDuringUpload"
        part_body = utils.random_text_to_long(5 * md.MB)

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

        self.assert_client_error(
            lambda: client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part_body.encode("utf-8"),
            ),
            404,
            md.NO_SUCH_UPLOAD,
        )
