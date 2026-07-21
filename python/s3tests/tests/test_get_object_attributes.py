"""GetObjectAttributes tests ported from Java testV2/GetObjectAttributes.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import checksum, utils


class TestGetObjectAttributes(S3TestBase):
    @pytest.mark.tag("Basic")
    def test_get_object_attributes_basic(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesBasic"

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "StorageClass", "ETag"],
        )
        assert response is not None
        assert response["ObjectSize"] == len(key)
        assert response["StorageClass"] == "STANDARD"
        assert response.get("ETag") is not None

    @pytest.mark.tag("SpecificAttributes")
    def test_get_object_attributes_specific_attributes(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesSpecificAttributes"

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        size_response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize"],
        )
        assert size_response is not None
        assert size_response["ObjectSize"] == len(key)
        assert size_response.get("Checksum") is None

        etag_response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ETag"],
        )
        assert etag_response is not None
        assert etag_response.get("ETag") is not None
        assert etag_response.get("ObjectSize") is None

    @pytest.mark.tag("Multipart")
    def test_get_object_attributes_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesMultipart"
        size = 10 * md.MB

        upload_data = self.setup_multipart_upload(client, bucket_name, key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "StorageClass", "ETag", "ObjectParts"],
        )
        assert response is not None
        assert response["ObjectSize"] == size
        assert response["StorageClass"] == "STANDARD"
        assert response.get("ETag") is not None
        assert response.get("ObjectParts") is not None
        assert response["ObjectParts"]["TotalPartsCount"] > 0
        assert response["ObjectParts"]["TotalPartsCount"] == len(upload_data.parts)

    @pytest.mark.tag("Checksum")
    def test_get_object_attributes_with_checksum(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesWithChecksum"

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ChecksumAlgorithm="SHA256",
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["Checksum"],
        )
        assert response is not None
        assert response.get("Checksum") is not None

    @pytest.mark.tag("ERROR")
    def test_get_object_attributes_non_existent_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesNonExistentObject"

        self.assert_client_error(
            lambda: client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ObjectSize"],
            ),
            404,
            md.NO_SUCH_KEY,
        )

    @pytest.mark.tag("ERROR")
    def test_get_object_attributes_non_existent_bucket(self):
        client = self.get_client()
        bucket_name = f"non-existent-bucket-{utils.random_text(10).lower()}"
        key = "testGetObjectAttributesNonExistentBucket"

        self.assert_client_error(
            lambda: client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ObjectSize"],
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("ERROR")
    def test_get_object_attributes_no_attributes(self):
        from botocore.exceptions import ClientError, ParamValidationError

        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesNoAttributes"

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        # Java reaches the service (400 InvalidRequest); botocore validates ObjectAttributes first.
        with pytest.raises((ClientError, ParamValidationError)) as exc_info:
            client.get_object_attributes(Bucket=bucket_name, Key=key)
        if isinstance(exc_info.value, ClientError):
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST


    @pytest.mark.tag("Versioning")
    def test_get_object_attributes_with_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesWithVersionId"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        content1 = f"{key}-v1"
        client.put_object(Bucket=bucket_name, Key=key, Body=content1.encode("utf-8"))
        content2 = f"{key}-v2"
        client.put_object(Bucket=bucket_name, Key=key, Body=content2.encode("utf-8"))

        list_response = client.list_object_versions(Bucket=bucket_name, Prefix=key)
        versions = list_response.get("Versions", [])
        assert len(versions) == 2

        first_version_id = versions[1]["VersionId"]
        second_version_id = versions[0]["VersionId"]

        first_version_response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            VersionId=first_version_id,
            ObjectAttributes=["ObjectSize"],
        )
        second_version_response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            VersionId=second_version_id,
            ObjectAttributes=["ObjectSize"],
        )
        assert first_version_response["ObjectSize"] == len(content1)
        assert second_version_response["ObjectSize"] == len(content2)

    @pytest.mark.tag("ERROR")
    def test_get_object_attributes_invalid_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesInvalidVersionId"

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        self.assert_client_error(
            lambda: client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                VersionId="f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59",
                ObjectAttributes=["ObjectSize"],
            ),
            404,
            md.NO_SUCH_VERSION,
        )

    @pytest.mark.tag("LargeMultipart")
    def test_get_object_attributes_large_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesLargeMultipart"
        size = 100 * md.MB
        part_size = 5 * md.MB

        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]
        parts = []
        part_count = size // part_size
        for part_number in range(1, part_count + 1):
            part_content = utils.random_text_to_long(part_size)
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_content.encode("utf-8"),
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "ObjectParts"],
        )
        assert response is not None
        assert response["ObjectSize"] == size
        assert response.get("ObjectParts") is not None
        assert response["ObjectParts"]["TotalPartsCount"] == part_count

    @pytest.mark.tag("Metadata")
    def test_get_object_attributes_with_metadata(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesWithMetadata"
        metadata = {
            "custom-key1": "custom-value1",
            "custom-key2": "custom-value2",
        }

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            Metadata=metadata,
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "ETag"],
        )
        assert response is not None
        assert response["ObjectSize"] == len(key)

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["Metadata"] == metadata

    @pytest.mark.tag("Encryption")
    def test_get_object_attributes_with_sse_s3(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesWithSSES3"

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ServerSideEncryption="AES256",
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "ETag"],
        )
        assert response is not None
        assert response["ObjectSize"] == len(key)

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response.get("ServerSideEncryption") == "AES256"

    @pytest.mark.tag("Async")
    def test_get_object_attributes_async(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesAsync"

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ObjectSize", "ETag"],
        )
        assert response is not None
        assert response["ObjectSize"] == len(key)
        assert response.get("ETag") is not None

    @pytest.mark.tag("ERROR")
    def test_get_object_attributes_async_error(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only()
        key = "testGetObjectAttributesAsyncError"

        self.assert_client_error(
            lambda: client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ObjectSize"],
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("AllAttributes")
    def test_get_object_attributes_all_attributes(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAttributesAllAttributes"
        size = 10 * md.MB
        checksum_type = "FULL_OBJECT"
        checksum_algorithm = "CRC64NVME"

        init_response = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            ChecksumType=checksum_type,
            ChecksumAlgorithm=checksum_algorithm,
        )
        upload_id = init_response["UploadId"]
        part_content = utils.random_text_to_long(size)
        part_params = checksum.apply_put_checksum_params(
            {
                "Bucket": bucket_name,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": 1,
                "Body": part_content.encode("utf-8"),
            },
            checksum_algorithm,
            part_content,
        )
        part_response = client.upload_part(**part_params)
        parts = [{"PartNumber": 1, "ETag": part_response["ETag"]}]

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            ChecksumType=checksum_type,
            MultipartUpload={"Parts": parts},
        )

        response = client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=[
                "ObjectSize",
                "StorageClass",
                "ETag",
                "ObjectParts",
                "Checksum",
            ],
        )
        assert response is not None
        assert response["ObjectSize"] == size
        assert response["StorageClass"] == "STANDARD"
        assert response.get("ETag") is not None
        assert response.get("ObjectParts") is not None
        assert response["ObjectParts"]["TotalPartsCount"] == 1
        assert response.get("Checksum") is not None
