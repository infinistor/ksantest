"""CopyObject tests ported from Java testV2/CopyObject.java."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import EncryptionType, SSE_CUSTOMER_ALGORITHM, SSE_KEY, SSE_KEY_MD5, S3TestBase
from s3tests.utils import checksum, utils

_COPY_SIZES = (1024, 256 * 1024, 1024 * 1024)
_PAST_DATE = datetime(1994, 9, 29, 19, 43, 31, tzinfo=timezone.utc)
_FUTURE_DATE = datetime(2100, 9, 29, 19, 43, 31, tzinfo=timezone.utc)
_CHECKSUM_CONFIGS = (
    ("when_required", "when_required"),
    ("when_required", "when_supported"),
    ("when_supported", "when_required"),
    ("when_supported", "when_supported"),
)


class TestCopyObject(S3TestBase):
    def _run_object_copy_matrix(
        self,
        prefix: str,
        source_object_encryption: bool,
        source_bucket_encryption: bool,
        target_bucket_encryption: bool,
        target_object_encryption: bool,
    ) -> None:
        for size in _COPY_SIZES:
            self.object_copy(
                prefix,
                source_object_encryption,
                source_bucket_encryption,
                target_bucket_encryption,
                target_object_encryption,
                size,
            )

    def _run_object_copy_encryption_type_matrix(
        self, prefix: str, source: EncryptionType, target: EncryptionType
    ) -> None:
        for size in _COPY_SIZES:
            self.object_copy_encryption_type(prefix, source, target, size)

    @staticmethod
    def _checksum_compare_copy(algorithm: str, content: str, response: dict) -> None:
        result = response.get("CopyObjectResult", {})
        expected = checksum.calculate_checksum(algorithm, content)
        actual = checksum.get_checksum(result, algorithm)
        assert actual == expected, f"{algorithm} copy checksum mismatch: {actual} != {expected}"

    @pytest.mark.tag("Check")
    def test_object_copy_zero_size(self):
        source = "testObjectCopyZeroSizeSource"
        target = "testObjectCopyZeroSizeTarget"
        client = self.get_client()
        bucket_name = self.create_objects(client, source)

        client.put_object(Bucket=bucket_name, Key=source, Body=b"")

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert response["ContentLength"] == 0

    @pytest.mark.tag("Check")
    def test_object_copy_same_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectCopySameBucketSource"
        target = "testObjectCopySameBucketTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("ContentType")
    def test_object_copy_verify_content_type(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectCopyVerifyContentTypeSource"
        target = "testObjectCopyVerifyContentTypeTarget"
        content_type = "text/bla"

        client.put_object(
            Bucket=bucket_name,
            Key=source,
            Body=source.encode("utf-8"),
            ContentType=content_type,
        )
        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source
        assert response["ContentType"] == content_type

    @pytest.mark.tag("Overwrite")
    def test_object_copy_to_itself(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectCopyToItself"

        client.put_object(Bucket=bucket_name, Key=source, Body=b"")

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=source,
                CopySource={"Bucket": bucket_name, "Key": source},
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Overwrite")
    def test_object_copy_to_itself_with_metadata(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectCopyToItselfWithMetadata"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        metadata = {"foo": "bar"}
        client.copy_object(
            Bucket=bucket_name,
            Key=source,
            CopySource={"Bucket": bucket_name, "Key": source},
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )

        response = client.get_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

    @pytest.mark.tag("Check")
    def test_object_copy_diff_bucket(self):
        client = self.get_client()
        source_bucket = self.create_bucket(client)
        target_bucket = self.create_bucket(client)
        source = "testObjectCopyDiffBucketSource"
        target = "testObjectCopyDiffBucketTarget"

        client.put_object(Bucket=source_bucket, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=target_bucket,
            Key=target,
            CopySource={"Bucket": source_bucket, "Key": source},
        )

        response = client.get_object(Bucket=target_bucket, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("Check")
    def test_object_copy_not_owned_bucket(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        source_bucket = self.create_bucket(client)
        target_bucket = self.create_bucket(alt_client)
        source = "testObjectCopyNotOwnedBucketSource"
        target = "testObjectCopyNotOwnedBucketTarget"

        client.put_object(Bucket=source_bucket, Key=source, Body=source.encode("utf-8"))

        self.assert_client_error(
            lambda: alt_client.copy_object(
                Bucket=target_bucket,
                Key=target,
                CopySource={"Bucket": source_bucket, "Key": source},
            ),
            403,
            md.ACCESS_DENIED,
        )
        alt_client.delete_bucket(Bucket=target_bucket)
        self.delete_bucket_list(target_bucket)

    @pytest.mark.tag("Check")
    def test_object_copy_not_owned_object_bucket(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client)
        source = "testObjectCopyNotOwnedObjectBucketSource"
        target = "testObjectCopyNotOwnedObjectBucketTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        acl = self.create_alt_acl("FULL_CONTROL")
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)
        client.put_object_acl(Bucket=bucket_name, Key=source, AccessControlPolicy=acl)

        alt_client.get_object(Bucket=bucket_name, Key=source)

        alt_client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
        )

    @pytest.mark.tag("Overwrite")
    def test_object_copy_canned_acl(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client)
        source = "testObjectCopyCannedAclSource"
        target = "testObjectCopyCannedAclTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            ACL="public-read",
        )
        alt_client.get_object(Bucket=bucket_name, Key=target)

        metadata = {"abc": "def"}
        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            ACL="public-read",
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )
        response = alt_client.get_object(Bucket=bucket_name, Key=target)

        assert self.get_body(response) == source
        assert response["Metadata"] == metadata

    @pytest.mark.tag("Check")
    def test_object_copy_retaining_metadata(self):
        client = self.get_client()
        for size in (3, 1024 * 1024):
            bucket_name = self.create_bucket(client)
            content_type = "audio/ogg"
            source = "testObjectCopyRetainingMetadataSource"
            target = "testObjectCopyRetainingMetadataTarget"
            metadata = {"source": "value1", "target": "value2"}

            client.put_object(
                Bucket=bucket_name,
                Key=source,
                Body=utils.random_text_to_long(size).encode("utf-8"),
                Metadata=metadata,
                ContentType=content_type,
            )
            client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
            )

            response = client.get_object(Bucket=bucket_name, Key=target)
            assert response["ContentType"] == content_type
            assert response["Metadata"] == metadata
            assert response["ContentLength"] == size

    @pytest.mark.tag("Check")
    def test_object_copy_replacing_metadata(self):
        client = self.get_client()
        for size in (3, 1024 * 1024):
            bucket_name = self.create_bucket(client)
            content_type = "audio/ogg"
            source = "testObjectCopyReplacingMetadataSource"
            target = "testObjectCopyReplacingMetadataTarget"
            metadata = {"source": "value1", "target": "value2"}

            client.put_object(
                Bucket=bucket_name,
                Key=source,
                Body=utils.random_text_to_long(size).encode("utf-8"),
                Metadata=metadata,
                ContentType=content_type,
            )

            response = client.get_object(Bucket=bucket_name, Key=source)
            assert response["ContentType"] == content_type
            assert response["Metadata"] == metadata

            metadata2 = {"key3": "value3", "key4": "value4"}
            client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                Metadata=metadata2,
                ContentType=content_type,
                MetadataDirective="REPLACE",
            )

            response = client.get_object(Bucket=bucket_name, Key=target)
            assert response["ContentType"] == content_type
            assert response["Metadata"] == metadata2
            assert response["ContentLength"] == size

    @pytest.mark.tag("ERROR")
    def test_object_copy_bucket_not_found(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key="testObjectCopyBucketNotFoundTarget",
                CopySource={
                    "Bucket": f"{bucket_name}-fake",
                    "Key": "testObjectCopyBucketNotFoundSource",
                },
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("ERROR")
    def test_object_copy_key_not_found(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key="testObjectCopyKeyNotFoundTarget",
                CopySource={"Bucket": bucket_name, "Key": "testObjectCopyKeyNotFoundSource"},
            ),
            404,
            md.NO_SUCH_KEY,
        )

    @pytest.mark.tag("Version")
    def test_object_copy_versioning_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        size = 5
        data = utils.random_text_to_long(size)
        source = "testObjectCopyVersionedBucketSource"
        target = "testObjectCopyVersionedBucketTarget"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source, Body=data.encode("utf-8"))
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )
        target_vid = copy_response["VersionId"]

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == data
        assert response["ContentLength"] == size

        target2 = "testObjectCopyVersionedBucketTarget2"
        client.copy_object(
            Bucket=bucket_name,
            Key=target2,
            CopySource={"Bucket": bucket_name, "Key": target, "VersionId": target_vid},
        )
        response = client.get_object(Bucket=bucket_name, Key=target2)
        assert self.get_body(response) == data
        assert response["ContentLength"] == size

        target_bucket = self.create_bucket(client)
        self.check_configure_versioning_retry(target_bucket, "Enabled")
        target3 = "testObjectCopyVersionedBucketTarget3"

        client.copy_object(
            Bucket=target_bucket,
            Key=target3,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )
        response = client.get_object(Bucket=target_bucket, Key=target3)
        assert self.get_body(response) == data
        assert response["ContentLength"] == size

        bucket_name3 = self.create_bucket(client)
        self.check_configure_versioning_retry(bucket_name3, "Enabled")
        target4 = "testObjectCopyVersionedBucketTarget4"
        client.copy_object(
            Bucket=bucket_name3,
            Key=target4,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )
        response = client.get_object(Bucket=bucket_name3, Key=target4)
        assert self.get_body(response) == data
        assert response["ContentLength"] == size

        target5 = "testObjectCopyVersionedBucketTarget5"
        client.copy_object(
            Bucket=bucket_name,
            Key=target5,
            CopySource={"Bucket": bucket_name, "Key": source},
        )
        response = client.get_object(Bucket=bucket_name, Key=target5)
        assert self.get_body(response) == data
        assert response["ContentLength"] == size

    @pytest.mark.tag("Version")
    def test_object_copy_versioning_url_encoding(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        source = "testObjectCopyVersionedUrlEncoding?Source"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=source)
        version_id = response["VersionId"]
        assert self.get_body(response) == source

        target = "testObjectCopyVersionedUrlEncoding&Target"
        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": version_id},
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("Multipart")
    def test_object_copy_versioning_multipart_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        size = 50 * md.MB
        source = "testObjectCopyVersioningMultipartUploadSource"
        source_metadata = {"foo": "bar"}

        uploads = self.setup_multipart_upload(client, bucket_name, source, size, metadata=source_metadata)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source,
            UploadId=uploads.upload_id,
            MultipartUpload=uploads.completed_multipart_upload(),
        )

        head_response = client.head_object(Bucket=bucket_name, Key=source)
        source_size = head_response["ContentLength"]
        source_vid = head_response["VersionId"]

        target = "testObjectCopyVersioningMultipartUploadTarget"
        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )

        head_response = client.head_object(Bucket=bucket_name, Key=target)
        target_vid = head_response["VersionId"]
        assert head_response["ContentLength"] == source_size
        assert head_response["Metadata"] == source_metadata
        self.check_content_using_range(bucket_name, target, uploads.get_body(), md.MB)

        target2 = "testObjectCopyVersioningMultipartUploadTarget2"
        client.copy_object(
            Bucket=bucket_name,
            Key=target2,
            CopySource={"Bucket": bucket_name, "Key": target, "VersionId": target_vid},
        )
        head_response = client.head_object(Bucket=bucket_name, Key=target2)
        assert head_response["ContentLength"] == source_size
        assert head_response["Metadata"] == source_metadata
        self.check_content_using_range(bucket_name, target2, uploads.get_body(), md.MB)

        target_bucket = self.create_bucket(client)
        self.check_configure_versioning_retry(target_bucket, "Enabled")
        target3 = "testObjectCopyVersioningMultipartUploadTarget3"
        client.copy_object(
            Bucket=target_bucket,
            Key=target3,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )
        head_response = client.head_object(Bucket=target_bucket, Key=target3)
        assert head_response["ContentLength"] == source_size
        assert head_response["Metadata"] == source_metadata
        self.check_content_using_range(target_bucket, target3, uploads.get_body(), md.MB)

        bucket_name3 = self.create_bucket(client)
        self.check_configure_versioning_retry(bucket_name3, "Enabled")
        target4 = "testObjectCopyVersioningMultipartUploadTarget4"
        client.copy_object(
            Bucket=bucket_name3,
            Key=target4,
            CopySource={"Bucket": bucket_name, "Key": source, "VersionId": source_vid},
        )
        head_response = client.head_object(Bucket=bucket_name3, Key=target4)
        assert head_response["ContentLength"] == source_size
        assert head_response["Metadata"] == source_metadata
        self.check_content_using_range(bucket_name3, target4, uploads.get_body(), md.MB)

        target5 = "testObjectCopyVersioningMultipartUploadTarget5"
        client.copy_object(
            Bucket=bucket_name,
            Key=target5,
            CopySource={"Bucket": bucket_name3, "Key": target4},
        )
        head_response = client.head_object(Bucket=bucket_name, Key=target5)
        assert head_response["ContentLength"] == source_size
        assert head_response["Metadata"] == source_metadata
        self.check_content_using_range(bucket_name, target5, uploads.get_body(), md.MB)

    @pytest.mark.tag("If Match")
    def test_copy_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfMatchGoodSource"
        target = "testCopyObjectIfMatchGoodTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfMatch=etag,
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_copy_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfMatchFailedSource"
        target = "testCopyObjectIfMatchFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfMatch="ABC",
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("If Match")
    def test_copy_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfNoneMatchGoodSource"
        target = "testCopyObjectIfNoneMatchGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfNoneMatch="ABC",
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_copy_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfNoneMatchFailedSource"
        target = "testCopyObjectIfNoneMatchFailedTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfNoneMatch=etag,
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("If Match")
    def test_copy_object_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfModifiedSinceGoodSource"
        target = "testCopyObjectIfModifiedSinceGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfModifiedSince=_PAST_DATE,
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_copy_object_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfModifiedSinceFailedSource"
        target = "testCopyObjectIfModifiedSinceFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        last_modified = client.head_object(Bucket=bucket_name, Key=source)["LastModified"]
        after = last_modified + timedelta(seconds=1)
        self.delay(1000)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfModifiedSince=after,
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("If Match")
    def test_copy_object_if_unmodified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfUnmodifiedSinceGoodSource"
        target = "testCopyObjectIfUnmodifiedSinceGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfUnmodifiedSince=_FUTURE_DATE,
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_copy_object_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfUnmodifiedSinceFailedSource"
        target = "testCopyObjectIfUnmodifiedSinceFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfUnmodifiedSince=_PAST_DATE,
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("If Match")
    def test_copy_object_if_match_with_if_unmodified_since(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfMatchWithIfUnmodifiedSinceSource"
        target = "testCopyObjectIfMatchWithIfUnmodifiedSinceTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfMatch=etag,
            CopySourceIfUnmodifiedSince=_PAST_DATE,
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("If Match")
    def test_copy_object_if_none_match_with_if_modified_since(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfNoneMatchWithIfModifiedSinceSource"
        target = "testCopyObjectIfNoneMatchWithIfModifiedSinceTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfNoneMatch=etag,
                CopySourceIfModifiedSince=_PAST_DATE,
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfMatchAndIfNoneMatchSource"
        target = "testCopyObjectIfMatchAndIfNoneMatchTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfMatch=etag,
                CopySourceIfNoneMatch=etag,
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectIfMatchAndIfNoneMatchAnySource"
        target = "testCopyObjectIfMatchAndIfNoneMatchAnyTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                CopySourceIfMatch=etag,
                CopySourceIfNoneMatch="*",
            ),
            412,
            md.PRECONDITION_FAILED,
        )

    @pytest.mark.tag("IfMatch")
    def test_copy_object_destination_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfMatchGoodSource"
        target = "testCopyObjectDestinationIfMatchGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        target_etag = client.put_object(Bucket=bucket_name, Key=target, Body=b"old")["ETag"]

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            IfMatch=target_etag,
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("IfMatch")
    def test_copy_object_destination_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfMatchFailedSource"
        target = "testCopyObjectDestinationIfMatchFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        client.put_object(Bucket=bucket_name, Key=target, Body=b"old")

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            ),
            412,
            md.PRECONDITION_FAILED,
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_destination_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfNoneMatchGoodSource"
        target = "testCopyObjectDestinationIfNoneMatchGoodTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            IfNoneMatch="*",
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_destination_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfNoneMatchFailedSource"
        target = "testCopyObjectDestinationIfNoneMatchFailedTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        client.put_object(Bucket=bucket_name, Key=target, Body=b"old")

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                IfNoneMatch="*",
            ),
            412,
            md.PRECONDITION_FAILED,
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_destination_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfMatchAndIfNoneMatchSource"
        target = "testCopyObjectDestinationIfMatchAndIfNoneMatchTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        target_etag = client.put_object(Bucket=bucket_name, Key=target, Body=b"old")["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                IfMatch=target_etag,
                IfNoneMatch=target_etag,
            ),
            501,
            md.NOT_IMPLEMENTED,
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_destination_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnySource"
        target = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnyTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        target_etag = client.put_object(Bucket=bucket_name, Key=target, Body=b"old")["ETag"]

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
                IfMatch=target_etag,
                IfNoneMatch="*",
            ),
            501,
            md.NOT_IMPLEMENTED,
        )

        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_copy_object_source_if_match_with_destination_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchSource"
        target = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchTarget"

        etag = client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))["ETag"]

        client.copy_object(
            Bucket=bucket_name,
            Key=target,
            CopySource={"Bucket": bucket_name, "Key": source},
            CopySourceIfMatch=etag,
            IfNoneMatch="*",
        )
        response = client.get_object(Bucket=bucket_name, Key=target)
        assert self.get_body(response) == source

    @pytest.mark.tag("encryption")
    def test_copy_nor_src_to_nor_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyNorSrcToNorBucketAndObj", False, False, False, False)

    @pytest.mark.tag("encryption")
    def test_copy_nor_src_to_nor_bucket_encryption_obj(self):
        self._run_object_copy_matrix("testCopyNorSrcToNorBucketEncryptionObj", False, False, False, True)

    @pytest.mark.tag("encryption")
    def test_copy_nor_src_to_encryption_bucket_nor_obj(self):
        self._run_object_copy_matrix("testCopyNorSrcToEncryptionBucketNorObj", False, False, True, False)

    @pytest.mark.tag("encryption")
    def test_copy_nor_src_to_encryption_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyNorSrcToEncryptionBucketAndObj", False, False, True, True)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_src_to_nor_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionSrcToNorBucketAndObj", True, False, False, False)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_src_to_nor_bucket_encryption_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionSrcToNorBucketEncryptionObj", True, False, False, True)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_src_to_encryption_bucket_nor_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionSrcToEncryptionBucketNorObj", True, False, True, False)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_src_to_encryption_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionSrcToEncryptionBucketAndObj", True, False, True, True)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionBucketNorObjToNorBucketAndObj", False, True, False, False)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketNorObjToNorBucketEncryptionObj", False, True, False, True
        )

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketNorObjToEncryptionBucketNorObj", False, True, True, False
        )

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketNorObjToEncryptionBucketAndObj", False, True, True, True
        )

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj(self):
        self._run_object_copy_matrix("testCopyEncryptionBucketAndObjToNorBucketAndObj", True, True, False, False)

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketAndObjToNorBucketEncryptionObj", True, True, False, True
        )

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketAndObjToEncryptionBucketNorObj", True, True, True, False
        )

    @pytest.mark.tag("encryption")
    def test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj(self):
        self._run_object_copy_matrix(
            "testCopyEncryptionBucketAndObjToEncryptionBucketAndObj", True, True, True, True
        )

    @pytest.mark.tag("encryption")
    def test_copy_to_normal_source(self):
        prefix = "testCopyToNormalSource"
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.NORMAL, EncryptionType.NORMAL)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.NORMAL, EncryptionType.SSE_S3)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.NORMAL, EncryptionType.SSE_C)

    @pytest.mark.tag("encryption")
    def test_copy_to_sse_s3_source(self):
        prefix = "testCopyToSseS3Source"
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_S3, EncryptionType.NORMAL)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_S3, EncryptionType.SSE_S3)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_S3, EncryptionType.SSE_C)

    @pytest.mark.tag("encryption")
    def test_copy_to_sse_c_source(self):
        prefix = "testCopyToSseCSource"
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_C, EncryptionType.NORMAL)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_C, EncryptionType.SSE_S3)
        self._run_object_copy_encryption_type_matrix(prefix, EncryptionType.SSE_C, EncryptionType.SSE_C)

    @pytest.mark.tag("ERROR")
    def test_copy_to_deleted_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyToDeletedObjectSource"
        target = "testCopyToDeletedObjectTarget"

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=source)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
            ),
            404,
            md.NO_SUCH_KEY,
        )

    @pytest.mark.tag("ERROR")
    def test_copy_to_delete_marker_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testCopyToDeleteMarkerObjectSource"
        target = "testCopyToDeleteMarkerObjectTarget"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=source)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=bucket_name,
                Key=target,
                CopySource={"Bucket": bucket_name, "Key": source},
            ),
            404,
            md.NO_SUCH_KEY,
        )

    @pytest.mark.tag("Overwrite")
    def test_object_versioning_copy_to_itself_with_metadata(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectVersioningCopyToItselfWithMetadataSource"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=source, Body=source.encode("utf-8"))

        metadata = {"foo": "bar"}
        client.copy_object(
            Bucket=bucket_name,
            Key=source,
            CopySource={"Bucket": bucket_name, "Key": source},
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )
        response = client.get_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

        version_response = client.list_object_versions(Bucket=bucket_name)
        assert len(version_response["Versions"]) == 2

    @pytest.mark.tag("Overwrite")
    def test_object_copy_to_itself_with_metadata_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectCopyToItselfWithMetadataOverwriteSource"
        metadata = {"foo": "bar"}

        client.put_object(
            Bucket=bucket_name,
            Key=source,
            Body=source.encode("utf-8"),
            Metadata=metadata,
        )
        response = client.head_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

        metadata["foo"] = "bar2"
        client.copy_object(
            Bucket=bucket_name,
            Key=source,
            CopySource={"Bucket": bucket_name, "Key": source},
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )
        response = client.head_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

    @pytest.mark.tag("Overwrite")
    def test_object_versioning_copy_to_itself_with_metadata_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source = "testObjectVersioningCopyToItselfWithMetadataOverwriteSource"
        metadata = {"foo": "bar"}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(
            Bucket=bucket_name,
            Key=source,
            Body=source.encode("utf-8"),
            Metadata=metadata,
        )
        response = client.head_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

        metadata["foo"] = "bar2"
        client.copy_object(
            Bucket=bucket_name,
            Key=source,
            CopySource={"Bucket": bucket_name, "Key": source},
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )
        response = client.head_object(Bucket=bucket_name, Key=source)
        assert response["Metadata"] == metadata

        version_response = client.list_object_versions(Bucket=bucket_name)
        assert len(version_response["Versions"]) == 2

    @pytest.mark.tag("ERROR")
    def test_copy_revoke_sse_algorithm(self):
        client = self.get_client_https(True)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        source_key = "testCopyRevokeSseAlgorithmSource"
        target_key = "testCopyRevokeSseAlgorithmTarget"
        data = utils.random_text_to_long(1024)

        client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        with pytest.raises(ClientError) as exc_info:
            client.copy_object(
                Bucket=bucket_name,
                Key=target_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                CopySourceSSECustomerKey=SSE_KEY,
                CopySourceSSECustomerKeyMD5=SSE_KEY_MD5,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    @pytest.mark.tag("checksum")
    def test_copy_object_checksum_use_chunk_encoding(self):
        bucket_name = self.create_bucket()

        for request_option, response_option in _CHECKSUM_CONFIGS:
            client = self.get_client_with_checksum(True, request_option, response_option)
            async_client = self.get_async_client(True, request_option, response_option)

            for checksum_algorithm in checksum.ALL_ALGORITHMS:
                prefix = f"req_{request_option}/resp_{response_option}"

                source_key = f"{prefix}/source/sync/{checksum_algorithm}"
                target_key = f"{prefix}/target/sync/{checksum_algorithm}"

                put_params = checksum.apply_put_checksum_params(
                    {
                        "Bucket": bucket_name,
                        "Key": source_key,
                        "Body": source_key.encode("utf-8"),
                    },
                    checksum_algorithm,
                    source_key,
                )
                put_response = client.put_object(**put_params)
                checksum.checksum_compare(checksum_algorithm, source_key, put_response)

                copy_response = client.copy_object(
                    Bucket=bucket_name,
                    Key=target_key,
                    CopySource={"Bucket": bucket_name, "Key": source_key},
                )
                self._checksum_compare_copy(checksum_algorithm, source_key, copy_response)

                async_source_key = f"{prefix}/source/async/{checksum_algorithm}"
                async_target_key = f"{prefix}/target/async/{checksum_algorithm}"

                async_put_params = checksum.apply_put_checksum_params(
                    {
                        "Bucket": bucket_name,
                        "Key": async_source_key,
                        "Body": async_source_key.encode("utf-8"),
                    },
                    checksum_algorithm,
                    async_source_key,
                )
                async_put_response = async_client.put_object(**async_put_params).join()
                checksum.checksum_compare(checksum_algorithm, async_source_key, async_put_response)

                async_copy_response = async_client.copy_object(
                    Bucket=bucket_name,
                    Key=async_target_key,
                    CopySource={"Bucket": bucket_name, "Key": async_source_key},
                ).join()
                self._checksum_compare_copy(checksum_algorithm, async_source_key, async_copy_response)

    @pytest.mark.tag("metadata")
    def test_copy_object_metadata_and_tags(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source_key = "testCopyObjectMetadataAndTagsSource"
        target_key = "testCopyObjectMetadataAndTagsTarget"

        metadata = {"foo": "bar"}
        tags = [{"Key": "tag1", "Value": "value1"}]

        client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=source_key.encode("utf-8"),
            Metadata=metadata,
            Tagging="tag1=value1",
        )

        response = client.get_object(Bucket=bucket_name, Key=source_key)
        assert response["Metadata"] == metadata

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=source_key)
        assert tag_response["TagSet"] == tags

        client.copy_object(
            Bucket=bucket_name,
            Key=target_key,
            CopySource={"Bucket": bucket_name, "Key": source_key},
        )

        response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert response["Metadata"] == metadata

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=target_key)
        assert tag_response["TagSet"] == tags
