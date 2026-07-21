"""Logging tests ported from Java testV2/Logging.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestLogging(S3TestBase):
    @pytest.mark.tag("Put/Get")
    def test_logging_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        response = client.get_bucket_logging(Bucket=bucket_name)
        assert response.get("LoggingEnabled") is None

    @pytest.mark.tag("Put/Get")
    def test_logging_set(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": ""}},
        )

    @pytest.mark.tag("Put/Get")
    def test_logging_set_get(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": ""}},
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        assert response["LoggingEnabled"]["TargetPrefix"] == ""
        assert response["LoggingEnabled"]["TargetBucket"] == target_bucket_name

    @pytest.mark.tag("Put/Get")
    def test_logging_prefix(self):
        prefix = "logs/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        assert response["LoggingEnabled"]["TargetPrefix"] == prefix
        assert response["LoggingEnabled"]["TargetBucket"] == target_bucket_name

    @pytest.mark.tag("Versioning")
    def test_logging_versioning(self):
        prefix = "logs/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        assert response["LoggingEnabled"]["TargetPrefix"] == prefix
        assert response["LoggingEnabled"]["TargetBucket"] == target_bucket_name

    @pytest.mark.tag("Encryption")
    def test_logging_encryption(self):
        prefix = "logs/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        client.put_bucket_encryption(
            Bucket=source_bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
        )
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        assert response["LoggingEnabled"]["TargetPrefix"] == prefix
        assert response["LoggingEnabled"]["TargetBucket"] == target_bucket_name

    @pytest.mark.tag("Error")
    def test_logging_bucket_not_found(self):
        source_bucket_name = self.get_new_bucket_name_only()
        target_bucket_name = self.get_new_bucket_name_only()
        prefix = "logs/"
        client = self.get_client()
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_logging(
                Bucket=source_bucket_name,
                BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET

    @pytest.mark.tag("Error")
    def test_logging_target_bucket_not_found(self):
        prefix = "logs/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.get_new_bucket_name_only()
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_logging(
                Bucket=source_bucket_name,
                BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_TARGET_BUCKET_FOR_LOGGING
