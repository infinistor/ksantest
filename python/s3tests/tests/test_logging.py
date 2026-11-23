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
        bucket_name = self.create_bucket(client, 1)
        response = client.get_bucket_logging(Bucket=bucket_name)
        assert response.get("LoggingEnabled") is None

    @pytest.mark.tag("Put/Get")
    def test_logging_set(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 2)
        target_bucket_name = self.create_bucket(client, 2)
        client.put_bucket_logging(
            Bucket=source_bucket_name,
            BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": ""}},
        )

    @pytest.mark.tag("Put/Get")
    def test_logging_set_get(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 3)
        target_bucket_name = self.create_bucket(client, 3)
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
        source_bucket_name = self.create_bucket(client, 4)
        target_bucket_name = self.create_bucket(client, 4)
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
        source_bucket_name = self.create_bucket(client, 5)
        target_bucket_name = self.create_bucket(client, 5)
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
        source_bucket_name = self.create_bucket(client, 6)
        target_bucket_name = self.create_bucket(client, 6)
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
        source_bucket_name = self.get_new_bucket_name_only(7)
        target_bucket_name = self.get_new_bucket_name_only(7)
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
        source_bucket_name = self.create_bucket(client, 8)
        target_bucket_name = self.get_new_bucket_name_only(8)
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_logging(
                Bucket=source_bucket_name,
                BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target_bucket_name, "TargetPrefix": prefix}},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_TARGET_BUCKET_FOR_LOGGING

    @staticmethod
    def _put_logging(client, source_bucket_name: str, logging_enabled: dict | None) -> None:
        status = {} if logging_enabled is None else {"LoggingEnabled": logging_enabled}
        client.put_bucket_logging(Bucket=source_bucket_name, BucketLoggingStatus=status)

    @pytest.mark.tag("Put/Get")
    def test_logging_disable(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 9)
        target_bucket_name = self.create_bucket(client, 9)
        self._put_logging(client, source_bucket_name, {"TargetBucket": target_bucket_name, "TargetPrefix": "logs/"})
        assert client.get_bucket_logging(Bucket=source_bucket_name).get("LoggingEnabled") is not None

        # 빈 설정을 넣으면 로깅이 해제된다
        self._put_logging(client, source_bucket_name, None)
        assert client.get_bucket_logging(Bucket=source_bucket_name).get("LoggingEnabled") is None

    @pytest.mark.tag("Put/Get")
    def test_logging_overwrite(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 10)
        target_bucket_name = self.create_bucket(client, 10)
        new_target_bucket_name = self.create_bucket(client, 10)
        self._put_logging(client, source_bucket_name, {"TargetBucket": target_bucket_name, "TargetPrefix": "logs/"})
        self._put_logging(
            client, source_bucket_name, {"TargetBucket": new_target_bucket_name, "TargetPrefix": "other/"}
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        assert response["LoggingEnabled"]["TargetBucket"] == new_target_bucket_name
        assert response["LoggingEnabled"]["TargetPrefix"] == "other/"

    @pytest.mark.tag("KeyFormat")
    def test_logging_simple_prefix(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 11)
        target_bucket_name = self.create_bucket(client, 11)
        self._put_logging(
            client,
            source_bucket_name,
            {
                "TargetBucket": target_bucket_name,
                "TargetPrefix": "logs/",
                "TargetObjectKeyFormat": {"SimplePrefix": {}},
            },
        )
        response = client.get_bucket_logging(Bucket=source_bucket_name)
        key_format = response["LoggingEnabled"]["TargetObjectKeyFormat"]
        assert "SimplePrefix" in key_format
        assert "PartitionedPrefix" not in key_format

    @pytest.mark.tag("KeyFormat")
    def test_logging_partitioned_prefix(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 12)
        target_bucket_name = self.create_bucket(client, 12)
        for date_source in ("EventTime", "DeliveryTime"):
            self._put_logging(
                client,
                source_bucket_name,
                {
                    "TargetBucket": target_bucket_name,
                    "TargetPrefix": "logs/",
                    "TargetObjectKeyFormat": {"PartitionedPrefix": {"PartitionDateSource": date_source}},
                },
            )
            response = client.get_bucket_logging(Bucket=source_bucket_name)
            key_format = response["LoggingEnabled"]["TargetObjectKeyFormat"]
            assert key_format["PartitionedPrefix"]["PartitionDateSource"] == date_source
            assert "SimplePrefix" not in key_format

    @pytest.mark.tag("Put/Get")
    def test_logging_same_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        # 소스 버킷 자신을 타깃으로 지정할 수 있다
        self._put_logging(client, bucket_name, {"TargetBucket": bucket_name, "TargetPrefix": "self/"})
        response = client.get_bucket_logging(Bucket=bucket_name)
        assert response["LoggingEnabled"]["TargetBucket"] == bucket_name
        assert response["LoggingEnabled"]["TargetPrefix"] == "self/"

    @pytest.mark.tag("Error")
    def test_logging_target_bucket_other_owner(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        source_bucket_name = self.create_bucket(client, 14)
        target_bucket_name = self.create_bucket(alt_client, 14)
        # 소스와 타깃 버킷의 소유자가 다르면 거부된다
        self.assert_client_error(
            lambda: self._put_logging(
                client, source_bucket_name, {"TargetBucket": target_bucket_name, "TargetPrefix": "logs/"}
            ),
            400,
            md.INVALID_TARGET_BUCKET_FOR_LOGGING,
        )

    @pytest.mark.tag("Error")
    def test_logging_get_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(15)
        self.assert_client_error(lambda: client.get_bucket_logging(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("Permission")
    def test_logging_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        source_bucket_name = self.create_bucket(client, 16)
        target_bucket_name = self.create_bucket(client, 16)
        self.assert_client_error(
            lambda: alt_client.get_bucket_logging(Bucket=source_bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: self._put_logging(
                alt_client, source_bucket_name, {"TargetBucket": target_bucket_name, "TargetPrefix": "logs/"}
            ),
            403,
            md.ACCESS_DENIED,
        )
