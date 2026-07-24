"""Replication tests ported from Java testV2/Replication.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestReplication(S3TestBase):
    _REPLICATION_ROLE = "arn:aws:iam::635518764071:role/replication"

    def _replication_config(self, target_bucket_arn: str, prefix: str | None = None) -> dict:
        rule: dict = {
            "Status": "Enabled",
            "Destination": {"Bucket": target_bucket_arn},
        }
        if prefix is not None:
            rule["Priority"] = 1
            rule["Filter"] = {"Prefix": prefix}
            rule["DeleteMarkerReplication"] = {"Status": "Disabled"}
        return {"Role": self._REPLICATION_ROLE, "Rules": [rule]}

    @pytest.mark.tag("Check")
    def test_replication_set(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 1)
        target_bucket_name = self.create_bucket(client, 1)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        client.put_bucket_replication(
            Bucket=source_bucket_name, ReplicationConfiguration=config
        )
        get_config = client.get_bucket_replication(Bucket=source_bucket_name)
        self.replication_config_compare(config, get_config["ReplicationConfiguration"])

        client.delete_bucket_replication(Bucket=source_bucket_name)
        with pytest.raises(ClientError) as exc_info:
            client.get_bucket_replication(Bucket=source_bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_source_bucket_name(self):
        client = self.get_client()
        source_bucket_name = self.get_new_bucket_name_only(2)
        target_bucket_name = self.get_new_bucket_name_only(2)
        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_source_bucket_versioning(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 3)
        target_bucket_name = self.create_bucket(client, 3)
        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_target_bucket_name(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 4)
        target_bucket_name = self.get_new_bucket_name_only(4)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_target_bucket_versioning(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 5)
        target_bucket_name = self.create_bucket(client, 5)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_bucket_versioning_suspend(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 6)
        target_bucket_name = self.create_bucket(client, 6)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        client.put_bucket_replication(
            Bucket=source_bucket_name, ReplicationConfiguration=config
        )

        self.assert_client_error(
            lambda: client.put_bucket_versioning(
                Bucket=source_bucket_name,
                VersioningConfiguration={"Status": "Suspended"},
            ),
            409,
            md.INVALID_BUCKET_STATE,
        )
