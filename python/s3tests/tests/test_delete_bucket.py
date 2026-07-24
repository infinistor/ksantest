"""DeleteBucket tests ported from Java testV2/DeleteBucket.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestDeleteBucket(S3TestBase):
    @pytest.mark.tag("ERROR")
    def test_bucket_delete_not_exist(self):
        bucket_name = self.get_new_bucket_name_only(1)
        client = self.get_client()
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET

    @pytest.mark.tag("ERROR")
    def test_bucket_delete_nonempty(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 2, "foo")
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_NOT_EMPTY

    @pytest.mark.tag("ERROR")
    def test_bucket_create_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        client.delete_bucket(Bucket=bucket_name)
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET
        self.delete_bucket_list(bucket_name)
