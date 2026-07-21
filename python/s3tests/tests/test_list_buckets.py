"""ListBuckets tests ported from Java testV2/ListBuckets.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestListBuckets(S3TestBase):
    @pytest.mark.tag("Get")
    def test_buckets_create_then_list(self):
        client = self.get_client()
        bucket_names = []
        for _ in range(5):
            bucket_names.append(self.create_bucket(client))

        response = client.list_buckets()
        bucket_list = self.get_bucket_list(response)

        for bucket_name in bucket_names:
            assert bucket_name in bucket_list, (
                f"S3 implementation's GET on Service did not return bucket we created: {bucket_name}"
            )

    @pytest.mark.tag("ERROR")
    def test_list_buckets_invalid_auth(self):
        bad_auth_client = self.get_bad_auth_client(None, None)
        with pytest.raises(ClientError) as exc_info:
            bad_auth_client.list_buckets()
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_ACCESS_KEY_ID

    @pytest.mark.tag("ERROR")
    def test_list_buckets_bad_auth(self):
        main_access_key = self.config.main_user.access_key
        bad_auth_client = self.get_bad_auth_client(main_access_key, None)
        with pytest.raises(ClientError) as exc_info:
            bad_auth_client.list_buckets()
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.SIGNATURE_DOES_NOT_MATCH

    @pytest.mark.tag("Metadata")
    def test_head_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        response = client.head_bucket(Bucket=bucket_name)
        assert response is not None
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.tag("Prefix")
    def test_list_buckets_prefix(self):
        client = self.get_client()
        prefix = "1111-my-test"
        bucket_name = utils.get_new_bucket_name(prefix)
        client.create_bucket(Bucket=bucket_name)

        for _ in range(5):
            self.create_bucket(client)

        response = client.list_buckets(Prefix=prefix)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 1
        assert bucket_list[0] == bucket_name
        client.delete_bucket(Bucket=bucket_name)
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("MaxBuckets")
    def test_list_buckets_max_buckets(self):
        client = self.get_client()
        for _ in range(5):
            self.create_bucket(client)

        full_response = client.list_buckets(Prefix=self.get_prefix())
        full_bucket_list = self.get_bucket_list(full_response)
        full_bucket_list.sort()

        response = client.list_buckets(Prefix=self.get_prefix(), MaxBuckets=2)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 2
        assert bucket_list == full_bucket_list[:2]

    @pytest.mark.tag("ContinuationToken")
    def test_list_buckets_continuation_token(self):
        client = self.get_client()
        for _ in range(5):
            self.create_bucket(client)

        full_response = client.list_buckets(Prefix=self.get_prefix())
        full_bucket_list = self.get_bucket_list(full_response)
        full_bucket_list.sort()

        response = client.list_buckets(Prefix=self.get_prefix(), MaxBuckets=2)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 2
        assert bucket_list == full_bucket_list[:2]

        response2 = client.list_buckets(
            Prefix=self.get_prefix(),
            MaxBuckets=2,
            ContinuationToken=response["ContinuationToken"],
        )
        bucket_list2 = self.get_bucket_list(response2)
        assert len(bucket_list2) == 2
        assert bucket_list2 == full_bucket_list[2:4]
