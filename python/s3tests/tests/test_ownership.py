"""Ownership tests ported from Java testV2/Ownership.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestOwnership(S3TestBase):
    @pytest.mark.tag("Get")
    def test_get_bucket_ownership(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1, object_ownership="BucketOwnerEnforced")
        client.get_bucket_ownership_controls(Bucket=bucket_name)

    @pytest.mark.tag("Put")
    def test_create_bucket_with_ownership(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2, object_ownership="BucketOwnerEnforced")
        response = client.get_bucket_ownership_controls(Bucket=bucket_name)
        assert response["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced"

    @pytest.mark.tag("Put")
    def test_change_bucket_ownership(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3, object_ownership="BucketOwnerEnforced")
        response = client.get_bucket_ownership_controls(Bucket=bucket_name)
        assert response["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced"
        client.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
        )
        response = client.get_bucket_ownership_controls(Bucket=bucket_name)
        assert response["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerPreferred"

    @pytest.mark.tag("Error")
    def test_bucket_ownership_deny_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4, object_ownership="BucketOwnerEnforced")
        response = client.get_bucket_ownership_controls(Bucket=bucket_name)
        assert response["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced"
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED

    @pytest.mark.tag("Error")
    def test_bucket_ownership_deny_object_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5, object_ownership="BucketOwnerEnforced")
        key = "testBucketOwnershipDenyObjectACL"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED

    @pytest.mark.tag("Check")
    def test_object_ownership_deny_change(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 6)
        key = "testObjectOwnershipDenyChange"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL="public-read")
        public_client = self.get_public_client()
        public_client.head_object(Bucket=bucket_name, Key=key)
        client.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        public_client.head_object(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("Error")
    def test_object_ownership_deny_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 7)
        key = "testObjectOwnershipDenyACL"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL="public-read")
        client.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        with pytest.raises(ClientError) as exc_info:
            client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_CONTROL_LIST_NOT_SUPPORTED
