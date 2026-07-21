"""Access tests ported from Java testV2/Access.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestAccess(S3TestBase):
    @pytest.mark.tag("Denied")
    def test_block_public_acl_and_policy(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership="ObjectWriter")

        access_conf = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": False,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": False,
        }
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=access_conf,
        )

        response = client.get_public_access_block(Bucket=bucket_name)
        actual = response["PublicAccessBlockConfiguration"]
        assert actual["BlockPublicAcls"] == access_conf["BlockPublicAcls"]
        assert actual["BlockPublicPolicy"] == access_conf["BlockPublicPolicy"]

        for acl in ("public-read", "public-read-write", "authenticated-read"):
            self.assert_client_error(
                lambda acl=acl: client.put_bucket_acl(Bucket=bucket_name, ACL=acl),
                403,
                md.ACCESS_DENIED,
            )

    @pytest.mark.tag("Denied")
    def test_block_public_acls(self):
        key = "testBlockPublicAcls"
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership="ObjectWriter")

        access_conf = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": False,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False,
        }
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=access_conf,
        )

        response = client.get_public_access_block(Bucket=bucket_name)
        assert (
            response["PublicAccessBlockConfiguration"]["BlockPublicAcls"]
            == access_conf["BlockPublicAcls"]
        )

        for acl in ("public-read", "public-read-write", "authenticated-read"):
            self.assert_client_error(
                lambda acl=acl: client.put_object(
                    Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL=acl
                ),
                403,
                md.ACCESS_DENIED,
            )

    @pytest.mark.tag("Denied")
    def test_block_public_policy(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership="ObjectWriter")

        access_conf = {
            "BlockPublicAcls": False,
            "IgnorePublicAcls": False,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": False,
        }
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=access_conf,
        )

        resource = utils.make_arn_resource(f"{bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", resource)
        self.assert_client_error(
            lambda: client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_delete_public_block(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership="ObjectWriter")

        access_conf = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": False,
        }
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=access_conf,
        )

        response = client.get_public_access_block(Bucket=bucket_name)
        actual = response["PublicAccessBlockConfiguration"]
        assert actual["BlockPublicAcls"] == access_conf["BlockPublicAcls"]
        assert actual["BlockPublicPolicy"] == access_conf["BlockPublicPolicy"]
        assert actual["IgnorePublicAcls"] == access_conf["IgnorePublicAcls"]
        assert actual["RestrictPublicBuckets"] == access_conf["RestrictPublicBuckets"]

        client.delete_public_access_block(Bucket=bucket_name)

        self.assert_client_error(
            lambda: client.get_public_access_block(Bucket=bucket_name),
            404,
            md.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION,
        )

    @pytest.mark.tag("Denied")
    def test_ignore_public_acls(self):
        key = "testIgnorePublicAcls"
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="public-read",
        )
        response = alt_client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == key

        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

        public_client = self.get_public_client()
        self.assert_client_error(
            lambda: public_client.list_objects(Bucket=bucket_name),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: public_client.get_object(Bucket=bucket_name, Key=key),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_put_public_block(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership="ObjectWriter")

        access_conf = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": False,
        }
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration=access_conf,
        )

        response = client.get_public_access_block(Bucket=bucket_name)
        actual = response["PublicAccessBlockConfiguration"]
        assert actual["BlockPublicAcls"] == access_conf["BlockPublicAcls"]
        assert actual["BlockPublicPolicy"] == access_conf["BlockPublicPolicy"]
        assert actual["IgnorePublicAcls"] == access_conf["IgnorePublicAcls"]
        assert actual["RestrictPublicBuckets"] == access_conf["RestrictPublicBuckets"]
        client.delete_public_access_block(Bucket=bucket_name)
