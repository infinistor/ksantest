"""Grants tests ported from Java testV2/Grants.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestGrants(S3TestBase):
    @pytest.mark.tag("Bucket")
    def test_bucket_acl_default(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_changed(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "public-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ"), response)

        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_private(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "private")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_public_read(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "public-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ"), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_public_rw(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ", "WRITE"), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_authenticated_read(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "authenticated-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_authenticated_acl("READ"), response)

    @pytest.mark.tag("Object")
    def test_object_acl_default(self):
        key = "testObjectAclDefault"
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Object")
    def test_object_acl_change(self):
        key = "testObjectAclCanned"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="public-read",
        )

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl("READ"), response)

        client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private")

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Object")
    def test_object_acl_private(self):
        key = "testObjectAclCannedPrivate"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="private",
        )

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Object")
    def test_object_acl_public_read(self):
        key = "testObjectAclCannedDuringCreate"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="public-read",
        )
        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl("READ"), response)

    @pytest.mark.tag("Object")
    def test_object_acl_public_rw(self):
        key = "testObjectAclCannedPublicRW"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="public-read-write",
        )

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl("READ", "WRITE"), response)

    @pytest.mark.tag("Object")
    def test_object_acl_authenticated_read(self):
        key = "testObjectAclCannedAuthenticatedRead"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="authenticated-read",
        )

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_authenticated_acl("READ"), response)

    @pytest.mark.tag("Object")
    def test_object_acl_bucket_owner_read(self):
        key = "testObjectAclBucketOwnerRead"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(main_client, "public-read-write")

        alt_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="bucket-owner-read",
        )
        response = alt_client.get_object_acl(Bucket=bucket_name, Key=key)

        expected = {
            "Owner": self.config.alt_user.to_owner(),
            "Grants": [
                self.config.alt_user.to_grant("FULL_CONTROL"),
                self.config.main_user.to_grant("READ"),
            ],
        }
        self.check_acl(expected, response)

    @pytest.mark.tag("Object")
    def test_bucket_object_writer_object_owner_full_control(self):
        key = "testBucketObjectWriterBucketOwnerFullControl"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(
            main_client, object_ownership="ObjectWriter", acl="public-read-write"
        )

        alt_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="bucket-owner-full-control",
        )

        response = main_client.get_object_acl(Bucket=bucket_name, Key=key)
        expected = self.create_acl(
            self.config.alt_user.to_owner(),
            self.config.main_user.to_grantee(),
            "FULL_CONTROL",
        )
        self.check_acl(expected, response)

    @pytest.mark.tag("Object")
    def test_bucket_owner_enforced_object_owner_full_control(self):
        key = "testBucketOwnerEnforcedBucketOwnerFullControl"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(
            main_client, object_ownership="BucketOwnerPreferred", acl="public-read-write"
        )

        alt_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ACL="bucket-owner-full-control",
        )

        response = main_client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Object")
    def test_object_acl_owner_not_change(self):
        key = "testObjectAclOwnerNotChange"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(main_client, "public-read-write")

        main_client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        acl1 = self.create_alt_acl("FULL_CONTROL")
        main_client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl1)

        acl2 = self.create_alt_acl("READ_ACP")
        alt_client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl2)

        response = alt_client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(acl2, response)

    @pytest.mark.tag("Effect")
    def test_bucket_acl_change_not_effect(self):
        key = "testBucketAclChangeNotEffect"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        response = client.head_object(Bucket=bucket_name, Key=key)
        content_type = response.get("ContentType")
        e_tag = response["ETag"]

        acl = self.create_alt_acl("FULL_CONTROL")
        client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl)

        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response.get("ContentType") == content_type
        assert response["ETag"] == e_tag

    @pytest.mark.tag("Permission")
    def test_bucket_acl_duplicated(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "private")
        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_full_control(self):
        self.check_bucket_acl("FULL_CONTROL")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_write(self):
        self.check_bucket_acl("WRITE")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_write_acp(self):
        self.check_bucket_acl("WRITE_ACP")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_read(self):
        self.check_bucket_acl("READ")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_read_acp(self):
        self.check_bucket_acl("READ_ACP")

    @pytest.mark.tag("Permission")
    def test_object_permission_full_control(self):
        self.check_object_acl("FULL_CONTROL")

    @pytest.mark.tag("Permission")
    def test_object_permission_write(self):
        self.check_object_acl("WRITE")

    @pytest.mark.tag("Permission")
    def test_object_permission_write_acp(self):
        self.check_object_acl("WRITE_ACP")

    @pytest.mark.tag("Permission")
    def test_object_permission_read(self):
        self.check_object_acl("READ")

    @pytest.mark.tag("Permission")
    def test_object_permission_read_acp(self):
        self.check_object_acl("READ_ACP")

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_grant_non_exist_user(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        bad_user = {"ID": "Foo", "Type": "CanonicalUser"}
        acl = self.add_bucket_user_grant(
            bucket_name,
            {"Grantee": bad_user, "Permission": "FULL_CONTROL"},
        )

        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_no_grants(self):
        key = "testBucketAclNoGrants"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.get_bucket_acl(Bucket=bucket_name)
        old_grants = response["Grants"]
        acl = {"Owner": response["Owner"], "Grants": []}

        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"A")

        client2 = self.get_client()
        client2.get_bucket_acl(Bucket=bucket_name)
        client2.put_bucket_acl(Bucket=bucket_name, ACL="private")

        acl = {"Owner": response["Owner"], "Grants": old_grants}
        client2.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)

    @pytest.mark.tag("Grant")
    def test_bucket_acl_multi_grants(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        acl = self.create_acl(
            self.config.main_user.to_owner(),
            self.config.alt_user.to_grantee(),
            "READ",
            "WRITE",
            "READ_ACP",
            "WRITE_ACP",
            "FULL_CONTROL",
        )

        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(acl, response)

    @pytest.mark.tag("Grant")
    def test_object_acl_multi_grants(self):
        key = "testObjectAclMultiGrants"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        acl = self.create_acl(
            self.config.main_user.to_owner(),
            self.config.alt_user.to_grantee(),
            "READ",
            "WRITE",
            "READ_ACP",
            "WRITE_ACP",
            "FULL_CONTROL",
        )

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl)

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(acl, response)

    @pytest.mark.tag("Delete")
    def test_bucket_acl_revoke_all(self):
        key = "testBucketAclRevokeAll"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.get_bucket_acl(Bucket=bucket_name)

        with pytest.raises(ClientError):
            client.put_bucket_acl(
                Bucket=bucket_name,
                AccessControlPolicy={"Owner": {}, "Grants": response["Grants"]},
            )

    @pytest.mark.tag("Delete")
    def test_object_acl_revoke_all(self):
        key = "testObjectAclRevokeAll"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.get_object_acl(Bucket=bucket_name, Key=key)

        with pytest.raises(ClientError):
            client.put_object_acl(
                Bucket=bucket_name,
                Key=key,
                AccessControlPolicy={"Owner": {}, "Grants": response["Grants"]},
            )

    @pytest.mark.tag("Error")
    def test_bucket_acl_revoke_all_id(self):
        key = "testBucketAclRevokeAllId"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.get_bucket_acl(Bucket=bucket_name)

        grantee = self.config.main_user.to_grantee()
        grantee.pop("ID", None)
        acl = {
            "Owner": response["Owner"],
            "Grants": [{"Grantee": grantee, "Permission": "FULL_CONTROL"}],
        }

        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl),
            400,
            md.MALFORMED_ACL_ERROR,
        )
