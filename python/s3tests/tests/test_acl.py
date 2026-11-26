"""ACL tests ported from Java testV2/ACL.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestACL(S3TestBase):
    @pytest.mark.tag("Access")
    def test_private_bucket_and_object(self):
        main_key = "test_private_bucket_and_object_main"
        alt_key = "test_private_bucket_and_object_alt"
        public_key = "test_private_bucket_and_object_public"
        bucket_name = self.setup_acl_objects("private", "private", main_key, alt_key, public_key, test_id=1)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_public_read_object(self):
        main_key = "test_private_bucket_public_read_object_main"
        alt_key = "test_private_bucket_public_read_object_alt"
        public_key = "test_private_bucket_public_read_object_public"
        bucket_name = self.setup_acl_objects("private", "public-read", main_key, alt_key, public_key, test_id=2)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_public_rw_object(self):
        main_key = "test_private_bucket_public_rw_object_main"
        alt_key = "test_private_bucket_public_rw_object_alt"
        public_key = "test_private_bucket_public_rw_object_public"
        bucket_name = self.setup_acl_objects("private", "public-read-write", main_key, alt_key, public_key, test_id=3)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_authenticated_read_object(self):
        main_key = "test_private_bucket_authenticated_read_object_main"
        alt_key = "test_private_bucket_authenticated_read_object_alt"
        public_key = "test_private_bucket_authenticated_read_object_public"
        bucket_name = self.setup_acl_objects("private", "authenticated-read", main_key, alt_key, public_key, test_id=4)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_bucket_owner_read_object(self):
        main_key = "test_private_bucket_bucket_owner_read_object_main"
        alt_key = "test_private_bucket_bucket_owner_read_object_alt"
        public_key = "test_private_bucket_bucket_owner_read_object_public"
        bucket_name = self.setup_acl_objects("private", "bucket-owner-read", main_key, alt_key, public_key, test_id=5)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_bucket_owner_read_object_upload_alt_user(self):
        main_key = "test_private_bucket_bucket_owner_read_object_upload_alt_user_main"
        alt_key = "test_private_bucket_bucket_owner_read_object_upload_alt_user_alt"
        public_key = "test_private_bucket_bucket_owner_read_object_upload_alt_user_public"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "bucket-owner-read", main_key, alt_key, public_key, test_id=6)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

        self.succeed_get_object(alt_client, bucket_name, main_key, main_key)
        self.succeed_get_object(client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.failed_put_object(alt_client, bucket_name, main_key, 403, md.ACCESS_DENIED)
        self.succeed_put_object(client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_private_bucket_bucket_owner_full_control_object(self):
        main_key = "test_private_bucket_bucket_owner_full_control_object_main"
        alt_key = "test_private_bucket_bucket_owner_full_control_object_alt"
        public_key = "test_private_bucket_bucket_owner_full_control_object_public"
        bucket_name = self.setup_acl_objects("private", "bucket-owner-full-control", main_key, alt_key, public_key, test_id=7)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_private_object(self):
        main_key = "test_public_read_bucket_private_object_main"
        alt_key = "test_public_read_bucket_private_object_alt"
        public_key = "test_public_read_bucket_private_object_public"
        bucket_name = self.setup_acl_objects("public-read", "private", main_key, alt_key, public_key, test_id=8)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_and_object(self):
        main_key = "test_public_read_bucket_and_object_main"
        alt_key = "test_public_read_bucket_and_object_alt"
        public_key = "test_public_read_bucket_and_object_public"
        bucket_name = self.setup_acl_objects("public-read", "public-read", main_key, alt_key, public_key, test_id=9)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_public_rw_object(self):
        main_key = "test_public_read_bucket_public_rw_object_main"
        alt_key = "test_public_read_bucket_public_rw_object_alt"
        public_key = "test_public_read_bucket_public_rw_object_public"
        bucket_name = self.setup_acl_objects("public-read", "public-read-write", main_key, alt_key, public_key, test_id=10)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_authenticated_read_object(self):
        main_key = "test_public_read_bucket_authenticated_read_object_main"
        alt_key = "test_public_read_bucket_authenticated_read_object_alt"
        public_key = "test_public_read_bucket_authenticated_read_object_public"
        bucket_name = self.setup_acl_objects("public-read", "authenticated-read", main_key, alt_key, public_key, test_id=11)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_bucket_owner_read_object(self):
        main_key = "test_public_read_bucket_bucket_owner_read_object_main"
        alt_key = "test_public_read_bucket_bucket_owner_read_object_alt"
        public_key = "test_public_read_bucket_bucket_owner_read_object_public"
        bucket_name = self.setup_acl_objects("public-read", "bucket-owner-read", main_key, alt_key, public_key, test_id=12)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_read_bucket_bucket_owner_full_control_object(self):
        main_key = "test_public_read_bucket_bucket_owner_full_control_object_main"
        alt_key = "test_public_read_bucket_bucket_owner_full_control_object_alt"
        public_key = "test_public_read_bucket_bucket_owner_full_control_object_public"
        bucket_name = self.setup_acl_objects("public-read", "bucket-owner-full-control", main_key, alt_key, public_key, test_id=13)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_private_object(self):
        main_key = "test_public_rw_bucket_private_object_main"
        alt_key = "test_public_rw_bucket_private_object_alt"
        alt_new_key = "test_public_rw_bucket_private_object_alt_new"
        public_key = "test_public_rw_bucket_private_object_public"
        public_new_key = "test_public_rw_bucket_private_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "private", main_key, alt_key, public_key, test_id=14)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_private_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_private_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_private_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_private_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_private_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "private", main_key, alt_key, public_key, test_id=15)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.failed_get_object(client, bucket_name, main_key, 403, md.ACCESS_DENIED)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

        alt_client.delete_object(Bucket=bucket_name, Key=alt_key)
        alt_client.delete_object(Bucket=bucket_name, Key=public_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_public_read_object(self):
        main_key = "test_public_rw_bucket_public_read_object_main"
        alt_key = "test_public_rw_bucket_public_read_object_alt"
        alt_new_key = "test_public_rw_bucket_public_read_object_alt_new"
        public_key = "test_public_rw_bucket_public_read_object_public"
        public_new_key = "test_public_rw_bucket_public_read_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "public-read", main_key, alt_key, public_key, test_id=16)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_public_read_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_public_read_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_public_read_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_public_read_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_public_read_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "public-read", main_key, alt_key, public_key, test_id=17)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

        alt_client.delete_object(Bucket=bucket_name, Key=alt_key)
        alt_client.delete_object(Bucket=bucket_name, Key=public_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_public_rw_object(self):
        main_key = "test_public_rw_bucket_public_rw_object_main"
        alt_key = "test_public_rw_bucket_public_rw_object_alt"
        alt_new_key = "test_public_rw_bucket_public_rw_object_alt_new"
        public_key = "test_public_rw_bucket_public_rw_object_public"
        public_new_key = "test_public_rw_bucket_public_rw_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "public-read-write", main_key, alt_key, public_key, test_id=18)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_public_rw_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_public_rw_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_public_rw_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_public_rw_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_public_rw_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "public-read-write", main_key, alt_key, public_key, test_id=19)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_authenticated_read_object(self):
        main_key = "test_public_rw_bucket_authenticated_read_object_main"
        alt_key = "test_public_rw_bucket_authenticated_read_object_alt"
        alt_new_key = "test_public_rw_bucket_authenticated_read_object_alt_new"
        public_key = "test_public_rw_bucket_authenticated_read_object_public"
        public_new_key = "test_public_rw_bucket_authenticated_read_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "authenticated-read", main_key, alt_key, public_key, test_id=20)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_authenticated_read_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_authenticated_read_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_authenticated_read_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_authenticated_read_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_authenticated_read_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "authenticated-read", main_key, alt_key, public_key, test_id=21)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_bucket_owner_read_object(self):
        main_key = "test_public_rw_bucket_bucket_owner_read_object_main"
        alt_key = "test_public_rw_bucket_bucket_owner_read_object_alt"
        alt_new_key = "test_public_rw_bucket_bucket_owner_read_object_alt_new"
        public_key = "test_public_rw_bucket_bucket_owner_read_object_public"
        public_new_key = "test_public_rw_bucket_bucket_owner_read_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "bucket-owner-read", main_key, alt_key, public_key, test_id=22)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_bucket_owner_read_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_bucket_owner_read_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_bucket_owner_read_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_bucket_owner_read_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_bucket_owner_read_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "bucket-owner-read", main_key, alt_key, public_key, test_id=23)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_bucket_owner_full_control_object(self):
        main_key = "test_public_rw_bucket_bucket_owner_full_control_object_main"
        alt_key = "test_public_rw_bucket_bucket_owner_full_control_object_alt"
        alt_new_key = "test_public_rw_bucket_bucket_owner_full_control_object_alt_new"
        public_key = "test_public_rw_bucket_bucket_owner_full_control_object_public"
        public_new_key = "test_public_rw_bucket_bucket_owner_full_control_object_public_new"
        bucket_name = self.setup_acl_objects("public-read-write", "bucket-owner-full-control", main_key, alt_key, public_key, test_id=24)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user(self):
        main_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_main"
        alt_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_alt"
        public_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_public"
        public_new_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_public_new"
        bucket_name = self.setup_acl_objects_by_alt("public-read-write", "bucket-owner-full-control", main_key, alt_key, public_key, test_id=25)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.succeed_put_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred(
        self,
    ):
        main_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred_main"
        alt_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred_alt"
        alt_new_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred_alt_new"
        public_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred_public"
        public_new_key = "test_public_rw_bucket_bucket_owner_full_control_object_by_alt_user_bucket_owner_preferred_public_new"
        bucket_name = self.setup_acl_objects_by_alt_with_ownership(
            "BucketOwnerPreferred",
            "public-read-write",
            "bucket-owner-full-control",
            main_key,
            alt_key,
            public_key,
            test_id=26,
        )

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(alt_client, bucket_name, alt_new_key, alt_new_key)
        self.succeed_put_object(public_client, bucket_name, public_new_key, public_new_key)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_private_object(self):
        main_key = "test_authenticated_read_bucket_private_object_main"
        alt_key = "test_authenticated_read_bucket_private_object_alt"
        public_key = "test_authenticated_read_bucket_private_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "private", main_key, alt_key, public_key, test_id=27)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_public_read_object(self):
        main_key = "test_authenticated_read_bucket_public_read_object_main"
        alt_key = "test_authenticated_read_bucket_public_read_object_alt"
        public_key = "test_authenticated_read_bucket_public_read_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "public-read", main_key, alt_key, public_key, test_id=28)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_public_rw_object(self):
        main_key = "test_authenticated_read_bucket_public_rw_object_main"
        alt_key = "test_authenticated_read_bucket_public_rw_object_alt"
        public_key = "test_authenticated_read_bucket_public_rw_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "public-read-write", main_key, alt_key, public_key, test_id=29)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.succeed_get_object(public_client, bucket_name, public_key, public_key)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_and_object(self):
        main_key = "test_authenticated_read_bucket_and_object_main"
        alt_key = "test_authenticated_read_bucket_and_object_alt"
        public_key = "test_authenticated_read_bucket_and_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "authenticated-read", main_key, alt_key, public_key, test_id=30)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.succeed_get_object(alt_client, bucket_name, alt_key, alt_key)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_bucket_owner_read_object(self):
        main_key = "test_authenticated_read_bucket_bucket_owner_read_object_main"
        alt_key = "test_authenticated_read_bucket_bucket_owner_read_object_alt"
        public_key = "test_authenticated_read_bucket_bucket_owner_read_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "bucket-owner-read", main_key, alt_key, public_key, test_id=31)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Access")
    def test_authenticated_read_bucket_bucket_owner_full_control_object(self):
        main_key = "test_authenticated_read_bucket_bucket_owner_full_control_object_main"
        alt_key = "test_authenticated_read_bucket_bucket_owner_full_control_object_alt"
        public_key = "test_authenticated_read_bucket_bucket_owner_full_control_object_public"
        bucket_name = self.setup_acl_objects("authenticated-read", "bucket-owner-full-control", main_key, alt_key, public_key, test_id=32)

        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()

        self.succeed_get_object(client, bucket_name, main_key, main_key)
        self.failed_get_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_get_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

        self.succeed_put_object(client, bucket_name, main_key, main_key)
        self.failed_put_object(alt_client, bucket_name, alt_key, 403, md.ACCESS_DENIED)
        self.failed_put_object(public_client, bucket_name, public_key, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("List")
    def test_private_bucket_list(self):
        keys = ["test_private_bucket_list1", "test_private_bucket_list2", "test_private_bucket_list3"]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("private", keys, test_id=33)

        self.succeed_list_objects(client, bucket_name, keys)
        self.failed_list_objects(alt_client, bucket_name, 403, md.ACCESS_DENIED)
        self.failed_list_objects(public_client, bucket_name, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("List")
    def test_public_read_bucket_list(self):
        keys = ["test_public_read_bucket_list1", "test_public_read_bucket_list2", "test_public_read_bucket_list3"]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("public-read", keys, test_id=34)

        self.succeed_list_objects(client, bucket_name, keys)
        self.succeed_list_objects(alt_client, bucket_name, keys)
        self.succeed_list_objects(public_client, bucket_name, keys)

    @pytest.mark.tag("List")
    def test_public_rw_bucket_list(self):
        keys = ["test_public_rw_bucket_list1", "test_public_rw_bucket_list2", "test_public_rw_bucket_list3"]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("public-read-write", keys, test_id=35)

        self.succeed_list_objects(client, bucket_name, keys)
        self.succeed_list_objects(alt_client, bucket_name, keys)
        self.succeed_list_objects(public_client, bucket_name, keys)

    @pytest.mark.tag("List")
    def test_authenticated_read_bucket_list(self):
        keys = [
            "test_authenticated_read_bucket_list1",
            "test_authenticated_read_bucket_list2",
            "test_authenticated_read_bucket_list3",
        ]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("authenticated-read", keys, test_id=36)

        self.succeed_list_objects(client, bucket_name, keys)
        self.succeed_list_objects(alt_client, bucket_name, keys)
        self.failed_list_objects(public_client, bucket_name, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_alt_user_full_control(self):
        bucket_name = self.setup_bucket_permission("FULL_CONTROL", 37)
        alt_client = self.get_alt_client()

        self.check_bucket_acl_allow_read(alt_client, bucket_name)
        self.check_bucket_acl_allow_read_acp(alt_client, bucket_name)
        self.check_bucket_acl_allow_write(alt_client, bucket_name)
        self.check_bucket_acl_allow_write_acp(alt_client, bucket_name)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_alt_user_read(self):
        bucket_name = self.setup_bucket_permission("READ", 38)
        alt_client = self.get_alt_client()

        self.check_bucket_acl_allow_read(alt_client, bucket_name)
        self.check_bucket_acl_deny_read_acp(alt_client, bucket_name)
        self.check_bucket_acl_deny_write(alt_client, bucket_name)
        self.check_bucket_acl_deny_write_acp(alt_client, bucket_name)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_alt_user_read_acp(self):
        bucket_name = self.setup_bucket_permission("READ_ACP", 39)
        alt_client = self.get_alt_client()

        self.check_bucket_acl_deny_read(alt_client, bucket_name)
        self.check_bucket_acl_allow_read_acp(alt_client, bucket_name)
        self.check_bucket_acl_deny_write(alt_client, bucket_name)
        self.check_bucket_acl_deny_write_acp(alt_client, bucket_name)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_alt_user_write(self):
        bucket_name = self.setup_bucket_permission("WRITE", 40)
        alt_client = self.get_alt_client()

        self.check_bucket_acl_deny_read(alt_client, bucket_name)
        self.check_bucket_acl_deny_read_acp(alt_client, bucket_name)
        self.check_bucket_acl_allow_write(alt_client, bucket_name)
        self.check_bucket_acl_deny_write_acp(alt_client, bucket_name)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_alt_user_write_acp(self):
        bucket_name = self.setup_bucket_permission("WRITE_ACP", 41)
        alt_client = self.get_alt_client()

        self.check_bucket_acl_deny_read(alt_client, bucket_name)
        self.check_bucket_acl_deny_read_acp(alt_client, bucket_name)
        self.check_bucket_acl_deny_write(alt_client, bucket_name)
        self.check_bucket_acl_allow_write_acp(alt_client, bucket_name)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_full_control(self):
        key = "test_object_permission_alt_user_full_control"
        bucket_name = self.setup_object_permission(key, "FULL_CONTROL", 42)
        alt_client = self.get_alt_client()

        self.check_object_acl_allow_read(alt_client, bucket_name, key)
        self.check_object_acl_allow_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_allow_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_read(self):
        key = "test_object_permission_alt_user_read"
        bucket_name = self.setup_object_permission(key, "READ", 43)
        alt_client = self.get_alt_client()

        self.check_object_acl_allow_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_read_acp(self):
        key = "test_object_permission_alt_user_read_acp"
        bucket_name = self.setup_object_permission(key, "READ_ACP", 44)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_allow_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_write(self):
        key = "test_object_permission_alt_user_write"
        bucket_name = self.setup_object_permission(key, "WRITE", 45)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_write_acp(self):
        key = "test_object_permission_alt_user_write_acp"
        bucket_name = self.setup_object_permission(key, "WRITE_ACP", 46)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_allow_write_acp(alt_client, bucket_name, key)

    # region ACL 권한 범위 (AWS 실측 기준)

    def _assert_denied(self, func) -> None:
        self.assert_client_error(func, 403, md.ACCESS_DENIED)

    def _grant_set(self, response: dict) -> list:
        """ACL 응답을 (대상, 권한) 목록으로 줄인다. 대상은 main, alt, 그룹 URI 중 하나다."""
        names = {self.config.main_user.id: "main", self.config.alt_user.id: "alt"}
        grants = []
        for grant in response["Grants"]:
            grantee = grant["Grantee"]
            grants.append((grantee.get("URI") or names.get(grantee.get("ID"), "other"), grant["Permission"]))
        return sorted(grants)

    def _setup_object_grant(self, key: str, permission: str, test_id: int) -> str:
        """비공개 버킷의 오브젝트에 서브 사용자 권한 하나만 준다."""
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, test_id)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=self.create_alt_acl(permission))
        return bucket_name

    @pytest.mark.tag("Scope")
    def test_bucket_acl_read_scope(self):
        key = "test_bucket_acl_read_scope"
        client = self.get_client()
        bucket_name = self.setup_bucket_permission("READ", 47)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key="multi")["UploadId"]

        alt_client = self.get_alt_client()
        # READ 는 목록 종류를 모두 연다
        alt_client.head_bucket(Bucket=bucket_name)
        assert alt_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 1
        assert len(alt_client.list_object_versions(Bucket=bucket_name)["Versions"]) == 1
        assert len(alt_client.list_multipart_uploads(Bucket=bucket_name)["Uploads"]) == 1
        # 목록 권한이 있으므로 없는 키는 404 다
        self.assert_client_error(lambda: alt_client.get_object(Bucket=bucket_name, Key="not-exist"), 404, md.NO_SUCH_KEY)

        # 오브젝트 읽기·쓰기와 버킷 설정은 열지 않는다
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.create_multipart_upload(Bucket=bucket_name, Key="new"))
        self._assert_denied(lambda: alt_client.list_parts(Bucket=bucket_name, Key="multi", UploadId=upload_id))
        self._assert_denied(lambda: alt_client.get_bucket_versioning(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.get_bucket_location(Bucket=bucket_name))
        response = alt_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key}]})
        assert not response.get("Deleted")
        assert response["Errors"][0]["Code"] == md.ACCESS_DENIED

    @pytest.mark.tag("Scope")
    def test_bucket_acl_write_scope(self):
        key = "test_bucket_acl_write_scope"
        client = self.get_client()
        bucket_name = self.setup_bucket_permission("WRITE", 48)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        client.put_object(Bucket=bucket_name, Key="other", Body=b"content")
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key="multi")["UploadId"]

        alt_client = self.get_alt_client()
        # WRITE 는 쓰기·덮어쓰기·삭제·멀티파트 시작을 연다(소유자가 올린 오브젝트도)
        alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x")
        alt_client.put_object(Bucket=bucket_name, Key=key, Body=b"overwritten")
        alt_client.delete_object(Bucket=bucket_name, Key=key)
        response = alt_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": "other"}]})
        assert [d["Key"] for d in response["Deleted"]] == ["other"]
        new_upload_id = alt_client.create_multipart_upload(Bucket=bucket_name, Key="alt-multi")["UploadId"]
        alt_client.abort_multipart_upload(Bucket=bucket_name, Key="alt-multi", UploadId=new_upload_id)
        assert [c["Key"] for c in client.list_objects_v2(Bucket=bucket_name)["Contents"]] == ["new"]

        # 읽기·목록은 열지 않는다. 목록 권한이 없어 없는 키도 403 이다
        self.assert_client_error(lambda: alt_client.head_bucket(Bucket=bucket_name), 403, "403")
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.list_multipart_uploads(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="not-exist"))
        self._assert_denied(lambda: alt_client.get_bucket_acl(Bucket=bucket_name))
        # 다른 사용자가 시작한 멀티파트는 중단할 수 없다
        self._assert_denied(
            lambda: alt_client.abort_multipart_upload(Bucket=bucket_name, Key="multi", UploadId=upload_id)
        )

    @pytest.mark.tag("Scope")
    def test_bucket_acl_full_control_scope(self):
        client = self.get_client()
        bucket_name = self.setup_bucket_permission("FULL_CONTROL", 49)
        alt_client = self.get_alt_client()
        assert alt_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0
        alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x")
        alt_client.delete_object(Bucket=bucket_name, Key="new")
        assert self._grant_set(alt_client.get_bucket_acl(Bucket=bucket_name)) == [
            ("alt", "FULL_CONTROL"),
            ("main", "FULL_CONTROL"),
        ]
        alt_client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=self.create_alt_acl("FULL_CONTROL"))

        # ACL 의 FULL_CONTROL 은 READ·WRITE·READ_ACP·WRITE_ACP 뿐이다. 버킷 설정과 삭제는 소유자만 한다
        self._assert_denied(lambda: alt_client.get_bucket_versioning(Bucket=bucket_name))
        self._assert_denied(
            lambda: alt_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        )
        self._assert_denied(
            lambda: alt_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": [{"Key": "a", "Value": "b"}]})
        )
        self._assert_denied(lambda: alt_client.get_bucket_policy(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.delete_bucket(Bucket=bucket_name))
        client.head_bucket(Bucket=bucket_name)

    @pytest.mark.tag("Scope")
    def test_object_acl_read_scope(self):
        key = "test_object_acl_read_scope"
        bucket_name = self._setup_object_grant(key, "READ", 50)
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert alt_client.head_object(Bucket=bucket_name, Key=key)["ContentLength"] == 7
        # READ 는 본문과 메타데이터만 연다
        self._assert_denied(lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=key))
        self._assert_denied(
            lambda: alt_client.get_object_attributes(Bucket=bucket_name, Key=key, ObjectAttributes=["ETag"])
        )
        self._assert_denied(lambda: alt_client.get_object_acl(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private"))
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key=key, Body=b"x"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Scope")
    def test_object_acl_full_control_scope(self):
        key = "test_object_acl_full_control_scope"
        bucket_name = self._setup_object_grant(key, "FULL_CONTROL", 51)
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert ("alt", "FULL_CONTROL") in self._grant_set(alt_client.get_object_acl(Bucket=bucket_name, Key=key))
        # 오브젝트의 FULL_CONTROL 에는 쓰기가 없다. 덮어쓰기·삭제는 버킷 권한으로 판정한다
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key=key, Body=b"x"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=key))
        self._assert_denied(
            lambda: alt_client.put_object_tagging(
                Bucket=bucket_name, Key=key, Tagging={"TagSet": [{"Key": "a", "Value": "b"}]}
            )
        )
        # WRITE_ACP 가 있으므로 ACL 은 바꿀 수 있다
        alt_client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private")

    @pytest.mark.tag("Copy")
    def test_object_acl_not_copied(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 52)
        source = {"Bucket": bucket_name, "Key": "source"}
        client.put_object(Bucket=bucket_name, Key="source", Body=b"content", ACL="public-read")
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key="source")) == [
            (md.ALL_USERS, "READ"),
            ("main", "FULL_CONTROL"),
        ]

        # 복사본은 원본의 ACL 을 물려받지 않는다
        client.copy_object(Bucket=bucket_name, Key="copy", CopySource=source)
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key="copy")) == [("main", "FULL_CONTROL")]
        # 복사 요청에 준 ACL 이 적용된다
        client.copy_object(Bucket=bucket_name, Key="canned", CopySource=source, ACL="authenticated-read")
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key="canned")) == [
            (md.AUTHENTICATED_USERS, "READ"),
            ("main", "FULL_CONTROL"),
        ]
        # grant 헤더만 주면 소유자 전권은 붙지 않는다
        client.copy_object(
            Bucket=bucket_name, Key="grant", CopySource=source, GrantRead=f'id="{self.config.alt_user.id}"'
        )
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key="grant")) == [("alt", "READ")]

    @pytest.mark.tag("Overwrite")
    def test_object_acl_reset_on_overwrite(self):
        key = "test_object_acl_reset_on_overwrite"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 53)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content", ACL="public-read")
        public_client = self.get_public_client()
        assert self.get_body(public_client.get_object(Bucket=bucket_name, Key=key)) == "content"

        # ACL 없이 덮어쓰면 비공개로 돌아간다
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key=key)) == [("main", "FULL_CONTROL")]
        self._assert_denied(lambda: self.get_public_client().get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Multipart")
    def test_object_acl_multipart(self):
        key = "test_object_acl_multipart"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 54)
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=key, ACL="public-read")["UploadId"]
        etag = client.upload_part(Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=b"content")["ETag"]
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
        )
        # 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트의 ACL 이 된다
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key=key)) == [
            (md.ALL_USERS, "READ"),
            ("main", "FULL_CONTROL"),
        ]
        public_client = self.get_public_client()
        assert self.get_body(public_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        public_client.head_object(Bucket=bucket_name, Key=key)
        # public-read 는 ACL 조회와 태그 조회를 열지 않는다
        self._assert_denied(lambda: public_client.get_object_acl(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: public_client.get_object_tagging(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Version")
    def test_object_acl_per_version(self):
        key = "test_object_acl_per_version"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 55)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version1 = client.put_object(Bucket=bucket_name, Key=key, Body=b"1", ACL="public-read")["VersionId"]
        version2 = client.put_object(Bucket=bucket_name, Key=key, Body=b"2")["VersionId"]

        # ACL 은 버전마다 따로다
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version1)) == [
            (md.ALL_USERS, "READ"),
            ("main", "FULL_CONTROL"),
        ]
        assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version2)) == [
            ("main", "FULL_CONTROL")
        ]
        public_client = self.get_public_client()
        assert self.get_body(public_client.get_object(Bucket=bucket_name, Key=key, VersionId=version1)) == "1"
        self._assert_denied(lambda: public_client.get_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: public_client.get_object(Bucket=bucket_name, Key=key, VersionId=version2))

    @pytest.mark.tag("Canned")
    def test_acl_canned_bucket_owner(self):
        key = "test_acl_canned_bucket_owner"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 56)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content", ACL="public-read")
        # 버킷 소유자가 곧 오브젝트 소유자면 bucket-owner-* 는 소유자 전권만 남긴다
        for acl in ("bucket-owner-read", "bucket-owner-full-control"):
            client.put_object_acl(Bucket=bucket_name, Key=key, ACL=acl)
            assert self._grant_set(client.get_object_acl(Bucket=bucket_name, Key=key)) == [("main", "FULL_CONTROL")]
        # 버킷에 주면 무시되고 비공개가 된다
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
        for acl in ("bucket-owner-read", "bucket-owner-full-control"):
            client.put_bucket_acl(Bucket=bucket_name, ACL=acl)
            assert self._grant_set(client.get_bucket_acl(Bucket=bucket_name)) == [("main", "FULL_CONTROL")]

    @pytest.mark.tag("Scope")
    def test_public_rw_bucket_anonymous_scope(self):
        key = "test_public_rw_bucket_anonymous_scope"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 57, "public-read-write")
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        client.create_multipart_upload(Bucket=bucket_name, Key="multi")

        public_client = self.get_public_client()
        assert public_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 1
        assert len(public_client.list_object_versions(Bucket=bucket_name)["Versions"]) == 1
        assert len(public_client.list_multipart_uploads(Bucket=bucket_name)["Uploads"]) == 1
        public_client.put_object(Bucket=bucket_name, Key="anonymous", Body=b"x")
        assert self.get_body(public_client.get_object(Bucket=bucket_name, Key="anonymous")) == "x"

        # 버킷이 열려 있어도 비공개 오브젝트와 ACL 은 열리지 않는다
        self._assert_denied(lambda: public_client.get_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: public_client.get_bucket_acl(Bucket=bucket_name))
        self._assert_denied(lambda: public_client.put_bucket_acl(Bucket=bucket_name, ACL="private"))
        # 익명 사용자는 멀티파트 업로드를 시작할 수 없다
        self._assert_denied(lambda: public_client.create_multipart_upload(Bucket=bucket_name, Key="new"))

    # endregion
