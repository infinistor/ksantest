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
