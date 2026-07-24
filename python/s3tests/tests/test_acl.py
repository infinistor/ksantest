"""ACL tests ported from Java testV2/ACL.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestACL(S3TestBase):
    @pytest.mark.tag("Access")
    def test_private_bucket_and_object(self):
        main_key = "testDefaultObjectPutGetMain"
        alt_key = "testDefaultObjectPutGetAlt"
        public_key = "testDefaultObjectPutGetPublic"
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
        main_key = "testPrivateBucketPublicObjectMain"
        alt_key = "testPrivateBucketPublicObjectAlt"
        public_key = "testPrivateBucketPublicObjectPublic"
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
        main_key = "testPrivateBucketPublicRWObjectMain"
        alt_key = "testPrivateBucketPublicRWObjectAlt"
        public_key = "testPrivateBucketPublicRWObjectPublic"
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
        main_key = "testPrivateBucketAuthenticatedObjectMain"
        alt_key = "testPrivateBucketAuthenticatedObjectAlt"
        public_key = "testPrivateBucketAuthenticatedObjectPublic"
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
        main_key = "testPrivateBucketBucketOwnerReadObjectMain"
        alt_key = "testPrivateBucketBucketOwnerReadObjectAlt"
        public_key = "testPrivateBucketBucketOwnerReadObjectPublic"
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
        main_key = "testPrivateBucketBucketOwnerReadObjectUploadAltUserMain"
        alt_key = "testPrivateBucketBucketOwnerReadObjectUploadAltUserAlt"
        public_key = "testPrivateBucketBucketOwnerReadObjectUploadAltUserPublic"
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
        main_key = "testPrivateBucketBucketOwnerFullControlObjectMain"
        alt_key = "testPrivateBucketBucketOwnerFullControlObjectAlt"
        public_key = "testPrivateBucketBucketOwnerFullControlObjectPublic"
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
        main_key = "testPublicReadBucketPrivateObjectMain"
        alt_key = "testPublicReadBucketPrivateObjectAlt"
        public_key = "testPublicReadBucketPrivateObjectPublic"
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
        main_key = "testPublicReadBucketAndObjectMain"
        alt_key = "testPublicReadBucketAndObjectAlt"
        public_key = "testPublicReadBucketAndObjectPublic"
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
        main_key = "testPublicReadBucketPublicRWObjectMain"
        alt_key = "testPublicReadBucketPublicRWObjectAlt"
        public_key = "testPublicReadBucketPublicRWObjectPublic"
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
        main_key = "testPublicReadBucketAuthenticatedReadObjectMain"
        alt_key = "testPublicReadBucketAuthenticatedReadObjectAlt"
        public_key = "testPublicReadBucketAuthenticatedReadObjectPublic"
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
        main_key = "testPublicReadBucketBucketOwnerReadObjectMain"
        alt_key = "testPublicReadBucketBucketOwnerReadObjectAlt"
        public_key = "testPublicReadBucketBucketOwnerReadObjectPublic"
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
        main_key = "testPublicReadBucketBucketOwnerFullControlObjectMain"
        alt_key = "testPublicReadBucketBucketOwnerFullControlObjectAlt"
        public_key = "testPublicReadBucketBucketOwnerFullControlObjectPublic"
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
        main_key = "testPublicRWBucketPrivateObjectMain"
        alt_key = "testPublicRWBucketPrivateObjectAlt"
        alt_new_key = "testPublicRWBucketPrivateObjectAltNew"
        public_key = "testPublicRWBucketPrivateObjectPublic"
        public_new_key = "testPublicRWBucketPrivateObjectPublicNew"
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
        main_key = "testPublicRWBucketPrivateObjectByAltUserMain"
        alt_key = "testPublicRWBucketPrivateObjectByAltUserAlt"
        public_key = "testPublicRWBucketPrivateObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketPrivateObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketPublicReadObjectMain"
        alt_key = "testPublicRWBucketPublicReadObjectAlt"
        alt_new_key = "testPublicRWBucketPublicReadObjectAltNew"
        public_key = "testPublicRWBucketPublicReadObjectPublic"
        public_new_key = "testPublicRWBucketPublicReadObjectPublicNew"
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
        main_key = "testPublicRWBucketPublicReadObjectByAltUserMain"
        alt_key = "testPublicRWBucketPublicReadObjectByAltUserAlt"
        public_key = "testPublicRWBucketPublicReadObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketPublicReadObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketPublicRWObjectMain"
        alt_key = "testPublicRWBucketPublicRWObjectAlt"
        alt_new_key = "testPublicRWBucketPublicRWObjectAltNew"
        public_key = "testPublicRWBucketPublicRWObjectPublic"
        public_new_key = "testPublicRWBucketPublicRWObjectPublicNew"
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
        main_key = "testPublicRWBucketPublicRWObjectByAltUserMain"
        alt_key = "testPublicRWBucketPublicRWObjectByAltUserAlt"
        public_key = "testPublicRWBucketPublicRWObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketPublicRWObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketAuthenticatedReadObjectMain"
        alt_key = "testPublicRWBucketAuthenticatedReadObjectAlt"
        alt_new_key = "testPublicRWBucketAuthenticatedReadObjectAltNew"
        public_key = "testPublicRWBucketAuthenticatedReadObjectPublic"
        public_new_key = "testPublicRWBucketAuthenticatedReadObjectPublicNew"
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
        main_key = "testPublicRWBucketAuthenticatedReadObjectByAltUserMain"
        alt_key = "testPublicRWBucketAuthenticatedReadObjectByAltUserAlt"
        public_key = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketBucketOwnerReadObjectMain"
        alt_key = "testPublicRWBucketBucketOwnerReadObjectAlt"
        alt_new_key = "testPublicRWBucketBucketOwnerReadObjectAltNew"
        public_key = "testPublicRWBucketBucketOwnerReadObjectPublic"
        public_new_key = "testPublicRWBucketBucketOwnerReadObjectPublicNew"
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
        main_key = "testPublicRWBucketBucketOwnerReadObjectByAltUserMain"
        alt_key = "testPublicRWBucketBucketOwnerReadObjectByAltUserAlt"
        public_key = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketBucketOwnerFullControlObjectMain"
        alt_key = "testPublicRWBucketBucketOwnerFullControlObjectAlt"
        alt_new_key = "testPublicRWBucketBucketOwnerFullControlObjectAltNew"
        public_key = "testPublicRWBucketBucketOwnerFullControlObjectPublic"
        public_new_key = "testPublicRWBucketBucketOwnerFullControlObjectPublicNew"
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
        main_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserMain"
        alt_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserAlt"
        public_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublic"
        public_new_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublicNew"
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
        main_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredMain"
        alt_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAlt"
        alt_new_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAltNew"
        public_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublic"
        public_new_key = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublicNew"
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
        main_key = "testAuthenticatedReadBucketPrivateObjectMain"
        alt_key = "testAuthenticatedReadBucketPrivateObjectAlt"
        public_key = "testAuthenticatedReadBucketPrivateObjectPublic"
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
        main_key = "testAuthenticatedReadBucketPublicReadObjectMain"
        alt_key = "testAuthenticatedReadBucketPublicReadObjectAlt"
        public_key = "testAuthenticatedReadBucketPublicReadObjectPublic"
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
        main_key = "testAuthenticatedReadBucketPublicRWObjectMain"
        alt_key = "testAuthenticatedReadBucketPublicRWObjectAlt"
        public_key = "testAuthenticatedReadBucketPublicRWObjectPublic"
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
        main_key = "testAuthenticatedReadBucketAndObjectMain"
        alt_key = "testAuthenticatedReadBucketAndObjectAlt"
        public_key = "testAuthenticatedReadBucketAndObjectPublic"
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
        main_key = "testAuthenticatedReadBucketBucketOwnerReadObjectMain"
        alt_key = "testAuthenticatedReadBucketBucketOwnerReadObjectAlt"
        public_key = "testAuthenticatedReadBucketBucketOwnerReadObjectPublic"
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
        main_key = "testAuthenticatedReadBucketBucketOwnerFullControlObjectMain"
        alt_key = "testAuthenticatedReadBucketBucketOwnerFullControlObjectAlt"
        public_key = "testAuthenticatedReadBucketBucketOwnerFullControlObjectPublic"
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
        keys = ["testPrivateBucketList1", "testPrivateBucketList2", "testPrivateBucketList3"]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("private", keys, test_id=33)

        self.succeed_list_objects(client, bucket_name, keys)
        self.failed_list_objects(alt_client, bucket_name, 403, md.ACCESS_DENIED)
        self.failed_list_objects(public_client, bucket_name, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("List")
    def test_public_read_bucket_list(self):
        keys = ["testPublicReadBucketList1", "testPublicReadBucketList2", "testPublicReadBucketList3"]
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.setup_acl_bucket("public-read", keys, test_id=34)

        self.succeed_list_objects(client, bucket_name, keys)
        self.succeed_list_objects(alt_client, bucket_name, keys)
        self.succeed_list_objects(public_client, bucket_name, keys)

    @pytest.mark.tag("List")
    def test_public_rw_bucket_list(self):
        keys = ["testPublicRWBucketList1", "testPublicRWBucketList2", "testPublicRWBucketList3"]
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
            "testAuthenticatedReadBucketList1",
            "testAuthenticatedReadBucketList2",
            "testAuthenticatedReadBucketList3",
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
        key = "testObjectPermissionAltUserFullControl"
        bucket_name = self.setup_object_permission(key, "FULL_CONTROL", 42)
        alt_client = self.get_alt_client()

        self.check_object_acl_allow_read(alt_client, bucket_name, key)
        self.check_object_acl_allow_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_allow_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_read(self):
        key = "testObjectPermissionAltUserRead"
        bucket_name = self.setup_object_permission(key, "READ", 43)
        alt_client = self.get_alt_client()

        self.check_object_acl_allow_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_read_acp(self):
        key = "testObjectPermissionAltUserReadAcp"
        bucket_name = self.setup_object_permission(key, "READ_ACP", 44)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_allow_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_write(self):
        key = "testObjectPermissionAltUserWrite"
        bucket_name = self.setup_object_permission(key, "WRITE", 45)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_deny_write_acp(alt_client, bucket_name, key)

    @pytest.mark.tag("Permission")
    def test_object_permission_alt_user_write_acp(self):
        key = "testObjectPermissionAltUserWriteAcp"
        bucket_name = self.setup_object_permission(key, "WRITE_ACP", 46)
        alt_client = self.get_alt_client()

        self.check_object_acl_deny_read(alt_client, bucket_name, key)
        self.check_object_acl_deny_read_acp(alt_client, bucket_name, key)
        self.check_object_acl_deny_write(alt_client, bucket_name, key)
        self.check_object_acl_allow_write_acp(alt_client, bucket_name, key)
