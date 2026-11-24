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
        bucket_name = self.create_bucket(client, 1)

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_changed(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 2, "public-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ"), response)

        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_private(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 3, "private")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_public_read(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 4, "public-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ"), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_public_rw(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 5, "public-read-write")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_public_acl("READ", "WRITE"), response)

    @pytest.mark.tag("Bucket")
    def test_bucket_acl_authenticated_read(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 6, "authenticated-read")

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(self.create_authenticated_acl("READ"), response)

    @pytest.mark.tag("Object")
    def test_object_acl_default(self):
        key = "test_object_acl_default"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(self.create_public_acl(), response)

    @pytest.mark.tag("Object")
    def test_object_acl_change(self):
        key = "test_object_acl_change"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 8)

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
        key = "test_object_acl_private"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 9)

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
        key = "test_object_acl_public_read"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 10)

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
        key = "test_object_acl_public_rw"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 11)

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
        key = "test_object_acl_authenticated_read"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 12)

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
        key = "test_object_acl_bucket_owner_read"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(main_client, 13, "public-read-write")

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
        key = "test_bucket_object_writer_object_owner_full_control"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(
            main_client, 14, object_ownership="ObjectWriter", acl="public-read-write"
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
        key = "test_bucket_owner_enforced_object_owner_full_control"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(
            main_client, 15, object_ownership="BucketOwnerPreferred", acl="public-read-write"
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
        key = "test_object_acl_owner_not_change"
        main_client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(main_client, 16, "public-read-write")

        main_client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        acl1 = self.create_alt_acl("FULL_CONTROL")
        main_client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl1)

        acl2 = self.create_alt_acl("READ_ACP")
        alt_client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl2)

        response = alt_client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(acl2, response)

    @pytest.mark.tag("Effect")
    def test_bucket_acl_change_not_effect(self):
        key = "test_bucket_acl_change_not_effect"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 17, "public-read-write")

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
        bucket_name = self.create_bucket_canned_acl(client, 18, "private")
        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

    @pytest.mark.tag("Permission")
    def test_bucket_permission_full_control(self):
        self.check_bucket_acl("FULL_CONTROL", 19)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_write(self):
        self.check_bucket_acl("WRITE", 20)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_write_acp(self):
        self.check_bucket_acl("WRITE_ACP", 21)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_read(self):
        self.check_bucket_acl("READ", 22)

    @pytest.mark.tag("Permission")
    def test_bucket_permission_read_acp(self):
        self.check_bucket_acl("READ_ACP", 23)

    @pytest.mark.tag("Permission")
    def test_object_permission_full_control(self):
        self.check_object_acl("FULL_CONTROL", 24)

    @pytest.mark.tag("Permission")
    def test_object_permission_write(self):
        self.check_object_acl("WRITE", 25)

    @pytest.mark.tag("Permission")
    def test_object_permission_write_acp(self):
        self.check_object_acl("WRITE_ACP", 26)

    @pytest.mark.tag("Permission")
    def test_object_permission_read(self):
        self.check_object_acl("READ", 27)

    @pytest.mark.tag("Permission")
    def test_object_permission_read_acp(self):
        self.check_object_acl("READ_ACP", 28)

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_grant_non_exist_user(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 29)

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
        key = "test_bucket_acl_no_grants"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 30)

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
        bucket_name = self.create_bucket_canned_acl(client, 31)
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
        key = "test_object_acl_multi_grants"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 32)
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
        key = "test_bucket_acl_revoke_all"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 33)

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.get_bucket_acl(Bucket=bucket_name)

        with pytest.raises(ClientError):
            client.put_bucket_acl(
                Bucket=bucket_name,
                AccessControlPolicy={"Owner": {}, "Grants": response["Grants"]},
            )

    @pytest.mark.tag("Delete")
    def test_object_acl_revoke_all(self):
        key = "test_object_acl_revoke_all"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 34)

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
        key = "test_bucket_acl_revoke_all_id"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 35)

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

    @staticmethod
    def _owner_policy(client, bucket_name: str, *grants) -> dict:
        """버킷 소유자를 Owner 로 하는 AccessControlPolicy. grants 가 없으면 소유자 FULL_CONTROL."""
        owner = client.get_bucket_acl(Bucket=bucket_name)["Owner"]
        default = {"Grantee": {"Type": "CanonicalUser", "ID": owner["ID"]}, "Permission": "FULL_CONTROL"}
        return {"Owner": {"ID": owner["ID"]}, "Grants": list(grants) if grants else [default]}

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_invalid_canned(self):
        key = "test_bucket_acl_invalid_canned"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 36)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, ACL="invalid"), 400, md.INVALID_ARGUMENT
        )
        self.assert_client_error(
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, ACL="invalid"), 400, md.INVALID_ARGUMENT
        )

    @pytest.mark.tag("ERROR")
    def test_acl_canned_with_grant_header(self):
        key = "test_acl_canned_with_grant_header"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 37)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        grant = f'uri="{md.AUTHENTICATED_USERS}"'
        # 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
        requests = [
            lambda: client.put_bucket_acl(Bucket=bucket_name, ACL="private", GrantRead=grant),
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private", GrantRead=grant),
            lambda: client.put_object(Bucket=bucket_name, Key=key + "2", Body=b"content", ACL="private", GrantRead=grant),
        ]
        for request in requests:
            self.assert_client_error(request, 400, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_canned_with_body(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 38)
        # 캔드 ACL 과 본문(AccessControlPolicy)은 함께 쓸 수 없다
        self.assert_client_error(
            lambda: client.put_bucket_acl(
                Bucket=bucket_name, ACL="private", AccessControlPolicy=self._owner_policy(client, bucket_name)
            ),
            400,
            md.UNEXPECTED_CONTENT,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_missing(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 39)
        # 캔드 ACL, 권한 부여 헤더, 본문 중 아무것도 없다
        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name), 400, md.MISSING_SECURITY_HEADER
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_invalid_permission(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 40)
        owner_id = client.get_bucket_acl(Bucket=bucket_name)["Owner"]["ID"]
        grant = {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "INVALID"}
        self.assert_client_error(
            lambda: client.put_bucket_acl(
                Bucket=bucket_name, AccessControlPolicy=self._owner_policy(client, bucket_name, grant)
            ),
            400,
            md.MALFORMED_ACL_ERROR,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_change_owner(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 41)
        policy = self._owner_policy(client, bucket_name)
        policy["Owner"] = {"ID": self.config.alt_user.id}
        # ACL 로 소유자를 바꿀 수 없다
        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy), 403, md.ACCESS_DENIED
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_invalid_grant_header(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 42)
        # 형식 오류, 없는 사용자 ID, 없는 그룹 URI
        for grant in ("invalid", 'id="invalid"', 'uri="http://example.com/invalid"'):
            self.assert_client_error(
                lambda grant=grant: client.put_bucket_acl(Bucket=bucket_name, GrantRead=grant),
                400,
                md.INVALID_ARGUMENT,
            )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_invalid_group_uri(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 43)
        grant = {"Grantee": {"Type": "Group", "URI": "http://example.com/invalid"}, "Permission": "READ"}
        self.assert_client_error(
            lambda: client.put_bucket_acl(
                Bucket=bucket_name, AccessControlPolicy=self._owner_policy(client, bucket_name, grant)
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Max")
    def test_bucket_acl_max_grants(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 44)
        owner_id = client.get_bucket_acl(Bucket=bucket_name)["Owner"]["ID"]
        grant = {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"}
        # 권한 부여는 100개까지
        client.put_bucket_acl(
            Bucket=bucket_name, AccessControlPolicy=self._owner_policy(client, bucket_name, *([grant] * 100))
        )
        self.assert_client_error(
            lambda: client.put_bucket_acl(
                Bucket=bucket_name, AccessControlPolicy=self._owner_policy(client, bucket_name, *([grant] * 101))
            ),
            400,
            md.MALFORMED_ACL_ERROR,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_acl_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(45)
        self.assert_client_error(lambda: client.get_bucket_acl(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, ACL="private"), 404, md.NO_SUCH_BUCKET
        )

    @pytest.mark.tag("Access")
    def test_acl_access_denied(self):
        key = "test_acl_access_denied"
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client, 46)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        requests = [
            lambda: alt_client.get_bucket_acl(Bucket=bucket_name),
            lambda: alt_client.put_bucket_acl(Bucket=bucket_name, ACL="private"),
            lambda: alt_client.get_object_acl(Bucket=bucket_name, Key=key),
            lambda: alt_client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private"),
        ]
        for request in requests:
            self.assert_client_error(request, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("ERROR")
    def test_object_acl_key_not_exist(self):
        key = "test_object_acl_key_not_exist"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 47)
        self.assert_client_error(lambda: client.get_object_acl(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)
        self.assert_client_error(
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private"), 404, md.NO_SUCH_KEY
        )

    @pytest.mark.tag("Version")
    def test_object_acl_version_id(self):
        key = "test_object_acl_version_id"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 48)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
        assert len(client.get_object_acl(Bucket=bucket_name, Key=key, VersionId="null")["Grants"]) == 1
        self.assert_client_error(
            lambda: client.get_object_acl(Bucket=bucket_name, Key=key, VersionId="invalid"), 400, md.INVALID_ARGUMENT
        )

    @pytest.mark.tag("Version")
    def test_object_acl_delete_marker(self):
        key = "test_object_acl_delete_marker"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 49)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=b"content")["VersionId"]
        marker_id = client.delete_object(Bucket=bucket_name, Key=key)["VersionId"]

        # 최신 버전이 삭제 마커이면 없는 키, 삭제 마커 버전을 지정하면 405
        self.assert_client_error(lambda: client.get_object_acl(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)
        self.assert_client_error(
            lambda: client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=marker_id),
            405,
            md.METHOD_NOT_ALLOWED,
        )
        self.assert_client_error(
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, VersionId=marker_id, ACL="private"),
            405,
            md.METHOD_NOT_ALLOWED,
        )
        assert len(client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)["Grants"]) == 1

    @pytest.mark.tag("Check")
    def test_put_object_with_grant_header(self):
        key = "test_put_object_with_grant_header"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 51)
        alt_id = self.config.alt_user.id
        # 권한 부여 헤더만 주면 소유자 FULL_CONTROL 없이 지정한 권한만 남는다
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content", GrantRead=f'id="{alt_id}"')
        grants = client.get_object_acl(Bucket=bucket_name, Key=key)["Grants"]
        assert [(grant["Grantee"]["ID"], grant["Permission"]) for grant in grants] == [(alt_id, "READ")]
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"
