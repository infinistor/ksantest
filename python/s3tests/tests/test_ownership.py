"""Ownership tests ported from Java testV2/Ownership.java."""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


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
        key = "test_bucket_ownership_deny_object_acl"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED

    @pytest.mark.tag("Check")
    def test_object_ownership_deny_change(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 6)
        key = "test_object_ownership_deny_change"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL="public-read")
        public_client = self.get_public_client()
        public_client.head_object(Bucket=bucket_name, Key=key)
        client.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        # BucketOwnerEnforced 로 바꾸면 오브젝트 ACL 이 무시되어 퍼블릭 접근이 막힌다.
        # AWS 는 이미 맺은 연결에서는 변경 전 권한으로 계속 응답하므로 새 클라이언트로 확인한다.
        new_public_client = self.get_public_client()
        self._wait_until(
            lambda: self._status(lambda: new_public_client.head_object(Bucket=bucket_name, Key=key)) == 403
        )

    @pytest.mark.tag("Error")
    def test_object_ownership_deny_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 7)
        key = "test_object_ownership_deny_acl"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL="public-read")
        client.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        with pytest.raises(ClientError) as exc_info:
            client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_CONTROL_LIST_NOT_SUPPORTED

    @staticmethod
    def _put_ownership(client, bucket_name: str, ownership: str) -> None:
        client.put_bucket_ownership_controls(
            Bucket=bucket_name, OwnershipControls={"Rules": [{"ObjectOwnership": ownership}]}
        )

    @staticmethod
    def _get_ownership(client, bucket_name: str) -> str:
        response = client.get_bucket_ownership_controls(Bucket=bucket_name)
        return response["OwnershipControls"]["Rules"][0]["ObjectOwnership"]

    @staticmethod
    def _object_owner_id(client, bucket_name: str, key: str) -> str:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=key, FetchOwner=True)
        return response["Contents"][0]["Owner"]["ID"]

    def _wait_until(self, check, timeout: int = 60, interval: int = 2) -> None:
        """설정 변경이 반영될 때까지 check 가 True 를 돌려주기를 기다린다."""
        deadline = time.monotonic() + timeout
        while True:
            if check():
                return
            assert time.monotonic() < deadline, "설정 변경이 제한 시간 안에 반영되지 않았습니다"
            time.sleep(interval)

    @staticmethod
    def _status(func) -> int:
        try:
            func()
            return 200
        except ClientError as e:
            return e.response["ResponseMetadata"]["HTTPStatusCode"]

    def _create_bucket_alt_writable(self, client, test_id: int) -> str:
        """ACL 로 alt 사용자에게 쓰기 권한을 준 ObjectWriter 버킷을 만든다."""
        bucket_name = self.create_bucket_canned_acl(client, test_id)
        client.put_bucket_acl(
            Bucket=bucket_name,
            GrantFullControl=f"id={self.config.main_user.id}",
            GrantWrite=f"id={self.config.alt_user.id}",
        )
        return bucket_name

    @pytest.mark.tag("Get")
    def test_bucket_ownership_default(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        # 소유권을 지정하지 않고 만든 버킷은 BucketOwnerEnforced 이다
        assert self._get_ownership(client, bucket_name) == "BucketOwnerEnforced"

    @pytest.mark.tag("Put")
    def test_put_bucket_ownership_all_values(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        for ownership in ("ObjectWriter", "BucketOwnerPreferred", "BucketOwnerEnforced"):
            self._put_ownership(client, bucket_name, ownership)
            assert self._get_ownership(client, bucket_name) == ownership

    @pytest.mark.tag("Error")
    def test_put_bucket_ownership_invalid(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        self.assert_client_error(
            lambda: self._put_ownership(client, bucket_name, "Invalid"), 400, md.MALFORMED_XML
        )

    @pytest.mark.tag("Delete")
    def test_delete_bucket_ownership(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11, "BucketOwnerPreferred")
        client.delete_bucket_ownership_controls(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.get_bucket_ownership_controls(Bucket=bucket_name),
            404,
            "OwnershipControlsNotFoundError",
        )
        # 설정이 없어도 삭제는 성공한다
        client.delete_bucket_ownership_controls(Bucket=bucket_name)

    @pytest.mark.tag("Error")
    def test_bucket_ownership_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(12)
        self.assert_client_error(
            lambda: client.get_bucket_ownership_controls(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: self._put_ownership(client, bucket_name, "ObjectWriter"), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: client.delete_bucket_ownership_controls(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )

    @pytest.mark.tag("Permission")
    def test_bucket_ownership_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 13)
        self.assert_client_error(
            lambda: alt_client.get_bucket_ownership_controls(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: self._put_ownership(alt_client, bucket_name, "ObjectWriter"), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: alt_client.delete_bucket_ownership_controls(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )

    @pytest.mark.tag("Error")
    def test_create_bucket_ownership_enforced_with_acl(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name(14)
        params = {"Bucket": bucket_name, "ObjectOwnership": "BucketOwnerEnforced", "ACL": "public-read"}
        self._apply_location_constraint(params)
        # BucketOwnerEnforced 버킷은 ACL 을 함께 지정해 만들 수 없다
        self.assert_client_error(
            lambda: client.create_bucket(**params), 400, "InvalidBucketAclWithObjectOwnership"
        )

    @pytest.mark.tag("Error")
    def test_create_bucket_invalid_ownership(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name(15)
        params = {"Bucket": bucket_name, "ObjectOwnership": "Invalid"}
        self._apply_location_constraint(params)
        self.assert_client_error(lambda: client.create_bucket(**params), 400, md.INVALID_ARGUMENT)

    @pytest.mark.tag("Error")
    def test_bucket_ownership_enforced_with_acl_grants(self):
        client = self.get_client()
        bucket_name = self._create_bucket_alt_writable(client, 16)
        # 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없다
        self.assert_client_error(
            lambda: self._put_ownership(client, bucket_name, "BucketOwnerEnforced"),
            400,
            "InvalidBucketAclWithObjectOwnership",
        )
        # ACL 을 초기화하면 바꿀 수 있다
        client.put_bucket_acl(Bucket=bucket_name, ACL="private")
        self._put_ownership(client, bucket_name, "BucketOwnerEnforced")
        assert self._get_ownership(client, bucket_name) == "BucketOwnerEnforced"

    @pytest.mark.tag("Owner")
    def test_object_owner_object_writer(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self._create_bucket_alt_writable(client, 17)
        key = "test_object_owner_object_writer"

        # ObjectWriter: 올린 사용자가 오브젝트 소유자가 되고, 버킷 소유자도 읽을 수 없다
        alt_client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        assert self._object_owner_id(client, bucket_name, key) == self.config.alt_user.id
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key), 403, md.ACCESS_DENIED
        )

        # bucket-owner-full-control 을 줘도 소유자는 올린 사용자다
        key2 = key + "2"
        alt_client.put_object(
            Bucket=bucket_name, Key=key2, Body=key.encode("utf-8"), ACL="bucket-owner-full-control"
        )
        assert self._object_owner_id(client, bucket_name, key2) == self.config.alt_user.id
        client.get_object(Bucket=bucket_name, Key=key2)

    @pytest.mark.tag("Owner")
    def test_object_owner_bucket_owner_preferred(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self._create_bucket_alt_writable(client, 18)
        self._put_ownership(client, bucket_name, "BucketOwnerPreferred")
        key = "test_object_owner_bucket_owner_preferred"

        # BucketOwnerPreferred: ACL 없이 올리면 올린 사용자가 소유자다
        alt_client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        assert self._object_owner_id(client, bucket_name, key) == self.config.alt_user.id

        # bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 된다
        key2 = key + "2"
        alt_client.put_object(
            Bucket=bucket_name, Key=key2, Body=key.encode("utf-8"), ACL="bucket-owner-full-control"
        )
        assert self._object_owner_id(client, bucket_name, key2) == self.config.main_user.id

    @pytest.mark.tag("Owner")
    def test_object_owner_bucket_owner_enforced(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 19, "BucketOwnerEnforced")
        key = "test_object_owner_bucket_owner_enforced"
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        principal = {"CanonicalUser": self.config.alt_user.id}
        client.put_bucket_policy(
            Bucket=bucket_name, Policy=self.make_json_policy("s3:PutObject", resource, principal)
        )

        # BucketOwnerEnforced: 누가 올려도 버킷 소유자가 소유자다
        self._wait_until(
            lambda: self._status(
                lambda: alt_client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
            )
            == 200
        )
        assert self._object_owner_id(client, bucket_name, key) == self.config.main_user.id

        # bucket-owner-full-control 은 허용되고 그 밖의 ACL 은 거부된다
        alt_client.put_object(
            Bucket=bucket_name, Key=key + "2", Body=key.encode("utf-8"), ACL="bucket-owner-full-control"
        )
        self.assert_client_error(
            lambda: alt_client.put_object(
                Bucket=bucket_name, Key=key + "3", Body=key.encode("utf-8"), ACL="private"
            ),
            400,
            md.ACCESS_CONTROL_LIST_NOT_SUPPORTED,
        )

    @pytest.mark.tag("Error")
    def test_bucket_ownership_enforced_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20, "BucketOwnerEnforced")
        key = "test_bucket_ownership_enforced_acl"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        # ACL 조회는 가능하고 소유자의 FULL_CONTROL 만 나온다
        for acl in (
            client.get_bucket_acl(Bucket=bucket_name),
            client.get_object_acl(Bucket=bucket_name, Key=key),
        ):
            assert len(acl["Grants"]) == 1
            assert acl["Grants"][0]["Grantee"]["ID"] == self.config.main_user.id
            assert acl["Grants"][0]["Permission"] == "FULL_CONTROL"

        # ACL 변경은 거부된다
        self.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, ACL="private"),
            400,
            md.ACCESS_CONTROL_LIST_NOT_SUPPORTED,
        )
        self.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key + "2", Body=b"a", ACL="public-read"),
            400,
            md.ACCESS_CONTROL_LIST_NOT_SUPPORTED,
        )
        # 소유자의 bucket-owner-full-control 은 허용된다
        client.put_object(Bucket=bucket_name, Key=key + "3", Body=b"a", ACL="bucket-owner-full-control")
