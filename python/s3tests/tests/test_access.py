"""Access tests ported from Java testV2/Access.java."""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestAccess(S3TestBase):
    @pytest.mark.tag("Denied")
    def test_block_public_acl_and_policy(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1, object_ownership="ObjectWriter")

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
        key = "test_block_public_acls"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2, object_ownership="ObjectWriter")

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
        bucket_name = self.create_bucket(client, 3, object_ownership="ObjectWriter")

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
        bucket_name = self.create_bucket(client, 4, object_ownership="ObjectWriter")

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
        key = "test_ignore_public_acls"
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client, 5)

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
        bucket_name = self.create_bucket(client, 6, object_ownership="ObjectWriter")

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

    _ALL_OFF = {
        "BlockPublicAcls": False,
        "IgnorePublicAcls": False,
        "BlockPublicPolicy": False,
        "RestrictPublicBuckets": False,
    }

    def _put_public_block(self, client, bucket_name: str, **overrides) -> dict:
        config = dict(self._ALL_OFF)
        config.update(overrides)
        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=config)
        return config

    def _create_raw_bucket(self, client, test_id: int) -> str:
        """퍼블릭 액세스 차단 설정을 건드리지 않고 기본값 그대로 버킷을 만든다."""
        bucket_name = self.get_new_bucket_name(test_id)
        params = {"Bucket": bucket_name}
        self._apply_location_constraint(params)
        client.create_bucket(**params)
        return bucket_name

    def _wait_until(self, check, timeout: int = 60) -> None:
        """설정 변경이 반영될 때까지 check 가 True 를 돌려주기를 기다린다."""
        deadline = time.monotonic() + timeout
        while True:
            if check():
                return
            assert time.monotonic() < deadline, "설정 변경이 제한 시간 안에 반영되지 않았습니다"
            time.sleep(2)

    @staticmethod
    def _get_object_status(client, bucket_name: str, key: str) -> int:
        try:
            client.get_object(Bucket=bucket_name, Key=key)
            return 200
        except ClientError as e:
            return e.response["ResponseMetadata"]["HTTPStatusCode"]

    @pytest.mark.tag("Check")
    def test_public_block_default(self):
        client = self.get_client()
        bucket_name = self._create_raw_bucket(client, 7)
        # 새 버킷은 네 가지 차단이 모두 켜진 상태로 만들어진다
        response = client.get_public_access_block(Bucket=bucket_name)
        assert response["PublicAccessBlockConfiguration"] == {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }

    @pytest.mark.tag("Check")
    def test_put_public_block_partial(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        # 지정하지 않은 항목은 false 로 저장된다
        client.put_public_access_block(
            Bucket=bucket_name, PublicAccessBlockConfiguration={"BlockPublicAcls": True}
        )
        response = client.get_public_access_block(Bucket=bucket_name)
        assert response["PublicAccessBlockConfiguration"] == {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": False,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False,
        }

    @pytest.mark.tag("Check")
    def test_delete_public_block_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        client.delete_public_access_block(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.get_public_access_block(Bucket=bucket_name),
            404,
            md.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION,
        )
        # 설정이 없어도 삭제는 성공한다
        client.delete_public_access_block(Bucket=bucket_name)

    @pytest.mark.tag("Error")
    def test_public_block_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(10)
        self.assert_client_error(
            lambda: client.get_public_access_block(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: self._put_public_block(client, bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: client.delete_public_access_block(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )

    @pytest.mark.tag("Permission")
    def test_public_block_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 11)
        self._put_public_block(client, bucket_name, BlockPublicAcls=True)
        self.assert_client_error(
            lambda: alt_client.get_public_access_block(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: self._put_public_block(alt_client, bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: alt_client.delete_public_access_block(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )

    @pytest.mark.tag("Denied")
    def test_block_public_acls_allow_private(self):
        key = "test_block_public_acls_allow_private"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12, "ObjectWriter")
        self._put_public_block(client, bucket_name, BlockPublicAcls=True)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        # 퍼블릭 ACL 은 오브젝트 ACL 변경에서도 거부된다
        self.assert_client_error(
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read"),
            403,
            md.ACCESS_DENIED,
        )
        # 퍼블릭이 아닌 ACL 은 허용된다
        client.put_object_acl(Bucket=bucket_name, Key=key, ACL="private")
        client.put_object(Bucket=bucket_name, Key=key + "2", Body=key.encode("utf-8"), ACL="private")
        client.put_bucket_acl(Bucket=bucket_name, ACL="private")

    @pytest.mark.tag("Policy")
    def test_bucket_policy_status(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        self._put_public_block(client, bucket_name)
        resource = utils.make_arn_resource(f"{bucket_name}/*")

        self.assert_client_error(
            lambda: client.get_bucket_policy_status(Bucket=bucket_name), 404, "NoSuchBucketPolicy"
        )

        # 모든 사용자에게 여는 정책은 퍼블릭으로 판정된다
        client.put_bucket_policy(Bucket=bucket_name, Policy=self.make_json_policy("s3:GetObject", resource))
        assert client.get_bucket_policy_status(Bucket=bucket_name)["PolicyStatus"]["IsPublic"] is True

        # 특정 사용자에게만 여는 정책은 퍼블릭이 아니다
        principal = {"CanonicalUser": self.config.alt_user.id}
        client.put_bucket_policy(
            Bucket=bucket_name, Policy=self.make_json_policy("s3:GetObject", resource, principal)
        )
        assert client.get_bucket_policy_status(Bucket=bucket_name)["PolicyStatus"]["IsPublic"] is False

    @pytest.mark.tag("Policy")
    def test_block_public_policy_allow_non_public(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        self._put_public_block(client, bucket_name, BlockPublicPolicy=True)
        resource = utils.make_arn_resource(f"{bucket_name}/*")

        # 퍼블릭 정책은 거부되지만 특정 사용자에게만 여는 정책은 허용된다
        self.assert_client_error(
            lambda: client.put_bucket_policy(
                Bucket=bucket_name, Policy=self.make_json_policy("s3:GetObject", resource)
            ),
            403,
            md.ACCESS_DENIED,
        )
        principal = {"CanonicalUser": self.config.alt_user.id}
        client.put_bucket_policy(
            Bucket=bucket_name, Policy=self.make_json_policy("s3:GetObject", resource, principal)
        )
        assert client.get_bucket_policy(Bucket=bucket_name)["Policy"]

    @pytest.mark.tag("Denied")
    def test_restrict_public_buckets(self):
        key = "test_restrict_public_buckets"
        client = self.get_client()
        alt_client = self.get_alt_client()
        public_client = self.get_public_client()
        bucket_name = self.create_bucket(client, 15)
        self._put_public_block(client, bucket_name)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        self.set_public_read_policy(client, bucket_name)

        # 퍼블릭 정책으로 누구나 읽을 수 있다
        self._wait_until(lambda: self._get_object_status(public_client, bucket_name, key) == 200)
        self._wait_until(lambda: self._get_object_status(alt_client, bucket_name, key) == 200)

        # RestrictPublicBuckets 를 켜면 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있다
        self._put_public_block(client, bucket_name, RestrictPublicBuckets=True)
        self._wait_until(lambda: self._get_object_status(public_client, bucket_name, key) == 403)
        self._wait_until(lambda: self._get_object_status(alt_client, bucket_name, key) == 403)
        assert self._get_object_status(client, bucket_name, key) == 200
