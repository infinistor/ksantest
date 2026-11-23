"""PutBucket tests ported from Java testV2/PutBucket.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestPutBucket(S3TestBase):
    @pytest.mark.tag("PUT")
    def test_bucket_list_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        response = client.list_objects(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 0

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_starts_non_alpha(self):
        bucket_name = self.get_new_bucket_name(2)
        self.check_bad_bucket_name(f"_{bucket_name}")
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_short_one(self):
        self.check_bad_bucket_name("a")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_short_two(self):
        self.check_bad_bucket_name("aa")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_long_60(self):
        self.bucket_create_naming_good_long(60)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_long_61(self):
        self.bucket_create_naming_good_long(61)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_long_62(self):
        self.bucket_create_naming_good_long(62)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_long_63(self):
        self.bucket_create_naming_good_long(63)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_long_64(self):
        self.bucket_create_naming_bad_long(64)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_ip(self):
        self.check_bad_bucket_name("192.168.11.123")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_underscore(self):
        self.check_bad_bucket_name("foo_bar")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_long(self):
        self.check_good_bucket_name(utils.random_bucket_name(""))

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_dash_at_end(self):
        self.check_bad_bucket_name("foo-")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_dot_dot(self):
        self.check_bad_bucket_name("foo..bar")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_dot_dash(self):
        self.check_bad_bucket_name("foo.-bar")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_dns_dash_dot(self):
        self.check_bad_bucket_name("foo-.bar")

    @pytest.mark.tag("Duplicate")
    def test_bucket_create_exists(self):
        bucket_name = self.get_new_bucket_name(17)
        client = self.get_client()
        self._do_create_bucket(client, bucket_name)
        with pytest.raises(ClientError) as exc_info:
            self._do_create_bucket(client, bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_ALREADY_OWNED_BY_YOU

    @pytest.mark.tag("Duplicate")
    def test_bucket_create_exists_nonowner(self):
        bucket_name = self.get_new_bucket_name(18)
        client = self.get_client()
        alt_client = self.get_alt_client()
        self._do_create_bucket(client, bucket_name)
        with pytest.raises(ClientError) as exc_info:
            self._do_create_bucket(alt_client, bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_ALREADY_EXISTS

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_starts_alpha(self):
        self.check_good_bucket_name("foo", "a" + self.get_prefix())

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_starts_digit(self):
        self.check_good_bucket_name("foo", "0" + self.get_prefix())

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_contains_period(self):
        self.check_good_bucket_name("aaa.111", None)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_contains_hyphen(self):
        self.check_good_bucket_name("aaa-111", None)

    @pytest.mark.tag("Duplicate")
    def test_bucket_recreate_not_overriding(self):
        keys = ["my_key1", "my_key2"]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 23, keys)
        objects = self.get_object_list(client, bucket_name, None)
        assert objects == keys

        with pytest.raises(ClientError):
            client.create_bucket(Bucket=bucket_name)

        objects = self.get_object_list(client, bucket_name, None)
        assert objects == keys

    @pytest.mark.tag("location")
    def test_get_bucket_location(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        client.get_bucket_location(Bucket=bucket_name)

    def _assert_create_error(self, bucket_name: str, status_code: int, error_code: str, **extra) -> None:
        client = self.get_client()
        self.assert_client_error(
            lambda: self._do_create_bucket(client, bucket_name, **extra), status_code, error_code
        )

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_uppercase(self):
        self._assert_create_error(self.get_new_bucket_name_only(25).upper(), 400, md.INVALID_BUCKET_NAME)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_punycode_prefix(self):
        # xn-- 로 시작하는 이름은 쓸 수 없다
        bucket_name = ("xn--" + self.get_new_bucket_name_only(26))[:63].rstrip("-.")
        self._assert_create_error(bucket_name, 400, md.INVALID_BUCKET_NAME)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_dot_at_edge(self):
        bucket_name = self.get_new_bucket_name_only(27)[:60].rstrip("-.")
        self._assert_create_error("." + bucket_name, 400, md.INVALID_BUCKET_NAME)
        self._assert_create_error(bucket_name + ".", 400, md.INVALID_BUCKET_NAME)

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_reserved(self):
        bucket_name = self.get_new_bucket_name_only(28)[:45].rstrip("-.")
        # 예약된 접두어·접미어가 붙은 이름은 만들 수 없다
        for prefix in ("sthree-", "amzn-s3-demo-"):
            self._assert_create_error(prefix + bucket_name, 403, md.ACCESS_DENIED)
        for suffix in ("-s3alias", "--ol-s3", ".mrap"):
            self._assert_create_error(bucket_name + suffix, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("location")
    def test_bucket_create_invalid_location(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(29)
        with pytest.raises(ClientError) as exc_info:
            client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "invalid-region-1"}
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        # us-east-1 엔드포인트는 InvalidLocationConstraint, 그 외 리전은 IllegalLocationConstraintException
        assert exc_info.value.response["Error"]["Code"] in (
            md.ILLEGAL_LOCATION_CONSTRAINT_EXCEPTION,
            md.INVALID_LOCATION_CONSTRAINT,
        )
        self.assert_client_error(lambda: client.get_bucket_location(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("location")
    def test_get_bucket_location_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(30)
        self.assert_client_error(lambda: client.get_bucket_location(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("location")
    def test_get_bucket_location_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 31)
        self.assert_client_error(lambda: alt_client.get_bucket_location(Bucket=bucket_name), 403, md.ACCESS_DENIED)

    @pytest.mark.tag("ACL")
    def test_bucket_create_acl_with_grant(self):
        # 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
        self._assert_create_error(
            self.get_new_bucket_name_only(32),
            400,
            md.INVALID_REQUEST,
            ACL="private",
            GrantRead=f'uri="{md.ALL_USERS}"',
        )

    @pytest.mark.tag("ACL")
    def test_bucket_create_invalid_acl(self):
        self._assert_create_error(self.get_new_bucket_name_only(33), 400, md.INVALID_ARGUMENT, ACL="invalid")
