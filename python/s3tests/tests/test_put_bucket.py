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
        bucket_name = self.create_bucket(client)
        response = client.list_objects(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 0

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_bad_starts_non_alpha(self):
        bucket_name = self.get_new_bucket_name_only()
        self.check_bad_bucket_name(f"_{bucket_name}")

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
        prefix = self.get_prefix()
        add_length = 63 - len(prefix)
        prefix = utils.random_text(add_length)
        self.check_good_bucket_name(prefix, None)

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
        bucket_name = self.get_new_bucket_name()
        client = self.get_client()
        self._do_create_bucket(client, bucket_name)
        with pytest.raises(ClientError) as exc_info:
            self._do_create_bucket(client, bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_ALREADY_OWNED_BY_YOU

    @pytest.mark.tag("Duplicate")
    def test_bucket_create_exists_nonowner(self):
        bucket_name = self.get_new_bucket_name()
        client = self.get_client()
        alt_client = self.get_alt_client()
        self._do_create_bucket(client, bucket_name)
        with pytest.raises(ClientError) as exc_info:
            self._do_create_bucket(alt_client, bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_ALREADY_EXISTS

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_starts_alpha(self):
        self.check_good_bucket_name("foo", f"a{self.get_prefix()}")

    @pytest.mark.tag("CreationRules")
    def test_bucket_create_naming_good_starts_digit(self):
        self.check_good_bucket_name("foo", f"0{self.get_prefix()}")

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
        bucket_name = self.create_objects_keys(client, keys)
        objects = self.get_object_list(client, bucket_name, None)
        assert objects == keys

        with pytest.raises(ClientError):
            client.create_bucket(Bucket=bucket_name)

        objects = self.get_object_list(client, bucket_name, None)
        assert objects == keys

    @pytest.mark.tag("location")
    def test_get_bucket_location(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.get_bucket_location(Bucket=bucket_name)
