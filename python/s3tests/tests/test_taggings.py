"""Taggings tests ported from Java testV2/Taggings.java."""

from __future__ import annotations

import base64
import json

import pytest

from s3tests.auth.aws2_signer import get_base64_encoded_sha1_hash
from s3tests.data import main_data as md
from s3tests.data.form_file import FormFile
from s3tests.test_base import S3TestBase
from s3tests.utils import net_utils, utils


class TestTaggings(S3TestBase):
    @pytest.mark.tag("Check")
    def test_set_tagging(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)

        tag_config = {"TagSet": [{"Key": "Hello", "Value": "World"}]}

        self.assert_client_error(
            lambda: client.get_bucket_tagging(Bucket=bucket_name),
            404,
            md.NO_SUCH_TAG_SET,
        )

        client.put_bucket_tagging(Bucket=bucket_name, Tagging=tag_config)

        response = client.get_bucket_tagging(Bucket=bucket_name)
        assert len(response["TagSet"]) == 1
        self.tag_compare(tag_config["TagSet"], response["TagSet"])
        client.delete_bucket_tagging(Bucket=bucket_name)

        self.assert_client_error(
            lambda: client.get_bucket_tagging(Bucket=bucket_name),
            404,
            md.NO_SUCH_TAG_SET,
        )

    @pytest.mark.tag("Check")
    def test_get_obj_tagging(self):
        key = "obj"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=2)
        input_tag_set = self.make_simple_tag_set(2)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, response["TagSet"])

    @pytest.mark.tag("Check")
    def test_get_obj_head_tagging(self):
        key = "obj"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=3)
        count = 2
        input_tag_set = self.make_simple_tag_set(count)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["TagCount"] == count

    @pytest.mark.tag("Max")
    def test_put_max_tags(self):
        key = "obj"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=4)
        input_tag_set = self.make_simple_tag_set(10)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, response["TagSet"])

    @pytest.mark.tag("Overflow")
    def test_put_excess_tags(self):
        key = "test put max tags"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=5)
        input_tag_set = self.make_simple_tag_set(11)

        self.assert_client_error(
            lambda: client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={"TagSet": input_tag_set},
            ),
            400,
            md.BAD_REQUEST,
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(response["TagSet"]) == 0

    @pytest.mark.tag("Max")
    def test_put_max_size_tags(self):
        key = "test put max key size"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=6)
        input_tag_set = self.make_detail_tag_set(10, 128, 256)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, response["TagSet"])

    @pytest.mark.tag("Overflow")
    def test_put_excess_key_tags(self):
        key = "test put excess key tags"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=7)
        input_tag_set = self.make_detail_tag_set(10, 129, 256)

        self.assert_client_error(
            lambda: client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={"TagSet": input_tag_set},
            ),
            400,
            md.INVALID_TAG,
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(response["TagSet"]) == 0

    @pytest.mark.tag("Overflow")
    def test_put_excess_val_tags(self):
        key = "test put excess value tags"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=8)
        input_tag_set = self.make_detail_tag_set(10, 128, 259)

        self.assert_client_error(
            lambda: client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={"TagSet": input_tag_set},
            ),
            400,
            md.INVALID_TAG,
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(response["TagSet"]) == 0

    @pytest.mark.tag("Overwrite")
    def test_put_modify_tags(self):
        key = "test put modify tags"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=9)
        input_tag_set = self.make_simple_tag_set(2)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, response["TagSet"])

        input_tag_set2 = self.make_detail_tag_set(1, 128, 128)
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set2},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set2, response["TagSet"])

    @pytest.mark.tag("Delete")
    def test_put_delete_tags(self):
        key = "test delete tags"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=10)
        input_tag_set = self.make_simple_tag_set(2)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, response["TagSet"])

        client.delete_object_tagging(Bucket=bucket_name, Key=key)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(response["TagSet"]) == 0

    @pytest.mark.tag("PutObject")
    def test_put_obj_with_tags(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        key = "test tag obj1"
        data = utils.random_text_to_long(100)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            Tagging="foo=bar&bar=",
        )
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == data

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(
            [{"Key": "bar", "Value": ""}, {"Key": "foo", "Value": "bar"}],
            get_response["TagSet"],
        )

    @pytest.mark.tag("Post")
    def test_post_object_tags_authenticated_request(self):
        if self.config.is_aws():
            pytest.skip("Post object tagging test is disabled on AWS")

        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        content_type = "text/plain"
        key = "foo.txt"

        tags = self.make_simple_tag_set(2)
        xml_input_tag_set = (
            "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag>"
            "<Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"
        )

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", 0, 1024],
                ["starts-with", "$tagging", ""],
            ],
        }
        policy = base64.b64encode(json.dumps(policy_document).encode("utf-8")).decode("ascii")
        signature = get_base64_encoded_sha1_hash(policy, self.config.main_user.secret_key)

        payload = {
            "key": key,
            "AWSAccessKeyId": self.config.main_user.access_key,
            "acl": "private",
            "signature": signature,
            "policy": policy,
            "tagging": xml_input_tag_set,
            "x-ignore-foo": "bar",
            "Content-Type": content_type,
        }
        file_data = FormFile(key, content_type, "bar")

        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(tags, get_response["TagSet"])

    @pytest.mark.tag("Check")
    def test_get_obj_non_tagging(self):
        key = "obj"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"",
            Tagging="",
        )
