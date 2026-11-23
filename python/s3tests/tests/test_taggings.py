"""Taggings tests ported from Java testV2/Taggings.java."""

from __future__ import annotations

import base64
import json
from urllib.parse import quote, urlencode

import pytest

from s3tests.auth.aws4_signer_base import get_amz_date, get_post_policy_signature
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
        key = "test_get_obj_tagging"
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
        key = "test_get_obj_head_tagging"
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
        key = "test_put_max_tags"
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
        key = "test_put_excess_tags"
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
        key = "test_put_max_size_tags"
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
        key = "test_put_excess_key_tags"
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
        key = "test_put_excess_val_tags"
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
        key = "test_put_modify_tags"
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
        key = "test_put_delete_tags"
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
        key = "test_put_obj_with_tags"
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

        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        content_type = "text/plain"
        key = "test_post_object_tags_authenticated_request"

        tags = self.make_simple_tag_set(2)
        xml_input_tag_set = (
            "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag>"
            "<Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"
        )

        amz_date = get_amz_date()
        date_stamp = amz_date[:8]
        region = self.config.region_name if self.config.region_name else "us-east-1"
        credential = f"{self.config.main_user.access_key}/{date_stamp}/{region}/s3/aws4_request"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", key],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", 0, 1024],
                ["starts-with", "$tagging", ""],
                {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                {"x-amz-credential": credential},
                {"x-amz-date": amz_date},
            ],
        }
        policy = base64.b64encode(json.dumps(policy_document).encode("utf-8")).decode("ascii")
        signature = get_post_policy_signature(
            self.config.main_user.secret_key, date_stamp, region, policy
        )

        payload = {
            "key": key,
            "acl": "private",
            "policy": policy,
            "tagging": xml_input_tag_set,
            "x-ignore-foo": "bar",
            "Content-Type": content_type,
            "x-amz-algorithm": "AWS4-HMAC-SHA256",
            "x-amz-credential": credential,
            "x-amz-date": amz_date,
            "x-amz-signature": signature,
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
        key = "test_get_obj_non_tagging"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"",
            Tagging="",
        )

    @pytest.mark.tag("PutObject")
    def test_put_obj_with_tags_encoded(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        key = "test_put_obj_with_tags_encoded"

        # 공백, '/', '+', '='는 x-amz-tagging 헤더에서 URL 인코딩되어 전송되므로 서버가 디코딩해서 저장해야 한다
        tags = [{"Key": "k 1", "Value": "a/b+c d"}, {"Key": "k2", "Value": "x=y"}]
        tagging = urlencode([(t["Key"], t["Value"]) for t in tags], quote_via=quote, safe="")

        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), Tagging=tagging)

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(tags, get_response["TagSet"])

    @pytest.mark.tag("Multipart")
    def test_multipart_upload_with_tags_encoded(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        key = "test_multipart_upload_with_tags_encoded"

        tags = [{"Key": "k 1", "Value": "a/b+c d"}, {"Key": "k2", "Value": "x=y"}]
        tagging = urlencode([(t["Key"], t["Value"]) for t in tags], quote_via=quote, safe="")

        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=key, Tagging=tagging)["UploadId"]
        part_response = client.upload_part(
            Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=key.encode("utf-8")
        )
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]},
        )

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(tags, get_response["TagSet"])

    @staticmethod
    def _tagging(*pairs) -> dict:
        return {"TagSet": [{"Key": key, "Value": value} for key, value in pairs]}

    @pytest.mark.tag("Max")
    def test_bucket_tagging_max(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)

        # 버킷 태그는 50개까지 넣을 수 있다
        tag_set = self.make_simple_tag_set(50)
        client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tag_set})
        self.tag_compare(tag_set, client.get_bucket_tagging(Bucket=bucket_name)["TagSet"])

        self.assert_client_error(
            lambda: client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": self.make_simple_tag_set(51)}),
            400,
            md.BAD_REQUEST,
        )
        assert len(client.get_bucket_tagging(Bucket=bucket_name)["TagSet"]) == 50

    @pytest.mark.tag("ERROR")
    def test_bucket_tagging_duplicate_key(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        self.assert_client_error(
            lambda: client.put_bucket_tagging(
                Bucket=bucket_name, Tagging=self._tagging(("key", "value1"), ("key", "value2"))
            ),
            400,
            md.INVALID_TAG,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_tagging_invalid_tag(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        invalid_tags = [
            ("aws:key", "value"),  # aws: 접두어는 시스템 태그 전용
            ("k" * 129, "value"),  # 키 128자 초과
            ("key", "v" * 257),  # 값 256자 초과
            ("key?<>", "value"),  # 허용하지 않는 문자
        ]
        for key, value in invalid_tags:
            self.assert_client_error(
                lambda key=key, value=value: client.put_bucket_tagging(
                    Bucket=bucket_name, Tagging=self._tagging((key, value))
                ),
                400,
                md.INVALID_TAG,
            )
        self.assert_client_error(lambda: client.get_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_TAG_SET)

    @pytest.mark.tag("Check")
    def test_bucket_tagging_empty_value(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        tagging = self._tagging(("key", ""))
        client.put_bucket_tagging(Bucket=bucket_name, Tagging=tagging)
        self.tag_compare(tagging["TagSet"], client.get_bucket_tagging(Bucket=bucket_name)["TagSet"])

    @pytest.mark.tag("Delete")
    def test_bucket_tagging_empty_set(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        client.put_bucket_tagging(Bucket=bucket_name, Tagging=self._tagging(("key", "value")))

        # 빈 태그 목록을 넣으면 태그가 지워진다
        client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": []})
        self.assert_client_error(lambda: client.get_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_TAG_SET)

        # 태그가 없어도 삭제는 성공한다
        response = client.delete_bucket_tagging(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @pytest.mark.tag("ERROR")
    def test_bucket_tagging_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(21)
        self.assert_client_error(lambda: client.get_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.put_bucket_tagging(Bucket=bucket_name, Tagging=self._tagging(("key", "value"))),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(lambda: client.delete_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("ERROR")
    def test_obj_tagging_duplicate_key(self):
        key = "test_obj_tagging_duplicate_key"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=22)
        self.assert_client_error(
            lambda: client.put_object_tagging(
                Bucket=bucket_name, Key=key, Tagging=self._tagging(("key", "value1"), ("key", "value2"))
            ),
            400,
            md.INVALID_TAG,
        )
        assert len(client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"]) == 0

    @pytest.mark.tag("ERROR")
    def test_obj_tagging_invalid_tag(self):
        key = "test_obj_tagging_invalid_tag"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=23)
        invalid_tags = [
            ("aws:key", "value"),  # aws: 접두어는 시스템 태그 전용
            ("key?<>", "value"),  # 허용하지 않는 문자
        ]
        for tag_key, tag_value in invalid_tags:
            self.assert_client_error(
                lambda tag_key=tag_key, tag_value=tag_value: client.put_object_tagging(
                    Bucket=bucket_name, Key=key, Tagging=self._tagging((tag_key, tag_value))
                ),
                400,
                md.INVALID_TAG,
            )
        assert len(client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"]) == 0

    @pytest.mark.tag("Check")
    def test_obj_tagging_special_characters(self):
        key = "test_obj_tagging_special_characters"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=24)
        # 허용 문자(영숫자, 공백, + - = . _ : / @)와 빈 값은 그대로 저장된다
        tagging = self._tagging(("a+b-=._:/@ c", "v+-=._:/@ d"), ("empty", ""))
        client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging)
        self.tag_compare(tagging["TagSet"], client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"])

    @pytest.mark.tag("Delete")
    def test_obj_tagging_empty_set(self):
        key = "test_obj_tagging_empty_set"
        client = self.get_client()
        bucket_name = self.create_key_with_random_content(client, key, 0, test_id=25)
        client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=self._tagging(("key", "value")))

        # 빈 태그 목록을 넣으면 태그가 지워진다
        client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging={"TagSet": []})
        assert len(client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"]) == 0

        # 태그가 없어도 삭제는 성공한다
        response = client.delete_object_tagging(Bucket=bucket_name, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @pytest.mark.tag("ERROR")
    def test_obj_tagging_key_not_exist(self):
        key = "test_obj_tagging_key_not_exist"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        self.assert_client_error(
            lambda: client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=self._tagging(("key", "value"))),
            404,
            md.NO_SUCH_KEY,
        )
        self.assert_client_error(lambda: client.get_object_tagging(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)
        self.assert_client_error(
            lambda: client.delete_object_tagging(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY
        )

    @pytest.mark.tag("Overflow")
    def test_put_obj_with_excess_tags(self):
        key = "test_put_obj_with_excess_tags"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        tagging = "&".join(f"key{i}=value{i}" for i in range(11))
        self.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key, Body=b"", Tagging=tagging), 400, md.BAD_REQUEST
        )
        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key=key), 404, "404")

    @pytest.mark.tag("ERROR")
    def test_put_obj_with_invalid_tags(self):
        key = "test_put_obj_with_invalid_tags"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        # 헤더로 준 태그의 키가 중복되면 InvalidArgument
        self.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key, Body=b"", Tagging="key=value1&key=value2"),
            400,
            md.INVALID_ARGUMENT,
        )
        for tagging in ("aws:key=value", "key%3F=value", "k" * 129 + "=value"):
            self.assert_client_error(
                lambda tagging=tagging: client.put_object(Bucket=bucket_name, Key=key, Body=b"", Tagging=tagging),
                400,
                md.INVALID_TAG,
            )
        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key=key), 404, "404")

    @pytest.mark.tag("Version")
    def test_obj_tagging_version(self):
        key = "test_obj_tagging_version"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version1 = client.put_object(Bucket=bucket_name, Key=key, Body=b"1")["VersionId"]
        version2 = client.put_object(Bucket=bucket_name, Key=key, Body=b"2")["VersionId"]
        tagging = self._tagging(("version", "1"))

        # 버전을 지정하면 그 버전의 태그만 바뀐다
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, VersionId=version1, Tagging=tagging)
        assert response["VersionId"] == version1

        response = client.get_object_tagging(Bucket=bucket_name, Key=key, VersionId=version1)
        assert response["VersionId"] == version1
        self.tag_compare(tagging["TagSet"], response["TagSet"])

        # 버전을 지정하지 않으면 최신 버전이 대상이다
        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert response["VersionId"] == version2
        assert len(response["TagSet"]) == 0

        response = client.delete_object_tagging(Bucket=bucket_name, Key=key, VersionId=version1)
        assert response["VersionId"] == version1
        assert len(client.get_object_tagging(Bucket=bucket_name, Key=key, VersionId=version1)["TagSet"]) == 0

    @pytest.mark.tag("Version")
    def test_obj_tagging_invalid_version(self):
        key = "test_obj_tagging_invalid_version"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=b"1")
        self.assert_client_error(
            lambda: client.get_object_tagging(Bucket=bucket_name, Key=key, VersionId="invalid"),
            400,
            md.INVALID_ARGUMENT,
        )
        self.assert_client_error(
            lambda: client.put_object_tagging(
                Bucket=bucket_name, Key=key, VersionId="invalid", Tagging=self._tagging(("key", "value"))
            ),
            400,
            md.INVALID_ARGUMENT,
        )
        self.assert_client_error(
            lambda: client.delete_object_tagging(Bucket=bucket_name, Key=key, VersionId="invalid"),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Version")
    def test_obj_tagging_delete_marker(self):
        key = "test_obj_tagging_delete_marker"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=b"1")["VersionId"]
        client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=self._tagging(("key", "value")))
        delete_marker = client.delete_object(Bucket=bucket_name, Key=key)["VersionId"]
        tagging = self._tagging(("key", "value2"))

        # 최신 버전이 삭제 마커이면 버전을 지정하든 않든 태그 요청은 405
        for extra in ({}, {"VersionId": delete_marker}):
            self.assert_client_error(
                lambda extra=extra: client.get_object_tagging(Bucket=bucket_name, Key=key, **extra),
                405,
                md.METHOD_NOT_ALLOWED,
            )
            self.assert_client_error(
                lambda extra=extra: client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging, **extra),
                405,
                md.METHOD_NOT_ALLOWED,
            )
            self.assert_client_error(
                lambda extra=extra: client.delete_object_tagging(Bucket=bucket_name, Key=key, **extra),
                405,
                md.METHOD_NOT_ALLOWED,
            )

        # 삭제 마커 아래의 실제 버전은 버전을 지정하면 그대로 조회된다
        response = client.get_object_tagging(Bucket=bucket_name, Key=key, VersionId=version_id)
        self.tag_compare(self._tagging(("key", "value"))["TagSet"], response["TagSet"])
