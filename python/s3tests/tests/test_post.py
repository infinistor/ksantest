"""Post tests ported from Java testV2/Post.java."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Dict, List, Optional

import pytest

from s3tests.auth.aws2_signer import get_base64_encoded_sha1_hash
from s3tests.auth.aws4_signer_base import get_amz_date, get_post_policy_signature
from s3tests.data import main_data as md
from s3tests.data.form_file import FormFile
from s3tests.test_base import S3TestBase
from s3tests.utils import net_utils, utils


class TestPost(S3TestBase):
    def _encode_policy(self, policy_document: Dict[str, Any]) -> str:
        return base64.b64encode(json.dumps(policy_document).encode("utf-8")).decode("ascii")

    def _sigv2_signature(self, policy: str) -> str:
        return get_base64_encoded_sha1_hash(policy, self.config.main_user.secret_key)

    def _region_name(self) -> str:
        return self.config.region_name if self.config.region_name else "us-east-1"

    def _post_sigv2(
        self,
        bucket_name: str,
        policy_document: Dict[str, Any],
        payload: Dict[str, str],
        file_data: FormFile,
        url_bucket: Optional[str] = None,
    ):
        policy = self._encode_policy(policy_document)
        payload["AWSAccessKeyId"] = self.config.main_user.access_key
        payload["signature"] = self._sigv2_signature(policy)
        payload["policy"] = policy
        return net_utils.post_upload(self.create_url(url_bucket or bucket_name), payload, file_data)

    def _post_sigv4(
        self,
        bucket_name: str,
        policy_document: Dict[str, Any],
        payload: Dict[str, str],
        file_data: FormFile,
        url_bucket: Optional[str] = None,
    ):
        policy = self._encode_policy(policy_document)
        amz_date = get_amz_date()
        date_stamp = amz_date[:8]
        region = self._region_name()
        credential = f"{self.config.main_user.access_key}/{date_stamp}/{region}/s3/aws4_request"
        signature = get_post_policy_signature(self.config.main_user.secret_key, date_stamp, region, policy)
        payload["policy"] = policy
        payload["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
        payload["x-amz-credential"] = credential
        payload["x-amz-date"] = amz_date
        payload["x-amz-signature"] = signature
        return net_utils.post_upload(self.create_url(url_bucket or bucket_name), payload, file_data)

    def _build_sigv2_conditions(
        self,
        bucket_name: str,
        content_type: str,
        key_prefix: str = "foo",
        min_size: int = 0,
        max_size: int = 1024,
        extra_conditions: Optional[List[Any]] = None,
    ) -> List[Any]:
        conditions: List[Any] = [
            {"bucket": bucket_name},
            ["starts-with", "$key", key_prefix],
            {"acl": "private"},
            ["starts-with", "$Content-Type", content_type],
            ["content-length-range", min_size, max_size],
        ]
        if extra_conditions:
            conditions.extend(extra_conditions)
        return conditions

    def _build_sigv4_conditions(
        self,
        bucket_name: str,
        content_type: str,
        key_prefix: str = "foo",
        min_size: int = 0,
        max_size: int = 1024,
        extra_conditions: Optional[List[Any]] = None,
    ) -> List[Any]:
        amz_date = get_amz_date()
        date_stamp = amz_date[:8]
        region = self._region_name()
        credential = f"{self.config.main_user.access_key}/{date_stamp}/{region}/s3/aws4_request"
        conditions = self._build_sigv2_conditions(bucket_name, content_type, key_prefix, min_size, max_size, extra_conditions)
        conditions.extend(
            [
                {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                {"x-amz-credential": credential},
                {"x-amz-date": amz_date},
            ]
        )
        return conditions

    @pytest.mark.tag("Upload")
    def test_post_object_anonymous_request(self):
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 1, "public-read-write")

        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "public-read",
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("Upload")
    def test_post_object_authenticated_request(self):
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv4_conditions(bucket_name, content_type),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "private",
            "Content-Type": content_type,
        }
        result = self._post_sigv4(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("Upload")
    def test_post_object_authenticated_no_content_type(self):
        self.skip_if_aws()
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 3, "public-read-write")

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["content-length-range", 0, 1024],
            ],
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private"}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("ERROR")
    def test_post_object_authenticated_request_bad_access_key(self):
        self.skip_if_aws()
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 4, "public-read-write")

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type),
        }
        policy = self._encode_policy(policy_document)
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "AWSAccessKeyId": "foo",
            "acl": "private",
            "signature": self._sigv2_signature(policy),
            "policy": policy,
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("StatusCode")
    def test_post_object_set_success_code(self):
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 5, "public-read-write")

        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "public-read",
            "Content-Type": content_type,
            "success_action_status": "201",
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 201, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("StatusCode")
    def test_post_object_set_invalid_success_code(self):
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 6, "public-read-write")

        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "public-read",
            "Content-Type": content_type,
            "success_action_status": "404",
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("Upload")
    def test_post_object_upload_larger_than_chunk(self):
        self.skip_if_aws()
        key = "foo.txt"
        content_type = "text/plain"
        size = 5 * 1024 * 1024
        data = utils.random_text_to_long(size)
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, min_size=0, max_size=size),
        }
        file_data = FormFile(key, content_type, data)
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == data, md.NOT_MATCHED

    @pytest.mark.tag("Upload")
    def test_post_object_set_key_from_filename(self):
        self.skip_if_aws()
        content_type = "text/plain"
        key = "foo.txt"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("Upload")
    def test_post_object_ignored_header(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(9)
        content_type = "text/plain"
        key = "foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "private",
            "x-ignore-foo": "bar",
            "Content-Type": content_type,
        }
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

    @pytest.mark.tag("Upload")
    def test_post_object_case_insensitive_condition_fields(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(10)
        content_type = "text/plain"
        key = "foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bUcKeT": bucket_name},
                ["StArTs-WiTh", "$KeY", "foo"],
                {"AcL": "private"},
                ["StArTs-WiTh", "$CoNtEnT-TyPe", content_type],
                ["content-length-range", 0, 1024],
            ],
        }
        policy = self._encode_policy(policy_document)
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "kEy": key,
            "AWSAccessKeyId": self.config.main_user.access_key,
            "aCl": "private",
            "signature": self._sigv2_signature(policy),
            "pOLICy": policy,
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 204, result.get_error_code()

    @pytest.mark.tag("Upload")
    def test_post_object_escaped_field_values(self):
        self.skip_if_aws()
        key = "\\$foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("Upload")
    def test_post_object_success_redirect_action(self):
        self.skip_if_aws()
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 12, "public-read-write")
        redirect_url = self.create_url(bucket_name)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name,
                content_type,
                extra_conditions=[["eq", "$successActionRedirect", redirect_url]],
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "private",
            "Content-Type": content_type,
            "successActionRedirect": redirect_url,
        }
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 200, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        etag = response["ETag"].replace('"', "")
        expected_url = f"{redirect_url}?bucket={bucket_name}&key={key}&etag=%22{etag}%22"
        assert result.url == expected_url

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_signature(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(13)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        policy = self._encode_policy(policy_document)
        signature = self._sigv2_signature(policy)
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "AWSAccessKeyId": self.config.main_user.access_key,
            "acl": "private",
            "signature": signature[:-1],
            "policy": policy,
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_access_key(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(14)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        policy = self._encode_policy(policy_document)
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "AWSAccessKeyId": self.config.main_user.access_key[:-1],
            "acl": "private",
            "signature": self._sigv2_signature(policy),
            "policy": policy,
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_date_format(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(15)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100).replace("T", " "),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_no_key_specified(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(16)
        content_type = "text/plain"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", 0, 1024],
            ],
        }
        file_data = FormFile("", content_type, "bar")
        payload = {"acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_signature(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(17)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        policy = self._encode_policy(policy_document)
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "AWSAccessKeyId": self.config.main_user.access_key,
            "acl": "private",
            "policy": policy,
            "Content-Type": content_type,
        }
        result = net_utils.post_upload(self.create_url(bucket_name), payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_policy_condition(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(18)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                ["starts-with", "$key", "\\$foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", 0, 1024],
            ],
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 403, result.message

    @pytest.mark.tag("Metadata")
    def test_post_object_user_specified_header(self):
        self.skip_if_aws()
        key = "foo.txt"
        content_type = "text/plain"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name,
                content_type,
                extra_conditions=[["starts-with", "$x-amz-meta-foo", "bar"]],
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "private",
            "x-amz-meta-foo": "bar-clamp",
            "Content-Type": content_type,
        }
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 204, result.get_error_code()

        response = client.get_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"]["foo"] == "bar-clamp"

    @pytest.mark.tag("ERROR")
    def test_post_object_request_missing_policy_specified_field(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(20)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name,
                content_type,
                key_prefix="\\$foo",
                extra_conditions=[["starts-with", "$x-amz-meta-foo", "bar"]],
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_condition_is_case_sensitive(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(21)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "CONDITIONS": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_expires_is_case_sensitive(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(22)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "EXPIRATION": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_expired_policy(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(23)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(-100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_request_field_value(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(24)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name,
                content_type,
                key_prefix="\\$foo",
                extra_conditions=[["eq", "$x-amz-meta-foo", ""]],
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "acl": "private",
            "x-amz-meta-foo": "bar-clamp",
            "Content-Type": content_type,
        }
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_expires_condition(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(25)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo"),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_conditions_list(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(26)
        content_type = "text/plain"
        key = "foo.txt"

        policy_document = {"expiration": self.get_time_to_add_minutes(100)}
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_upload_size_limit_exceeded(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(27)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo", min_size=0, max_size=0),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_content_length_argument(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(28)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "\\$foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", 0],
            ],
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_content_length_argument(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(29)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "\\$foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["content-length-range", -1, 0],
            ],
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_upload_size_below_minimum(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(30)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(bucket_name, content_type, key_prefix="\\$foo", min_size=512, max_size=1024),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_empty_conditions(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket(31)
        content_type = "text/plain"
        key = "foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [],
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("PresignedURL")
    def test_presigned_url_put_get(self):
        bucket_name = self.create_bucket(32)
        key = "test_presigned_url_put_get"

        put_url = self.generate_presigned_put_url(bucket_name, key)
        assert self.put_object_url(put_url, key) == 200

        get_url = self.generate_presigned_get_url(bucket_name, key)
        assert self.get_object_url(get_url) == 200

    @pytest.mark.tag("PresignedURL")
    def test_presigned_url_put_signed_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 37)
        key = "test_presigned_url_put_signed_acl"

        put_url = self.generate_presigned_put_url(bucket_name, key, acl="public-read")
        result = net_utils.put_upload(put_url, "PUT", {"x-amz-acl": "public-read"}, key)
        assert result.status_code == 200, result.message

    @pytest.mark.tag("PresignedURL")
    def test_presigned_url_put_unsigned_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 38)
        key = "test_presigned_url_put_unsigned_acl"

        put_url = self.generate_presigned_put_url(bucket_name, key)
        result = net_utils.put_upload(put_url, "PUT", {"x-amz-acl": "public-read"}, key)
        assert result.status_code == 403, result.message
        assert md.ACCESS_DENIED in result.message, result.message

        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key=key), 404, "404")

    @pytest.mark.tag("PresignedURL")
    def test_presigned_url_put_unsigned_copy_source(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 39)
        source_key = "source"
        target_key = "target"
        client.put_object(Bucket=bucket_name, Key=source_key, Body=source_key.encode("utf-8"))

        put_url = self.generate_presigned_put_url(bucket_name, target_key)
        # 본문이 없으면 서명 검사 전에 MissingContentLength로 막는 구현이 있어 빈 본문을 보낸다.
        headers = {"x-amz-copy-source": f"{bucket_name}/{source_key}"}
        result = net_utils.put_upload(put_url, "PUT", headers, "")
        assert result.status_code == 403, result.message
        assert md.ACCESS_DENIED in result.message, result.message

        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key=target_key), 404, "404")

    @pytest.mark.tag("signV4")
    def test_put_object_v4(self):
        bucket_name = self.create_bucket(33)
        key = "test_put_object_v4"
        content = utils.random_text_to_long(100)
        result = self.put_object_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()

    @pytest.mark.tag("signV4")
    def test_put_object_chunked_v4(self):
        bucket_name = self.create_bucket(34)
        key = "test_put_object_chunked_v4"
        content = utils.random_text_to_long(100)
        result = self.put_object_chunked_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()

    @pytest.mark.tag("signV4")
    def test_get_object_v4(self):
        key = "test_get_object_v4"
        size = 100
        client = self.get_client()
        bucket_name = self.create_bucket(client, 35)
        content = utils.random_text_to_long(size)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        result = self.get_object_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()
        assert len(result.get_content()) == size
        assert result.get_content() == content

    @pytest.mark.tag("ERROR")
    def test_post_object_wrong_bucket(self):
        bucket_name = self.get_new_bucket_name(36)
        bad_bucket_name = self.get_new_bucket_name(36)
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv4_conditions(
                bucket_name,
                content_type,
                key_prefix="\\$foo",
                min_size=512,
                max_size=1024,
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {
            "key": key,
            "bucket": bucket_name,
            "acl": "private",
            "Content-Type": content_type,
        }
        result = self._post_sigv4(bucket_name, policy_document, payload, file_data, url_bucket=bad_bucket_name)
        assert result.status_code == 404, result.get_error_code()

    # region SigV4 POST (AWS 실측 기준)

    def _post_form(
        self,
        bucket_name: str,
        fields: Dict[str, str],
        conditions: Optional[List[Any]] = None,
        body: str = "bar",
        user: Any = None,
        policy: Any = None,
        mutate: Any = None,
    ):
        """SigV4 로 서명한 폼 업로드. 날짜·자격 증명은 정책과 폼에 같은 값을 쓴다.

        policy: None 이면 bucket, key(아무 값), x-amz-* 조건에 conditions 를 더한 정책을 만든다.
                함수면 (기본 조건 목록)을 받아 정책 문서를 돌려준다. 문자열이면 그대로 policy 필드에 넣는다.
        mutate: 서명까지 끝난 폼 필드를 고치는 함수.
        """
        user = user or self.config.main_user
        amz_date = get_amz_date()
        date_stamp = amz_date[:8]
        region = self._region_name()
        credential = f"{user.access_key}/{date_stamp}/{region}/s3/aws4_request"
        base_conditions: List[Any] = [
            {"bucket": bucket_name},
            ["starts-with", "$key", ""],
            {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
            {"x-amz-credential": credential},
            {"x-amz-date": amz_date},
        ]
        if isinstance(policy, str):
            encoded = policy
        else:
            if policy is None:
                document = {
                    "expiration": self.get_time_to_add_minutes(100),
                    "conditions": base_conditions + list(conditions or []),
                }
            else:
                document = policy(base_conditions)
            encoded = self._encode_policy(document)
        payload = dict(fields)
        payload["policy"] = encoded
        payload["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
        payload["x-amz-credential"] = credential
        payload["x-amz-date"] = amz_date
        payload["x-amz-signature"] = get_post_policy_signature(user.secret_key, date_stamp, region, encoded)
        if mutate:
            mutate(payload)
        return net_utils.post_upload(self.create_url(bucket_name), payload, FormFile("foo.txt", "text/plain", body))

    @staticmethod
    def _error_code(result) -> str:
        start = result.message.find("<Code>")
        end = result.message.find("</Code>")
        return result.message[start + 6 : end] if start != -1 and end != -1 else ""

    def _assert_post_error(self, result, status_code: int, error_code: str) -> None:
        assert result.status_code == status_code, result.message
        assert self._error_code(result) == error_code, result.message

    def _assert_post_success(self, result) -> None:
        assert result.status_code == 204, result.message

    @pytest.mark.tag("Upload")
    def test_post_object_v4_stored_fields(self):
        key = "test_post_object_v4_stored_fields"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 37)
        fields = {
            "key": key,
            "Content-Type": "image/png",
            "Cache-Control": "max-age=60",
            "Content-Disposition": "attachment; filename=a.txt",
            "Content-Encoding": "identity",
            "x-amz-meta-foo": "bar",
            "x-amz-server-side-encryption": "AES256",
            "x-amz-website-redirect-location": "/other.txt",
            "tagging": "<Tagging><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag></TagSet></Tagging>",
        }
        conditions = [{name: value} for name, value in fields.items() if name != "key"]
        self._assert_post_success(self._post_form(bucket_name, fields, conditions))

        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ContentLength"] == 3
        assert response["ContentType"] == "image/png"
        assert response["CacheControl"] == "max-age=60"
        assert response["ContentDisposition"] == "attachment; filename=a.txt"
        assert response["ContentEncoding"] == "identity"
        assert response["Metadata"] == {"foo": "bar"}
        assert response["ServerSideEncryption"] == "AES256"
        assert response["WebsiteRedirectLocation"] == "/other.txt"
        assert client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"] == [{"Key": "k1", "Value": "v1"}]

    @pytest.mark.tag("Upload")
    def test_post_object_v4_overwrite(self):
        key = "test_post_object_v4_overwrite"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 38)
        self._assert_post_success(self._post_form(bucket_name, {"key": key}, body="first"))
        self._assert_post_success(self._post_form(bucket_name, {"key": key}, body="second"))
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "second"
        # 빈 파일도 올릴 수 있다
        self._assert_post_success(self._post_form(bucket_name, {"key": "empty"}, body=""))
        assert client.head_object(Bucket=bucket_name, Key="empty")["ContentLength"] == 0

    @pytest.mark.tag("Versioning")
    def test_post_object_v4_versioning(self):
        key = "test_post_object_v4_versioning"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 39)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self._assert_post_success(self._post_form(bucket_name, {"key": key}, body="first"))
        self._assert_post_success(self._post_form(bucket_name, {"key": key}, body="second"))
        versions = client.list_object_versions(Bucket=bucket_name)["Versions"]
        assert len(versions) == 2
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "second"

    @pytest.mark.tag("Policy")
    def test_post_object_v4_condition_matched(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 40)
        # 접두어가 맞는 키
        self._assert_post_success(
            self._post_form(bucket_name, {"key": "user/a.txt"}, [["starts-with", "$key", "user/"]])
        )
        # 빈 접두어는 아무 값이나 받는다
        self._assert_post_success(
            self._post_form(
                bucket_name, {"key": "any", "x-amz-meta-foo": "anything"}, [["starts-with", "$x-amz-meta-foo", ""]]
            )
        )
        # 조건의 필드 이름은 대소문자를 구분하지 않는다
        self._assert_post_success(
            self._post_form(
                bucket_name, {"key": "case", "x-amz-meta-foo": "value"}, [["starts-with", "$X-Amz-Meta-Foo", ""]]
            )
        )
        # x-ignore- 로 시작하는 필드는 조건이 없어도 된다
        self._assert_post_success(self._post_form(bucket_name, {"key": "ignore", "x-ignore-foo": "bar"}))
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 4

    @pytest.mark.tag("Policy")
    def test_post_object_v4_condition_failed(self):
        key = "test_post_object_v4_condition_failed"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 41)
        cases = [
            # (폼 필드, 추가 조건)
            ({"key": "other/a.txt"}, [["starts-with", "$key", "user/"]]),
            ({"key": key, "Content-Type": "text/html"}, [["eq", "$Content-Type", "text/plain"]]),
            ({"key": key, "x-amz-meta-foo": "2"}, [{"x-amz-meta-foo": "1"}]),
            # eq 는 값의 대소문자를 구분한다
            ({"key": key, "x-amz-meta-foo": "ABC"}, [["eq", "$x-amz-meta-foo", "abc"]]),
            # 정책에 있는 필드가 폼에 없다
            ({"key": key}, [{"x-amz-meta-foo": "bar"}]),
            # 폼에 있는 필드가 정책에 없다
            ({"key": key, "x-amz-meta-foo": "bar"}, []),
        ]
        for fields, conditions in cases:
            self._assert_post_error(self._post_form(bucket_name, fields, conditions), 403, md.ACCESS_DENIED)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Policy")
    def test_post_object_v4_required_conditions(self):
        key = "test_post_object_v4_required_conditions"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 42)
        expiration = self.get_time_to_add_minutes(100)

        def without(index: int):
            return lambda base: {"expiration": expiration, "conditions": base[:index] + base[index + 1 :]}

        # bucket, key, x-amz-algorithm, x-amz-credential, x-amz-date 조건은 모두 있어야 한다
        for index in range(5):
            self._assert_post_error(
                self._post_form(bucket_name, {"key": key}, policy=without(index)), 403, md.ACCESS_DENIED
            )
        # 다른 버킷 이름을 적은 정책
        other = lambda base: {"expiration": expiration, "conditions": [{"bucket": "other-bucket-name"}] + base[1:]}  # noqa: E731
        self._assert_post_error(self._post_form(bucket_name, {"key": key}, policy=other), 403, md.ACCESS_DENIED)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Policy")
    def test_post_object_v4_expired_policy(self):
        key = "test_post_object_v4_expired_policy"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 43)
        expired = lambda base: {"expiration": self.get_time_to_add_minutes(-10), "conditions": base}  # noqa: E731
        self._assert_post_error(self._post_form(bucket_name, {"key": key}, policy=expired), 403, md.ACCESS_DENIED)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Policy")
    def test_post_object_v4_invalid_policy_document(self):
        key = "test_post_object_v4_invalid_policy_document"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 44)
        expiration = self.get_time_to_add_minutes(100)
        policies = [
            "!!!not-base64!!!",
            base64.b64encode(b"not json").decode("ascii"),
            lambda base: {"conditions": base},  # expiration 누락
            lambda base: {"expiration": expiration},  # conditions 누락
            lambda base: {"expiration": expiration, "conditions": []},
            lambda base: {"expiration": "2030-01-01 00:00:00", "conditions": base},  # ISO8601 이 아니다
            lambda base: {"expiration": expiration, "conditions": base, "unknown": "value"},
            lambda base: {"EXPIRATION": expiration, "CONDITIONS": base},  # 이름은 소문자여야 한다
            # 조건 형식이 잘못됐다
            lambda base: {"expiration": expiration, "conditions": base + [["ends-with", "$key", "txt"]]},
            lambda base: {"expiration": expiration, "conditions": base + [["eq", "$key"]]},
            lambda base: {"expiration": expiration, "conditions": base + [["content-length-range", 0]]},
        ]
        for policy in policies:
            self._assert_post_error(
                self._post_form(bucket_name, {"key": key}, policy=policy), 400, md.INVALID_POLICY_DOCUMENT
            )
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Policy")
    def test_post_object_v4_content_length_range(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 45)
        # 본문은 3바이트. 범위는 양 끝을 포함한다
        self._assert_post_success(self._post_form(bucket_name, {"key": "max"}, [["content-length-range", 0, 3]]))
        self._assert_post_success(self._post_form(bucket_name, {"key": "min"}, [["content-length-range", 3, 10]]))
        self._assert_post_error(
            self._post_form(bucket_name, {"key": "large"}, [["content-length-range", 0, 2]]),
            400,
            md.ENTITY_TOO_LARGE,
        )
        self._assert_post_error(
            self._post_form(bucket_name, {"key": "small"}, [["content-length-range", 4, 10]]),
            400,
            md.ENTITY_TOO_SMALL,
        )
        assert [c["Key"] for c in client.list_objects_v2(Bucket=bucket_name)["Contents"]] == ["max", "min"]

    @pytest.mark.tag("Auth")
    def test_post_object_v4_invalid_signature(self):
        key = "test_post_object_v4_invalid_signature"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 46)

        def set_field(name: str, value: str):
            return lambda payload: payload.__setitem__(name, value)

        def bad_access_key(payload: Dict[str, str]) -> None:
            scope = payload["x-amz-credential"].split("/", 1)[1]
            payload["x-amz-credential"] = f"AKIAXXXXXXXXXXXXXXXX/{scope}"

        self._assert_post_error(
            self._post_form(bucket_name, {"key": key}, mutate=set_field("x-amz-signature", "0" * 64)),
            403,
            md.SIGNATURE_DOES_NOT_MATCH,
        )
        self._assert_post_error(
            self._post_form(bucket_name, {"key": key}, mutate=bad_access_key), 403, md.INVALID_ACCESS_KEY_ID
        )
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Auth")
    def test_post_object_v4_invalid_auth_field(self):
        key = "test_post_object_v4_invalid_auth_field"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 47)

        def remove(name: str):
            return lambda payload: payload.pop(name)

        def set_field(name: str, value: str):
            return lambda payload: payload.__setitem__(name, value)

        def wrong_service(payload: Dict[str, str]) -> None:
            payload["x-amz-credential"] = payload["x-amz-credential"].replace("/s3/aws4_request", "/ec2/aws4_request")

        mutations = [
            # 서명 필드가 하나라도 없다
            remove("x-amz-signature"),
            remove("x-amz-date"),
            remove("x-amz-algorithm"),
            remove("x-amz-credential"),
            # 값이 잘못됐다
            set_field("x-amz-algorithm", "AWS4-HMAC-SHA1"),
            set_field("x-amz-date", "2030-01-01"),
            set_field("x-amz-credential", "malformed"),
            wrong_service,
        ]
        for mutate in mutations:
            self._assert_post_error(
                self._post_form(bucket_name, {"key": key}, mutate=mutate), 400, md.INVALID_ARGUMENT
            )
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Access")
    def test_post_object_v4_access_denied(self):
        key = "test_post_object_v4_access_denied"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 48)
        # 쓰기 권한이 없는 사용자가 서명한 폼
        self._assert_post_error(
            self._post_form(bucket_name, {"key": key}, user=self.config.alt_user), 403, md.ACCESS_DENIED
        )
        # 서명 없는 폼
        result = net_utils.post_upload(self.create_url(bucket_name), {"key": key}, FormFile("foo.txt", "text/plain", "bar"))
        self._assert_post_error(result, 403, md.ACCESS_DENIED)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("ERROR")
    def test_post_object_v4_invalid_key(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 49)
        self._assert_post_error(self._post_form(bucket_name, {}), 400, md.INVALID_ARGUMENT)
        self._assert_post_error(self._post_form(bucket_name, {"key": ""}), 400, md.INVALID_ARGUMENT)
        # 키는 1024바이트까지
        self._assert_post_error(self._post_form(bucket_name, {"key": "k" * 1025}), 400, md.KEY_TOO_LONG)
        self._assert_post_success(self._post_form(bucket_name, {"key": "k" * 1024}))
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 1

    @pytest.mark.tag("ERROR")
    def test_post_object_v4_invalid_field_value(self):
        key = "test_post_object_v4_invalid_field_value"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 50)
        duplicated_tag = (
            "<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>"
            "<Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>"
        )
        cases = [
            ("acl", "invalid", md.INVALID_ARGUMENT),
            ("x-amz-server-side-encryption", "AES128", md.INVALID_ARGUMENT),
            ("tagging", "<Tagging>", md.MALFORMED_XML),
            ("tagging", duplicated_tag, md.INVALID_TAG),
            ("x-amz-website-redirect-location", "invalid", md.INVALID_REDIRECT_LOCATION),
            ("x-amz-meta-big", "v" * 2100, md.METADATA_TOO_LARGE),
        ]
        for name, value, error_code in cases:
            result = self._post_form(bucket_name, {"key": key, name: value}, [["starts-with", f"${name}", ""]])
            self._assert_post_error(result, 400, error_code)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Checksum")
    def test_post_object_v4_content_md5(self):
        key = "test_post_object_v4_content_md5"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 51)

        def post(md5: str):
            return self._post_form(bucket_name, {"key": key, "Content-MD5": md5}, [{"Content-MD5": md5}])

        self._assert_post_error(post(utils.get_md5("other")), 400, md.BAD_DIGEST)
        self._assert_post_error(post("invalid"), 400, md.INVALID_DIGEST)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0
        self._assert_post_success(post(utils.get_md5("bar")))
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "bar"

    @pytest.mark.tag("Checksum")
    def test_post_object_v4_checksum(self):
        key = "test_post_object_v4_checksum"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 52)

        def post(checksum: str):
            fields = {"key": key, "x-amz-checksum-algorithm": "SHA256", "x-amz-checksum-sha256": checksum}
            return self._post_form(bucket_name, fields, [{name: value} for name, value in fields.items() if name != "key"])

        def sha256(value: bytes) -> str:
            return base64.b64encode(hashlib.sha256(value).digest()).decode("ascii")

        self._assert_post_error(post(sha256(b"other")), 400, md.BAD_DIGEST)
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0
        self._assert_post_success(post(sha256(b"bar")))
        response = client.head_object(Bucket=bucket_name, Key=key, ChecksumMode="ENABLED")
        assert response["ChecksumSHA256"] == sha256(b"bar")

    @pytest.mark.tag("ACL")
    def test_post_object_v4_acl(self):
        client = self.get_client()
        # ACL 을 쓰는 버킷: acl 필드가 오브젝트 ACL 이 된다
        bucket_name = self.create_bucket_canned_acl(client, 53)
        self._assert_post_success(
            self._post_form(bucket_name, {"key": "public", "acl": "public-read"}, [{"acl": "public-read"}])
        )
        grants = client.get_object_acl(Bucket=bucket_name, Key="public")["Grants"]
        assert sorted((g["Grantee"].get("URI", ""), g["Permission"]) for g in grants) == [
            ("", "FULL_CONTROL"),
            (md.ALL_USERS, "READ"),
        ]

        # ACL 을 끈 버킷: 소유자 전권을 뜻하는 값만 받는다
        enforced = self.create_bucket(client, 53, "BucketOwnerEnforced")
        for acl in ("private", "bucket-owner-full-control"):
            self._assert_post_success(self._post_form(enforced, {"key": acl, "acl": acl}, [{"acl": acl}]))
        self._assert_post_error(
            self._post_form(enforced, {"key": "public", "acl": "public-read"}, [{"acl": "public-read"}]),
            400,
            md.ACCESS_CONTROL_LIST_NOT_SUPPORTED,
        )
        assert client.list_objects_v2(Bucket=enforced)["KeyCount"] == 2

    # endregion
