"""Post tests ported from Java testV2/Post.java."""

from __future__ import annotations

import base64
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
        signature = get_post_policy_signature(
            self.config.main_user.secret_key, date_stamp, region, policy
        )
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
        conditions = self._build_sigv2_conditions(
            bucket_name, content_type, key_prefix, min_size, max_size, extra_conditions
        )
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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

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
        bucket_name = self.create_bucket(client)

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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")

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
        bucket_name = self.create_bucket(client)

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, min_size=0, max_size=size
            ),
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
        bucket_name = self.create_bucket(client)

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
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket(client)

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
        bucket_name = self.create_bucket_canned_acl(client, "public-read-write")
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
        expected_url = f'{redirect_url}?bucket={bucket_name}&key={key}&etag=%22{etag}%22'
        assert result.url == expected_url

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_signature(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100).replace("T", " "),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_no_key_specified(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
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
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket(client)

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
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "CONDITIONS": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_expires_is_case_sensitive(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "EXPIRATION": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_expired_policy(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(-100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 403, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_invalid_request_field_value(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo"
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_conditions_list(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo", min_size=0, max_size=0
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_missing_content_length_argument(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        content_type = "text/plain"
        key = "\\$foo.txt"

        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": self._build_sigv2_conditions(
                bucket_name, content_type, key_prefix="\\$foo", min_size=512, max_size=1024
            ),
        }
        file_data = FormFile(key, content_type, "bar")
        payload = {"key": key, "acl": "private", "Content-Type": content_type}
        result = self._post_sigv2(bucket_name, policy_document, payload, file_data)
        assert result.status_code == 400, result.get_error_code()

    @pytest.mark.tag("ERROR")
    def test_post_object_empty_conditions(self):
        self.skip_if_aws()
        bucket_name = self.create_bucket()
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
        bucket_name = self.create_bucket()
        key = "foo"

        put_url = self.generate_presigned_put_url(bucket_name, key)
        assert self.put_object_url(put_url, key) == 200

        get_url = self.generate_presigned_get_url(bucket_name, key)
        assert self.get_object_url(get_url) == 200

    @pytest.mark.tag("signV4")
    def test_put_object_v4(self):
        bucket_name = self.create_bucket()
        key = "foo"
        content = utils.random_text_to_long(100)
        result = self.put_object_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()

    @pytest.mark.tag("signV4")
    def test_put_object_chunked_v4(self):
        bucket_name = self.create_bucket()
        key = "foo"
        content = utils.random_text_to_long(100)
        result = self.put_object_chunked_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()

    @pytest.mark.tag("signV4")
    def test_get_object_v4(self):
        key = "foo"
        size = 100
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        content = utils.random_text_to_long(size)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        result = self.get_object_v4(bucket_name, key, content)
        assert result.status_code == 200, result.get_error_code()
        assert len(result.get_content()) == size
        assert result.get_content() == content

    @pytest.mark.tag("ERROR")
    def test_post_object_wrong_bucket(self):
        bucket_name = self.get_new_bucket_name()
        bad_bucket_name = self.get_new_bucket_name()
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
        result = self._post_sigv4(
            bucket_name, policy_document, payload, file_data, url_bucket=bad_bucket_name
        )
        assert result.status_code == 404, result.get_error_code()
