"""SSE-C tests ported from Java testV2/SSE_C.java."""

from __future__ import annotations

import base64
import json

import pytest
from botocore.exceptions import ClientError

from s3tests.auth.aws2_signer import get_base64_encoded_sha1_hash
from s3tests.data import main_data as md
from s3tests.data.form_file import FormFile
from s3tests.test_base import SSE_CUSTOMER_ALGORITHM, SSE_KEY, SSE_KEY_MD5, S3TestBase
from s3tests.utils import net_utils, utils

SSE_ALGORITHM = "AES256"


class TestSseC(S3TestBase):
    @pytest.mark.tag("PutGet")
    def test_encrypted_transfer_1b(self):
        self.encryption_sse_customer_write(1)

    @pytest.mark.tag("PutGet")
    def test_encrypted_transfer_1kb(self):
        self.encryption_sse_customer_write(1024)

    @pytest.mark.tag("PutGet")
    def test_encrypted_transfer_1mb(self):
        self.encryption_sse_customer_write(1024 * 1024)

    @pytest.mark.tag("PutGet")
    def test_encrypted_transfer_13b(self):
        self.encryption_sse_customer_write(13)

    @pytest.mark.tag("metadata")
    def test_encryption_sse_c_method_head(self):
        key = "obj"
        size = 1000
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        data = utils.random_text_to_long(size)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

        client.head_object(
            Bucket=bucket_name,
            Key=key,
            SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
            SSECustomerKey=SSE_KEY,
            SSECustomerKeyMD5=SSE_KEY_MD5,
        )

    @pytest.mark.tag("ERROR")
    def test_encryption_sse_c_present(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 1000
        data = utils.random_text_to_long(size)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    @pytest.mark.tag("ERROR")
    def test_encryption_sse_c_other_key(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 100
        data = utils.random_text_to_long(size)

        sse_b = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4="
        sse_b_md5 = "arxBvwY2V4SiOne6yppVPQ=="

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        with pytest.raises(ClientError) as exc_info:
            client.get_object(
                Bucket=bucket_name,
                Key=key,
                SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
                SSECustomerKey=sse_b,
                SSECustomerKeyMD5=sse_b_md5,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    @pytest.mark.tag("ERROR")
    def test_encryption_sse_c_invalid_md5(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 100
        data = utils.random_text_to_long(size)

        with pytest.raises(ClientError) as exc_info:
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data.encode("utf-8"),
                SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
                SSECustomerKey=SSE_KEY,
                SSECustomerKeyMD5="AAAAAAAAAAAAAAAAAAAAAA==",
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    @pytest.mark.tag("ERROR")
    def test_encryption_sse_c_no_md5(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 100
        data = utils.random_text_to_long(size)

        def _strip_ssec_md5(request, **kwargs):
            for header in list(request.headers.keys()):
                if header.lower() == "x-amz-server-side-encryption-customer-key-md5":
                    del request.headers[header]

        client.meta.events.register("before-sign.s3.PutObject", _strip_ssec_md5)
        try:
            self.assert_client_error(
                lambda: client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data.encode("utf-8"),
                    SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
                    SSECustomerKey=SSE_KEY,
                ),
                400,
                md.INVALID_ARGUMENT,
            )
        finally:
            client.meta.events.unregister("before-sign.s3.PutObject", _strip_ssec_md5)

    @pytest.mark.tag("ERROR")
    def test_encryption_sse_c_no_key(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 100
        data = utils.random_text_to_long(size)

        self.assert_client_error(
            lambda: client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data.encode("utf-8"),
                SSECustomerKeyMD5=SSE_KEY_MD5,
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_encryption_key_no_sse_c(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 100
        data = utils.random_text_to_long(size)

        self.assert_client_error(
            lambda: client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data.encode("utf-8"),
                SSECustomerKey=SSE_KEY,
                SSECustomerKeyMD5=SSE_KEY_MD5,
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Multipart")
    def test_encryption_sse_c_multipart_upload(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "multipartEnc"
        size = 50 * md.MB
        metadata = {"foo": "bar"}

        upload_data = self.setup_sse_c_multipart_upload(client, bucket_name, key, size, metadata)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        # Include SSECustomerKeyMD5 as well to avoid client-side key re-encoding issues.
        head_response = client.head_object(Bucket=bucket_name, Key=key, **self.sse_c_extra_args())
        assert head_response["Metadata"] == metadata
        assert head_response["SSECustomerAlgorithm"] == SSE_ALGORITHM

        body = upload_data.get_body()
        self.check_content_using_range_enc(client, bucket_name, key, body, md.MB)
        self.check_content_using_range_enc(client, bucket_name, key, body, 10 * md.MB)
        self.check_content_using_random_range_enc(client, bucket_name, key, body, size, 100)

    @pytest.mark.tag("Multipart")
    def test_encryption_sse_c_multipart_bad_download(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "multipartEnc"
        size = 50 * md.MB
        metadata = {"foo": "bar"}

        sse_get_key = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4="
        sse_get_md5 = "arxBvwY2V4SiOne6yppVPQ=="

        upload_data = self.setup_sse_c_multipart_upload(client, bucket_name, key, size, metadata)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        head_response = client.head_object(
            Bucket=bucket_name,
            Key=key,
            SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
            SSECustomerKey=SSE_KEY,
            SSECustomerKeyMD5=SSE_KEY_MD5,
        )
        assert head_response["Metadata"] == metadata
        assert head_response["SSECustomerAlgorithm"] == SSE_ALGORITHM

        with pytest.raises(ClientError) as exc_info:
            client.get_object(
                Bucket=bucket_name,
                Key=key,
                SSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
                SSECustomerKey=sse_get_key,
                SSECustomerKeyMD5=sse_get_md5,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    @pytest.mark.tag("Post")
    def test_encryption_sse_c_post_object_authenticated_request(self):
        if self.config.is_aws():
            pytest.skip("SSE-C POST object test is not supported on AWS")

        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)

        content_type = "text/plain"
        key = "foo.txt"
        policy_document = {
            "expiration": self.get_time_to_add_minutes(100),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", content_type],
                ["starts-with", "$x-amz-server-side-encryption-customer-algorithm", SSE_ALGORITHM],
                ["starts-with", "$x-amz-server-side-encryption-customer-key", SSE_KEY],
                ["starts-with", "$x-amz-server-side-encryption-customer-key-md5", SSE_KEY_MD5],
                ["content-length-range", 0, 1024],
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
            "Content-Type": content_type,
            "x-amz-server-side-encryption-customer-algorithm": SSE_ALGORITHM,
            "x-amz-server-side-encryption-customer-key": SSE_KEY,
            "x-amz-server-side-encryption-customer-key-md5": SSE_KEY_MD5,
        }

        result = net_utils.post_upload(
            self.create_url(bucket_name),
            payload,
            FormFile(key, content_type, "bar"),
        )
        assert result.status_code == 204

        response = client.get_object(Bucket=bucket_name, Key=key, **self.sse_c_extra_args())
        body = self.get_body(response)
        assert body == "bar"
        assert response["SSECustomerAlgorithm"] == SSE_ALGORITHM

    @pytest.mark.tag("Get")
    def test_encryption_sse_c_get_object_many(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 15 * 1024 * 1024
        data = utils.random_text_to_long(size)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        self.check_content_enc(bucket_name, key, data, 50)

    @pytest.mark.tag("Get")
    def test_encryption_sse_c_range_object_many(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "obj"
        size = 15 * 1024 * 1024
        data = utils.random_text_to_long(size)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            **self.sse_c_extra_args(),
        )

        response = client.get_object(Bucket=bucket_name, Key=key, **self.sse_c_extra_args())
        body = self.get_body(response)
        assert body == data, md.NOT_MATCHED
        assert response["SSECustomerAlgorithm"] == SSE_ALGORITHM

        self.check_content_using_random_range_enc(client, bucket_name, key, data, size, 50)

    @pytest.mark.tag("Multipart")
    def test_sse_c_encryption_multipart_copy_part_upload(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        source_key = "multipartEnc"
        size = 50 * md.MB

        upload_data = self.setup_sse_c_multipart_upload(client, bucket_name, source_key, size, None)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        head_response = client.head_object(Bucket=bucket_name, Key=source_key, **self.sse_c_extra_args())
        assert head_response["SSECustomerAlgorithm"] == SSE_ALGORITHM

        target_key = "multipartEncCopy"
        upload_data2 = self.multipart_copy_sse_c(
            client, bucket_name, source_key, bucket_name, target_key, size
        )
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key,
            UploadId=upload_data2.upload_id,
            MultipartUpload=upload_data2.completed_multipart_upload(),
        )
        self.check_copy_content_sse_c(client, bucket_name, source_key, bucket_name, target_key)

    @pytest.mark.tag("Multipart")
    def test_sse_c_encryption_multipart_copy_many(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        source_key = "multipartEnc"
        size = 10 * md.MB
        body = ""

        upload_data = self.setup_multipart_upload(client, bucket_name, source_key, size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        body += upload_data.body
        self.check_content_using_range(bucket_name, source_key, body, md.MB)

        target_key1 = "my_multipart1"
        upload_data2 = self.multipart_copy(
            client, bucket_name, source_key, bucket_name, target_key1, size, None
        )
        copy_data1 = self.multipart_upload(client, bucket_name, target_key1, size, 5 * md.MB, upload_data2)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key1,
            UploadId=copy_data1.upload_id,
            MultipartUpload=copy_data1.completed_multipart_upload(),
        )

        body += copy_data1.body
        self.check_content_using_range(bucket_name, target_key1, body, md.MB)

        target_key2 = "my_multipart2"
        upload_data3 = self.multipart_copy(
            client, bucket_name, target_key1, bucket_name, target_key2, size * 2, None
        )
        copy_data2 = self.multipart_upload(client, bucket_name, target_key2, size, 5 * md.MB, upload_data3)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key2,
            UploadId=copy_data2.upload_id,
            MultipartUpload=copy_data2.completed_multipart_upload(),
        )

        body += copy_data2.body
        self.check_content_using_range(bucket_name, target_key2, body, md.MB)
