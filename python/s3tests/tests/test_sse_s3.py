"""SSE-S3 tests ported from Java testV2/SSE_S3.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils

SSE_ALGORITHM = "AES256"
_COPY_SIZES = (1024, 256 * 1024, 1024 * 1024)


class TestSseS3(S3TestBase):
    @pytest.mark.tag("PutGet")
    def test_sse_s3_encrypted_transfer_1b(self):
        self.encryption_sse_s3_write(1)

    @pytest.mark.tag("PutGet")
    def test_sse_s3_encrypted_transfer_1kb(self):
        self.encryption_sse_s3_write(1024)

    @pytest.mark.tag("PutGet")
    def test_sse_s3_encrypted_transfer_1mb(self):
        self.encryption_sse_s3_write(1024 * 1024)

    @pytest.mark.tag("PutGet")
    def test_sse_s3_encrypted_transfer_13b(self):
        self.encryption_sse_s3_write(13)

    @pytest.mark.tag("Metadata")
    def test_sse_s3_encryption_method_head(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "obj"
        data = utils.random_text_to_long(1000)
        metadata = {"foo": "bar"}

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            Metadata=metadata,
            ServerSideEncryption=SSE_ALGORITHM,
        )

        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata
        assert response["ServerSideEncryption"] == SSE_ALGORITHM

    @pytest.mark.tag("Multipart")
    def test_sse_s3_encryption_multipart_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "multipartEnc"
        size = 50 * md.MB
        content_type = "text/plain"
        metadata = {"foo": "bar"}

        create_response = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            ServerSideEncryption=SSE_ALGORITHM,
            ContentType=content_type,
            Metadata=metadata,
        )
        upload_id = create_response["UploadId"]

        parts = utils.generate_random_string(size, 5 * md.MB)
        part_etags = []
        data = ""
        count = 1
        for part in parts:
            part_number = count
            count += 1
            data += part
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part.encode("utf-8"),
            )
            part_etags.append({"PartNumber": part_number, "ETag": part_response["ETag"]})

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": part_etags},
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["Metadata"] == metadata
        assert head_response["ContentType"] == content_type
        assert head_response["ServerSideEncryption"] == SSE_ALGORITHM

        self.check_content_using_range(bucket_name, key, data, md.MB)
        self.check_content_using_range(bucket_name, key, data, 10 * md.MB)
        self.check_content_using_random_range(bucket_name, key, data, 100)

    @pytest.mark.tag("encryption")
    def test_get_bucket_encryption(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        if self.config.is_aws():
            response = client.get_bucket_encryption(Bucket=bucket_name)
            rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
            assert (
                rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM
            )
        else:
            with pytest.raises(ClientError):
                client.get_bucket_encryption(Bucket=bucket_name)

    @pytest.mark.tag("encryption")
    def test_put_bucket_encryption(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

    @pytest.mark.tag("encryption")
    def test_delete_bucket_encryption(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        assert len(response["ServerSideEncryptionConfiguration"]["Rules"]) == 1

        client.delete_bucket_encryption(Bucket=bucket_name)

        if self.config.is_aws():
            get_response = client.get_bucket_encryption(Bucket=bucket_name)
            rule = get_response["ServerSideEncryptionConfiguration"]["Rules"][0]
            assert (
                rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM
            )
        else:
            with pytest.raises(ClientError):
                client.get_bucket_encryption(Bucket=bucket_name)

    @pytest.mark.tag("encryption")
    def test_put_bucket_encryption_and_object_set_check(self):
        keys = ["for/bar", "test/"]
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        self.create_objects_list(client, keys, bucket_name)

        for key in keys:
            head_response = client.head_object(Bucket=bucket_name, Key=key)
            assert head_response["ServerSideEncryption"] == SSE_ALGORITHM

    @pytest.mark.tag("CopyObject")
    def test_copy_object_encryption_1kb(self):
        self.encryption_sse_s3_copy(1024)

    @pytest.mark.tag("CopyObject")
    def test_copy_object_encryption_256kb(self):
        self.encryption_sse_s3_copy(256 * 1024)

    @pytest.mark.tag("CopyObject")
    def test_copy_object_encryption_1mb(self):
        self.encryption_sse_s3_copy(1024 * 1024)

    @pytest.mark.tag("PutGet")
    def test_sse_s3_bucket_put_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        data = utils.random_text_to_long(1000)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        key = "bar"
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(get_response)
        assert get_response["ServerSideEncryption"] == SSE_ALGORITHM
        assert body == data, md.NOT_MATCHED

    @pytest.mark.tag("PutGet")
    def test_sse_s3_bucket_put_get_use_chunk_encoding(self):
        client = self.get_client_https(True)
        bucket_name = self.create_bucket(client)
        data = utils.random_text_to_long(1000)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        key = "bar"
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(get_response)
        assert get_response["ServerSideEncryption"] == SSE_ALGORITHM
        assert body == data, md.NOT_MATCHED

    @pytest.mark.tag("PutGet")
    def test_sse_s3_bucket_put_get_not_chunk_encoding(self):
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        data = utils.random_text_to_long(1000)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        key = "bar"
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(get_response)
        assert get_response["ServerSideEncryption"] == SSE_ALGORITHM
        assert body == data, md.NOT_MATCHED

    @pytest.mark.tag("PresignedURL")
    def test_sse_s3_bucket_presigned_url_put_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        put_url = self.generate_presigned_put_url(bucket_name, key)
        assert self.put_object_url(put_url, key) == 200

        get_url = self.generate_presigned_get_url(bucket_name, key)
        assert self.get_object_url(get_url) == 200

    @pytest.mark.tag("PresignedURL")
    def test_sse_s3_bucket_presigned_url_put_get_v4(self):
        client = self.get_client(True)
        bucket_name = self.create_bucket(client)
        key = "foo"

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        put_url = self.generate_presigned_put_url(bucket_name, key)
        assert self.put_object_url(put_url, key) == 200

        get_url = self.generate_presigned_get_url(bucket_name, key)
        assert self.get_object_url(get_url) == 200

    @pytest.mark.tag("Get")
    def test_sse_s3_get_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        data = utils.random_text_to_long(15 * md.MB)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content(bucket_name, key, data, 50)

    @pytest.mark.tag("Get")
    def test_sse_s3_range_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        size = 15 * 1024 * 1024
        data = utils.random_text_to_long(size)

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content_using_random_range(bucket_name, key, data, 100)

    @pytest.mark.tag("Multipart")
    def test_sse_s3_encryption_multipart_copy_part_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source_key = "multipartEnc"
        size = 50 * md.MB
        metadata = {"foo": "bar"}

        upload_data = self.setup_sse_multipart_upload(client, bucket_name, source_key, size, metadata)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert list_response["KeyCount"] == 1
        assert self.get_bytes_used(list_response) == size

        head_response = client.head_object(Bucket=bucket_name, Key=source_key)
        assert head_response["Metadata"] == metadata
        assert head_response["ServerSideEncryption"] == SSE_ALGORITHM

        self.check_content_using_range(bucket_name, source_key, upload_data.get_body(), md.MB)

        target_key = "multipartEncCopy"
        copy_data = self.multipart_copy(
            client, bucket_name, source_key, bucket_name, target_key, size, metadata
        )
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=target_key,
            UploadId=copy_data.upload_id,
            MultipartUpload=copy_data.completed_multipart_upload(),
        )

        self.check_copy_content_using_range(bucket_name, source_key, bucket_name, target_key, md.MB)

    @pytest.mark.tag("Multipart")
    def test_sse_s3_encryption_multipart_copy_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        source_key = "multipartEnc"
        size = 10 * md.MB
        body = ""

        upload_data = self.setup_sse_multipart_upload(client, bucket_name, source_key, size, None)
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

        body += upload_data2.body
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

        body += upload_data3.body
        self.check_content_using_range(bucket_name, target_key2, body, md.MB)

    @pytest.mark.tag("Retroactive")
    def test_sse_s3_not_retroactive(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        data = utils.random_text_to_long(1000)

        put_key = "put"
        copy_key = "copy"
        multi_key = "multi"

        client.put_object(Bucket=bucket_name, Key=put_key, Body=data.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=copy_key,
            CopySource={"Bucket": bucket_name, "Key": put_key},
        )

        upload_data = self.setup_sse_multipart_upload(client, bucket_name, multi_key, 5 * md.MB, None)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=multi_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )

        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": SSE_ALGORITHM},
                    }
                ]
            },
        )

        response = client.get_bucket_encryption(Bucket=bucket_name)
        rule = response["ServerSideEncryptionConfiguration"]["Rules"][0]
        assert rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == SSE_ALGORITHM

        get_response = client.get_object(Bucket=bucket_name, Key=put_key)
        body = self.get_body(get_response)
        assert body == data, md.NOT_MATCHED

        get_response = client.get_object(Bucket=bucket_name, Key=copy_key)
        body = self.get_body(get_response)
        assert len(body) == len(data)
        assert body == data, md.NOT_MATCHED

        self.check_content_using_range(bucket_name, multi_key, upload_data.body, md.MB)

        put_key2 = "put2"
        copy_key2 = "copy2"
        multi_key2 = "multi2"
        data2 = utils.random_text_to_long(1000)
        client.put_object(Bucket=bucket_name, Key=put_key2, Body=data2.encode("utf-8"))

        client.copy_object(
            Bucket=bucket_name,
            Key=copy_key2,
            CopySource={"Bucket": bucket_name, "Key": put_key2},
        )

        upload_data2 = self.setup_sse_multipart_upload(client, bucket_name, multi_key2, 5 * md.MB, None)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=multi_key2,
            UploadId=upload_data2.upload_id,
            MultipartUpload=upload_data2.completed_multipart_upload(),
        )

        client.delete_bucket_encryption(Bucket=bucket_name)

        get_response = client.get_object(Bucket=bucket_name, Key=put_key2)
        body = self.get_body(get_response)
        assert len(body) == len(data2)
        assert body == data2, md.NOT_MATCHED
        assert get_response["ServerSideEncryption"] == SSE_ALGORITHM

        get_response = client.get_object(Bucket=bucket_name, Key=copy_key2)
        body = self.get_body(get_response)
        assert len(body) == len(data2)
        assert body == data2, md.NOT_MATCHED
        assert get_response["ServerSideEncryption"] == SSE_ALGORITHM

        self.check_content_using_range(bucket_name, multi_key2, upload_data2.body, md.MB)
