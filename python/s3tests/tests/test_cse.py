"""CSE tests ported from Java testV2/CSE.java."""

from __future__ import annotations

import random

import pytest

from s3tests.data import aes256
from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestCSE(S3TestBase):
    @pytest.mark.tag("PutGet")
    def test_cse_encrypted_transfer_1b(self):
        self.encryption_cse_write("testCseEncryptedTransfer1b", 1)

    @pytest.mark.tag("PutGet")
    def test_cse_encrypted_transfer_1kb(self):
        self.encryption_cse_write("testCseEncryptedTransfer1kb", 1024)

    @pytest.mark.tag("PutGet")
    def test_cse_encrypted_transfer_1mb(self):
        self.encryption_cse_write("testCseEncryptedTransfer1MB", 1024 * 1024)

    @pytest.mark.tag("PutGet")
    def test_cse_encrypted_transfer_13b(self):
        self.encryption_cse_write("testCseEncryptedTransfer13b", 13)

    @pytest.mark.tag("Metadata")
    def test_cse_encryption_method_head(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseEncryptionMethodHead/obj"
        size = 1000
        content_type = "text/plain"
        data = utils.random_text_to_long(size)
        aes_key = utils.random_text_to_long(32)

        encoding = aes256.encrypt(data, aes_key)
        metadata = {"key": aes_key}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding.encode("utf-8"),
            Metadata=metadata,
            ContentType=content_type,
            ContentLength=len(encoding),
        )

        get_metadata = client.head_object(Bucket=bucket_name, Key=key)
        assert get_metadata["Metadata"] == metadata

    @pytest.mark.tag("ERROR")
    def test_cse_encryption_non_decryption(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseEncryptionNonDecryption/obj"
        size = 1000
        content_type = "text/plain"
        data = utils.random_text_to_long(size)
        aes_key = utils.random_text_to_long(32)

        encoding = aes256.encrypt(data, aes_key)
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding.encode("utf-8"),
            ContentType=content_type,
            ContentLength=len(encoding),
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert body != data

    @pytest.mark.tag("ERROR")
    def test_cse_non_encryption_decryption(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseNonEncryptionDecryption"
        size = 1000
        content_type = "text/plain"
        data = utils.random_text_to_long(size)
        aes_key = utils.random_text_to_long(32)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data.encode("utf-8"),
            ContentType=content_type,
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        encoding_body = self.get_body(response)
        with pytest.raises(Exception):
            aes256.decrypt(encoding_body, aes_key)

    @pytest.mark.tag("RangeRead")
    def test_cse_encryption_range_read(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseEncryptionRangeRead"
        content_type = "text/plain"
        aes_key = utils.random_text_to_long(32)

        data = utils.random_text_to_long(1024 * 1024)
        encoding = aes256.encrypt(data, aes_key)
        metadata = {"x-amz-meta-key": aes_key}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding.encode("utf-8"),
            Metadata=metadata,
            ContentType=content_type,
            ContentLength=len(encoding),
        )

        rng = random.Random()
        start_point = rng.randint(0, 1024 * 1024 - 1001)
        end_point = start_point + 999
        response = client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range=f"bytes={start_point}-{end_point}",
        )
        encoding_body = self.get_body(response)
        assert encoding[start_point : start_point + 1000] == encoding_body, md.NOT_MATCHED

    @pytest.mark.tag("Multipart")
    def test_cse_encryption_multipart_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseEncryptionMultipartUpload"
        size = 50 * md.MB
        content_type = "text/plain"
        data = utils.random_text_to_long(size)
        aes_key = utils.random_text_to_long(32)

        encoding = aes256.encrypt(data, aes_key)
        metadata = {"key": aes_key}

        init_response = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            ContentType=content_type,
            Metadata=metadata,
        )
        upload_id = init_response["UploadId"]

        parts_data = self.cut_string_data(encoding, 5 * md.MB)
        part_etags = []
        for index, part in enumerate(parts_data, start=1):
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=index,
                Body=part.encode("utf-8"),
            )
            part_etags.append({"PartNumber": index, "ETag": part_response["ETag"]})

        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": part_etags},
        )

        head_response = client.list_objects_v2(Bucket=bucket_name)
        assert head_response["KeyCount"] == 1
        assert self.get_bytes_used(head_response) == len(encoding)

        get_response = client.head_object(Bucket=bucket_name, Key=key)
        assert get_response["Metadata"] == metadata
        assert get_response["ContentType"] == content_type

        self.check_content_using_range(bucket_name, key, encoding, md.MB)
        self.check_content_using_range(bucket_name, key, encoding, 10 * md.MB)
        self.check_content_using_random_range(bucket_name, key, encoding, 100)

    @pytest.mark.tag("Get")
    def test_cse_get_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseGetObjectMany"
        content_type = "text/plain"
        aes_key = utils.random_text_to_long(32)
        data = utils.random_text_to_long(15 * md.MB)

        encoding = aes256.encrypt(data, aes_key)
        metadata = {"AESkey": aes_key}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding.encode("utf-8"),
            ContentType=content_type,
            ContentLength=len(encoding),
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        encoding_body = self.get_body(response)
        body = aes256.decrypt(encoding_body, aes_key)
        assert data == body, md.NOT_MATCHED
        self.check_content(bucket_name, key, encoding, 50)

    @pytest.mark.tag("Get")
    def test_cse_range_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testCseRangeObjectMany"
        content_type = "text/plain"
        aes_key = utils.random_text_to_long(32)
        file_size = 15 * 1024 * 1024
        data = utils.random_text_to_long(file_size)

        encoding = aes256.encrypt(data, aes_key)
        metadata = {"AESkey": aes_key}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding.encode("utf-8"),
            ContentType=content_type,
            ContentLength=len(encoding),
        )

        response = client.get_object(Bucket=bucket_name, Key=key)
        encoding_body = self.get_body(response)
        body = aes256.decrypt(encoding_body, aes_key)
        assert data == body, md.NOT_MATCHED
        self.check_content_using_random_range(bucket_name, key, encoding, 50)
