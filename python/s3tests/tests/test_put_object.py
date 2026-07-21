"""PutObject tests ported from Java testV2/PutObject.java."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import checksum, utils

RETENTION_DATE = datetime(2030, 2, 1, tzinfo=timezone.utc)


class TestPutObject(S3TestBase):
    @pytest.mark.tag("PUT")
    def test_bucket_list_distinct(self):
        client = self.get_client()
        bucket_name1 = self.create_bucket(client)
        bucket_name2 = self.create_bucket(client)
        client.put_object(Bucket=bucket_name1, Key="foo", Body=b"bar")
        response = client.list_objects(Bucket=bucket_name2)
        assert len(response.get("Contents", [])) == 0

    @pytest.mark.tag("ERROR")
    def test_object_write_to_non_exist_bucket(self):
        key = "foo"
        client = self.get_client()
        bucket_name = self.get_new_bucket_name()
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET

    @pytest.mark.tag("metadata")
    def test_object_head_zero_bytes(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"")
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ContentLength"] == 0

    @pytest.mark.tag("metadata")
    def test_object_write_check_etag(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        response = client.put_object(Bucket=bucket_name, Key="foo", Body=b"bar")
        assert response["ETag"].replace('"', "") == "37b51d194a7513e45b56f6524f2d51f2"

    @pytest.mark.tag("cacheControl")
    def test_object_write_cache_control(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        body = "bar"
        cache_control = "public, max-age=14HttpStatus.SC_BAD_REQUEST"
        content_type = "text/plain"
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            CacheControl=cache_control,
            ContentType=content_type,
            ContentLength=len(body),
        )
        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["CacheControl"] == cache_control
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body

    @pytest.mark.skip(reason="Java @Disabled: JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
    @pytest.mark.tag("Expires")
    def test_object_write_expires(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        body = "bar"
        expires = self.get_time_to_add_seconds(6000)
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            Expires=expires,
            ContentType="text/plain",
            ContentLength=len(body),
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Expires"] == expires

    @pytest.mark.tag("Update")
    def test_object_write_read_update_read_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        body = "bar"
        client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body
        body2 = "soup"
        client.put_object(Bucket=bucket_name, Key=key, Body=body2.encode("utf-8"))
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body2
        client.delete_object(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_none_to_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": "my"}
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), Metadata=metadata)
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata

    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_none_to_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": ""}
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), Metadata=metadata)
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata

    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_overwrite_to_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": "my"}
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), Metadata=metadata)
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata
        metadata2 = {"meta1": ""}
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), Metadata=metadata2)
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata2

    @pytest.mark.skip(reason="Java @Disabled: JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
    @pytest.mark.tag("metadata")
    def test_object_set_get_non_utf8_metadata(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": "\nmy_meta"}
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=b"bar", Metadata=metadata)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403, 500)

    @pytest.mark.skip(reason="Java @Disabled: JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_empty_to_unreadable_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": "\nasdf"}
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=b"bar", Metadata=metadata)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403, 500)

    @pytest.mark.skip(reason="Java @Disabled: JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_empty_to_unreadable_suffix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata = {"meta1": "asdf\n"}
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=b"bar", Metadata=metadata)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403, 500)

    @pytest.mark.tag("metadata")
    def test_object_metadata_replaced_on_put(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        body = "bar"
        metadata = {"meta1": "bar"}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="text/plain",
            Metadata=metadata,
        )
        client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert len(response.get("Metadata", {})) == 0

    @pytest.mark.tag("Encoding")
    def test_object_write_file(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        data_str = "bar"
        data = data_str.encode("us-ascii").decode("us-ascii")
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("us-ascii"))
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == data_str

    @pytest.mark.tag("SpecialKeyName")
    def test_bucket_create_special_key_names(self):
        keys = [
            "!",
            "-",
            "_",
            ".",
            "'",
            "()",
            "&",
            "$",
            "@",
            "=",
            ";",
            "/",
            ":",
            "+",
            "  ",
            ",",
            "?",
            "{}",
            "^",
            "%",
            "`",
            "[]",
            "<>",
            "~",
            "#",
            "|",
        ]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, keys)
        objects = self.get_object_list(client, bucket_name, None)
        for key in keys:
            assert key in objects
            response = client.get_object(Bucket=bucket_name, Key=key)
            body = self.get_body(response)
            if key.endswith("/"):
                assert body == ""
            else:
                assert body == key

    @pytest.mark.tag("SpecialKeyName")
    def test_bucket_list_special_prefix(self):
        keys = ["Bla/1", "Bla/2", "Bla/3", "Bla/4", "abcd"]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, keys)
        objects = self.get_object_list(client, bucket_name, None)
        assert len(objects) == 5
        objects = self.get_object_list(client, bucket_name, "Bla/")
        assert len(objects) == 4

    @pytest.mark.tag("Lock")
    def test_object_lock_uploading_obj(self):
        bucket_name = self.get_new_bucket_name()
        client = self.get_client()
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = "testObjectLockUploadingObjV2"
        content_md5 = utils.get_md5(key)
        put_response = client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ContentMD5=content_md5,
            ContentType="text/plain",
            ContentLength=len(key),
            ObjectLockMode="GOVERNANCE",
            ObjectLockRetainUntilDate=RETENTION_DATE,
            ObjectLockLegalHoldStatus="ON",
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ObjectLockMode"] == "GOVERNANCE"
        assert response["ObjectLockRetainUntilDate"] == RETENTION_DATE
        assert response["ObjectLockLegalHoldStatus"] == "ON"
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=put_response["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Space")
    def test_object_infix_space(self):
        keys = ["a a/", "b b/f1", "c/f 2", "d d/f 3"]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Space")
    def test_object_suffix_space(self):
        keys = ["a /", "b /f1", "c/f2 ", "d /f3 "]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("SpecialCharacters")
    def test_put_object_special_characters(self):
        keys = [
            "!",
            "!/",
            "!/!",
            "$",
            "$/",
            "$/$",
            "'",
            "'/",
            "'/'",
            "(",
            "(/",
            "(/(",
            ")",
            ")/",
            ")/)",
            "*",
            "*/",
            "*/*",
            ":",
            ":/",
            ":/:",
            "[",
            "[/",
            "[/[",
            "]",
            "]/",
            "]/]",
        ]
        client = self.get_client(True)
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Encoding")
    def test_put_object_special_characters_use_chunk_encoding(self):
        keys = [
            "!",
            "!/",
            "!/!",
            "$",
            "$/",
            "$/$",
            "'",
            "'/",
            "'/'",
            "(",
            "(/",
            "(/(",
            ")",
            ")/",
            ")/)",
            "*",
            "*/",
            "*/*",
            ":",
            ":/",
            ":/:",
            "[",
            "[/",
            "[/[",
            "]",
            "]/",
            "]/]",
        ]
        client = self.get_client(True)
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Encoding")
    def test_put_object_use_special_characters_chunk_encoding_and_disable_payload_signing(self):
        keys = [
            "!",
            "!/",
            "!/!",
            "$",
            "$/",
            "$/$",
            "'",
            "'/",
            "'/'",
            "(",
            "(/",
            "(/(",
            ")",
            ")/",
            ")/)",
            "*",
            "*/",
            "*/*",
            ":",
            ":/",
            ":/:",
            "[",
            "[/",
            "[/[",
            "]",
            "]/",
            "]/]",
        ]
        client = self.get_client(True)
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Encoding")
    def test_put_object_special_characters_not_chunk_encoding(self):
        keys = [
            "!",
            "!/",
            "!/!",
            "$",
            "$/",
            "$/$",
            "'",
            "'/",
            "'/'",
            "(",
            "(/",
            "(/(",
            ")",
            ")/",
            ")/)",
            "*",
            "*/",
            "*/*",
            ":",
            ":/",
            ":/:",
            "[",
            "[/",
            "[/[",
            "]",
            "]/",
            "]/]",
        ]
        client = self.get_client(False)
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Encoding")
    def test_put_object_special_characters_not_chunk_encoding_and_disable_payload_signing(self):
        keys = [
            "!",
            "!/",
            "!/!",
            "$",
            "$/",
            "$/$",
            "'",
            "'/",
            "'/'",
            "(",
            "(/",
            "(/(",
            ")",
            ")/",
            ")/)",
            "*",
            "*/",
            "*/*",
            ":",
            ":/",
            ":/:",
            "[",
            "[/",
            "[/[",
            "]",
            "]/",
            "]/]",
        ]
        client = self.get_client(False)
        bucket_name = self.create_objects_keys(client, keys)
        response = client.list_objects(Bucket=bucket_name)
        assert self.get_keys(response.get("Contents")) == keys

    @pytest.mark.tag("Directory")
    def test_put_object_dir_and_file(self):
        key = "aaa"
        directory_name = "aaa/"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object(Bucket=bucket_name, Key=directory_name, Body=b"")
        response = client.list_objects(Bucket=bucket_name)
        keys = self.get_keys(response.get("Contents"))
        assert len(keys) == 2

        bucket_name2 = self.create_bucket(client)
        client.put_object(Bucket=bucket_name2, Key=directory_name, Body=b"")
        client.put_object(Bucket=bucket_name2, Key=key, Body=key.encode("utf-8"))
        response = client.list_objects(Bucket=bucket_name2)
        keys = self.get_keys(response.get("Contents"))
        assert len(keys) == 2

        bucket_name3 = self.create_bucket(client)
        new_key = "aaa/bbb/ccc"
        client.put_object(Bucket=bucket_name3, Key=key, Body=key.encode("utf-8"))
        client.put_object(Bucket=bucket_name3, Key=new_key, Body=new_key.encode("utf-8"))
        response = client.list_objects(Bucket=bucket_name3)
        keys = self.get_keys(response.get("Contents"))
        assert len(keys) == 2

    @pytest.mark.tag("Overwrite")
    def test_object_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "temp"
        content1 = utils.random_text_to_long(10 * md.KB)
        content2 = utils.random_text_to_long(1 * md.MB)
        client.put_object(Bucket=bucket_name, Key=key, Body=content1.encode("utf-8"))
        client.put_object(Bucket=bucket_name, Key=key, Body=content2.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        assert len(body) == len(content2)
        assert body == content2, md.NOT_MATCHED

    @pytest.mark.tag("PUT")
    def test_object_emoji(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "test❤🍕🍔🚗"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        response = client.list_objects(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 1

    @pytest.mark.tag("metadata")
    def test_object_set_get_metadata_utf8(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        metadata_key1 = "meta1"
        metadata_key2 = "meta2"
        metadata1 = "utf-8"
        metadata2 = "UTF-8"
        content_type = "text/plain; charset=UTF-8"
        metadata = {metadata_key1: metadata1, metadata_key2: metadata2}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"bar",
            ContentType=content_type,
            Metadata=metadata,
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"][metadata_key1] == metadata1
        assert response["Metadata"][metadata_key2] == metadata2

    def _put_with_checksum(self, client, bucket_name: str, key: str, algorithm: str) -> dict:
        params = {
            "Bucket": bucket_name,
            "Key": key,
            "Body": key.encode("utf-8"),
        }
        checksum.apply_put_checksum_params(params, algorithm, key)
        return client.put_object(**params)

    @pytest.mark.tag("checksum")
    def test_put_object_checksum_use_chunk_encoding(self):
        bucket_name = self.create_bucket()
        configs = [
            ("when_required", "when_required"),
            ("when_required", "when_supported"),
            ("when_supported", "when_required"),
            ("when_supported", "when_supported"),
        ]
        for request_option, response_option in configs:
            client = self.get_client_with_checksum(True, request_option, response_option)
            async_client = self.get_async_client(True, request_option, response_option)
            for algorithm in checksum.ALL_ALGORITHMS:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{algorithm}"
                async_key = f"{prefix}/async/{algorithm}"
                response = self._put_with_checksum(client, bucket_name, key, algorithm)
                checksum.checksum_compare(algorithm, key, response)
                async_params = {
                    "Bucket": bucket_name,
                    "Key": async_key,
                    "Body": async_key.encode("utf-8"),
                }
                checksum.apply_put_checksum_params(async_params, algorithm, async_key)
                async_response = async_client.put_object(**async_params).join()
                checksum.checksum_compare(algorithm, async_key, async_response)

    @pytest.mark.tag("checksum")
    def test_put_object_checksum(self):
        bucket_name = self.create_bucket()
        configs = [
            ("when_required", "when_required"),
            ("when_required", "when_supported"),
            ("when_supported", "when_required"),
            ("when_supported", "when_supported"),
        ]
        for request_option, response_option in configs:
            client = self.get_client_with_checksum(False, request_option, response_option)
            async_client = self.get_async_client(False, request_option, response_option)
            for algorithm in checksum.ALL_ALGORITHMS:
                prefix = f"req_{request_option}/resp_{response_option}"
                key = f"{prefix}/sync/{algorithm}"
                async_key = f"{prefix}/async/{algorithm}"
                response = self._put_with_checksum(client, bucket_name, key, algorithm)
                checksum.checksum_compare(algorithm, key, response)
                async_params = {
                    "Bucket": bucket_name,
                    "Key": async_key,
                    "Body": async_key.encode("utf-8"),
                }
                checksum.apply_put_checksum_params(async_params, algorithm, async_key)
                async_response = async_client.put_object(**async_params).join()
                checksum.checksum_compare(algorithm, async_key, async_response)

    @pytest.mark.tag("checksum")
    def test_put_object_checksum_with_value(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        for algorithm in checksum.ALL_ALGORITHMS:
            key = f"precomputed/{algorithm}"
            params = {
                "Bucket": bucket_name,
                "Key": key,
                "Body": key.encode("utf-8"),
            }
            checksum.set_checksum(params, algorithm, checksum.calculate_checksum(algorithm, key))
            response = client.put_object(**params)
            checksum.checksum_compare(algorithm, key, response)

    @pytest.mark.tag("checksum-failure")
    def test_put_object_checksum_failure(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        for algorithm in checksum.ALL_ALGORITHMS:
            key = f"wrong-checksum/{algorithm}"
            wrong_value = checksum.calculate_checksum(algorithm, f"{key}-wrong")
            params = {
                "Bucket": bucket_name,
                "Key": key,
                "Body": key.encode("utf-8"),
            }
            checksum.set_checksum(params, algorithm, wrong_value)
            with pytest.raises(ClientError) as exc_info:
                client.put_object(**params)
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert exc_info.value.response["Error"]["Code"] == md.BAD_DIGEST

    @pytest.mark.tag("IfMatch")
    def test_put_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testPutObjectIfMatchGood"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"old")["ETag"]
        client.put_object(Bucket=bucket_name, Key=key, Body=b"new", IfMatch=etag)
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "new"

    @pytest.mark.tag("IfMatch")
    def test_put_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testPutObjectIfMatchFailed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"old")
        with pytest.raises(ClientError) as exc_info:
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"new",
                IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfNoneMatch")
    def test_put_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testPutObjectIfNoneMatchGood"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar", IfNoneMatch="*")
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfNoneMatch")
    def test_put_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testPutObjectIfNoneMatchFailed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"old")
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=b"new", IfNoneMatch="*")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_put_object_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testPutObjectIfMatchAndIfNoneMatch"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"old")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"new",
                IfMatch=etag,
                IfNoneMatch="*",
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert exc_info.value.response["Error"]["Code"] == md.NOT_IMPLEMENTED
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == "old"

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_max_length(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = utils.random_object_name(md.MAX_KEY_LENGTH)
        body = "test-max-length"
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        assert response["ETag"] is not None
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_min_length(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "a"
        body = "test-min-length"
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        assert response["ETag"] is not None
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_too_long(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = utils.random_object_name(md.MAX_KEY_LENGTH + 1)
        body = "test-too-long"
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.KEY_TOO_LONG

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_special_characters_at_start(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        special_chars = [
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "-",
            "_",
            "+",
            "=",
            "[",
            "]",
            "{",
            "}",
            "|",
            "\\",
            ":",
            ";",
            '"',
            "'",
            "<",
            ">",
            ",",
            ".",
            "?",
            "/",
            "~",
            "`",
        ]
        for special_char in special_chars:
            remaining_length = md.MAX_KEY_LENGTH - len(special_char)
            key = special_char + utils.random_object_name(remaining_length)
            body = f"test-body-{special_char}"
            assert len(key) == md.MAX_KEY_LENGTH
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_special_characters_at_end(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        special_chars = [
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "-",
            "_",
            "+",
            "=",
            "[",
            "]",
            "{",
            "}",
            "|",
            "\\",
            ":",
            ";",
            '"',
            "'",
            "<",
            ">",
            ",",
            ".",
            "?",
            "/",
            "~",
            "`",
        ]
        for special_char in special_chars:
            remaining_length = md.MAX_KEY_LENGTH - len(special_char)
            key = utils.random_object_name(remaining_length) + special_char
            body = f"test-body-{special_char}"
            assert len(key) == md.MAX_KEY_LENGTH
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_unicode_characters(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        unicode_chars = ["한", "中", "日", "а", "α", "ع", "т", "ф"]
        for unicode_char in unicode_chars:
            single_char_bytes = len(unicode_char.encode("utf-8"))
            max_length = 200 // single_char_bytes
            safe_length = max(1, max_length - 1)
            key = unicode_char * safe_length
            body = f"unicode-test-{unicode_char}"
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_unicode_characters_too_long(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        unicode_chars = ["한", "中", "日", "а", "α", "ع", "т", "ф"]
        for unicode_char in unicode_chars:
            single_char_bytes = len(unicode_char.encode("utf-8"))
            max_length = 1024 // single_char_bytes
            too_long_length = max_length + 1
            key = unicode_char * too_long_length
            body = f"unicode-test-fail-{unicode_char}"
            with pytest.raises(ClientError) as exc_info:
                client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert exc_info.value.response["Error"]["Code"] == md.KEY_TOO_LONG

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_with_leading_and_trailing_spaces(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        test_cases = [1, 2, 3, 5]
        for space_count in test_cases:
            spaces = " " * space_count
            middle_length = md.MAX_KEY_LENGTH - (space_count * 2)
            middle = utils.random_object_name(middle_length)
            key = spaces + middle + spaces
            body = f"space-test-{space_count}"
            assert len(key) == md.MAX_KEY_LENGTH
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_with_consecutive_slashes(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        keys = [
            "folder//double-slash",
            "folder///triple-slash",
            "//leading-double-slash",
            "trailing-double-slash//",
            "folder////multiple-slashes",
        ]
        for key in keys:
            body = "slash-test-" + key.replace("/", "-")
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body

    @pytest.mark.tag("KeyLength")
    def test_put_object_key_boundary_lengths(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        test_cases = [md.MAX_KEY_LENGTH - 1, md.MAX_KEY_LENGTH, 500, 100, 50]
        for length in test_cases:
            key = utils.random_object_name(length)
            body = f"boundary-test-{length}"
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            assert response["ETag"] is not None
            get_response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(get_response) == body
