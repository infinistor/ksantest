"""GetObject tests ported from Java testV2/GetObject.java."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import checksum, utils

PAST_DATE = datetime(1994, 9, 29, 19, 43, 31, tzinfo=timezone.utc)
FUTURE_DATE = datetime(2100, 9, 29, 19, 43, 31, tzinfo=timezone.utc)


class TestGetObject(S3TestBase):
    @pytest.mark.tag("ERROR")
    def test_object_read_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key="test_object_read_not_exist")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        key = "test_get_object_if_match_good"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        response = client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        key = "test_get_object_if_match_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED

    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        key = "test_get_object_if_none_match_good"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        key = "test_get_object_if_none_match_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key, IfNoneMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfModifiedSince")
    def test_get_object_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        key = "test_get_object_if_modified_since_good"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key, IfModifiedSince=PAST_DATE)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfModifiedSince")
    def test_get_object_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        key = "test_get_object_if_modified_since_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key)
        after = response["LastModified"] + timedelta(seconds=1)
        self.delay(1000)
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfModifiedSince=after)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_get_object_if_unmodified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        key = "test_get_object_if_unmodified_since_good"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=PAST_DATE)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_get_object_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        key = "test_get_object_if_unmodified_since_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=FUTURE_DATE)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_with_if_unmodified_since(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        key = "test_get_object_if_match_with_if_unmodified_since"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        response = client.get_object(
            Bucket=bucket_name,
            Key=key,
            IfMatch=etag,
            IfUnmodifiedSince=PAST_DATE,
        )
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_none_match_with_if_modified_since(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        key = "test_get_object_if_none_match_with_if_modified_since"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfNoneMatch=etag,
                IfModifiedSince=PAST_DATE,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        key = "test_get_object_if_match_and_if_none_match"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        key = "test_get_object_if_match_and_if_none_match_any"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag, IfNoneMatch="*")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfMatch")
    def test_head_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        key = "test_head_object_if_match_good"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        response = client.head_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        assert response["ETag"] == etag

    @pytest.mark.tag("IfMatch")
    def test_head_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        key = "test_head_object_if_match_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    @pytest.mark.tag("IfNoneMatch")
    def test_head_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        key = "test_head_object_if_none_match_good"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    @pytest.mark.tag("IfNoneMatch")
    def test_head_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        key = "test_head_object_if_none_match_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key, IfNoneMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        assert response["ContentLength"] == 3

    @pytest.mark.tag("IfModifiedSince")
    def test_head_object_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        key = "test_head_object_if_modified_since_good"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key, IfModifiedSince=PAST_DATE)
        assert response["ContentLength"] == 3

    @pytest.mark.tag("IfModifiedSince")
    def test_head_object_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        key = "test_head_object_if_modified_since_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key)
        after = response["LastModified"] + timedelta(seconds=1)
        self.delay(1000)
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfModifiedSince=after)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_head_object_if_unmodified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        key = "test_head_object_if_unmodified_since_good"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=PAST_DATE)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_head_object_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)
        key = "test_head_object_if_unmodified_since_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=FUTURE_DATE)
        assert response["ContentLength"] == 3

    @pytest.mark.tag("Range")
    def test_ranged_request_response_code(self):
        key = "test_ranged_request_response_code"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=4-7")
        assert self.get_body(response) == content[4:8]
        assert response["ContentRange"] == "bytes 4-7/11"

    @pytest.mark.tag("Range")
    def test_ranged_big_request_response_code(self):
        key = "test_ranged_big_request_response_code"
        content = utils.random_text_to_long(8 * md.MB)
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=3145728-5242880")
        assert self.get_body(response) == content[3145728:5242881]
        assert response["ContentRange"] == "bytes 3145728-5242880/8388608"

    @pytest.mark.tag("Range")
    def test_ranged_request_skip_leading_bytes_response_code(self):
        key = "test_ranged_request_skip_leading_bytes_response_code"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=4-")
        assert self.get_body(response) == content[4:]
        assert response["ContentRange"] == "bytes 4-10/11"

    @pytest.mark.tag("Range")
    def test_ranged_request_return_trailing_bytes_response_code(self):
        key = "test_ranged_request_return_trailing_bytes_response_code"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 25)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=-7")
        assert self.get_body(response) == content[-7:]
        assert response["ContentRange"] == "bytes 4-10/11"

    @pytest.mark.tag("Range")
    def test_ranged_request_invalid_range(self):
        key = "test_ranged_request_invalid_range"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, Range="bytes=40-50")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_RANGE

    @pytest.mark.tag("Range")
    def test_ranged_request_empty_object(self):
        key = "test_ranged_request_empty_object"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, Range="bytes=40-50")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_RANGE

    @pytest.mark.tag("Get")
    def test_get_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        key = "test_get_object_many"
        data = utils.random_text_to_long(15 * md.MB)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content(bucket_name, key, data, 50)

    @pytest.mark.tag("Get")
    def test_range_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        key = "test_range_object_many"
        data = utils.random_text_to_long(15 * md.MB)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content_using_random_range(bucket_name, key, data, 50)

    @pytest.mark.tag("Range")
    @pytest.mark.tag("checksum")
    def test_range_get_checksum(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 36)
        data = utils.random_text_to_long(5 * md.MB)

        for algorithm in checksum.ALL_ALGORITHMS:
            key = f"test_range_get_checksum/{algorithm}"
            params = {"Bucket": bucket_name, "Key": key, "Body": data.encode("utf-8")}
            checksum.apply_put_checksum_params(params, algorithm, data)
            response = client.put_object(**params)

            checksum.checksum_compare(algorithm, data, response)
            self.check_content_using_random_range(bucket_name, key, data, 50)

    @pytest.mark.tag("Header")
    def test_object_response_headers(self):
        key = "test_object_response_headers"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 30, [key])
        response = client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseCacheControl="no-cache",
            ResponseContentDisposition="bla",
            ResponseContentEncoding="aaa",
            ResponseContentLanguage="esperanto",
            ResponseContentType="foo/bar",
            ResponseExpires=datetime.now(timezone.utc),
        )
        assert response["CacheControl"] == "no-cache"
        assert response["ContentDisposition"] == "bla"
        assert response["ContentEncoding"] == "aaa"
        assert response["ContentLanguage"] == "esperanto"
        assert response["ContentType"] == "foo/bar"

    @pytest.mark.tag("Range")
    def test_multipart_object_range(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        key = "test_multipart_object_range"
        multipart_upload_data = self.complete_multipart_upload_data(client, bucket_name, key, 5 * md.MB, 5 * md.MB)
        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=1)
        body = self.get_body(response)
        assert body == multipart_upload_data.get_body()[: 5 * md.MB]

    @pytest.mark.tag("Get")
    def test_get_object_ignore(self):
        key = "test_get_object_ignore"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 32, [key])
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert response["ContentLength"] == len(key)

    @pytest.mark.tag("ERROR")
    def test_get_object_after_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        key = "test_get_object_after_delete"
        body = "testContent"
        client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body
        assert get_response["ContentLength"] == len(body)
        client.delete_object(Bucket=bucket_name, Key=key)
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY

    @pytest.mark.tag("ERROR")
    def test_get_object_after_delete_versioning(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 34)
        key = "test_get_object_after_delete_versioning"
        body = "testContent"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body
        assert get_response["ContentLength"] == len(body)
        client.delete_object(Bucket=bucket_name, Key=key)
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY

    @pytest.mark.tag("Versioning")
    def test_get_object_delete_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 35)
        key = "test_get_object_delete_marker"
        body = "testContent"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == body
        client.delete_object(Bucket=bucket_name, Key=key)
        list_response = client.list_object_versions(Bucket=bucket_name)
        versions = list_response.get("Versions", [])
        delete_markers = list_response.get("DeleteMarkers", [])
        assert len(delete_markers) == 1
        assert len(versions) == 1
        delete_marker = delete_markers[0]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, VersionId=delete_marker["VersionId"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 405
        assert exc_info.value.response["Error"]["Code"] == md.METHOD_NOT_ALLOWED
        version = versions[0]
        response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        assert self.get_body(response) == body

    def _put_digits(self, client, test_id: int, key: str) -> str:
        """10바이트(0123456789) 오브젝트를 하나 넣은 버킷을 만든다."""
        bucket_name = self.create_bucket(client, test_id)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"0123456789")
        return bucket_name

    @pytest.mark.tag("ERROR")
    def test_get_object_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(37)
        self.assert_client_error(lambda: client.get_object(Bucket=bucket_name, Key="key"), 404, md.NO_SUCH_BUCKET)
        # HEAD 는 본문이 없어 상태 코드만 온다
        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key="key"), 404, "404")

    @pytest.mark.tag("ERROR")
    def test_head_object_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 38)
        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key="key"), 404, "404")

    @pytest.mark.tag("Access")
    def test_get_object_access_denied(self):
        key = "test_get_object_access_denied"
        client = self.get_client()
        bucket_name = self._put_digits(client, 39, key)
        for other_client in (self.get_alt_client(), self.get_public_client()):
            self.assert_client_error(
                lambda other_client=other_client: other_client.get_object(Bucket=bucket_name, Key=key),
                403,
                md.ACCESS_DENIED,
            )
            # 목록 권한이 없으면 없는 키도 404 가 아니라 403 이다
            self.assert_client_error(
                lambda other_client=other_client: other_client.get_object(Bucket=bucket_name, Key="not_exist"),
                403,
                md.ACCESS_DENIED,
            )
            self.assert_client_error(
                lambda other_client=other_client: other_client.head_object(Bucket=bucket_name, Key=key), 403, "403"
            )

    @pytest.mark.tag("Range")
    def test_ranged_request_ignored(self):
        key = "test_ranged_request_ignored"
        client = self.get_client()
        bucket_name = self._put_digits(client, 40, key)
        # 여러 구간, 잘못된 형식, 다른 단위, 뒤집힌 구간은 무시하고 전체를 200 으로 돌려준다
        for value in ("bytes=0-1,3-4", "bytes=abc", "items=0-1", "bytes=5-2"):
            response = client.get_object(Bucket=bucket_name, Key=key, Range=value)
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ContentRange" not in response
            assert self.get_body(response) == "0123456789"

    @pytest.mark.tag("Range")
    def test_ranged_request_beyond_end(self):
        key = "test_ranged_request_beyond_end"
        client = self.get_client()
        bucket_name = self._put_digits(client, 41, key)
        # 끝이 크기를 넘으면 마지막 바이트까지로 잘린다
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=5-100")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["ContentRange"] == "bytes 5-9/10"
        assert self.get_body(response) == "56789"

        # 뒤에서부터의 길이가 크기를 넘으면 전체를 206 으로 돌려준다
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=-100")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["ContentRange"] == "bytes 0-9/10"
        assert self.get_body(response) == "0123456789"

        # 시작이 크기와 같거나, 뒤에서 0바이트를 요청하면 416
        for value in ("bytes=10-20", "bytes=-0"):
            self.assert_client_error(
                lambda value=value: client.get_object(Bucket=bucket_name, Key=key, Range=value),
                416,
                md.INVALID_RANGE,
            )

    @pytest.mark.tag("Range")
    def test_head_object_range(self):
        key = "test_head_object_range"
        client = self.get_client()
        bucket_name = self._put_digits(client, 42, key)
        response = client.head_object(Bucket=bucket_name, Key=key, Range="bytes=0-4")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["ContentLength"] == 5
        assert response["ContentRange"] == "bytes 0-4/10"

    @pytest.mark.tag("PartNumber")
    def test_get_object_part_number(self):
        key = "test_get_object_part_number"
        client = self.get_client()
        bucket_name = self._put_digits(client, 43, key)
        # 멀티파트가 아닌 오브젝트는 1번 파트가 전체다
        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=1)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["ContentRange"] == "bytes 0-9/10"
        assert "PartsCount" not in response
        assert self.get_body(response) == "0123456789"

        response = client.head_object(Bucket=bucket_name, Key=key, PartNumber=1)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["ContentLength"] == 10

        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, PartNumber=2), 416, md.INVALID_PART_NUMBER
        )

    @pytest.mark.tag("PartNumber")
    def test_get_object_part_number_invalid(self):
        key = "test_get_object_part_number_invalid"
        client = self.get_client()
        bucket_name = self._put_digits(client, 44, key)
        # 파트 번호는 1~10000
        for part_number in (0, 10001):
            self.assert_client_error(
                lambda part_number=part_number: client.get_object(
                    Bucket=bucket_name, Key=key, PartNumber=part_number
                ),
                400,
                md.INVALID_ARGUMENT,
            )
        # Range 와 partNumber 는 함께 쓸 수 없다
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, PartNumber=1, Range="bytes=0-1"),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Version")
    def test_get_object_version_id_unversioned(self):
        key = "test_get_object_version_id_unversioned"
        client = self.get_client()
        bucket_name = self._put_digits(client, 45, key)
        # 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key, VersionId="null")) == "0123456789"
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, VersionId="invalid"), 400, md.INVALID_ARGUMENT
        )
        self.assert_client_error(
            lambda: client.head_object(Bucket=bucket_name, Key=key, VersionId="invalid"), 400, "400"
        )

    @pytest.mark.tag("Version")
    def test_get_object_null_version_not_exist(self):
        key = "test_get_object_null_version_not_exist"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 46)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # 버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없다
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, VersionId="null"), 404, md.NO_SUCH_VERSION
        )
        self.assert_client_error(
            lambda: client.head_object(Bucket=bucket_name, Key=key, VersionId="null"), 404, "404"
        )

    @pytest.mark.tag("Header")
    def test_get_object_response_headers_anonymous(self):
        key = "test_get_object_response_headers_anonymous"
        client = self.get_client()
        bucket_name = self._put_digits(client, 47, key)
        # 익명 요청에는 response-* 재정의를 쓸 수 없다
        self.assert_client_error(
            lambda: self.get_public_client().get_object(
                Bucket=bucket_name, Key=key, ResponseContentType="text/html"
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Header")
    def test_head_object_response_headers(self):
        key = "test_head_object_response_headers"
        client = self.get_client()
        bucket_name = self._put_digits(client, 48, key)
        response = client.head_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentType="text/html",
            ResponseContentDisposition="inline",
            ResponseCacheControl="max-age=1",
            ResponseContentEncoding="identity",
            ResponseContentLanguage="en",
        )
        assert response["ContentType"] == "text/html"
        assert response["ContentDisposition"] == "inline"
        assert response["CacheControl"] == "max-age=1"
        assert response["ContentEncoding"] == "identity"
        assert response["ContentLanguage"] == "en"

    @pytest.mark.tag("Header")
    def test_get_object_default_headers(self):
        key = "test_get_object_default_headers"
        client = self.get_client()
        bucket_name = self._put_digits(client, 49, key)
        for response in (client.get_object(Bucket=bucket_name, Key=key), client.head_object(Bucket=bucket_name, Key=key)):
            assert response["AcceptRanges"] == "bytes"
            assert response["ContentType"] == "binary/octet-stream"
            assert response["ContentLength"] == 10
            assert response["ETag"] == '"781e5e245d69b566979b86e28d23f2c7"'
            assert response["LastModified"] is not None

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_wildcard(self):
        key = "test_get_object_if_match_wildcard"
        client = self.get_client()
        bucket_name = self._put_digits(client, 50, key)
        # If-Match: * 는 오브젝트가 있으면 통과, If-None-Match: * 는 304
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key, IfMatch="*")) == "0123456789"
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, IfNoneMatch="*"), 304, "304"
        )
