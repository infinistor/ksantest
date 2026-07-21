"""GetObject tests ported from Java testV2/GetObject.java."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils

PAST_DATE = datetime(1994, 9, 29, 19, 43, 31, tzinfo=timezone.utc)
FUTURE_DATE = datetime(2100, 9, 29, 19, 43, 31, tzinfo=timezone.utc)


class TestGetObject(S3TestBase):
    @pytest.mark.tag("ERROR")
    def test_object_read_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key="foo")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        response = client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(
                Bucket=bucket_name, Key=key, IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED

    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(
            Bucket=bucket_name, Key=key, IfNoneMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        )
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfModifiedSince")
    def test_get_object_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key, IfModifiedSince=PAST_DATE)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfModifiedSince")
    def test_get_object_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
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
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=PAST_DATE)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_get_object_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.get_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=FUTURE_DATE)
        assert self.get_body(response) == "bar"

    @pytest.mark.tag("IfMatch")
    def test_get_object_if_match_with_if_unmodified_since(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectIfMatchWithIfUnmodifiedSince"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectIfNoneMatchWithIfModifiedSince"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectIfMatchAndIfNoneMatch"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_get_object_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectIfMatchAndIfNoneMatchAny"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, IfMatch=etag, IfNoneMatch="*")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
        assert exc_info.value.response.get("Error", {}).get("Code") in (None, "", "304")

    @pytest.mark.tag("IfMatch")
    def test_head_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfMatchGood"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        response = client.head_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        assert response["ETag"] == etag

    @pytest.mark.tag("IfMatch")
    def test_head_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfMatchFailed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.head_object(
                Bucket=bucket_name, Key=key, IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    @pytest.mark.tag("IfNoneMatch")
    def test_head_object_if_none_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfNoneMatchGood"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")["ETag"]
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfNoneMatch=etag)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    @pytest.mark.tag("IfNoneMatch")
    def test_head_object_if_none_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfNoneMatchFailed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(
            Bucket=bucket_name, Key=key, IfNoneMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        )
        assert response["ContentLength"] == 3

    @pytest.mark.tag("IfModifiedSince")
    def test_head_object_if_modified_since_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfModifiedSinceGood"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key, IfModifiedSince=PAST_DATE)
        assert response["ContentLength"] == 3

    @pytest.mark.tag("IfModifiedSince")
    def test_head_object_if_modified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfModifiedSinceFailed"
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
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfUnmodifiedSinceGood"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=PAST_DATE)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    @pytest.mark.tag("ifUnmodifiedSince")
    def test_head_object_if_unmodified_since_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectIfUnmodifiedSinceFailed"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        response = client.head_object(Bucket=bucket_name, Key=key, IfUnmodifiedSince=FUTURE_DATE)
        assert response["ContentLength"] == 3

    @pytest.mark.tag("Range")
    def test_ranged_request_response_code(self):
        key = "obj"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=4-7")
        assert self.get_body(response) == content[4:8]
        assert response["ContentRange"] == "bytes 4-7/11"

    @pytest.mark.tag("Range")
    def test_ranged_big_request_response_code(self):
        key = "obj"
        content = utils.random_text_to_long(8 * md.MB)
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=3145728-5242880")
        assert self.get_body(response) == content[3145728:5242881]
        assert response["ContentRange"] == "bytes 3145728-5242880/8388608"

    @pytest.mark.tag("Range")
    def test_ranged_request_skip_leading_bytes_response_code(self):
        key = "obj"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=4-")
        assert self.get_body(response) == content[4:]
        assert response["ContentRange"] == "bytes 4-10/11"

    @pytest.mark.tag("Range")
    def test_ranged_request_return_trailing_bytes_response_code(self):
        key = "obj"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key, Range="bytes=-7")
        assert self.get_body(response) == content[-7:]
        assert response["ContentRange"] == "bytes 4-10/11"

    @pytest.mark.tag("Range")
    def test_ranged_request_invalid_range(self):
        key = "obj"
        content = "contentData"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, Range="bytes=40-50")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_RANGE

    @pytest.mark.tag("Range")
    def test_ranged_request_empty_object(self):
        key = "obj"
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key, Range="bytes=40-50")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_RANGE

    @pytest.mark.tag("Get")
    def test_get_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        data = utils.random_text_to_long(15 * md.MB)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content(bucket_name, key, data, 50)

    @pytest.mark.tag("Get")
    def test_range_object_many(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "foo"
        data = utils.random_text_to_long(15 * md.MB)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        self.check_content_using_random_range(bucket_name, key, data, 50)

    @pytest.mark.tag("Header")
    def test_object_response_headers(self):
        key = "testObjectResponseHeaders"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, [key])
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
        bucket_name = self.create_bucket(client)
        key = "testMultipartObjectRange"
        multipart_upload_data = self.complete_multipart_upload_data(
            client, bucket_name, key, 5 * md.MB, 5 * md.MB
        )
        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=1)
        body = self.get_body(response)
        assert body == multipart_upload_data.get_body()[: 5 * md.MB]

    @pytest.mark.tag("Get")
    def test_get_object_ignore(self):
        key = "testObjectIgnore"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, [key])
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert response["ContentLength"] == len(key)

    @pytest.mark.tag("ERROR")
    def test_get_object_after_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAfterDelete"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectAfterDeleteVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectDeleteMarker"
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
            client.get_object(
                Bucket=bucket_name, Key=key, VersionId=delete_marker["VersionId"]
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 405
        assert exc_info.value.response["Error"]["Code"] == md.METHOD_NOT_ALLOWED
        version = versions[0]
        response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        assert self.get_body(response) == body
