"""DeleteObjects tests ported from Java testV2/DeleteObjects.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestDeleteObjects(S3TestBase):
    @pytest.mark.tag("ListObject")
    def test_multi_object_delete(self):
        key_names = ["test_multi_object_delete0", "test_multi_object_delete1", "test_multi_object_delete2"]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 1, key_names)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        object_list = self.get_key_versions(key_names)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("ListObjectsV2")
    def test_multi_object_v2_delete(self):
        key_names = ["test_multi_object_v2_delete0", "test_multi_object_v2_delete1", "test_multi_object_v2_delete2"]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 2, key_names)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        object_list = self.get_key_versions(key_names)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("Versioning")
    def test_multi_object_delete_versions(self):
        key_names = [
            "test_multi_object_delete_versions0",
            "test_multi_object_delete_versions1",
            "test_multi_object_delete_versions2",
        ]
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for key in key_names:
            self.create_multiple_versions(client, bucket_name, key, 3, check_versions=False)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        object_list = self.get_key_versions(key_names)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)

        list_response = client.list_objects_v2(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("quiet")
    def test_multi_object_delete_quiet(self):
        key_names = [
            "test_multi_object_delete_quiet0",
            "test_multi_object_delete_quiet1",
            "test_multi_object_delete_quiet2",
        ]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 4, key_names)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        object_list = self.get_key_versions(key_names)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list, "Quiet": True})
        assert len(del_response.get("Deleted", [])) == 0

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("Directory")
    def test_directory_delete(self):
        key_names = [
            "a/b/",
            "a/b/c/d/testDirectoryDelete1",
            "a/b/c/d/testDirectoryDelete2",
            "1/2/",
            "1/2/3/4/testDirectoryDelete1",
            "q/w/e/r/testDirectoryDelete",
        ]
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 5, key_names)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        client.delete_object(Bucket=bucket_name, Key="a/b/")
        client.delete_object(Bucket=bucket_name, Key="1/2/")
        client.delete_object(Bucket=bucket_name, Key="q/w/")

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 4

        client.delete_object(Bucket=bucket_name, Key="a/b/")
        client.delete_object(Bucket=bucket_name, Key="1/2/")
        client.delete_object(Bucket=bucket_name, Key="q/w/")

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 4

    @pytest.mark.tag("versioning")
    def test_directory_delete_versions(self):
        key_names = [
            "a/",
            "a/testDirectoryDeleteVersions1",
            "a/testDirectoryDeleteVersions2",
            "b/",
            "b/testDirectoryDeleteVersions1",
        ]
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for key in key_names:
            self.create_multiple_versions(client, bucket_name, key, 3, check_versions=False)

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == len(key_names)

        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 15

        client.delete_object(Bucket=bucket_name, Key="a/")

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 4

        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 15
        assert len(vers_response.get("DeleteMarkers", [])) == 1

        delete_list = ["a/obj1", "a/obj2"]
        object_list = self.get_key_versions(delete_list)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == 2

        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 15
        assert len(vers_response.get("DeleteMarkers", [])) == 3

    @pytest.mark.tag("DeleteObjects")
    def test_delete_objects(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        key_count = 100
        key_names = []
        for index in range(key_count):
            key = f"key-{index:03d}"
            key_names.append(key)
            client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == key_count

        object_list = self.get_key_versions(key_names)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == key_count

        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

        for key in key_names:
            with pytest.raises(ClientError) as exc_info:
                client.get_object(Bucket=bucket_name, Key=key)
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    @pytest.mark.tag("versioning")
    def test_delete_objects_with_versioning(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        method_name = "test_delete_objects_with_versioning"
        key_names = [f"{method_name}-{index}" for index in range(5)]

        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for key in key_names:
            self.create_multiple_versions(client, bucket_name, key, 2, check_versions=False)

        initial_vers_response = client.list_object_versions(Bucket=bucket_name)
        initial_versions = initial_vers_response.get("Versions", [])
        non_current_versions = []
        for key in key_names:
            key_versions = [version for version in initial_versions if version["Key"] == key]
            if key_versions:
                oldest_version = key_versions[-1]
                non_current_versions.append({"Key": oldest_version["Key"], "VersionId": oldest_version["VersionId"]})

        object_list = self.get_key_versions(key_names)
        mixed_delete_list = list(object_list) + list(non_current_versions)
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": mixed_delete_list})
        assert len(del_response.get("Deleted", [])) == len(key_names) + len(non_current_versions)

        vers_response = client.list_object_versions(Bucket=bucket_name)
        delete_markers = vers_response.get("DeleteMarkers", [])
        remaining_versions = vers_response.get("Versions", [])
        assert len(delete_markers) == 5
        assert len(remaining_versions) == 5

        final_versions = vers_response.get("Versions", [])
        final_delete_markers = vers_response.get("DeleteMarkers", [])
        delete_list = []
        for version in final_versions:
            delete_list.append({"Key": version["Key"], "VersionId": version["VersionId"]})
        for delete_marker in final_delete_markers:
            delete_list.append({"Key": delete_marker["Key"], "VersionId": delete_marker["VersionId"]})

        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_list})
        assert len(del_response.get("Deleted", [])) == len(final_versions) + len(final_delete_markers)

        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 0
        assert len(vers_response.get("DeleteMarkers", [])) == 0

    @pytest.mark.tag("versioning")
    def test_delete_objects_with_versioning_delete_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        key = "test_delete_objects_with_versioning_delete_marker"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key)
        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 1
        assert len(vers_response.get("DeleteMarkers", [])) == 1

    @pytest.mark.tag("versioning")
    def test_versioning_multi_object_delete_with_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        key_names = [
            "test_versioning_multi_object_delete_with_marker-0",
            "test_versioning_multi_object_delete_with_marker-1",
            "test_versioning_multi_object_delete_with_marker-2",
        ]
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for key in key_names:
            client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        object_list = self.get_key_versions(key_names)
        client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})

        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == len(key_names)
        assert len(vers_response.get("DeleteMarkers", [])) == len(key_names)

    @pytest.mark.tag("versioning")
    def test_versioning_multi_object_delete_with_marker_create(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        key = "test_versioning_multi_object_delete_with_marker_create"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for _ in range(10):
            client.delete_object(Bucket=bucket_name, Key=key)
        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 0
        assert len(vers_response.get("DeleteMarkers", [])) == 10

    @pytest.mark.tag("versioning")
    def test_versioning_multi_object_delete_with_marker_create_objects(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        key = "test_versioning_multi_object_delete_with_marker_create_objects"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for _ in range(10):
            object_list = self.get_key_versions([key])
            client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        vers_response = client.list_object_versions(Bucket=bucket_name)
        assert len(vers_response.get("Versions", [])) == 0
        assert len(vers_response.get("DeleteMarkers", [])) == 10

    @pytest.mark.tag("IfMatch")
    def test_delete_object_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        key = "test_delete_object_if_match_good"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        client.delete_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("IfMatch")
    def test_delete_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        key = "test_delete_object_if_match_failed"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, IfMatch="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert exc_info.value.response["Error"]["Code"] == md.PRECONDITION_FAILED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1

    @pytest.mark.tag("IfMatch")
    def test_delete_object_if_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        key = "test_delete_object_if_match_any"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key, IfMatch="*")
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_delete_object_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        key = "test_delete_object_if_match_and_if_none_match"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        with pytest.raises(ClientError) as exc_info:
            self.delete_object_with_headers(
                client,
                {"If-None-Match": etag},
                Bucket=bucket_name,
                Key=key,
                IfMatch=etag,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert exc_info.value.response["Error"]["Code"] == md.NOT_IMPLEMENTED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_delete_object_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        key = "test_delete_object_if_match_and_if_none_match_any"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        with pytest.raises(ClientError) as exc_info:
            self.delete_object_with_headers(
                client,
                {"If-None-Match": "*"},
                Bucket=bucket_name,
                Key=key,
                IfMatch=etag,
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert exc_info.value.response["Error"]["Code"] == md.NOT_IMPLEMENTED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1

    @pytest.mark.tag("IfMatch")
    def test_delete_objects_if_match_good(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        key_names = ["test_delete_objects_if_match_good0", "test_delete_objects_if_match_good1"]
        object_list = []
        for key in key_names:
            etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
            object_list.append({"Key": key, "ETag": etag})
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == len(key_names)
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("IfMatch")
    def test_delete_objects_if_match_mixed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        good_key = "test_delete_objects_if_match_mixed_good"
        bad_key = "test_delete_objects_if_match_mixed_bad"
        good_etag = client.put_object(Bucket=bucket_name, Key=good_key, Body=good_key.encode("utf-8"))["ETag"]
        client.put_object(Bucket=bucket_name, Key=bad_key, Body=bad_key.encode("utf-8"))
        object_list = [
            {"Key": good_key, "ETag": good_etag},
            {"Key": bad_key, "ETag": '"ABCDEFGHIJKLMNOPQRSTUVWXYZ"'},
        ]
        del_response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})
        assert len(del_response.get("Deleted", [])) == 1
        assert del_response["Deleted"][0]["Key"] == good_key
        assert len(del_response.get("Errors", [])) == 1
        assert del_response["Errors"][0]["Key"] == bad_key
        assert del_response["Errors"][0]["Code"] == md.PRECONDITION_FAILED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1
        assert list_response["Contents"][0]["Key"] == bad_key

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_delete_objects_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        key = "test_delete_objects_if_match_and_if_none_match"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        object_list = [{"Key": key}]
        with pytest.raises(ClientError) as exc_info:
            self.delete_objects_with_headers(
                client,
                {"If-Match": etag, "If-None-Match": etag},
                Bucket=bucket_name,
                Delete={"Objects": object_list},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert exc_info.value.response["Error"]["Code"] == md.NOT_IMPLEMENTED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_delete_objects_if_match_and_if_none_match_any(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)
        key = "test_delete_objects_if_match_and_if_none_match_any"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        object_list = [{"Key": key}]
        with pytest.raises(ClientError) as exc_info:
            self.delete_objects_with_headers(
                client,
                {"If-Match": etag, "If-None-Match": "*"},
                Bucket=bucket_name,
                Delete={"Objects": object_list},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert exc_info.value.response["Error"]["Code"] == md.NOT_IMPLEMENTED
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 1

    @pytest.mark.tag("Check")
    def test_delete_object_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        # 없는 키를 지워도 성공한다
        response = client.delete_object(Bucket=bucket_name, Key="not_exist")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
        assert "DeleteMarker" not in response
        assert "VersionId" not in response

    @pytest.mark.tag("ERROR")
    def test_delete_object_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(23)
        self.assert_client_error(lambda: client.delete_object(Bucket=bucket_name, Key="key"), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": "key"}]}),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("Access")
    def test_delete_object_access_denied(self):
        key = "test_delete_object_access_denied"
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_objects_keys(client, 24, [key])
        self.assert_client_error(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key), 403, md.ACCESS_DENIED)

        # DeleteObjects 는 요청 자체는 200 이고 키마다 AccessDenied 를 돌려준다
        response = alt_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key}]})
        assert not response.get("Deleted")
        assert len(response["Errors"]) == 1
        assert response["Errors"][0]["Key"] == key
        assert response["Errors"][0]["Code"] == md.ACCESS_DENIED

        client.head_object(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("Version")
    def test_delete_object_version_id_unversioned(self):
        key = "test_delete_object_version_id_unversioned"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 25, [key])
        self.assert_client_error(
            lambda: client.delete_object(Bucket=bucket_name, Key=key, VersionId="invalid"), 400, md.INVALID_ARGUMENT
        )
        client.head_object(Bucket=bucket_name, Key=key)

        # 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전으로 지울 수 있다
        response = client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        assert response["VersionId"] == "null"
        assert "DeleteMarker" not in response
        self.assert_client_error(lambda: client.get_object(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("Check")
    def test_delete_objects_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        keys = ["not_exist1", "not_exist2"]
        # 없는 키도 삭제된 것으로 응답한다
        response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key} for key in keys]})
        assert sorted(item["Key"] for item in response["Deleted"]) == keys
        assert not response.get("Errors")

    @pytest.mark.tag("ERROR")
    def test_delete_objects_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        self.assert_client_error(
            lambda: client.delete_objects(Bucket=bucket_name, Delete={"Objects": []}), 400, md.MALFORMED_XML
        )

    @pytest.mark.tag("Max")
    def test_delete_objects_max_keys(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        # 한 번에 1000개까지 지울 수 있다
        response = client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": [{"Key": f"key{i}"} for i in range(1000)]}
        )
        assert len(response["Deleted"]) == 1000
        self.assert_client_error(
            lambda: client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": [{"Key": f"key{i}"} for i in range(1001)]}
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Check")
    def test_delete_objects_duplicate_keys(self):
        key = "test_delete_objects_duplicate_keys"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 29, [key])
        # 같은 키를 두 번 넣어도 한 번만 응답한다
        response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key}, {"Key": key}]})
        assert [item["Key"] for item in response["Deleted"]] == [key]
        assert not response.get("Errors")

    @pytest.mark.tag("Version")
    def test_delete_objects_invalid_version_id(self):
        key = "test_delete_objects_invalid_version_id"
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 30, [key])

        def check_no_such_version():
            # 요청은 200 이고 키마다 NoSuchVersion 을 돌려준다
            response = client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": [{"Key": key, "VersionId": "invalid"}]}
            )
            assert not response.get("Deleted")
            assert len(response["Errors"]) == 1
            assert response["Errors"][0]["Key"] == key
            assert response["Errors"][0]["VersionId"] == "invalid"
            assert response["Errors"][0]["Code"] == md.NO_SUCH_VERSION

        check_no_such_version()
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        check_no_such_version()
        client.head_object(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("Version")
    def test_delete_object_versioning_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        # 버저닝 버킷에서는 없는 키를 지워도 삭제 마커가 생긴다
        response = client.delete_object(Bucket=bucket_name, Key="not_exist")
        assert response["DeleteMarker"] is True
        assert response["VersionId"]

        response = client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": "not_exist2"}]})
        assert response["Deleted"][0]["DeleteMarker"] is True
        assert response["Deleted"][0]["DeleteMarkerVersionId"]

        markers = client.list_object_versions(Bucket=bucket_name)["DeleteMarkers"]
        assert sorted(marker["Key"] for marker in markers) == ["not_exist", "not_exist2"]

    @pytest.mark.tag("Version")
    def test_delete_object_versioning_invalid_version_id(self):
        key = "test_delete_object_versioning_invalid_version_id"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 32)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=b"content")["VersionId"]
        self.assert_client_error(
            lambda: client.delete_object(Bucket=bucket_name, Key=key, VersionId="invalid"), 400, md.INVALID_ARGUMENT
        )

        # 없는 null 버전을 지워도 성공하고 삭제 마커는 생기지 않는다
        response = client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        assert response["VersionId"] == "null"
        assert "DeleteMarker" not in response

        response = client.list_object_versions(Bucket=bucket_name)
        assert [version["VersionId"] for version in response["Versions"]] == [version_id]
        assert not response.get("DeleteMarkers")

    @pytest.mark.tag("Version")
    def test_delete_object_remove_delete_marker(self):
        key = "test_delete_object_remove_delete_marker"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=b"content")["VersionId"]
        marker_id = client.delete_object(Bucket=bucket_name, Key=key)["VersionId"]
        self.assert_client_error(lambda: client.get_object(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)

        # 삭제 마커를 지우면 x-amz-delete-marker: true 로 응답하고 이전 버전이 다시 보인다
        response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=marker_id)
        assert response["DeleteMarker"] is True
        assert response["VersionId"] == marker_id
        assert client.get_object(Bucket=bucket_name, Key=key)["VersionId"] == version_id
