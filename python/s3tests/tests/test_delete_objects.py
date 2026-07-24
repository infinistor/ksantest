"""DeleteObjects tests ported from Java testV2/DeleteObjects.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestDeleteObjects(S3TestBase):
    @pytest.mark.tag("ListObject")
    def test_multi_object_delete(self):
        key_names = ["testMultiObjectDelete0", "testMultiObjectDelete1", "testMultiObjectDelete2"]
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
        key_names = ["testMultiObjectV2Delete0", "testMultiObjectV2Delete1", "testMultiObjectV2Delete2"]
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
            "testMultiObjectDeleteVersions0",
            "testMultiObjectDeleteVersions1",
            "testMultiObjectDeleteVersions2",
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
            "testMultiObjectDeleteQuiet0",
            "testMultiObjectDeleteQuiet1",
            "testMultiObjectDeleteQuiet2",
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
        method_name = "testDeleteObjectsWithVersioning"
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
        key = "testDeleteObjectsWithVersioningDeleteMarker"
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
            "testVersioningMultiObjectDeleteWithMarker-0",
            "testVersioningMultiObjectDeleteWithMarker-1",
            "testVersioningMultiObjectDeleteWithMarker-2",
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
        key = "testVersioningMultiObjectDeleteWithMarkerCreate"
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
        key = "testVersioningMultiObjectDeleteWithMarkerCreateObjects"
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
        key = "testDeleteObjectIfMatchGood"
        etag = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["ETag"]
        client.delete_object(Bucket=bucket_name, Key=key, IfMatch=etag)
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("IfMatch")
    def test_delete_object_if_match_failed(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        key = "testDeleteObjectIfMatchFailed"
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
        key = "testDeleteObjectIfMatchAny"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key, IfMatch="*")
        list_response = client.list_objects(Bucket=bucket_name)
        assert len(list_response.get("Contents", [])) == 0

    @pytest.mark.tag("IfMatch")
    @pytest.mark.tag("IfNoneMatch")
    def test_delete_object_if_match_and_if_none_match(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        key = "testDeleteObjectIfMatchAndIfNoneMatch"
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
        key = "testDeleteObjectIfMatchAndIfNoneMatchAny"
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
        key_names = ["testDeleteObjectsIfMatchGood0", "testDeleteObjectsIfMatchGood1"]
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
        good_key = "testDeleteObjectsIfMatchMixedGood"
        bad_key = "testDeleteObjectsIfMatchMixedBad"
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
        key = "testDeleteObjectsIfMatchAndIfNoneMatch"
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
        key = "testDeleteObjectsIfMatchAndIfNoneMatchAny"
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
