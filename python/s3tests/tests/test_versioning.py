"""Versioning tests ported from Java testV2/Versioning.java."""

from __future__ import annotations

import threading

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestVersioning(S3TestBase):
    @pytest.mark.tag("Check")
    def test_versioning_bucket_create_suspend(self):
        bucket_name = self.create_bucket(1)
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.check_configure_versioning_retry(bucket_name, "Suspended")

    @pytest.mark.tag("Object")
    def test_versioning_obj_create_read_remove(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 5
        self.do_test_create_remove_versions(client, bucket_name, key, num_versions, 0, 0)
        self.do_test_create_remove_versions(client, bucket_name, key, num_versions, 4, -1)

    @pytest.mark.tag("Object")
    def test_versioning_obj_create_read_remove_head(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 5
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(
            client, bucket_name, key, num_versions, version_ids, contents, True
        )

        removed_version_id = version_ids.pop(0)
        contents.pop(0)
        num_versions -= 1

        client.delete_object(Bucket=bucket_name, Key=key, VersionId=removed_version_id)

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == contents[-1]

        delete_response = client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version_id = delete_response["VersionId"]
        version_ids.append(delete_marker_version_id)

        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == num_versions
        assert len(list_response.get("DeleteMarkers", [])) == 1
        assert list_response["DeleteMarkers"][0]["VersionId"] == delete_marker_version_id

    @pytest.mark.tag("Object")
    def test_versioning_obj_plain_null_version_removal(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        key = "foo"
        content = "foo data"
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0

    @pytest.mark.tag("Object")
    def test_versioning_obj_plain_null_version_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        key = "foo"
        content = "foo zzz"
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        content2 = "zzz"
        client.put_object(Bucket=bucket_name, Key=key, Body=content2.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == content2
        version_id = response["VersionId"]
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == content
        client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0

    @pytest.mark.tag("Object")
    def test_versioning_obj_plain_null_version_overwrite_suspended(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        key = "foo"
        content = "foo zzz"
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        content2 = "zzz"
        client.put_object(Bucket=bucket_name, Key=key, Body=content2.encode("utf-8"))
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == content2
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 1
        client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        with pytest.raises(ClientError) as exc_info:
            client.get_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_KEY

    @pytest.mark.tag("Object")
    def test_versioning_obj_suspend_versions(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 5
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        self.overwrite_suspended_versioning_obj(
            client, bucket_name, key, version_ids, contents, "null content 1"
        )
        self.overwrite_suspended_versioning_obj(
            client, bucket_name, key, version_ids, contents, "null content 2"
        )
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        self.overwrite_suspended_versioning_obj(
            client, bucket_name, key, version_ids, contents, "null content 3"
        )
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.create_multiple_versions_track(client, bucket_name, key, 3, version_ids, contents, True)
        num_versions += 3
        for _ in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, 0)
        assert len(version_ids) == 0
        assert len(contents) == 0

    @pytest.mark.tag("Object")
    def test_versioning_obj_create_versions_remove_all(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 10
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        for _ in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, 0)
        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 0

    @pytest.mark.tag("Object")
    def test_versioning_obj_create_versions_remove_special_names(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        keys = ["_", ":", " "]
        num_versions = 10
        version_ids: list[str] = []
        contents: list[str] = []
        for key in keys:
            self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
            for _ in range(num_versions):
                self.remove_obj_version(client, bucket_name, key, version_ids, contents, 0)
            response = client.list_object_versions(Bucket=bucket_name)
            assert len(response.get("Versions", [])) == 0

    @pytest.mark.tag("Multipart")
    def test_versioning_obj_create_overwrite_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 3
        version_ids: list[str] = []
        contents: list[str] = []
        for _ in range(num_versions):
            contents.append(self.do_test_multipart_upload_contents(bucket_name, key, 3))
        response = client.list_object_versions(Bucket=bucket_name)
        for version in response.get("Versions", []):
            version_ids.append(version["VersionId"])
        version_ids.reverse()
        self.check_obj_versions(client, bucket_name, key, version_ids, contents)
        for _ in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, 0)
        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 0

    @pytest.mark.tag("Multipart")
    def test_versioning_obj_mix_put_and_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        self.check_configure_versioning_retry(bucket_name, "Enabled")

        key = "testVersioningObjMixPutAndMultipart"
        version_ids: list[str] = []
        contents: list[str] = []

        # putObject 1KB
        content_1kb = utils.random_text_to_long(1 * md.KB)
        put_1kb = client.put_object(Bucket=bucket_name, Key=key, Body=content_1kb.encode("utf-8"))
        assert put_1kb["VersionId"] is not None
        version_ids.append(put_1kb["VersionId"])
        contents.append(content_1kb)

        # MultipartUpload 50MB
        upload_50mb = self.setup_multipart_upload(client, bucket_name, key, 50 * md.MB)
        comp_50mb = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_50mb.upload_id,
            MultipartUpload=upload_50mb.completed_multipart_upload(),
        )
        assert comp_50mb["VersionId"] is not None
        version_ids.append(comp_50mb["VersionId"])
        contents.append(upload_50mb.get_body())

        # putObject 1MB
        content_1mb = utils.random_text_to_long(1 * md.MB)
        put_1mb = client.put_object(Bucket=bucket_name, Key=key, Body=content_1mb.encode("utf-8"))
        assert put_1mb["VersionId"] is not None
        version_ids.append(put_1mb["VersionId"])
        contents.append(content_1mb)

        # MultipartUpload 10MB
        upload_10mb = self.setup_multipart_upload(client, bucket_name, key, 10 * md.MB)
        comp_10mb = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_10mb.upload_id,
            MultipartUpload=upload_10mb.completed_multipart_upload(),
        )
        assert comp_10mb["VersionId"] is not None
        version_ids.append(comp_10mb["VersionId"])
        contents.append(upload_10mb.get_body())

        # listObjectVersions: 최신 버전부터 반환
        list_response = client.list_object_versions(Bucket=bucket_name)
        versions = list_response.get("Versions", [])
        assert len(versions) == 4
        expected_newest_first = list(reversed(version_ids))
        for i, version in enumerate(versions):
            assert version["Key"] == key
            assert version["VersionId"] == expected_newest_first[i]
            assert version["Size"] == len(contents[len(contents) - 1 - i])

        # 업로드 순서대로 versionId 지정 GetObject 후 내용 검증
        for i, version_id in enumerate(version_ids):
            get_response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
            body = self.get_body(get_response)
            assert body == contents[i], md.NOT_MATCHED

    @pytest.mark.tag("Check")
    def test_versioning_obj_list_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key1 = "obj"
        key2 = "obj-1"
        num_versions = 5
        version_ids: list[str] = []
        contents: list[str] = []
        version_ids2: list[str] = []
        contents2: list[str] = []
        for i in range(num_versions):
            body = f"content-{i}"
            response = client.put_object(Bucket=bucket_name, Key=key1, Body=body.encode("utf-8"))
            contents.append(body)
            version_ids.append(response["VersionId"])
        for i in range(num_versions):
            body = f"content-{i}"
            response = client.put_object(Bucket=bucket_name, Key=key2, Body=body.encode("utf-8"))
            contents2.append(body)
            version_ids2.append(response["VersionId"])
        list_response = client.list_object_versions(Bucket=bucket_name)
        versions = self.reverse_versions(list(list_response.get("Versions", [])))
        index = 0
        for i in range(5):
            version = versions[index]
            assert version["VersionId"] == version_ids2[i]
            assert version["Key"] == key2
            self.check_obj_content(client, bucket_name, key2, version["VersionId"], contents2[i])
            index += 1
        for i in range(5):
            version = versions[index]
            assert version["VersionId"] == version_ids[i]
            assert version["Key"] == key1
            self.check_obj_content(client, bucket_name, key1, version["VersionId"], contents[i])
            index += 1

    @pytest.mark.tag("Copy")
    def test_versioning_copy_obj_version(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "obj"
        num_versions = 3
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        for i in range(num_versions):
            new_key_name = f"key_{i}"
            client.copy_object(
                CopySource={"Bucket": bucket_name, "Key": key, "VersionId": version_ids[i]},
                Bucket=bucket_name,
                Key=new_key_name,
            )
            get_response = client.get_object(Bucket=bucket_name, Key=new_key_name)
            assert self.get_body(get_response) == contents[i]
        another_bucket_name = self.create_bucket(client, 12)
        for i in range(num_versions):
            new_key_name = f"key_{i}"
            client.copy_object(
                CopySource={"Bucket": bucket_name, "Key": key, "VersionId": version_ids[i]},
                Bucket=another_bucket_name,
                Key=new_key_name,
            )
            get_response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
            assert self.get_body(get_response) == contents[i]
        new_key_name2 = "newKey"
        client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=another_bucket_name,
            Key=new_key_name2,
        )
        response = client.get_object(Bucket=another_bucket_name, Key=new_key_name2)
        assert self.get_body(response) == contents[-1]

    @pytest.mark.tag("Delete")
    def test_versioning_multi_object_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "key"
        num_versions = 2
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        list_response = client.list_object_versions(Bucket=bucket_name)
        versions = self.reverse_versions(list(list_response.get("Versions", [])))
        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0
        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0

    @pytest.mark.tag("DeleteMarker")
    def test_versioning_multi_object_delete_with_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "key"
        num_versions = 2
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        client.delete_object(Bucket=bucket_name, Key=key)
        response = client.list_object_versions(Bucket=bucket_name)
        versions = response.get("Versions", [])
        delete_markers = response.get("DeleteMarkers", [])
        version_ids.append(delete_markers[0]["VersionId"])
        assert len(version_ids) == 3
        assert len(delete_markers) == 1
        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        for delete_marker in delete_markers:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=delete_marker["VersionId"])
        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 0
        assert len(response.get("DeleteMarkers", [])) == 0
        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version["VersionId"])
        for delete_marker in delete_markers:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=delete_marker["VersionId"])
        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 0
        assert len(response.get("DeleteMarkers", [])) == 0

    @pytest.mark.tag("DeleteMarker")
    def test_versioning_multi_object_delete_with_marker_create(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "key"
        client.delete_object(Bucket=bucket_name, Key=key)
        response = client.list_object_versions(Bucket=bucket_name)
        delete_marker = response.get("DeleteMarkers", [])
        assert len(delete_marker) == 1
        assert delete_marker[0]["Key"] == key

    @pytest.mark.tag("ACL")
    def test_versioned_object_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "xyz"
        num_versions = 3
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        version_id = version_ids[1]
        response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
        user = self.config.main_user.to_owner()
        assert user["ID"] == response["Owner"]["ID"]
        my_grants = [
            {
                "Grantee": self.config.main_user.to_grantee(),
                "Permission": "FULL_CONTROL",
            }
        ]
        self.check_grants(my_grants, response["Grants"])

    @pytest.mark.tag("ACL")
    def test_versioned_object_acl_no_version_specified(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 17)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "xyz"
        num_versions = 3
        version_ids: list[str] = []
        contents: list[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        acl = self.create_public_acl()
        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(acl, response)
        client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read")
        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        acl = self.create_public_acl("READ")
        self.check_acl(acl, response)

    @pytest.mark.tag("Check")
    def test_versioned_concurrent_object_create_and_remove(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "my_obj"
        num_versions = 3
        all_tasks: list[threading.Thread] = []
        for _ in range(3):
            all_tasks.extend(self.do_create_versioned_obj_concurrent(client, bucket_name, key, num_versions))
            all_tasks.extend(self.do_clear_versioned_bucket_concurrent(client, bucket_name))
        for task in all_tasks:
            task.join()
        t_list3 = self.do_clear_versioned_bucket_concurrent(client, bucket_name)
        for task in t_list3:
            task.join()
        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 0

    @pytest.mark.tag("Check")
    def test_versioning_bucket_atomic_upload_return_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        key = "bar"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=b"bar")
        version_id = put_response["VersionId"]
        list_response = client.list_object_versions(Bucket=bucket_name)
        for version in list_response.get("Versions", []):
            assert version["VersionId"] == version_id

    @pytest.mark.tag("MultiPart")
    def test_versioning_bucket_multipart_upload_return_version_id(self):
        size = 50 * md.MB
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        key = "bar"
        metadata = {"foo": "baz"}
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        upload_data = self.setup_multipart_upload(client, bucket_name, key, size, metadata=metadata)
        comp_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = comp_response["VersionId"]
        list_response = client.list_object_versions(Bucket=bucket_name)
        for version in list_response.get("Versions", []):
            assert version["VersionId"] == version_id

    @pytest.mark.tag("metadata")
    def test_versioning_get_object_head(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "foo"
        versions: list[str] = []
        for i in range(1, 6):
            response = client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=utils.random_text_to_long(i).encode("utf-8"),
            )
            versions.append(response["VersionId"])
        for i in range(5):
            response = client.head_object(Bucket=bucket_name, Key=key, VersionId=versions[i])
            assert response["ContentLength"] == i + 1

    @pytest.mark.tag("Delete")
    def test_versioning_latest(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        key = "foo"
        versions: list[str] = []
        for i in range(1, 6):
            response = client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=utils.random_text_to_long(i).encode("utf-8"),
            )
            versions.insert(0, response["VersionId"])
        while len(versions) > 1:
            delete_version_id = versions[0]
            versions.remove(delete_version_id)
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=delete_version_id)
            list_version = versions[0]
            response = client.head_object(Bucket=bucket_name, Key=key)
            assert response["VersionId"] == list_version

    @pytest.mark.tag("ERROR")
    def test_versioning_invalid_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        key = "testVersioningInvalidVersionId"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object(
                Bucket=bucket_name,
                Key=key,
                VersionId="f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59",
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_VERSION

    @pytest.mark.tag("Copy")
    def test_versioning_copy_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        source_key = "source"
        target_key = "target"
        content = "content-version1"
        expected_versions: list[str] = []
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        source_version1 = put_response["VersionId"]
        expected_versions.append(source_version1)
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Bucket=bucket_name,
            Key=target_key,
        )
        target_version1 = copy_response["VersionId"]
        expected_versions.append(target_version1)
        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_version1
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 2
        for version in list_response.get("Versions", []):
            assert version["VersionId"] in expected_versions
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Bucket=bucket_name,
            Key=target_key,
        )
        target_version2 = copy_response["VersionId"]
        expected_versions.append(target_version2)
        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_version2
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 3
        assert len(expected_versions) == len(list_response.get("Versions", []))
        for version in list_response.get("Versions", []):
            assert version["VersionId"] in expected_versions
        metadata = {"test-key": "test-value"}
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Bucket=bucket_name,
            Key=target_key,
            ContentType="text/plain",
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )
        target_version3 = copy_response["VersionId"]
        expected_versions.append(target_version3)
        metadata_response = client.head_object(Bucket=bucket_name, Key=target_key)
        assert metadata_response["Metadata"]["test-key"] == "test-value"
        assert metadata_response["ContentType"] == "text/plain"
        assert metadata_response["VersionId"] == target_version3
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 4
        assert len(expected_versions) == len(list_response.get("Versions", []))
        for version in list_response.get("Versions", []):
            assert version["VersionId"] in expected_versions
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Bucket=bucket_name,
            Key=target_key,
        )
        target_version4 = copy_response.get("VersionId")
        assert target_version4 is None or target_version4 == "null"
        expected_versions.append("null")
        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 5
        assert len(expected_versions) == len(list_response.get("Versions", []))
        for version in list_response.get("Versions", []):
            version_id = self.norm_version_id(version.get("VersionId"))
            assert version_id in expected_versions
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Bucket=bucket_name,
            Key=target_key,
        )
        target_version5 = copy_response.get("VersionId")
        assert target_version5 is None or target_version5 == "null"
        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 5
        assert len(expected_versions) == len(list_response.get("Versions", []))
        for version in list_response.get("Versions", []):
            version_id = self.norm_version_id(version.get("VersionId"))
            assert version_id in expected_versions

    @pytest.mark.tag("Object")
    def test_versioning_unversioned_all_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 25)
        key = "testVersioningUnversionedAllVersionId"
        multipart_key = key + "-multipart"
        copy_key = key + "-copy"
        content = "testContent"
        size = 5 * md.MB
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        assert put_response.get("VersionId") is None
        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response.get("VersionId") is None
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert get_response.get("VersionId") is None
        assert self.get_body(get_response) == content
        upload_data = self.setup_multipart_upload(client, bucket_name, multipart_key, size)
        comp_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=multipart_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        assert comp_response.get("VersionId") is None
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=bucket_name,
            Key=copy_key,
        )
        assert copy_response.get("VersionId") is None
        list_objects = client.list_objects(Bucket=bucket_name)
        assert len(list_objects.get("Contents", [])) == 3
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 3
        for version in list_versions.get("Versions", []):
            assert version["VersionId"] == "null"

    @pytest.mark.tag("Check")
    def test_versioning_enabled_all_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        key = "testVersioningEnabledAllVersionId"
        multipart_key = key + "-multipart"
        copy_key = key + "-copy"
        content = "testContent"
        size = 5 * md.MB
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]
        assert version_id is not None
        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["VersionId"] == version_id
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert get_response["VersionId"] == version_id
        assert self.get_body(get_response) == content
        upload_data = self.setup_multipart_upload(client, bucket_name, multipart_key, size)
        comp_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=multipart_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        multipart_version_id = comp_response["VersionId"]
        assert multipart_version_id is not None
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=bucket_name,
            Key=copy_key,
        )
        copy_version_id = copy_response["VersionId"]
        assert copy_version_id is not None
        list_objects = client.list_objects(Bucket=bucket_name)
        assert len(list_objects.get("Contents", [])) == 3
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 3
        version_ids = [v["VersionId"] for v in list_versions.get("Versions", [])]
        assert version_id in version_ids
        assert multipart_version_id in version_ids
        assert copy_version_id in version_ids

    @pytest.mark.tag("Check")
    def test_versioning_suspended_all_version_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        key = "testVersioningSuspendedAllVersionId"
        multipart_key = key + "-multipart"
        copy_key = key + "-copy"
        content = "testContent"
        size = 5 * md.MB
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        assert put_response.get("VersionId") is None
        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["VersionId"] == "null"
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert get_response["VersionId"] == "null"
        assert self.get_body(get_response) == content
        upload_data = self.setup_multipart_upload(client, bucket_name, multipart_key, size)
        comp_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=multipart_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        assert comp_response.get("VersionId") is None
        copy_response = client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=bucket_name,
            Key=copy_key,
        )
        copy_version_id = copy_response.get("VersionId")
        assert copy_version_id is None or copy_version_id == "null"
        list_objects = client.list_objects(Bucket=bucket_name)
        assert len(list_objects.get("Contents", [])) == 3
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 3
        for version in list_versions.get("Versions", []):
            assert version["VersionId"] == "null"

    @pytest.mark.tag("Check")
    def test_versioning_list_versions_off_enabled_suspended(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        key = "testVersioningListVersionsOffEnabledSuspended"
        content_off = "content-off"
        content_enabled = "content-enabled"
        content_suspended = "content-suspended"
        off_response = client.put_object(Bucket=bucket_name, Key=key, Body=content_off.encode("utf-8"))
        assert off_response.get("VersionId") is None
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        enabled_response = client.put_object(
            Bucket=bucket_name, Key=key, Body=content_enabled.encode("utf-8")
        )
        enabled_version_id = enabled_response["VersionId"]
        assert enabled_version_id is not None
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        suspended_response = client.put_object(
            Bucket=bucket_name, Key=key, Body=content_suspended.encode("utf-8")
        )
        assert suspended_response.get("VersionId") is None
        list_objects = client.list_objects(Bucket=bucket_name)
        assert len(list_objects.get("Contents", [])) == 1
        assert list_objects["Contents"][0]["Key"] == key
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 2
        version_ids = [v["VersionId"] for v in list_versions.get("Versions", [])]
        assert enabled_version_id in version_ids
        assert "null" in version_ids or None in version_ids
        latest = next(v for v in list_versions["Versions"] if v.get("IsLatest"))
        latest_version_id = self.norm_version_id(latest.get("VersionId"))
        assert latest_version_id == "null"
        assert latest["Size"] == len(content_suspended)
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == content_suspended
        get_by_version = client.get_object(
            Bucket=bucket_name, Key=key, VersionId=enabled_version_id
        )
        assert self.get_body(get_by_version) == content_enabled
        assert get_by_version["VersionId"] == enabled_version_id

    @pytest.mark.tag("Check")
    def test_versioning_list_versions_off_enabled_suspended_different_keys(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        key_off = "testVersioningListVersionsOff"
        key_enabled = "testVersioningListVersionsEnabled"
        key_suspended = "testVersioningListVersionsSuspended"
        content_off = "content-off"
        content_enabled = "content-enabled"
        content_suspended = "content-suspended"
        off_response = client.put_object(
            Bucket=bucket_name, Key=key_off, Body=content_off.encode("utf-8")
        )
        assert off_response.get("VersionId") is None
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        enabled_response = client.put_object(
            Bucket=bucket_name, Key=key_enabled, Body=content_enabled.encode("utf-8")
        )
        enabled_version_id = enabled_response["VersionId"]
        assert enabled_version_id is not None
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        suspended_response = client.put_object(
            Bucket=bucket_name, Key=key_suspended, Body=content_suspended.encode("utf-8")
        )
        assert suspended_response.get("VersionId") is None
        list_objects = client.list_objects(Bucket=bucket_name)
        assert len(list_objects.get("Contents", [])) == 3
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 3
        version_by_key = {v["Key"]: v["VersionId"] for v in list_versions["Versions"]}
        assert self.norm_version_id(version_by_key[key_off]) == "null"
        assert version_by_key[key_enabled] == enabled_version_id
        assert self.norm_version_id(version_by_key[key_suspended]) == "null"
        null_version_count = sum(
            1
            for v in list_versions["Versions"]
            if v.get("VersionId") is None or v.get("VersionId") == "null"
        )
        assert null_version_count == 2
        off_head_version_id = client.head_object(Bucket=bucket_name, Key=key_off).get("VersionId")
        assert off_head_version_id is None or off_head_version_id == "null"
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key_off)) == content_off
        assert client.head_object(Bucket=bucket_name, Key=key_enabled)["VersionId"] == enabled_version_id
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key_enabled)) == content_enabled
        assert client.head_object(Bucket=bucket_name, Key=key_suspended)["VersionId"] == "null"
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key_suspended)) == content_suspended

    @pytest.mark.tag("Check")
    def test_versioning_delete_null_version_after_suspend(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        key = "testVersioningDeleteNullVersionAfterSuspend"
        content_enabled = "content-enabled"
        content_suspended = "content-suspended"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content-off")
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        enabled_version_id = client.put_object(
            Bucket=bucket_name, Key=key, Body=content_enabled.encode("utf-8")
        )["VersionId"]
        assert enabled_version_id is not None
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        client.put_object(Bucket=bucket_name, Key=key, Body=content_suspended.encode("utf-8"))
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == content_suspended
        client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")
        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == content_enabled
        assert get_response["VersionId"] == enabled_version_id
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 1
        assert list_versions["Versions"][0]["VersionId"] == enabled_version_id

    @pytest.mark.tag("Check")
    def test_versioning_list_versions_multiple_enabled_then_suspended(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        key = "testVersioningListVersionsMultipleEnabledThenSuspended"
        enabled_version_ids: list[str] = []
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content-off")
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        for i in range(1, 4):
            version_id = client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=f"content-enabled-{i}".encode("utf-8"),
            )["VersionId"]
            assert version_id is not None
            enabled_version_ids.append(version_id)
        self.check_configure_versioning_retry(bucket_name, "Suspended")
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content-suspended")
        list_versions = client.list_object_versions(Bucket=bucket_name)
        assert len(list_versions.get("Versions", [])) == 4
        version_ids = [v["VersionId"] for v in list_versions["Versions"]]
        for enabled_version_id in enabled_version_ids:
            assert enabled_version_id in version_ids
        assert "null" in version_ids or None in version_ids
        latest = next(v for v in list_versions["Versions"] if v.get("IsLatest"))
        assert self.norm_version_id(latest.get("VersionId")) == "null"
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "content-suspended"

    @pytest.mark.tag("HeadObject")
    def test_versioning_head_object_delete_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 32)
        key = "testVersioningHeadObjectDeleteMarker"
        content = "testContent"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]
        head_response = client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["ContentLength"] == len(content)
        assert head_response["VersionId"] == version_id
        client.delete_object(Bucket=bucket_name, Key=key)
        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 1
        assert len(list_response.get("DeleteMarkers", [])) == 1
        assert list_response["DeleteMarkers"][0]["Key"] == key
        with pytest.raises(ClientError) as exc_info:
            client.head_object(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        # botocore HeadObject often surfaces Code as "404" (no XML body); Java SDK may report NoSuchKey.
        assert exc_info.value.response["Error"]["Code"] in (md.NO_SUCH_KEY, "404")
