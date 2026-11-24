"""Backend tests ported from Java testV2/Backend.java."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import backend_headers as bh
from s3tests.data import main_data as md
from s3tests.data.multipart_upload_data import MultipartUploadData, MultipartUploadV2Data
from s3tests.test_base import DEFAULT_PART_SIZE, S3TestBase
from s3tests.utils import checksum, utils


# 복제 시 원본 GetObject 응답에서 대상으로 넘어오는 체크섬 알고리즘 목록
REPLICATION_CHECKSUMS = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"]


class TestBackend(S3TestBase):
    @staticmethod
    def _register_put_object_version_headers(client, version_id: str) -> None:
        def _inject(request, **kwargs):
            request.headers[bh.IFS_VERSION_ID] = version_id
            request.headers[bh.KSAN_VERSION_ID] = version_id

        client.meta.events.register("before-sign.s3.PutObject", _inject)

    @staticmethod
    def _register_copy_object_version_headers(client, version_id: str) -> None:
        def _inject(request, **kwargs):
            request.headers[bh.IFS_VERSION_ID] = version_id
            request.headers[bh.KSAN_VERSION_ID] = version_id

        client.meta.events.register("before-sign.s3.CopyObject", _inject)

    # @pytest.mark.tag("PUT")
    # def test_put_object(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 1)
    #     key = "test_put_object"
    #     content = "test content"

    #     response = backend_client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    #     get_response = client.get_object(Bucket=bucket_name, Key=key)
    #     assert self.get_body(get_response) == content

    # @pytest.mark.tag("GET")
    # def test_get_object(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 2)
    #     key = "test_get_object"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    #     response = backend_client.get_object(Bucket=bucket_name, Key=key)
    #     assert self.get_body(response) == content

    # @pytest.mark.tag("DELETE")
    # def test_delete_object(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 3)
    #     key = "test_delete_object"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    #     response = backend_client.delete_object(Bucket=bucket_name, Key=key)
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    #     list_response = client.list_objects_v2(Bucket=bucket_name)
    #     assert len(list_response.get("Contents", [])) == 0

    # @pytest.mark.tag("COPY")
    # def test_copy_object(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     source_bucket = self.create_bucket(client, 4)
    #     target_bucket = self.create_bucket(client, 4)
    #     source_key = "test_copy_object_source_key"
    #     target_key = "test_copy_object_target_key"
    #     content = "test content"

    #     client.put_object(Bucket=source_bucket, Key=source_key, Body=content.encode("utf-8"))

    #     response = backend_client.copy_object(
    #         CopySource={"Bucket": source_bucket, "Key": source_key},
    #         Bucket=target_bucket,
    #         Key=target_key,
    #     )
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    #     get_response = client.get_object(Bucket=target_bucket, Key=target_key)
    #     assert self.get_body(get_response) == content

    # @pytest.mark.tag("MULTIPART")
    # def test_multipart_upload(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 5)
    #     key = "test_multipart_upload"
    #     size = 10 * md.MB

    #     upload_data = self.setup_multipart_upload(backend_client, bucket_name, key, size, DEFAULT_PART_SIZE)
    #     complete_response = backend_client.complete_multipart_upload(
    #         Bucket=bucket_name,
    #         Key=key,
    #         UploadId=upload_data.upload_id,
    #         MultipartUpload=upload_data.completed_multipart_upload(),
    #     )
    #     version_id = complete_response["VersionId"]

    #     response = client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
    #     assert response["ContentLength"] == size

    #     self.check_content_using_range(bucket_name, key, upload_data.get_body(), md.MB)

    # @pytest.mark.tag("ACL")
    # def test_put_object_acl(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket_canned_acl(client, 6)
    #     key = "test_put_object_acl"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    #     response = backend_client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read")
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    #     acl_response = client.get_object_acl(Bucket=bucket_name, Key=key)
    #     assert len(acl_response["Grants"]) == 2

    # @pytest.mark.tag("ACL")
    # def test_get_object_acl(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket_canned_acl(client, 7)
    #     key = "test_get_object_acl"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"), ACL="public-read")

    #     response = backend_client.get_object_acl(Bucket=bucket_name, Key=key)
    #     assert len(response["Grants"]) == 2

    # @pytest.mark.tag("TAGGING")
    # def test_put_object_tagging(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 8)
    #     key = "test_put_object_tagging"
    #     content = "test content"
    #     tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    #     response = backend_client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging)
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    #     get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    #     assert len(get_response["TagSet"]) == 1
    #     assert get_response["TagSet"][0]["Key"] == "testKey"
    #     assert get_response["TagSet"][0]["Value"] == "testValue"

    # @pytest.mark.tag("TAGGING")
    # def test_get_object_tagging(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 9)
    #     key = "test_get_object_tagging"
    #     content = "test content"
    #     tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

    #     client.put_object(
    #         Bucket=bucket_name,
    #         Key=key,
    #         Body=content.encode("utf-8"),
    #         Tagging="testKey=testValue",
    #     )

    #     response = backend_client.get_object_tagging(Bucket=bucket_name, Key=key)
    #     assert len(response["TagSet"]) == 1
    #     assert response["TagSet"][0]["Key"] == "testKey"
    #     assert response["TagSet"][0]["Value"] == "testValue"

    # @pytest.mark.tag("TAGGING")
    # def test_delete_object_tagging(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client, 10)
    #     key = "test_delete_object_tagging"
    #     content = "test content"
    #     tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

    #     client.put_object(
    #         Bucket=bucket_name,
    #         Key=key,
    #         Body=content.encode("utf-8"),
    #         Tagging="testKey=testValue",
    #     )

    #     response = backend_client.delete_object_tagging(Bucket=bucket_name, Key=key)
    #     assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    #     get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    #     assert len(get_response["TagSet"]) == 0

    @pytest.mark.tag("PUT")
    def test_put_object_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 11)
        key = "test_put_object_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        response = backend_client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(get_response) == content

    @pytest.mark.tag("PUT")
    def test_put_object_versioning_with_version_id(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 12)
        key = "test_put_object_versioning_with_version_id_source"
        key2 = "test_put_object_versioning_with_version_id_target"
        content = "test content"
        content2 = "test content2"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        self._register_put_object_version_headers(backend_client, version_id)
        response = backend_client.put_object(Bucket=bucket_name, Key=key2, Body=content2.encode("utf-8"))
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_response = client.get_object(Bucket=bucket_name, Key=key2, VersionId=version_id)
        assert self.get_body(get_response) == content2
        assert get_response["VersionId"] == version_id

    @pytest.mark.tag("GET")
    def test_get_object_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 13)
        key = "test_get_object_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        response = backend_client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert self.get_body(response) == content

    @pytest.mark.tag("DELETE")
    def test_delete_object_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 14)
        key = "test_delete_object_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        response = backend_client.delete_object(Bucket=bucket_name, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("DeleteMarkers", [])) == 1

        delete_response = backend_client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
        )
        assert delete_response["ResponseMetadata"]["HTTPStatusCode"] == 204

        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0
        assert len(list_response.get("DeleteMarkers", [])) == 1

    @pytest.mark.tag("DELETE")
    def test_delete_objects_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 15)
        method_name = "test_delete_objects_versioning"
        key_names = [f"{method_name}-{i}" for i in range(5)]
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        for key in key_names:
            client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        object_list = self.get_key_versions(key_names)
        delete_response = backend_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": object_list},
        )
        assert len(delete_response.get("Deleted", [])) == 5

        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("DeleteMarkers", [])) == 5
        assert len(list_response.get("Versions", [])) == 5

        delete_list = []
        for version in list_response.get("Versions", []):
            delete_list.append({"Key": version["Key"], "VersionId": version["VersionId"]})
        for delete_marker in list_response.get("DeleteMarkers", []):
            delete_list.append({"Key": delete_marker["Key"], "VersionId": delete_marker["VersionId"]})

        final_delete_response = backend_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": delete_list},
        )
        assert len(final_delete_response.get("Deleted", [])) == 10

        list_response = client.list_object_versions(Bucket=bucket_name)
        assert len(list_response.get("Versions", [])) == 0
        assert len(list_response.get("DeleteMarkers", [])) == 0

    @pytest.mark.tag("HEAD")
    def test_head_object_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 16)
        key = "test_head_object_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        response = backend_client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["ContentLength"] == len(content)
        assert response["VersionId"] == version_id

    @pytest.mark.tag("COPY")
    def test_copy_object_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        source_bucket = self.create_bucket(client, 17)
        target_bucket = self.create_bucket(client, 17)
        source_key = "test_copy_object_versioning_source"
        source_key2 = "test_copy_object_versioning_source2"
        target_key = "test_copy_object_versioning_target"
        content = "test content"

        self.check_configure_versioning_retry(source_bucket, "Enabled")
        self.check_configure_versioning_retry(target_bucket, "Enabled")

        put_response = client.put_object(Bucket=source_bucket, Key=source_key, Body=content.encode("utf-8"))
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key, "VersionId": source_vid},
            Bucket=source_bucket,
            Key=source_key2,
        )
        target_vid = copy_response["VersionId"]

        self._register_copy_object_version_headers(backend_client, target_vid)
        backend_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key2, "VersionId": target_vid},
            Bucket=target_bucket,
            Key=target_key,
        )

        get_response = client.get_object(Bucket=target_bucket, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_vid

    @pytest.mark.tag("MULTIPART")
    def test_multipart_upload_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 18)
        key = "test_multipart_upload_versioning"
        size = 10 * md.MB

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        upload_data = self.setup_multipart_upload(backend_client, bucket_name, key, size, DEFAULT_PART_SIZE)
        complete_response = backend_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        response = client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert response["ContentLength"] == size

        self.check_content_using_range(bucket_name, key, upload_data.get_body(), md.MB)

    @pytest.mark.tag("ACL")
    def test_put_object_acl_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_canned_acl(client, 19)
        key = "test_put_object_acl_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        response = backend_client.put_object_acl(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            ACL="public-read",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert len(acl_response["Grants"]) == 2

    @pytest.mark.tag("ACL")
    def test_get_object_acl_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_canned_acl(client, 20)
        key = "test_get_object_acl_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"), ACL="public-read")

        response = backend_client.get_object_acl(Bucket=bucket_name, Key=key)
        assert len(response["Grants"]) == 2

    @pytest.mark.tag("TAGGING")
    def test_put_object_tagging_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 21)
        key = "test_put_object_tagging_versioning"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        response = backend_client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(get_response["TagSet"]) == 1
        assert get_response["TagSet"][0]["Key"] == "testKey"
        assert get_response["TagSet"][0]["Value"] == "testValue"

    @pytest.mark.tag("TAGGING")
    def test_get_object_tagging_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 22)
        key = "test_get_object_tagging_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            Tagging="testKey=testValue",
        )

        response = backend_client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(response["TagSet"]) == 1
        assert response["TagSet"][0]["Key"] == "testKey"
        assert response["TagSet"][0]["Value"] == "testValue"

    @pytest.mark.tag("TAGGING")
    def test_delete_object_tagging_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 23)
        key = "test_delete_object_tagging_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            Tagging="testKey=testValue",
        )

        response = backend_client.delete_object_tagging(Bucket=bucket_name, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(get_response["TagSet"]) == 0

    @pytest.mark.tag("RETENTION")
    def test_put_object_retention_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_object_lock(client, 24)
        key = "test_put_object_retention_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        retain_until_date = self.get_expired_date_instant(datetime.now(timezone.utc), 1)
        response = backend_client.put_object_retention(
            Bucket=bucket_name,
            Key=key,
            Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until_date},
            BypassGovernanceRetention=True,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=put_response["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("RETENTION")
    def test_get_object_retention_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_object_lock(client, 25)
        key = "test_get_object_retention_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        with pytest.raises(ClientError):
            backend_client.get_object_retention(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("RETENTION")
    def test_put_and_get_object_retention_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_object_lock(client, 26)
        key = "test_put_and_get_object_retention_versioning"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        retain_until_date = self.get_expired_date_instant(datetime.now(timezone.utc), 1)
        put_retention_response = backend_client.put_object_retention(
            Bucket=bucket_name,
            Key=key,
            Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until_date},
            BypassGovernanceRetention=True,
        )
        assert put_retention_response["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_retention_response = backend_client.get_object_retention(Bucket=bucket_name, Key=key)
        assert get_retention_response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert get_retention_response["Retention"]["Mode"] == "GOVERNANCE"
        assert get_retention_response["Retention"]["RetainUntilDate"] == retain_until_date

        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=put_response["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Replication")
    def test_put_object_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        source_bucket_name = self.create_bucket(client, 27)
        target_bucket_name = self.create_bucket(client, 27)
        key = "test_put_object_replication"
        content = "test content"

        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        put_response = client.put_object(Bucket=source_bucket_name, Key=key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, source_bucket_name, key, target_bucket_name, key, version_id)

        get_response = client.get_object(Bucket=target_bucket_name, Key=key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == version_id

    @pytest.mark.tag("Replication")
    def test_put_object_with_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        source_bucket_name = self.create_bucket(client, 28)
        target_bucket_name = self.create_bucket(client, 28)
        key = "test_put_object_with_tagging_replication"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        put_response = client.put_object(
            Bucket=source_bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            Tagging="testKey=testValue",
        )
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, source_bucket_name, key, target_bucket_name, key, version_id)

        get_response = client.get_object(Bucket=target_bucket_name, Key=key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == version_id

        tag_response = client.get_object_tagging(Bucket=target_bucket_name, Key=key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

    @pytest.mark.tag("Replication")
    def test_put_object_with_metadata_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        source_bucket_name = self.create_bucket(client, 29)
        target_bucket_name = self.create_bucket(client, 29)
        key = "test_put_object_with_metadata_replication"
        content = "test content"
        metadata = {"test-key": "testValue"}

        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        put_response = client.put_object(
            Bucket=source_bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            Metadata=metadata,
        )
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, source_bucket_name, key, target_bucket_name, key, version_id)

        get_response = client.get_object(Bucket=target_bucket_name, Key=key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == version_id
        assert get_response.get("Metadata", {}) == metadata

    @pytest.mark.tag("Replication")
    def test_copy_object_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket = self.create_bucket(client, 30)
        source_key = "test_copy_object_replication_source"
        source_key2 = "test_copy_object_replication_source2"
        target_key = "test_copy_object_replication_target"
        content = "test content"

        self.check_configure_versioning_retry(bucket, "Enabled")

        put_response = client.put_object(Bucket=bucket, Key=source_key, Body=content.encode("utf-8"))
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            CopySource={"Bucket": bucket, "Key": source_key, "VersionId": source_vid},
            Bucket=bucket,
            Key=source_key2,
        )
        target_vid = copy_response["VersionId"]

        self.backend_copy_object(backend_client, bucket, source_key2, bucket, target_key, target_vid, target_vid)

        get_response = client.get_object(Bucket=bucket, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_vid

    @pytest.mark.tag("Replication")
    def test_copy_object_with_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket = self.create_bucket(client, 31)
        source_key = "test_copy_object_with_tagging_replication_source"
        source_key2 = "test_copy_object_with_tagging_replication_source2"
        target_key = "test_copy_object_with_tagging_replication_target"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket, "Enabled")

        put_response = client.put_object(
            Bucket=bucket,
            Key=source_key,
            Body=content.encode("utf-8"),
            Tagging="testKey=testValue",
        )
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            CopySource={"Bucket": bucket, "Key": source_key, "VersionId": source_vid},
            Bucket=bucket,
            Key=source_key2,
        )
        target_vid = copy_response["VersionId"]

        self.backend_copy_object(backend_client, bucket, source_key2, bucket, target_key, target_vid, target_vid)

        get_response = client.get_object(Bucket=bucket, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_vid

        tag_response = client.get_object_tagging(Bucket=bucket, Key=target_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

    @pytest.mark.tag("Replication")
    def test_copy_object_with_metadata_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket = self.create_bucket(client, 32)
        source_key = "test_copy_object_with_metadata_replication_source"
        source_key2 = "test_copy_object_with_metadata_replication_source2"
        target_key = "test_copy_object_with_metadata_replication_target"
        content = "test content"
        metadata = {"test-key": "testValue"}

        self.check_configure_versioning_retry(bucket, "Enabled")

        put_response = client.put_object(
            Bucket=bucket,
            Key=source_key,
            Body=content.encode("utf-8"),
            Metadata=metadata,
        )
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            CopySource={"Bucket": bucket, "Key": source_key, "VersionId": source_vid},
            Bucket=bucket,
            Key=source_key2,
        )
        target_vid = copy_response["VersionId"]

        self.backend_copy_object(backend_client, bucket, source_key2, bucket, target_key, target_vid, target_vid)

        get_response = client.get_object(Bucket=bucket, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_vid
        assert get_response.get("Metadata", {}) == metadata

    @pytest.mark.tag("Replication")
    def test_copy_object_metadata_replace_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket = self.create_bucket(client, 33)
        source_key = "test_copy_object_metadata_replace_replication_source"
        source_key2 = "test_copy_object_metadata_replace_replication_source2"
        target_key = "test_copy_object_metadata_replace_replication_target"
        content = "test content"
        metadata = {"test-key": "testValue"}
        metadata2 = {"test-key2": "testValue2"}

        self.check_configure_versioning_retry(bucket, "Enabled")

        put_response = client.put_object(
            Bucket=bucket,
            Key=source_key,
            Body=content.encode("utf-8"),
            Metadata=metadata,
        )
        source_vid = put_response["VersionId"]

        copy_response = client.copy_object(
            CopySource={"Bucket": bucket, "Key": source_key, "VersionId": source_vid},
            Bucket=bucket,
            Key=source_key2,
            Metadata=metadata2,
            MetadataDirective="REPLACE",
        )
        target_vid = copy_response["VersionId"]

        self.backend_copy_object(backend_client, bucket, source_key2, bucket, target_key, target_vid, target_vid)

        get_response = client.get_object(Bucket=bucket, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == target_vid
        assert get_response.get("Metadata", {}) == metadata2

    @pytest.mark.tag("Replication")
    def test_multipart_upload_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 34)
        source_key = "test_multipart_upload_replication_source"
        target_key = "test_multipart_upload_replication_target"
        size = 10 * md.MB

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        upload_data = self.setup_multipart_upload(client, bucket_name, source_key, size, DEFAULT_PART_SIZE)
        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        self.backend_multipart_upload(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        get_response = client.head_object(Bucket=bucket_name, Key=target_key, VersionId=version_id)
        assert get_response["ContentLength"] == size
        assert get_response["VersionId"] == version_id

        self.check_content_using_range(bucket_name, target_key, upload_data.get_body(), md.MB, version_id)

    @pytest.mark.tag("Replication")
    def test_multipart_upload_with_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 35)
        source_key = "test_multipart_upload_with_tagging_replication_source"
        target_key = "test_multipart_upload_with_tagging_replication_target"
        size = 10 * md.MB
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        init_upload_data = MultipartUploadData()
        create_response = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            Tagging="testKey=testValue",
        )
        init_upload_data.upload_id = create_response["UploadId"]
        upload_data = self.multipart_upload(client, bucket_name, source_key, size, DEFAULT_PART_SIZE, init_upload_data)

        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=source_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

        self.backend_multipart_upload(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        get_response = client.head_object(Bucket=bucket_name, Key=target_key, VersionId=version_id)
        assert get_response["ContentLength"] == size
        assert get_response["VersionId"] == version_id

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=target_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

        self.check_content_using_range(bucket_name, target_key, upload_data.get_body(), md.MB, version_id)

    @pytest.mark.tag("Replication")
    def test_multipart_upload_with_metadata_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 36)
        source_key = "test_multipart_upload_with_metadata_replication_source"
        target_key = "test_multipart_upload_with_metadata_replication_target"
        size = 10 * md.MB
        metadata = {"test-key": "testValue"}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        upload_data = self.setup_multipart_upload(client, bucket_name, source_key, size, DEFAULT_PART_SIZE, metadata=metadata)
        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        metadata_response = client.head_object(Bucket=bucket_name, Key=source_key)
        assert metadata_response.get("Metadata", {}) == metadata

        self.backend_multipart_upload(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        get_response = client.head_object(Bucket=bucket_name, Key=target_key, VersionId=version_id)
        assert get_response["ContentLength"] == size
        assert get_response["VersionId"] == version_id
        assert get_response.get("Metadata", {}) == metadata

        self.check_content_using_range(bucket_name, target_key, upload_data.get_body(), md.MB, version_id)

    @pytest.mark.tag("Replication")
    def test_put_object_acl_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_canned_acl(client, 37)
        source_key = "test_put_object_acl_replication_source"
        target_key = "test_put_object_acl_replication_target"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        client.put_object_acl(Bucket=bucket_name, Key=source_key, ACL="public-read")

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)
        self.backend_put_object_acl(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == version_id

        acl_response = client.get_object_acl(Bucket=bucket_name, Key=target_key)
        assert len(acl_response["Grants"]) == 2
        self.check_acl(self.create_public_acl("READ"), acl_response)

    @pytest.mark.tag("Replication")
    def test_put_object_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 38)
        source_key = "test_put_object_tagging_replication_source"
        target_key = "test_put_object_tagging_replication_target"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=content.encode("utf-8"),
            Tagging="testKey=testValue",
        )
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)
        self.backend_put_object_tagging(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        get_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert self.get_body(get_response) == content
        assert get_response["VersionId"] == version_id

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=target_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

    @pytest.mark.tag("Replication")
    def test_delete_object_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 39)
        source_key = "test_delete_object_replication_source"
        target_key = "test_delete_object_replication_target"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        delete_response = client.delete_object(Bucket=bucket_name, Key=source_key)
        marker_version_id = delete_response["VersionId"]

        self.backend_delete_object(backend_client, bucket_name, target_key, marker_version_id)

        list_response = client.list_object_versions(Bucket=bucket_name)

        assert len(list_response.get("DeleteMarkers", [])) == 2
        assert list_response["DeleteMarkers"][0]["VersionId"] == marker_version_id
        assert list_response["DeleteMarkers"][1]["VersionId"] == marker_version_id

        assert len(list_response.get("Versions", [])) == 2
        assert list_response["Versions"][0]["VersionId"] == version_id
        assert list_response["Versions"][1]["VersionId"] == version_id

    @pytest.mark.tag("Replication")
    def test_delete_object_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 40)
        source_key = "test_delete_object_tagging_replication_source"
        target_key = "test_delete_object_tagging_replication_target"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        client.put_object_tagging(Bucket=bucket_name, Key=source_key, Tagging=tagging)

        self.backend_put_object_tagging(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=target_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

        tag_response2 = client.get_object_tagging(Bucket=bucket_name, Key=source_key)
        self.tag_compare(tagging["TagSet"], tag_response2["TagSet"])

        client.delete_object_tagging(Bucket=bucket_name, Key=target_key)

        self.backend_delete_object_tagging(backend_client, bucket_name, target_key, version_id)

        tag_response3 = client.get_object_tagging(Bucket=bucket_name, Key=source_key)
        self.tag_compare(tagging["TagSet"], tag_response3["TagSet"])

        tag_response4 = client.get_object_tagging(Bucket=bucket_name, Key=target_key)
        assert len(tag_response4["TagSet"]) == 0

    @pytest.mark.tag("PUT")
    def test_put_object_checksum_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 41)

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        for algorithm in REPLICATION_CHECKSUMS:
            key = f"client/{algorithm}"
            backend_key = f"backend/{algorithm}"
            value = checksum.calculate_checksum(algorithm, key)
            backend_value = checksum.calculate_checksum(algorithm, backend_key)

            # 대조군: 일반 클라이언트로 업로드하면 체크섬이 저장된다
            params = {"Bucket": bucket_name, "Key": key, "Body": key.encode("utf-8")}
            checksum.set_checksum(params, algorithm, value)
            client.put_object(**params)
            self._check_stored_checksum(client, bucket_name, key, None, algorithm, value)

            # Backend 클라이언트로 업로드
            params = {"Bucket": bucket_name, "Key": backend_key, "Body": backend_key.encode("utf-8")}
            checksum.set_checksum(params, algorithm, backend_value)
            backend_client.put_object(**params)
            self._check_stored_checksum(client, bucket_name, backend_key, None, algorithm, backend_value)

    @pytest.mark.tag("PUT")
    def test_put_object_checksum_versioning_with_version_id(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 42)

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        # 반복마다 원본 버전 ID가 바뀌므로 핸들러는 한 번만 등록하고 값을 바꿔 끼운다
        current = {"version_id": ""}

        def _inject(request, **kwargs):
            request.headers[bh.IFS_VERSION_ID] = current["version_id"]
            request.headers[bh.KSAN_VERSION_ID] = current["version_id"]

        backend_client.meta.events.register("before-sign.s3.PutObject", _inject)

        for algorithm in REPLICATION_CHECKSUMS:
            source_key = f"source/{algorithm}"
            target_key = f"target/{algorithm}"
            content = "test content"
            value = checksum.calculate_checksum(algorithm, content)

            params = {"Bucket": bucket_name, "Key": source_key, "Body": content.encode("utf-8")}
            checksum.set_checksum(params, algorithm, value)
            version_id = client.put_object(**params)["VersionId"]
            self._check_stored_checksum(client, bucket_name, source_key, version_id, algorithm, value)

            # Backend 클라이언트로 원본과 같은 버전 ID, 체크섬 값을 지정해 업로드
            current["version_id"] = version_id
            params = {"Bucket": bucket_name, "Key": target_key, "Body": content.encode("utf-8")}
            checksum.set_checksum(params, algorithm, value)
            backend_client.put_object(**params)
            self._check_stored_checksum(client, bucket_name, target_key, version_id, algorithm, value)

    @pytest.mark.tag("MULTIPART")
    def test_multipart_upload_checksum_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 43)

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        for algorithm in checksum.FULL_OBJECT_ALGORITHMS:
            key = f"client/{algorithm}"
            backend_key = f"backend/{algorithm}"

            # 대조군: 일반 클라이언트로 업로드하면 체크섬이 저장된다
            body = self._checksum_multipart_upload(client, bucket_name, key, algorithm)
            self._check_stored_checksum(
                client, bucket_name, key, None, algorithm, checksum.calculate_checksum(algorithm, body)
            )

            # Backend 클라이언트로 업로드
            backend_body = self._checksum_multipart_upload(backend_client, bucket_name, backend_key, algorithm)
            self._check_stored_checksum(
                client, bucket_name, backend_key, None, algorithm, checksum.calculate_checksum(algorithm, backend_body)
            )

    @pytest.mark.tag("MULTIPART")
    def test_multipart_upload_checksum_replication(self):
        """복제기와 같이 원본 체크섬 헤더(객체 전체 값)를 멀티파트 시작 요청에 복사하고 파트를 다르게 나눠
        Backend 헤더로 업로드해도 성공하는지 확인 (COMPOSITE, FULL_OBJECT)"""
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 47)

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        # 반복마다 값이 바뀌므로 핸들러는 한 번만 등록하고 값을 바꿔 끼운다
        current = {"checksum_headers": {}, "version_id": ""}

        def _inject_checksum(request, **kwargs):
            for name, value in current["checksum_headers"].items():
                request.headers[name] = value

        def _inject_version(request, **kwargs):
            request.headers[bh.IFS_VERSION_ID] = current["version_id"]
            request.headers[bh.KSAN_VERSION_ID] = current["version_id"]

        backend_client.meta.events.register("before-sign.s3.CreateMultipartUpload", _inject_checksum)
        backend_client.meta.events.register("before-sign.s3.CompleteMultipartUpload", _inject_version)

        for checksum_type, algorithms in (
            ("COMPOSITE", checksum.COMPOSITE_ALGORITHMS),
            ("FULL_OBJECT", checksum.FULL_OBJECT_ALGORITHMS),
        ):
            for algorithm in REPLICATION_CHECKSUMS:
                if algorithm not in algorithms:
                    continue
                source_key = f"source/{checksum_type}/{algorithm}"
                target_key = f"target/{checksum_type}/{algorithm}"

                # 일반 클라이언트로 6MB + 5MB 멀티파트 업로드
                upload_data = MultipartUploadV2Data()
                upload_data.upload_id = client.create_multipart_upload(
                    Bucket=bucket_name, Key=source_key, ChecksumType=checksum_type, ChecksumAlgorithm=algorithm
                )["UploadId"]
                for part in utils.generate_random_string(6 * md.MB, 5 * md.MB):
                    upload_data.append_body(part)
                    part_params = checksum.apply_put_checksum_params(
                        {
                            "Bucket": bucket_name,
                            "Key": source_key,
                            "UploadId": upload_data.upload_id,
                            "PartNumber": upload_data.next_part_number(),
                            "Body": part.encode("utf-8"),
                        },
                        algorithm,
                        part,
                    )
                    upload_data.add_part_with_checksum(algorithm, client.upload_part(**part_params))
                version_id = client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=source_key,
                    UploadId=upload_data.upload_id,
                    ChecksumType=checksum_type,
                    MultipartUpload={"Parts": upload_data.parts},
                )["VersionId"]
                body = upload_data.get_body()
                if checksum_type == "COMPOSITE":
                    value = checksum.calculate_checksum_by_base64(
                        algorithm, [checksum.get_checksum(p, algorithm) for p in upload_data.parts]
                    )
                else:
                    value = checksum.calculate_checksum(algorithm, body)
                self._check_stored_checksum(client, bucket_name, source_key, version_id, algorithm, value)

                # 복제기와 같이 Backend 클라이언트로 원본을 HEAD 하고 체크섬 헤더를 멀티파트 시작 요청에 복사
                head = backend_client.head_object(Bucket=bucket_name, Key=source_key, VersionId=version_id)
                current["checksum_headers"] = {
                    name: value
                    for name, value in head["ResponseMetadata"]["HTTPHeaders"].items()
                    if name.lower().startswith("x-amz-checksum-")
                }
                target_upload_id = backend_client.create_multipart_upload(Bucket=bucket_name, Key=target_key)[
                    "UploadId"
                ]

                # 원본과 다른 경계(5MB 단위)로 나눠 체크섬 없이 업로드
                target_parts = MultipartUploadV2Data()
                for start in range(0, len(body), 5 * md.MB):
                    response = backend_client.upload_part(
                        Bucket=bucket_name,
                        Key=target_key,
                        UploadId=target_upload_id,
                        PartNumber=target_parts.next_part_number(),
                        Body=body[start : start + 5 * md.MB].encode("utf-8"),
                    )
                    target_parts.add_part(response["ETag"])
                current["version_id"] = version_id
                backend_client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=target_key,
                    UploadId=target_upload_id,
                    MultipartUpload={"Parts": target_parts.parts},
                )

                # 업로드가 성공하고 내용이 같다. 대상 체크섬은 보지 않는다
                response = client.get_object(Bucket=bucket_name, Key=target_key, VersionId=version_id)
                assert response["Body"].read().decode("utf-8") == body, target_key

    @pytest.mark.tag("Replication")
    def test_put_object_retention_remove_replication(self):
        """GOVERNANCE 보존을 bypass + 빈 Retention 으로 제거하면 복제 대상의 보존도 제거되는지 확인"""
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket_object_lock(client, 44)
        source_key = "test_put_object_retention_remove_replication_source"
        target_key = "test_put_object_retention_remove_replication_target"
        content = "test content"
        retention = {
            "Mode": "GOVERNANCE",
            "RetainUntilDate": self.get_expired_date_instant(datetime.now(timezone.utc), 1),
        }

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        try:
            # Backend 클라이언트로 복사
            self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

            # 원본에 GOVERNANCE 보존 설정 후 Backend 클라이언트로 보존 복제
            client.put_object_retention(Bucket=bucket_name, Key=source_key, Retention=retention)
            self._backend_put_object_retention(
                backend_client, bucket_name, source_key, bucket_name, target_key, version_id
            )
            target = client.get_object_retention(Bucket=bucket_name, Key=target_key, VersionId=version_id)
            assert target["Retention"] == retention

            # bypass 없이 빈 Retention 으로 제거하면 거부
            self.assert_client_error(
                lambda: client.put_object_retention(Bucket=bucket_name, Key=source_key, Retention={}),
                403,
                md.ACCESS_DENIED,
            )

            # bypass + 빈 Retention 으로 원본 보존 제거
            client.put_object_retention(
                Bucket=bucket_name, Key=source_key, Retention={}, BypassGovernanceRetention=True
            )
            self._assert_no_retention(client, bucket_name, source_key, version_id)

            # Backend 클라이언트로 보존 제거 복제
            self._backend_put_object_retention(
                backend_client, bucket_name, source_key, bucket_name, target_key, version_id
            )
            self._assert_no_retention(client, bucket_name, target_key, version_id)

            # 제거 뒤 다시 설정한 보존도 복제
            client.put_object_retention(Bucket=bucket_name, Key=source_key, Retention=retention)
            self._backend_put_object_retention(
                backend_client, bucket_name, source_key, bucket_name, target_key, version_id
            )
            target = client.get_object_retention(Bucket=bucket_name, Key=target_key, VersionId=version_id)
            assert target["Retention"] == retention
        finally:
            for key in (source_key, target_key):
                client.delete_object(
                    Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True
                )

    @pytest.mark.tag("Replication")
    def test_put_object_replication_status(self):
        """복제 상태 갱신 요청(PUT /{bucket}/-)으로 원본의 x-amz-replication-status 가 COMPLETED, FAILED 로 바뀌는지 확인"""
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 45)
        key = "test_put_object_replication_status"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        # 일반 클라이언트로 업로드. 갱신 전에는 복제 상태가 없다
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["VersionId"]
        assert "ReplicationStatus" not in client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)

        objects = [
            {"Key": key, "VersionId": version_id},
            {"Key": "not-exist-key", "VersionId": version_id},
        ]

        # Backend 클라이언트로 COMPLETED 기록. 없는 객체가 섞여 있어도 200
        response = self._backend_put_replication_status(
            backend_client, bucket_name, bh.HEADER_REPLICATION_STATUS_COMPLETED, objects
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        head = client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert head.get("ReplicationStatus") == "COMPLETED"
        get = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        get["Body"].read()
        assert get.get("ReplicationStatus") == "COMPLETED"

        # Backend 클라이언트로 FAILED 기록
        self._backend_put_replication_status(
            backend_client, bucket_name, bh.HEADER_REPLICATION_STATUS_FAILED, objects
        )
        head = client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert head.get("ReplicationStatus") == "FAILED"

        # 갱신 요청이 "-" 객체나 새 버전을 만들지 않는다
        versions = client.list_object_versions(Bucket=bucket_name).get("Versions", [])
        assert [(v["Key"], v["VersionId"]) for v in versions] == [(key, version_id)]

    @pytest.mark.tag("Replication")
    def test_put_object_replication_status_invalid(self):
        """복제 상태 갱신 요청에 REPLICA 상태나 빈 ObjectList 를 보내면 InvalidArgument 로 거부되는지 확인"""
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client, 46)
        key = "test_put_object_replication_status_invalid"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))["VersionId"]
        objects = [{"Key": key, "VersionId": version_id}]

        # REPLICA 는 갱신할 수 없다
        self.assert_client_error(
            lambda: self._backend_put_replication_status(
                backend_client, bucket_name, bh.HEADER_REPLICATION_STATUS_REPLICA, objects
            ),
            400,
            md.INVALID_ARGUMENT,
        )

        # 빈 ObjectList 는 거부된다
        self.assert_client_error(
            lambda: self._backend_put_replication_status(
                backend_client, bucket_name, bh.HEADER_REPLICATION_STATUS_COMPLETED, []
            ),
            400,
            md.INVALID_ARGUMENT,
        )

        # 복제 상태는 그대로 없고, "-" 객체도 만들어지지 않는다
        assert "ReplicationStatus" not in client.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        versions = client.list_object_versions(Bucket=bucket_name).get("Versions", [])
        assert [v["Key"] for v in versions] == [key]

    # 복제 상태 갱신 API 의 객체 이름. GW 는 Backend 헤더 + PUT + 이 이름 + x-amz-replication-status 헤더를 이 API 로 처리한다.
    REPLICATION_STATUS_KEY = "-"

    @classmethod
    def _backend_put_replication_status(cls, backend_client, bucket_name: str, status: str, objects: list) -> dict:
        """복제기와 같이 원본 객체들의 복제 상태를 갱신한다.
        PUT /{bucket}/- 에 x-amz-replication-status 헤더와 {"ObjectList":[{"path","versionId"}]} 본문을 보낸다."""
        body = json.dumps(
            {"ObjectList": [{"path": o["Key"], "versionId": o["VersionId"]} for o in objects]},
            separators=(",", ":"),
        )

        def _inject(request, **kwargs):
            request.headers[bh.HEADER_REPLICATION_STATUS] = status
            request.headers[bh.KSAN_REPLICATION] = bh.HEADER_DATA

        event = "before-sign.s3.PutObject"
        backend_client.meta.events.register(event, _inject, unique_id="replication-status")
        try:
            return backend_client.put_object(Bucket=bucket_name, Key=cls.REPLICATION_STATUS_KEY, Body=body.encode("utf-8"))
        finally:
            backend_client.meta.events.unregister(event, unique_id="replication-status")

    @staticmethod
    def _backend_put_object_retention(
        backend_client, source_bucket: str, source_key: str, target_bucket: str, target_key: str, version_id: str
    ) -> None:
        """복제기와 같이 원본 보존을 읽어 대상에 설정한다. 원본에 보존이 없으면 빈 Retention 으로 대상 보존을 제거한다."""
        try:
            retention = backend_client.get_object_retention(
                Bucket=source_bucket, Key=source_key, VersionId=version_id
            )["Retention"]
        except ClientError as e:
            assert e.response["Error"]["Code"] == md.NO_SUCH_OBJECT_LOCK_CONFIGURATION
            retention = {}
        response = backend_client.put_object_retention(
            Bucket=target_bucket,
            Key=target_key,
            VersionId=version_id,
            Retention=retention,
            BypassGovernanceRetention=True,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def _assert_no_retention(self, client, bucket_name: str, key: str, version_id: str) -> None:
        """GetObjectRetention 이 NoSuchObjectLockConfiguration(404) 인지 확인"""
        self.assert_client_error(
            lambda: client.get_object_retention(Bucket=bucket_name, Key=key, VersionId=version_id),
            404,
            md.NO_SUCH_OBJECT_LOCK_CONFIGURATION,
        )

    @staticmethod
    def _checksum_multipart_upload(client, bucket_name: str, key: str, algorithm: str) -> str:
        """FULL_OBJECT 체크섬을 지정해 파트 2개로 멀티파트 업로드하고 업로드한 본문을 반환"""
        upload_data = MultipartUploadV2Data()
        upload_data.upload_id = client.create_multipart_upload(
            Bucket=bucket_name, Key=key, ChecksumType="FULL_OBJECT", ChecksumAlgorithm=algorithm
        )["UploadId"]
        for part in utils.generate_random_string(6 * md.MB, 5 * md.MB):
            upload_data.append_body(part)
            part_params = checksum.apply_put_checksum_params(
                {
                    "Bucket": bucket_name,
                    "Key": key,
                    "UploadId": upload_data.upload_id,
                    "PartNumber": upload_data.next_part_number(),
                    "Body": part.encode("utf-8"),
                },
                algorithm,
                part,
            )
            upload_data.add_part_with_checksum(algorithm, client.upload_part(**part_params))
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            ChecksumType="FULL_OBJECT",
            MultipartUpload={"Parts": upload_data.parts},
        )
        return upload_data.get_body()

    @staticmethod
    def _check_stored_checksum(client, bucket_name: str, key: str, version_id, algorithm: str, expected: str) -> None:
        """일반 클라이언트로 HeadObject(ChecksumMode=ENABLED)해 저장된 체크섬이 기대값과 같은지 확인"""
        params = {"Bucket": bucket_name, "Key": key, "ChecksumMode": "ENABLED"}
        if version_id:
            params["VersionId"] = version_id
        response = client.head_object(**params)
        assert checksum.get_checksum(response, algorithm) == expected, f"{key} {algorithm} 체크섬"
        if version_id:
            assert response["VersionId"] == version_id
