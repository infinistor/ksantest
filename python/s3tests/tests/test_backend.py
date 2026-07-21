"""Backend tests ported from Java testV2/Backend.java."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import backend_headers as bh
from s3tests.data import main_data as md
from s3tests.data.multipart_upload_data import MultipartUploadData
from s3tests.test_base import DEFAULT_PART_SIZE, S3TestBase


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
    #     bucket_name = self.create_bucket(client)
    #     key = "testPutObject"
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
    #     bucket_name = self.create_bucket(client)
    #     key = "testGetObject"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    #     response = backend_client.get_object(Bucket=bucket_name, Key=key)
    #     assert self.get_body(response) == content

    # @pytest.mark.tag("DELETE")
    # def test_delete_object(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client)
    #     key = "testDeleteObject"
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
    #     source_bucket = self.create_bucket(client)
    #     target_bucket = self.create_bucket(client)
    #     source_key = "sourceKey"
    #     target_key = "targetKey"
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
    #     bucket_name = self.create_bucket(client)
    #     key = "testMultipartUpload"
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
    #     bucket_name = self.create_bucket_canned_acl(client)
    #     key = "testPutObjectAcl"
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
    #     bucket_name = self.create_bucket_canned_acl(client)
    #     key = "testGetObjectAcl"
    #     content = "test content"

    #     client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"), ACL="public-read")

    #     response = backend_client.get_object_acl(Bucket=bucket_name, Key=key)
    #     assert len(response["Grants"]) == 2

    # @pytest.mark.tag("TAGGING")
    # def test_put_object_tagging(self):
    #     self.skip_if_aws()
    #     client = self.get_client()
    #     backend_client = self.get_backend_client()
    #     bucket_name = self.create_bucket(client)
    #     key = "testPutObjectTagging"
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
    #     bucket_name = self.create_bucket(client)
    #     key = "testGetObjectTagging"
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
    #     bucket_name = self.create_bucket(client)
    #     key = "testDeleteObjectTagging"
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
        bucket_name = self.create_bucket(client)
        key = "testPutObjectVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testPutObjectVersioningWithVersionIdSource"
        key2 = "testPutObjectVersioningWithVersionIdTarget"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testDeleteObjectVersioning"
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
        bucket_name = self.create_bucket(client)
        method_name = "testDeleteObjectsVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testHeadObjectVersioning"
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
        source_bucket = self.create_bucket(client)
        target_bucket = self.create_bucket(client)
        source_key = "sourceKey"
        source_key2 = "sourceKey2"
        target_key = "targetKey"
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
        bucket_name = self.create_bucket(client)
        key = "testMultipartUploadVersioning"
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
        bucket_name = self.create_bucket_canned_acl(client)
        key = "testPutObjectAclVersioning"
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
        bucket_name = self.create_bucket_canned_acl(client)
        key = "testGetObjectAclVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testPutObjectTaggingVersioning"
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
        bucket_name = self.create_bucket(client)
        key = "testGetObjectTaggingVersioning"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

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
        bucket_name = self.create_bucket(client)
        key = "testDeleteObjectTaggingVersioning"
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
        bucket_name = self.get_new_bucket_name()
        key = "testPutObjectRetentionVersioning"
        content = "test content"

        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
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
        bucket_name = self.get_new_bucket_name()
        key = "testGetObjectRetentionVersioning"
        content = "test content"

        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        self.check_configure_versioning_retry(bucket_name, "Enabled")

        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        with pytest.raises(ClientError):
            backend_client.get_object_retention(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("RETENTION")
    def test_put_and_get_object_retention_versioning(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.get_new_bucket_name()
        key = "testPutAndGetObjectRetentionVersioning"
        content = "test content"

        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
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
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        key = "testBackendReplication"
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
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        key = "testBackendReplicationTagging"
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
        source_bucket_name = self.create_bucket(client)
        target_bucket_name = self.create_bucket(client)
        key = "testBackendReplicationMetadata"
        content = "test content"
        metadata = {"testKey": "testValue"}

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
        bucket = self.create_bucket(client)
        source_key = "sourceKey"
        source_key2 = "sourceKey2"
        target_key = "targetKey"
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
        bucket = self.create_bucket(client)
        source_key = "sourceKey"
        source_key2 = "sourceKey2"
        target_key = "targetKey"
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
        bucket = self.create_bucket(client)
        source_key = "sourceKey"
        source_key2 = "sourceKey2"
        target_key = "targetKey"
        content = "test content"
        metadata = {"testKey": "testValue"}

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
        bucket = self.create_bucket(client)
        source_key = "sourceKey"
        source_key2 = "sourceKey2"
        target_key = "targetKey"
        content = "test content"
        metadata = {"testKey": "testValue"}
        metadata2 = {"testKey2": "testValue2"}

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
        bucket_name = self.create_bucket(client)
        source_key = "testMultipartUploadReplicationSource"
        target_key = "testMultipartUploadReplicationTarget"
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

        self.backend_multipart_upload(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

        get_response = client.head_object(Bucket=bucket_name, Key=target_key, VersionId=version_id)
        assert get_response["ContentLength"] == size
        assert get_response["VersionId"] == version_id

        self.check_content_using_range(bucket_name, target_key, upload_data.get_body(), md.MB, version_id)

    @pytest.mark.tag("Replication")
    def test_multipart_upload_with_tagging_replication(self):
        self.skip_if_aws()
        client = self.get_client()
        backend_client = self.get_backend_client()
        bucket_name = self.create_bucket(client)
        source_key = "testMultipartUploadTaggingReplicationSource"
        target_key = "testMultipartUploadTaggingReplicationTarget"
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
        upload_data = self.multipart_upload(
            client, bucket_name, source_key, size, DEFAULT_PART_SIZE, init_upload_data
        )

        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        tag_response = client.get_object_tagging(Bucket=bucket_name, Key=source_key)
        self.tag_compare(tagging["TagSet"], tag_response["TagSet"])

        self.backend_multipart_upload(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

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
        bucket_name = self.create_bucket(client)
        source_key = "testMultipartUploadMetadataReplicationSource"
        target_key = "testMultipartUploadMetadataReplicationTarget"
        size = 10 * md.MB
        metadata = {"testKey": "testValue"}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        upload_data = self.setup_multipart_upload(
            client, bucket_name, source_key, size, DEFAULT_PART_SIZE, metadata=metadata
        )
        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=source_key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        version_id = complete_response["VersionId"]

        metadata_response = client.head_object(Bucket=bucket_name, Key=source_key)
        assert metadata_response.get("Metadata", {}) == metadata

        self.backend_multipart_upload(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

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
        bucket_name = self.create_bucket_canned_acl(client)
        source_key = "testPutObjectAclReplicationSource"
        target_key = "testPutObjectAclReplicationTarget"
        content = "test content"

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        client.put_object_acl(Bucket=bucket_name, Key=source_key, ACL="public-read")

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)
        self.backend_put_object_acl(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

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
        bucket_name = self.create_bucket(client)
        source_key = "testPutObjectTaggingReplicationSource"
        target_key = "testPutObjectTaggingReplicationTarget"
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
        self.backend_put_object_tagging(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

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
        bucket_name = self.create_bucket(client)
        source_key = "testDeleteObjectReplicationSource"
        target_key = "testDeleteObjectReplicationTarget"
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
        bucket_name = self.create_bucket(client)
        source_key = "testDeleteObjectTaggingReplicationSource"
        target_key = "testDeleteObjectTaggingReplicationTarget"
        content = "test content"
        tagging = {"TagSet": [{"Key": "testKey", "Value": "testValue"}]}

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        put_response = client.put_object(Bucket=bucket_name, Key=source_key, Body=content.encode("utf-8"))
        version_id = put_response["VersionId"]

        self.backend_put_object(backend_client, bucket_name, source_key, bucket_name, target_key, version_id)

        client.put_object_tagging(Bucket=bucket_name, Key=source_key, Tagging=tagging)

        self.backend_put_object_tagging(
            backend_client, bucket_name, source_key, bucket_name, target_key, version_id
        )

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
