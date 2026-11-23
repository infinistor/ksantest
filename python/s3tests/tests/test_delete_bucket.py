"""DeleteBucket tests ported from Java testV2/DeleteBucket.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestDeleteBucket(S3TestBase):
    @pytest.mark.tag("ERROR")
    def test_bucket_delete_not_exist(self):
        bucket_name = self.get_new_bucket_name_only(1)
        client = self.get_client()
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET

    @pytest.mark.tag("ERROR")
    def test_bucket_delete_nonempty(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 2, "foo")
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.BUCKET_NOT_EMPTY

    @pytest.mark.tag("ERROR")
    def test_bucket_create_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        client.delete_bucket(Bucket=bucket_name)
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("Versioning")
    def test_bucket_delete_versioned(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        key = "test_bucket_delete_versioned"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key)

        # 현재 오브젝트가 없어도 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없다
        assert client.list_objects_v2(Bucket=bucket_name).get("KeyCount", 0) == 0
        self.assert_client_error(lambda: client.delete_bucket(Bucket=bucket_name), 409, md.BUCKET_NOT_EMPTY)

        versions = client.list_object_versions(Bucket=bucket_name)
        for version in versions.get("Versions", []) + versions.get("DeleteMarkers", []):
            client.delete_object(Bucket=bucket_name, Key=version["Key"], VersionId=version["VersionId"])
        client.delete_bucket(Bucket=bucket_name)
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("Multipart")
    def test_bucket_delete_with_multipart_upload(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        key = "test_bucket_delete_with_multipart_upload"
        upload = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        client.upload_part(
            Bucket=bucket_name, Key=key, UploadId=upload["UploadId"], PartNumber=1, Body=b"a" * 1024
        )

        # 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있다
        client.delete_bucket(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.list_objects_v2(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("Permission")
    def test_bucket_delete_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 6)
        self.assert_client_error(lambda: alt_client.delete_bucket(Bucket=bucket_name), 403, md.ACCESS_DENIED)
        # 버킷은 그대로 남아 있다
        client.head_bucket(Bucket=bucket_name)

    @pytest.mark.tag("Check")
    def test_bucket_delete_with_configurations(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": [{"Key": "key", "Value": "value"}]})
        client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration={"CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]},
        )
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": 1}}]
            },
        )

        # 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있다
        client.delete_bucket(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.list_objects_v2(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("Check")
    def test_bucket_delete_recreate(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        key = "test_bucket_delete_recreate"
        client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": [{"Key": "key", "Value": "value"}]})
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key)
        client.delete_bucket(Bucket=bucket_name)

        # 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않다
        params = {"Bucket": bucket_name}
        self._apply_location_constraint(params)
        client.create_bucket(**params)
        assert client.list_objects_v2(Bucket=bucket_name).get("KeyCount", 0) == 0
        self.assert_client_error(
            lambda: client.get_bucket_tagging(Bucket=bucket_name), 404, "NoSuchTagSet"
        )
