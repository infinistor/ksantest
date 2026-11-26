"""BucketOwner tests ported from Java testV2/BucketOwner.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestBucketOwner(S3TestBase):
    @pytest.mark.tag("Check")
    def test_bucket_owner_head_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)

        client.head_bucket(Bucket=bucket_name, ExpectedBucketOwner=self.get_owner())

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_head_bucket_mismatch(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)

        self.assert_client_error(
            lambda: client.head_bucket(Bucket=bucket_name, ExpectedBucketOwner=self.get_wrong_owner()),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_bucket_owner_list_objects_v2(self):
        key = "test_bucket_owner_list_objects_v2"
        client = self.get_client()
        bucket_name = self.create_objects(client, 3, key)

        response = client.list_objects_v2(Bucket=bucket_name, ExpectedBucketOwner=self.get_owner())
        assert response["KeyCount"] == 1

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_list_objects_v2_mismatch(self):
        key = "test_bucket_owner_list_objects_v2_mismatch"
        client = self.get_client()
        bucket_name = self.create_objects(client, 4, key)

        self.assert_client_error(
            lambda: client.list_objects_v2(Bucket=bucket_name, ExpectedBucketOwner=self.get_wrong_owner()),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_bucket_owner_put_object(self):
        key = "test_bucket_owner_put_object"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ExpectedBucketOwner=self.get_owner(),
        )

        self.succeed_get_object(client, bucket_name, key, key)

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_put_object_mismatch(self):
        key = "test_bucket_owner_put_object_mismatch"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)

        self.assert_client_error(
            lambda: client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=key.encode("utf-8"),
                ExpectedBucketOwner=self.get_wrong_owner(),
            ),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_bucket_owner_get_object(self):
        key = "test_bucket_owner_get_object"
        client = self.get_client()
        bucket_name = self.create_objects(client, 7, key)

        response = client.get_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_owner())
        assert self.get_body(response) == key

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_get_object_mismatch(self):
        key = "test_bucket_owner_get_object_mismatch"
        client = self.get_client()
        bucket_name = self.create_objects(client, 8, key)

        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_wrong_owner()),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Check")
    def test_bucket_owner_delete_object(self):
        key = "test_bucket_owner_delete_object"
        client = self.get_client()
        bucket_name = self.create_objects(client, 9, key)

        client.delete_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_owner())

        self.failed_get_object(client, bucket_name, key, 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_delete_object_mismatch(self):
        key = "test_bucket_owner_delete_object_mismatch"
        client = self.get_client()
        bucket_name = self.create_objects(client, 10, key)

        self.assert_client_error(
            lambda: client.delete_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_wrong_owner()),
            403,
            md.ACCESS_DENIED,
        )

        self.succeed_get_object(client, bucket_name, key, key)

    @pytest.mark.tag("Check")
    def test_source_bucket_owner_copy_object(self):
        source = "test_source_bucket_owner_copy_object_source"
        target = "test_source_bucket_owner_copy_object_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 11, source)
        target_bucket = self.create_bucket(client, 11)

        client.copy_object(
            Bucket=target_bucket,
            Key=target,
            CopySource={"Bucket": source_bucket, "Key": source},
            ExpectedBucketOwner=self.get_owner(),
            ExpectedSourceBucketOwner=self.get_owner(),
        )

        self.succeed_get_object(client, target_bucket, target, source)

    @pytest.mark.tag("ERROR")
    def test_source_bucket_owner_copy_object_mismatch(self):
        source = "test_source_bucket_owner_copy_object_mismatch_source"
        target = "test_source_bucket_owner_copy_object_mismatch_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 12, source)
        target_bucket = self.create_bucket(client, 12)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=target_bucket,
                Key=target,
                CopySource={"Bucket": source_bucket, "Key": source},
                ExpectedBucketOwner=self.get_owner(),
                ExpectedSourceBucketOwner=self.get_wrong_owner(),
            ),
            403,
            md.ACCESS_DENIED,
        )

        self.failed_get_object(client, target_bucket, target, 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_copy_object_mismatch(self):
        source = "test_bucket_owner_copy_object_mismatch_source"
        target = "test_bucket_owner_copy_object_mismatch_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 13, source)
        target_bucket = self.create_bucket(client, 13)

        self.assert_client_error(
            lambda: client.copy_object(
                Bucket=target_bucket,
                Key=target,
                CopySource={"Bucket": source_bucket, "Key": source},
                ExpectedBucketOwner=self.get_wrong_owner(),
                ExpectedSourceBucketOwner=self.get_owner(),
            ),
            403,
            md.ACCESS_DENIED,
        )

        self.failed_get_object(client, target_bucket, target, 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("Check")
    def test_source_bucket_owner_upload_part_copy(self):
        source = "test_source_bucket_owner_upload_part_copy_source"
        target = "test_source_bucket_owner_upload_part_copy_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 14, source)
        target_bucket = self.create_bucket(client, 14)

        upload_id = client.create_multipart_upload(Bucket=target_bucket, Key=target)["UploadId"]

        response = client.upload_part_copy(
            Bucket=target_bucket,
            Key=target,
            UploadId=upload_id,
            PartNumber=1,
            CopySource={"Bucket": source_bucket, "Key": source},
            ExpectedBucketOwner=self.get_owner(),
            ExpectedSourceBucketOwner=self.get_owner(),
        )
        assert response["CopyPartResult"]["ETag"]

        client.abort_multipart_upload(Bucket=target_bucket, Key=target, UploadId=upload_id)

    @pytest.mark.tag("ERROR")
    def test_source_bucket_owner_upload_part_copy_mismatch(self):
        source = "test_source_bucket_owner_upload_part_copy_mismatch_source"
        target = "test_source_bucket_owner_upload_part_copy_mismatch_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 15, source)
        target_bucket = self.create_bucket(client, 15)

        upload_id = client.create_multipart_upload(Bucket=target_bucket, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: client.upload_part_copy(
                Bucket=target_bucket,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
                CopySource={"Bucket": source_bucket, "Key": source},
                ExpectedBucketOwner=self.get_owner(),
                ExpectedSourceBucketOwner=self.get_wrong_owner(),
            ),
            403,
            md.ACCESS_DENIED,
        )

        client.abort_multipart_upload(Bucket=target_bucket, Key=target, UploadId=upload_id)

    @pytest.mark.tag("ERROR")
    def test_bucket_owner_upload_part_copy_mismatch(self):
        source = "test_bucket_owner_upload_part_copy_mismatch_source"
        target = "test_bucket_owner_upload_part_copy_mismatch_target"
        client = self.get_client()
        source_bucket = self.create_objects(client, 16, source)
        target_bucket = self.create_bucket(client, 16)

        upload_id = client.create_multipart_upload(Bucket=target_bucket, Key=target)["UploadId"]

        self.assert_client_error(
            lambda: client.upload_part_copy(
                Bucket=target_bucket,
                Key=target,
                UploadId=upload_id,
                PartNumber=1,
                CopySource={"Bucket": source_bucket, "Key": source},
                ExpectedBucketOwner=self.get_wrong_owner(),
                ExpectedSourceBucketOwner=self.get_owner(),
            ),
            403,
            md.ACCESS_DENIED,
        )

        client.abort_multipart_upload(Bucket=target_bucket, Key=target, UploadId=upload_id)

    # region 그 밖의 API (AWS 실측 기준)
    # x-amz-expected-bucket-owner 는 버킷·오브젝트를 다루는 거의 모든 API 가 받는다.
    # 소유자가 다르면 다른 검사보다 먼저 403 AccessDenied 다.

    def _assert_owner_mismatch(self, func) -> None:
        self.assert_client_error(func, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("List")
    def test_bucket_owner_list_operations(self):
        key = "test_bucket_owner_list_operations"
        client = self.get_client()
        bucket_name = self.create_objects(client, 17, key)
        client.create_multipart_upload(Bucket=bucket_name, Key="multi")
        owner, wrong = self.get_owner(), self.get_wrong_owner()

        assert len(client.list_objects(Bucket=bucket_name, ExpectedBucketOwner=owner)["Contents"]) == 1
        assert len(client.list_object_versions(Bucket=bucket_name, ExpectedBucketOwner=owner)["Versions"]) == 1
        assert len(client.list_multipart_uploads(Bucket=bucket_name, ExpectedBucketOwner=owner)["Uploads"]) == 1

        self._assert_owner_mismatch(lambda: client.list_objects(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(lambda: client.list_object_versions(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(
            lambda: client.list_multipart_uploads(Bucket=bucket_name, ExpectedBucketOwner=wrong)
        )

    @pytest.mark.tag("Bucket")
    def test_bucket_owner_bucket_config(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        owner, wrong = self.get_owner(), self.get_wrong_owner()
        versioning = {"Status": "Enabled"}

        client.get_bucket_location(Bucket=bucket_name, ExpectedBucketOwner=owner)
        client.get_bucket_acl(Bucket=bucket_name, ExpectedBucketOwner=owner)
        self._assert_owner_mismatch(lambda: client.get_bucket_location(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(lambda: client.get_bucket_acl(Bucket=bucket_name, ExpectedBucketOwner=wrong))

        # 소유자가 다르면 설정이 바뀌지 않는다
        self._assert_owner_mismatch(
            lambda: client.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration=versioning, ExpectedBucketOwner=wrong
            )
        )
        assert "Status" not in client.get_bucket_versioning(Bucket=bucket_name, ExpectedBucketOwner=owner)
        client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration=versioning, ExpectedBucketOwner=owner)
        self._assert_owner_mismatch(
            lambda: client.get_bucket_versioning(Bucket=bucket_name, ExpectedBucketOwner=wrong)
        )

    @pytest.mark.tag("Bucket")
    def test_bucket_owner_bucket_tagging_cors(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        owner, wrong = self.get_owner(), self.get_wrong_owner()
        tagging = {"TagSet": [{"Key": "a", "Value": "b"}]}
        cors = {"CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]}

        self._assert_owner_mismatch(
            lambda: client.put_bucket_tagging(Bucket=bucket_name, Tagging=tagging, ExpectedBucketOwner=wrong)
        )
        self._assert_owner_mismatch(
            lambda: client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors, ExpectedBucketOwner=wrong)
        )
        client.put_bucket_tagging(Bucket=bucket_name, Tagging=tagging, ExpectedBucketOwner=owner)
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors, ExpectedBucketOwner=owner)
        assert client.get_bucket_tagging(Bucket=bucket_name, ExpectedBucketOwner=owner)["TagSet"] == tagging["TagSet"]
        assert len(client.get_bucket_cors(Bucket=bucket_name, ExpectedBucketOwner=owner)["CORSRules"]) == 1

        self._assert_owner_mismatch(lambda: client.get_bucket_tagging(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(lambda: client.get_bucket_cors(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(
            lambda: client.delete_bucket_tagging(Bucket=bucket_name, ExpectedBucketOwner=wrong)
        )
        self._assert_owner_mismatch(lambda: client.delete_bucket_cors(Bucket=bucket_name, ExpectedBucketOwner=wrong))
        # 삭제되지 않았다
        assert client.get_bucket_tagging(Bucket=bucket_name)["TagSet"] == tagging["TagSet"]
        client.delete_bucket_tagging(Bucket=bucket_name, ExpectedBucketOwner=owner)
        client.delete_bucket_cors(Bucket=bucket_name, ExpectedBucketOwner=owner)
        self.assert_client_error(lambda: client.get_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_TAG_SET)

    @pytest.mark.tag("Priority")
    def test_bucket_owner_not_configured(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        owner, wrong = self.get_owner(), self.get_wrong_owner()
        requests = [
            (lambda o: client.get_bucket_policy(Bucket=bucket_name, ExpectedBucketOwner=o), md.NO_SUCH_BUCKET_POLICY),
            (
                lambda o: client.get_bucket_lifecycle_configuration(Bucket=bucket_name, ExpectedBucketOwner=o),
                md.NO_SUCH_LIFECYCLE_CONFIGURATION,
            ),
            (lambda o: client.get_bucket_cors(Bucket=bucket_name, ExpectedBucketOwner=o), md.NO_SUCH_CORS_CONFIGURATION),
            (
                lambda o: client.get_bucket_website(Bucket=bucket_name, ExpectedBucketOwner=o),
                md.NO_SUCH_WEBSITE_CONFIGURATION,
            ),
        ]
        for request, error_code in requests:
            # 소유자가 맞으면 설정 없음(404), 다르면 설정을 보기 전에 403 이다
            self.assert_client_error(lambda request=request: request(owner), 404, error_code)
            self._assert_owner_mismatch(lambda request=request: request(wrong))

    @pytest.mark.tag("Object")
    def test_bucket_owner_object_read(self):
        key = "test_bucket_owner_object_read"
        client = self.get_client()
        bucket_name = self.create_objects(client, 21, key)
        owner, wrong = self.get_owner(), self.get_wrong_owner()

        assert client.head_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)["ContentLength"] == len(key)
        assert len(client.get_object_acl(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)["Grants"]) == 1
        response = client.get_object_attributes(
            Bucket=bucket_name, Key=key, ObjectAttributes=["ObjectSize"], ExpectedBucketOwner=owner
        )
        assert response["ObjectSize"] == len(key)

        self.assert_client_error(
            lambda: client.head_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=wrong), 403, "403"
        )
        self._assert_owner_mismatch(lambda: client.get_object_acl(Bucket=bucket_name, Key=key, ExpectedBucketOwner=wrong))
        self._assert_owner_mismatch(
            lambda: client.get_object_attributes(
                Bucket=bucket_name, Key=key, ObjectAttributes=["ObjectSize"], ExpectedBucketOwner=wrong
            )
        )

    @pytest.mark.tag("Object")
    def test_bucket_owner_object_tagging(self):
        key = "test_bucket_owner_object_tagging"
        client = self.get_client()
        bucket_name = self.create_objects(client, 22, key)
        owner, wrong = self.get_owner(), self.get_wrong_owner()
        tagging = {"TagSet": [{"Key": "a", "Value": "b"}]}

        self._assert_owner_mismatch(
            lambda: client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging, ExpectedBucketOwner=wrong)
        )
        assert client.get_object_tagging(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)["TagSet"] == []
        client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=tagging, ExpectedBucketOwner=owner)

        self._assert_owner_mismatch(
            lambda: client.get_object_tagging(Bucket=bucket_name, Key=key, ExpectedBucketOwner=wrong)
        )
        self._assert_owner_mismatch(
            lambda: client.delete_object_tagging(Bucket=bucket_name, Key=key, ExpectedBucketOwner=wrong)
        )
        assert client.get_object_tagging(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)["TagSet"] == tagging["TagSet"]
        client.delete_object_tagging(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)
        assert client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"] == []

    @pytest.mark.tag("Priority")
    def test_bucket_owner_key_not_exist(self):
        key = "test_bucket_owner_key_not_exist"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        # 소유자가 맞으면 없는 키(404), 다르면 키를 보기 전에 403 이다
        self.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_owner()),
            404,
            md.NO_SUCH_KEY,
        )
        self._assert_owner_mismatch(
            lambda: client.get_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=self.get_wrong_owner())
        )

    @pytest.mark.tag("Object")
    def test_bucket_owner_delete_objects(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        self.create_objects(client, bucket_name, ["a", "b"])
        delete = {"Objects": [{"Key": "a"}, {"Key": "b"}]}

        # 요청 전체가 거부되고 하나도 지워지지 않는다
        self._assert_owner_mismatch(
            lambda: client.delete_objects(Bucket=bucket_name, Delete=delete, ExpectedBucketOwner=self.get_wrong_owner())
        )
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 2

        response = client.delete_objects(Bucket=bucket_name, Delete=delete, ExpectedBucketOwner=self.get_owner())
        assert len(response["Deleted"]) == 2
        assert client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 0

    @pytest.mark.tag("Multipart")
    def test_bucket_owner_multipart(self):
        key = "test_bucket_owner_multipart"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 25)
        owner, wrong = self.get_owner(), self.get_wrong_owner()

        self._assert_owner_mismatch(
            lambda: client.create_multipart_upload(Bucket=bucket_name, Key=key, ExpectedBucketOwner=wrong)
        )
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)["UploadId"]

        self._assert_owner_mismatch(
            lambda: client.upload_part(
                Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=b"content", ExpectedBucketOwner=wrong
            )
        )
        etag = client.upload_part(
            Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=b"content", ExpectedBucketOwner=owner
        )["ETag"]

        self._assert_owner_mismatch(
            lambda: client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id, ExpectedBucketOwner=wrong)
        )
        response = client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id, ExpectedBucketOwner=owner)
        assert len(response["Parts"]) == 1

        parts = {"Parts": [{"ETag": etag, "PartNumber": 1}]}
        self._assert_owner_mismatch(
            lambda: client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, ExpectedBucketOwner=wrong)
        )
        self._assert_owner_mismatch(
            lambda: client.complete_multipart_upload(
                Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload=parts, ExpectedBucketOwner=wrong
            )
        )
        # 거부된 중단·완료는 업로드를 건드리지 않는다
        client.complete_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload=parts, ExpectedBucketOwner=owner
        )
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "content"

    @pytest.mark.tag("Bucket")
    def test_bucket_owner_delete_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        self._assert_owner_mismatch(
            lambda: client.delete_bucket(Bucket=bucket_name, ExpectedBucketOwner=self.get_wrong_owner())
        )
        client.head_bucket(Bucket=bucket_name)

        client.delete_bucket(Bucket=bucket_name, ExpectedBucketOwner=self.get_owner())
        self.delete_bucket_list(bucket_name)
        self.assert_client_error(lambda: client.head_bucket(Bucket=bucket_name), 404, "404")

    @pytest.mark.tag("Priority")
    def test_bucket_owner_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(27)
        # 버킷이 없으면 소유자 값과 상관없이 404 다
        for owner in (self.get_owner(), self.get_wrong_owner()):
            self.assert_client_error(
                lambda owner=owner: client.list_objects_v2(Bucket=bucket_name, ExpectedBucketOwner=owner),
                404,
                md.NO_SUCH_BUCKET,
            )

    @pytest.mark.tag("Access")
    def test_bucket_owner_other_user(self):
        key = "test_bucket_owner_other_user"
        client = self.get_client()
        bucket_name = self.create_objects(client, 28, key)
        alt_client = self.get_alt_client()
        # 소유자 값이 맞아도 권한이 생기지는 않는다
        for owner in (self.get_owner(), self.get_wrong_owner()):
            self._assert_owner_mismatch(
                lambda owner=owner: alt_client.list_objects_v2(Bucket=bucket_name, ExpectedBucketOwner=owner)
            )
            self._assert_owner_mismatch(
                lambda owner=owner: alt_client.get_object(Bucket=bucket_name, Key=key, ExpectedBucketOwner=owner)
            )

    # endregion
