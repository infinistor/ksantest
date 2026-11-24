"""ListBuckets tests ported from Java testV2/ListBuckets.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestListBuckets(S3TestBase):
    @pytest.mark.tag("Get")
    def test_buckets_create_then_list(self):
        client = self.get_client()
        bucket_names = []
        for _ in range(5):
            bucket_names.append(self.create_bucket(client, 1))

        response = client.list_buckets()
        bucket_list = self.get_bucket_list(response)

        for bucket_name in bucket_names:
            assert bucket_name in bucket_list, (
                f"S3 implementation's GET on Service did not return bucket we created: {bucket_name}"
            )

    @pytest.mark.tag("ERROR")
    def test_list_buckets_invalid_auth(self):
        bad_auth_client = self.get_bad_auth_client(None, None)
        with pytest.raises(ClientError) as exc_info:
            bad_auth_client.list_buckets()
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_ACCESS_KEY_ID

    @pytest.mark.tag("ERROR")
    def test_list_buckets_bad_auth(self):
        main_access_key = self.config.main_user.access_key
        bad_auth_client = self.get_bad_auth_client(main_access_key, None)
        with pytest.raises(ClientError) as exc_info:
            bad_auth_client.list_buckets()
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.SIGNATURE_DOES_NOT_MATCH

    @pytest.mark.tag("Metadata")
    def test_head_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        response = client.head_bucket(Bucket=bucket_name)
        assert response is not None
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.tag("Prefix")
    def test_list_buckets_prefix(self):
        client = self.get_client()
        prefix = "1111-my-test"
        bucket_name = utils.get_new_bucket_name(prefix, self.get_suite_id(), 5)
        client.create_bucket(Bucket=bucket_name)

        for _ in range(5):
            self.create_bucket(client, 5)

        response = client.list_buckets(Prefix=prefix)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 1
        assert bucket_list[0] == bucket_name
        client.delete_bucket(Bucket=bucket_name)
        self.delete_bucket_list(bucket_name)

    @pytest.mark.tag("MaxBuckets")
    def test_list_buckets_max_buckets(self):
        client = self.get_client()
        for _ in range(5):
            self.create_bucket(client, 6)

        # {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
        prefix = f"{self.get_prefix()}{self.get_suite_id()}-6-"

        full_response = client.list_buckets(Prefix=prefix)
        full_bucket_list = self.get_bucket_list(full_response)
        full_bucket_list.sort()

        response = client.list_buckets(Prefix=prefix, MaxBuckets=2)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 2
        assert bucket_list == full_bucket_list[:2]

    @pytest.mark.tag("ContinuationToken")
    def test_list_buckets_continuation_token(self):
        client = self.get_client()
        for _ in range(5):
            self.create_bucket(client, 7)

        # {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
        prefix = f"{self.get_prefix()}{self.get_suite_id()}-7-"

        full_response = client.list_buckets(Prefix=prefix)
        full_bucket_list = self.get_bucket_list(full_response)
        full_bucket_list.sort()

        response = client.list_buckets(Prefix=prefix, MaxBuckets=2)
        bucket_list = self.get_bucket_list(response)
        assert len(bucket_list) == 2
        assert bucket_list == full_bucket_list[:2]

        response2 = client.list_buckets(
            Prefix=prefix,
            MaxBuckets=2,
            ContinuationToken=response["ContinuationToken"],
        )
        bucket_list2 = self.get_bucket_list(response2)
        assert len(bucket_list2) == 2
        assert bucket_list2 == full_bucket_list[2:4]

    @staticmethod
    def _http_status(func) -> int:
        """HEAD 요청은 응답 본문이 없어 에러 코드 대신 상태 코드만 확인한다."""
        with pytest.raises(ClientError) as exc_info:
            func()
        return exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]

    @pytest.mark.tag("Metadata")
    def test_head_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(8)
        assert self._http_status(lambda: client.head_bucket(Bucket=bucket_name)) == 404

    @pytest.mark.tag("Permission")
    def test_head_bucket_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 9)
        assert self._http_status(lambda: alt_client.head_bucket(Bucket=bucket_name)) == 403

    @pytest.mark.tag("Prefix")
    def test_list_buckets_prefix_not_match(self):
        client = self.get_client()
        self.create_bucket(client, 11)
        response = client.list_buckets(Prefix="zzzz-no-such-bucket-prefix-" + self.get_suite_id())
        assert len(response.get("Buckets", [])) == 0
        assert response.get("ContinuationToken") is None

    @pytest.mark.tag("ERROR")
    def test_list_buckets_max_buckets_invalid(self):
        client = self.get_client()
        # max-buckets 는 1 이상 10000 이하만 허용된다
        self.assert_client_error(lambda: client.list_buckets(MaxBuckets=10001), 400, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ContinuationToken")
    def test_list_buckets_paging(self):
        client = self.get_client()
        for _ in range(5):
            self.create_bucket(client, 14)
        prefix = f"{self.get_prefix()}{self.get_suite_id()}-14-"
        full_bucket_list = self.get_bucket_list(client.list_buckets(Prefix=prefix))
        assert len(full_bucket_list) == 5

        # 토큰이 없어질 때까지 끝까지 넘기면 전체 목록과 순서까지 같다
        paged_bucket_list = []
        page_count = 0
        token = None
        while True:
            params = {"Prefix": prefix, "MaxBuckets": 2}
            if token:
                params["ContinuationToken"] = token
            response = client.list_buckets(**params)
            page = self.get_bucket_list(response)
            assert len(page) <= 2
            paged_bucket_list.extend(page)
            page_count += 1
            token = response.get("ContinuationToken")
            if not token:
                break
        assert page_count == 3
        assert paged_bucket_list == full_bucket_list
        assert paged_bucket_list == sorted(paged_bucket_list)

    @pytest.mark.tag("Get")
    def test_list_buckets_owner(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        response = client.list_buckets(Prefix=bucket_name)
        assert len(response["Buckets"]) == 1
        assert response["Buckets"][0]["Name"] == bucket_name
        assert response["Buckets"][0]["CreationDate"] is not None
        assert response["Owner"]["ID"] == self.config.main_user.id

    @pytest.mark.tag("Permission")
    def test_list_buckets_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 16)
        # 다른 사용자의 목록에는 내 버킷이 나오지 않는다
        response = alt_client.list_buckets(Prefix=bucket_name)
        assert len(response.get("Buckets", [])) == 0
