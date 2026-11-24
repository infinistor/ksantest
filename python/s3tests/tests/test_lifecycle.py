"""LifeCycle tests ported from Java testV2/LifeCycle.java."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestLifeCycle(S3TestBase):
    @pytest.mark.tag("Check")
    def test_lifecycle_set(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule2",
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test2/"},
                "Status": "Disabled",
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Get")
    def test_lifecycle_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 31},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule2",
                "Expiration": {"Days": 120},
                "Filter": {"Prefix": "test2/"},
                "Status": "Enabled",
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        self.prefix_lifecycle_configuration_check(rules, response["Rules"])

    @pytest.mark.tag("Check")
    def test_lifecycle_get_no_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        rules = [
            {
                "Expiration": {"Days": 31},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            },
            {
                "Expiration": {"Days": 120},
                "Filter": {"Prefix": "test2/"},
                "Status": "Enabled",
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        get_rules = response["Rules"]
        for index, rule in enumerate(rules):
            assert get_rules[index].get("ID") is not None
            assert rule["Expiration"].get("Date") == get_rules[index]["Expiration"].get("Date")
            assert rule["Expiration"]["Days"] == get_rules[index]["Expiration"]["Days"]
            assert rule["Filter"]["Prefix"] == get_rules[index]["Filter"]["Prefix"]
            assert rule["Status"] == get_rules[index]["Status"]

    @pytest.mark.tag("Version")
    def test_lifecycle_expiration_versioning_enabled(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        key = "test1/test_lifecycle_expiration_versioning_enabled"
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.create_multiple_versions(client, bucket_name, key, 1, True)
        client.delete_object(Bucket=bucket_name, Key=key)

        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "expire1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 1
        assert len(response.get("DeleteMarkers", [])) == 1

    @pytest.mark.tag("Check")
    def test_lifecycle_id_too_long(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        rules = [
            {
                "ID": utils.random_text_to_long(256),
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Duplicate")
    def test_lifecycle_same_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule1",
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test2/"},
                "Status": "Disabled",
            },
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_invalid_status(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test1/"},
                "Status": "invalid",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Date")
    def test_lifecycle_set_date(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        expiration_date = datetime(2099, 11, 10, tzinfo=timezone.utc)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Date": expiration_date},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("ERROR")
    def test_lifecycle_set_invalid_date(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        # Java Calendar without setTimeZone uses JVM default tz, not UTC midnight.
        if datetime.now().astimezone().utcoffset().total_seconds() == 0:
            expiration_date = datetime(2099, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
        else:
            expiration_date = datetime(2099, 11, 10, tzinfo=datetime.now().astimezone().tzinfo)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Date": expiration_date},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Version")
    def test_lifecycle_set_noncurrent(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 10, "past/foo", "future/bar")
        rules = [
            {
                "ID": "rule1",
                "NoncurrentVersionExpiration": {"NoncurrentDays": 2},
                "Filter": {"Prefix": "past/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule2",
                "NoncurrentVersionExpiration": {"NoncurrentDays": 3},
                "Filter": {"Prefix": "future/"},
                "Status": "Enabled",
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Version")
    def test_lifecycle_noncurrent_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.create_multiple_versions(client, bucket_name, "test1/a", 3, True)
        self.create_multiple_versions(client, bucket_name, "test2/abc", 3, False)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response.get("Versions", [])

        rules = [
            {
                "ID": "rule1",
                "NoncurrentVersionExpiration": {"NoncurrentDays": 2},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        assert len(versions) == 6

    @pytest.mark.tag("DeleteMarker")
    def test_lifecycle_set_delete_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Filter")
    def test_lifecycle_set_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": "foo"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Filter")
    def test_lifecycle_set_empty_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("DeleteMarker")
    def test_lifecycle_delete_marker_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.create_multiple_versions(client, bucket_name, "test1/a", 1, True)
        self.create_multiple_versions(client, bucket_name, "test2/abc", 1, False)
        client.delete_object(Bucket=bucket_name, Key="test1/a")
        client.delete_object(Bucket=bucket_name, Key="test2/abc")

        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 2
        assert len(response.get("DeleteMarkers", [])) == 2

        rules = [
            {
                "ID": "rule1",
                "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Multipart")
    def test_lifecycle_set_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        rules = [
            {
                "ID": "rule1",
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
            },
            {
                "ID": "rule2",
                "Filter": {"Prefix": "test2/"},
                "Status": "Enabled",
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})

    @pytest.mark.tag("Multipart")
    def test_lifecycle_multipart_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        key_names = ["test1/a", "test2/b"]
        upload_ids = []
        for key in key_names:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
            upload_ids.append(response["UploadId"])

        list_response = client.list_multipart_uploads(Bucket=bucket_name)
        uploads = list_response.get("Uploads", [])

        rules = [
            {
                "ID": "rule1",
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        assert len(uploads) == 2

        for upload in uploads:
            client.abort_multipart_upload(Bucket=bucket_name, Key=upload["Key"], UploadId=upload["UploadId"])

    @pytest.mark.tag("Delete")
    def test_lifecycle_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule2",
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test2/"},
                "Status": "Disabled",
            },
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        client.delete_bucket_lifecycle(Bucket=bucket_name)

    @pytest.mark.tag("ERROR")
    def test_lifecycle_set_expiration_zero(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 0},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.skip(reason="Expires is not returned")
    @pytest.mark.tag("metadata")
    def test_lifecycle_set_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={"Rules": rules})
        key = "test1/test_lifecycle_set_expiration"
        content = "test"
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        expired_time = self.get_expired_date_instant(head_response["LastModified"], 1)
        assert head_response["Expires"] == expired_time

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert get_response["Expires"] == expired_time

    @staticmethod
    def _put_rules(client, bucket_name: str, rules: list, **kwargs):
        return client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}, **kwargs
        )

    @staticmethod
    def _rule(**actions) -> dict:
        """ID rule1, 전체 대상(Prefix ""), Enabled 인 규칙에 주어진 항목을 덮어쓴다."""
        rule = {"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": ""}}
        rule.update(actions)
        return rule

    @pytest.mark.tag("Get")
    def test_lifecycle_get_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)
        self.assert_client_error(
            lambda: client.get_bucket_lifecycle_configuration(Bucket=bucket_name),
            404,
            md.NO_SUCH_LIFECYCLE_CONFIGURATION,
        )

    @pytest.mark.tag("Delete")
    def test_lifecycle_delete_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        # 설정이 없어도 삭제는 성공한다
        response = client.delete_bucket_lifecycle(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @pytest.mark.tag("ERROR")
    def test_lifecycle_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(23)
        rules = [self._rule(Expiration={"Days": 1})]
        self.assert_client_error(
            lambda: client.get_bucket_lifecycle_configuration(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, rules), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.delete_bucket_lifecycle(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_access_denied(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 24)
        rules = [self._rule(Expiration={"Days": 1})]
        self._put_rules(client, bucket_name, rules)

        self.assert_client_error(
            lambda: alt_client.get_bucket_lifecycle_configuration(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(lambda: self._put_rules(alt_client, bucket_name, rules), 403, md.ACCESS_DENIED)
        self.assert_client_error(
            lambda: alt_client.delete_bucket_lifecycle(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )
        # 설정은 그대로 남아 있다
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        assert len(response["Rules"]) == 1

    @pytest.mark.tag("ERROR")
    def test_lifecycle_no_action(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 25)
        self.assert_client_error(
            lambda: self._put_rules(client, bucket_name, [self._rule()]), 400, md.INVALID_REQUEST
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_no_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        # Filter 도 Prefix 도 없는 규칙은 거부된다
        rules = [{"ID": "rule1", "Status": "Enabled", "Expiration": {"Days": 1}}]
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, rules), 400, md.MALFORMED_XML)

    @pytest.mark.tag("ERROR")
    def test_lifecycle_empty_rules(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, []), 400, md.MALFORMED_XML)

    @pytest.mark.tag("ERROR")
    def test_lifecycle_days_and_date(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        expiration = {"Days": 1, "Date": datetime(2099, 11, 10, tzinfo=timezone.utc)}
        self.assert_client_error(
            lambda: self._put_rules(client, bucket_name, [self._rule(Expiration=expiration)]),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_negative_days(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        self.assert_client_error(
            lambda: self._put_rules(client, bucket_name, [self._rule(Expiration={"Days": -1})]),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Transition")
    def test_lifecycle_transition(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        transitions = [
            {"Days": 30, "StorageClass": "STANDARD_IA"},
            {"Days": 60, "StorageClass": "GLACIER"},
        ]
        self._put_rules(client, bucket_name, [self._rule(Transitions=transitions, Expiration={"Days": 90})])

        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert len(rules) == 1
        assert sorted(rules[0]["Transitions"], key=lambda t: t["Days"]) == transitions
        assert rules[0]["Expiration"]["Days"] == 90

    @pytest.mark.tag("Transition")
    def test_lifecycle_transition_invalid_storage_class(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        # STANDARD 로는 전환할 수 없다
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Transitions=[{"Days": 30, "StorageClass": "STANDARD"}])]
            ),
            400,
            md.INVALID_ARGUMENT,
        )
        # 없는 스토리지 클래스
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Transitions=[{"Days": 30, "StorageClass": "INVALID"}])]
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Transition")
    def test_lifecycle_expiration_before_transition(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 32)
        transitions = [{"Days": 30, "StorageClass": "STANDARD_IA"}]
        # 만료일은 전환일보다 커야 한다
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Transitions=transitions, Expiration={"Days": 30})]
            ),
            400,
            md.INVALID_ARGUMENT,
        )
        self._put_rules(client, bucket_name, [self._rule(Transitions=transitions, Expiration={"Days": 31})])

    @pytest.mark.tag("Transition")
    def test_lifecycle_noncurrent_transition(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 34)
        transitions = [{"NoncurrentDays": 30, "StorageClass": "STANDARD_IA"}]
        self._put_rules(client, bucket_name, [self._rule(NoncurrentVersionTransitions=transitions)])

        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["NoncurrentVersionTransitions"] == transitions

    @pytest.mark.tag("Version")
    def test_lifecycle_noncurrent_zero_days(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 35)
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(NoncurrentVersionExpiration={"NoncurrentDays": 0})]
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Version")
    def test_lifecycle_newer_noncurrent_versions(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 36)
        expiration = {"NoncurrentDays": 1, "NewerNoncurrentVersions": 5}
        self._put_rules(client, bucket_name, [self._rule(NoncurrentVersionExpiration=expiration)])

        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["NoncurrentVersionExpiration"] == expiration

        # 보존할 버전 수는 1~100 만 허용한다
        for count in (0, 101):
            self.assert_client_error(
                lambda count=count: self._put_rules(
                    client,
                    bucket_name,
                    [self._rule(NoncurrentVersionExpiration={"NoncurrentDays": 1, "NewerNoncurrentVersions": count})],
                ),
                400,
                md.INVALID_ARGUMENT,
            )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_delete_marker_with_days(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 37)
        # ExpiredObjectDeleteMarker 는 Days 와 함께 쓸 수 없다
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Expiration={"Days": 1, "ExpiredObjectDeleteMarker": True})]
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_delete_marker_with_tag(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 38)
        rule = self._rule(Filter={"Tag": {"Key": "key", "Value": "value"}}, Expiration={"ExpiredObjectDeleteMarker": True})
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, [rule]), 400, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_lifecycle_abort_multipart_with_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 39)
        # AbortIncompleteMultipartUpload 는 태그·크기 필터와 함께 쓸 수 없다
        for rule_filter in ({"Tag": {"Key": "key", "Value": "value"}}, {"ObjectSizeGreaterThan": 10}):
            rule = self._rule(Filter=rule_filter, AbortIncompleteMultipartUpload={"DaysAfterInitiation": 1})
            self.assert_client_error(
                lambda rule=rule: self._put_rules(client, bucket_name, [rule]), 400, md.INVALID_REQUEST
            )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_abort_multipart_zero_days(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 40)
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(AbortIncompleteMultipartUpload={"DaysAfterInitiation": 0})]
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Filter")
    def test_lifecycle_tag_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 41)
        rule_filter = {"Tag": {"Key": "key", "Value": "value"}}
        self._put_rules(client, bucket_name, [self._rule(Filter=rule_filter, Expiration={"Days": 1})])

        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["Filter"] == rule_filter

    @pytest.mark.tag("Filter")
    def test_lifecycle_and_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 42)
        rule_filter = {
            "And": {
                "Prefix": "test1/",
                "Tags": [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}],
                "ObjectSizeGreaterThan": 10,
                "ObjectSizeLessThan": 100,
            }
        }
        self._put_rules(client, bucket_name, [self._rule(Filter=rule_filter, Expiration={"Days": 1})])

        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        actual = rules[0]["Filter"]["And"]
        assert actual["Prefix"] == "test1/"
        self.tag_compare(rule_filter["And"]["Tags"], actual["Tags"])
        assert actual["ObjectSizeGreaterThan"] == 10
        assert actual["ObjectSizeLessThan"] == 100

    @pytest.mark.tag("Filter")
    def test_lifecycle_and_filter_invalid(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 43)
        # And 안에 조건이 하나뿐이면 거부된다
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Filter={"And": {"Prefix": "test1/"}}, Expiration={"Days": 1})]
            ),
            400,
            md.MALFORMED_XML,
        )
        # And 안의 태그 키는 중복될 수 없다
        duplicated = {"And": {"Prefix": "test1/", "Tags": [{"Key": "key", "Value": "1"}, {"Key": "key", "Value": "2"}]}}
        self.assert_client_error(
            lambda: self._put_rules(client, bucket_name, [self._rule(Filter=duplicated, Expiration={"Days": 1})]),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Filter")
    def test_lifecycle_object_size_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 44)
        self._put_rules(
            client, bucket_name, [self._rule(Filter={"ObjectSizeGreaterThan": 500}, Expiration={"Days": 1})]
        )
        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["Filter"] == {"ObjectSizeGreaterThan": 500}

        self._put_rules(client, bucket_name, [self._rule(Filter={"ObjectSizeLessThan": 500}, Expiration={"Days": 1})])
        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["Filter"] == {"ObjectSizeLessThan": 500}

    @pytest.mark.tag("Filter")
    def test_lifecycle_object_size_filter_invalid(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 45)
        # ObjectSizeLessThan 은 1 이상이어야 한다
        self.assert_client_error(
            lambda: self._put_rules(
                client, bucket_name, [self._rule(Filter={"ObjectSizeLessThan": 0}, Expiration={"Days": 1})]
            ),
            400,
            md.INVALID_REQUEST,
        )
        # ObjectSizeLessThan 은 ObjectSizeGreaterThan 보다 커야 한다
        size_filter = {"And": {"ObjectSizeGreaterThan": 100, "ObjectSizeLessThan": 100}}
        self.assert_client_error(
            lambda: self._put_rules(client, bucket_name, [self._rule(Filter=size_filter, Expiration={"Days": 1})]),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Filter")
    def test_lifecycle_legacy_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 46)
        # Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식도 그대로 저장·조회된다
        rule = {"ID": "rule1", "Status": "Enabled", "Prefix": "legacy/", "Expiration": {"Days": 1}}
        self._put_rules(client, bucket_name, [rule])
        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]
        assert rules[0]["Prefix"] == "legacy/"
        assert "Filter" not in rules[0]

        # Prefix 와 Filter 를 함께 줄 수는 없다
        both = dict(rule, Filter={"Prefix": "test1/"})
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, [both]), 400, md.MALFORMED_XML)

    @pytest.mark.tag("Max")
    def test_lifecycle_max_rules(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 47)

        def make_rules(count: int) -> list:
            return [
                {"ID": f"rule{i}", "Status": "Enabled", "Filter": {"Prefix": f"test{i}/"}, "Expiration": {"Days": 1}}
                for i in range(count)
            ]

        # 규칙은 1000개까지 넣을 수 있다
        self._put_rules(client, bucket_name, make_rules(1000))
        assert len(client.get_bucket_lifecycle_configuration(Bucket=bucket_name)["Rules"]) == 1000
        self.assert_client_error(lambda: self._put_rules(client, bucket_name, make_rules(1001)), 400, md.MALFORMED_XML)

    @pytest.mark.tag("metadata")
    def test_lifecycle_expiration_header(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 49)
        rule = {"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "test1/"}, "Expiration": {"Days": 1}}
        self._put_rules(client, bucket_name, [rule])

        # 만료 규칙에 걸리는 오브젝트는 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려준다
        key = "test1/test_lifecycle_expiration_header"
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=b"test")
        assert 'rule-id="rule1"' in put_response["Expiration"]
        assert "expiry-date=" in put_response["Expiration"]
        assert 'rule-id="rule1"' in client.head_object(Bucket=bucket_name, Key=key)["Expiration"]
        assert 'rule-id="rule1"' in client.get_object(Bucket=bucket_name, Key=key)["Expiration"]

        # 규칙에 걸리지 않는 오브젝트에는 헤더가 없다
        other = "test2/test_lifecycle_expiration_header"
        assert "Expiration" not in client.put_object(Bucket=bucket_name, Key=other, Body=b"test")
        assert "Expiration" not in client.head_object(Bucket=bucket_name, Key=other)
