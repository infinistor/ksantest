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
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Get")
    def test_lifecycle_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        self.prefix_lifecycle_configuration_check(rules, response["Rules"])

    @pytest.mark.tag("Check")
    def test_lifecycle_get_no_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
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
        bucket_name = self.create_bucket(client)
        key = "test1/a"
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == 1
        assert len(response.get("DeleteMarkers", [])) == 1

    @pytest.mark.tag("Check")
    def test_lifecycle_id_too_long(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": utils.random_text_to_long(256),
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Duplicate")
    def test_lifecycle_same_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
            lambda: client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_invalid_status(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 2},
                "Filter": {"Prefix": "test1/"},
                "Status": "invalid",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Date")
    def test_lifecycle_set_date(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        expiration_date = datetime(2099, 11, 10, tzinfo=timezone.utc)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Date": expiration_date},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("ERROR")
    def test_lifecycle_set_invalid_date(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
            lambda: client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Version")
    def test_lifecycle_set_noncurrent(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "past/foo", "future/bar")
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Version")
    def test_lifecycle_noncurrent_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
        assert len(versions) == 6

    @pytest.mark.tag("DeleteMarker")
    def test_lifecycle_set_delete_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Filter")
    def test_lifecycle_set_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": "foo"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Filter")
    def test_lifecycle_set_empty_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("DeleteMarker")
    def test_lifecycle_delete_marker_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Multipart")
    def test_lifecycle_set_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )

    @pytest.mark.tag("Multipart")
    def test_lifecycle_multipart_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
        assert len(uploads) == 2

        for upload in uploads:
            client.abort_multipart_upload(
                Bucket=bucket_name, Key=upload["Key"], UploadId=upload["UploadId"]
            )

    @pytest.mark.tag("Delete")
    def test_lifecycle_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
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
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
        client.delete_bucket_lifecycle(Bucket=bucket_name)

    @pytest.mark.tag("ERROR")
    def test_lifecycle_set_expiration_zero(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 0},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        self.assert_client_error(
            lambda: client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("metadata")
    def test_lifecycle_set_expiration(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "test1/"},
                "Status": "Enabled",
            }
        ]
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
        )
        key = "test1/a"
        content = "test"
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

        head_response = client.head_object(Bucket=bucket_name, Key=key)
        expired_time = self.get_expired_date_instant(head_response["LastModified"], 1)
        assert head_response["Expires"] == expired_time

        get_response = client.get_object(Bucket=bucket_name, Key=key)
        assert get_response["Expires"] == expired_time
