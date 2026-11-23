"""Lock tests ported from Java testV2/Lock.java."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils

RETENTION_DATE = datetime(2030, 2, 1, tzinfo=timezone.utc)
RETENTION_DATE_APRIL = datetime(2030, 4, 1, tzinfo=timezone.utc)


class TestLock(S3TestBase):
    @pytest.mark.tag("Put")
    def test_created_bucket_enable_object_lock(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )

    @pytest.mark.tag("Check")
    def test_object_lock_put_obj_lock(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 2)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "COMPLIANCE",
                    "Years": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        version_response = client.get_bucket_versioning(Bucket=bucket_name)
        assert version_response.get("Status") == "Enabled"

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_bucket(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Years": 1,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_BUCKET_STATE

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_with_days_and_years(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 4)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Years": 1,
                    "Days": 1,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_days(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 5)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 0,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_ARGUMENT

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_years(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 6)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Years": -1,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_ARGUMENT

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_mode(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 7)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "invalid",
                    "Years": 1,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_status(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 8)
        conf = {
            "ObjectLockEnabled": "Disabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Years": 1,
                }
            },
        }
        with pytest.raises(ClientError) as exc_info:
            client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML

    @pytest.mark.tag("Version")
    def test_object_lock_suspend_versioning(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 9)
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={"Status": "Suspended"},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_BUCKET_STATE

    @pytest.mark.tag("Check")
    def test_object_lock_get_obj_lock(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 10)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        response = client.get_object_lock_configuration(Bucket=bucket_name)
        self.lock_compare(conf, response["ObjectLockConfiguration"])

    @pytest.mark.tag("Check")
    def test_object_lock_put_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 11)
        key = "test_object_lock_put_object"
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ContentMD5=utils.get_md5(key),
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ObjectLockMode"] == "GOVERNANCE"
        assert response.get("ObjectLockRetainUntilDate") is not None
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=response["VersionId"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=response["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Check")
    def test_object_lock_copy_object(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 12)
        bucket_name2 = self.create_bucket_object_lock(client, 12)
        key = "test_object_lock_copy_object-lock"
        key_copy = key + "-copy"
        key2 = "test_object_lock_copy_object"
        key2_copy = key2 + "-copy"
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ContentMD5=utils.get_md5(key),
        )
        client.put_object(Bucket=bucket_name2, Key=key2, Body=key2.encode("utf-8"))
        client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=bucket_name2,
            Key=key_copy,
        )
        client.copy_object(
            CopySource={"Bucket": bucket_name2, "Key": key2},
            Bucket=bucket_name,
            Key=key2_copy,
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ObjectLockMode"] == "GOVERNANCE"
        assert response.get("ObjectLockRetainUntilDate") is not None
        response2 = client.head_object(Bucket=bucket_name2, Key=key_copy)
        assert response2.get("ObjectLockMode") is None
        assert response2.get("ObjectLockRetainUntilDate") is None
        response3 = client.head_object(Bucket=bucket_name2, Key=key2)
        assert response3.get("ObjectLockMode") is None
        assert response3.get("ObjectLockRetainUntilDate") is None
        response4 = client.head_object(Bucket=bucket_name, Key=key2_copy)
        assert response4["ObjectLockMode"] == "GOVERNANCE"
        assert response4.get("ObjectLockRetainUntilDate") is not None
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=response["VersionId"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        with pytest.raises(ClientError) as exc_info2:
            client.delete_object(Bucket=bucket_name, Key=key2_copy, VersionId=response4["VersionId"])
        assert exc_info2.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info2.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=response["VersionId"],
            BypassGovernanceRetention=True,
        )
        client.delete_object(
            Bucket=bucket_name2,
            Key=key_copy,
            VersionId=response2["VersionId"],
            BypassGovernanceRetention=True,
        )
        client.delete_object(
            Bucket=bucket_name2,
            Key=key2,
            VersionId=response3["VersionId"],
            BypassGovernanceRetention=True,
        )
        client.delete_object(
            Bucket=bucket_name,
            Key=key2_copy,
            VersionId=response4["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Check")
    def test_object_lock_multipart(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 13)
        key = "test_object_lock_multipart"
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        upload_data = self.setup_multipart_upload(client, bucket_name, key, 1 * md.MB)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ObjectLockMode"] == "GOVERNANCE"
        assert response.get("ObjectLockRetainUntilDate") is not None
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=response["VersionId"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=response["VersionId"],
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("ERROR")
    def test_object_lock_md5(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 14)
        key = "test_object_lock_md5"
        content = utils.random_text_to_long(1 * md.MB)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        with pytest.raises(ClientError) as exc_info:
            client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST
        init_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = init_response["UploadId"]
        with pytest.raises(ClientError) as exc_info2:
            client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=content.encode("utf-8"),
            )
        assert exc_info2.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info2.value.response["Error"]["Code"] == md.INVALID_REQUEST
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    @pytest.mark.tag("ERROR")
    def test_object_lock_get_obj_lock_invalid_bucket(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name(15)
        client.create_bucket(Bucket=bucket_name)
        with pytest.raises(ClientError) as exc_info:
            client.get_object_lock_configuration(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention(self):
        key = "test_object_lock_put_obj_retention"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 16)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = response["VersionId"]
        client.put_object_retention(
            Bucket=bucket_name,
            Key=key,
            Retention={"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE},
        )
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention_invalid_bucket(self):
        key = "test_object_lock_put_obj_retention_invalid_bucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        with pytest.raises(ClientError) as exc_info:
            client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention_invalid_mode(self):
        key = "test_object_lock_put_obj_retention_invalid_mode"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 18)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        retention = {"Mode": "invalid", "RetainUntilDate": RETENTION_DATE}
        with pytest.raises(ClientError) as exc_info:
            client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML

    @pytest.mark.tag("retention")
    def test_object_lock_get_obj_retention(self):
        key = "test_object_lock_get_obj_retention"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 19)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.retention_compare(retention, response["Retention"])
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("retention")
    def test_object_lock_get_obj_retention_invalid_bucket(self):
        key = "test_object_lock_get_obj_retention_invalid_bucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object_retention(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention_versionid(self):
        key = "test_object_lock_put_obj_retention_versionid"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 21)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.retention_compare(retention, response["Retention"])
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Priority")
    def test_object_lock_put_obj_retention_override_default_retention(self):
        key = "test_object_lock_put_obj_retention_override_default_retention"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 22)
        conf = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 1,
                }
            },
        }
        client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
        put_response = client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=key.encode("utf-8"),
            ContentMD5=utils.get_md5(key),
            ContentLength=len(key),
        )
        version_id = put_response["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.retention_compare(retention, response["Retention"])
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Overwrite")
    def test_object_lock_put_obj_retention_increase_period(self):
        key = "test_object_lock_put_obj_retention_increase_period"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 23)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention1 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
        retention2 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE_APRIL}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention2)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.retention_compare(retention2, response["Retention"])
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Overwrite")
    def test_object_lock_put_obj_retention_shorten_period(self):
        key = "test_object_lock_put_obj_retention_shorten_period"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 24)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention1 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE_APRIL}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
        retention2 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        with pytest.raises(ClientError) as exc_info:
            client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention2)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Overwrite")
    def test_object_lock_put_obj_retention_shorten_period_bypass(self):
        key = "test_object_lock_put_obj_retention_shorten_period_bypass"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 25)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention1 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE_APRIL}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
        retention2 = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(
            Bucket=bucket_name,
            Key=key,
            Retention=retention2,
            BypassGovernanceRetention=True,
        )
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.retention_compare(retention2, response["Retention"])
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("ERROR")
    def test_object_lock_delete_object_with_retention(self):
        key = "test_object_lock_delete_object_with_retention"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 26)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Retention")
    def test_object_lock_delete_object_with_retention_bypass(self):
        key = "test_object_lock_delete_object_with_retention_bypass"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 27)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        version_id = put_response["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("Retention")
    def test_object_lock_delete_objects_with_retention_bypass(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 28)
        key_versions: list[dict[str, str]] = []
        for i in range(10):
            key = f"test_object_lock_delete_objects_with_retention_bypass-{i:03d}"
            put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
            version_id = put_response["VersionId"]
            retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
            client.put_object_retention(
                Bucket=bucket_name,
                Key=key,
                Retention=retention,
                VersionId=version_id,
            )
            key_versions.append({"Key": key, "VersionId": version_id})
        client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": key_versions},
            BypassGovernanceRetention=True,
        )

    @pytest.mark.tag("LegalHold")
    def test_object_lock_put_legal_hold(self):
        key = "test_object_lock_put_legal_hold"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 29)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "ON"},
        )
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )

    @pytest.mark.tag("LegalHold")
    def test_object_lock_put_legal_hold_invalid_bucket(self):
        key = "test_object_lock_put_legal_hold_invalid_bucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=key,
                LegalHold={"Status": "ON"},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("LegalHold")
    def test_object_lock_put_legal_hold_invalid_status(self):
        key = "test_object_lock_put_legal_hold_invalid_status"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 31)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.put_object_legal_hold(
                Bucket=bucket_name,
                Key=key,
                LegalHold={"Status": "abc"},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML

    @pytest.mark.tag("LegalHold")
    def test_object_lock_get_legal_hold(self):
        key = "test_object_lock_get_legal_hold"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 32)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "ON"},
        )
        response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        assert response["LegalHold"]["Status"] == "ON"
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )
        response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        assert response["LegalHold"]["Status"] == "OFF"

    @pytest.mark.tag("LegalHold")
    def test_object_lock_get_legal_hold_invalid_bucket(self):
        key = "test_object_lock_get_legal_hold_invalid_bucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("LegalHold")
    def test_object_lock_delete_object_with_legal_hold_on(self):
        key = "test_object_lock_delete_object_with_legal_hold_on"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 34)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "ON"},
        )
        with pytest.raises(ClientError) as exc_info:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=put_response["VersionId"])
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc_info.value.response["Error"]["Code"] == md.ACCESS_DENIED
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )

    @pytest.mark.tag("LegalHold")
    def test_object_lock_delete_object_with_legal_hold_off(self):
        key = "test_object_lock_delete_object_with_legal_hold_off"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 35)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=put_response["VersionId"])

    @pytest.mark.tag("LegalHold")
    def test_object_lock_get_obj_metadata(self):
        key = "test_object_lock_get_obj_metadata"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 36)
        put_response = client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "ON"},
        )
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["ObjectLockMode"] == retention["Mode"]
        assert response["ObjectLockRetainUntilDate"] == retention["RetainUntilDate"]
        assert response["ObjectLockLegalHoldStatus"] == "ON"
        client.put_object_legal_hold(
            Bucket=bucket_name,
            Key=key,
            LegalHold={"Status": "OFF"},
        )
        client.delete_object(
            Bucket=bucket_name,
            Key=key,
            VersionId=put_response["VersionId"],
            BypassGovernanceRetention=True,
        )

    PAST_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.tag("retention")
    def test_object_lock_get_not_configured(self):
        key = "test_object_lock_get_not_configured"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 37)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # 보존 기한·법적 보존을 설정한 적이 없는 오브젝트
        self.assert_client_error(
            lambda: client.get_object_retention(Bucket=bucket_name, Key=key),
            404,
            md.NO_SUCH_OBJECT_LOCK_CONFIGURATION,
        )
        self.assert_client_error(
            lambda: client.get_object_legal_hold(Bucket=bucket_name, Key=key),
            404,
            md.NO_SUCH_OBJECT_LOCK_CONFIGURATION,
        )

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention_past_date(self):
        key = "test_object_lock_put_obj_retention_past_date"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 38)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self.assert_client_error(
            lambda: client.put_object_retention(
                Bucket=bucket_name, Key=key, Retention={"Mode": "GOVERNANCE", "RetainUntilDate": self.PAST_DATE}
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_object_lock_key_not_exist(self):
        key = "test_object_lock_key_not_exist"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 39)
        requests = [
            lambda: client.put_object_retention(
                Bucket=bucket_name, Key=key, Retention={"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
            ),
            lambda: client.get_object_retention(Bucket=bucket_name, Key=key),
            lambda: client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={"Status": "ON"}),
            lambda: client.get_object_legal_hold(Bucket=bucket_name, Key=key),
        ]
        for request in requests:
            self.assert_client_error(request, 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_object_invalid_headers(self):
        key = "test_object_lock_put_object_invalid_headers"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 40)
        invalid_headers = [
            {"ObjectLockMode": "GOVERNANCE"},  # 모드만
            {"ObjectLockRetainUntilDate": RETENTION_DATE},  # 기한만
            {"ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": self.PAST_DATE},  # 지난 기한
            {"ObjectLockMode": "INVALID", "ObjectLockRetainUntilDate": RETENTION_DATE},
            {"ObjectLockLegalHoldStatus": "INVALID"},
        ]
        for headers in invalid_headers:
            self.assert_client_error(
                lambda headers=headers: client.put_object(Bucket=bucket_name, Key=key, Body=b"content", **headers),
                400,
                md.INVALID_ARGUMENT,
            )
        self.assert_client_error(lambda: client.head_object(Bucket=bucket_name, Key=key), 404, "404")

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_object_invalid_bucket(self):
        key = "test_object_lock_put_object_invalid_bucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 41)
        # Object Lock 을 켜지 않은 버킷에는 잠금 헤더를 쓸 수 없다
        for headers in (
            {"ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": RETENTION_DATE},
            {"ObjectLockLegalHoldStatus": "ON"},
        ):
            self.assert_client_error(
                lambda headers=headers: client.put_object(Bucket=bucket_name, Key=key, Body=b"content", **headers),
                400,
                md.INVALID_REQUEST,
            )

    @pytest.mark.tag("legalhold")
    def test_object_lock_legal_hold_on_put_object(self):
        key = "test_object_lock_legal_hold_on_put_object"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 42)
        # 잠금 헤더가 있는 PutObject 는 Content-MD5 나 체크섬이 있어야 한다
        version_id = client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"content",
            ContentMD5=utils.get_md5("content"),
            ObjectLockLegalHoldStatus="ON",
        )["VersionId"]
        try:
            response = client.head_object(Bucket=bucket_name, Key=key)
            assert response["ObjectLockLegalHoldStatus"] == "ON"
            assert "ObjectLockMode" not in response

            # 버전을 지정하지 않은 삭제(삭제 마커)와 덮어쓰기는 막지 않는다
            assert client.delete_object(Bucket=bucket_name, Key=key)["DeleteMarker"] is True
            assert client.put_object(Bucket=bucket_name, Key=key, Body=b"new")["VersionId"] != version_id

            # 법적 보존 중인 버전은 bypass 를 줘도 지울 수 없다
            self.assert_client_error(
                lambda: client.delete_object(
                    Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True
                ),
                403,
                md.ACCESS_DENIED,
            )
            response = client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": [{"Key": key, "VersionId": version_id}]}
            )
            assert not response.get("Deleted")
            assert response["Errors"][0]["Code"] == md.ACCESS_DENIED
            assert self.get_body(client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)) == "content"
        finally:
            client.put_object_legal_hold(
                Bucket=bucket_name, Key=key, VersionId=version_id, LegalHold={"Status": "OFF"}
            )

    @pytest.mark.tag("ERROR")
    def test_object_lock_delete_marker(self):
        key = "test_object_lock_delete_marker"
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 43)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        marker_id = client.delete_object(Bucket=bucket_name, Key=key)["VersionId"]
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": RETENTION_DATE}
        # 삭제 마커에는 보존 기한·법적 보존을 걸 수 없다(버전을 지정하든 않든)
        for extra in ({"VersionId": marker_id}, {}):
            requests = [
                lambda extra=extra: client.put_object_retention(
                    Bucket=bucket_name, Key=key, Retention=retention, **extra
                ),
                lambda extra=extra: client.get_object_retention(Bucket=bucket_name, Key=key, **extra),
                lambda extra=extra: client.put_object_legal_hold(
                    Bucket=bucket_name, Key=key, LegalHold={"Status": "ON"}, **extra
                ),
            ]
            for request in requests:
                self.assert_client_error(request, 405, md.METHOD_NOT_ALLOWED)

    @pytest.mark.tag("ERROR")
    def test_object_lock_put_obj_lock_invalid_rule(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 44)

        def put(conf: dict):
            return client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)

        def rule(**retention) -> dict:
            return {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": retention}}

        # ObjectLockEnabled 누락, 모드 누락
        self.assert_client_error(
            lambda: put({"Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}}}), 400, md.MALFORMED_XML
        )
        self.assert_client_error(lambda: put(rule(Days=1)), 400, md.MALFORMED_XML)
        # 기간 누락
        self.assert_client_error(lambda: put(rule(Mode="GOVERNANCE")), 400, md.INVALID_REQUEST)
        # 기본 보존 기간은 36500일(100년)까지
        self.assert_client_error(lambda: put(rule(Mode="GOVERNANCE", Days=36501)), 400, md.INVALID_ARGUMENT)
        self.assert_client_error(lambda: put(rule(Mode="GOVERNANCE", Years=101)), 400, md.INVALID_ARGUMENT)
        put(rule(Mode="GOVERNANCE", Days=36500))
        response = client.get_object_lock_configuration(Bucket=bucket_name)
        assert response["ObjectLockConfiguration"]["Rule"]["DefaultRetention"] == {"Mode": "GOVERNANCE", "Days": 36500}

    @pytest.mark.tag("Check")
    def test_object_lock_put_obj_lock_remove_rule(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 45)
        client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
            },
        )
        # Rule 없이 다시 넣으면 기본 보존 규칙이 지워진다
        client.put_object_lock_configuration(
            Bucket=bucket_name, ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"}
        )
        response = client.get_object_lock_configuration(Bucket=bucket_name)
        assert response["ObjectLockConfiguration"] == {"ObjectLockEnabled": "Enabled"}

        key = "test_object_lock_put_obj_lock_remove_rule"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        assert "ObjectLockMode" not in client.head_object(Bucket=bucket_name, Key=key)

    @pytest.mark.tag("Access")
    def test_object_lock_get_obj_lock_access_denied(self):
        client = self.get_client()
        bucket_name = self.create_bucket_object_lock(client, 46)
        self.assert_client_error(
            lambda: self.get_alt_client().get_object_lock_configuration(Bucket=bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: client.get_object_lock_configuration(Bucket=self.get_new_bucket_name_only(46)),
            404,
            md.NO_SUCH_BUCKET,
        )
