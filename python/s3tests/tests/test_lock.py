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
        key = "testObjectLockPutObject"
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
        key = "testObjectLockCopyObject-lock"
        key_copy = key + "-copy"
        key2 = "testObjectLockCopyObject"
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
        key = "testObjectLockMultipart"
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
        key = "testObjectLockMD5"
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
        key = "testObjectLockPutObjRetention"
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
        key = "testObjectLockPutObjRetentionInvalidBucket"
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
        key = "testObjectLockPutObjRetentionInvalidMode"
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
        key = "testObjectLockGetObjRetention"
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
        key = "testObjectLockGetObjRetentionInvalidBucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object_retention(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("retention")
    def test_object_lock_put_obj_retention_versionid(self):
        key = "testObjectLockPutObjRetentionVersionid"
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
        key = "testObjectLockPutObjRetentionOverrideDefaultRetention"
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
        key = "testObjectLockPutObjRetentionIncreasePeriod"
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
        key = "testObjectLockPutObjRetentionShortenPeriod"
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
        key = "testObjectLockPutObjRetentionShortenPeriodBypass"
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
        key = "testObjectLockDeleteObjectWithRetention"
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
        key = "testObjectLockDeleteObjectWithRetentionBypass"
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
            key = f"testObjectLockDeleteObjectsWithRetentionBypass-{i:03d}"
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
        key = "testObjectLockPutLegalHold"
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
        key = "testObjectLockPutLegalHoldInvalidBucket"
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
        key = "testObjectLockPutLegalHoldInvalidStatus"
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
        key = "testObjectLockGetLegalHold"
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
        key = "testObjectLockGetLegalHoldInvalidBucket"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        with pytest.raises(ClientError) as exc_info:
            client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_REQUEST

    @pytest.mark.tag("LegalHold")
    def test_object_lock_delete_object_with_legal_hold_on(self):
        key = "testObjectLockDeleteObjectWithLegalHoldOn"
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
        key = "testObjectLockDeleteObjectWithLegalHoldOff"
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
        key = "testObjectLockGetObjMetadata"
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
