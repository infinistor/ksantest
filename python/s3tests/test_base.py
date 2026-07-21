"""Common test base ported from Java testV2/TestBase."""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, TypeVar

import boto3
from urllib.parse import unquote, unquote_plus
import pytest
import urllib3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError

from s3tests.auth import aws4_signer_base as aws4
from s3tests.config import S3Config, resolve_config_path
from s3tests.data import aes256, backend_headers as bh
from s3tests.data import main_data as md
from s3tests.data.http_result import HttpResult
from s3tests.data.multipart_upload_data import MultipartUploadV2Data
from s3tests.data.user_data import UserData
from s3tests.utils import checksum, net_utils, utils

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_PART_SIZE = 5 * md.MB
SSE_CUSTOMER_ALGORITHM = "AES256"
SSE_KEY = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="
SSE_KEY_MD5 = "DWygnHRtgiJ77HCm+1rvHw=="

T = TypeVar("T")


class EncryptionType(Enum):
    NORMAL = "NORMAL"
    SSE_S3 = "SSE_S3"
    SSE_C = "SSE_C"


class S3TestBase:
    def setup_method(self) -> None:
        self.config = S3Config(resolve_config_path())
        self.config.get_config()
        self._buckets: List[str] = []

    # region client factory
    def _endpoint_url(self, is_secure: bool) -> str:
        if is_secure:
            if self.config.is_aws():
                return net_utils.create_region_https(self.config.region_name)
            return net_utils.create_url_to_https(self.config.url, self.config.ssl_port)
        if self.config.is_aws():
            return net_utils.create_region_http(self.config.region_name)
        return net_utils.create_url_to_http(self.config.url, self.config.port)

    def _botocore_config(
        self,
        use_chunk_encoding: bool = True,
        request_checksum_calculation: str = "when_required",
        response_checksum_validation: str = "when_required",
    ) -> Config:
        return Config(
            signature_version=self.config.get_boto_signature_version(),
            s3={
                "addressing_style": "path",
                "payload_signing_enabled": use_chunk_encoding,
            },
            request_checksum_calculation=request_checksum_calculation,
            response_checksum_validation=response_checksum_validation,
            retries={"max_attempts": 1},
            connect_timeout=300,
            read_timeout=300,
        )

    def create_client(
        self,
        is_secure: bool,
        user: Optional[UserData],
        use_chunk_encoding: bool = True,
        extra_headers: Optional[Dict[str, str]] = None,
        request_checksum_calculation: str = "when_required",
        response_checksum_validation: str = "when_required",
    ) -> Any:
        endpoint_url = self._endpoint_url(is_secure)
        kwargs: Dict[str, Any] = {
            "service_name": "s3",
            "endpoint_url": endpoint_url,
            "region_name": self.config.region_name or "ap-northeast-2",
            "config": self._botocore_config(
                use_chunk_encoding,
                request_checksum_calculation,
                response_checksum_validation,
            ),
            "verify": False,
        }
        if user is None:
            kwargs["config"] = Config(
                signature_version=UNSIGNED,
                s3={"addressing_style": "path"},
                retries={"max_attempts": 1},
            )
        else:
            kwargs["aws_access_key_id"] = user.access_key
            kwargs["aws_secret_access_key"] = user.secret_key

        client = boto3.client(**kwargs)
        self._register_create_bucket_location(client)

        headers: Dict[str, str] = {}
        if user and user.x_auth_token:
            headers["X-Auth-Token"] = user.x_auth_token
        if extra_headers:
            headers.update(extra_headers)

        if headers:
            def _inject_headers(request, **kwargs):
                for key, value in headers.items():
                    request.headers[key] = value

            client.meta.events.register("before-sign.s3.*", _inject_headers)

        return client

    def _register_create_bucket_location(self, client: Any) -> None:
        """Inject LocationConstraint for AWS non-us-east-1 (boto3 does not auto-add)."""
        if not self.config.is_aws():
            return
        region = self.config.region_name or "us-east-1"
        if region == "us-east-1":
            return

        def _inject_location(params, context, **kwargs):
            if "CreateBucketConfiguration" not in params:
                params["CreateBucketConfiguration"] = {"LocationConstraint": region}

        client.meta.events.register("before-parameter-build.s3.CreateBucket", _inject_location)

    def get_old_client(self) -> Any:
        endpoint_url = net_utils.create_url_to_http(self.config.url, self.config.old_port)
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=self.config.region_name or "ap-northeast-2",
            aws_access_key_id=self.config.main_user.access_key,
            aws_secret_access_key=self.config.main_user.secret_key,
            config=Config(
                signature_version=self.config.get_boto_signature_version(),
                s3={"addressing_style": "path"},
                retries={"max_attempts": 1},
            ),
            verify=False,
        )
        self._register_create_bucket_location(client)
        return client

    def get_client(
        self,
        use_chunk_encoding: bool = False,
        request_checksum_calculation: str = "when_required",
        response_checksum_validation: str = "when_required",
    ) -> Any:
        return self.create_client(
            self.config.is_secure,
            self.config.main_user,
            use_chunk_encoding,
            request_checksum_calculation=request_checksum_calculation,
            response_checksum_validation=response_checksum_validation,
        )

    def get_client_https(self, use_chunk_encoding: bool = False) -> Any:
        return self.create_client(True, self.config.main_user, use_chunk_encoding)

    def get_alt_client(self) -> Any:
        return self.create_client(self.config.is_secure, self.config.alt_user, True)

    def get_public_client(self) -> Any:
        return self.create_client(self.config.is_secure, None, True)

    def get_bad_auth_client(self, access_key: Optional[str], secret_key: Optional[str]) -> Any:
        user = UserData(
            access_key=access_key or "aaaaaaaaaaaaaaa",
            secret_key=secret_key or "bbbbbbbbbbbbbbb",
        )
        return self.create_client(self.config.is_secure, user, True)

    def get_backend_client(self) -> Any:
        return self.create_client(
            self.config.is_secure,
            self.config.backend_user,
            True,
            extra_headers=bh.BACKEND_HEADERS,
        )

    # endregion

    # region bucket helpers
    def get_prefix(self) -> str:
        return f"v2-{self.config.bucket_prefix}"

    def get_new_bucket_name(self) -> str:
        bucket_name = utils.get_new_bucket_name(self.get_prefix())
        self._buckets.append(bucket_name)
        return bucket_name

    def get_new_bucket_name_only(self, length: Optional[int] = None) -> str:
        bucket_name = utils.get_new_bucket_name(self.get_prefix())
        if length is not None:
            if len(bucket_name) > length:
                bucket_name = bucket_name[:length]
            elif len(bucket_name) < length:
                bucket_name += utils.random_text(length - len(bucket_name))
        return bucket_name

    _ACL_UNSET = object()

    def create_bucket(
        self,
        client: Optional[Any] = None,
        object_ownership: Optional[str] = None,
        acl: Any = _ACL_UNSET,
    ) -> str:
        """Match Java createBucket overloads.

        - createBucket(client)
        - createBucket(client, ownership)
        - createBucket(client, ownership, acl): disable BPA, then putBucketAcl
          (acl may be None; BPA is still disabled — used by createBucketCannedAcl)
        """
        client = client or self.get_client()
        bucket_name = self.get_new_bucket_name()
        params: Dict[str, Any] = {"Bucket": bucket_name}
        if object_ownership:
            params["ObjectOwnership"] = object_ownership
        self._apply_location_constraint(params)
        client.create_bucket(**params)
        # Java 3-arg overload: ownership + acl (acl may be null)
        if acl is not self._ACL_UNSET:
            self.disable_public_access_block(client, bucket_name)
            if acl is not None:
                client.put_bucket_acl(Bucket=bucket_name, ACL=acl)
        if self.config.is_old_system():
            old_params: Dict[str, Any] = {"Bucket": bucket_name}
            self._apply_location_constraint(old_params)
            self.get_old_client().create_bucket(**old_params)
        return bucket_name

    def disable_public_access_block(self, client: Any, bucket_name: str) -> None:
        """Java putPublicAccessBlock(all false) so canned/public ACLs work on AWS."""
        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )

    def create_bucket_canned_acl(self, client: Optional[Any] = None, acl: Optional[str] = None) -> str:
        """Java createBucketCannedAcl: ObjectWriter + BPA disabled (+ optional ACL)."""
        return self.create_bucket(
            client or self.get_client(), object_ownership="ObjectWriter", acl=acl
        )

    def _apply_location_constraint(self, params: Dict[str, Any]) -> None:
        """AWS requires LocationConstraint for regions other than us-east-1."""
        if not self.config.is_aws():
            return
        region = self.config.region_name or "us-east-1"
        if region == "us-east-1":
            return
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}

    def _do_create_bucket(self, client: Any, bucket_name: str, **extra: Any) -> None:
        params: Dict[str, Any] = {"Bucket": bucket_name, **extra}
        self._apply_location_constraint(params)
        client.create_bucket(**params)

    def create_objects(self, client: Any, *args: Any) -> Optional[str]:
        """Java-compatible createObjects overloads.

        create_objects(client, key1, key2, ...) -> bucket_name
        create_objects(client, [keys]) -> bucket_name
        create_objects(client, bucket_name, [keys]) -> None
        """
        if not args:
            raise TypeError("create_objects requires keys or (bucket_name, keys)")

        if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], (list, tuple)):
            bucket_name = args[0]
            keys = list(args[1])
            self._put_object_keys(client, bucket_name, keys)
            return None

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            keys = list(args[0])
        else:
            keys = [str(k) for k in args]

        bucket_name = self.create_bucket(client)
        self._put_object_keys(client, bucket_name, keys)
        return bucket_name

    def create_objects_keys(self, *args: Any) -> str:
        """Java createObjects(List) / createObjects(client, List)."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            client = self.get_client()
            keys = list(args[0])
        elif len(args) == 2 and isinstance(args[1], (list, tuple)):
            client = args[0]
            keys = list(args[1])
        else:
            raise TypeError("create_objects_keys expects ([keys]) or (client, [keys])")
        bucket_name = self.create_bucket(client)
        self._put_object_keys(client, bucket_name, keys)
        return bucket_name

    def create_objects_list(
        self, client: Any, keys: Sequence[str], bucket_name: Optional[str] = None
    ) -> str:
        bucket_name = bucket_name or self.create_bucket(client)
        self._put_object_keys(client, bucket_name, list(keys))
        return bucket_name

    @staticmethod
    def _put_object_keys(client: Any, bucket_name: str, keys: Sequence[str]) -> None:
        for key in keys:
            body = b"" if key.endswith("/") else key.encode("utf-8")
            client.put_object(Bucket=bucket_name, Key=key, Body=body)

    def create_key_with_random_content(
        self, client: Any, key: str, size: int, bucket_name: Optional[str] = None
    ) -> str:
        if size < 1:
            size = 7 * md.MB
        data = utils.random_text_to_long(size)
        if bucket_name is None:
            bucket_name = self.create_bucket(client)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
        return bucket_name

    def delete_bucket_list(self, bucket_name: str) -> None:
        if bucket_name in self._buckets:
            self._buckets.remove(bucket_name)

    def clear(self, test_name: str = "") -> None:
        if test_name:
            print(f"Test End : {test_name}")
        for bucket_name in self._buckets:
            print(f"Bucket : {bucket_name}")
        if not self.config.not_delete:
            self.bucket_clear()

    def bucket_clear(self) -> None:
        client = self.get_client()
        while self._buckets:
            bucket_name = self._buckets.pop(0)
            if bucket_name:
                self.bucket_clear_one(client, bucket_name)

    def bucket_clear_one(self, client: Any, bucket_name: str) -> None:
        self._abort_bucket_multipart_uploads(client, bucket_name)
        self._clear_bucket_object_versions(client, bucket_name)
        try:
            client.delete_bucket(Bucket=bucket_name)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            print(f"Error : Bucket({bucket_name}) Delete Failed({code}, {status})")

    def _abort_bucket_multipart_uploads(self, client: Any, bucket_name: str) -> None:
        try:
            key_marker = None
            upload_id_marker = None
            while True:
                params: Dict[str, Any] = {"Bucket": bucket_name}
                if key_marker:
                    params["KeyMarker"] = key_marker
                if upload_id_marker:
                    params["UploadIdMarker"] = upload_id_marker
                response = client.list_multipart_uploads(**params)
                for upload in response.get("Uploads", []):
                    client.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                if not response.get("IsTruncated"):
                    break
                key_marker = response.get("NextKeyMarker")
                upload_id_marker = response.get("NextUploadIdMarker")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            print(f"Error : Bucket({bucket_name}) Abort Multipart Uploads Failed({code}, {status})")

    def _clear_bucket_object_versions(self, client: Any, bucket_name: str) -> None:
        try:
            while True:
                response = client.list_object_versions(Bucket=bucket_name)
                for version in response.get("Versions", []):
                    client.delete_object(
                        Bucket=bucket_name,
                        Key=version["Key"],
                        VersionId=version["VersionId"],
                    )
                for marker in response.get("DeleteMarkers", []):
                    client.delete_object(
                        Bucket=bucket_name,
                        Key=marker["Key"],
                        VersionId=marker["VersionId"],
                    )
                if not response.get("IsTruncated"):
                    break
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            print(f"Error : Bucket({bucket_name}) Clear Failed({code}, {status})")

    # endregion

    # region list helpers
    @staticmethod
    def get_bucket_list(response: Dict[str, Any]) -> List[str]:
        if not response:
            return []
        return [item["Name"] for item in response.get("Buckets", [])]

    @staticmethod
    def _maybe_decode_list_value(value: str, encoding_type: Optional[str]) -> str:
        if encoding_type != "url":
            return value
        if "%" in value:
            return unquote(value)
        # S3 encodes spaces as '+' in prefixes (e.g. quux+ab/). Do not treat '+' as
        # space in object keys like asdf+b that are already decoded by the SDK.
        if "+" in value and " " not in value and value.endswith("/"):
            return unquote_plus(value)
        return value

    @staticmethod
    def get_response_prefix(response: Dict[str, Any]) -> Optional[str]:
        prefix = response.get("Prefix")
        if prefix is None:
            return None
        return S3TestBase._maybe_decode_list_value(prefix, response.get("EncodingType"))

    @staticmethod
    def get_response_start_after(response: Dict[str, Any]) -> Optional[str]:
        start_after = response.get("StartAfter")
        if start_after is None:
            return None
        return S3TestBase._maybe_decode_list_value(start_after, response.get("EncodingType"))

    @staticmethod
    def is_blank_string(value: Optional[str]) -> bool:
        return value is None or value == "" or value.isspace()

    @staticmethod
    def get_keys(
        object_list: Optional[Sequence[Dict[str, Any]]],
        encoding_type: Optional[str] = None,
    ) -> List[str]:
        if not object_list:
            return []
        return [
            S3TestBase._maybe_decode_list_value(item["Key"], encoding_type)
            for item in object_list
        ]

    @staticmethod
    def get_prefix_list(
        prefix_list: Optional[Sequence[Dict[str, Any]]],
        encoding_type: Optional[str] = None,
    ) -> List[str]:
        if not prefix_list:
            return []
        return [
            S3TestBase._maybe_decode_list_value(item["Prefix"], encoding_type)
            for item in prefix_list
        ]

    @staticmethod
    def get_body(response: Dict[str, Any]) -> str:
        body = response["Body"].read()
        if isinstance(body, bytes):
            return body.decode("utf-8")
        return str(body)

    @staticmethod
    def version_ids(versions: Optional[Sequence[Dict[str, Any]]]) -> List[str]:
        if not versions:
            return []
        return [item["VersionId"] for item in versions]

    @staticmethod
    def get_keys2(
        object_list: Optional[Sequence[Dict[str, Any]]],
        encoding_type: Optional[str] = None,
    ) -> List[str]:
        return S3TestBase.get_keys(object_list, encoding_type)

    @staticmethod
    def _normalize_owner_display_name(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value

    @staticmethod
    def _normalize_list_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(microsecond=0)
        return value.astimezone(timezone.utc).replace(microsecond=0)

    @staticmethod
    def assert_list_object_return_data(obj: Dict[str, Any], key_data: Dict[str, Any]) -> None:
        assert key_data["e_tag"] == obj["ETag"]
        assert key_data["content_length"] == obj["Size"]
        owner = obj.get("Owner")
        if owner and owner.get("ID") is not None:
            assert key_data["id"] == owner["ID"]
            # AWS omits Owner.DisplayName in some regions (e.g. ap-northeast-2).
            if "DisplayName" in owner:
                expected = S3TestBase._normalize_owner_display_name(key_data.get("display_name"))
                actual = S3TestBase._normalize_owner_display_name(owner.get("DisplayName"))
                assert expected == actual
        assert S3TestBase._normalize_list_datetime(key_data["last_modified"]) == S3TestBase._normalize_list_datetime(
            obj["LastModified"]
        )

    @staticmethod
    def get_object_to_key(key: str, key_list: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for item in key_list:
            if item.get("key") == key:
                return item
        return None

    # endregion

    # region ACL / policy helpers
    @staticmethod
    def create_public_grantee() -> Dict[str, str]:
        return {"Type": "Group", "URI": md.ALL_USERS}

    @staticmethod
    def create_authenticated_grantee() -> Dict[str, str]:
        return {"Type": "Group", "URI": md.AUTHENTICATED_USERS}

    @staticmethod
    def _as_grantee(principal: Dict[str, str]) -> Dict[str, str]:
        """Ensure Grantee has Type (botocore requires it; Owner dict often omits Type)."""
        if "Type" in principal:
            return dict(principal)
        if "URI" in principal:
            return {"Type": "Group", "URI": principal["URI"]}
        out = {"Type": "CanonicalUser", "ID": principal["ID"]}
        if principal.get("DisplayName"):
            out["DisplayName"] = principal["DisplayName"]
        return out

    def create_acl(
        self,
        owner: Dict[str, str],
        grantee: Dict[str, str],
        *permissions: str,
    ) -> Dict[str, Any]:
        owner_grantee = self._as_grantee(owner)
        grants = [
            {
                "Grantee": owner_grantee,
                "Permission": "FULL_CONTROL",
            }
        ]
        for permission in permissions:
            grants.append({"Grantee": self._as_grantee(grantee), "Permission": permission})
        return {"Owner": owner, "Grants": grants}

    def create_acl_default(self) -> Dict[str, Any]:
        owner = self.config.main_user.to_owner()
        return self.create_acl(owner, self.config.main_user.to_grantee(), "FULL_CONTROL")

    def create_public_acl(self, *permissions: str) -> Dict[str, Any]:
        owner = self.config.main_user.to_owner()
        return self.create_acl(owner, self.create_public_grantee(), *permissions)

    def create_authenticated_acl(self, *permissions: str) -> Dict[str, Any]:
        owner = self.config.main_user.to_owner()
        return self.create_acl(owner, self.create_authenticated_grantee(), *permissions)

    def create_alt_acl(self, *permissions: str) -> Dict[str, Any]:
        owner = self.config.main_user.to_owner()
        return self.create_acl(owner, self.config.alt_user.to_grantee(), *permissions)

    def setup_acl_bucket(
        self,
        acl: str,
        keys: Sequence[str],
        object_ownership: str = "ObjectWriter",
    ) -> str:
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership=object_ownership, acl=acl)
        self.create_objects_list(client, keys, bucket_name)
        return bucket_name

    def setup_acl_objects(
        self,
        bucket_acl: str,
        object_acl: str,
        *keys: str,
        object_ownership: str = "ObjectWriter",
    ) -> str:
        client = self.get_client()
        bucket_name = self.create_bucket(client, object_ownership=object_ownership, acl=bucket_acl)
        for key in keys:
            client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL=object_acl)
        return bucket_name

    def setup_acl_objects_by_alt(
        self,
        bucket_acl: str,
        object_acl: str,
        *keys: str,
        object_ownership: str = "ObjectWriter",
    ) -> str:
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(
            self.get_client(), object_ownership=object_ownership, acl=bucket_acl
        )
        for key in keys:
            alt_client.put_object(
                Bucket=bucket_name, Key=key, Body=key.encode("utf-8"), ACL=object_acl
            )
        return bucket_name

    def setup_acl_objects_by_alt_with_ownership(
        self,
        object_ownership: str,
        bucket_acl: str,
        object_acl: str,
        *keys: str,
    ) -> str:
        return self.setup_acl_objects_by_alt(
            bucket_acl, object_acl, *keys, object_ownership=object_ownership
        )

    def setup_acl_objects_with_ownership(
        self,
        object_ownership: str,
        bucket_acl: str,
        object_acl: str,
        *keys: str,
    ) -> str:
        return self.setup_acl_objects(
            bucket_acl, object_acl, *keys, object_ownership=object_ownership
        )

    def add_bucket_user_grant(self, bucket_name: str, grant: Dict[str, Any]) -> Dict[str, Any]:
        client = self.get_client()
        response = client.get_bucket_acl(Bucket=bucket_name)
        grants = list(response.get("Grants", []))
        grants.append(grant)
        return self.normalize_acl_policy({"Owner": response["Owner"], "Grants": grants})

    def normalize_acl_policy(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        grants = []
        for grant in policy.get("Grants", []):
            grants.append(
                {
                    "Grantee": self._as_grantee(grant["Grantee"]),
                    "Permission": grant["Permission"],
                }
            )
        return {"Owner": policy["Owner"], "Grants": grants}

    def setup_bucket_permission(self, permission: str) -> str:
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        acl = self.create_alt_acl(permission)
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)
        return bucket_name

    def setup_object_permission(self, key: str, permission: str) -> str:
        client = self.get_client()
        bucket_name = self.create_bucket(
            client, object_ownership="ObjectWriter", acl="public-read-write"
        )
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        acl = self.create_alt_acl(permission)
        client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl)
        return bucket_name

    @staticmethod
    def _grants_sort(grants: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def sort_key(grant: Dict[str, Any]) -> str:
            grantee = grant.get("Grantee", {})
            grantee_id = grantee.get("ID") or grantee.get("URI", "")
            return f"{grantee_id}{grant.get('Permission', '')}"

        unique: Dict[str, Dict[str, Any]] = {}
        for grant in grants:
            unique[sort_key(grant)] = grant
        return sorted(unique.values(), key=sort_key)

    def check_acl(self, expected: Dict[str, Any], actual: Dict[str, Any]) -> None:
        if self.config.is_aws():
            assert expected["Owner"]["ID"] == actual["Owner"]["ID"]
        else:
            assert expected["Owner"] == actual["Owner"]

        expected_grants = self._grants_sort(expected.get("Grants", []))
        actual_grants = self._grants_sort(actual.get("Grants", []))
        assert len(expected_grants) == len(actual_grants)
        for index, expected_grant in enumerate(expected_grants):
            actual_grant = actual_grants[index]
            assert expected_grant["Permission"] == actual_grant["Permission"]
            exp_g = self._as_grantee(expected_grant["Grantee"])
            act_g = self._as_grantee(actual_grant["Grantee"])
            assert exp_g.get("ID") == act_g.get("ID")
            assert exp_g.get("Type") == act_g.get("Type")
            assert exp_g.get("URI") == act_g.get("URI")

    def check_bucket_acl(self, permission: str) -> None:
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        acl = self.create_alt_acl(permission)
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)
        response = client.get_bucket_acl(Bucket=bucket_name)
        self.check_acl(acl, response)

    def check_object_acl(self, permission: str) -> None:
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        key = f"testObjectPermission{permission}"
        acl = self.create_acl(
            self.config.main_user.to_owner(),
            self.config.main_user.to_grantee(),
            permission,
        )
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.put_object_acl(Bucket=bucket_name, Key=key, AccessControlPolicy=acl)
        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        self.check_acl(acl, response)

    @staticmethod
    def _tagging_sort(tags: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
        return sorted(tags, key=lambda tag: f"{tag['Key']}{tag['Value']}", reverse=True)

    def tag_compare(
        self,
        expected: Sequence[Dict[str, str]],
        actual: Sequence[Dict[str, str]],
    ) -> None:
        assert len(expected) == len(actual)
        order_expected = self._tagging_sort(expected)
        order_actual = self._tagging_sort(actual)
        for index, expected_tag in enumerate(order_expected):
            actual_tag = order_actual[index]
            assert expected_tag["Key"] == actual_tag["Key"]
            assert expected_tag["Value"] == actual_tag["Value"]

    @staticmethod
    def make_json_statement(
        action: str,
        resource: str,
        effect: Optional[str] = None,
        principal: Optional[Dict[str, Any]] = None,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        statement: Dict[str, Any] = {
            md.POLICY_EFFECT: effect or md.POLICY_EFFECT_ALLOW,
            md.POLICY_PRINCIPAL: principal or {"AWS": "*"},
            md.POLICY_ACTION: action,
            md.POLICY_RESOURCE: resource,
        }
        if conditions:
            statement[md.POLICY_CONDITION] = conditions
        return statement

    @staticmethod
    def make_json_policy(
        action: str,
        resource: str,
        principal: Optional[Dict[str, Any]] = None,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> str:
        policy = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                S3TestBase.make_json_statement(action, resource, None, principal, conditions)
            ],
        }
        return json.dumps(policy)

    @staticmethod
    def make_json_policy_statements(*statements: Dict[str, Any]) -> str:
        policy = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: list(statements),
        }
        return json.dumps(policy)

    @staticmethod
    def make_simple_tag_set(count: int) -> List[Dict[str, str]]:
        return [{"Key": str(i), "Value": str(i)} for i in range(count)]

    @staticmethod
    def make_detail_tag_set(count: int, key_size: int, value_size: int) -> List[Dict[str, str]]:
        return [
            {
                "Key": utils.random_text_to_long(key_size),
                "Value": utils.random_text_to_long(value_size),
            }
            for _ in range(count)
        ]

    # endregion

    # region validation helpers
    @staticmethod
    def assert_client_error(func: Callable[[], T], status_code: int, error_code: str) -> None:
        with pytest.raises(ClientError) as exc_info:
            func()
        error = exc_info.value.response.get("Error", {})
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == status_code
        actual_code = error.get("Code")
        # botocore HEAD ops (HeadBucket/HeadObject) often put the status string in Error.Code
        # because there is no XML error body. Java ACL deny helpers only assert status for headBucket.
        if actual_code in (error_code, str(status_code)):
            return
        assert actual_code == error_code

    @staticmethod
    def succeed_get_object(client: Any, bucket_name: str, key: str, content: str) -> None:
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = S3TestBase.get_body(response)
        assert body == content, md.NOT_MATCHED

    @staticmethod
    def failed_get_object(client: Any, bucket_name: str, key: str, status_code: int, error_code: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key),
            status_code,
            error_code,
        )

    @staticmethod
    def succeed_put_object(client: Any, bucket_name: str, key: str, content: str) -> None:
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    @staticmethod
    def failed_put_object(client: Any, bucket_name: str, key: str, status_code: int, error_code: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key, Body=b""),
            status_code,
            error_code,
        )

    @staticmethod
    def succeed_list_objects(client: Any, bucket_name: str, keys: Sequence[str]) -> None:
        response = client.list_objects(Bucket=bucket_name)
        key_list = S3TestBase.get_keys(response.get("Contents"))
        assert list(keys) == key_list

    @staticmethod
    def failed_list_objects(client: Any, bucket_name: str, status_code: int, error_code: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.list_objects(Bucket=bucket_name),
            status_code,
            error_code,
        )

    @staticmethod
    def check_bucket_acl_allow_read(client: Any, bucket_name: str) -> None:
        client.head_bucket(Bucket=bucket_name)

    @staticmethod
    def check_bucket_acl_deny_read(client: Any, bucket_name: str) -> None:
        # Match Java: assert status 403 only for HeadBucket.
        with pytest.raises(ClientError) as exc_info:
            client.head_bucket(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    @staticmethod
    def check_bucket_acl_allow_read_acp(client: Any, bucket_name: str) -> None:
        client.get_bucket_acl(Bucket=bucket_name)

    @staticmethod
    def check_bucket_acl_deny_read_acp(client: Any, bucket_name: str) -> None:
        S3TestBase.assert_client_error(lambda: client.get_bucket_acl(Bucket=bucket_name), 403, md.ACCESS_DENIED)

    @staticmethod
    def check_bucket_acl_allow_write(client: Any, bucket_name: str) -> None:
        key = "checkBucketAclAllowWrite"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))
        client.delete_object(Bucket=bucket_name, Key=key)

    @staticmethod
    def check_bucket_acl_deny_write(client: Any, bucket_name: str) -> None:
        key = "checkBucketAclDenyWrite"
        S3TestBase.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8")),
            403,
            md.ACCESS_DENIED,
        )

    @staticmethod
    def check_bucket_acl_allow_write_acp(client: Any, bucket_name: str) -> None:
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read-write")

    @staticmethod
    def check_bucket_acl_deny_write_acp(client: Any, bucket_name: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.put_bucket_acl(Bucket=bucket_name, ACL="public-read"),
            403,
            md.ACCESS_DENIED,
        )

    @staticmethod
    def check_object_acl_allow_read(client: Any, bucket_name: str, key: str) -> None:
        response = client.get_object(Bucket=bucket_name, Key=key)
        S3TestBase.get_body(response)

    @staticmethod
    def check_object_acl_deny_read(client: Any, bucket_name: str, key: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.get_object(Bucket=bucket_name, Key=key),
            403,
            md.ACCESS_DENIED,
        )

    @staticmethod
    def check_object_acl_allow_read_acp(client: Any, bucket_name: str, key: str) -> None:
        client.get_object_acl(Bucket=bucket_name, Key=key)

    @staticmethod
    def check_object_acl_deny_read_acp(client: Any, bucket_name: str, key: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.get_object_acl(Bucket=bucket_name, Key=key),
            403,
            md.ACCESS_DENIED,
        )

    @staticmethod
    def check_object_acl_allow_write(client: Any, bucket_name: str, key: str) -> None:
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]},
        )

    @staticmethod
    def check_object_acl_deny_write(client: Any, bucket_name: str, key: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8")),
            403,
            md.ACCESS_DENIED,
        )

    @staticmethod
    def check_object_acl_allow_write_acp(client: Any, bucket_name: str, key: str) -> None:
        client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read-write")

    @staticmethod
    def check_object_acl_deny_write_acp(client: Any, bucket_name: str, key: str) -> None:
        S3TestBase.assert_client_error(
            lambda: client.put_object_acl(Bucket=bucket_name, Key=key, ACL="public-read"),
            403,
            md.ACCESS_DENIED,
        )

    def delay(self, milliseconds: int) -> None:
        time.sleep(milliseconds / 1000.0)

    def get_time_to_add_seconds(self, seconds: int) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=seconds)

    def get_time_to_add_minutes(self, minutes: int) -> str:
        dt = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def create_url(self, bucket_name: str, key: Optional[str] = None) -> str:
        protocol = md.HTTPS if self.config.is_secure else md.HTTP
        port = self.config.ssl_port if self.config.is_secure else self.config.port
        if self.config.is_aws():
            if key:
                return f"{protocol}{bucket_name}.s3-{self.config.region_name}.amazonaws.com/{key}"
            return f"{protocol}{bucket_name}.s3-{self.config.region_name}.amazonaws.com"
        base = net_utils.append_port_if_non_default(
            f"{protocol}{self.config.url}".rstrip("/"),
            port,
            443 if self.config.is_secure else 80,
        )
        if key:
            return f"{base}/{bucket_name}/{key}"
        return f"{base}/{bucket_name}"

    def check_configure_versioning_retry(self, bucket_name: str, status: str) -> None:
        client = self.get_client()
        client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": status},
        )
        read_status = None
        for _ in range(5):
            try:
                response = client.get_bucket_versioning(Bucket=bucket_name)
                read_status = response.get("Status")
                if read_status == status:
                    break
                self.delay(1000)
            except Exception:
                read_status = None
        assert read_status == status

    def check_obj_content(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        version_id: str,
        content: Optional[str],
    ) -> None:
        response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        if content is not None:
            body = self.get_body(response)
            assert body == content, md.NOT_MATCHED
        else:
            assert response is None

    def validate_list_object(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str,
        marker: str,
        max_keys: int,
        is_truncated: bool,
        check_keys: List[str],
        check_prefixes: List[str],
        next_marker: str,
    ) -> str:
        client = self.get_client()
        response = client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            Marker=marker,
            MaxKeys=max_keys,
        )
        assert response.get("IsTruncated") == is_truncated
        assert response.get("NextMarker") == next_marker
        keys = self.get_keys(response.get("Contents"))
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(keys) == len(check_keys)
        assert len(prefixes) == len(check_prefixes)
        assert keys == check_keys
        assert prefixes == check_prefixes
        return response.get("NextMarker", "")

    def validate_list_object_v2(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str,
        continuation_token: Optional[str],
        max_keys: int,
        is_truncated: bool,
        check_keys: List[str],
        check_prefixes: List[str],
        last: bool,
    ) -> Optional[str]:
        client = self.get_client()
        params: Dict[str, Any] = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
            "MaxKeys": max_keys,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**params)
        assert response.get("IsTruncated") == is_truncated
        if last:
            assert response.get("NextContinuationToken") is None
        keys = self.get_keys(response.get("Contents"))
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert keys == check_keys
        assert prefixes == check_prefixes
        return response.get("NextContinuationToken")

    def sse_c_extra_args(self) -> Dict[str, str]:
        return {
            "SSECustomerAlgorithm": SSE_CUSTOMER_ALGORITHM,
            "SSECustomerKey": SSE_KEY,
            "SSECustomerKeyMD5": SSE_KEY_MD5,
        }

    @staticmethod
    def s3event_compare(expected: Sequence[str], actual: Sequence[str]) -> None:
        assert list(expected) == list(actual)

    @staticmethod
    def prefix_lifecycle_configuration_check(expected: Sequence[Dict[str, Any]], actual: Sequence[Dict[str, Any]]) -> None:
        assert len(expected) == len(actual)
        for left, right in zip(expected, actual):
            assert left.get("ID") == right.get("ID")
            assert left.get("Expiration", {}).get("Date") == right.get("Expiration", {}).get("Date")
            assert left.get("Expiration", {}).get("Days") == right.get("Expiration", {}).get("Days")
            assert left.get("Expiration", {}).get("ExpiredObjectDeleteMarker") == right.get("Expiration", {}).get(
                "ExpiredObjectDeleteMarker"
            )
            assert left.get("Filter", {}).get("Prefix") == right.get("Filter", {}).get("Prefix")
            assert left.get("Status") == right.get("Status")
            if left.get("NoncurrentVersionExpiration"):
                assert left["NoncurrentVersionExpiration"]["NoncurrentDays"] == right["NoncurrentVersionExpiration"][
                    "NoncurrentDays"
                ]
            if left.get("AbortIncompleteMultipartUpload"):
                assert left["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] == right[
                    "AbortIncompleteMultipartUpload"
                ]["DaysAfterInitiation"]

    def create_multiple_versions(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        num_versions: int,
        check_versions: bool = True,
    ) -> None:
        version_ids: List[str] = []
        contents: List[str] = []
        for index in range(num_versions):
            body = f"content-{index}"
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            version_ids.append(response["VersionId"])
            contents.append(body)
        if check_versions:
            self.check_obj_versions(client, bucket_name, key, version_ids, contents)

    def check_obj_versions(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        version_ids: Sequence[str],
        contents: Sequence[str],
    ) -> None:
        response = client.list_object_versions(Bucket=bucket_name)
        versions = list(response.get("Versions", []))
        versions.reverse()
        for index, version in enumerate(versions):
            assert version["VersionId"] == version_ids[index]
            if key:
                assert version["Key"] == key
            self.check_obj_content(client, bucket_name, key, version["VersionId"], contents[index])

    @staticmethod
    def cut_string_data(data: str, part_size: int) -> List[str]:
        result: List[str] = []
        start = 0
        while start < len(data):
            end = min(start + part_size, len(data))
            result.append(data[start:end])
            start = end
        return result

    @staticmethod
    def get_key_versions(key_list: Sequence[str]) -> List[Dict[str, str]]:
        return [{"Key": key} for key in key_list]

    @staticmethod
    def get_bytes_used(response: Dict[str, Any]) -> int:
        return sum(item.get("Size", 0) for item in response.get("Contents", []))

    def setup_multipart_upload(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        size: int,
        part_size: int = 0,
        metadata: Optional[Dict[str, str]] = None,
    ) -> MultipartUploadV2Data:
        if part_size < 1:
            part_size = DEFAULT_PART_SIZE
        upload_data = MultipartUploadV2Data(part_size=part_size)
        params: Dict[str, Any] = {"Bucket": bucket_name, "Key": key}
        if metadata:
            params["Metadata"] = metadata
        create_response = client.create_multipart_upload(**params)
        upload_data.upload_id = create_response["UploadId"]
        parts = utils.generate_random_string(size, part_size)
        for part in parts:
            upload_data.append_body(part)
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                PartNumber=upload_data.next_part_number(),
                Body=part.encode("utf-8"),
                ContentMD5=utils.get_md5(part),
            )
            upload_data.add_part(part_response["ETag"])
        return upload_data

    def multipart_upload(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        size: int,
        part_size: int,
        upload_data: MultipartUploadV2Data,
    ) -> MultipartUploadV2Data:
        parts = utils.generate_random_string(size, part_size)
        for part in parts:
            upload_data.append_body(part)
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                PartNumber=upload_data.next_part_number(),
                Body=part.encode("utf-8"),
                ContentMD5=utils.get_md5(part),
            )
            upload_data.add_part(part_response["ETag"])
        return upload_data

    def check_content_using_range(
        self,
        bucket_name: str,
        key: str,
        data: str,
        step: int,
        version_id: Optional[str] = None,
    ) -> None:
        client = self.get_client()
        head_params: Dict[str, Any] = {"Bucket": bucket_name, "Key": key}
        if version_id:
            head_params["VersionId"] = version_id
        head_response = client.head_object(**head_params)
        size = head_response["ContentLength"]
        assert len(data) == size

        index = 0
        while index < size:
            start = index
            end = min(start + step, size) - 1
            get_params: Dict[str, Any] = {
                "Bucket": bucket_name,
                "Key": key,
                "Range": f"bytes={start}-{end - 1}",
            }
            if version_id:
                get_params["VersionId"] = version_id
            response = client.get_object(**get_params)
            body = self.get_body(response)
            part_body = data[start:end]
            assert end - start == response["ContentLength"]
            assert body == part_body, md.NOT_MATCHED
            index += step

    def check_content(self, bucket_name: str, key: str, data: str, loop_count: int) -> None:
        client = self.get_client()
        for _ in range(loop_count):
            response = client.get_object(Bucket=bucket_name, Key=key)
            assert self.get_body(response) == data, md.NOT_MATCHED

    def check_content_using_random_range(self, bucket_name: str, key: str, data: str, loop_count: int) -> None:
        client = self.get_client()
        file_size = len(data)
        for _ in range(loop_count):
            start, length = self._get_random_range(file_size)
            # Match Java RangeSet: end = start + length; request bytes=start-end (length+1 bytes)
            end = start + length
            response = client.get_object(Bucket=bucket_name, Key=key, Range=f"bytes={start}-{end}")
            body = self.get_body(response)
            range_body = data[start : end + 1]
            assert response["ContentLength"] - 1 == length
            assert body == range_body, md.NOT_MATCHED

    @staticmethod
    def _get_random_range(file_size: int) -> tuple[int, int]:
        max_size = 500
        rand = random.Random()
        start = rand.randint(0, max(0, file_size - max_size * 2))
        max_length = file_size - start
        if max_length > max_size:
            max_length = max_size
        length = rand.randint(max_length, max_size + max_size - 1) if max_length <= max_size else rand.randint(1, max_length)
        if length <= 0:
            length = 1
        return start, length

    def encryption_cse_write(self, key: str, file_size: int) -> None:
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        aes_key = utils.random_text_to_long(32)
        data = utils.random_text_to_long(file_size)
        encoding_data = aes256.encrypt(data, aes_key)
        metadata = {"x-amz-meta-key": aes_key}
        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=encoding_data.encode("utf-8"),
            Metadata=metadata,
            ContentType="text/plain",
            ContentLength=len(encoding_data),
        )
        response = client.get_object(Bucket=bucket_name, Key=key)
        encoding_body = self.get_body(response)
        body = aes256.decrypt(encoding_body, aes_key)
        assert body == data, md.NOT_MATCHED

    def replication_config_compare(self, expected: Dict[str, Any], actual: Dict[str, Any]) -> None:
        assert expected["Role"] == actual["Role"]
        expected_rules = expected.get("Rules", [])
        actual_rules = actual.get("Rules", [])
        for left, right in zip(expected_rules, actual_rules):
            assert left.get("DeleteMarkerReplication") == right.get("DeleteMarkerReplication")
            assert left.get("Destination") == right.get("Destination")
            assert left.get("ExistingObjectReplication") == right.get("ExistingObjectReplication")
            assert left.get("Filter") == right.get("Filter")
            assert left.get("SourceSelectionCriteria") == right.get("SourceSelectionCriteria")
            assert left.get("Status") == right.get("Status")

    def cors_request_and_check(
        self,
        method: str,
        bucket_name: str,
        headers: Dict[str, str],
        status_code: int,
        expect_allow_origin: Optional[str],
        expect_allow_methods: Optional[str],
        key: Optional[str] = None,
    ) -> None:
        url = self.create_url(bucket_name, key)
        code, response_headers = net_utils.cors_request(url, method, headers)
        assert code == status_code
        assert response_headers.get("access-control-allow-origin") == expect_allow_origin
        assert response_headers.get("access-control-allow-methods") == expect_allow_methods

    def get_expired_date_instant(self, day: datetime, days: int) -> datetime:
        value = day + timedelta(days=days)
        if self.config.is_aws():
            value = value + timedelta(days=1)
            value = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return value

    def get_s3_presigner(self) -> Any:
        # AWS needs regional endpoint; empty config.url yields invalid "http:"
        endpoint_url = self._endpoint_url(self.config.is_secure)
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=self.config.region_name or "ap-northeast-2",
            aws_access_key_id=self.config.main_user.access_key,
            aws_secret_access_key=self.config.main_user.secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            verify=False,
        )

    def put_object_url(self, url: str, content: str) -> int:
        request = urllib.request.Request(url, data=content.encode("utf-8"), method="PUT")
        context = urllib.request.ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=context, timeout=300) as response:
            return response.status

    def get_object_url(self, url: str) -> int:
        request = urllib.request.Request(url, method="GET")
        context = urllib.request.ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=context, timeout=300) as response:
            return response.status

    def generate_presigned_put_url(self, bucket_name: str, key: str, expires_in: int = 600) -> str:
        client = self.get_s3_presigner()
        return client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=expires_in,
            HttpMethod="PUT",
        )

    def generate_presigned_get_url(self, bucket_name: str, key: str, expires_in: int = 600) -> str:
        client = self.get_s3_presigner()
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=expires_in,
            HttpMethod="GET",
        )

    def put_object_v4(self, bucket_name: str, key: str, content: str) -> HttpResult:
        endpoint = self.create_url(bucket_name, key)
        content_hash = aws4.hash_text(content)
        content_hash_string = content_hash.hex()
        headers = {
            "x-amz-content-sha256": content_hash_string,
            "x-amz-decoded-content-length": str(len(content)),
        }
        from s3tests.auth.aws4_signer_auth_header import AWS4SignerForAuthorizationHeader

        signer = AWS4SignerForAuthorizationHeader(endpoint, "PUT", "s3", self.config.region_name)
        headers["Authorization"] = signer.compute_signature(
            headers,
            None,
            content_hash_string,
            self.config.main_user.access_key,
            self.config.main_user.secret_key,
        )
        return net_utils.put_upload(endpoint, "PUT", headers, content)

    def put_object_chunked_v4(self, bucket_name: str, key: str, content: str) -> HttpResult:
        from s3tests.auth.aws4_signer_chunked import AWS4SignerForChunkedUpload

        endpoint = self.create_url(bucket_name, key)
        headers = {
            "x-amz-content-sha256": AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256,
            "content-encoding": "aws-chunked",
            "x-amz-decoded-content-length": str(len(content)),
        }
        signer = AWS4SignerForChunkedUpload(endpoint, "PUT", "s3", self.config.region_name)
        total_length = AWS4SignerForChunkedUpload.calculate_chunked_content_length(
            len(content), net_utils.USER_DATA_BLOCK_SIZE
        )
        headers["content-length"] = str(total_length)
        headers["Authorization"] = signer.compute_signature(
            headers,
            None,
            AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256,
            self.config.main_user.access_key,
            self.config.main_user.secret_key,
        )
        return net_utils.put_upload_chunked(endpoint, "PUT", headers, signer, content)

    def get_object_v4(self, bucket_name: str, key: str, content: str) -> HttpResult:
        from s3tests.auth.aws4_signer_chunked import AWS4SignerForChunkedUpload

        endpoint = self.create_url(bucket_name, key)
        headers = {"x-amz-content-sha256": aws4.EMPTY_BODY_SHA256}
        signer = AWS4SignerForChunkedUpload(endpoint, "GET", "s3", self.config.region_name)
        headers["Authorization"] = signer.compute_signature(
            headers,
            None,
            aws4.EMPTY_BODY_SHA256,
            self.config.main_user.access_key,
            self.config.main_user.secret_key,
        )
        return net_utils.put_upload(endpoint, "GET", headers, None)

    @staticmethod
    def backend_put_object(
        client: Any,
        source_bucket_name: str,
        source_key: str,
        target_bucket_name: str,
        target_key: str,
        version_id: str,
    ) -> None:
        response = client.get_object(Bucket=source_bucket_name, Key=source_key, VersionId=version_id)
        body = response["Body"].read()
        metadata = response.get("Metadata", {})
        extra_args: Dict[str, Any] = {
            "Metadata": metadata,
            "ContentType": response.get("ContentType"),
        }
        client.put_object(
            Bucket=target_bucket_name,
            Key=target_key,
            Body=body,
            **extra_args,
            **{
                "Metadata": metadata,
            },
        )
        extra_headers = {
            bh.IFS_VERSION_ID: version_id,
            bh.KSAN_VERSION_ID: version_id,
        }
        client.meta.events.register(
            "before-sign.s3.PutObject",
            lambda request, **kwargs: [request.headers.__setitem__(k, v) for k, v in extra_headers.items()],
        )

    @staticmethod
    def backend_copy_object(
        client: Any,
        source_bucket_name: str,
        source_key: str,
        target_bucket_name: str,
        target_key: str,
        source_version_id: str,
        target_version_id: str,
    ) -> None:
        def _inject(request, **kwargs):
            request.headers[bh.IFS_VERSION_ID] = target_version_id
            request.headers[bh.KSAN_VERSION_ID] = target_version_id

        client.meta.events.register("before-sign.s3.CopyObject", _inject)
        client.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": source_key, "VersionId": source_version_id},
            Bucket=target_bucket_name,
            Key=target_key,
        )

    @staticmethod
    def backend_multipart_upload(
        client: Any,
        source_bucket_name: str,
        source_key: str,
        target_bucket_name: str,
        target_key: str,
        version_id: str,
    ) -> None:
        metadata = client.head_object(Bucket=source_bucket_name, Key=source_key, VersionId=version_id)
        init = client.create_multipart_upload(
            Bucket=target_bucket_name,
            Key=target_key,
            Metadata=metadata.get("Metadata", {}),
            ContentType=metadata.get("ContentType"),
        )
        upload_id = init["UploadId"]
        part_size = 5 * md.MB
        size = metadata["ContentLength"]
        parts: List[Dict[str, Any]] = []
        index = 0
        part_number = 1
        while index < size:
            start = index
            end = min(start + part_size, size) - 1
            obj = client.get_object(
                Bucket=source_bucket_name,
                Key=source_key,
                VersionId=version_id,
                Range=f"bytes={start}-{end}",
            )
            part = client.upload_part(
                Bucket=target_bucket_name,
                Key=target_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=obj["Body"].read(),
            )
            parts.append({"ETag": part["ETag"], "PartNumber": part_number})
            part_number += 1
            index = end + 1
        client.complete_multipart_upload(
            Bucket=target_bucket_name,
            Key=target_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    @staticmethod
    def backend_put_object_acl(
        client: Any,
        source_bucket_name: str,
        source_key: str,
        target_bucket_name: str,
        target_key: str,
        version_id: str,
    ) -> None:
        acl = client.get_object_acl(Bucket=source_bucket_name, Key=source_key, VersionId=version_id)
        client.put_object_acl(
            Bucket=target_bucket_name,
            Key=target_key,
            VersionId=version_id,
            AccessControlPolicy=acl,
        )

    @staticmethod
    def backend_put_object_tagging(
        client: Any,
        source_bucket_name: str,
        source_key: str,
        target_bucket_name: str,
        target_key: str,
        version_id: str,
    ) -> None:
        tagging = client.get_object_tagging(Bucket=source_bucket_name, Key=source_key, VersionId=version_id)
        client.put_object_tagging(
            Bucket=target_bucket_name,
            Key=target_key,
            VersionId=version_id,
            Tagging={"TagSet": tagging.get("TagSet", [])},
        )

    @staticmethod
    def backend_delete_object(client: Any, bucket_name: str, key: str, version_id: str) -> None:
        def _inject(request, **kwargs):
            request.headers[bh.DELETE_MARKER_VERSION_ID] = version_id
            request.headers[bh.KSAN_DELETE_MARKER_VERSION_ID] = version_id

        client.meta.events.register("before-sign.s3.PutObject", _inject)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"")

    @staticmethod
    def backend_delete_object_tagging(client: Any, bucket_name: str, key: str, version_id: str) -> None:
        client.delete_object_tagging(Bucket=bucket_name, Key=key, VersionId=version_id)

    def collect_select_object_content(self, client: Any, **params: Any) -> str:
        response = client.select_object_content(**params)
        chunks: List[str] = []
        for event in response["Payload"]:
            if "Records" in event:
                chunks.append(event["Records"]["Payload"].decode("utf-8"))
        return "".join(chunks)

    def skip_if_aws(self) -> None:
        if self.config.is_aws():
            pytest.skip("Test not supported on AWS")

    def create_bucket_object_lock(self, client: Any) -> str:
        bucket_name = self.get_new_bucket_name()
        self._do_create_bucket(client, bucket_name, ObjectLockEnabledForBucket=True)
        if self.config.is_old_system():
            self._do_create_bucket(self.get_old_client(), bucket_name)
        return bucket_name

    def unblock_sse_c(self, bucket_name: str) -> None:
        """Match Java unblockSseC: set BlockedEncryptionTypes=NONE so SSE-C is allowed.

        botocore < ~1.40 rejects BlockedEncryptionTypes in the model, so we fall back to a
        signed PutBucketEncryption REST call (same XML Java/AWS CLI send).
        """
        if not self.config.is_aws():
            return
        client = self.get_client()
        sse_config = {
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    "BlockedEncryptionTypes": {"EncryptionType": ["NONE"]},
                }
            ]
        }
        try:
            client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration=sse_config,
            )
        except ParamValidationError:
            self._put_bucket_encryption_raw(bucket_name, block_encryption_type="NONE")

        for _ in range(5):
            try:
                if self._sse_c_is_unblocked(client, bucket_name):
                    return
            except Exception:
                pass
            self.delay(1000)
        pytest.fail(f"SSE-C unblock failed: {bucket_name}")

    def _put_bucket_encryption_raw(self, bucket_name: str, block_encryption_type: str) -> None:
        """PutBucketEncryption via signed REST — works when botocore model lacks the field."""
        import base64
        import hashlib

        from s3tests.auth.aws4_signer_auth_header import AWS4SignerForAuthorizationHeader

        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<Rule>"
            "<ApplyServerSideEncryptionByDefault>"
            "<SSEAlgorithm>AES256</SSEAlgorithm>"
            "</ApplyServerSideEncryptionByDefault>"
            "<BlockedEncryptionTypes>"
            f"<EncryptionType>{block_encryption_type}</EncryptionType>"
            "</BlockedEncryptionTypes>"
            "</Rule>"
            "</ServerSideEncryptionConfiguration>"
        )
        body_bytes = body.encode("utf-8")
        endpoint = f"{self._endpoint_url(self.config.is_secure)}/{bucket_name}?encryption"
        content_hash = hashlib.sha256(body_bytes).hexdigest()
        content_md5 = base64.b64encode(hashlib.md5(body_bytes).digest()).decode("ascii")
        headers = {
            "Content-Type": "application/xml",
            "Content-MD5": content_md5,
            "x-amz-content-sha256": content_hash,
            "Content-Length": str(len(body_bytes)),
        }
        signer = AWS4SignerForAuthorizationHeader(
            endpoint, "PUT", "s3", self.config.region_name or "us-east-1"
        )
        headers["Authorization"] = signer.compute_signature(
            headers,
            {"encryption": ""},
            content_hash,
            self.config.main_user.access_key,
            self.config.main_user.secret_key,
        )
        result = net_utils.put_upload(endpoint, "PUT", headers, body)
        if result.status_code not in (200, 204):
            pytest.fail(
                f"SSE-C unblock REST failed: {bucket_name} "
                f"status={result.status_code} body={result.message}"
            )

    @staticmethod
    def _sse_c_is_unblocked(client: Any, bucket_name: str) -> bool:
        response = client.get_bucket_encryption(Bucket=bucket_name)
        for rule in response["ServerSideEncryptionConfiguration"]["Rules"]:
            blocked_types = rule.get("BlockedEncryptionTypes") or {}
            enc_types = blocked_types.get("EncryptionType") or []
            if isinstance(blocked_types, list):
                enc_types = blocked_types
            if isinstance(enc_types, str):
                enc_types = [enc_types]
            if "SSE-C" in enc_types:
                return False
        return True

    @staticmethod
    def reverse_versions(versions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(reversed(versions))

    @staticmethod
    def norm_version_id(version_id: Optional[str]) -> str:
        return "null" if version_id is None else version_id

    def check_grants(self, expected: List[Dict[str, Any]], actual: List[Dict[str, Any]]) -> None:
        expected_grants = sorted(expected, key=lambda g: f"{g['Grantee'].get('ID', '')}{g['Permission']}")
        actual_grants = sorted(actual, key=lambda g: f"{g['Grantee'].get('ID', '')}{g['Permission']}")
        assert len(expected_grants) == len(actual_grants)
        for left, right in zip(expected_grants, actual_grants):
            assert left["Permission"] == right["Permission"]
            assert left["Grantee"]["ID"] == right["Grantee"]["ID"]
            assert left["Grantee"]["Type"] == right["Grantee"]["Type"]

    def lock_compare(self, expected: Dict[str, Any], actual: Dict[str, Any]) -> None:
        assert expected["ObjectLockEnabled"] == actual["ObjectLockEnabled"]
        exp_rule = expected["Rule"]["DefaultRetention"]
        act_rule = actual["Rule"]["DefaultRetention"]
        assert exp_rule["Mode"] == act_rule["Mode"]
        assert exp_rule.get("Years") == act_rule.get("Years")
        assert exp_rule.get("Days") == act_rule.get("Days")

    def retention_compare(self, expected: Dict[str, Any], actual: Dict[str, Any]) -> None:
        assert expected["Mode"] == actual["Mode"]
        assert expected["RetainUntilDate"] == actual["RetainUntilDate"]

    def create_multiple_versions_track(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        num_versions: int,
        version_ids: List[str],
        contents: List[str],
        check_version: bool = True,
    ) -> None:
        for index in range(num_versions):
            body = f"content-{index}"
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body.encode("utf-8"))
            version_ids.append(response["VersionId"])
            contents.append(body)
        if check_version:
            self.check_obj_versions(client, bucket_name, key, version_ids, contents)

    def remove_obj_version(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        version_ids: List[str],
        contents: List[str],
        index: int,
    ) -> None:
        assert len(version_ids) == len(contents)
        rm_version_id = version_ids.pop(index)
        rm_content = contents.pop(index)
        self.check_obj_content(client, bucket_name, key, rm_version_id, rm_content)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=rm_version_id)
        if version_ids:
            self.check_obj_versions(client, bucket_name, key, version_ids, contents)

    def do_test_create_remove_versions(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        num_versions: int,
        remove_start_idx: int,
        idx_inc: int,
    ) -> None:
        version_ids: List[str] = []
        contents: List[str] = []
        self.create_multiple_versions_track(client, bucket_name, key, num_versions, version_ids, contents, True)
        idx = remove_start_idx
        for _ in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)
            idx += idx_inc

    def delete_suspended_versioning_obj(
        self, client: Any, bucket_name: str, key: str, version_ids: List[str], contents: List[str]
    ) -> None:
        client.delete_object(Bucket=bucket_name, Key=key)
        assert len(version_ids) == len(contents)
        for i in range(len(version_ids) - 1, -1, -1):
            if version_ids[i] == "null":
                version_ids.pop(i)
                contents.pop(i)

    def overwrite_suspended_versioning_obj(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        version_ids: List[str],
        contents: List[str],
        content: str,
    ) -> None:
        client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        assert len(version_ids) == len(contents)
        for i in range(len(version_ids) - 1, -1, -1):
            if version_ids[i] == "null":
                version_ids.pop(i)
                contents.pop(i)
        contents.append(content)
        version_ids.append("null")

    def do_create_versioned_obj_concurrent(
        self, client: Any, bucket_name: str, key: str, count: int
    ) -> List[threading.Thread]:
        threads: List[threading.Thread] = []

        def _put(item: str) -> None:
            client.put_object(Bucket=bucket_name, Key=key + item, Body=f"data {item}".encode("utf-8"))

        for i in range(count):
            thread = threading.Thread(target=_put, args=(str(i),))
            thread.start()
            threads.append(thread)
        return threads

    def do_clear_versioned_bucket_concurrent(self, client: Any, bucket_name: str) -> List[threading.Thread]:
        threads: List[threading.Thread] = []
        response = client.list_object_versions(Bucket=bucket_name)
        for version in response.get("Versions", []):

            def _delete(v: Dict[str, Any] = version) -> None:
                client.delete_object(Bucket=bucket_name, Key=v["Key"], VersionId=v["VersionId"])

            thread = threading.Thread(target=_delete)
            thread.start()
            threads.append(thread)
        return threads

    @staticmethod
    def parts_etag_compare(expected: List[Dict[str, Any]], actual: List[Dict[str, Any]]) -> None:
        assert len(expected) == len(actual)
        for left, right in zip(expected, actual):
            assert left["PartNumber"] == right["PartNumber"]
            assert left["ETag"].replace('"', "") == right["ETag"].replace('"', "")

    def setup_sse_multipart_upload(
        self, client: Any, bucket_name: str, key: str, size: int, metadata: Optional[Dict[str, str]] = None
    ) -> MultipartUploadV2Data:
        params: Dict[str, Any] = {"Bucket": bucket_name, "Key": key, "ServerSideEncryption": "AES256"}
        if metadata:
            params["Metadata"] = metadata
        upload_data = MultipartUploadV2Data()
        create_response = client.create_multipart_upload(**params)
        upload_data.upload_id = create_response["UploadId"]
        return self.multipart_upload(client, bucket_name, key, size, DEFAULT_PART_SIZE, upload_data)

    def setup_sse_c_multipart_upload(
        self, client: Any, bucket_name: str, key: str, size: int, metadata: Optional[Dict[str, str]] = None
    ) -> MultipartUploadV2Data:
        upload_data = MultipartUploadV2Data(part_size=5 * md.MB)
        params: Dict[str, Any] = {"Bucket": bucket_name, "Key": key, **self.sse_c_extra_args()}
        if metadata:
            params["Metadata"] = metadata
        create_response = client.create_multipart_upload(**params)
        upload_data.upload_id = create_response["UploadId"]
        parts = utils.generate_random_string(size, 5 * md.MB)
        for part in parts:
            upload_data.append_body(part)
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                PartNumber=upload_data.next_part_number(),
                Body=part.encode("utf-8"),
                **self.sse_c_extra_args(),
            )
            upload_data.add_part(part_response["ETag"])
        return upload_data

    def multipart_copy(
        self,
        client: Any,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
        size: int,
        metadata: Optional[Dict[str, str]] = None,
        version_id: Optional[str] = None,
        part_size: int = 0,
    ) -> MultipartUploadV2Data:
        if part_size <= 0:
            part_size = 5 * md.MB
        data = MultipartUploadV2Data()
        params: Dict[str, Any] = {"Bucket": target_bucket, "Key": target_key}
        if metadata:
            params["Metadata"] = metadata
        response = client.create_multipart_upload(**params)
        data.upload_id = response["UploadId"]
        count = 1
        index = 0
        while index < size:
            start = index
            end = min(index + part_size, size) - 1
            copy_source: Any = {"Bucket": source_bucket, "Key": source_key}
            if version_id:
                copy_source["VersionId"] = version_id
            part_response = client.upload_part_copy(
                CopySource=copy_source,
                Bucket=target_bucket,
                Key=target_key,
                UploadId=data.upload_id,
                PartNumber=count,
                CopySourceRange=f"bytes={start}-{end}",
            )
            data.add_part(part_response["CopyPartResult"]["ETag"], count)
            count += 1
            index = end + 1
        return data

    def multipart_copy_sse_c(
        self, client: Any, source_bucket: str, source_key: str, target_bucket: str, target_key: str, size: int
    ) -> MultipartUploadV2Data:
        data = MultipartUploadV2Data()
        part_size = 5 * md.MB
        response = client.create_multipart_upload(Bucket=target_bucket, Key=target_key, **self.sse_c_extra_args())
        data.upload_id = response["UploadId"]
        count = 1
        index = 0
        while index < size:
            start = index
            end = min(start + part_size, size) - 1
            part_response = client.upload_part_copy(
                CopySource={"Bucket": source_bucket, "Key": source_key},
                Bucket=target_bucket,
                Key=target_key,
                UploadId=data.upload_id,
                PartNumber=count,
                CopySourceRange=f"bytes={start}-{end}",
                CopySourceSSECustomerAlgorithm=SSE_CUSTOMER_ALGORITHM,
                CopySourceSSECustomerKey=SSE_KEY,
                CopySourceSSECustomerKeyMD5=SSE_KEY_MD5,
                **self.sse_c_extra_args(),
            )
            data.add_part(part_response["CopyPartResult"]["ETag"], count)
            count += 1
            index = end + 1
        return data

    def multipart_upload_resend(
        self,
        client: Any,
        bucket_name: str,
        key: str,
        size: int,
        metadata: Optional[Dict[str, str]],
        resend_parts: List[int],
    ) -> MultipartUploadV2Data:
        upload_data = MultipartUploadV2Data()
        params: Dict[str, Any] = {"Bucket": bucket_name, "Key": key, "ContentType": "text/plain"}
        if metadata:
            params["Metadata"] = metadata
        create_response = client.create_multipart_upload(**params)
        upload_data.upload_id = create_response["UploadId"]
        parts = utils.generate_random_string(size, DEFAULT_PART_SIZE)
        for part in parts:
            upload_data.append_body(part)
            part_number = upload_data.next_part_number()
            part_response = client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_data.upload_id,
                PartNumber=part_number,
                Body=part.encode("utf-8"),
            )
            upload_data.add_part(part_response["ETag"], part_number)
            if part_number in resend_parts:
                client.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_data.upload_id,
                    PartNumber=part_number,
                    Body=part.encode("utf-8"),
                )
        return upload_data

    def check_upload_multipart_resend(self, bucket_name: str, key: str, size: int, resend_parts: List[int]) -> None:
        metadata = {"foo": "bar"}
        client = self.get_client()
        upload_data = self.multipart_upload_resend(client, bucket_name, key, size, metadata, resend_parts)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        response = client.head_object(Bucket=bucket_name, Key=key)
        assert response["Metadata"] == metadata
        body = upload_data.get_body()
        self.check_content_using_range(bucket_name, key, body, md.MB)
        self.check_content_using_range(bucket_name, key, body, 10 * md.MB)

    def do_test_multipart_upload_contents(self, bucket_name: str, key: str, num_parts: int) -> str:
        payload = utils.random_text_to_long(5 * md.MB)
        client = self.get_client()
        create_response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = create_response["UploadId"]
        parts: List[Dict[str, Any]] = []
        all_payload = ""
        for i in range(num_parts):
            part_number = i + 1
            part_response = client.upload_part(
                Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=part_number, Body=payload.encode("utf-8")
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})
            all_payload += payload
        last_payload = utils.random_text_to_long(md.MB)
        last_part_response = client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=num_parts + 1,
            Body=last_payload.encode("utf-8"),
        )
        parts.append({"PartNumber": num_parts + 1, "ETag": last_part_response["ETag"]})
        all_payload += last_payload
        client.complete_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        response = client.get_object(Bucket=bucket_name, Key=key)
        assert self.get_body(response) == all_payload, md.NOT_MATCHED
        return all_payload

    def check_copy_content(
        self, source_bucket: str, source_key: str, target_bucket: str, target_key: str, version_id: Optional[str] = None
    ) -> None:
        client = self.get_client()
        source_params: Dict[str, Any] = {"Bucket": source_bucket, "Key": source_key}
        if version_id:
            source_params["VersionId"] = version_id
        source_response = client.get_object(**source_params)
        target_response = client.get_object(Bucket=target_bucket, Key=target_key)
        assert source_response["ContentLength"] == target_response["ContentLength"]
        assert self.get_body(source_response) == self.get_body(target_response), md.NOT_MATCHED

    def check_copy_content_sse_c(
        self, client: Any, source_bucket: str, source_key: str, target_bucket: str, target_key: str
    ) -> None:
        # Include KeyMD5 so botocore does not re-base64 the already-encoded SSE_KEY.
        sse_args = self.sse_c_extra_args()
        source_response = client.get_object(Bucket=source_bucket, Key=source_key, **sse_args)
        target_response = client.get_object(Bucket=target_bucket, Key=target_key, **sse_args)
        assert source_response["ContentLength"] == target_response["ContentLength"]
        assert self.get_body(source_response) == self.get_body(target_response), md.NOT_MATCHED

    def check_copy_content_using_range(
        self,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
        step: Optional[int] = None,
    ) -> None:
        client = self.get_client()
        target_response = client.get_object(Bucket=target_bucket, Key=target_key)
        target_data = self.get_body(target_response)
        target_size = target_response["ContentLength"]
        if step is None:
            source_response = client.get_object(
                Bucket=source_bucket, Key=source_key, Range=f"bytes=0-{target_size - 1}"
            )
            assert source_response["ContentLength"] == target_size
            assert self.get_body(source_response) == target_data, md.NOT_MATCHED
            return
        size = client.head_object(Bucket=source_bucket, Key=source_key)["ContentLength"]
        index = 0
        while index < size:
            start = index
            end = min(start + step, size) - 1
            source_response = client.get_object(
                Bucket=source_bucket, Key=source_key, Range=f"bytes={start}-{end}"
            )
            target_response = client.get_object(
                Bucket=target_bucket, Key=target_key, Range=f"bytes={start}-{end}"
            )
            assert source_response["ContentLength"] == target_response["ContentLength"]
            assert self.get_body(source_response) == self.get_body(target_response), md.NOT_MATCHED
            index += step

    def check_content_enc(self, bucket_name: str, key: str, data: str, loop_count: int) -> None:
        client = self.get_client_https(False)
        sse_args = self.sse_c_extra_args()
        for _ in range(loop_count):
            response = client.get_object(Bucket=bucket_name, Key=key, **sse_args)
            assert self.get_body(response) == data, md.NOT_MATCHED

    def check_content_using_range_enc(self, client: Any, bucket_name: str, key: str, data: str, step: int) -> None:
        sse_args = self.sse_c_extra_args()
        size = client.head_object(Bucket=bucket_name, Key=key, **sse_args)["ContentLength"]
        index = 0
        while index < size:
            start = index
            end = min(index + step, size - 1)
            response = client.get_object(
                Bucket=bucket_name, Key=key, Range=f"bytes={start}-{end - 1}", **sse_args
            )
            assert self.get_body(response) == data[start:end], md.NOT_MATCHED
            index += step

    def check_content_using_random_range_enc(
        self, client: Any, bucket_name: str, key: str, data: str, file_size: int, loop_count: int
    ) -> None:
        sse_args = self.sse_c_extra_args()
        for _ in range(loop_count):
            start, length = self._get_random_range(file_size)
            end = start + length
            response = client.get_object(
                Bucket=bucket_name, Key=key, Range=f"bytes={start}-{end - 1}", **sse_args
            )
            assert self.get_body(response) == data[start:end], md.NOT_MATCHED

    def encryption_sse_customer_write(self, file_size: int) -> None:
        client = self.get_client_https(False)
        bucket_name = self.create_bucket(client)
        self.unblock_sse_c(bucket_name)
        key = "test"
        data = utils.random_text_to_long(file_size)
        client.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"), **self.sse_c_extra_args())
        response = client.get_object(Bucket=bucket_name, Key=key, **self.sse_c_extra_args())
        assert self.get_body(response) == data, md.NOT_MATCHED
        assert response["SSECustomerKeyMD5"] == SSE_KEY_MD5

    def encryption_sse_s3_write(self, file_size: int) -> None:
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        data = utils.random_text_to_long(file_size)
        client.put_object(Bucket=bucket_name, Key="test", Body=data.encode("utf-8"), ServerSideEncryption="AES256")
        response = client.get_object(Bucket=bucket_name, Key="test")
        assert self.get_body(response) == data, md.NOT_MATCHED
        assert response.get("ServerSideEncryption") == "AES256"

    def encryption_sse_s3_copy(self, file_size: int) -> None:
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": False}]
            },
        )
        source_key = "bar"
        client.put_object(Bucket=bucket_name, Key=source_key, Body=utils.random_text_to_long(file_size).encode("utf-8"))
        source_response = client.get_object(Bucket=bucket_name, Key=source_key)
        source_body = self.get_body(source_response)
        assert source_response.get("ServerSideEncryption") == "AES256"
        target_key = "foo"
        client.copy_object(
            Bucket=bucket_name, Key=target_key, CopySource={"Bucket": bucket_name, "Key": source_key}
        )
        target_response = client.get_object(Bucket=bucket_name, Key=target_key)
        assert target_response.get("ServerSideEncryption") == "AES256"
        assert self.get_body(target_response) == source_body, md.NOT_MATCHED

    def object_copy(
        self,
        prefix: str,
        source_object_encryption: bool,
        source_bucket_encryption: bool,
        target_bucket_encryption: bool,
        target_object_encryption: bool,
        file_size: int,
    ) -> None:
        source_key = prefix + "Source"
        target_key = prefix + "Target"
        client = self.get_client()
        source_bucket = self.create_bucket(client)
        target_bucket = self.create_bucket(client)
        data = utils.random_text_to_long(file_size)
        sse_config = {
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": False}]
        }
        if source_bucket_encryption:
            client.put_bucket_encryption(Bucket=source_bucket, ServerSideEncryptionConfiguration=sse_config)
        if target_bucket_encryption:
            client.put_bucket_encryption(Bucket=target_bucket, ServerSideEncryptionConfiguration=sse_config)
        put_params: Dict[str, Any] = {"Bucket": source_bucket, "Key": source_key, "Body": data.encode("utf-8")}
        if source_object_encryption:
            put_params["ServerSideEncryption"] = "AES256"
        client.put_object(**put_params)
        source_response = client.get_object(Bucket=source_bucket, Key=source_key)
        source_body = self.get_body(source_response)
        if source_object_encryption or source_bucket_encryption or self.config.is_aws():
            assert source_response.get("ServerSideEncryption") == "AES256"
        else:
            assert source_response.get("ServerSideEncryption") is None
        copy_params: Dict[str, Any] = {
            "Bucket": target_bucket,
            "Key": target_key,
            "CopySource": {"Bucket": source_bucket, "Key": source_key},
        }
        if target_object_encryption:
            copy_params["ServerSideEncryption"] = "AES256"
        client.copy_object(**copy_params)
        target_response = client.get_object(Bucket=target_bucket, Key=target_key)
        if target_bucket_encryption or target_object_encryption or self.config.is_aws():
            assert target_response.get("ServerSideEncryption") == "AES256"
        else:
            assert target_response.get("ServerSideEncryption") is None
        assert self.get_body(target_response) == source_body, md.NOT_MATCHED

    def object_copy_encryption_type(self, prefix: str, source: EncryptionType, target: EncryptionType, size: int) -> None:
        source_key = prefix + "Source"
        target_key = prefix + "Target"
        client = self.get_client_https(True)
        bucket_name = self.create_bucket(client)
        if source == EncryptionType.SSE_C or target == EncryptionType.SSE_C:
            self.unblock_sse_c(bucket_name)
        data = utils.random_text_to_long(size)
        put_params: Dict[str, Any] = {"Bucket": bucket_name, "Key": source_key, "Body": data.encode("utf-8")}
        get_source: Dict[str, Any] = {"Bucket": bucket_name, "Key": source_key}
        get_target: Dict[str, Any] = {"Bucket": bucket_name, "Key": target_key}
        copy_params: Dict[str, Any] = {
            "Bucket": bucket_name,
            "Key": target_key,
            "CopySource": {"Bucket": bucket_name, "Key": source_key},
            "MetadataDirective": "REPLACE",
        }
        if source == EncryptionType.SSE_S3:
            put_params["ServerSideEncryption"] = "AES256"
        elif source == EncryptionType.SSE_C:
            put_params.update(self.sse_c_extra_args())
            # KeyMD5 must be set; otherwise botocore base64-encodes SSE_KEY again.
            get_source.update(self.sse_c_extra_args())
            copy_params.update(
                {
                    "CopySourceSSECustomerAlgorithm": SSE_CUSTOMER_ALGORITHM,
                    "CopySourceSSECustomerKey": SSE_KEY,
                    "CopySourceSSECustomerKeyMD5": SSE_KEY_MD5,
                }
            )
        if target == EncryptionType.SSE_S3:
            copy_params["ServerSideEncryption"] = "AES256"
        elif target == EncryptionType.SSE_C:
            copy_params.update(self.sse_c_extra_args())
            get_target.update(self.sse_c_extra_args())
        elif target == EncryptionType.NORMAL:
            copy_params["Metadata"] = {}
        client.put_object(**put_params)
        client.put_object(Bucket=bucket_name, Key="temp", Body=data.encode("utf-8"))
        assert self.get_body(client.get_object(**get_source)) == data, md.NOT_MATCHED
        client.copy_object(**copy_params)
        assert self.get_body(client.get_object(**get_target)) == data, md.NOT_MATCHED

    def get_client_with_checksum(self, use_chunk_encoding: bool, request_option: str, response_option: str) -> Any:
        endpoint_url = self._endpoint_url(self.config.is_secure)
        config = Config(
            signature_version=self.config.get_boto_signature_version(),
            s3={"addressing_style": "path", "payload_signing_enabled": use_chunk_encoding},
            retries={"max_attempts": 1},
            connect_timeout=300,
            read_timeout=300,
            request_checksum_calculation=request_option,
            response_checksum_validation=response_option,
        )
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=self.config.region_name or "ap-northeast-2",
            aws_access_key_id=self.config.main_user.access_key,
            aws_secret_access_key=self.config.main_user.secret_key,
            config=config,
            verify=False,
        )

    def get_async_client(self, use_chunk_encoding: bool, request_option: str, response_option: str) -> "AsyncS3Client":
        return AsyncS3Client(self.get_client_with_checksum(use_chunk_encoding, request_option, response_option))

    def multipart_upload_checksum(
        self, client: Any, bucket_name: str, key: str, checksum_type: str, checksum_algorithm: str
    ) -> None:
        size = 10 * md.MB
        part_size = 5 * md.MB
        upload_data = MultipartUploadV2Data()
        create_response = client.create_multipart_upload(
            Bucket=bucket_name, Key=key, ChecksumType=checksum_type, ChecksumAlgorithm=checksum_algorithm
        )
        upload_data.upload_id = create_response["UploadId"]
        for part in utils.generate_random_string(size, part_size):
            upload_data.append_body(part)
            part_params = checksum.apply_put_checksum_params(
                {
                    "Bucket": bucket_name,
                    "Key": key,
                    "UploadId": upload_data.upload_id,
                    "PartNumber": upload_data.next_part_number(),
                    "Body": part.encode("utf-8"),
                },
                checksum_algorithm,
                part,
            )
            part_response = client.upload_part(**part_params)
            checksum.checksum_compare_part(checksum_algorithm, part, part_response)
            upload_data.add_part_with_checksum(checksum_algorithm, part_response)
        complete_response = client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            ChecksumType=checksum_type,
            MultipartUpload={"Parts": upload_data.parts},
        )
        assert complete_response.get("ChecksumType") == checksum_type
        checksum.checksum_compare_multipart(checksum_algorithm, upload_data, complete_response)

    def get_object_list(self, client: Any, bucket_name: str, prefix: Optional[str]) -> List[str]:
        params: Dict[str, Any] = {"Bucket": bucket_name}
        if prefix is not None:
            params["Prefix"] = prefix
        response = client.list_objects(**params)
        return self.get_keys(response.get("Contents"))

    def check_bad_bucket_name(self, bucket_name: str) -> None:
        with pytest.raises((ParamValidationError, ClientError, ValueError)):
            self.get_client().create_bucket(Bucket=bucket_name)

    def check_good_bucket_name(self, name: str, prefix: Optional[str]) -> None:
        if not prefix:
            prefix = self.get_prefix()
        bucket_name = f"{prefix}{name}"
        self._buckets.append(bucket_name)
        self._do_create_bucket(self.get_client(), bucket_name)

    def bucket_create_naming_good_long(self, length: int) -> None:
        bucket_name = self.get_new_bucket_name_only(length)
        self._buckets.append(bucket_name)
        self._do_create_bucket(self.get_client(), bucket_name)

    def bucket_create_naming_bad_long(self, length: int) -> None:
        bucket_name = self.get_new_bucket_name_only(length)
        self.check_bad_bucket_name(bucket_name)

    def complete_multipart_upload_data(
        self, client: Any, bucket_name: str, key: str, size: int, part_size: int
    ) -> MultipartUploadV2Data:
        upload_data = self.setup_multipart_upload(client, bucket_name, key, size, part_size)
        client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_data.upload_id,
            MultipartUpload=upload_data.completed_multipart_upload(),
        )
        return upload_data

    def delete_object_with_headers(
        self, client: Any, extra_headers: Dict[str, str], **params: Any
    ) -> Dict[str, Any]:
        def inject(request, **kwargs):
            for header, value in extra_headers.items():
                request.headers[header] = value

        client.meta.events.register("before-sign.s3.DeleteObject", inject)
        try:
            return client.delete_object(**params)
        finally:
            client.meta.events.unregister("before-sign.s3.DeleteObject", inject)

    def delete_objects_with_headers(
        self, client: Any, extra_headers: Dict[str, str], **params: Any
    ) -> Dict[str, Any]:
        def inject(request, **kwargs):
            for header, value in extra_headers.items():
                request.headers[header] = value

        client.meta.events.register("before-sign.s3.DeleteObjects", inject)
        try:
            return client.delete_objects(**params)
        finally:
            client.meta.events.unregister("before-sign.s3.DeleteObjects", inject)

    # endregion


class AsyncS3Client:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._executor = ThreadPoolExecutor(max_workers=4)

    def _run(self, func: Callable[..., Any], **kwargs: Any) -> "AsyncResult":
        return AsyncResult(self._executor.submit(func, **kwargs))

    def create_multipart_upload(self, **kwargs: Any) -> "AsyncResult":
        return self._run(self._client.create_multipart_upload, **kwargs)

    def upload_part(self, **kwargs: Any) -> "AsyncResult":
        return self._run(self._client.upload_part, **kwargs)

    def complete_multipart_upload(self, **kwargs: Any) -> "AsyncResult":
        return self._run(self._client.complete_multipart_upload, **kwargs)

    def put_object(self, **kwargs: Any) -> "AsyncResult":
        return self._run(self._client.put_object, **kwargs)

    def copy_object(self, **kwargs: Any) -> "AsyncResult":
        return self._run(self._client.copy_object, **kwargs)


class AsyncResult:
    def __init__(self, future: Future[Any]) -> None:
        self._future = future

    def result(self) -> Any:
        return self._future.result()

    def join(self) -> Any:
        return self.result()
