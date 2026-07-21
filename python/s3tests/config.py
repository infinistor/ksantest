"""INI configuration loader ported from Java S3Config."""

from __future__ import annotations

import configparser
import os
from pathlib import Path

from s3tests.data.main_data import S3TESTS_INI
from s3tests.data.user_data import UserData

STR_FILENAME = "config.ini"
STR_SIGNATURE_VERSION_V2 = "S3SignerType"
STR_SIGNATURE_VERSION_V4 = "AWSS3V4SignerType"


def resolve_config_path(file_name: str | None = None) -> str:
    if file_name:
        return file_name
    env_path = os.environ.get(S3TESTS_INI)
    if env_path:
        return env_path
    prop_path = os.environ.get("S3TESTS_INI_PROP")
    if prop_path:
        return prop_path
    return STR_FILENAME


class S3Config:
    def __init__(self, file_name: str | None = None) -> None:
        self.file_name = resolve_config_path(file_name)
        self._ini = configparser.ConfigParser()
        self.url: str = ""
        self.port: int = -1
        self.old_port: int = -1
        self.ssl_port: int = -1
        self.region_name: str = ""
        self.signature_version: str = ""
        self.is_secure: bool = False
        self.bucket_prefix: str = ""
        self.not_delete: bool = False
        self.main_user = UserData()
        self.alt_user = UserData()
        self.backend_user = UserData()

    def get_config(self) -> bool:
        path = Path(self.file_name)
        if not path.is_file():
            raise FileNotFoundError(f"Config file not found: {self.file_name}")
        self._ini.read(path, encoding="utf-8")

        self.url = self._read_string("S3", "URL") or ""
        self.port = self._read_int("S3", "Port")
        self.old_port = self._read_int("S3", "OldPort")
        self.ssl_port = self._read_int("S3", "SSLPort")
        self.region_name = self._read_string("S3", "RegionName") or ""
        self.signature_version = self._read_string("S3", "SignatureVersion") or ""
        self.is_secure = self._read_bool("S3", "IsSecure")
        self.bucket_prefix = self._read_string("Fixtures", "BucketPrefix") or ""
        self.not_delete = self._read_bool("Fixtures", "NotDelete")

        self.main_user = self._read_user("Main User")
        self.alt_user = self._read_user("Alt User")
        self.backend_user = self._read_user("Backend User")
        return True

    def get_signature_version(self) -> str:
        if self.signature_version == "2":
            return STR_SIGNATURE_VERSION_V2
        return STR_SIGNATURE_VERSION_V4

    def get_boto_signature_version(self) -> str:
        return "s3" if self.signature_version == "2" else "s3v4"

    def is_aws(self) -> bool:
        return not self.url or not self.url.strip()

    def is_old_system(self) -> bool:
        return self.old_port > 0

    def _read_user(self, section: str) -> UserData:
        return UserData(
            display_name=self._read_string(section, "DisplayName") or "",
            id=self._read_string(section, "UserID") or "",
            email=self._read_string(section, "Email") or "",
            access_key=self._read_string(section, "AccessKey") or "",
            secret_key=self._read_string(section, "SecretKey") or "",
            kms=self._read_string(section, "KMS") or "",
            x_auth_token=self._read_string(section, "XAuthToken") or "",
        )

    def _read_string(self, section: str, key: str) -> str | None:
        if not self._ini.has_section(section) or not self._ini.has_option(section, key):
            return None
        return self._ini.get(section, key)

    def _read_int(self, section: str, key: str) -> int:
        value = self._read_string(section, key)
        if value is None or not value.strip():
            return -1
        return int(value)

    def _read_bool(self, section: str, key: str) -> bool:
        value = self._read_string(section, key)
        if value is None:
            return False
        return value.strip().lower() in {"1", "true", "yes", "on"}
