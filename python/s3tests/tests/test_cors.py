"""Cors tests ported from Java testV2/Cors.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestCors(S3TestBase):
    @pytest.mark.skip(reason="Java s3tests wrapper commented out")
    @pytest.mark.tag("Check")
    def test_set_cors(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        allowed_methods = ["GET", "PUT"]
        allowed_origins = ["*.get", "*.put"]
        cors_config = {
            "CORSRules": [
                {
                    "AllowedMethods": allowed_methods,
                    "AllowedOrigins": allowed_origins,
                }
            ]
        }

        self.assert_client_error(
            lambda: client.get_bucket_cors(Bucket=bucket_name),
            404,
            md.NO_SUCH_CORS_CONFIGURATION,
        )

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        response = client.get_bucket_cors(Bucket=bucket_name)
        assert response["CORSRules"][0]["AllowedMethods"] == allowed_methods
        assert response["CORSRules"][0]["AllowedOrigins"] == allowed_origins

        client.delete_bucket_cors(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.get_bucket_cors(Bucket=bucket_name),
            404,
            md.NO_SUCH_CORS_CONFIGURATION,
        )
