"""Website tests ported from Java testV2/Website.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestWebsite(S3TestBase):
    @pytest.mark.tag("Check")
    def test_website_get_buckets(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        with pytest.raises(ClientError) as exc_info:
            client.get_bucket_website(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_WEBSITE_CONFIGURATION

    @pytest.mark.tag("Check")
    def test_website_put_buckets(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        web_config = {
            "ErrorDocument": {"Key": "HttpStatus.SC_BAD_REQUEST"},
            "IndexDocument": {"Suffix": "a"},
        }
        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config)
        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["ErrorDocument"] == web_config["ErrorDocument"]
        assert response["IndexDocument"] == web_config["IndexDocument"]

    @pytest.mark.tag("Delete")
    def test_website_delete_buckets(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        web_config = {
            "ErrorDocument": {"Key": "HttpStatus.SC_BAD_REQUEST"},
            "IndexDocument": {"Suffix": "a"},
        }
        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config)
        client.delete_bucket_website(Bucket=bucket_name)
