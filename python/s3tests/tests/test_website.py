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
        bucket_name = self.create_bucket(client, 1)
        with pytest.raises(ClientError) as exc_info:
            client.get_bucket_website(Bucket=bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_WEBSITE_CONFIGURATION

    @pytest.mark.tag("Check")
    def test_website_put_buckets(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
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
        bucket_name = self.create_bucket(client, 3)
        web_config = {
            "ErrorDocument": {"Key": "HttpStatus.SC_BAD_REQUEST"},
            "IndexDocument": {"Suffix": "a"},
        }
        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config)
        client.delete_bucket_website(Bucket=bucket_name)

    @pytest.mark.tag("Delete")
    def test_website_get_after_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        web_config = {
            "ErrorDocument": {"Key": "error.html"},
            "IndexDocument": {"Suffix": "index.html"},
        }
        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config)
        client.get_bucket_website(Bucket=bucket_name)
        client.delete_bucket_website(Bucket=bucket_name)

        self.assert_client_error(
            lambda: client.get_bucket_website(Bucket=bucket_name), 404, md.NO_SUCH_WEBSITE_CONFIGURATION
        )

    @pytest.mark.tag("Delete")
    def test_website_delete_not_configured(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)

        response = client.delete_bucket_website(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @pytest.mark.tag("Check")
    def test_website_index_document_only(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)

        client.put_bucket_website(
            Bucket=bucket_name, WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}}
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["IndexDocument"] == {"Suffix": "index.html"}
        assert "ErrorDocument" not in response
        assert "RedirectAllRequestsTo" not in response
        assert response.get("RoutingRules", []) == []

    @pytest.mark.tag("Overwrite")
    def test_website_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)

        client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"},
            },
        )
        client.put_bucket_website(
            Bucket=bucket_name, WebsiteConfiguration={"IndexDocument": {"Suffix": "main.html"}}
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["IndexDocument"] == {"Suffix": "main.html"}
        assert "ErrorDocument" not in response

    @pytest.mark.tag("Redirect")
    def test_website_redirect_all_requests(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)

        redirect = {"HostName": "example.com", "Protocol": "https"}
        client.put_bucket_website(
            Bucket=bucket_name, WebsiteConfiguration={"RedirectAllRequestsTo": redirect}
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["RedirectAllRequestsTo"] == redirect
        assert "IndexDocument" not in response
        assert "ErrorDocument" not in response

    @pytest.mark.tag("Redirect")
    def test_website_redirect_all_requests_host_only(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)

        client.put_bucket_website(
            Bucket=bucket_name, WebsiteConfiguration={"RedirectAllRequestsTo": {"HostName": "example.com"}}
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["RedirectAllRequestsTo"] == {"HostName": "example.com"}

    @pytest.mark.tag("RoutingRule")
    def test_website_routing_rules(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)

        rules = [
            {
                "Condition": {"KeyPrefixEquals": "docs/"},
                "Redirect": {"ReplaceKeyPrefixWith": "documents/"},
            },
            {
                "Condition": {"HttpErrorCodeReturnedEquals": "404"},
                "Redirect": {
                    "HostName": "example.com",
                    "Protocol": "https",
                    "HttpRedirectCode": "301",
                    "ReplaceKeyWith": "error.html",
                },
            },
            {"Redirect": {"HostName": "example.com"}},
        ]
        client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"},
                "RoutingRules": rules,
            },
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["IndexDocument"] == {"Suffix": "index.html"}
        assert response["ErrorDocument"] == {"Key": "error.html"}
        assert response["RoutingRules"] == rules

    @pytest.mark.tag("RoutingRule")
    def test_website_routing_rules_max(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)

        rules = self._create_routing_rules(50)
        client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}, "RoutingRules": rules},
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["RoutingRules"] == rules

    @pytest.mark.tag("ERROR")
    def test_website_routing_rules_over_max(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)

        web_config = {
            "IndexDocument": {"Suffix": "index.html"},
            "RoutingRules": self._create_routing_rules(51),
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_website_no_index_document(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)

        web_config = {"ErrorDocument": {"Key": "error.html"}}
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ERROR")
    def test_website_empty_index_suffix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)

        web_config = {"IndexDocument": {"Suffix": ""}}
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ERROR")
    def test_website_index_suffix_with_slash(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)

        web_config = {"IndexDocument": {"Suffix": "dir/index.html"}}
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ERROR")
    def test_website_redirect_all_with_index_document(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)

        web_config = {
            "RedirectAllRequestsTo": {"HostName": "example.com"},
            "IndexDocument": {"Suffix": "index.html"},
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ERROR")
    def test_website_redirect_all_with_routing_rules(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)

        web_config = {
            "RedirectAllRequestsTo": {"HostName": "example.com"},
            "RoutingRules": self._create_routing_rules(1),
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_ARGUMENT)

    @pytest.mark.tag("ERROR")
    def test_website_redirect_all_invalid_protocol(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)

        web_config = {"RedirectAllRequestsTo": {"HostName": "example.com", "Protocol": "ftp"}}
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_website_routing_rule_replace_key_and_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)

        web_config = {
            "IndexDocument": {"Suffix": "index.html"},
            "RoutingRules": [
                {
                    "Condition": {"KeyPrefixEquals": "docs/"},
                    "Redirect": {"ReplaceKeyWith": "index.html", "ReplaceKeyPrefixWith": "documents/"},
                }
            ],
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_website_routing_rule_invalid_redirect_code(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)

        web_config = {
            "IndexDocument": {"Suffix": "index.html"},
            "RoutingRules": [{"Redirect": {"HostName": "example.com", "HttpRedirectCode": "200"}}],
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_REQUEST)

    @pytest.mark.tag("ERROR")
    def test_website_routing_rule_invalid_error_code(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)

        web_config = {
            "IndexDocument": {"Suffix": "index.html"},
            "RoutingRules": [
                {
                    "Condition": {"HttpErrorCodeReturnedEquals": "200"},
                    "Redirect": {"HostName": "example.com"},
                }
            ],
        }
        self._check_put_website_failed(client, bucket_name, web_config, md.INVALID_REQUEST)

    @pytest.mark.tag("RoutingRule")
    def test_website_routing_rule_empty_redirect(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)

        rules = [{"Condition": {"KeyPrefixEquals": "docs/"}, "Redirect": {}}]
        client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}, "RoutingRules": rules},
        )

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["RoutingRules"] == rules

    @pytest.mark.tag("ERROR")
    def test_website_bucket_not_found(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(23)

        web_config = {"IndexDocument": {"Suffix": "index.html"}}
        self.assert_client_error(
            lambda: client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(lambda: client.get_bucket_website(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(lambda: client.delete_bucket_website(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("Access")
    def test_website_access_denied(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 24)

        web_config = {"IndexDocument": {"Suffix": "index.html"}}
        self.assert_client_error(
            lambda: alt_client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config),
            403,
            md.ACCESS_DENIED,
        )

        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config)

        self.assert_client_error(lambda: alt_client.get_bucket_website(Bucket=bucket_name), 403, md.ACCESS_DENIED)
        self.assert_client_error(lambda: alt_client.delete_bucket_website(Bucket=bucket_name), 403, md.ACCESS_DENIED)

        response = client.get_bucket_website(Bucket=bucket_name)
        assert response["IndexDocument"] == {"Suffix": "index.html"}

    @staticmethod
    def _create_routing_rules(count: int) -> list:
        return [
            {
                "Condition": {"KeyPrefixEquals": f"prefix{i}/"},
                "Redirect": {"ReplaceKeyPrefixWith": f"target{i}/"},
            }
            for i in range(count)
        ]

    # PutBucketWebsite가 400으로 실패하고 설정이 남지 않았는지 확인
    def _check_put_website_failed(self, client, bucket_name: str, web_config: dict, error_code: str) -> None:
        self.assert_client_error(
            lambda: client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=web_config),
            400,
            error_code,
        )
        self.assert_client_error(
            lambda: client.get_bucket_website(Bucket=bucket_name), 404, md.NO_SUCH_WEBSITE_CONFIGURATION
        )
