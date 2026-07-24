"""Metrics tests ported from Java testV2/Metrics.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError, ParamValidationError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestMetrics(S3TestBase):
    @pytest.mark.tag("List")
    def test_metrics(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        response = client.list_bucket_metrics_configurations(Bucket=bucket_name)
        assert len(response.get("MetricsConfigurationList", [])) == 0

    @pytest.mark.tag("Put")
    def test_put_metrics(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id="metrics-id",
            MetricsConfiguration={"Id": "metrics-id"},
        )

    @pytest.mark.tag("Check")
    def test_check_metrics(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id="metrics-id",
            MetricsConfiguration={"Id": "metrics-id"},
        )
        response = client.list_bucket_metrics_configurations(Bucket=bucket_name)
        assert len(response["MetricsConfigurationList"]) == 1

    @pytest.mark.tag("Get")
    def test_get_metrics(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        metric_id = "metrics-id"
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Id"] == metric_id

    @pytest.mark.tag("Delete")
    def test_delete_metrics(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        metric_id = "metrics-id"
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id},
        )
        client.delete_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)

    @pytest.mark.tag("Error")
    def test_get_metrics_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        with pytest.raises(ClientError) as exc_info:
            client.get_bucket_metrics_configuration(Bucket=bucket_name, Id="metrics-id")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_CONFIGURATION

    @pytest.mark.tag("Error")
    def test_delete_metrics_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        with pytest.raises(ClientError) as exc_info:
            client.delete_bucket_metrics_configuration(Bucket=bucket_name, Id="metrics-id")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_CONFIGURATION

    @pytest.mark.tag("Error")
    def test_put_metrics_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(8)
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id="metrics-id",
                MetricsConfiguration={"Id": "metrics-id"},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc_info.value.response["Error"]["Code"] == md.NO_SUCH_BUCKET

    @pytest.mark.tag("Error")
    def test_put_metrics_empty_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        with pytest.raises(ClientError) as exc_info:
            client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id="",
                MetricsConfiguration={"Id": ""},
            )
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc_info.value.response["Error"]["Code"] == md.INVALID_CONFIGURATION_ID

    @pytest.mark.tag("Error")
    def test_put_metrics_no_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        # botocore rejects missing Id client-side; Java SDK may raise similarly.
        with pytest.raises((ClientError, ParamValidationError)):
            client.put_bucket_metrics_configuration(Bucket=bucket_name)

    @pytest.mark.tag("Overwrite")
    def test_put_metrics_duplicate_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        metric_id = "metrics-id"
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id, "Filter": {"Prefix": "test1"}},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Filter"]["Prefix"] == "test1"
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id, "Filter": {"Prefix": "test2"}},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Filter"]["Prefix"] == "test2"

    @pytest.mark.tag("Filtering")
    def test_metrics_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        metric_id = "metrics-id"
        prefix = "test"
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id, "Filter": {"Prefix": prefix}},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Filter"]["Prefix"] == prefix

    @pytest.mark.tag("Filtering")
    def test_metrics_tag(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        metric_id = "metrics-id"
        tag = {"Key": "key", "Value": "value"}
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id, "Filter": {"Tag": tag}},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Filter"]["Tag"]["Key"] == tag["Key"]
        assert response["MetricsConfiguration"]["Filter"]["Tag"]["Value"] == tag["Value"]

    @pytest.mark.tag("Filtering")
    def test_metrics_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        metric_id = "metrics-id"
        prefix = "test"
        tag = {"Key": "key", "Value": "value"}
        client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration={"Id": metric_id, "Filter": {"And": {"Prefix": prefix, "Tags": [tag]}}},
        )
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Filter"]["And"]["Prefix"] == prefix
        assert response["MetricsConfiguration"]["Filter"]["And"]["Tags"][0]["Key"] == tag["Key"]
        assert response["MetricsConfiguration"]["Filter"]["And"]["Tags"][0]["Value"] == tag["Value"]
