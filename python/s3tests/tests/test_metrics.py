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

    @staticmethod
    def _put_metrics(client, bucket_name: str, metric_id: str, config: dict | None = None):
        return client.put_bucket_metrics_configuration(
            Bucket=bucket_name,
            Id=metric_id,
            MetricsConfiguration=config if config is not None else {"Id": metric_id},
        )

    @pytest.mark.tag("Max")
    def test_put_metrics_id_length(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        # ID 는 64자까지 허용한다
        metric_id = "a" * 64
        self._put_metrics(client, bucket_name, metric_id)
        response = client.get_bucket_metrics_configuration(Bucket=bucket_name, Id=metric_id)
        assert response["MetricsConfiguration"]["Id"] == metric_id

        self.assert_client_error(lambda: self._put_metrics(client, bucket_name, "a" * 65), 400, md.MALFORMED_XML)

    @pytest.mark.tag("Error")
    def test_put_metrics_invalid_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        # ID 에는 영숫자와 - _ . 만 쓸 수 있다
        self.assert_client_error(lambda: self._put_metrics(client, bucket_name, "a b/c"), 400, md.MALFORMED_XML)
        assert not client.list_bucket_metrics_configurations(Bucket=bucket_name).get("MetricsConfigurationList")

    @pytest.mark.tag("Error")
    def test_put_metrics_id_mismatch(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        # 쿼리의 id 와 본문의 Id 가 다르면 거부된다
        self.assert_client_error(
            lambda: self._put_metrics(client, bucket_name, "metrics-id", {"Id": "other-id"}), 400, md.MALFORMED_XML
        )
        assert not client.list_bucket_metrics_configurations(Bucket=bucket_name).get("MetricsConfigurationList")

    @pytest.mark.tag("Error")
    def test_put_metrics_invalid_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        tag = {"Key": "key", "Value": "value"}
        # And 안에 조건이 하나뿐이면 거부된다
        self.assert_client_error(
            lambda: self._put_metrics(
                client, bucket_name, "metrics-id", {"Id": "metrics-id", "Filter": {"And": {"Prefix": "test"}}}
            ),
            400,
            md.MALFORMED_XML,
        )
        # And 없이 Prefix 와 Tag 를 함께 줄 수 없다
        self.assert_client_error(
            lambda: self._put_metrics(
                client, bucket_name, "metrics-id", {"Id": "metrics-id", "Filter": {"Prefix": "test", "Tag": tag}}
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("List")
    def test_list_metrics_pagination(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        ids = [f"metrics-{i:03d}" for i in range(101)]
        for metric_id in ids:
            self._put_metrics(client, bucket_name, metric_id)

        # 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
        response = client.list_bucket_metrics_configurations(Bucket=bucket_name)
        assert len(response["MetricsConfigurationList"]) == 100
        assert response["IsTruncated"] is True
        listed = [config["Id"] for config in response["MetricsConfigurationList"]]

        response = client.list_bucket_metrics_configurations(
            Bucket=bucket_name, ContinuationToken=response["NextContinuationToken"]
        )
        assert len(response["MetricsConfigurationList"]) == 1
        assert response["IsTruncated"] is False
        listed += [config["Id"] for config in response["MetricsConfigurationList"]]
        assert sorted(listed) == ids

    @pytest.mark.tag("List")
    def test_list_metrics_invalid_token(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        self.assert_client_error(
            lambda: client.list_bucket_metrics_configurations(Bucket=bucket_name, ContinuationToken="invalid"),
            400,
            md.MALFORMED_CONTINUATION_TOKEN,
        )

    @pytest.mark.tag("Error")
    def test_metrics_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(21)
        self.assert_client_error(
            lambda: client.list_bucket_metrics_configurations(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: client.get_bucket_metrics_configuration(Bucket=bucket_name, Id="metrics-id"),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(
            lambda: client.delete_bucket_metrics_configuration(Bucket=bucket_name, Id="metrics-id"),
            404,
            md.NO_SUCH_BUCKET,
        )
