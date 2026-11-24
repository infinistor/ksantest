"""Notification tests ported from Java testV2/Notification.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestNotification(S3TestBase):
    @pytest.mark.tag("Get")
    def test_notification_get_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(result.get("LambdaFunctionConfigurations", [])) == 0
        assert len(result.get("QueueConfigurations", [])) == 0
        assert len(result.get("TopicConfigurations", [])) == 0

    @pytest.mark.tag("Put")
    def test_notification_put(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        role_id = "my-lambda"
        function_arn = self._lambda_function_arn()
        s3_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
        notification = {
            "LambdaFunctionConfigurations": [
                {
                    "Id": role_id,
                    "LambdaFunctionArn": function_arn,
                    "Events": s3_events,
                }
            ]
        }
        self._put_notification(client, bucket_name, notification)

    @pytest.mark.tag("Get")
    def test_notification_get(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        role_id = "my-lambda"
        function_arn = self._lambda_function_arn()
        s3_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
        notification = {
            "LambdaFunctionConfigurations": [
                {
                    "Id": role_id,
                    "LambdaFunctionArn": function_arn,
                    "Events": s3_events,
                }
            ]
        }
        self._put_notification(client, bucket_name, notification)
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        result_lambda = result["LambdaFunctionConfigurations"][0]
        self.s3event_compare(s3_events, result_lambda["Events"])

    @pytest.mark.tag("Delete")
    def test_notification_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        role_id = "my-lambda"
        function_arn = self._lambda_function_arn()
        s3_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
        notification = {
            "LambdaFunctionConfigurations": [
                {
                    "Id": role_id,
                    "LambdaFunctionArn": function_arn,
                    "Events": s3_events,
                }
            ]
        }
        self._put_notification(client, bucket_name, notification)
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        result_lambda = result["LambdaFunctionConfigurations"][0]
        self.s3event_compare(s3_events, result_lambda["Events"])
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={},
        )
        delete_result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(delete_result.get("LambdaFunctionConfigurations", [])) == 0

    @pytest.mark.tag("Error")
    def test_notification_put_invalid_arn(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        # arn: 접두어와 리전이 없는 잘못된 형식
        function_arn = f"aws:lambda::{self.config.main_user.id}:function:my-function"
        notification = {
            "LambdaFunctionConfigurations": [
                {
                    "Id": "my-lambda",
                    "LambdaFunctionArn": function_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        }
        self.assert_client_error(
            lambda: self._put_notification(client, bucket_name, notification),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("EventBridge")
    def test_notification_event_bridge(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        # EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"EventBridgeConfiguration": {}},
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert "EventBridgeConfiguration" in result
        assert len(result.get("LambdaFunctionConfigurations", [])) == 0
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={},
        )
        delete_result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert "EventBridgeConfiguration" not in delete_result

    def _lambda_function_arn(self) -> str:
        """Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.

        AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
        """
        region = self.config.region_name or "us-east-1"
        return f"arn:aws:lambda:{region}:{self.config.main_user.expected_owner_id}:function:my-function"

    def _put_notification(self, client, bucket_name: str, notification: dict) -> None:
        """대상 리소스 검증을 건너뛰고 알림 설정을 저장한다 (x-amz-skip-destination-validation)."""
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification,
            SkipDestinationValidation=True,
        )

    def _arn(self, service: str, resource: str) -> str:
        region = self.config.region_name or "us-east-1"
        return f"arn:aws:{service}:{region}:{self.config.main_user.expected_owner_id}:{resource}"

    def _lambda_config(self, config_id: str = "my-lambda", events=None, filter_rules=None) -> dict:
        config: dict = {
            "Id": config_id,
            "LambdaFunctionArn": self._lambda_function_arn(),
            "Events": list(events or ["s3:ObjectCreated:*"]),
        }
        if filter_rules is not None:
            config["Filter"] = {"Key": {"FilterRules": filter_rules}}
        return config

    @pytest.mark.tag("Queue")
    def test_notification_queue(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        queue_arn = self._arn("sqs", "my-queue")
        s3_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
        self._put_notification(
            client,
            bucket_name,
            {"QueueConfigurations": [{"Id": "my-queue", "QueueArn": queue_arn, "Events": s3_events}]},
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(result["QueueConfigurations"]) == 1
        assert result["QueueConfigurations"][0]["Id"] == "my-queue"
        assert result["QueueConfigurations"][0]["QueueArn"] == queue_arn
        self.s3event_compare(s3_events, result["QueueConfigurations"][0]["Events"])

        self._put_notification(client, bucket_name, {})
        delete_result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(delete_result.get("QueueConfigurations", [])) == 0

    @pytest.mark.tag("Topic")
    def test_notification_topic(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 8)
        topic_arn = self._arn("sns", "my-topic")
        s3_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
        self._put_notification(
            client,
            bucket_name,
            {"TopicConfigurations": [{"Id": "my-topic", "TopicArn": topic_arn, "Events": s3_events}]},
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(result["TopicConfigurations"]) == 1
        assert result["TopicConfigurations"][0]["Id"] == "my-topic"
        assert result["TopicConfigurations"][0]["TopicArn"] == topic_arn
        self.s3event_compare(s3_events, result["TopicConfigurations"][0]["Events"])

        self._put_notification(client, bucket_name, {})
        delete_result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(delete_result.get("TopicConfigurations", [])) == 0

    @pytest.mark.tag("Filter")
    def test_notification_filter(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        rules = [{"Name": "prefix", "Value": "logs/"}, {"Name": "suffix", "Value": ".txt"}]
        self._put_notification(
            client,
            bucket_name,
            {"LambdaFunctionConfigurations": [self._lambda_config(filter_rules=rules)]},
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        result_rules = result["LambdaFunctionConfigurations"][0]["Filter"]["Key"]["FilterRules"]
        # AWS 는 필터 이름을 Prefix/Suffix 로 정규화해 돌려주므로 대소문자를 무시하고 비교한다
        assert sorted((r["Name"].lower(), r["Value"]) for r in result_rules) == [
            ("prefix", "logs/"),
            ("suffix", ".txt"),
        ]

    @pytest.mark.tag("Error")
    def test_notification_invalid_event(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        self.assert_client_error(
            lambda: self._put_notification(
                client,
                bucket_name,
                {"LambdaFunctionConfigurations": [self._lambda_config(events=["s3:Foo:*"])]},
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Error")
    def test_notification_invalid_filter_name(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        self.assert_client_error(
            lambda: self._put_notification(
                client,
                bucket_name,
                {
                    "LambdaFunctionConfigurations": [
                        self._lambda_config(filter_rules=[{"Name": "foo", "Value": "a"}])
                    ]
                },
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Error")
    def test_notification_overlapping_event(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        # 필터 없이 같은 이벤트를 두 설정이 공유하면 거부된다
        self.assert_client_error(
            lambda: self._put_notification(
                client,
                bucket_name,
                {"LambdaFunctionConfigurations": [self._lambda_config("c1"), self._lambda_config("c2")]},
            ),
            400,
            md.INVALID_ARGUMENT,
        )
        # 접두어가 겹치지 않으면 같은 이벤트라도 허용된다
        self._put_notification(
            client,
            bucket_name,
            {
                "LambdaFunctionConfigurations": [
                    self._lambda_config("c1", filter_rules=[{"Name": "prefix", "Value": "a/"}]),
                    self._lambda_config("c2", filter_rules=[{"Name": "prefix", "Value": "b/"}]),
                ]
            },
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert sorted(c["Id"] for c in result["LambdaFunctionConfigurations"]) == ["c1", "c2"]

    @pytest.mark.tag("Error")
    def test_notification_duplicate_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        self.assert_client_error(
            lambda: self._put_notification(
                client,
                bucket_name,
                {
                    "LambdaFunctionConfigurations": [
                        self._lambda_config("c1", filter_rules=[{"Name": "prefix", "Value": "a/"}]),
                        self._lambda_config("c1", filter_rules=[{"Name": "prefix", "Value": "b/"}]),
                    ]
                },
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("Error")
    def test_notification_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(15)
        self.assert_client_error(
            lambda: client.get_bucket_notification_configuration(Bucket=bucket_name),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(
            lambda: self._put_notification(client, bucket_name, {}),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("Permission")
    def test_notification_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 16)
        self.assert_client_error(
            lambda: alt_client.get_bucket_notification_configuration(Bucket=bucket_name),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: self._put_notification(alt_client, bucket_name, {}),
            403,
            md.ACCESS_DENIED,
        )
