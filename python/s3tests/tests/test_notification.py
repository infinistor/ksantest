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
        self.skip_if_aws()
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
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification,
        )

    @pytest.mark.tag("Get")
    def test_notification_get(self):
        self.skip_if_aws()
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
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification,
        )
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        result_lambda = result["LambdaFunctionConfigurations"][0]
        self.s3event_compare(s3_events, result_lambda["Events"])

    @pytest.mark.tag("Delete")
    def test_notification_delete(self):
        self.skip_if_aws()
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
        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification,
        )
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
            lambda: client.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification,
            ),
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
