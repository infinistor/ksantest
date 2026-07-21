"""Notification tests ported from Java testV2/Notification.java."""

from __future__ import annotations

import pytest

from s3tests.test_base import S3TestBase


class TestNotification(S3TestBase):
    @pytest.mark.tag("Get")
    def test_notification_get_empty(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        result = client.get_bucket_notification_configuration(Bucket=bucket_name)
        assert len(result.get("LambdaFunctionConfigurations", [])) == 0
        assert len(result.get("QueueConfigurations", [])) == 0
        assert len(result.get("TopicConfigurations", [])) == 0

    @pytest.mark.tag("Put")
    def test_notification_put(self):
        self.skip_if_aws()
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        role_id = "my-lambda"
        function_arn = f"aws:lambda::{self.config.main_user.id}:function:my-function"
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
        bucket_name = self.create_bucket(client)
        role_id = "my-lambda"
        function_arn = f"aws:lambda::{self.config.main_user.id}:function:my-function"
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
        bucket_name = self.create_bucket(client)
        role_id = "my-lambda"
        function_arn = f"aws:lambda::{self.config.main_user.id}:function:my-function"
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
