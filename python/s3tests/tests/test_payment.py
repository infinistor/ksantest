"""Payment tests ported from Java testV2/Payment.java."""

from __future__ import annotations

import pytest

from s3tests.test_base import S3TestBase


@pytest.mark.skip(reason="Java s3tests wrapper commented out")
class TestPayment(S3TestBase):
    @pytest.mark.tag("Put")
    def test_put_bucket_request_payment(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_bucket_request_payment(
            Bucket=bucket_name,
            RequestPaymentConfiguration={"Payer": "Requester"},
        )

    @pytest.mark.tag("Get")
    def test_get_bucket_request_payment(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        result = client.get_bucket_request_payment(Bucket=bucket_name)
        assert result["Payer"] == "BucketOwner"

    @pytest.mark.tag("Get")
    def test_set_get_bucket_request_payment(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        client.put_bucket_request_payment(
            Bucket=bucket_name,
            RequestPaymentConfiguration={"Payer": "Requester"},
        )
        result = client.get_bucket_request_payment(Bucket=bucket_name)
        assert result["Payer"] == "Requester"
