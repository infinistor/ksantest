# """Accelerate tests ported from Java testV2/Accelerate.java."""

# from __future__ import annotations

# import pytest
# from botocore.exceptions import ClientError

# from s3tests.data import main_data as md
# from s3tests.test_base import S3TestBase


# class TestAccelerate(S3TestBase):
#     @pytest.mark.tag("Put")
#     def test_put_bucket_accelerate(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client, 1)
#         client.put_bucket_accelerate_configuration(
#             Bucket=bucket_name,
#             AccelerateConfiguration={"Status": "Enabled"},
#         )

#     @pytest.mark.tag("Get")
#     def test_get_bucket_accelerate(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client, 2)
#         client.put_bucket_accelerate_configuration(
#             Bucket=bucket_name,
#             AccelerateConfiguration={"Status": "Enabled"},
#         )
#         response = client.get_bucket_accelerate_configuration(Bucket=bucket_name)
#         assert response["Status"] == "Enabled"

#     @pytest.mark.tag("Change")
#     def test_change_bucket_accelerate(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client, 3)
#         client.put_bucket_accelerate_configuration(
#             Bucket=bucket_name,
#             AccelerateConfiguration={"Status": "Enabled"},
#         )
#         response = client.get_bucket_accelerate_configuration(Bucket=bucket_name)
#         assert response["Status"] == "Enabled"
#         client.put_bucket_accelerate_configuration(
#             Bucket=bucket_name,
#             AccelerateConfiguration={"Status": "Suspended"},
#         )
#         response = client.get_bucket_accelerate_configuration(Bucket=bucket_name)
#         assert response["Status"] == "Suspended"

#     @pytest.mark.tag("Error")
#     def test_put_bucket_accelerate_invalid(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client, 4)
#         with pytest.raises(ClientError) as exc_info:
#             client.put_bucket_accelerate_configuration(
#                 Bucket=bucket_name,
#                 AccelerateConfiguration={"Status": "Invalid"},
#             )
#         assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
#         assert exc_info.value.response["Error"]["Code"] == md.MALFORMED_XML
