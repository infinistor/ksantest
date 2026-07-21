# """Analytics tests ported from Java testV2/Analytics.java."""

# from __future__ import annotations

# import pytest

# from s3tests.data import main_data as md
# from s3tests.test_base import S3TestBase


# class TestAnalytics(S3TestBase):
#     def _analytics_config(
#         self,
#         config_id: str,
#         target_bucket_name: str,
#         schema_version: str = "V_1",
#         output_format: str = "CSV",
#     ) -> dict:
#         return {
#             "Id": config_id,
#             "StorageClassAnalysis": {
#                 "DataExport": {
#                     "OutputSchemaVersion": schema_version,
#                     "Destination": {
#                         "S3BucketDestination": {
#                             "Bucket": f"arn:aws:s3:::{target_bucket_name}",
#                             "Format": output_format,
#                         }
#                     },
#                 }
#             },
#         }

#     @pytest.mark.tag("Put")
#     def test_put_bucket_analytics(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test",
#             AnalyticsConfiguration=self._analytics_config("test", target_bucket_name),
#         )

#     @pytest.mark.tag("Get")
#     def test_get_bucket_analytics(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test",
#             AnalyticsConfiguration=self._analytics_config("test", target_bucket_name),
#         )
#         response = client.get_bucket_analytics_configuration(Bucket=bucket_name, Id="test")
#         config = response["AnalyticsConfiguration"]
#         assert config["Id"] == "test"
#         data_export = config["StorageClassAnalysis"]["DataExport"]
#         assert data_export["OutputSchemaVersion"] == "V_1"
#         destination = data_export["Destination"]["S3BucketDestination"]
#         assert destination["Bucket"] == f"arn:aws:s3:::{target_bucket_name}"
#         assert destination["Format"] == "CSV"

#     @pytest.mark.tag("Put")
#     def test_add_bucket_analytics(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test",
#             AnalyticsConfiguration=self._analytics_config("test", target_bucket_name),
#         )
#         response = client.list_bucket_analytics_configurations(Bucket=bucket_name)
#         assert len(response["AnalyticsConfigurationList"]) == 1
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test2",
#             AnalyticsConfiguration=self._analytics_config("test2", target_bucket_name),
#         )
#         response = client.list_bucket_analytics_configurations(Bucket=bucket_name)
#         assert len(response["AnalyticsConfigurationList"]) == 2

#     @pytest.mark.tag("List")
#     def test_list_bucket_analytics(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test",
#             AnalyticsConfiguration=self._analytics_config("test", target_bucket_name),
#         )
#         response = client.list_bucket_analytics_configurations(Bucket=bucket_name)
#         assert len(response["AnalyticsConfigurationList"]) == 1
#         config = response["AnalyticsConfigurationList"][0]
#         assert config["Id"] == "test"
#         data_export = config["StorageClassAnalysis"]["DataExport"]
#         assert data_export["OutputSchemaVersion"] == "V_1"
#         destination = data_export["Destination"]["S3BucketDestination"]
#         assert destination["Bucket"] == f"arn:aws:s3:::{target_bucket_name}"
#         assert destination["Format"] == "CSV"

#     @pytest.mark.tag("Delete")
#     def test_delete_bucket_analytics(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         client.put_bucket_analytics_configuration(
#             Bucket=bucket_name,
#             Id="test",
#             AnalyticsConfiguration=self._analytics_config("test", target_bucket_name),
#         )
#         client.delete_bucket_analytics_configuration(Bucket=bucket_name, Id="test")
#         self.assert_client_error(
#             lambda: client.get_bucket_analytics_configuration(Bucket=bucket_name, Id="test"),
#             404,
#             md.NO_SUCH_CONFIGURATION,
#         )

#     @pytest.mark.tag("Error")
#     def test_put_bucket_analytics_invalid(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         target_bucket_name = self.create_bucket(client)
#         self.assert_client_error(
#             lambda: client.put_bucket_analytics_configuration(
#                 Bucket=bucket_name,
#                 Id="test",
#                 AnalyticsConfiguration=self._analytics_config("test", target_bucket_name, schema_version="V_2"),
#             ),
#             400,
#             md.MALFORMED_XML,
#         )
#         self.assert_client_error(
#             lambda: client.put_bucket_analytics_configuration(
#                 Bucket=bucket_name,
#                 Id="",
#                 AnalyticsConfiguration=self._analytics_config("", target_bucket_name),
#             ),
#             400,
#             md.INVALID_CONFIGURATION_ID,
#         )
#         self.assert_client_error(
#             lambda: client.put_bucket_analytics_configuration(
#                 Bucket=bucket_name,
#                 Id="test",
#                 AnalyticsConfiguration=self._analytics_config("test", target_bucket_name, output_format="JSON"),
#             ),
#             400,
#             md.MALFORMED_XML,
#         )
