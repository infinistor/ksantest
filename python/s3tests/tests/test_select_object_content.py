# """SelectObjectContent tests ported from Java testV2/SelectObjectContent.java."""

# from __future__ import annotations

# import time

# import pytest

# from s3tests.data import main_data as md
# from s3tests.test_base import S3TestBase


# @pytest.mark.skip(reason="Java s3tests wrapper commented out")
# class TestSelectObjectContent(S3TestBase):
#     def _csv_input_serialization(self, file_header_info: str = "USE") -> dict:
#         return {
#             "CSV": {"FileHeaderInfo": file_header_info},
#             "CompressionType": "NONE",
#         }

#     def _csv_output_serialization(self) -> dict:
#         return {"CSV": {}}

#     def _json_input_serialization(self) -> dict:
#         return {
#             "JSON": {"Type": "LINES"},
#             "CompressionType": "NONE",
#         }

#     def _json_output_serialization(self) -> dict:
#         return {"JSON": {}}

#     @pytest.mark.tag("Select")
#     def test_select_object_content_csv_basic(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "data.csv"
#         csv_content = "name,age,city\nAlice,30,Seoul\nBob,25,Busan"
#         client.put_object(Bucket=bucket_name, Key=key, Body=csv_content.encode("utf-8"))

#         result = self.collect_select_object_content(
#             client,
#             Bucket=bucket_name,
#             Key=key,
#             Expression="SELECT * FROM s3object",
#             ExpressionType="SQL",
#             InputSerialization=self._csv_input_serialization(),
#             OutputSerialization=self._csv_output_serialization(),
#         )
#         assert result is not None
#         assert "Alice" in result and "Bob" in result
#         assert "Seoul" in result and "Busan" in result

#     @pytest.mark.tag("Select")
#     def test_select_object_content_csv_with_where(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "data.csv"
#         csv_content = "name,age,city\nAlice,30,Seoul\nBob,25,Busan\nCharlie,30,Incheon"
#         client.put_object(Bucket=bucket_name, Key=key, Body=csv_content.encode("utf-8"))

#         result = self.collect_select_object_content(
#             client,
#             Bucket=bucket_name,
#             Key=key,
#             Expression='SELECT * FROM s3object WHERE s3."age" = 30',
#             ExpressionType="SQL",
#             InputSerialization=self._csv_input_serialization(),
#             OutputSerialization=self._csv_output_serialization(),
#         )
#         assert result is not None
#         assert "Alice" in result and "Charlie" in result
#         assert "Bob" not in result or "25" in result

#     @pytest.mark.tag("Select")
#     def test_select_object_content_csv_limit(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "data.csv"
#         csv_content = "id,value\n1,a\n2,b\n3,c"
#         client.put_object(Bucket=bucket_name, Key=key, Body=csv_content.encode("utf-8"))

#         result = self.collect_select_object_content(
#             client,
#             Bucket=bucket_name,
#             Key=key,
#             Expression="SELECT * FROM s3object LIMIT 2",
#             ExpressionType="SQL",
#             InputSerialization=self._csv_input_serialization(),
#             OutputSerialization=self._csv_output_serialization(),
#         )
#         assert result is not None
#         assert "1" in result and "2" in result

#     @pytest.mark.tag("Select")
#     def test_select_object_content_json_basic(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "data.json"
#         json_content = '{"name":"Alice","age":30}\n{"name":"Bob","age":25}'
#         client.put_object(Bucket=bucket_name, Key=key, Body=json_content.encode("utf-8"))

#         result = self.collect_select_object_content(
#             client,
#             Bucket=bucket_name,
#             Key=key,
#             Expression="SELECT * FROM s3object s WHERE s.age > 26",
#             ExpressionType="SQL",
#             InputSerialization=self._json_input_serialization(),
#             OutputSerialization=self._json_output_serialization(),
#         )
#         assert result is not None
#         assert "Alice" in result and "30" in result

#     @pytest.mark.tag("ERROR")
#     def test_select_object_content_non_existent_bucket(self):
#         client = self.get_client()
#         bucket_name = f"non-existent-bucket-{int(time.time() * 1000)}"
#         key = "data.csv"

#         self.assert_client_error(
#             lambda: self.collect_select_object_content(
#                 client,
#                 Bucket=bucket_name,
#                 Key=key,
#                 Expression="SELECT * FROM s3object",
#                 ExpressionType="SQL",
#                 InputSerialization=self._csv_input_serialization("NONE"),
#                 OutputSerialization=self._csv_output_serialization(),
#             ),
#             404,
#             md.NO_SUCH_BUCKET,
#         )

#     @pytest.mark.tag("ERROR")
#     def test_select_object_content_non_existent_object(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "non-existent-key.csv"

#         self.assert_client_error(
#             lambda: self.collect_select_object_content(
#                 client,
#                 Bucket=bucket_name,
#                 Key=key,
#                 Expression="SELECT * FROM s3object",
#                 ExpressionType="SQL",
#                 InputSerialization=self._csv_input_serialization("NONE"),
#                 OutputSerialization=self._csv_output_serialization(),
#             ),
#             404,
#             md.NO_SUCH_KEY,
#         )

#     @pytest.mark.tag("Select")
#     def test_select_object_content_csv_empty_rows(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)
#         key = "empty.csv"
#         csv_content = "col1,col2\n"
#         client.put_object(Bucket=bucket_name, Key=key, Body=csv_content.encode("utf-8"))

#         result = self.collect_select_object_content(
#             client,
#             Bucket=bucket_name,
#             Key=key,
#             Expression="SELECT * FROM s3object",
#             ExpressionType="SQL",
#             InputSerialization=self._csv_input_serialization(),
#             OutputSerialization=self._csv_output_serialization(),
#         )
#         assert result is not None
