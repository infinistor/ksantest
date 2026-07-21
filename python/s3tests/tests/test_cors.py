# """Cors tests ported from Java testV2/Cors.java."""

# from __future__ import annotations

# import pytest

# from s3tests.data import main_data as md
# from s3tests.test_base import S3TestBase


# class TestCors(S3TestBase):
#     def _create_public_cors_bucket(self, client):
#         bucket_name = self.create_bucket_canned_acl(client)
#         client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
#         return bucket_name

#     def _assert_no_cors_configuration(self, client, bucket_name):
#         self.assert_client_error(
#             lambda: client.get_bucket_cors(Bucket=bucket_name),
#             404,
#             md.NO_SUCH_CORS_CONFIGURATION,
#         )

#     @pytest.mark.tag("Check")
#     def test_set_cors(self):
#         client = self.get_client()
#         bucket_name = self.create_bucket(client)

#         allowed_methods = ["GET", "PUT"]
#         allowed_origins = ["*.get", "*.put"]
#         cors_config = {
#             "CORSRules": [
#                 {
#                     "AllowedMethods": allowed_methods,
#                     "AllowedOrigins": allowed_origins,
#                 }
#             ]
#         }

#         self.assert_client_error(
#             lambda: client.get_bucket_cors(Bucket=bucket_name),
#             404,
#             md.NO_SUCH_CORS_CONFIGURATION,
#         )

#         client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

#         response = client.get_bucket_cors(Bucket=bucket_name)
#         assert response["CORSRules"][0]["AllowedMethods"] == allowed_methods
#         assert response["CORSRules"][0]["AllowedOrigins"] == allowed_origins

#         client.delete_bucket_cors(Bucket=bucket_name)
#         self.assert_client_error(
#             lambda: client.get_bucket_cors(Bucket=bucket_name),
#             404,
#             md.NO_SUCH_CORS_CONFIGURATION,
#         )

#     @pytest.mark.tag("Post")
#     def test_cors_origin_response(self):
#         client = self.get_client()
#         bucket_name = self._create_public_cors_bucket(client)
#         cors_config = {
#             "CORSRules": [
#                 {"AllowedMethods": ["GET"], "AllowedOrigins": ["*suffix"]},
#                 {"AllowedMethods": ["GET"], "AllowedOrigins": ["start*end"]},
#                 {"AllowedMethods": ["GET"], "AllowedOrigins": ["prefix*"]},
#                 {"AllowedMethods": ["PUT"], "AllowedOrigins": ["*.put"]},
#             ]
#         }

#         self._assert_no_cors_configuration(client, bucket_name)
#         client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

#         checks = [
#             ("GET", {}, 200, None, None, None),
#             ("GET", {"Origin": "foo.suffix"}, 200, "foo.suffix", "GET", None),
#             ("GET", {"Origin": "foo.bar"}, 200, None, None, None),
#             ("GET", {"Origin": "foo.suffix.get"}, 200, None, None, None),
#             ("GET", {"Origin": "start_end"}, 200, "start_end", "GET", None),
#             ("GET", {"Origin": "start1end"}, 200, "start1end", "GET", None),
#             ("GET", {"Origin": "start12end"}, 200, "start12end", "GET", None),
#             ("GET", {"Origin": "0start12end"}, 200, None, None, None),
#             ("GET", {"Origin": "prefix"}, 200, "prefix", "GET", None),
#             ("GET", {"Origin": "prefix.suffix"}, 200, "prefix.suffix", "GET", None),
#             ("GET", {"Origin": "bla.prefix"}, 200, None, None, None),
#             ("GET", {"Origin": "foo.suffix"}, 404, "foo.suffix", "GET", "bar"),
#             ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "GET", "content-length": "0"}, 403, "foo.suffix", "GET", "bar"),
#             ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "PUT", "content-length": "0"}, 403, None, None, "bar"),
#             ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "DELETE", "content-length": "0"}, 403, None, None, "bar"),
#             ("PUT", {"Origin": "foo.suffix", "content-length": "0"}, 403, None, None, "bar"),
#             ("PUT", {"Origin": "foo.put", "content-length": "0"}, 403, "foo.put", "PUT", "bar"),
#             ("OPTIONS", {}, 400, None, None, None),
#             ("OPTIONS", {"Origin": "foo.suffix"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "foo.bla"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "foo.suffix", "Access-Control-Request-Method": "GET", "content-length": "0"}, 200, "foo.suffix", "GET", "bar"),
#             ("OPTIONS", {"Origin": "foo.bar", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "foo.suffix.get", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "start_end", "Access-Control-Request-Method": "GET"}, 200, "start_end", "GET", None),
#             ("OPTIONS", {"Origin": "start1end", "Access-Control-Request-Method": "GET"}, 200, "start1end", "GET", None),
#             ("OPTIONS", {"Origin": "start12end", "Access-Control-Request-Method": "GET"}, 200, "start12end", "GET", None),
#             ("OPTIONS", {"Origin": "0start12end", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "prefix", "Access-Control-Request-Method": "GET"}, 200, "prefix", "GET", None),
#             ("OPTIONS", {"Origin": "prefix.suffix", "Access-Control-Request-Method": "GET"}, 200, "prefix.suffix", "GET", None),
#             ("OPTIONS", {"Origin": "bla.prefix", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "foo.put", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
#             ("OPTIONS", {"Origin": "foo.put", "Access-Control-Request-Method": "PUT"}, 200, "foo.put", "PUT", None),
#         ]
#         for method, headers, status, allow_origin, allow_methods, key in checks:
#             self.cors_request_and_check(
#                 method,
#                 bucket_name,
#                 headers,
#                 status,
#                 allow_origin,
#                 allow_methods,
#                 key,
#             )

#     @pytest.mark.tag("Post")
#     def test_cors_origin_wildcard(self):
#         client = self.get_client()
#         bucket_name = self._create_public_cors_bucket(client)
#         cors_config = {"CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]}

#         self._assert_no_cors_configuration(client, bucket_name)
#         client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

#         self.cors_request_and_check("GET", bucket_name, {}, 200, None, None)
#         self.cors_request_and_check(
#             "GET", bucket_name, {"Origin": "example.origin"}, 200, "*", "GET"
#         )

#     @pytest.mark.tag("Post")
#     def test_cors_header_option(self):
#         client = self.get_client()
#         bucket_name = self._create_public_cors_bucket(client)
#         cors_config = {
#             "CORSRules": [
#                 {
#                     "AllowedMethods": ["GET"],
#                     "AllowedOrigins": ["*"],
#                     "ExposeHeaders": ["x-amz-meta-header1"],
#                 }
#             ]
#         }

#         self._assert_no_cors_configuration(client, bucket_name)
#         client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

#         headers = {
#             "Origin": "example.origin",
#             "Access-Control-Request-headers": "x-amz-meta-header2",
#             "Access-Control-Request-Method": "GET",
#         }
#         self.cors_request_and_check("OPTIONS", bucket_name, headers, 403, None, None, "bar")
