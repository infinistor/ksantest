/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.S3;
using Newtonsoft.Json.Linq;
using s3tests.Client;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Xunit;
using s3tests.Data;
using s3tests.Utils;

namespace s3tests.Test
{
	public class Post : TestBase
	{
		public Post(Xunit.Abstractions.ITestOutputHelper output) => this.Output = output;

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectAnonymousRequest()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();

			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var contentType = "text/plain";
			var key = "TestPostObjectAnonymousRequest";
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "public-read" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectAuthenticatedRequest()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var contentType = "text/plain";
			var key = "TestPostObjectAuthenticatedRequest";

			var policyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", contentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var bytesJsonPolicyDocument = Encoding.UTF8.GetBytes(policyDocument.ToString());
			var policy = Convert.ToBase64String(bytesJsonPolicyDocument);

			var signature = S3Utils.GetBase64EncodedSHA1Hash(policy, Config.MainUser.SecretKey);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", signature },
					{ "policy", policy },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "content-type 헤더 정보 없이 post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectAuthenticatedNoContentType()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);
			var contentType = "text/plain";
			var key = "TestPostObjectAuthenticatedNoContentType";

			var policyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var bytesJsonPolicyDocument = Encoding.UTF8.GetBytes(policyDocument.ToString());
			var policy = Convert.ToBase64String(bytesJsonPolicyDocument);

			var signature = S3Utils.GetBase64EncodedSHA1Hash(policy, Config.MainUser.SecretKey);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", signature },
					{ "policy", policy },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AccessKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectAuthenticatedRequestBadAccessKey()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var contentType = "text/plain";
			var key = "TestPostObjectAuthenticatedRequestBadAccessKey";

			var policyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", contentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var bytesJsonPolicyDocument = Encoding.UTF8.GetBytes(policyDocument.ToString());
			var policy = Convert.ToBase64String(bytesJsonPolicyDocument);

			var signature = S3Utils.GetBase64EncodedSHA1Hash(policy, Config.MainUser.SecretKey);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "AWSAccessKeyId", "foo" },
					{ "acl", "private" },
					{ "signature", signature },
					{ "policy", policy },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.Forbidden, result.StatusCode, result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Status code")]
		[Trait(MainData.Explanation, "[성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectSetSuccessCode()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();

			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var contentType = "text/plain";
			var key = "TestPostObjectSetSuccessCode";
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "public-read" },
					{ "success_action_status" , "201" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.Created, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Status code")]
		[Trait(MainData.Explanation, "[성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectSetInvalidSuccessCode()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var contentType = "text/plain";
			var key = "TestPostObjectSetInvalidSuccessCode";
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "public-read" },
					{ "success_action_status" , "404" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectUploadLargerThanChunk()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var contentType = "text/plain";
			var key = "TestPostObjectUploadLargerThanChunk";
			var size = 5 * 1024 * 1024;

			var policyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", contentType } },
						{ new JArray() { "content-length-range", 0, size } },
					}
				},
			};

			var bytesJsonPolicyDocument = Encoding.UTF8.GetBytes(policyDocument.ToString());
			var policy = Convert.ToBase64String(bytesJsonPolicyDocument);

			var signature = S3Utils.GetBase64EncodedSHA1Hash(policy, Config.MainUser.SecretKey);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", signature },
					{ "policy", policy },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};

			var result = PostUpload(bucketName, payload);
			AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

			var response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(response);
			Assert.Equal(key, body);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] " +
									 "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_set_key_from_filename()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);

			var Response = client.GetObject(bucketName, Key);
			Assert.Equal("bar", S3Utils.GetBody(Response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_ignored_header()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "x-ignore-foo" , "bar"},
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[헤더정보에 대소문자를 섞어서 사용할 경우] " +
									 "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_case_insensitive_condition_fields()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bUcKeT", bucketName } } },
						{ new JArray() { "StArTs-WiTh", "$KeY", "foo" } },
						{ new JObject() { { "AcL", "private" } } },
						{ new JArray() { "StArTs-WiTh", "$CoNtEnT-TyPe", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "kEy", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "aCl", "private" },
					{ "signature", Signature },
					{ "pOLICy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[오브젝트 이름에 '\'를 사용할 경우] " +
									 "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_escaped_field_values()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "\\$foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "\\$foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);

			var Response = client.GetObject(bucketName, Key);
			Assert.Equal("bar", S3Utils.GetBody(Response));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_success_redirect_action()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var ContentType = "text/plain";
			var Key = "foo.txt";
			var RedirectURL = GetURL(bucketName);

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "eq", "$success_action_redirect", RedirectURL } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "success_action_redirect" , RedirectURL },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.OK, Result.StatusCode, Result.Message);

			var Response = client.GetObject(bucketName, Key);
			Assert.Equal(string.Format("{0}?bucket={1}&key={2}&etag=%22{3}%22", RedirectURL, bucketName, Key, Response.ETag.Replace("\"", "")), Result.URL);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_invalid_signature()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "\\$foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature[0..^1] },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AccessKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_invalid_access_key()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "\\$foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey[0..^1] },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_invalid_date_format()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.Now.AddMinutes(100) },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "\\$foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_no_key_specified()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = "", ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_missing_signature()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "\\$foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_missing_policy_condition()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "[사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_post_object_user_specified_header()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "starts-with", "$x-amz-meta-foo", "bar" } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "x-amz-meta-foo" , "barclamp" },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);

			var Response = client.GetObject(bucketName, Key);
			Assert.Equal("barclamp", Response.Metadata["foo"]);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_request_missing_policy_specified_field()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "eq", "$x-amz-meta-foo", "" } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_condition_is_case_sensitive()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "CONDITIONS", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_expires_is_case_sensitive()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"EXPIRATION", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_expired_policy()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(-100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_invalid_request_field_value()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "eq", "$x-amz-meta-foo", "" } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "x-amz-meta-foo" , "barclamp" },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_missing_expires_condition()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_missing_conditions_list()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_upload_size_limit_exceeded()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 0 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_missing_content_length_argument()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력)] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_invalid_content_length_argument()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", -1, 0 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_upload_size_below_minimum()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 512, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_post_object_empty_conditions()
		{
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_presignedurl_put_get()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "foo";

			var PutURL = client.GeneratePresignedURL(bucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, Key);

			Assert.Equal(HttpStatusCode.OK, PutResponse.StatusCode);

			var GetURL = client.GeneratePresignedURL(bucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.Equal(HttpStatusCode.OK, GetResponse.StatusCode);
		}


		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "[SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_presignedurl_put_get_v4()
		{
			var bucketName = GetNewBucketName();
			var client = GetClientV4();
			var Key = "foo";

			client.PutBucket(bucketName);

			var PutURL = client.GeneratePresignedURL(bucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, Key);

			Assert.Equal(HttpStatusCode.OK, PutResponse.StatusCode);

			var GetURL = client.GeneratePresignedURL(bucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.Equal(HttpStatusCode.OK, GetResponse.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "signV4")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_v4()
		{
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "foo";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			var client = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey);

			var Response = client.PutObject(Key, Content, ContentType: ContentType);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "signV4")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_chunked_v4()
		{
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "foo";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			var client = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey);

			var Response = client.PutObjectChunked(Key, Content, ContentType: ContentType);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "signV4")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 오브젝트 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectV4()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "foo";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			client.PutObject(bucketName, Key, Content);

			var MyClient = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey);
			var Response = MyClient.GetObject(Key, out string Body);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
			Assert.Equal(Size, Body.Length);
			Assert.Equal(Content, Body);
		}
	}
}
