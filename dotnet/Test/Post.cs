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
using System.Text;
using System.Security.Cryptography;
using System.Linq;
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;
using s3tests.Client;
using System;
using System.Collections.Generic;
using System.Net;
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
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

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
			TestId = 2;
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

			var sign = SignPostPolicy(policyDocument);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "private" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};
			sign.Apply(payload);

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
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);
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

			var sign = SignPostPolicy(policyDocument);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "private" },
					{ "file", fileData },
			};
			sign.Apply(payload);

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
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

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

			var sign = SignPostPolicy(policyDocument);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "private" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};
			sign.Apply(payload);
			// 정상 서명 후 credential의 AccessKey 부분만 존재하지 않는 값으로 교체한다.
			payload["x-amz-credential"] = sign.Credential.Replace(Config.MainUser.AccessKey, "foo");

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
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

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
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

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
			TestId = 7;
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

			var sign = SignPostPolicy(policyDocument);
			var fileData = new FormFile() { Name = key, ContentType = contentType, Body = key };
			var payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "acl", "private" },
					{ "Content-Type", contentType },
					{ "file", fileData },
			};
			sign.Apply(payload);

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
		public void TestPostObjectSetKeyFromFilename()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "TestPostObjectSetKeyFromFilename.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

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
		public void TestPostObjectIgnoredHeader()
		{
			TestId = 9;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectIgnoredHeader.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "x-ignore-foo" , "bar"},
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[헤더정보에 대소문자를 섞어서 사용할 경우] " +
									 "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectCaseInsensitiveConditionFields()
		{
			TestId = 10;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectCaseInsensitiveConditionFields.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bUcKeT", bucketName } } },
						{ new JArray() { "StArTs-WiTh", "$KeY", Key } },
						{ new JObject() { { "AcL", "private" } } },
						{ new JArray() { "StArTs-WiTh", "$CoNtEnT-TyPe", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "kEy", Key },
					{ "aCl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.NoContent, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[오브젝트 이름에 '\'를 사용할 경우] " +
									 "post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectEscapedFieldValues()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectEscapedFieldValues.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

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
		public void TestPostObjectSuccessRedirectAction()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

			var ContentType = "text/plain";
			var Key = "TestPostObjectSuccessRedirectAction.txt";
			var RedirectURL = GetURL(bucketName);

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "eq", "$success_action_redirect", RedirectURL } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "success_action_redirect" , RedirectURL },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.OK, Result.StatusCode, Result.Message);

			var Response = client.GetObject(bucketName, Key);
			// S3는 경로가 없는 리다이렉트 URL에 '/'를 붙여 정규화하므로 Uri로 맞춰서 비교한다.
			var expectedUrl = new Uri(string.Format("{0}?bucket={1}&key={2}&etag=%22{3}%22",
				RedirectURL, bucketName, Key, Response.ETag.Replace("\"", "")));
			Assert.Equal(expectedUrl, new Uri(Result.URL));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectInvalidSignature()
		{
			TestId = 13;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectInvalidSignature.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);
			// 정상 서명 후 서명값 끝 한 글자를 잘라 무효화한다.
			Payload["x-amz-signature"] = Sign.Signature[0..^1];

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AccessKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectInvalidAccessKey()
		{
			TestId = 14;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectInvalidAccessKey.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);
			// 정상 서명 후 credential의 AccessKey 끝 한 글자를 잘라 무효화한다.
			Payload["x-amz-credential"] = Sign.Credential.Replace(Config.MainUser.AccessKey, Config.MainUser.AccessKey[0..^1]);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectInvalidDateFormat()
		{
			TestId = 15;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectInvalidDateFormat.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.Now.AddMinutes(100) },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectNoKeySpecified()
		{
			TestId = 16;
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

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = "", ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectMissingSignature()
		{
			TestId = 17;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectMissingSignature.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);
			// 서명 필드를 누락시킨다.
			Payload.Remove("x-amz-signature");

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectMissingPolicyCondition()
		{
			TestId = 18;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectMissingPolicyCondition.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "[사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectUserSpecifiedHeader()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var ContentType = "text/plain";
			var Key = "TestPostObjectUserSpecifiedHeader.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "starts-with", "$x-amz-meta-foo", "bar" } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "x-amz-meta-foo" , "barclamp" },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

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
		public void TestPostObjectRequestMissingPolicySpecifiedField()
		{
			TestId = 20;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectRequestMissingPolicySpecifiedField.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "eq", "$x-amz-meta-foo", "" } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectConditionIsCaseSensitive()
		{
			TestId = 21;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectConditionIsCaseSensitive.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "CONDITIONS", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectExpiresIsCaseSensitive()
		{
			TestId = 22;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectExpiresIsCaseSensitive.txt";

			var PolicyDocument = new JObject()
			{
				{"EXPIRATION", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectExpiredPolicy()
		{
			TestId = 23;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectExpiredPolicy.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(-100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectInvalidRequestFieldValue()
		{
			TestId = 24;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectInvalidRequestFieldValue.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
						{ new JArray() { "eq", "$x-amz-meta-foo", "" } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "x-amz-meta-foo" , "barclamp" },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectMissingExpiresCondition()
		{
			TestId = 25;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectMissingExpiresCondition.txt";

			var PolicyDocument = new JObject()
			{
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectMissingConditionsList()
		{
			TestId = 26;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectMissingConditionsList.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectUploadSizeLimitExceeded()
		{
			TestId = 27;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectUploadSizeLimitExceeded.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0, 0 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectMissingContentLengthArgument()
		{
			TestId = 28;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectMissingContentLengthArgument.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 0 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력)] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectInvalidContentLengthArgument()
		{
			TestId = 29;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectInvalidContentLengthArgument.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", -1, 0 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectUploadSizeBelowMinimum()
		{
			TestId = 30;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectUploadSizeBelowMinimum.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 512, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.BadRequest, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectEmptyConditions()
		{
			TestId = 31;
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "TestPostObjectEmptyConditions.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			// 조건 목록이 비어 있으면 정책이 어떤 폼 필드도 허용하지 않으므로
			// S3는 "Extra input fields"로 403을 반환한다.
			var Result = PostUpload(bucketName, Payload);
			AssertX.Equal(HttpStatusCode.Forbidden, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[policy에 명시된 버킷과 다른 버킷으로 업로드할 경우] post 방식으로 파일 업로드시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectWrongBucket()
		{
			TestId = 36;
			var bucketName = GetNewBucketName();
			var badBucketName = GetNewBucketName();

			var ContentType = "text/plain";
			var Key = "\\$TestPostObjectWrongBucket.txt";

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", bucketName } } },
						{ new JArray() { "starts-with", "$key", Key } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "content-length-range", 512, 1024 } },
					}
				},
			};

			var Sign = SignPostPolicy(PolicyDocument);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "bucket", bucketName },
					{ "acl", "private" },
					{ "Content-Type", ContentType },
					{ "file", FileData },
			};
			Sign.Apply(Payload);

			// policy에 적힌 버킷이 아니라 존재하지 않는 버킷으로 업로드한다.
			var Result = PostUpload(badBucketName, Payload);
			AssertX.Equal(HttpStatusCode.NotFound, Result.StatusCode, Result.Message);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPresignedUrlPutGet()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "TestPresignedUrlPutGet";

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
		public void TestPutObjectV4()
		{
			TestId = 33;
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "TestPutObjectV4";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			var client = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey, Config.S3.RegionName);

			var Response = client.PutObject(Key, Content, ContentType: ContentType);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "signV4")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectChunkedV4()
		{
			TestId = 34;
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "TestPutObjectChunkedV4";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			var client = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey, Config.S3.RegionName);

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
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "TestGetObjectV4";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			client.PutObject(bucketName, Key, Content);

			var MyClient = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey, Config.S3.RegionName);
			var Response = MyClient.GetObject(Key, out string Body);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
			Assert.Equal(Size, Body.Length);
			Assert.Equal(Content, Body);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "PresignedURL에 서명된 x-amz-acl 헤더를 그대로 보내면 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPresignedUrlPutSignedAcl()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "TestPresignedUrlPutSignedAcl";

			var putURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddMinutes(10), HttpVerb.PUT,
				acl: S3CannedACL.PublicRead);
			var headers = new Dictionary<string, string> { ["x-amz-acl"] = "public-read" };
			var response = PutObject(putURL, key, headers: headers);
			var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Assert.True(HttpStatusCode.OK == response.StatusCode, body);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "PresignedURL에 서명되지 않은 x-amz-acl 헤더를 붙이면 AccessDenied로 거부 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPresignedUrlPutUnsignedAcl()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "TestPresignedUrlPutUnsignedAcl";

			var putURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddMinutes(10), HttpVerb.PUT);
			var headers = new Dictionary<string, string> { ["x-amz-acl"] = "public-read" };
			var response = PutObject(putURL, key, headers: headers);
			var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Assert.True(HttpStatusCode.Forbidden == response.StatusCode, body);
			Assert.Contains(MainData.ACCESS_DENIED, body);

			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "PresignedURL(PUT)에 서명되지 않은 x-amz-copy-source 헤더를 붙이면 AccessDenied로 거부 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPresignedUrlPutUnsignedCopySource()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var sourceKey = "source";
			var targetKey = "target";

			client.PutObject(bucketName, key: sourceKey, body: sourceKey);

			var putURL = client.GeneratePresignedURL(bucketName, targetKey, DateTime.Now.AddMinutes(10), HttpVerb.PUT);
			var headers = new Dictionary<string, string> { ["x-amz-copy-source"] = $"{bucketName}/{sourceKey}" };
			// 본문이 없으면 서명 검사 전에 MissingContentLength로 막히는 구현이 있어 빈 본문을 보낸다.
			var response = PutObject(putURL, "", headers: headers);
			var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Assert.True(HttpStatusCode.Forbidden == response.StatusCode, body);
			Assert.Contains(MainData.ACCESS_DENIED, body);

			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, targetKey));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}

		#region SigV4 POST (AWS 실측 기준)

		private static string PostExpiration(int minutes) => DateTime.UtcNow.AddMinutes(minutes).ToString("yyyy-MM-ddTHH:mm:ssZ");

		/// <summary>폼 필드(key 제외)를 그대로 정책 조건으로 만든다.</summary>
		private static JArray FieldConditions(Dictionary<string, object> fields)
		{
			var conditions = new JArray();
			foreach (var field in fields)
				if (field.Key != "key") conditions.Add(new JObject() { { field.Key, field.Value.ToString() } });
			return conditions;
		}

		/// <summary>
		/// SigV4 로 서명한 폼 업로드. 날짜·자격 증명은 정책과 폼에 같은 값을 쓴다.
		/// </summary>
		/// <param name="policy">null 이면 bucket, key(아무 값), x-amz-* 조건에 conditions 를 더한 정책을 만든다. 함수면 기본 조건 목록을 받아 정책 문서를 돌려준다.</param>
		/// <param name="rawPolicy">null 이 아니면 그대로 policy 필드에 넣는다.</param>
		/// <param name="mutate">서명까지 끝난 폼 필드를 고치는 함수.</param>
		private MyResult PostForm(string bucketName, Dictionary<string, object> fields, JArray conditions = null, string body = "bar",
			UserData user = null, Func<List<JToken>, JObject> policy = null, string rawPolicy = null,
			Action<Dictionary<string, object>> mutate = null)
		{
			user ??= Config.MainUser;
			var amzDate = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ");
			var dateStamp = amzDate[..8];
			var region = string.IsNullOrWhiteSpace(Config.S3.RegionName) ? "us-east-1" : Config.S3.RegionName;
			var credential = $"{user.AccessKey}/{dateStamp}/{region}/s3/aws4_request";
			var baseConditions = new List<JToken>()
			{
				new JObject() { { "bucket", bucketName } },
				new JArray() { "starts-with", "$key", "" },
				new JObject() { { "x-amz-algorithm", PostPolicyV4.Algorithm } },
				new JObject() { { "x-amz-credential", credential } },
				new JObject() { { "x-amz-date", amzDate } },
			};

			var encoded = rawPolicy;
			if (encoded == null)
			{
				JObject document;
				if (policy == null)
				{
					var all = new JArray(baseConditions);
					if (conditions != null)
						foreach (var condition in conditions) all.Add(condition);
					document = new JObject() { { "expiration", PostExpiration(100) }, { "conditions", all } };
				}
				else
					document = policy(baseConditions);
				encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(document.ToString()));
			}

			var payload = new Dictionary<string, object>(fields)
			{
				["policy"] = encoded,
				["x-amz-algorithm"] = PostPolicyV4.Algorithm,
				["x-amz-credential"] = credential,
				["x-amz-date"] = amzDate,
				["x-amz-signature"] = S3Utils.GetPostPolicySignature(user.SecretKey, dateStamp, region, encoded),
			};
			mutate?.Invoke(payload);
			payload["file"] = new FormFile() { Name = "foo.txt", ContentType = "text/plain", Body = body };
			return PostUpload(bucketName, payload);
		}

		private static void AssertPostError(MyResult result, HttpStatusCode statusCode, string errorCode)
		{
			AssertX.Equal(statusCode, result.StatusCode, result.Message);
			AssertX.Equal(errorCode, result.ErrorCode, result.Message);
		}

		private static void AssertPostSuccess(MyResult result)
			=> AssertX.Equal(HttpStatusCode.NoContent, result.StatusCode, result.Message);

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 올릴 때 폼 필드로 준 헤더·메타데이터·암호화·태그가 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectV4StoredFields()
		{
			TestId = 37;
			var key = "TestPostObjectV4StoredFields";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var fields = new Dictionary<string, object>()
			{
				{ "key", key },
				{ "Content-Type", "image/png" },
				{ "Cache-Control", "max-age=60" },
				{ "Content-Disposition", "attachment; filename=a.txt" },
				{ "Content-Encoding", "identity" },
				{ "x-amz-meta-foo", "bar" },
				{ "x-amz-server-side-encryption", "AES256" },
				{ "x-amz-website-redirect-location", "/other.txt" },
				{ "tagging", "<Tagging><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag></TagSet></Tagging>" },
			};
			AssertPostSuccess(PostForm(bucketName, fields, FieldConditions(fields)));

			var response = client.HeadObject(bucketName, key);
			Assert.Equal(3, response.ContentLength);
			Assert.Equal("image/png", response.Headers.ContentType);
			Assert.Equal("max-age=60", response.Headers.CacheControl);
			Assert.Equal("attachment; filename=a.txt", response.Headers.ContentDisposition);
			Assert.Equal("identity", response.Headers.ContentEncoding);
			Assert.Single(response.Metadata.Keys);
			Assert.Equal("bar", response.Metadata["x-amz-meta-foo"]);
			Assert.Equal(ServerSideEncryptionMethod.AES256, response.ServerSideEncryptionMethod);
			Assert.Equal("/other.txt", response.WebsiteRedirectLocation);
			var tagSet = client.GetObjectTagging(bucketName, key).Tagging;
			Assert.Single(tagSet);
			Assert.Equal("k1", tagSet[0].Key);
			Assert.Equal("v1", tagSet[0].Value);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Upload")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 같은 키에 다시 올리면 덮어쓰고, 빈 파일도 올릴 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectV4Overwrite()
		{
			TestId = 38;
			var key = "TestPostObjectV4Overwrite";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertPostSuccess(PostForm(bucketName, new() { { "key", key } }, body: "first"));
			AssertPostSuccess(PostForm(bucketName, new() { { "key", key } }, body: "second"));
			Assert.Equal("second", S3Utils.GetBody(client.GetObject(bucketName, key)));
			// 빈 파일도 올릴 수 있다
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "empty" } }, body: ""));
			Assert.Equal(0, client.HeadObject(bucketName, "empty").ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 버전 관리 버킷에 post 방식으로 같은 키를 두 번 올리면 버전이 둘 생기는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectV4Versioning()
		{
			TestId = 39;
			var key = "TestPostObjectV4Versioning";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			AssertPostSuccess(PostForm(bucketName, new() { { "key", key } }, body: "first"));
			AssertPostSuccess(PostForm(bucketName, new() { { "key", key } }, body: "second"));
			var versions = client.ListVersions(bucketName).Versions;
			Assert.Equal(2, versions.Count);
			Assert.Equal("second", S3Utils.GetBody(client.GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 정책 조건에 맞는 폼은 post 방식 업로드에 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPostObjectV4ConditionMatched()
		{
			TestId = 40;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 접두어가 맞는 키
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "user/a.txt" } },
				[new JArray() { "starts-with", "$key", "user/" }]));
			// 빈 접두어는 아무 값이나 받는다
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "any" }, { "x-amz-meta-foo", "anything" } },
				[new JArray() { "starts-with", "$x-amz-meta-foo", "" }]));
			// 조건의 필드 이름은 대소문자를 구분하지 않는다
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "case" }, { "x-amz-meta-foo", "value" } },
				[new JArray() { "starts-with", "$X-Amz-Meta-Foo", "" }]));
			// x-ignore- 로 시작하는 필드는 조건이 없어도 된다
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "ignore" }, { "x-ignore-foo", "bar" } }));
			Assert.Equal(4, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 정책 조건에 맞지 않는 폼은 post 방식 업로드가 AccessDenied 로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4ConditionFailed()
		{
			TestId = 41;
			var key = "TestPostObjectV4ConditionFailed";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var cases = new List<(Dictionary<string, object> Fields, JArray Conditions)>()
			{
				// (폼 필드, 추가 조건)
				(new() { { "key", "other/a.txt" } }, [new JArray() { "starts-with", "$key", "user/" }]),
				(new() { { "key", key }, { "Content-Type", "text/html" } }, [new JArray() { "eq", "$Content-Type", "text/plain" }]),
				(new() { { "key", key }, { "x-amz-meta-foo", "2" } }, [new JObject() { { "x-amz-meta-foo", "1" } }]),
				// eq 는 값의 대소문자를 구분한다
				(new() { { "key", key }, { "x-amz-meta-foo", "ABC" } }, [new JArray() { "eq", "$x-amz-meta-foo", "abc" }]),
				// 정책에 있는 필드가 폼에 없다
				(new() { { "key", key } }, [new JObject() { { "x-amz-meta-foo", "bar" } }]),
				// 폼에 있는 필드가 정책에 없다
				(new() { { "key", key }, { "x-amz-meta-foo", "bar" } }, []),
			};
			foreach (var (fields, conditions) in cases)
				AssertPostError(PostForm(bucketName, fields, conditions), HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 정책에 필수 조건이 하나라도 없거나 버킷 이름이 다르면 post 방식 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4RequiredConditions()
		{
			TestId = 42;
			var key = "TestPostObjectV4RequiredConditions";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var expiration = PostExpiration(100);

			Func<List<JToken>, JObject> Without(int index) => conditions => new JObject()
			{
				{ "expiration", expiration },
				{ "conditions", new JArray(conditions.Where((_, i) => i != index)) },
			};

			// bucket, key, x-amz-algorithm, x-amz-credential, x-amz-date 조건은 모두 있어야 한다
			for (var index = 0; index < 5; index++)
				AssertPostError(PostForm(bucketName, new() { { "key", key } }, policy: Without(index)),
					HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			// 다른 버킷 이름을 적은 정책
			JObject Other(List<JToken> conditions)
			{
				var all = new JArray() { new JObject() { { "bucket", "other-bucket-name" } } };
				foreach (var condition in conditions.Skip(1)) all.Add(condition);
				return new JObject() { { "expiration", expiration }, { "conditions", all } };
			}
			AssertPostError(PostForm(bucketName, new() { { "key", key } }, policy: Other), HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 만료된 정책으로 post 방식 업로드가 AccessDenied 로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4ExpiredPolicy()
		{
			TestId = 43;
			var key = "TestPostObjectV4ExpiredPolicy";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			JObject Expired(List<JToken> conditions)
				=> new() { { "expiration", PostExpiration(-10) }, { "conditions", new JArray(conditions) } };
			AssertPostError(PostForm(bucketName, new() { { "key", key } }, policy: Expired), HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 형식이 잘못된 정책 문서로 post 방식 업로드가 InvalidPolicyDocument 로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4InvalidPolicyDocument()
		{
			TestId = 44;
			var key = "TestPostObjectV4InvalidPolicyDocument";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var expiration = PostExpiration(100);

			static JArray With(List<JToken> conditions, JToken extra)
			{
				var all = new JArray(conditions);
				all.Add(extra);
				return all;
			}

			var rawPolicies = new List<string>()
			{
				"!!!not-base64!!!",
				Convert.ToBase64String(Encoding.ASCII.GetBytes("not json")),
			};
			var policies = new List<Func<List<JToken>, JObject>>()
			{
				conditions => new() { { "conditions", new JArray(conditions) } }, // expiration 누락
				conditions => new() { { "expiration", expiration } }, // conditions 누락
				conditions => new() { { "expiration", expiration }, { "conditions", new JArray() } },
				conditions => new() { { "expiration", "2030-01-01 00:00:00" }, { "conditions", new JArray(conditions) } }, // ISO8601 이 아니다
				conditions => new() { { "expiration", expiration }, { "conditions", new JArray(conditions) }, { "unknown", "value" } },
				conditions => new() { { "EXPIRATION", expiration }, { "CONDITIONS", new JArray(conditions) } }, // 이름은 소문자여야 한다
				// 조건 형식이 잘못됐다
				conditions => new() { { "expiration", expiration }, { "conditions", With(conditions, new JArray() { "ends-with", "$key", "txt" }) } },
				conditions => new() { { "expiration", expiration }, { "conditions", With(conditions, new JArray() { "eq", "$key" }) } },
				conditions => new() { { "expiration", expiration }, { "conditions", With(conditions, new JArray() { "content-length-range", 0 }) } },
			};
			foreach (var rawPolicy in rawPolicies)
				AssertPostError(PostForm(bucketName, new() { { "key", key } }, rawPolicy: rawPolicy),
					HttpStatusCode.BadRequest, MainData.INVALID_POLICY_DOCUMENT);
			foreach (var policy in policies)
				AssertPostError(PostForm(bucketName, new() { { "key", key } }, policy: policy),
					HttpStatusCode.BadRequest, MainData.INVALID_POLICY_DOCUMENT);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[SignatureVersion4] content-length-range 조건이 양 끝을 포함하고 범위를 벗어나면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4ContentLengthRange()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 본문은 3바이트. 범위는 양 끝을 포함한다
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "max" } }, [new JArray() { "content-length-range", 0, 3 }]));
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "min" } }, [new JArray() { "content-length-range", 3, 10 }]));
			AssertPostError(PostForm(bucketName, new() { { "key", "large" } }, [new JArray() { "content-length-range", 0, 2 }]),
				HttpStatusCode.BadRequest, MainData.ENTITY_TOO_LARGE);
			AssertPostError(PostForm(bucketName, new() { { "key", "small" } }, [new JArray() { "content-length-range", 4, 10 }]),
				HttpStatusCode.BadRequest, MainData.ENTITY_TOO_SMALL);
			Assert.Equal(new List<string>() { "max", "min" }, GetKeys(client.ListObjectsV2(bucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Auth")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 서명이나 액세스 키가 틀리면 post 방식 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4InvalidSignature()
		{
			TestId = 46;
			var key = "TestPostObjectV4InvalidSignature";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			static void BadAccessKey(Dictionary<string, object> payload)
			{
				var scope = payload["x-amz-credential"].ToString().Split('/', 2)[1];
				payload["x-amz-credential"] = $"AKIAXXXXXXXXXXXXXXXX/{scope}";
			}

			AssertPostError(PostForm(bucketName, new() { { "key", key } }, mutate: payload => payload["x-amz-signature"] = new string('0', 64)),
				HttpStatusCode.Forbidden, MainData.SIGNATURE_DOES_NOT_MATCH);
			AssertPostError(PostForm(bucketName, new() { { "key", key } }, mutate: BadAccessKey),
				HttpStatusCode.Forbidden, MainData.INVALID_ACCESS_KEY_ID);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Auth")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 서명 필드가 없거나 값이 잘못되면 post 방식 업로드가 InvalidArgument 로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4InvalidAuthField()
		{
			TestId = 47;
			var key = "TestPostObjectV4InvalidAuthField";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			static Action<Dictionary<string, object>> Remove(string name) => payload => payload.Remove(name);
			static Action<Dictionary<string, object>> SetField(string name, string value) => payload => payload[name] = value;
			static void WrongService(Dictionary<string, object> payload)
				=> payload["x-amz-credential"] = payload["x-amz-credential"].ToString().Replace("/s3/aws4_request", "/ec2/aws4_request");

			var mutations = new List<Action<Dictionary<string, object>>>()
			{
				// 서명 필드가 하나라도 없다
				Remove("x-amz-signature"),
				Remove("x-amz-date"),
				Remove("x-amz-algorithm"),
				Remove("x-amz-credential"),
				// 값이 잘못됐다
				SetField("x-amz-algorithm", "AWS4-HMAC-SHA1"),
				SetField("x-amz-date", "2030-01-01"),
				SetField("x-amz-credential", "malformed"),
				WrongService,
			};
			foreach (var mutate in mutations)
				AssertPostError(PostForm(bucketName, new() { { "key", key } }, mutate: mutate),
					HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 쓰기 권한이 없는 사용자의 폼과 서명 없는 폼은 post 방식 업로드가 AccessDenied 로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4AccessDenied()
		{
			TestId = 48;
			var key = "TestPostObjectV4AccessDenied";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 쓰기 권한이 없는 사용자가 서명한 폼
			AssertPostError(PostForm(bucketName, new() { { "key", key } }, user: Config.AltUser),
				HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			// 서명 없는 폼
			var payload = new Dictionary<string, object>()
			{
				{ "key", key },
				{ "file", new FormFile() { Name = "foo.txt", ContentType = "text/plain", Body = "bar" } },
			};
			AssertPostError(PostUpload(bucketName, payload), HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[SignatureVersion4] key 필드가 없거나 비었거나 1024바이트를 넘으면 post 방식 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4InvalidKey()
		{
			TestId = 49;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertPostError(PostForm(bucketName, []), HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT);
			AssertPostError(PostForm(bucketName, new() { { "key", "" } }), HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT);
			// 키는 1024바이트까지
			AssertPostError(PostForm(bucketName, new() { { "key", new string('k', 1025) } }), HttpStatusCode.BadRequest, MainData.KEY_TOO_LONG);
			AssertPostSuccess(PostForm(bucketName, new() { { "key", new string('k', 1024) } }));
			Assert.Equal(1, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 폼 필드 값이 잘못되면 post 방식 업로드가 필드별 에러 코드로 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4InvalidFieldValue()
		{
			TestId = 50;
			var key = "TestPostObjectV4InvalidFieldValue";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var duplicatedTag = "<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>"
				+ "<Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>";
			var cases = new List<(string Name, string Value, string ErrorCode)>()
			{
				("acl", "invalid", MainData.INVALID_ARGUMENT),
				("x-amz-server-side-encryption", "AES128", MainData.INVALID_ARGUMENT),
				("tagging", "<Tagging>", MainData.MALFORMED_XML),
				("tagging", duplicatedTag, MainData.INVALID_TAG),
				("x-amz-website-redirect-location", "invalid", MainData.INVALID_REDIRECT_LOCATION),
				("x-amz-meta-big", new string('v', 2100), MainData.METADATA_TOO_LARGE),
			};
			foreach (var (name, value, errorCode) in cases)
			{
				var result = PostForm(bucketName, new() { { "key", key }, { name, value } },
					[new JArray() { "starts-with", $"${name}", "" }]);
				AssertPostError(result, HttpStatusCode.BadRequest, errorCode);
			}
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Checksum")]
		[Trait(MainData.Explanation, "[SignatureVersion4] Content-MD5 필드가 본문과 맞지 않거나 형식이 틀리면 post 방식 업로드가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4ContentMd5()
		{
			TestId = 51;
			var key = "TestPostObjectV4ContentMd5";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			MyResult Post(string md5) => PostForm(bucketName, new() { { "key", key }, { "Content-MD5", md5 } },
				[new JObject() { { "Content-MD5", md5 } }]);

			AssertPostError(Post(S3Utils.GetMD5("other")), HttpStatusCode.BadRequest, MainData.BAD_DIGEST);
			AssertPostError(Post("invalid"), HttpStatusCode.BadRequest, MainData.INVALID_DIGEST);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
			AssertPostSuccess(Post(S3Utils.GetMD5("bar")));
			Assert.Equal("bar", S3Utils.GetBody(client.GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "Checksum")]
		[Trait(MainData.Explanation, "[SignatureVersion4] x-amz-checksum-sha256 필드가 본문과 맞지 않으면 실패하고 맞으면 체크섬이 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4Checksum()
		{
			TestId = 52;
			var key = "TestPostObjectV4Checksum";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			MyResult Post(string checksum)
			{
				var fields = new Dictionary<string, object>()
				{
					{ "key", key },
					{ "x-amz-checksum-algorithm", "SHA256" },
					{ "x-amz-checksum-sha256", checksum },
				};
				return PostForm(bucketName, fields, FieldConditions(fields));
			}

			static string Sha256(string value) => Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

			AssertPostError(Post(Sha256("other")), HttpStatusCode.BadRequest, MainData.BAD_DIGEST);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
			AssertPostSuccess(Post(Sha256("bar")));
			var response = client.HeadObject(bucketName, key, checksumMode: ChecksumMode.ENABLED);
			Assert.Equal(Sha256("bar"), response.ChecksumSHA256);
		}

		[Fact]
		[Trait(MainData.Major, "Post")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "[SignatureVersion4] acl 필드가 오브젝트 ACL 이 되고, ACL 을 끈 버킷은 소유자 전권을 뜻하는 값만 받는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPostObjectV4Acl()
		{
			TestId = 53;
			var client = GetClient();
			// ACL 을 쓰는 버킷: acl 필드가 오브젝트 ACL 이 된다
			var bucketName = GetNewBucketCannedAcl(client);
			AssertPostSuccess(PostForm(bucketName, new() { { "key", "public" }, { "acl", "public-read" } },
				[new JObject() { { "acl", "public-read" } }]));
			var grants = client.GetObjectACL(bucketName, "public").AccessControlList.Grants
				.Select(g => (Target: g.Grantee.URI ?? "", Permission: g.Permission.Value))
				.OrderBy(g => g.Target, StringComparer.Ordinal).ThenBy(g => g.Permission, StringComparer.Ordinal).ToList();
			Assert.Equal(new List<(string, string)>()
			{
				("", "FULL_CONTROL"),
				("http://acs.amazonaws.com/groups/global/AllUsers", "READ"),
			}, grants);

			// ACL 을 끈 버킷: 소유자 전권을 뜻하는 값만 받는다
			var enforced = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			foreach (var acl in new[] { "private", "bucket-owner-full-control" })
				AssertPostSuccess(PostForm(enforced, new() { { "key", acl }, { "acl", acl } }, [new JObject() { { "acl", acl } }]));
			AssertPostError(PostForm(enforced, new() { { "key", "public" }, { "acl", "public-read" } },
				[new JObject() { { "acl", "public-read" } }]), HttpStatusCode.BadRequest, MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED);
			Assert.Equal(2, client.ListObjectsV2(enforced).KeyCount);
		}

		#endregion
	}
}
