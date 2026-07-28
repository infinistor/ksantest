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
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);

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
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

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
			var bucketName = GetNewBucket();

			var ContentType = "text/plain";
			var Key = "foo.txt";

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
			var bucketName = GetNewBucketName();
			var badBucketName = GetNewBucketName();

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
		[Trait(MainData.Minor, "signV4")]
		[Trait(MainData.Explanation, "[SignatureVersion4] post 방식으로 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectV4()
		{
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "foo";
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
			var bucketName = GetNewBucket();
			var ContentType = "text/plain";
			var Key = "foo";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key = "foo";
			var Size = 100;
			var Content = S3Utils.RandomTextToLong(Size);

			client.PutObject(bucketName, Key, Content);

			var MyClient = new MyHttpClient(GetURL(bucketName), Config.MainUser.AccessKey, Config.MainUser.SecretKey, Config.S3.RegionName);
			var Response = MyClient.GetObject(Key, out string Body);
			Assert.Equal(HttpStatusCode.OK, Response.StatusCode);
			Assert.Equal(Size, Body.Length);
			Assert.Equal(Content, Body);
		}
	}
}
