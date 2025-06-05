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
using System;
using System.Net;
using Xunit;

namespace s3tests
{
	public class ACL : TestBase
	{
		public ACL(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawGet()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var unauthenticatedClient = GetUnauthenticatedClient();
			var response = unauthenticatedClient.GetObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
									 "권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawGetBucketGone()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			client.DeleteObject(bucketName, key);
			client.DeleteBucket(bucketName);

			var unauthenticatedClient = GetUnauthenticatedClient();
			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.GetObject(bucketName, key));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트를 삭제할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectDeleteKeyBucketGone()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			client.DeleteObject(bucketName, key);
			client.DeleteBucket(bucketName);

			var unauthenticatedClient = GetUnauthenticatedClient();
			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.DeleteObject(bucketName, key));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
									 "권한없는 사용자가 삭제된 오브젝트에 접근할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawGetObjectGone()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			client.DeleteObject(bucketName, key);

			var unauthenticatedClient = GetUnauthenticatedClient();
			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.GetObject(bucketName, key));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] " +
									 "권한없는 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawGetBucketAcl()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, key);

			var unauthenticatedClient = GetUnauthenticatedClient();
			var response = unauthenticatedClient.GetObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = private] " +
									 "권한없는 사용자가 공용버킷의 개인 오브젝트에 접근할때 에러확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawGetObjectAcl()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.Private, key);

			var unauthenticatedClient = GetUnauthenticatedClient();
			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.GetObject(bucketName, key));

			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
									 "로그인한 사용자가 공용 버킷의 공용 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawAuthenticated()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);

			var client = GetClient();
			var response = client.GetObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "[Bucket_ACL = priavte, Object_ACL = priavte] " +
			"로그인한 사용자가 GetObject의 반환헤더값을 설정하고 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawResponseHeaders()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.Private, key);
			var client = GetClient();

			var response = client.GetObject(bucketName, key,
				responseCacheControl: "no-cache",
				responseContentDisposition: "bla",
				responseContentEncoding: "aaa",
				responseContentLanguage: "esperanto",
				responseContentType: "foo/bar",
				responseExpires: "123");


			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
			Assert.Equal("foo/bar", response.Headers.ContentType);
			Assert.Equal("bla", response.Headers.ContentDisposition);
			Assert.Equal("aaa", response.Headers.ContentEncoding);
			Assert.Equal("no-cache", response.Headers.CacheControl);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] " +
									 "로그인한 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawAuthenticatedBucketAcl()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, key);

			var client = GetClient();
			var response = client.GetObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = private] " +
									 "로그인한 사용자가 공용버킷의 개인 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawAuthenticatedObjectAcl()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.Private, key);

			var client = GetClient();
			var response = client.GetObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
									 "로그인한 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawAuthenticatedBucketGone()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			client.DeleteObject(bucketName, key);
			client.DeleteBucket(bucketName);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
									 "로그인한 사용자가 삭제된 오브젝트에 접근할때 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawAuthenticatedObjectGone()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			client.DeleteObject(bucketName, key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
			"로그인이 만료되지 않은 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectRawGetXAmzExpiresNotExpired()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			var url = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var response = GetObject(url);

			Assert.Equal(HttpStatusCode.OK, response.StatusCode);
			response.Close();
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
			"로그인이 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawGetXAmzExpiresOutRangeZero()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();
			var url = client.GeneratePresignedURL(bucketName, key, DateTime.MinValue, HttpVerb.GET);

			var e = Assert.Throws<WebException>(() => GetObject(url));
			var response = e.Response as HttpWebResponse;
			var Message = GetResponseErrorCode(response.GetResponseStream());
			Assert.Equal(MainData.AUTHORIZATION_QUERY_PARAMETERS_ERROR, Message);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "[Bucket_ACL = public-read, Object_ACL = public-read] " +
			"로그인 유효주기가 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawGetXAmzExpiresOutPositiveRange()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.PublicRead, S3CannedACL.PublicRead, key);
			var client = GetClient();

			var url = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddDays(-1), HttpVerb.GET);
			var e = Assert.Throws<WebException>(() => GetObject(url));
			var response = e.Response as HttpWebResponse;
			var Message = GetResponseErrorCode(response.GetResponseStream());
			Assert.Equal(MainData.AUTHORIZATION_QUERY_PARAMETERS_ERROR, Message);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Update")]
		[Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectAnonPut()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			client.PutObject(bucketName, key);

			var unauthenticatedClient = GetUnauthenticatedClient();

			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.PutObject(bucketName, key, body: "bar"));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Update")]
		[Trait(MainData.Explanation, "로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectUnauthenticatedPutWriteAccess()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			client.PutObject(bucketName, key);

			var unauthenticatedClient = GetUnauthenticatedClient();

			var e = Assert.Throws<AggregateException>(() => unauthenticatedClient.PutObject(bucketName, key, body: "bar"));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Default")]
		[Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPutAuthenticated()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.PutObject(bucketName, key: "foo", body: "foo");
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Default")]
		[Trait(MainData.Explanation, "[Bucket_ACL = Default, Object_ACL = Default] " +
									 "Post방식으로 만료된 로그인 정보를 설정하여 오브젝트 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectRawPutAuthenticatedExpired()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			client.PutObject(bucketName, key: key);

			var url = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddDays(-1), HttpVerb.PUT);
			var e = Assert.Throws<WebException>(() => PutObject(url));
			var response = e.Response as HttpWebResponse;
			var Message = GetResponseErrorCode(response.GetResponseStream());
			Assert.Equal(MainData.AUTHORIZATION_QUERY_PARAMETERS_ERROR, Message);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "[Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestAclPrivateBucketPublicReadObject()
		{
			var key = "foo";
			var bucketName = SetupBucketObjectACL(S3CannedACL.Private, S3CannedACL.PublicRead, key);

			var client = GetClient();
			TestACL(bucketName, key, client, true);

			var altClient = GetAltClient();
			TestACL(bucketName, key, altClient, true);

			var unauthenticatedClient = GetUnauthenticatedClient();
			TestACL(bucketName, key, unauthenticatedClient, true);
		}
	}
}
