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
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class GetObject : TestBase
	{
		public GetObject(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectReadNotExist()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectReadNotExist";

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		[Trait(MainData.Explanation, "존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectIfMatchGood()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfMatchGood";

			var putResponse = client.PutObject(bucketName, key, body: key);
			var eTag = putResponse.ETag;

			var getResponse = client.GetObject(bucketName, key, ifMatch: eTag);
			var body = S3Utils.GetBody(getResponse);
			Assert.Equal(key, body);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectIfMatchFailed()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfMatchFailed";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifMatch: "ABCORZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(HttpStatusCode.PreconditionFailed.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectIfNoneMatchGood()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfNoneMatchGood";

			var putResponse = client.PutObject(bucketName, key, body: key);
			var eTag = putResponse.ETag;

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
			Assert.Equal(HttpStatusCode.NotModified.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectIfNoneMatchFailed()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfNoneMatchFailed";

			client.PutObject(bucketName, key, body: key);

			var getResponse = client.GetObject(bucketName, key, ifNoneMatch: "ABCORZ");
			var body = S3Utils.GetBody(getResponse);
			Assert.Equal(key, body);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectIfModifiedSinceGood()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfModifiedSinceGood";

			client.PutObject(bucketName, key, body: key);

			var getResponse = client.GetObject(bucketName, key, ifModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			var body = S3Utils.GetBody(getResponse);
			Assert.Equal(key, body);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후로 설정] " +
							"지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectIfModifiedSinceFailed()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfModifiedSinceFailed";

			client.PutObject(bucketName, key, body: key);
			var response = client.GetObject(bucketName, key);
			var lastModified = response.LastModified;
			var after = lastModified?.AddSeconds(1);
			Delay(1000);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifModifiedSinceDateTime: after));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
			Assert.Equal(HttpStatusCode.NotModified.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestGetObjectIfMatchWithIfUnmodifiedSince()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfMatchWithIfUnmodifiedSince";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var response = client.GetObject(bucketName, key, ifMatch: eTag, ifUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			Assert.Equal("bar", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestGetObjectIfNoneMatchWithIfModifiedSince()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfNoneMatchWithIfModifiedSince";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifNoneMatch: eTag, ifModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestGetObjectIfMatchAndIfNoneMatch()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfMatchAndIfNoneMatch";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifMatch: eTag, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestGetObjectIfMatchAndIfNoneMatchAny()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfMatchAndIfNoneMatchAny";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifMatch: eTag, ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestHeadObjectIfMatchGood()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfMatchGood";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var response = client.HeadObject(bucketName, key, ifMatch: eTag);
			Assert.Equal(eTag, response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestHeadObjectIfMatchFailed()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfMatchFailed";

			client.PutObject(bucketName, key, body: "bar");
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestHeadObjectIfNoneMatchGood()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfNoneMatchGood";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestHeadObjectIfNoneMatchFailed()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfNoneMatchFailed";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifNoneMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		public void TestHeadObjectIfModifiedSinceGood()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfModifiedSinceGood";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		public void TestHeadObjectIfModifiedSinceFailed()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfModifiedSinceFailed";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key);
			var after = response.LastModified?.AddSeconds(1);
			Delay(1000);

			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifModifiedSinceDateTime: after));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		public void TestHeadObjectIfUnmodifiedSinceGood()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfUnmodifiedSinceGood";

			client.PutObject(bucketName, key, body: "bar");
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		public void TestHeadObjectIfUnmodifiedSinceFailed()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestHeadObjectIfUnmodifiedSinceFailed";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Header")]
		public void TestObjectResponseHeaders()
		{
			TestId = 30;
			var key = "TestObjectResponseHeaders";
			var client = GetClient();
			var bucketName = SetupObjects(new List<string> { key }, body: key);

			var response = client.GetObject(bucketName, key,
				responseCacheControl: "no-cache",
				responseContentDisposition: "bla",
				responseContentEncoding: "aaa",
				responseContentLanguage: "esperanto",
				responseContentType: "foo/bar",
				responseExpires: DateTime.UtcNow.ToString("R"));

			Assert.Equal("no-cache", response.Headers.CacheControl);
			Assert.Equal("bla", response.Headers.ContentDisposition);
			Assert.Equal("aaa", response.Headers.ContentEncoding);
			Assert.Equal("esperanto", response.Headers.ContentLanguage);
			Assert.Equal("foo/bar", response.Headers.ContentType);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestMultipartObjectRange()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartObjectRange";

			var uploadData = MultipartUpload(client, bucketName, key, 5 * MainData.MB, 5 * MainData.MB);
			var response = client.GetObject(bucketName, key, partNumber: 1);
			Assert.Equal(uploadData.Body[..(5 * MainData.MB)], S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		public void TestGetObjectIgnore()
		{
			TestId = 32;
			var key = "TestGetObjectIgnore";
			var client = GetClient();
			var bucketName = SetupObjects(new List<string> { key }, body: key);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(key.Length, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		public void TestGetObjectAfterDelete()
		{
			TestId = 33;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectAfterDelete";
			const string body = "testContent";

			client.PutObject(bucketName, key, body: body);
			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(body, S3Utils.GetBody(getResponse));
			Assert.Equal(body.Length, getResponse.ContentLength);

			client.DeleteObject(bucketName, key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		public void TestGetObjectAfterDeleteVersioning()
		{
			TestId = 34;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectAfterDeleteVersioning";
			const string body = "testContent";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: body);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(body, S3Utils.GetBody(getResponse));
			Assert.Equal(body.Length, getResponse.ContentLength);

			client.DeleteObject(bucketName, key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestGetObjectDeleteMarker()
		{
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectDeleteMarker";
			const string body = "testContent";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: body);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(body, S3Utils.GetBody(getResponse));

			client.DeleteObject(bucketName, key);

			var listResponse = client.ListVersions(bucketName);
			var deleteMarkers = GetDeleteMarkers(listResponse.Versions);
			var versions = GetVersions(listResponse.Versions);
			Assert.Single(deleteMarkers);
			Assert.Single(versions);

			var deleteMarker = deleteMarkers[0];
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, versionId: deleteMarker.VersionId));
			Assert.Equal(HttpStatusCode.MethodNotAllowed, GetStatus(e));
			Assert.Equal(MainData.METHOD_NOT_ALLOWED, GetErrorCode(e));

			var version = versions[0];
			var response = client.GetObject(bucketName, key, versionId: version.VersionId);
			Assert.Equal(body, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		public void TestGetObjectIfUnmodifiedSinceGood()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfUnmodifiedSinceGood";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(HttpStatusCode.PreconditionFailed.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		public void TestGetObjectIfUnmodifiedSinceFailed()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfUnmodifiedSinceFailed";

			client.PutObject(bucketName, key, body: key);

			var response = client.GetObject(bucketName, key, ifUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedRequestResponseCode()
		{
			TestId = 22;
			var key = "TestRangedRequestResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange(4, 7));

			Assert.Equal(content.Substring(4, 4), S3Utils.GetBody(response));
			Assert.Equal("bytes 4-7/11", response.ContentRange);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedBigRequestResponseCode()
		{
			TestId = 23;
			var key = "TestRangedBigRequestResponseCode";
			var content = S3Utils.RandomTextToLong(8 * MainData.MB);

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange(3145728, 5242880));

			Assert.Equal(content.Substring(3145728, 5242880 - 3145728 + 1), S3Utils.GetBody(response));
			Assert.Equal("bytes 3145728-5242880/8388608", response.ContentRange);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedRequestSkipLeadingBytesResponseCode()
		{
			TestId = 24;
			var key = "TestRangedRequestSkipLeadingBytesResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange("bytes=4-"));

			Assert.Equal(content[4..], S3Utils.GetBody(response));
			Assert.Equal("bytes 4-10/11", response.ContentRange);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedRequestReturnTrailingBytesResponseCode()
		{
			TestId = 25;
			var key = "TestRangedRequestReturnTrailingBytesResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange("bytes=-7"));

			Assert.Equal(content.Substring(content.Length - 7, 7), S3Utils.GetBody(response));
			Assert.Equal("bytes 4-10/11", response.ContentRange);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedRequestInvalidRange()
		{
			TestId = 26;
			var key = "TestRangedRequestInvalidRange";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, range: new ByteRange(40, 50)));
			Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, GetStatus(e));
			Assert.Equal(MainData.INVALID_RANGE, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		public void TestRangedRequestEmptyObject()
		{
			TestId = 27;
			var key = "TestRangedRequestEmptyObject";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: "");
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, range: new ByteRange(40, 50)));
			Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, GetStatus(e));
			Assert.Equal(MainData.INVALID_RANGE, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		public void TestGetObjectMany()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectMany";
			var data = S3Utils.RandomTextToLong(15 * 1024 * 1024);

			client.PutObject(bucketName, key, body: data);
			CheckContent(client, bucketName, key, data, loopCount: 100);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		public void TestRangeObjectMany()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestRangeObjectMany";
			var data = S3Utils.RandomTextToLong(15 * 1024 * 1024);

			client.PutObject(bucketName, key, body: data);
			CheckContentUsingRandomRange(client, bucketName, key, data, 100);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "모든 체크섬 알고리즘으로 업로드한 오브젝트의 Range 다운로드 내용 확인")]
		public void TestRangeGetChecksum()
		{
			TestId = 36;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var data = S3Utils.RandomTextToLong(5 * 1024 * 1024);

			foreach (var checksum in CheckSum.AllAlgorithms)
			{
				var key = $"TestRangeGetChecksum/{checksum.Value}";
				var response = client.PutObject(bucketName, key, body: data, checksumAlgorithm: checksum);

				ChecksumCompare(checksum, data, response);
				CheckContentUsingRandomRange(client, bucketName, key, data, 50);
			}
		}

		/// <summary>10바이트(0123456789) 오브젝트를 하나 넣은 버킷을 만든다. TestId를 먼저 대입해야 한다.</summary>
		private string PutDigits(S3Client client, string key)
		{
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, key, body: Digits);
			return bucketName;
		}

		private const string Digits = "0123456789";

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 버킷의 오브젝트를 조회하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectBucketNotExist()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.GetObject(bucketName, "key"));
			// HEAD 는 본문이 없어 상태 코드만 온다
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.HeadObject(bucketName, "key"));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 오브젝트에 HeadObject를 하면 404인지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestHeadObjectNotExist()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			AssertS3Error(HttpStatusCode.NotFound, null, () => client.HeadObject(bucketName, "key"));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "권한이 없는 사용자와 익명 사용자의 조회가 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAccessDenied()
		{
			TestId = 39;
			var key = "TestGetObjectAccessDenied";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			foreach (var otherClient in new[] { GetAltClient(), GetPublicClient() })
			{
				AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => otherClient.GetObject(bucketName, key));
				// 목록 권한이 없으면 없는 키도 404 가 아니라 403 이다
				AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED,
					() => otherClient.GetObject(bucketName, "not_exist"));
				AssertS3Error(HttpStatusCode.Forbidden, null, () => otherClient.HeadObject(bucketName, key));
			}
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "해석할 수 없는 Range는 무시하고 전체를 200으로 돌려주는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedRequestIgnored()
		{
			TestId = 40;
			var key = "TestRangedRequestIgnored";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 여러 구간, 잘못된 형식, 다른 단위, 뒤집힌 구간은 무시하고 전체를 200 으로 돌려준다
			foreach (var value in new[] { "bytes=0-1,3-4", "bytes=abc", "items=0-1", "bytes=5-2" })
			{
				var response = client.GetObject(bucketName, key, range: new ByteRange(value));
				Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
				Assert.True(string.IsNullOrEmpty(response.ContentRange));
				Assert.Equal(Digits, GetBody(response));
			}
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "Range가 오브젝트 끝을 넘으면 잘라서 돌려주고, 범위 밖이면 416인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedRequestBeyondEnd()
		{
			TestId = 41;
			var key = "TestRangedRequestBeyondEnd";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 끝이 크기를 넘으면 마지막 바이트까지로 잘린다
			var response = client.GetObject(bucketName, key, range: new ByteRange("bytes=5-100"));
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
			Assert.Equal("bytes 5-9/10", response.ContentRange);
			Assert.Equal("56789", GetBody(response));

			// 뒤에서부터의 길이가 크기를 넘으면 전체를 206 으로 돌려준다
			response = client.GetObject(bucketName, key, range: new ByteRange("bytes=-100"));
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
			Assert.Equal("bytes 0-9/10", response.ContentRange);
			Assert.Equal(Digits, GetBody(response));

			// 시작이 크기와 같거나, 뒤에서 0바이트를 요청하면 416
			foreach (var value in new[] { "bytes=10-20", "bytes=-0" })
				AssertS3Error(HttpStatusCode.RequestedRangeNotSatisfiable, MainData.INVALID_RANGE,
					() => client.GetObject(bucketName, key, range: new ByteRange(value)));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "HeadObject에 Range를 주면 206과 해당 구간 정보를 돌려주는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestHeadObjectRange()
		{
			TestId = 42;
			var key = "TestHeadObjectRange";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			var response = client.HeadObject(bucketName, key, range: "bytes=0-4");
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
			Assert.Equal(5, response.ContentLength);
			Assert.Equal("bytes 0-4/10", response.ContentRange);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "PartNumber")]
		[Trait(MainData.Explanation, "멀티파트가 아닌 오브젝트는 1번 파트가 전체이고 2번 파트는 없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectPartNumber()
		{
			TestId = 43;
			var key = "TestGetObjectPartNumber";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 멀티파트가 아닌 오브젝트는 1번 파트가 전체다
			var response = client.GetObject(bucketName, key, partNumber: 1);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
			Assert.Equal("bytes 0-9/10", response.ContentRange);
			Assert.Null(response.PartsCount);
			Assert.Equal(Digits, GetBody(response));

			var headResponse = client.HeadObject(bucketName, key, partNumber: 1);
			Assert.Equal(HttpStatusCode.PartialContent, headResponse.HttpStatusCode);
			Assert.Equal(10, headResponse.ContentLength);

			AssertS3Error(HttpStatusCode.RequestedRangeNotSatisfiable, MainData.INVALID_PART_NUMBER,
				() => client.GetObject(bucketName, key, partNumber: 2));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "PartNumber")]
		[Trait(MainData.Explanation, "Range와 함께 쓴 partNumber는 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectPartNumberInvalid()
		{
			TestId = 44;
			var key = "TestGetObjectPartNumberInvalid";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 파트 번호는 1~10000 이지만, .NET SDK가 범위를 벗어난 값(0, 10001)을 요청 전에 ArgumentException으로 막아 보낼 수 없다
			// Range 와 partNumber 는 함께 쓸 수 없다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST,
				() => client.GetObject(bucketName, key, partNumber: 1, range: new ByteRange("bytes=0-1")));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버저닝을 쓰지 않는 버킷에서 null 버전은 조회되고 잘못된 버전 ID는 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectVersionIdUnversioned()
		{
			TestId = 45;
			var key = "TestGetObjectVersionIdUnversioned";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
			Assert.Equal(Digits, GetBody(client.GetObject(bucketName, key, versionId: "null")));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.GetObject(bucketName, key, versionId: "invalid"));
			AssertS3Error(HttpStatusCode.BadRequest, null, () => client.HeadObject(bucketName, key, versionId: "invalid"));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectNullVersionNotExist()
		{
			TestId = 46;
			var key = "TestGetObjectNullVersionNotExist";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: "content");

			// 버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_VERSION,
				() => client.GetObject(bucketName, key, versionId: "null"));
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.HeadObject(bucketName, key, versionId: "null"));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "익명 요청에 response-* 재정의를 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectResponseHeadersAnonymous()
		{
			TestId = 47;
			var key = "TestGetObjectResponseHeadersAnonymous";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// 익명 요청에는 response-* 재정의를 쓸 수 없다
			// .NET SDK는 익명 자격 증명에 response-* 재정의를 주면 요청 전에 NullReferenceException이 나므로 서명 없는 HTTP 요청으로 보낸다
			using var response = GetObject(GetURL(bucketName, key) + "?response-content-type=text/html");
			Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
			Assert.Equal(MainData.INVALID_REQUEST, GetResponseErrorCode(response.Content.ReadAsStream()));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "HeadObject에서도 response-* 재정의가 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestHeadObjectResponseHeaders()
		{
			TestId = 48;
			var key = "TestHeadObjectResponseHeaders";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			var response = client.HeadObject(bucketName, key,
				responseContentType: "text/html",
				responseContentDisposition: "inline",
				responseCacheControl: "max-age=1",
				responseContentEncoding: "identity",
				responseContentLanguage: "en");
			Assert.Equal("text/html", response.Headers.ContentType);
			Assert.Equal("inline", response.Headers.ContentDisposition);
			Assert.Equal("max-age=1", response.Headers.CacheControl);
			Assert.Equal("identity", response.Headers.ContentEncoding);
			Assert.Equal("en", response.ContentLanguage);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "아무 설정 없이 올린 오브젝트의 기본 응답 헤더 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectDefaultHeaders()
		{
			TestId = 49;
			var key = "TestGetObjectDefaultHeaders";
			var client = GetClient();
			var bucketName = PutDigits(client, key);
			var eTag = "\"781e5e245d69b566979b86e28d23f2c7\"";

			// 기본 Content-Type(binary/octet-stream)은 .NET SDK가 PutObject에 항상 Content-Type을 붙여 보내므로 확인할 수 없다
			using (var response = client.GetObject(bucketName, key))
			{
				Assert.Equal("bytes", response.AcceptRanges);
				Assert.Equal(10, response.ContentLength);
				Assert.Equal(eTag, response.ETag);
				Assert.NotNull(response.LastModified);
			}

			var headResponse = client.HeadObject(bucketName, key);
			Assert.Equal("bytes", headResponse.AcceptRanges);
			Assert.Equal(10, headResponse.ContentLength);
			Assert.Equal(eTag, headResponse.ETag);
			Assert.NotNull(headResponse.LastModified);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "If-Match: * 는 통과하고 If-None-Match: * 는 304인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectIfMatchWildcard()
		{
			TestId = 50;
			var key = "TestGetObjectIfMatchWildcard";
			var client = GetClient();
			var bucketName = PutDigits(client, key);

			// If-Match: * 는 오브젝트가 있으면 통과, If-None-Match: * 는 304
			Assert.Equal(Digits, GetBody(client.GetObject(bucketName, key, ifMatch: "*")));
			AssertS3Error(HttpStatusCode.NotModified, null, () => client.GetObject(bucketName, key, ifNoneMatch: "*"));
		}
	}
}
