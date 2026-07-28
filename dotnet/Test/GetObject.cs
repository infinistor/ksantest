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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectIfMatchWithIfUnmodifiedSince";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var response = client.GetObject(bucketName, key, ifMatch: eTag, ifUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			Assert.Equal("bar", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestGetObjectIfNoneMatchWithIfModifiedSince()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectIfNoneMatchWithIfModifiedSince";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifNoneMatch: eTag, ifModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestGetObjectIfMatchAndIfNoneMatch()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectIfMatchAndIfNoneMatch";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifMatch: eTag, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestGetObjectIfMatchAndIfNoneMatchAny()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectIfMatchAndIfNoneMatchAny";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifMatch: eTag, ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestHeadObjectIfMatchGood()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfMatchGood";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var response = client.HeadObject(bucketName, key, ifMatch: eTag);
			Assert.Equal(eTag, response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		public void TestHeadObjectIfMatchFailed()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfMatchFailed";

			client.PutObject(bucketName, key, body: "bar");
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestHeadObjectIfNoneMatchGood()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfNoneMatchGood";

			var eTag = client.PutObject(bucketName, key, body: "bar").ETag;
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		public void TestHeadObjectIfNoneMatchFailed()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfNoneMatchFailed";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifNoneMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		public void TestHeadObjectIfModifiedSinceGood()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfModifiedSinceGood";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		public void TestHeadObjectIfModifiedSinceFailed()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfModifiedSinceFailed";

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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfUnmodifiedSinceGood";

			client.PutObject(bucketName, key, body: "bar");
			var e = Assert.Throws<AggregateException>(() => client.HeadObject(bucketName, key, ifUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		public void TestHeadObjectIfUnmodifiedSinceFailed()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testHeadObjectIfUnmodifiedSinceFailed";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.HeadObject(bucketName, key, ifUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			Assert.Equal(3, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Header")]
		public void TestObjectResponseHeaders()
		{
			var key = "testObjectResponseHeaders";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testMultipartObjectRange";

			var uploadData = MultipartUpload(client, bucketName, key, 5 * MainData.MB, 5 * MainData.MB);
			var response = client.GetObject(bucketName, key, partNumber: 1);
			Assert.Equal(uploadData.Body[..(5 * MainData.MB)], S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		public void TestGetObjectIgnore()
		{
			var key = "testObjectIgnore";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAfterDelete";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAfterDeleteVersioning";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectDeleteMarker";
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestRangeObjectMany";
			var data = S3Utils.RandomTextToLong(15 * 1024 * 1024);

			client.PutObject(bucketName, key, body: data);
			CheckContentUsingRandomRange(client, bucketName, key, data, 100);
		}

	}
}
