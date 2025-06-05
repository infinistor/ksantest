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
using Amazon.S3.Model;
using System;
using System.Net;
using System.Threading;
using Xunit;

namespace s3tests
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
			var body = GetBody(getResponse);
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
			var body = GetBody(getResponse);
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
			var body = GetBody(getResponse);
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
			var after = lastModified.AddSeconds(1);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, ifModifiedSinceDateTime: after));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
			Assert.Equal(HttpStatusCode.NotModified.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "IfUnmodifiedSince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(IfUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
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
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후으로 설정] " +
							"지정일(IfUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectIfUnmodifiedSinceFailed()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectIfUnmodifiedSinceFailed";

			client.PutObject(bucketName, key, body: key);

			var response = client.GetObject(bucketName, key, ifUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			var body = GetBody(response);
			Assert.Equal(key, body);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "지정한 범위로 오브젝트 다운로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedRequestResponseCode()
		{
			var key = "TestRangedRequestResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange(4, 7));

			var fetchedContent = GetBody(response);
			Assert.Equal(content.Substring(4, 4), fetchedContent);
			Assert.Equal("bytes 4-7/11", response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedBigRequestResponseCode()
		{
			var key = "TestRangedBigRequestResponseCode";
			var content = RandomTextToLong(8 * MainData.MB);

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange(3145728, 5242880));

			var fetchedContent = GetBody(response);
			Assert.Equal(content.Substring(3145728, 5242880 - 3145728 + 1), fetchedContent);
			Assert.Equal("bytes 3145728-5242880/8388608", response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedRequestSkipLeadingBytesResponseCode()
		{
			var key = "TestRangedRequestSkipLeadingBytesResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange("bytes=4-"));

			var fetchedContent = GetBody(response);
			Assert.Equal(content[4..], fetchedContent);
			Assert.Equal("bytes 4-10/11", response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangedRequestReturnTrailingBytesResponseCode()
		{
			var key = "TestRangedRequestReturnTrailingBytesResponseCode";
			var content = "testcontent";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var response = client.GetObject(bucketName, key, range: new ByteRange("bytes=-7"));

			var fetchedContent = GetBody(response);
			Assert.Equal(content.Substring(content.Length - 7, 7), fetchedContent);
			Assert.Equal("bytes 4-10/11", response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
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
		[Trait(MainData.Explanation, "비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestRangedRequestEmptyObject()
		{
			var key = "TestRangedRequestEmptyObject";
			var content = "";

			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: content);
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, range: new ByteRange(40, 50)));
			Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, GetStatus(e));
			Assert.Equal(MainData.INVALID_RANGE, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectMany()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestGetObjectMany";
			var data = RandomTextToLong(15 * 1024 * 1024);

			client.PutObject(bucketName, key, body: data);
			CheckContent(client, bucketName, key, data, loopCount: 100);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRangeObjectMany()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestRangeObjectMany";
			var size = 15 * 1024 * 1024;
			var data = RandomTextToLong(size);

			client.PutObject(bucketName, key, body: data);
			CheckContentUsingRandomRange(client, bucketName, key, data, 100);
		}

		[Fact]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Restore")]
		[Trait(MainData.Explanation, "오브젝트 복구 명령이 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestRestoreObject()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestRestoreObject";

			client.PutObject(bucketName, key, key);

			var response = client.RestoreObject(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}
	}
}
