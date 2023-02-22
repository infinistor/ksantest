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

		[Fact(DisplayName = "test_object_read_not_exist")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_read_not_exist()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key: "bar"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_get_object_ifmatch_good")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		[Trait(MainData.Explanation, "존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifmatch_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			var PutResponse = Client.PutObject(BucketName, KeyName, Body: "bar");
			var ETag = PutResponse.ETag;

			var GetResponse = Client.GetObject(BucketName, KeyName, IfMatch: ETag);
			var Body = GetBody(GetResponse);
			Assert.Equal("bar", Body);
		}

		[Fact(DisplayName = "test_get_object_ifmatch_failed")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmatch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifmatch_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, KeyName, IfMatch: "ABCORZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(HttpStatusCode.PreconditionFailed.ToString(), GetErrorCode(e));
		}


		[Fact(DisplayName = "test_get_object_ifnonematch_good")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifnonematch_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			var PutResponse = Client.PutObject(BucketName, KeyName, Body: "bar");
			var ETag = PutResponse.ETag;

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, KeyName, IfNoneMatch: ETag));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
			Assert.Equal(HttpStatusCode.NotModified.ToString(), GetErrorCode(e));
		}

		[Fact(DisplayName = "test_get_object_ifnonematch_failed")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifnonematch")]
		[Trait(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifnonematch_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var GetResponse = Client.GetObject(BucketName, KeyName, IfNoneMatch: "ABCORZ");
			var Body = GetBody(GetResponse);
			Assert.Equal("bar", Body);
		}


		[Fact(DisplayName = "test_get_object_ifmodifiedsince_good")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifmodifiedsince_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var GetResponse = Client.GetObject(BucketName, KeyName, IfModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			var Body = GetBody(GetResponse);
			Assert.Equal("bar", Body);
		}

		[Fact(DisplayName = "test_get_object_ifmodifiedsince_failed")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후로 설정] " +
							"지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifmodifiedsince_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");
			var Response = Client.GetObject(BucketName, KeyName);
			var LastModified = Response.LastModified;
			var After = LastModified.AddSeconds(1);

			Thread.Sleep(1000);

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, KeyName, IfModifiedSinceDateTime: After));
			Assert.Equal(HttpStatusCode.NotModified, GetStatus(e));
			Assert.Equal(HttpStatusCode.NotModified.ToString(), GetErrorCode(e));
		}


		[Fact(DisplayName = "test_get_object_ifunmodifiedsince_good")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifunmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifunmodifiedsince_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, KeyName, IfUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(HttpStatusCode.PreconditionFailed.ToString(), GetErrorCode(e));
		}



		[Fact(DisplayName = "test_get_object_ifunmodifiedsince_failed")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Ifunmodifiedsince")]
		[Trait(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후으로 설정] " +
							"지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifunmodifiedsince_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var Response = Client.GetObject(BucketName, KeyName, IfUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			var Body = GetBody(Response);
			Assert.Equal("bar", Body);
		}

		[Fact(DisplayName = "test_ranged_request_response_code")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "지정한 범위로 오브젝트 다운로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange(4, 7));

			var FetchedContent = GetBody(Response);
			Assert.Equal(Content.Substring(4, 4), FetchedContent);
			Assert.Equal("bytes 4-7/11", Response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_ranged_big_request_response_code")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_big_request_response_code()
		{
			var Key = "testobj";
			var Content = RandomTextToLong(8 * MainData.MB);

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange(3145728, 5242880));

			var FetchedContent = GetBody(Response);
			Assert.Equal(Content.Substring(3145728, 5242880 - 3145728 + 1), FetchedContent);
			Assert.Equal("bytes 3145728-5242880/8388608", Response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_ranged_request_skip_leading_bytes_response_code")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_skip_leading_bytes_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange("bytes=4-"));

			var FetchedContent = GetBody(Response);
			Assert.Equal(Content[4..], FetchedContent);
			Assert.Equal("bytes 4-10/11", Response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_ranged_request_return_trailing_bytes_response_code")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_return_trailing_bytes_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange("bytes=-7"));

			var FetchedContent = GetBody(Response);
			Assert.Equal(Content.Substring(Content.Length - 7, 7), FetchedContent);
			Assert.Equal("bytes 4-10/11", Response.ContentRange);
			Assert.Equal(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_ranged_request_invalid_range")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_ranged_request_invalid_range()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key, Range: new ByteRange(40, 50)));
			Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, GetStatus(e));
			Assert.Equal(MainData.InvalidRange, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_ranged_request_empty_object")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_ranged_request_empty_object()
		{
			var Key = "testobj";
			var Content = "";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key, Range: new ByteRange(40, 50)));
			Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, GetStatus(e));
			Assert.Equal(MainData.InvalidRange, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_get_object_many")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			var Data = RandomTextToLong(15 * 1024 * 1024);

			Client.PutObject(BucketName, Key, Body: Data);
			CheckContent(Client, BucketName, Key, Data, LoopCount: 100);
		}

		[Fact(DisplayName = "test_range_object_many")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_range_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			var Size = 15 * 1024 * 1024;
			var Data = RandomTextToLong(Size);

			Client.PutObject(BucketName, Key, Body: Data);
			CheckContentUsingRandomRange(Client, BucketName, Key, Data, 100);
		}

		[Fact(DisplayName = "test_restore_object")]
		[Trait(MainData.Major, "GetObject")]
		[Trait(MainData.Minor, "Restore")]
		[Trait(MainData.Explanation, "오브젝트 복구 명령이 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_restore_object()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";

			Client.PutObject(BucketName, Key, Key);

			Client.RestoreObject(BucketName, Key);
		}
	}
}
