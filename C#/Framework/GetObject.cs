/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace s3tests2
{
	[TestClass]
	public class GetObject : TestBase
	{
		[TestMethod("test_object_read_not_exist")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_read_not_exist()
		{
			Console.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			Console.WriteLine(Config.ToString());
			Console.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			var BucketName = GetNewBucket();
			var Client = GetClient();


			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key: "bar"));

			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
			Assert.AreEqual(MainData.NoSuchKey, ErrorCode);
		}

		[TestMethod("test_get_object_ifmatch_good")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifmatch")]
		[TestProperty(MainData.Explanation, "존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifmatch_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			var PutResponse = Client.PutObject(BucketName, KeyName, Body: "bar");
			var ETag = PutResponse.ETag;

			var GetResponse = Client.GetObject(BucketName, KeyName, IfMatch: ETag);
			var Body = GetBody(GetResponse);
			Assert.AreEqual("bar", Body);
		}

		[TestMethod("test_get_object_ifmatch_failed")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifmatch")]
		[TestProperty(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifmatch_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, KeyName, IfMatch: "ABCORZ"));

			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.PreconditionFailed, StatusCode);
			Assert.AreEqual(HttpStatusCode.PreconditionFailed.ToString(), ErrorCode);
		}


		[TestMethod("test_get_object_ifnonematch_good")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifnonematch")]
		[TestProperty(MainData.Explanation, "오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifnonematch_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			var PutResponse = Client.PutObject(BucketName, KeyName, Body: "bar");
			var ETag = PutResponse.ETag;

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, KeyName, IfNoneMatch: ETag));

			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.NotModified, StatusCode);
			Assert.AreEqual(HttpStatusCode.NotModified.ToString(), ErrorCode);
		}

		[TestMethod("test_get_object_ifnonematch_failed")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifnonematch")]
		[TestProperty(MainData.Explanation, "오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifnonematch_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var GetResponse = Client.GetObject(BucketName, KeyName, IfNoneMatch: "ABCORZ");
			var Body = GetBody(GetResponse);
			Assert.AreEqual("bar", Body);
		}


		[TestMethod("test_get_object_ifmodifiedsince_good")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifmodifiedsince")]
		[TestProperty(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifmodifiedsince_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var GetResponse = Client.GetObject(BucketName, KeyName, IfModifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT");
			var Body = GetBody(GetResponse);
			Assert.AreEqual("bar", Body);
		}

		[TestMethod("test_get_object_ifmodifiedsince_failed")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifmodifiedsince")]
		[TestProperty(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후로 설정] " +
							"지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
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

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, KeyName, IfModifiedSinceDateTime: After));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.NotModified, StatusCode);
			Assert.AreEqual(HttpStatusCode.NotModified.ToString(), ErrorCode);
		}


		[TestMethod("test_get_object_ifunmodifiedsince_good")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifunmodifiedsince")]
		[TestProperty(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이전으로 설정] " +
							"지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 실패")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_get_object_ifunmodifiedsince_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, KeyName, IfUnmodifiedSince: "Sat, 29 Oct 1994 19:43:31 GMT"));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.PreconditionFailed, StatusCode);
			Assert.AreEqual(HttpStatusCode.PreconditionFailed.ToString(), ErrorCode);
		}



		[TestMethod("test_get_object_ifunmodifiedsince_failed")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Ifunmodifiedsince")]
		[TestProperty(MainData.Explanation, "[지정일을 오브젝트 업로드 시간 이후으로 설정] " +
							"지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 성공")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_ifunmodifiedsince_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";

			Client.PutObject(BucketName, KeyName, Body: "bar");

			var Response = Client.GetObject(BucketName, KeyName, IfUnmodifiedSince: "Fri, 29 Oct 2100 19:43:31 GMT");
			var Body = GetBody(Response);
			Assert.AreEqual("bar", Body);
		}

		[TestMethod("test_ranged_request_response_code")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "지정한 범위로 오브젝트 다운로드가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange(4, 7));

			var FetchedContent = GetBody(Response);
			Assert.AreEqual(Content.Substring(4, 4), FetchedContent);
			Assert.AreEqual("bytes 4-7/11", Response.ContentRange);
			Assert.AreEqual(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[TestMethod("test_ranged_big_request_response_code")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_big_request_response_code()
		{
			var Key = "testobj";
			var Content = RandomTextToLong(8 * MainData.MB);

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange(3145728, 5242880));

			var FetchedContent = GetBody(Response);
			Assert.AreEqual(Content.Substring(3145728, 5242880 - 3145728 + 1), FetchedContent);
			Assert.AreEqual("bytes 3145728-5242880/8388608", Response.ContentRange);
			Assert.AreEqual(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[TestMethod("test_ranged_request_skip_leading_bytes_response_code")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_skip_leading_bytes_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange("bytes=4-"));

			var FetchedContent = GetBody(Response);
			Assert.AreEqual(Content.Substring(4), FetchedContent);
			Assert.AreEqual("bytes 4-10/11", Response.ContentRange);
			Assert.AreEqual(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[TestMethod("test_ranged_request_return_trailing_bytes_response_code")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_ranged_request_return_trailing_bytes_response_code()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var Response = Client.GetObject(BucketName, Key, Range: new ByteRange("bytes=-7"));

			var FetchedContent = GetBody(Response);
			Assert.AreEqual(Content.Substring(Content.Length - 7, 7), FetchedContent);
			Assert.AreEqual("bytes 4-10/11", Response.ContentRange);
			Assert.AreEqual(HttpStatusCode.PartialContent, Response.HttpStatusCode);
		}

		[TestMethod("test_ranged_request_invalid_range")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_ranged_request_invalid_range()
		{
			var Key = "testobj";
			var Content = "testcontent";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key, Range: new ByteRange(40, 50)));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.RequestedRangeNotSatisfiable, StatusCode);
			Assert.AreEqual(MainData.InvalidRange, ErrorCode);
		}

		[TestMethod("test_ranged_request_empty_object")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Range")]
		[TestProperty(MainData.Explanation, "비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_ranged_request_empty_object()
		{
			var Key = "testobj";
			var Content = "";

			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: Content);
			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key, Range: new ByteRange(40, 50)));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.RequestedRangeNotSatisfiable, StatusCode);
			Assert.AreEqual(MainData.InvalidRange, ErrorCode);
		}

		[TestMethod("test_get_object_many")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			var Data = RandomTextToLong(15 * 1024 * 1024);

			Client.PutObject(BucketName, Key, Body: Data);
			CheckContent(Client, BucketName, Key, Data, 100);
		}

		[TestMethod("test_range_object_many")]
		[TestProperty(MainData.Major, "GetObject")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
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
	}
}
