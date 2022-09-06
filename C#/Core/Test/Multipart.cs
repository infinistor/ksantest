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
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class Multipart : TestBase
	{
		public Multipart(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_multipart_upload_empty")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "비어있는 오브젝트를 멀티파트로 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_empty()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key1 = "mymultipart";
			var Size = 0;

			var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size);
			var e = Assert.Throws<AggregateException>(() => Client.CompleteMultipartUpload(BucketName, Key1, UploadData.UploadId, UploadData.Parts));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_multipart_upload_small")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_small()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key1 = "mymultipart";
			var Size = 1;

			var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size);
			Client.CompleteMultipartUpload(BucketName, Key1, UploadData.UploadId, UploadData.Parts);
			var Response = Client.GetObject(BucketName, Key1);
			Assert.Equal(Size, Response.ContentLength);
		}

		[Fact(DisplayName = "test_multipart_copy_small")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "버킷a에서 버킷b로 멀티파트 복사 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_small()
		{
			var SrcKey = "foo";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey);

			var DestBucketName = GetNewBucket();
			var DestKey = "mymultipart";
			var Size = 1;
			var Client = GetClient();

			var UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);

			var Response = Client.GetObject(DestBucketName, DestKey);
			Assert.Equal(Size, Response.ContentLength);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact(DisplayName = "test_multipart_copy_invalid_range")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "범위설정을 잘못한 멀티파트 복사 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_copy_invalid_range()
		{
			var Client = GetClient();
			var SrcKey = "source";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, Size: 5);

			var DestKey = "dest";
			var Response = Client.InitiateMultipartUpload(SrcBucketName, DestKey);
			var UploadId = Response.UploadId;

			var e = Assert.Throws<AggregateException>(() => Client.CopyPart(SrcBucketName, SrcKey, SrcBucketName, DestKey, UploadId, 0, 0, 21));
			Assert.Contains(GetStatus(e), new List<HttpStatusCode>() { HttpStatusCode.BadRequest, HttpStatusCode.RequestedRangeNotSatisfiable });
			Assert.Equal(MainData.InvalidArgument, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_multipart_copy_without_range")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "범위를 지정한 멀티파트 복사 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_without_range()
		{
			var Client = GetClient();
			var SrcKey = "source";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, Size: 10);
			var DestBucketName = GetNewBucket();
			var DestKey = "mymultipartcopy";

			var InitResponse = Client.InitiateMultipartUpload(DestBucketName, DestKey);
			var UploadId = InitResponse.UploadId;
			var Parts = new List<PartETag>();

			var CopyResponse = Client.CopyPart(SrcBucketName, SrcKey, DestBucketName, DestKey, UploadId, 1, 0, 9);
			Parts.Add(new PartETag(1, CopyResponse.ETag));
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadId, Parts);

			var Response = Client.GetObject(DestBucketName, DestKey);
			Assert.Equal(10, Response.ContentLength);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact(DisplayName = "test_multipart_copy_special_names")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Special Names")]
		[Trait(MainData.Explanation, "특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_special_names()
		{
			var SrcBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();

			var DestKey = "mymultipart";
			var Size = 1;
			var Client = GetClient();

			foreach (var SrcKey in new List<string>() { " ", "_", "__", "?versionId" })
			{
				SetupKeyWithRandomContent(SrcKey, BucketName: SrcBucketName);
				var UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
				Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
				var Response = Client.GetObject(DestBucketName, DestKey);
				Assert.Equal(Size, Response.ContentLength);
				CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);
			}
		}

		[Fact(DisplayName = "test_multipart_upload")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "멀티파트 업로드 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var ContentType = "text/bla";
			var Size = 50 * MainData.MB;
			var MetadataList = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar") };
			var Client = GetClient();

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: MetadataList, ContentType: ContentType);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, Key);
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);
			Assert.Equal(MetadataList, GetMetaData(GetResponse.Metadata));
			var Body = GetBody(GetResponse);
			Assert.Equal(UploadData.Body, Body);

			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 1000000);
			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 10000000);
			CheckContentUsingRandomRange(Client, BucketName, Key, UploadData.Body, 100);
		}

		[Fact(DisplayName = "test_multipart_copy_versioned")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_versioned()
		{
			var SrcBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();

			var DestKey = "mymultipart";
			CheckVersioning(SrcBucketName, VersionStatus.Off);

			var SrcKey = "foo";
			CheckConfigureVersioningRetry(SrcBucketName, VersionStatus.Enabled);

			var Size = 15 * MainData.MB;
			SetupKeyWithRandomContent(SrcKey, Size: Size, BucketName: SrcBucketName);
			SetupKeyWithRandomContent(SrcKey, Size: Size, BucketName: SrcBucketName);
			SetupKeyWithRandomContent(SrcKey, Size: Size, BucketName: SrcBucketName);

			var VersionId = new List<string>();
			var Client = GetClient();
			var ListResponse = Client.ListVersions(SrcBucketName);
			foreach (var version in ListResponse.Versions)
				VersionId.Add(version.VersionId);

			foreach (var VId in VersionId)
			{
				var UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size, VersionId: VId);
				Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
				var Response = Client.GetObject(DestBucketName, DestKey);
				Assert.Equal(Size, Response.ContentLength);
				CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, VId);
			}
		}

		[Fact(DisplayName = "test_multipart_upload_resend_part")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_resend_part()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var Size = 50 * MainData.MB;

			CheckUploadMultipartResend(BucketName, Key, Size, new List<int>() { 0 });
			CheckUploadMultipartResend(BucketName, Key, Size, new List<int>() { 1 });
			CheckUploadMultipartResend(BucketName, Key, Size, new List<int>() { 2 });
			CheckUploadMultipartResend(BucketName, Key, Size, new List<int>() { 1, 2 });
			CheckUploadMultipartResend(BucketName, Key, Size, new List<int>() { 0, 1, 2, 3, 4, 5 });
		}

		[Fact(DisplayName = "test_multipart_upload_multiple_sizes")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_multiple_sizes()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var Client = GetClient();

			var Size = 5 * MainData.MB;
			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			Size = 5 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			Size = 5 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			Size = 10 * MainData.MB;
			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			Size = 10 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			Size = 10 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

		}

		[Fact(DisplayName = "test_multipart_copy_multiple_sizes")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_multiple_sizes()
		{
			var SrcKey = "foo";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, Size: 12 * MainData.MB);

			var DestBucketName = GetNewBucket();
			var DestKey = "mymultipart";
			var Client = GetClient();

			var Size = 5 * MainData.MB;
			var UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			Size = 5 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			Size = 5 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			Size = 10 * MainData.MB;
			UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			Size = 10 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			Size = 10 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartCopy(Client, SrcBucketName, SrcKey, DestBucketName, DestKey, Size);
			Client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(Client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact(DisplayName = "test_multipart_upload_size_too_small")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_size_too_small()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var Client = GetClient();

			var Size = 1 * MainData.MB;
			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size: Size, PartSize: 10 * MainData.KB);
			var e = Assert.Throws<AggregateException>(() => Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.EntityTooSmall, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_multipart_upload_contents")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "내용물을 채운 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_contents()
		{
			var BucketName = GetNewBucket();
			TestMultipartUploadContents(BucketName, "mymultipart", 3);
		}

		[Fact(DisplayName = "test_multipart_upload_overwrite_existing_object")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "OverWrite")]
		[Trait(MainData.Explanation, "업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_overwrite_existing_object()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "mymultipart";
			var Payload = RandomTextToLong(5 * MainData.MB);
			var NumParts = 2;

			Client.PutObject(BucketName, Key, Body: Payload);

			var InitResponse = Client.InitiateMultipartUpload(BucketName, Key);
			var UploadId = InitResponse.UploadId;
			var Parts = new List<PartETag>();
			var AllPayload = "";

			for (int i = 0; i < NumParts; i++)
			{
				var PartNumber = i + 1;
				var PartResponse = Client.UploadPart(BucketName, Key, UploadId, Payload, PartNumber);
				Parts.Add(new PartETag(PartNumber, PartResponse.ETag));
				AllPayload += Payload;
			}

			Client.CompleteMultipartUpload(BucketName, Key, UploadId, Parts);

			var Response = Client.GetObject(BucketName, Key);
			var Text = GetBody(Response);
			Assert.Equal(AllPayload, Text);
		}

		[Fact(DisplayName = "test_abort_multipart_upload")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_abort_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var Size = 10 * MainData.MB;
			var Client = GetClient();

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.AbortMultipartUpload(BucketName, Key, UploadData.UploadId);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(0, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(0, BytesUsed);
		}

		[Fact(DisplayName = "test_abort_multipart_upload_not_found")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않은 멀티파트 업로드 중단 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_abort_multipart_upload_not_found()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "mymultipart";
			Client.PutObject(BucketName, Key);

			var e = Assert.Throws<AggregateException>(() => Client.AbortMultipartUpload(BucketName, Key, "56788"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchUpload, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_list_multipart_upload")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 업로드 중인 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_list_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "mymultipart";
			var Key2 = "mymultipart2";

			var UploadIds = new List<string>
			{
				SetupMultipartUpload(Client, BucketName, Key, 5 * MainData.MB).UploadId,
				SetupMultipartUpload(Client, BucketName, Key, 6 * MainData.MB).UploadId,
				SetupMultipartUpload(Client, BucketName, Key2, 5 * MainData.MB).UploadId,
			};

			var Response = Client.ListMultipartUploads(BucketName);
			var GetUploadIds = new List<string>();

			foreach (var UploadData in Response.MultipartUploads) GetUploadIds.Add(UploadData.UploadId);

			foreach (var UploadId in UploadIds) Assert.Contains(UploadId, GetUploadIds);

			Client.AbortMultipartUpload(BucketName, Key, UploadIds[0]);
			Client.AbortMultipartUpload(BucketName, Key, UploadIds[1]);
			Client.AbortMultipartUpload(BucketName, Key2, UploadIds[2]);
		}

		[Fact(DisplayName = "test_multipart_upload_missing_part")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_missing_part()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "mymultipart";

			var InitResponse = Client.InitiateMultipartUpload(BucketName, Key);
			var UploadId = InitResponse.UploadId;

			var Parts = new List<PartETag>();
			var PartResponse = Client.UploadPart(BucketName, Key, UploadId, "\x00", 1);
			Parts.Add(new PartETag(9999, PartResponse.ETag));

			var e = Assert.Throws<AggregateException>(() => Client.CompleteMultipartUpload(BucketName, Key, UploadId, Parts));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.InvalidPart, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_multipart_upload_incorrect_etag")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_incorrect_etag()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "mymultipart";

			var InitResponse = Client.InitiateMultipartUpload(BucketName, Key);
			var UploadId = InitResponse.UploadId;

			var Parts = new List<PartETag>();
			var PartResponse = Client.UploadPart(BucketName, Key, UploadId, "\x00", 1);
			Parts.Add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

			var e = Assert.Throws<AggregateException>(() => Client.CompleteMultipartUpload(BucketName, Key, UploadId, Parts));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.InvalidPart, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_atomic_multipart_upload_write")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 " +
									 "시작 또는 중단했을때 오브젝트에 영향이 없음을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_atomic_multipart_upload_write()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			Client.PutObject(BucketName, Key, Body: "bar");

			var InitResponse = Client.InitiateMultipartUpload(BucketName, Key);
			var UploadId = InitResponse.UploadId;

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.Equal("bar", Body);

			Client.AbortMultipartUpload(BucketName, Key, UploadId);

			Response = Client.GetObject(BucketName, Key);
			Body = GetBody(Response);
			Assert.Equal("bar", Body);
		}

		[Fact(DisplayName = "test_multipart_upload_list")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 업로드 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_list()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var ContentType = "text/bla";
			var Size = 50 * MainData.MB;
			var MetadataList = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar") };
			var Client = GetClient();

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: MetadataList, ContentType: ContentType);

			var Response = Client.ListParts(BucketName, Key, UploadData.UploadId);
			Assert.Equal(UploadData.Parts.Count, Response.Parts.Count);
			PartsETagCompare(UploadData.Parts, Response.Parts);
		}

		[Fact(DisplayName = "test_abort_multipart_upload_list")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_abort_multipart_upload_list()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var Size = 10 * MainData.MB;
			var Client = GetClient();

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size);
			Client.AbortMultipartUpload(BucketName, Key, UploadData.UploadId);

			var ListResponse = Client.ListMultipartUploads(BucketName);
			Assert.Empty(ListResponse.MultipartUploads);
		}

		[Fact(DisplayName = "test_multipart_copy_many")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_many()
		{
			var BucketName = GetNewBucket();
			var SrcKey = "mymultipart";
			var Size = 10 * MainData.MB;
			var Client = GetClient();
			var Body = "";
			// 멀티파트 업로드
			var UploadData = SetupMultipartUpload(Client, BucketName, SrcKey, Size);
			Client.CompleteMultipartUpload(BucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, SrcKey, Body);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1";
			UploadData = SetupMultipartCopy(Client, BucketName, SrcKey, BucketName, DestKey1, Size);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey1, Size, UploadData: UploadData);
			Client.CompleteMultipartUpload(BucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey1, Body);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2";
			UploadData = SetupMultipartCopy(Client, BucketName, DestKey1, BucketName, DestKey2, Size * 2);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey2, Size, UploadData: UploadData);
			Client.CompleteMultipartUpload(BucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey2, Body);
		}

		[Fact(DisplayName = "test_multipart_list_parts")]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_list_parts()
		{
			var BucketName = GetNewBucket();
			var Key = "mymultipart";
			var ContentType = "text/bla";
			var Size = 50 * MainData.MB;
			var Client = GetClient();

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, PartSize: MainData.MB, ContentType: ContentType);

			for (var i = 0; i < 41; i += 10)
			{
				var Response = Client.ListParts(BucketName, Key, UploadData.UploadId, MaxParts: 10, PartNumberMarker: i);
				Assert.Equal(10, Response.Parts.Count);
				PartsETagCompare(UploadData.Parts.GetRange(i, 10), Response.Parts);
			}
		}
	}
}
