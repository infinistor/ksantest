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
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class Multipart : TestBase
	{
		public Multipart(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "비어있는 오브젝트를 멀티파트로 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartUploadEmpty()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key1 = "mymultipart";
			var size = 0;

			var UploadData = SetupMultipartUpload(client, bucketName, Key1, size);
			var e = Assert.Throws<AggregateException>(() => client.CompleteMultipartUpload(bucketName, Key1, UploadData.UploadId, UploadData.Parts));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadSmall()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key1 = "mymultipart";
			var size = 1;

			var UploadData = SetupMultipartUpload(client, bucketName, Key1, size);
			client.CompleteMultipartUpload(bucketName, Key1, UploadData.UploadId, UploadData.Parts);
			var Response = client.GetObject(bucketName, Key1);
			Assert.Equal(size, Response.ContentLength);
		}

		[Fact]
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
			var size = 1;
			var client = GetClient();

			var UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);

			var Response = client.GetObject(DestBucketName, DestKey);
			Assert.Equal(size, Response.ContentLength);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "범위설정을 잘못한 멀티파트 복사 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_copy_invalid_range()
		{
			var client = GetClient();
			var SrcKey = "source";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 5);

			var DestKey = "dest";
			var Response = client.InitiateMultipartUpload(SrcBucketName, DestKey);
			var UploadId = Response.UploadId;

			var e = Assert.Throws<AggregateException>(() => client.CopyPart(SrcBucketName, SrcKey, SrcBucketName, DestKey, UploadId, 0, 0, 21));
			Assert.Contains(GetStatus(e), new List<HttpStatusCode>() { HttpStatusCode.BadRequest, HttpStatusCode.RequestedRangeNotSatisfiable });
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Range")]
		[Trait(MainData.Explanation, "범위를 지정한 멀티파트 복사 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_without_range()
		{
			var client = GetClient();
			var SrcKey = "source";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 10);
			var DestBucketName = GetNewBucket();
			var DestKey = "mymultipartcopy";

			var InitResponse = client.InitiateMultipartUpload(DestBucketName, DestKey);
			var UploadId = InitResponse.UploadId;
			var Parts = new List<PartETag>();

			var CopyResponse = client.CopyPart(SrcBucketName, SrcKey, DestBucketName, DestKey, UploadId, 1, 0, 9);
			Parts.Add(new PartETag(1, CopyResponse.ETag));
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadId, Parts);

			var Response = client.GetObject(DestBucketName, DestKey);
			Assert.Equal(10, Response.ContentLength);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Special Names")]
		[Trait(MainData.Explanation, "특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_special_names()
		{
			var SrcBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();

			var DestKey = "mymultipart";
			var size = 1;
			var client = GetClient();

			foreach (var SrcKey in new List<string>() { " ", "_", "__", "?versionId" })
			{
				SetupKeyWithRandomContent(SrcKey, bucketName: SrcBucketName);
				var UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
				client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
				var Response = client.GetObject(DestBucketName, DestKey);
				Assert.Equal(size, Response.ContentLength);
				CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "멀티파트 업로드 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var client = GetClient();

			var UploadData = SetupMultipartUpload(client, bucketName, key, size, metadataList: metadataList, contentType: ContentType);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(size, BytesUsed);

			var GetResponse = client.GetObject(bucketName, key);
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);
			Assert.Equal(metadataList, GetMetaData(GetResponse.Metadata));
			var body = GetBody(GetResponse);
			Assert.Equal(UploadData.Body, body);

			CheckContentUsingRange(client, bucketName, key, UploadData.Body, 1000000);
			CheckContentUsingRange(client, bucketName, key, UploadData.Body, 10000000);
			CheckContentUsingRandomRange(client, bucketName, key, UploadData.Body, 100);
		}

		[Fact]
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

			var size = 15 * MainData.MB;
			SetupKeyWithRandomContent(SrcKey, size: size, bucketName: SrcBucketName);
			SetupKeyWithRandomContent(SrcKey, size: size, bucketName: SrcBucketName);
			SetupKeyWithRandomContent(SrcKey, size: size, bucketName: SrcBucketName);

			var VersionIds = new List<string>();
			var client = GetClient();
			var ListResponse = client.ListVersions(SrcBucketName);
			foreach (var version in ListResponse.Versions)
				VersionIds.Add(version.VersionId);

			foreach (var versionId in VersionIds)
			{
				var UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size, versionId: versionId);
				client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
				var Response = client.GetObject(DestBucketName, DestKey);
				Assert.Equal(size, Response.ContentLength);
				CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey, versionId);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_resend_part()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var size = 50 * MainData.MB;

			CheckUploadMultipartResend(bucketName, key, size, new List<int>() { 0 });
			CheckUploadMultipartResend(bucketName, key, size, new List<int>() { 1 });
			CheckUploadMultipartResend(bucketName, key, size, new List<int>() { 2 });
			CheckUploadMultipartResend(bucketName, key, size, new List<int>() { 1, 2 });
			CheckUploadMultipartResend(bucketName, key, size, new List<int>() { 0, 1, 2, 3, 4, 5 });
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_multiple_sizes()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var client = GetClient();

			var size = 5 * MainData.MB;
			var UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 5 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 5 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB;
			UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_multiple_sizes()
		{
			var SrcKey = "foo";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 12 * MainData.MB);

			var DestBucketName = GetNewBucket();
			var DestKey = "mymultipart";
			var client = GetClient();

			var size = 5 * MainData.MB;
			var UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			size = 5 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			size = 5 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			size = 10 * MainData.MB;
			UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			size = 10 * MainData.MB + 100 * MainData.KB;
			UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);

			size = 10 * MainData.MB + 600 * MainData.KB;
			UploadData = SetupMultipartCopy(client, SrcBucketName, SrcKey, DestBucketName, DestKey, size);
			client.CompleteMultipartUpload(DestBucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContentUsingRange(client, SrcBucketName, SrcKey, DestBucketName, DestKey);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_size_too_small()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var client = GetClient();

			var size = 1 * MainData.MB;
			var UploadData = SetupMultipartUpload(client, bucketName, key, size: size, partSize: 10 * MainData.KB);
			var e = Assert.Throws<AggregateException>(() => client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ENTITY_TOO_SMALL, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "내용물을 채운 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_contents()
		{
			var bucketName = GetNewBucket();
			TestMultipartUploadContents(bucketName, "mymultipart", 3);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_overwrite_existing_object()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "mymultipart";
			var Payload = RandomTextToLong(5 * MainData.MB);
			var NumParts = 2;

			client.PutObject(bucketName, key, body: Payload);

			var InitResponse = client.InitiateMultipartUpload(bucketName, key);
			var UploadId = InitResponse.UploadId;
			var Parts = new List<PartETag>();
			var AllPayload = "";

			for (int i = 0; i < NumParts; i++)
			{
				var PartNumber = i + 1;
				var PartResponse = client.UploadPart(bucketName, key, UploadId, Payload, PartNumber);
				Parts.Add(new PartETag(PartNumber, PartResponse.ETag));
				AllPayload += Payload;
			}

			client.CompleteMultipartUpload(bucketName, key, UploadId, Parts);

			var Response = client.GetObject(bucketName, key);
			var Text = GetBody(Response);
			Assert.Equal(AllPayload, Text);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_abort_multipart_upload()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var size = 10 * MainData.MB;
			var client = GetClient();

			var UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.AbortMultipartUpload(bucketName, key, UploadData.UploadId);

			var HeadResponse = client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(0, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(0, BytesUsed);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않은 멀티파트 업로드 중단 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_abort_multipart_upload_not_found()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "mymultipart";
			client.PutObject(bucketName, key);

			var e = Assert.Throws<AggregateException>(() => client.AbortMultipartUpload(bucketName, key, "56788"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_UPLOAD, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 업로드 중인 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_list_multipart_upload()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "mymultipart";
			var Key2 = "mymultipart2";

			var UploadIds = new List<string>
			{
				SetupMultipartUpload(client, bucketName, key, 5 * MainData.MB).UploadId,
				SetupMultipartUpload(client, bucketName, key, 6 * MainData.MB).UploadId,
				SetupMultipartUpload(client, bucketName, Key2, 5 * MainData.MB).UploadId,
			};

			var Response = client.ListMultipartUploads(bucketName);
			var GetUploadIds = new List<string>();

			foreach (var UploadData in Response.MultipartUploads) GetUploadIds.Add(UploadData.UploadId);

			foreach (var UploadId in UploadIds) Assert.Contains(UploadId, GetUploadIds);

			client.AbortMultipartUpload(bucketName, key, UploadIds[0]);
			client.AbortMultipartUpload(bucketName, key, UploadIds[1]);
			client.AbortMultipartUpload(bucketName, Key2, UploadIds[2]);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_missing_part()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "mymultipart";

			var InitResponse = client.InitiateMultipartUpload(bucketName, key);
			var UploadId = InitResponse.UploadId;

			var Parts = new List<PartETag>();
			var PartResponse = client.UploadPart(bucketName, key, UploadId, "\x00", 1);
			Parts.Add(new PartETag(9999, PartResponse.ETag));

			var e = Assert.Throws<AggregateException>(() => client.CompleteMultipartUpload(bucketName, key, UploadId, Parts));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_PART, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_multipart_upload_incorrect_etag()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "mymultipart";

			var InitResponse = client.InitiateMultipartUpload(bucketName, key);
			var UploadId = InitResponse.UploadId;

			var Parts = new List<PartETag>();
			var PartResponse = client.UploadPart(bucketName, key, UploadId, "\x00", 1);
			Parts.Add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

			var e = Assert.Throws<AggregateException>(() => client.CompleteMultipartUpload(bucketName, key, UploadId, Parts));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_PART, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 " +
									 "시작 또는 중단했을때 오브젝트에 영향이 없음을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_atomic_multipart_upload_write()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			client.PutObject(bucketName, key, body: "bar");

			var InitResponse = client.InitiateMultipartUpload(bucketName, key);
			var UploadId = InitResponse.UploadId;

			var Response = client.GetObject(bucketName, key);
			var body = GetBody(Response);
			Assert.Equal("bar", body);

			client.AbortMultipartUpload(bucketName, key, UploadId);

			Response = client.GetObject(bucketName, key);
			body = GetBody(Response);
			Assert.Equal("bar", body);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 업로드 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_upload_list()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var client = GetClient();

			var UploadData = SetupMultipartUpload(client, bucketName, key, size, metadataList: metadataList, contentType: ContentType);

			var Response = client.ListParts(bucketName, key, UploadData.UploadId);
			Assert.Equal(UploadData.Parts.Count, Response.Parts.Count);
			PartsETagCompare(UploadData.Parts, Response.Parts);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_abort_multipart_upload_list()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var size = 10 * MainData.MB;
			var client = GetClient();

			var UploadData = SetupMultipartUpload(client, bucketName, key, size);
			client.AbortMultipartUpload(bucketName, key, UploadData.UploadId);

			var ListResponse = client.ListMultipartUploads(bucketName);
			Assert.Empty(ListResponse.MultipartUploads);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multipart_copy_many()
		{
			var bucketName = GetNewBucket();
			var SrcKey = "mymultipart";
			var size = 10 * MainData.MB;
			var client = GetClient();
			var body = "";
			// 멀티파트 업로드
			var UploadData = SetupMultipartUpload(client, bucketName, SrcKey, size);
			client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, SrcKey, body);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1";
			UploadData = SetupMultipartCopy(client, bucketName, SrcKey, bucketName, DestKey1, size);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(client, bucketName, DestKey1, size, uploadData: UploadData);
			client.CompleteMultipartUpload(bucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, DestKey1, body);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2";
			UploadData = SetupMultipartCopy(client, bucketName, DestKey1, bucketName, DestKey2, size * 2);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(client, bucketName, DestKey2, size, uploadData: UploadData);
			client.CompleteMultipartUpload(bucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, DestKey2, body);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartListParts()
		{
			var bucketName = GetNewBucket();
			var key = "mymultipart";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var client = GetClient();

			var UploadData = SetupMultipartUpload(client, bucketName, key, size, partSize: MainData.MB, contentType: ContentType);

			for (var i = 0; i < 41; i += 10)
			{
				var Response = client.ListParts(bucketName, key, UploadData.UploadId, maxParts: 10, partNumberMarker: i);
				Assert.Equal(10, Response.Parts.Count);
				PartsETagCompare(UploadData.Parts.GetRange(i, 10), Response.Parts);
			}
		}
	}
}
