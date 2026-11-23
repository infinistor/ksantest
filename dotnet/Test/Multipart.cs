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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
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
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key1 = "TestMultipartUploadEmpty-000";
			var size = 0;

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, Key1, size);
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
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Key1 = "TestMultipartUploadSmall-000";
			var size = 1;

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, Key1, size);
			client.CompleteMultipartUpload(bucketName, Key1, UploadData.UploadId, UploadData.Parts);
			var Response = client.GetObject(bucketName, Key1);
			Assert.Equal(size, Response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "버킷a에서 버킷b로 멀티파트 복사 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartCopySmall()
		{
			TestId = 3;
			var SrcKey = "TestMultipartCopySmallSource";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey);

			var DestBucketName = GetNewBucket();
			var DestKey = "TestMultipartCopySmallDestination";
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
		public void TestMultipartCopyInvalidRange()
		{
			TestId = 4;
			var client = GetClient();
			var SrcKey = "TestMultipartCopyInvalidRangeSource";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 5);

			var DestKey = "TestMultipartCopyInvalidRangeDestination";
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
		public void TestMultipartCopyWithoutRange()
		{
			TestId = 5;
			var client = GetClient();
			var SrcKey = "TestMultipartCopyWithoutRangeSource";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 10);
			var DestBucketName = GetNewBucket();
			var DestKey = "TestMultipartCopyWithoutRangeDestination";

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
		public void TestMultipartCopySpecialNames()
		{
			TestId = 6;
			var SrcBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();

			var DestKey = "TestMultipartCopySpecialNamesDestination";
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
		public void TestMultipartUpload()
		{
			TestId = 7;
			var bucketName = GetNewBucket();
			var key = "TestMultipartUpload";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var client = GetClient();

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, metadataList: metadataList, contentType: ContentType);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(size, BytesUsed);

			var GetResponse = client.GetObject(bucketName, key);
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);
			Assert.Equal(metadataList, GetMetaData(GetResponse.Metadata));
			var body = S3Utils.GetBody(GetResponse);
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
		public void TestMultipartCopyVersioned()
		{
			TestId = 8;
			var SrcBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();

			var DestKey = "TestMultipartCopyVersionedDestination";
			CheckVersioning(SrcBucketName, VersionStatus.Off);

			var SrcKey = "TestMultipartCopyVersionedSource";
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
		public void TestMultipartUploadResendPart()
		{
			TestId = 9;
			var bucketName = GetNewBucket();
			var key = "TestMultipartUploadResendPart";
			var size = 50 * MainData.MB;

			CheckUploadMultipartResend(bucketName, key, size, [0]);
			CheckUploadMultipartResend(bucketName, key, size, [1]);
			CheckUploadMultipartResend(bucketName, key, size, [2]);
			CheckUploadMultipartResend(bucketName, key, size, [1, 2]);
			CheckUploadMultipartResend(bucketName, key, size, [0, 1, 2, 3, 4, 5]);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadMultipleSizes()
		{
			TestId = 10;
			var bucketName = GetNewBucket();
			var key = "TestMultipartUploadMultipleSizes";
			var client = GetClient();

			var size = 5 * MainData.MB;
			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 5 * MainData.MB + 100 * MainData.KB;
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 5 * MainData.MB + 600 * MainData.KB;
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB;
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB + 100 * MainData.KB;
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			size = 10 * MainData.MB + 600 * MainData.KB;
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartCopyMultipleSizes()
		{
			TestId = 11;
			var SrcKey = "TestMultipartCopyMultipleSizesSource";
			var SrcBucketName = SetupKeyWithRandomContent(SrcKey, size: 12 * MainData.MB);

			var DestBucketName = GetNewBucket();
			var DestKey = "TestMultipartCopyMultipleSizesDestination";
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
		public void TestMultipartUploadSizeTooSmall()
		{
			TestId = 12;
			var bucketName = GetNewBucket();
			var key = "TestMultipartUploadSizeTooSmall";
			var client = GetClient();

			var size = 1 * MainData.MB;
			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size: size, partSize: 10 * MainData.KB);
			var e = Assert.Throws<AggregateException>(() => client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ENTITY_TOO_SMALL, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "내용물을 채운 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadContents()
		{
			TestId = 13;
			var bucketName = GetNewBucket();
			DoTestMultipartUploadContents(bucketName, "mymultipart", 3);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadOverwriteExistingObject()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartUploadOverwriteExistingObject";
			var Payload = S3Utils.RandomTextToLong(5 * MainData.MB);
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
			var Text = S3Utils.GetBody(Response);
			Assert.Equal(AllPayload, Text);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestAbortMultipartUpload()
		{
			TestId = 16;
			var bucketName = GetNewBucket();
			var key = "TestAbortMultipartUpload";
			var size = 10 * MainData.MB;
			var client = GetClient();

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
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
		public void TestAbortMultipartUploadNotFound()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestAbortMultipartUploadNotFound";
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
		public void TestListMultipartUpload()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestListMultipartUpload";
			var Key2 = "TestListMultipartUpload-001";

			var UploadIds = new List<string>
			{
				S3Utils.SetupMultipartUpload(client, bucketName, key, 5 * MainData.MB).UploadId,
				S3Utils.SetupMultipartUpload(client, bucketName, key, 6 * MainData.MB).UploadId,
				S3Utils.SetupMultipartUpload(client, bucketName, Key2, 5 * MainData.MB).UploadId,
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
		public void TestMultipartUploadMissingPart()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartUploadMissingPart";

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
		public void TestMultipartUploadIncorrectEtag()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartUploadIncorrectEtag";

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
		public void TestAtomicMultipartUploadWrite()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestAtomicMultipartUploadWrite";
			client.PutObject(bucketName, key, body: "bar");

			var InitResponse = client.InitiateMultipartUpload(bucketName, key);
			var UploadId = InitResponse.UploadId;

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal("bar", body);

			client.AbortMultipartUpload(bucketName, key, UploadId);

			Response = client.GetObject(bucketName, key);
			body = S3Utils.GetBody(Response);
			Assert.Equal("bar", body);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "멀티파트 업로드 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadList()
		{
			TestId = 22;
			var bucketName = GetNewBucket();
			var key = "TestMultipartUploadList";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var client = GetClient();

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, metadataList: metadataList, contentType: ContentType);

			var Response = client.ListParts(bucketName, key, UploadData.UploadId);
			Assert.Equal(UploadData.Parts.Count, Response.Parts.Count);
			PartsETagCompare(UploadData.Parts, Response.Parts);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드하는 도중 중단 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestAbortMultipartUploadList()
		{
			TestId = 23;
			var bucketName = GetNewBucket();
			var key = "TestAbortMultipartUploadList";
			var size = 10 * MainData.MB;
			var client = GetClient();

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.AbortMultipartUpload(bucketName, key, UploadData.UploadId);

			var ListResponse = client.ListMultipartUploads(bucketName);
			Assert.Empty(ListResponse.MultipartUploads);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartCopyMany()
		{
			TestId = 24;
			var bucketName = GetNewBucket();
			var SrcKey = "TestMultipartCopyManySource";
			var size = 10 * MainData.MB;
			var client = GetClient();
			var body = "";
			// 멀티파트 업로드
			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, SrcKey, size);
			client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, SrcKey, body);

			// 멀티파트 카피
			var DestKey1 = "TestMultipartCopyManyDestination-000";
			UploadData = SetupMultipartCopy(client, bucketName, SrcKey, bucketName, DestKey1, size);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, DestKey1, size, uploadData: UploadData);
			client.CompleteMultipartUpload(bucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, DestKey1, body);

			// 멀티파트 카피
			var DestKey2 = "TestMultipartCopyManyDestination-001";
			UploadData = SetupMultipartCopy(client, bucketName, DestKey1, bucketName, DestKey2, size * 2);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, DestKey2, size, uploadData: UploadData);
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
			TestId = 25;
			var bucketName = GetNewBucket();
			var key = "TestMultipartListParts";
			var ContentType = "text/bla";
			var size = 50 * MainData.MB;
			var client = GetClient();

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, partSize: MainData.MB, contentType: ContentType);

			for (var i = 0; i < 41; i += 10)
			{
				var Response = client.ListParts(bucketName, key, UploadData.UploadId, maxParts: 10, partNumberMarker: i);
				Assert.Equal(10, Response.Parts.Count);
				PartsETagCompare(UploadData.Parts.GetRange(i, 10), Response.Parts);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "ChunkEncoding 사용 시 체크섬 타입/알고리즘별 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadChecksumUseChunkEncoding()
		{
			TestId = 26;
			var bucketName = GetNewBucket();
			RunMultipartChecksumConfigs(bucketName, useHttps: true);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "체크섬 타입/알고리즘별 멀티파트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadChecksum()
		{
			TestId = 27;
			var bucketName = GetNewBucket();
			RunMultipartChecksumConfigs(bucketName, useHttps: false);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum-failure")]
		[Trait(MainData.Explanation, "지원되지 않는 체크섬 타입/알고리즘 조합 시 InvalidRequest 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartUploadChecksumFailure()
		{
			TestId = 28;
			var bucketName = GetNewBucket();
			var unsupportedFullObject = new[]
			{
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256,
			};
			var unsupportedComposite = new[] { ChecksumAlgorithm.CRC64NVME };

			var configs = new (RequestChecksumCalculation Req, ResponseChecksumValidation Resp, ChecksumType Type, ChecksumAlgorithm[] Algs)[]
			{
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.FULL_OBJECT, unsupportedFullObject),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.FULL_OBJECT, unsupportedFullObject),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.COMPOSITE, unsupportedComposite),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.COMPOSITE, unsupportedComposite),
			};

			foreach (var config in configs)
			{
				var client = GetClientHttpsV4(config.Req, config.Resp);
				foreach (var checksum in config.Algs)
				{
					var key = $"fail/{config.Type.Value}/{checksum.Value}";
					var e = Assert.Throws<AggregateException>(() =>
						MultipartUploadChecksum(client, bucketName, key, config.Type, checksum));
					Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
					Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
				}
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "멀티파트 복사 시 체크섬 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartCopyChecksum()
		{
			TestId = 29;
			var client = GetClientHttpsV4(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED);
			var bucketName = GetNewBucket(client);
			foreach (var checksum in CheckSum.FullObjectAlgorithms)
			{
				var sourceKey = $"src/{checksum.Value}";
				var destKey = $"dst/{checksum.Value}";
				var putResponse = client.PutObject(bucketName, sourceKey, body: sourceKey, checksumAlgorithm: checksum);
				ChecksumCompare(checksum, sourceKey, putResponse);

				MultipartUploadChecksum(client, bucketName, destKey, ChecksumType.FULL_OBJECT, checksum);
			}
		}

		private void RunMultipartChecksumConfigs(string bucketName, bool useHttps)
		{
			var configs = new (RequestChecksumCalculation Req, ResponseChecksumValidation Resp, ChecksumType Type, IReadOnlyList<ChecksumAlgorithm> Algs)[]
			{
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.FULL_OBJECT, CheckSum.FullObjectAlgorithms),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.FULL_OBJECT, CheckSum.FullObjectAlgorithms),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.FULL_OBJECT, CheckSum.FullObjectAlgorithms),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.FULL_OBJECT, CheckSum.FullObjectAlgorithms),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.COMPOSITE, CheckSum.CompositeAlgorithms),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.COMPOSITE, CheckSum.CompositeAlgorithms),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_REQUIRED, ChecksumType.COMPOSITE, CheckSum.CompositeAlgorithms),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED, ChecksumType.COMPOSITE, CheckSum.CompositeAlgorithms),
			};

			foreach (var config in configs)
			{
				var client = useHttps
					? GetClientHttpsV4(config.Req, config.Resp)
					: GetClient(config.Req, config.Resp);
				foreach (var checksum in config.Algs)
				{
					var key = $"req_{config.Req}/resp_{config.Resp}/{config.Type.Value}/{checksum.Value}";
					MultipartUploadChecksum(client, bucketName, key, config.Type, checksum);
				}
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "OverWrite")]
		[Trait(MainData.Explanation, "멀티파트로 업로드한 오브젝트를 PutObject로 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectOverwriteMultipartUpload()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestPutObjectOverwriteMultipartUpload";
			var multipartSize = 10 * MainData.MB;
			var content = S3Utils.RandomTextToLong(1 * MainData.MB);

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, multipartSize);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			client.PutObject(bucketName, key, body: content);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(content.Length, headResponse.ContentLength);

			var response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(response);
			Assert.Equal(content.Length, body.Length);
			Assert.Equal(content, body);

			CheckContentUsingRange(client, bucketName, key, content, MainData.KB);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "체크섬 알고리즘 없이 체크섬 타입만 지정하여 멀티파트 업로드 생성 시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestcreateMultipartUploadEmptyChecksumAlgorithm()
		{
			TestId = 30;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestcreateMultipartUploadEmptyChecksumAlgorithm";

			var e = Assert.Throws<AggregateException>(() =>
				client.InitiateMultipartUpload(bucketName, key, checksumType: ChecksumType.FULL_OBJECT));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "체크섬 타입 없이 알고리즘만 지정하여 생성한 멀티파트 업로드를 타입 지정 완료로 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestcreateMultipartUploadEmptyChecksumType()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestcreateMultipartUploadEmptyChecksumType";
			var size = 10 * MainData.MB;
			var partSize = 5 * MainData.MB;
			var checksumType = ChecksumType.COMPOSITE;
			var checksum = ChecksumAlgorithm.CRC32;
			var uploadData = new MultipartUploadData { PartSize = partSize };

			var createResponse = client.InitiateMultipartUpload(bucketName, key, checksumAlgorithm: checksum);
			uploadData.UploadId = createResponse.UploadId;

			var remain = size;
			while (remain > 0)
			{
				var now = remain > partSize ? partSize : remain;
				var part = S3Utils.RandomTextToLong(now);
				uploadData.AppendBody(part);
				var partResponse = client.UploadPart(bucketName, key, uploadData.UploadId, part,
					uploadData.NextPartNumber, checksumAlgorithm: checksum);
				ChecksumCompare(checksum, part, partResponse);
				uploadData.AddPart(checksum, partResponse);
				remain -= now;
			}

			var completeResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId,
				uploadData.Parts, checksumType: checksumType);
			ChecksumCompare(checksum, uploadData, completeResponse);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스와 일치하는 copy-source-if-match 조건으로 UploadPartCopy 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestUploadPartCopyIfMatchGood()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfMatchGoodSource";
			var target = "testUploadPartCopyIfMatchGoodTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var partResponse = client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, eTagToMatch: eTag);
			client.CompleteMultipartUpload(bucketName, target, uploadId, [new PartETag(1, partResponse.ETag)]);

			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스와 일치하지 않는 copy-source-if-match 조건으로 UploadPartCopy 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfMatchFailed()
		{
			TestId = 33;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfMatchFailedSource";
			var target = "testUploadPartCopyIfMatchFailedTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, eTagToMatch: "ABC"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스와 일치하지 않는 copy-source-if-none-match 조건으로 UploadPartCopy 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestUploadPartCopyIfNoneMatchGood()
		{
			TestId = 34;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfNoneMatchGoodSource";
			var target = "testUploadPartCopyIfNoneMatchGoodTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var partResponse = client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, eTagToNotMatch: "ABC");
			client.CompleteMultipartUpload(bucketName, target, uploadId, [new PartETag(1, partResponse.ETag)]);

			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스와 일치하는 copy-source-if-none-match 조건으로 UploadPartCopy 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfNoneMatchFailed()
		{
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfNoneMatchFailedSource";
			var target = "testUploadPartCopyIfNoneMatchFailedTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, eTagToNotMatch: eTag));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "UploadPartCopy에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfMatchAndIfNoneMatch()
		{
			TestId = 36;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfMatchAndIfNoneMatchSource";
			var target = "testUploadPartCopyIfMatchAndIfNoneMatchTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, ifMatch: "ABC", ifNoneMatch: "DEF"));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "UploadPartCopy에 If-Match와 If-None-Match:* 를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfMatchAndIfNoneMatchAny()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfMatchAndIfNoneMatchAnySource";
			var target = "testUploadPartCopyIfMatchAndIfNoneMatchAnyTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, ifMatch: "ABC", ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 업로드 이전 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestUploadPartCopyIfModifiedSinceGood()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfModifiedSinceGoodSource";
			var target = "testUploadPartCopyIfModifiedSinceGoodTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var partResponse = client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1,
				modifiedSince: new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc));
			client.CompleteMultipartUpload(bucketName, target, uploadId, [new PartETag(1, partResponse.ETag)]);

			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 업로드 이후 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfModifiedSinceFailed()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfModifiedSinceFailedSource";
			var target = "testUploadPartCopyIfModifiedSinceFailedTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var lastModified = client.GetObjectMetadata(bucketName, source).LastModified.Value;
			var after = lastModified.AddSeconds(1);

			Delay(1000);

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1, modifiedSince: after));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestUploadPartCopyIfUnmodifiedSinceGood()
		{
			TestId = 40;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfUnmodifiedSinceGoodSource";
			var target = "testUploadPartCopyIfUnmodifiedSinceGoodTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var partResponse = client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1,
				unmodifiedSince: new DateTime(2100, 9, 29, 19, 43, 31, DateTimeKind.Utc));
			client.CompleteMultipartUpload(bucketName, target, uploadId, [new PartETag(1, partResponse.ETag)]);

			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyIfUnmodifiedSinceFailed()
		{
			TestId = 41;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testUploadPartCopyIfUnmodifiedSinceFailedSource";
			var target = "testUploadPartCopyIfUnmodifiedSinceFailedTarget";

			client.PutObject(bucketName, source, body: source);
			var uploadId = client.InitiateMultipartUpload(bucketName, target).UploadId;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyPart(bucketName, source, bucketName, target, uploadId, 1, 0, source.Length - 1,
					unmodifiedSince: new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc)));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			client.AbortMultipartUpload(bucketName, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상과 일치하는 If-Match 조건으로 CompleteMultipartUpload 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCompleteMultipartUploadIfMatchGood()
		{
			TestId = 42;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfMatchGood";
			var size = 5 * MainData.MB;

			var eTag = client.PutObject(bucketName, key, body: "old").ETag;

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifMatch: eTag);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(uploadData.Body, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상과 일치하지 않는 If-Match 조건으로 CompleteMultipartUpload 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadIfMatchFailed()
		{
			TestId = 43;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfMatchFailed";
			var size = 5 * MainData.MB;

			client.PutObject(bucketName, key, body: "old");

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			var e = Assert.Throws<AggregateException>(() =>
				client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));

			Assert.Contains(GetStatus(e), new List<HttpStatusCode>() { HttpStatusCode.PreconditionFailed, HttpStatusCode.OK });
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "존재하지 않는 키에 If-None-Match:* 조건으로 CompleteMultipartUpload 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCompleteMultipartUploadIfNoneMatchGood()
		{
			TestId = 44;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfNoneMatchGood";
			var size = 5 * MainData.MB;

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifNoneMatch: "*");

			var response = client.GetObject(bucketName, key);
			Assert.Equal(uploadData.Body, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "이미 존재하는 키에 If-None-Match:* 조건으로 CompleteMultipartUpload 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadIfNoneMatchFailed()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfNoneMatchFailed";
			var size = 5 * MainData.MB;

			client.PutObject(bucketName, key, body: "old");

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			var e = Assert.Throws<AggregateException>(() =>
				client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifNoneMatch: "*"));

			Assert.Contains(GetStatus(e), new List<HttpStatusCode>() { HttpStatusCode.PreconditionFailed, HttpStatusCode.OK });
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "CompleteMultipartUpload에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadIfMatchAndIfNoneMatch()
		{
			TestId = 46;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfMatchAndIfNoneMatch";
			var size = 5 * MainData.MB;

			var eTag = client.PutObject(bucketName, key, body: "old").ETag;

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			var e = Assert.Throws<AggregateException>(() =>
				client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifMatch: eTag, ifNoneMatch: eTag));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "CompleteMultipartUpload에 If-Match와 If-None-Match:* 를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadIfMatchAndIfNoneMatchAny()
		{
			TestId = 47;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestCompleteMultipartUploadIfMatchAndIfNoneMatchAny";
			var size = 5 * MainData.MB;

			var eTag = client.PutObject(bucketName, key, body: "old").ETag;

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			var e = Assert.Throws<AggregateException>(() =>
				client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts, ifMatch: eTag, ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Cancel")]
		[Trait(MainData.Explanation, "멀티파트 업로드를 중단한 뒤 파트 업로드 시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartUploadAbortDuringUpload()
		{
			TestId = 48;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestMultipartUploadAbortDuringUpload";
			var partBody = S3Utils.RandomTextToLong(5 * MainData.MB);

			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			client.AbortMultipartUpload(bucketName, key, uploadId);

			var e = Assert.Throws<AggregateException>(() => client.UploadPart(bucketName, key, uploadId, partBody, 1));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_UPLOAD, GetErrorCode(e));
		}

		private const string SmallPart = "0123456789";

		/// <summary>10바이트짜리 파트를 count 개 올리고 (uploadId, 파트 목록)을 돌려준다.</summary>
		private static (string UploadId, List<PartETag> Parts) UploadSmallParts(S3Client client, string bucketName, string key, int count)
		{
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;
			var parts = new List<PartETag>();
			for (var number = 1; number <= count; number++)
				parts.Add(new PartETag(number, client.UploadPart(bucketName, key, uploadId, SmallPart, number).ETag));
			return (uploadId, parts);
		}

		private static List<int?> GetPartNumbers(ListPartsResponse response)
			=> (response.Parts ?? []).ConvertAll(part => part.PartNumber);

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "파트 번호가 1~10000을 벗어나면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartInvalidPartNumber()
		{
			TestId = 49;
			var key = "TestUploadPartInvalidPartNumber";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			// 파트 번호는 1~10000
			foreach (var partNumber in new[] { 0, 10001 })
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
					() => client.UploadPart(bucketName, key, uploadId, "content", partNumber));
			client.UploadPart(bucketName, key, uploadId, "content", 10000);
			Assert.Equal([10000], GetPartNumbers(client.ListParts(bucketName, key, uploadId)));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 업로드 ID나 다른 키의 업로드 ID로 파트 업로드/목록 조회를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartNoSuchUpload()
		{
			TestId = 50;
			var key = "TestUploadPartNoSuchUpload";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			// 없는 업로드 ID, 다른 키의 업로드 ID
			foreach (var (targetKey, targetUploadId) in new[] { (key, "invalid"), (key + "_other", uploadId) })
			{
				AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
					() => client.UploadPart(bucketName, targetKey, targetUploadId, "content", 1));
				AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
					() => client.ListParts(bucketName, targetKey, targetUploadId));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ContentMD5")]
		[Trait(MainData.Explanation, "본문과 다른 Content-MD5로 파트를 올리면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartContentMd5Mismatch()
		{
			TestId = 51;
			var key = "TestUploadPartContentMd5Mismatch";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			AssertS3Error(HttpStatusCode.BadRequest, MainData.BAD_DIGEST,
				() => client.UploadPart(bucketName, key, uploadId, "content", 1, md5Digest: S3Utils.GetMD5("other")));
			Assert.Empty(GetPartNumbers(client.ListParts(bucketName, key, uploadId)));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "권한이 없는 사용자의 멀티파트 요청이 모두 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartAccessDenied()
		{
			TestId = 52;
			var key = "TestMultipartAccessDenied";
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, parts) = UploadSmallParts(client, bucketName, key, 1);

			var denied = new List<Action>
			{
				() => altClient.InitiateMultipartUpload(bucketName, key),
				() => altClient.UploadPart(bucketName, key, uploadId, "content", 2),
				() => altClient.ListParts(bucketName, key, uploadId),
				() => altClient.ListMultipartUploads(bucketName),
				() => altClient.CompleteMultipartUpload(bucketName, key, uploadId, parts),
				() => altClient.AbortMultipartUpload(bucketName, key, uploadId),
			};
			foreach (var request in denied)
				AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, request);

			// 업로드는 그대로 남아 있다
			Assert.Equal([1], GetPartNumbers(client.ListParts(bucketName, key, uploadId)));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 버킷에 멀티파트 업로드 생성/목록 조회를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartBucketNotExist()
		{
			TestId = 53;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET,
				() => client.InitiateMultipartUpload(bucketName, "key"));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.ListMultipartUploads(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "StorageClass")]
		[Trait(MainData.Explanation, "잘못된 스토리지 클래스로 멀티파트 업로드를 만들면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestMultipartInvalidStorageClass()
		{
			TestId = 54;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_STORAGE_CLASS,
				() => client.InitiateMultipartUpload(bucketName, "key", storageClass: new S3StorageClass("INVALID")));
			Assert.Empty(client.ListMultipartUploads(bucketName).MultipartUploads ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "ListParts의 max-parts와 part-number-marker 동작 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListPartsMaxParts()
		{
			TestId = 55;
			var key = "TestListPartsMaxParts";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, _) = UploadSmallParts(client, bucketName, key, 3);

			var response = client.ListParts(bucketName, key, uploadId);
			Assert.Equal([1, 2, 3], GetPartNumbers(response));
			Assert.False(response.IsTruncated);
			Assert.Equal(1000, response.MaxParts);

			response = client.ListParts(bucketName, key, uploadId, maxParts: 2);
			Assert.Equal([1, 2], GetPartNumbers(response));
			Assert.True(response.IsTruncated);
			Assert.Equal(2, response.NextPartNumberMarker);

			response = client.ListParts(bucketName, key, uploadId, partNumberMarker: 2);
			Assert.Equal([3], GetPartNumbers(response));
			Assert.False(response.IsTruncated);

			response = client.ListParts(bucketName, key, uploadId, maxParts: 0);
			Assert.Empty(GetPartNumbers(response));
			Assert.False(response.IsTruncated);

			// 1000 보다 큰 값은 1000 으로 낮춘다
			Assert.Equal(1000, client.ListParts(bucketName, key, uploadId, maxParts: 1001).MaxParts);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.ListParts(bucketName, key, uploadId, maxParts: -1));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "파트 번호가 중복이면 완료가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadInvalidPartOrder()
		{
			TestId = 56;
			var key = "TestCompleteMultipartUploadInvalidPartOrder";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, parts) = UploadSmallParts(client, bucketName, key, 2);

			// 파트 번호는 오름차순이어야 하고 중복될 수 없다
			// .NET SDK가 파트 목록을 번호순으로 정렬해 보내므로 내림차순은 보낼 수 없어 중복만 확인한다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_PART_ORDER,
				() => client.CompleteMultipartUpload(bucketName, key, uploadId, [parts[0], parts[0]]));
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.HeadObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "파트 목록 없이 완료하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCompleteMultipartUploadNoParts()
		{
			TestId = 57;
			var key = "TestCompleteMultipartUploadNoParts";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, _) = UploadSmallParts(client, bucketName, key, 1);

			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => client.CompleteMultipartUpload(bucketName, key, uploadId, []));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올린 파트 중 일부만 골라 완료할 수 있고 ETag에 따옴표가 없어도 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCompleteMultipartUploadSkipPart()
		{
			TestId = 58;
			var key = "TestCompleteMultipartUploadSkipPart";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;
			var bodies = new[] { new string('a', 5 * MainData.MB), new string('b', 5 * MainData.MB), new string('c', 10) };
			var eTags = new List<string>();
			for (var index = 0; index < bodies.Length; index++)
				eTags.Add(client.UploadPart(bucketName, key, uploadId, bodies[index], index + 1).ETag);

			// 올린 파트 중 일부만 골라 완료할 수 있고, ETag 는 따옴표가 없어도 된다
			var response = client.CompleteMultipartUpload(bucketName, key, uploadId,
				[new PartETag(1, eTags[0].Trim('"')), new PartETag(3, eTags[2].Trim('"'))]);
			Assert.EndsWith("-2\"", response.ETag);
			Assert.Equal(bucketName, response.BucketName);
			Assert.Equal(key, response.Key);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(bodies[0].Length + bodies[2].Length, getResponse.ContentLength);
			Assert.True(bodies[0] + bodies[2] == GetBody(getResponse), "Source does not match target");
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "완료한 업로드를 같은 내용으로 다시 완료하면 같은 결과로 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCompleteMultipartUploadTwice()
		{
			TestId = 59;
			var key = "TestCompleteMultipartUploadTwice";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, parts) = UploadSmallParts(client, bucketName, key, 1);

			var first = client.CompleteMultipartUpload(bucketName, key, uploadId, parts);
			Assert.EndsWith("-1\"", first.ETag);

			// 완료한 업로드를 같은 내용으로 다시 완료하면 같은 결과로 성공한다
			var second = client.CompleteMultipartUpload(bucketName, key, uploadId, parts);
			Assert.Equal(first.ETag, second.ETag);

			// 완료한 업로드에는 파트를 더 올리거나 목록을 볼 수 없고, 중단 요청은 성공한다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
				() => client.UploadPart(bucketName, key, uploadId, "content", 2));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD, () => client.ListParts(bucketName, key, uploadId));
			var response = client.AbortMultipartUpload(bucketName, key, uploadId);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);
			Assert.Equal(SmallPart, GetBody(client.GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "이미 중단한 업로드를 다시 중단해도 성공하고, 그 업로드로는 더 진행할 수 없는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestAbortMultipartUploadTwice()
		{
			TestId = 60;
			var key = "TestAbortMultipartUploadTwice";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var (uploadId, parts) = UploadSmallParts(client, bucketName, key, 1);
			client.AbortMultipartUpload(bucketName, key, uploadId);

			// 이미 중단한 업로드를 다시 중단해도 성공한다
			var response = client.AbortMultipartUpload(bucketName, key, uploadId);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);

			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
				() => client.UploadPart(bucketName, key, uploadId, "content", 1));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
				() => client.CompleteMultipartUpload(bucketName, key, uploadId, parts));
		}

		/// <summary>a/1 에 2개, a/2, b/1, c 에 1개씩 업로드를 만든다.</summary>
		private static void CreateUploads(S3Client client, string bucketName)
		{
			foreach (var key in new[] { "a/1", "a/1", "a/2", "b/1", "c" })
				client.InitiateMultipartUpload(bucketName, key);
		}

		private static List<string> GetUploadKeys(ListMultipartUploadsResponse response)
			=> (response.MultipartUploads ?? []).ConvertAll(upload => upload.Key);

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "ListMultipartUploads의 prefix와 delimiter 동작 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListMultipartUploadsPrefixDelimiter()
		{
			TestId = 61;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CreateUploads(client, bucketName);

			var response = client.ListMultipartUploads(bucketName);
			Assert.Equal(["a/1", "a/1", "a/2", "b/1", "c"], GetUploadKeys(response));
			Assert.False(response.IsTruncated);
			Assert.Equal(1000, response.MaxUploads);

			response = client.ListMultipartUploads(bucketName, prefix: "a/");
			Assert.Equal(["a/1", "a/1", "a/2"], GetUploadKeys(response));

			response = client.ListMultipartUploads(bucketName, delimiter: "/");
			Assert.Equal(["c"], GetUploadKeys(response));
			Assert.Equal(["a/", "b/"], response.CommonPrefixes);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "ListMultipartUploads의 키 마커와 업로드 ID 마커로 이어서 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListMultipartUploadsPagination()
		{
			TestId = 62;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CreateUploads(client, bucketName);

			var response = client.ListMultipartUploads(bucketName, maxUploads: 2);
			Assert.Equal(["a/1", "a/1"], GetUploadKeys(response));
			Assert.True(response.IsTruncated);
			Assert.Equal("a/1", response.NextKeyMarker);
			Assert.Equal(response.MultipartUploads[1].UploadId, response.NextUploadIdMarker);

			response = client.ListMultipartUploads(bucketName, maxUploads: 2,
				keyMarker: response.NextKeyMarker, uploadIdMarker: response.NextUploadIdMarker);
			Assert.Equal(["a/2", "b/1"], GetUploadKeys(response));
			Assert.True(response.IsTruncated);

			// 키 마커만 주면 그 키의 업로드는 모두 건너뛴다
			response = client.ListMultipartUploads(bucketName, keyMarker: "a/1");
			Assert.Equal(["a/2", "b/1", "c"], GetUploadKeys(response));
			Assert.False(response.IsTruncated);
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "ListMultipartUploads의 max-uploads 경계값 동작 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListMultipartUploadsMaxUploads()
		{
			TestId = 63;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CreateUploads(client, bucketName);

			var response = client.ListMultipartUploads(bucketName, maxUploads: 0);
			Assert.Empty(GetUploadKeys(response));
			Assert.False(response.IsTruncated);

			// 1000 보다 큰 값은 1000 으로 낮춘다
			Assert.Equal(1000, client.ListMultipartUploads(bucketName, maxUploads: 1001).MaxUploads);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.ListMultipartUploads(bucketName, maxUploads: -1));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "UploadPartCopy의 원본 키/원본 버킷/업로드 ID가 없으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopySourceNotExist()
		{
			TestId = 64;
			var key = "TestUploadPartCopySourceNotExist";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, "source", body: SmallPart);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			// 래퍼가 범위를 항상 보내므로 원본 전체(0-9)를 지정한다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY,
				() => client.CopyPart(bucketName, "not_exist", bucketName, key, uploadId, 1, 0, 9));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET,
				() => client.CopyPart(GetNewBucketName(false), "source", bucketName, key, uploadId, 1, 0, 9));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_UPLOAD,
				() => client.CopyPart(bucketName, "source", bucketName, key, "invalid", 1, 0, 9));
			Assert.Empty(GetPartNumbers(client.ListParts(bucketName, key, uploadId)));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "UploadPartCopy의 범위 형식이 bytes=시작-끝이 아니면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestUploadPartCopyInvalidRangeFormat()
		{
			TestId = 65;
			var key = "TestUploadPartCopyInvalidRangeFormat";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutObject(bucketName, "source", body: SmallPart);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;

			// bytes=시작-끝 형식만 허용한다(뒤에서부터의 길이 지정 불가)
			foreach (var value in new[] { "bytes=abc", "bytes=-5" })
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
					() => client.CopyPart(bucketName, "source", bucketName, key, uploadId, 1, 0, 9, copySourceRange: value));
		}

		[Fact]
		[Trait(MainData.Major, "Multipart")]
		[Trait(MainData.Minor, "PartNumber")]
		[Trait(MainData.Explanation, "멀티파트 오브젝트를 partNumber로 조회하면 그 파트만 206으로 돌려주는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartGetPartNumber()
		{
			TestId = 66;
			var key = "TestMultipartGetPartNumber";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key).UploadId;
			var bodies = new[] { new string('a', 5 * MainData.MB), new string('b', 10) };
			var parts = new List<PartETag>();
			for (var index = 0; index < bodies.Length; index++)
				parts.Add(new PartETag(index + 1, client.UploadPart(bucketName, key, uploadId, bodies[index], index + 1).ETag));
			client.CompleteMultipartUpload(bucketName, key, uploadId, parts);
			var first = bodies[0].Length;
			var total = bodies[0].Length + bodies[1].Length;

			// partNumber 를 주면 그 파트만 206 으로 돌려주고 전체 파트 수를 알려준다
			var headResponse = client.HeadObject(bucketName, key, partNumber: 1);
			Assert.Equal(HttpStatusCode.PartialContent, headResponse.HttpStatusCode);
			Assert.Equal(first, headResponse.ContentLength);
			Assert.Equal(2, headResponse.PartsCount);
			Assert.Equal($"bytes 0-{first - 1}/{total}", headResponse.ContentRange);

			var response = client.GetObject(bucketName, key, partNumber: 2);
			Assert.Equal(HttpStatusCode.PartialContent, response.HttpStatusCode);
			Assert.Equal(2, response.PartsCount);
			Assert.Equal($"bytes {first}-{total - 1}/{total}", response.ContentRange);
			Assert.Equal(bodies[1], GetBody(response));

			AssertS3Error(HttpStatusCode.RequestedRangeNotSatisfiable, MainData.INVALID_PART_NUMBER,
				() => client.GetObject(bucketName, key, partNumber: 3));
		}
	}
}
