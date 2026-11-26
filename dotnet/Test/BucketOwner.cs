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
	public class BucketOwner : TestBase
	{
		public BucketOwner(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소유자 아이디로 HeadBucket이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerHeadBucket()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.HeadBucket(bucketName, expectedBucketOwner: Owner);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 HeadBucket을 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerHeadBucketMismatch()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.HeadBucket(bucketName, expectedBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소유자 아이디로 ListObjectsV2가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerListObjectsV2()
		{
			TestId = 3;
			var key = "TestBucketOwnerListObjectsV2";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			var response = client.ListObjectsV2(bucketName, expectedBucketOwner: Owner);
			Assert.Equal(1, response.KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 ListObjectsV2를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerListObjectsV2Mismatch()
		{
			TestId = 4;
			var key = "TestBucketOwnerListObjectsV2Mismatch";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.ListObjectsV2(bucketName, expectedBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소유자 아이디로 오브젝트 업로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerPutObject()
		{
			TestId = 5;
			var key = "TestBucketOwnerPutObject";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: key, expectedBucketOwner: Owner);

			SucceedGetObject(client, bucketName, key, key);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 오브젝트 업로드를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerPutObjectMismatch()
		{
			TestId = 6;
			var key = "TestBucketOwnerPutObjectMismatch";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: key, expectedBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소유자 아이디로 오브젝트 다운로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerGetObject()
		{
			TestId = 7;
			var key = "TestBucketOwnerGetObject";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			using var response = client.GetObject(bucketName, key, expectedBucketOwner: Owner);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 오브젝트 다운로드를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerGetObjectMismatch()
		{
			TestId = 8;
			var key = "TestBucketOwnerGetObjectMismatch";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, expectedBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소유자 아이디로 오브젝트 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerDeleteObject()
		{
			TestId = 9;
			var key = "TestBucketOwnerDeleteObject";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			client.DeleteObject(bucketName, key, expectedBucketOwner: Owner);

			FailedGetObject(client, bucketName, key, HttpStatusCode.NotFound, MainData.NO_SUCH_KEY);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 오브젝트 삭제를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerDeleteObjectMismatch()
		{
			TestId = 10;
			var key = "TestBucketOwnerDeleteObjectMismatch";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, expectedBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			SucceedGetObject(client, bucketName, key, key);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소스/대상 소유자 아이디로 오브젝트 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestSourceBucketOwnerCopyObject()
		{
			TestId = 11;
			var source = "TestSourceBucketOwnerCopyObjectSource";
			var target = "TestSourceBucketOwnerCopyObjectTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			client.CopyObject(sourceBucket, source, targetBucket, target,
				expectedBucketOwner: Owner, expectedSourceBucketOwner: Owner);

			SucceedGetObject(client, targetBucket, target, source);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소스 소유자 아이디로 오브젝트 복사를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestSourceBucketOwnerCopyObjectMismatch()
		{
			TestId = 12;
			var source = "TestSourceBucketOwnerCopyObjectMismatchSource";
			var target = "TestSourceBucketOwnerCopyObjectMismatchTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(sourceBucket, source, targetBucket, target,
				expectedBucketOwner: Owner, expectedSourceBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			FailedGetObject(client, targetBucket, target, HttpStatusCode.NotFound, MainData.NO_SUCH_KEY);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 대상 소유자 아이디로 오브젝트 복사를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerCopyObjectMismatch()
		{
			TestId = 13;
			var source = "TestBucketOwnerCopyObjectMismatchSource";
			var target = "TestBucketOwnerCopyObjectMismatchTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(sourceBucket, source, targetBucket, target,
				expectedBucketOwner: WrongOwner, expectedSourceBucketOwner: Owner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			FailedGetObject(client, targetBucket, target, HttpStatusCode.NotFound, MainData.NO_SUCH_KEY);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "올바른 소스/대상 소유자 아이디로 파트 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestSourceBucketOwnerUploadPartCopy()
		{
			TestId = 14;
			var source = "TestSourceBucketOwnerUploadPartCopySource";
			var target = "TestSourceBucketOwnerUploadPartCopyTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			var uploadId = client.InitiateMultipartUpload(targetBucket, target).UploadId;

			var response = client.CopyPart(sourceBucket, source, targetBucket, target, uploadId, 1, 0, source.Length - 1,
				expectedBucketOwner: Owner, expectedSourceBucketOwner: Owner);
			Assert.NotNull(response.ETag);

			client.AbortMultipartUpload(targetBucket, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 소스 소유자 아이디로 파트 복사를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestSourceBucketOwnerUploadPartCopyMismatch()
		{
			TestId = 15;
			var source = "TestSourceBucketOwnerUploadPartCopyMismatchSource";
			var target = "TestSourceBucketOwnerUploadPartCopyMismatchTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			var uploadId = client.InitiateMultipartUpload(targetBucket, target).UploadId;

			var e = Assert.Throws<AggregateException>(() => client.CopyPart(sourceBucket, source, targetBucket, target,
				uploadId, 1, 0, source.Length - 1,
				expectedBucketOwner: Owner, expectedSourceBucketOwner: WrongOwner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.AbortMultipartUpload(targetBucket, target, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 대상 소유자 아이디로 파트 복사를 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerUploadPartCopyMismatch()
		{
			TestId = 16;
			var source = "TestBucketOwnerUploadPartCopyMismatchSource";
			var target = "TestBucketOwnerUploadPartCopyMismatchTarget";
			var sourceBucket = SetupObjects([source]);
			var client = GetClient();
			var targetBucket = GetNewBucket(client);

			var uploadId = client.InitiateMultipartUpload(targetBucket, target).UploadId;

			var e = Assert.Throws<AggregateException>(() => client.CopyPart(sourceBucket, source, targetBucket, target,
				uploadId, 1, 0, source.Length - 1,
				expectedBucketOwner: WrongOwner, expectedSourceBucketOwner: Owner));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.AbortMultipartUpload(targetBucket, target, uploadId);
		}

		#region 그 밖의 API (AWS 실측 기준)
		// x-amz-expected-bucket-owner 는 버킷·오브젝트를 다루는 거의 모든 API 가 받는다.
		// 소유자가 다르면 다른 검사보다 먼저 403 AccessDenied 다.

		private static void AssertOwnerMismatch(Action call) => AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, call);

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "목록 조회 API(ListObjects, ListObjectVersions, ListMultipartUploads)가 소유자 아이디를 확인하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerListOperations()
		{
			TestId = 17;
			var key = "TestBucketOwnerListOperations";
			var bucketName = SetupObjects([key]);
			var client = GetClient();
			client.InitiateMultipartUpload(bucketName, "multi");

			Assert.Single(client.ListObjects(bucketName, expectedBucketOwner: Owner).S3Objects);
			Assert.Single(client.ListVersions(bucketName, expectedBucketOwner: Owner).Versions);
			Assert.Single(client.ListMultipartUploads(bucketName, expectedBucketOwner: Owner).MultipartUploads);

			AssertOwnerMismatch(() => client.ListObjects(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.ListVersions(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.ListMultipartUploads(bucketName, expectedBucketOwner: WrongOwner));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "버킷 설정 API(Location, ACL, Versioning)가 소유자 아이디를 확인하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerBucketConfig()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.GetBucketLocation(bucketName, expectedBucketOwner: Owner);
			client.GetBucketACL(bucketName, expectedBucketOwner: Owner);
			AssertOwnerMismatch(() => client.GetBucketLocation(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.GetBucketACL(bucketName, expectedBucketOwner: WrongOwner));

			// 소유자가 다르면 설정이 바뀌지 않는다
			AssertOwnerMismatch(() => client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled, expectedBucketOwner: WrongOwner));
			Assert.Equal(VersionStatus.Off, client.GetBucketVersioning(bucketName, expectedBucketOwner: Owner).VersioningConfig.Status);
			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled, expectedBucketOwner: Owner);
			AssertOwnerMismatch(() => client.GetBucketVersioning(bucketName, expectedBucketOwner: WrongOwner));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "버킷 태그·CORS 설정/조회/삭제가 소유자 아이디를 확인하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerBucketTaggingCors()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var tagSet = new List<Tag>() { new() { Key = "a", Value = "b" } };
			var cors = new CORSConfiguration() { Rules = [new CORSRule() { AllowedMethods = ["GET"], AllowedOrigins = ["*"] }] };

			AssertOwnerMismatch(() => client.PutBucketTagging(bucketName, tagSet, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.PutCORSConfiguration(bucketName, cors, expectedBucketOwner: WrongOwner));
			client.PutBucketTagging(bucketName, tagSet, expectedBucketOwner: Owner);
			client.PutCORSConfiguration(bucketName, cors, expectedBucketOwner: Owner);
			CheckTagSet(tagSet, client.GetBucketTagging(bucketName, expectedBucketOwner: Owner).TagSet);
			Assert.Single(client.GetCORSConfiguration(bucketName, expectedBucketOwner: Owner).Configuration.Rules);

			AssertOwnerMismatch(() => client.GetBucketTagging(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.GetCORSConfiguration(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.DeleteBucketTagging(bucketName, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.DeleteCORSConfiguration(bucketName, expectedBucketOwner: WrongOwner));
			// 삭제되지 않았다
			CheckTagSet(tagSet, client.GetBucketTagging(bucketName).TagSet);
			client.DeleteBucketTagging(bucketName, expectedBucketOwner: Owner);
			client.DeleteCORSConfiguration(bucketName, expectedBucketOwner: Owner);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketTagging(bucketName), MainData.NO_SUCH_TAG_SET);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "설정이 없는 버킷에서 소유자가 맞으면 404, 다르면 403인지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerNotConfigured()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var requests = new List<(Func<string, AmazonWebServiceResponse> Request, string ErrorCode)>()
			{
				(o => client.GetBucketPolicy(bucketName, expectedBucketOwner: o), MainData.NO_SUCH_BUCKET_POLICY),
				(o => client.GetLifecycleConfiguration(bucketName, expectedBucketOwner: o), MainData.NO_SUCH_LIFECYCLE_CONFIGURATION),
				(o => client.GetCORSConfiguration(bucketName, expectedBucketOwner: o), MainData.NO_SUCH_CORS_CONFIGURATION),
				(o => client.GetBucketWebsite(bucketName, expectedBucketOwner: o), MainData.NO_SUCH_WEBSITE_CONFIGURATION),
			};
			foreach (var (request, errorCode) in requests)
			{
				// 소유자가 맞으면 설정 없음(404), 다르면 설정을 보기 전에 403 이다
				CheckErrorResponse(HttpStatusCode.NotFound, () => request(Owner), errorCode);
				AssertOwnerMismatch(() => request(WrongOwner));
			}
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "오브젝트 조회 API(HeadObject, GetObjectAcl, GetObjectAttributes)가 소유자 아이디를 확인하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerObjectRead()
		{
			TestId = 21;
			var key = "TestBucketOwnerObjectRead";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			Assert.Equal(key.Length, client.HeadObject(bucketName, key, expectedBucketOwner: Owner).ContentLength);
			Assert.Single(client.GetObjectACL(bucketName, key, expectedBucketOwner: Owner).AccessControlList.Grants);
			var response = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ObjectSize], expectedBucketOwner: Owner);
			Assert.Equal(key.Length, response.ObjectSize);

			AssertS3Error(HttpStatusCode.Forbidden, null, () => client.HeadObject(bucketName, key, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.GetObjectACL(bucketName, key, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ObjectSize], expectedBucketOwner: WrongOwner));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "오브젝트 태그 설정/조회/삭제가 소유자 아이디를 확인하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerObjectTagging()
		{
			TestId = 22;
			var key = "TestBucketOwnerObjectTagging";
			var bucketName = SetupObjects([key]);
			var client = GetClient();
			var tagging = new Tagging() { TagSet = [new Tag() { Key = "a", Value = "b" }] };

			AssertOwnerMismatch(() => client.PutObjectTagging(bucketName, key, tagging, expectedBucketOwner: WrongOwner));
			Assert.Empty(client.GetObjectTagging(bucketName, key, expectedBucketOwner: Owner).Tagging ?? []);
			client.PutObjectTagging(bucketName, key, tagging, expectedBucketOwner: Owner);

			AssertOwnerMismatch(() => client.GetObjectTagging(bucketName, key, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.DeleteObjectTagging(bucketName, key, expectedBucketOwner: WrongOwner));
			CheckTagSet(tagging.TagSet, client.GetObjectTagging(bucketName, key, expectedBucketOwner: Owner).Tagging);
			client.DeleteObjectTagging(bucketName, key, expectedBucketOwner: Owner);
			Assert.Empty(client.GetObjectTagging(bucketName, key).Tagging ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "없는 키를 조회할 때 소유자가 맞으면 404, 다르면 403인지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerKeyNotExist()
		{
			TestId = 23;
			var key = "TestBucketOwnerKeyNotExist";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 소유자가 맞으면 없는 키(404), 다르면 키를 보기 전에 403 이다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.GetObject(bucketName, key, expectedBucketOwner: Owner));
			AssertOwnerMismatch(() => client.GetObject(bucketName, key, expectedBucketOwner: WrongOwner));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로 DeleteObjects를 하면 하나도 지워지지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerDeleteObjects()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			SetupObjects(["a", "b"], bucketName);
			var delete = new List<KeyVersion>() { new() { Key = "a" }, new() { Key = "b" } };

			// 요청 전체가 거부되고 하나도 지워지지 않는다
			AssertOwnerMismatch(() => client.DeleteObjects(bucketName, delete, expectedBucketOwner: WrongOwner));
			Assert.Equal(2, client.ListObjectsV2(bucketName).KeyCount);

			var response = client.DeleteObjects(bucketName, delete, expectedBucketOwner: Owner);
			Assert.Equal(2, response.DeletedObjects.Count);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount);
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "멀티파트 API 전체가 소유자 아이디를 확인하고 거부된 요청은 업로드를 건드리지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerMultipart()
		{
			TestId = 25;
			var key = "TestBucketOwnerMultipart";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			AssertOwnerMismatch(() => client.InitiateMultipartUpload(bucketName, key, expectedBucketOwner: WrongOwner));
			var uploadId = client.InitiateMultipartUpload(bucketName, key, expectedBucketOwner: Owner).UploadId;

			AssertOwnerMismatch(() => client.UploadPart(bucketName, key, uploadId, "content", 1, expectedBucketOwner: WrongOwner));
			var etag = client.UploadPart(bucketName, key, uploadId, "content", 1, expectedBucketOwner: Owner).ETag;

			AssertOwnerMismatch(() => client.ListParts(bucketName, key, uploadId, expectedBucketOwner: WrongOwner));
			var response = client.ListParts(bucketName, key, uploadId, expectedBucketOwner: Owner);
			Assert.Single(response.Parts);

			var parts = new List<PartETag>() { new(1, etag) };
			AssertOwnerMismatch(() => client.AbortMultipartUpload(bucketName, key, uploadId, expectedBucketOwner: WrongOwner));
			AssertOwnerMismatch(() => client.CompleteMultipartUpload(bucketName, key, uploadId, parts, expectedBucketOwner: WrongOwner));
			// 거부된 중단·완료는 업로드를 건드리지 않는다
			client.CompleteMultipartUpload(bucketName, key, uploadId, parts, expectedBucketOwner: Owner);
			SucceedGetObject(client, bucketName, key, "content");
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "잘못된 소유자 아이디로는 버킷을 지울 수 없고 올바른 소유자 아이디로는 지워지는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerDeleteBucket()
		{
			TestId = 26;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertOwnerMismatch(() => client.DeleteBucket(bucketName, expectedBucketOwner: WrongOwner));
			client.HeadBucket(bucketName);

			client.DeleteBucket(bucketName, expectedBucketOwner: Owner);
			AssertS3Error(HttpStatusCode.NotFound, null, () => client.HeadBucket(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "버킷이 없으면 소유자 값과 상관없이 404 NoSuchBucket인지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerBucketNotExist()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			// 버킷이 없으면 소유자 값과 상관없이 404 다
			foreach (var owner in new[] { Owner, WrongOwner })
				AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.ListObjectsV2(bucketName, expectedBucketOwner: owner));
		}

		[Fact]
		[Trait(MainData.Major, "BucketOwner")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "소유자 아이디가 맞아도 다른 사용자에게 권한이 생기지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnerOtherUser()
		{
			TestId = 28;
			var key = "TestBucketOwnerOtherUser";
			var bucketName = SetupObjects([key]);
			var altClient = GetAltClient();
			// 소유자 값이 맞아도 권한이 생기지는 않는다
			foreach (var owner in new[] { Owner, WrongOwner })
			{
				AssertOwnerMismatch(() => altClient.ListObjectsV2(bucketName, expectedBucketOwner: owner));
				AssertOwnerMismatch(() => altClient.GetObject(bucketName, key, expectedBucketOwner: owner));
			}
		}

		private static void CheckTagSet(List<Tag> expected, List<Tag> actual)
		{
			Assert.Equal(expected.Count, actual.Count);
			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].Key, actual[i].Key);
				Assert.Equal(expected[i].Value, actual[i].Value);
			}
		}

		#endregion
	}
}
