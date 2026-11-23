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

namespace s3tests.Test
{
	public class DeleteBucket : TestBase
	{
		public DeleteBucket(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteNotExist()
		{
			TestId = 1;
			var bucketName = GetNewBucketName(false);
			var client = GetClient();

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(exception));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(exception));
		}
		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteNonempty()
		{
			TestId = 2;
			var bucketName = SetupObjects(["foo"]);
			var client = GetClient();

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(exception));
			Assert.Equal(MainData.BUCKET_NOT_EMPTY, GetErrorCode(exception));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateDelete()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.DeleteBucket(bucketName);

			var exception = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(exception));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(exception));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "버저닝 버킷에 이전 버전과 삭제 마커만 남아 있어도 버킷 삭제가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteVersioned()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestBucketDeleteVersioned";
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: key);
			client.DeleteObject(bucketName, key);

			// 현재 오브젝트가 없어도 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없다
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount ?? 0);
			var e = Assert.Throws<AggregateException>(() => client.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.BUCKET_NOT_EMPTY, GetErrorCode(e));

			// SDK는 버전과 삭제 마커를 Versions 하나로 합쳐서 돌려준다
			var versions = client.ListVersions(bucketName);
			foreach (var version in versions.Versions ?? [])
				client.DeleteObject(bucketName, version.Key, versionId: version.VersionId);
			client.DeleteBucket(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketDeleteWithMultipartUpload()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestBucketDeleteWithMultipartUpload";
			var upload = client.InitiateMultipartUpload(bucketName, key);
			client.UploadPart(bucketName, key, upload.UploadId, new string('a', 1024), 1);

			// 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있다
			client.DeleteBucket(bucketName);
			var e = Assert.Throws<AggregateException>(() => client.ListObjectsV2(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "다른 사용자가 버킷을 삭제하려 했을 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketDeleteAltUser()
		{
			TestId = 6;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			var e = Assert.Throws<AggregateException>(() => altClient.DeleteBucket(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			// 버킷은 그대로 남아 있다
			client.HeadBucket(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷 설정(태그, CORS, Lifecycle)이 남아 있어도 비어 있으면 삭제할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketDeleteWithConfigurations()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketTagging(bucketName, [new Tag { Key = "key", Value = "value" }]);
			client.PutCORSConfiguration(bucketName, new CORSConfiguration
			{
				Rules = [new CORSRule { AllowedMethods = ["GET"], AllowedOrigins = ["*"] }]
			});
			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration
			{
				Rules =
				[
					new LifecycleRule
					{
						Id = "rule1",
						Status = LifecycleRuleStatus.Enabled,
						Filter = new LifecycleFilter { LifecycleFilterPredicate = new LifecyclePrefixPredicate { Prefix = "" } },
						Expiration = new LifecycleRuleExpiration { Days = 1 }
					}
				]
			});

			// 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있다
			client.DeleteBucket(bucketName);
			var e = Assert.Throws<AggregateException>(() => client.ListObjectsV2(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteBucket")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷을 삭제하고 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않은지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketDeleteRecreate()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestBucketDeleteRecreate";
			client.PutBucketTagging(bucketName, [new Tag { Key = "key", Value = "value" }]);
			client.PutObject(bucketName, key, body: key);
			client.DeleteObject(bucketName, key);
			client.DeleteBucket(bucketName);

			// 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않다
			client.PutBucket(bucketName);
			Assert.Equal(0, client.ListObjectsV2(bucketName).KeyCount ?? 0);
			var e = Assert.Throws<AggregateException>(() => client.GetBucketTagging(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_TAG_SET, GetErrorCode(e));
		}
	}
}
