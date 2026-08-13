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
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Ownership : TestBase
	{
		public Ownership(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷 소유권 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetBucketOwnership()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			client.GetBucketOwnershipControls(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "소유권 설정으로 버킷 생성")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCreateBucketWithOwnership()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var response = client.GetBucketOwnershipControls(bucketName);
			Assert.Equal(ObjectOwnership.BucketOwnerEnforced,
				response.OwnershipControls.Rules[0].ObjectOwnership);
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 소유권 설정 변경")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestChangeBucketOwnership()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var response = client.GetBucketOwnershipControls(bucketName);
			Assert.Equal(ObjectOwnership.BucketOwnerEnforced,
				response.OwnershipControls.Rules[0].ObjectOwnership);

			client.PutBucketOwnershipControls(bucketName, new OwnershipControls
			{
				Rules = [new OwnershipControlsRule { ObjectOwnership = ObjectOwnership.BucketOwnerPreferred }]
			});

			response = client.GetBucketOwnershipControls(bucketName);
			Assert.Equal(ObjectOwnership.BucketOwnerPreferred,
				response.OwnershipControls.Rules[0].ObjectOwnership);
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "BucketOwnerEnforced 버킷 ACL 설정 거부")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipDenyACL()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var response = client.GetBucketOwnershipControls(bucketName);
			Assert.Equal(ObjectOwnership.BucketOwnerEnforced,
				response.OwnershipControls.Rules[0].ObjectOwnership);

			var e = Assert.Throws<AggregateException>(() =>
				client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "BucketOwnerEnforced 버킷 객체 ACL 설정 거부")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipDenyObjectACL()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var key = "TestBucketOwnershipDenyObjectACL";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() =>
				client.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "소유권 변경 후 공개 객체 접근 유지")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectOwnershipDenyChange()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "TestObjectOwnershipDenyChange";

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicRead);

			var publicClient = GetPublicClient();
			publicClient.GetObjectMetadata(bucketName, key);

			client.PutBucketOwnershipControls(bucketName, new OwnershipControls
			{
				Rules = [new OwnershipControlsRule { ObjectOwnership = ObjectOwnership.BucketOwnerEnforced }]
			});

			publicClient.GetObjectMetadata(bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "BucketOwnerEnforced 이후 객체 ACL 변경 거부")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectOwnershipDenyACL()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "TestObjectOwnershipDenyACL";

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicRead);

			client.PutBucketOwnershipControls(bucketName, new OwnershipControls
			{
				Rules = [new OwnershipControlsRule { ObjectOwnership = ObjectOwnership.BucketOwnerEnforced }]
			});

			var e = Assert.Throws<AggregateException>(() =>
				client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, GetErrorCode(e));
		}
	}
}
