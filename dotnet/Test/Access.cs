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
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Access : TestBase
	{
		public Access(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "[BlockPublicAcls, BlockPublicPolicy] 접근권한블록이 정상적으로 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicAclAndPolicy()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = false,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var response = client.GetPublicAccessBlock(bucketName);
			Assert.Equal(access.BlockPublicAcls, response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(access.BlockPublicPolicy, response.PublicAccessBlockConfiguration.BlockPublicPolicy);

			// 여러 acl 테스트
			var aclList = new[] {
				S3CannedACL.PublicRead,
				S3CannedACL.PublicReadWrite,
				S3CannedACL.AuthenticatedRead
			};

			foreach (var acl in aclList)
			{
				var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: acl));
				Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
				Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicPutBucketAcls()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = false,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var response = client.GetPublicAccessBlock(bucketName);
			Assert.Equal(access.BlockPublicAcls, response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(access.BlockPublicPolicy, response.PublicAccessBlockConfiguration.BlockPublicPolicy);

			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: S3CannedACL.PublicReadWrite));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: S3CannedACL.AuthenticatedRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicObjectCannedAcls()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = false,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var response = client.GetPublicAccessBlock(bucketName);
			Assert.Equal(access.BlockPublicAcls, response.PublicAccessBlockConfiguration.BlockPublicAcls);

			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, "foo1", body: "", acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, "foo2", body: "", acl: S3CannedACL.PublicReadWrite));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, "foo3", body: "", acl: S3CannedACL.AuthenticatedRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicPolicy()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = false,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var resource = S3Utils.MakeArnResource(string.Format("{0}/*", bucketName));
			var policyDocument = S3Utils.MakeJsonPolicy("s3:GetObject", resource);
			var e = Assert.Throws<AggregateException>(() => client.PutBucketPolicy(bucketName, policyDocument.ToString()));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "[IgnorePublicAcls] 접근권한블록이 정상적으로 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestIgnorePublicAcls()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var altClient = GetAltClient();

			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
			altClient.ListObjects(bucketName);

			client.PutObject(bucketName, "key1", body: "abcde", acl: S3CannedACL.PublicRead);
			var response = altClient.GetObject(bucketName, "key1");
			Assert.Equal("abcde", S3Utils.GetBody(response));

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = true,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);

			var unauthenticatedClient = GetUnauthenticatedClient();
			Assert.Throws<AggregateException>(() => unauthenticatedClient.ListObjects(bucketName));
			Assert.Throws<AggregateException>(() => unauthenticatedClient.GetObject(bucketName, "key1"));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록 삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeletePublicBlock()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = true,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var response = client.GetPublicAccessBlock(bucketName);
			Assert.Equal(access.BlockPublicAcls, response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(access.BlockPublicPolicy, response.PublicAccessBlockConfiguration.BlockPublicPolicy);
			Assert.Equal(access.IgnorePublicAcls, response.PublicAccessBlockConfiguration.IgnorePublicAcls);
			Assert.Equal(access.RestrictPublicBuckets, response.PublicAccessBlockConfiguration.RestrictPublicBuckets);

			var delResponse = client.DeletePublicAccessBlock(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, delResponse.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => client.GetPublicAccessBlock(bucketName));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록 설정 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutPublicBlock()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			PublicAccessBlockConfiguration access = new()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = true,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			client.PutPublicAccessBlock(bucketName, access);

			var response = client.GetPublicAccessBlock(bucketName);
			Assert.Equal(access.BlockPublicAcls, response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(access.BlockPublicPolicy, response.PublicAccessBlockConfiguration.BlockPublicPolicy);
			Assert.Equal(access.IgnorePublicAcls, response.PublicAccessBlockConfiguration.IgnorePublicAcls);
			Assert.Equal(access.RestrictPublicBuckets, response.PublicAccessBlockConfiguration.RestrictPublicBuckets);
		}
	}
}
