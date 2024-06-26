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

namespace s3tests
{
	public class Access : TestBase
	{
		public Access(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_put_public_block")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록 설정 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_public_block()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = true,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			var Response = Client.GetPublicAccessBlock(BucketName);
			Assert.Equal(AccessConf.BlockPublicAcls, Response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(AccessConf.BlockPublicPolicy, Response.PublicAccessBlockConfiguration.BlockPublicPolicy);
			Assert.Equal(AccessConf.IgnorePublicAcls, Response.PublicAccessBlockConfiguration.IgnorePublicAcls);
			Assert.Equal(AccessConf.RestrictPublicBuckets, Response.PublicAccessBlockConfiguration.RestrictPublicBuckets);
		}

		[Fact(DisplayName = "test_block_public_put_bucket_acls")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_block_public_put_bucket_acls()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = false,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			var Response = Client.GetPublicAccessBlock(BucketName);
			Assert.Equal(AccessConf.BlockPublicAcls, Response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(AccessConf.BlockPublicPolicy, Response.PublicAccessBlockConfiguration.BlockPublicPolicy);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicReadWrite));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => Client.PutBucketACL(BucketName, ACL: S3CannedACL.AuthenticatedRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact(DisplayName = "test_block_public_object_canned_acls")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_block_public_object_canned_acls()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = false,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			var Response = Client.GetPublicAccessBlock(BucketName);
			Assert.Equal(AccessConf.BlockPublicAcls, Response.PublicAccessBlockConfiguration.BlockPublicAcls);

			var e = Assert.Throws<AggregateException>(() => Client.PutObject(BucketName, "foo1", Body: "", ACL: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => Client.PutObject(BucketName, "foo2", Body: "", ACL: S3CannedACL.PublicReadWrite));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));

			e = Assert.Throws<AggregateException>(() => Client.PutObject(BucketName, "foo3", Body: "", ACL: S3CannedACL.AuthenticatedRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact(DisplayName = "test_block_public_policy")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_block_public_policy()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = false,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			var Resource = MakeArnResource(string.Format("{0}/*", BucketName));
			var PolicyDocument = MakeJsonPolicy("s3:GetObject", Resource);
			var e = Assert.Throws<AggregateException>(() => Client.PutBucketPolicy(BucketName, PolicyDocument.ToString()));
		}

		[Fact(DisplayName = "test_ignore_public_acls")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한블록으로 개인버킷처럼 설정한뒤 " +
									 "버킷의acl권한을 public-read로 변경해도 적용되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_ignore_public_acls()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var AltClient = GetAltClient();

			Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead);
			AltClient.ListObjects(BucketName);

			Client.PutObject(BucketName, "key1", Body: "abcde", ACL: S3CannedACL.PublicRead);
			var Response = AltClient.GetObject(BucketName, "key1");
			Assert.Equal("abcde", GetBody(Response));

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = true,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			Client.PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead);


			var UnauthenticatedClient = GetUnauthenticatedClient();
			Assert.Throws<AggregateException>(() => UnauthenticatedClient.ListObjects(BucketName));
			Assert.Throws<AggregateException>(() => UnauthenticatedClient.GetObject(BucketName, "key1"));
		}

		[Fact(DisplayName = "test_delete_public_block")]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록 삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_delete_public_block()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var AccessConf = new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = true,
				IgnorePublicAcls = true,
				BlockPublicPolicy = true,
				RestrictPublicBuckets = false
			};
			Client.PutPublicAccessBlock(BucketName, AccessConf);

			var Response = Client.GetPublicAccessBlock(BucketName);
			Assert.Equal(AccessConf.BlockPublicAcls, Response.PublicAccessBlockConfiguration.BlockPublicAcls);
			Assert.Equal(AccessConf.BlockPublicPolicy, Response.PublicAccessBlockConfiguration.BlockPublicPolicy);
			Assert.Equal(AccessConf.IgnorePublicAcls, Response.PublicAccessBlockConfiguration.IgnorePublicAcls);
			Assert.Equal(AccessConf.RestrictPublicBuckets, Response.PublicAccessBlockConfiguration.RestrictPublicBuckets);

			var DelResponse = Client.DeletePublicAccessBlock(BucketName);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

			var e = Assert.Throws<AggregateException>(() => Client.GetPublicAccessBlock(BucketName));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchPublicAccessBlockConfiguration, GetErrorCode(e));
		}
	}
}
