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
using Newtonsoft.Json.Linq;
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
			TestId = 1;
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
		[Trait(MainData.Explanation, "버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicAcls()
		{
			TestId = 2;
			var key = "TestBlockPublicAcls";
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.ObjectWriter);

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

			S3CannedACL[] publicAcls = [S3CannedACL.PublicRead, S3CannedACL.PublicReadWrite, S3CannedACL.AuthenticatedRead];

			foreach (var acl in publicAcls)
			{
				var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: key, acl: acl));
				Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
				Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicPolicy()
		{
			TestId = 3;
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
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var altClient = GetAltClient();

			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
			altClient.ListObjects(bucketName);

			var key = nameof(TestIgnorePublicAcls);
			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicRead);
			var response = altClient.GetObject(bucketName, key);
			Assert.Equal(key, S3Utils.GetBody(response));

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
			Assert.Throws<AggregateException>(() => unauthenticatedClient.GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 접근권한 블록 삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeletePublicBlock()
		{
			TestId = 4;
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
			TestId = 6;
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

		/// <summary>지정하지 않은 항목은 모두 false 인 퍼블릭 액세스 차단 설정을 건다.</summary>
		private static void PutPublicBlock(S3Client client, string bucketName, bool blockPublicAcls = false,
			bool ignorePublicAcls = false, bool blockPublicPolicy = false, bool restrictPublicBuckets = false)
			=> client.PutPublicAccessBlock(bucketName, new PublicAccessBlockConfiguration
			{
				BlockPublicAcls = blockPublicAcls,
				IgnorePublicAcls = ignorePublicAcls,
				BlockPublicPolicy = blockPublicPolicy,
				RestrictPublicBuckets = restrictPublicBuckets
			});

		private static HttpStatusCode GetObjectStatus(S3Client client, string bucketName, string key)
			=> GetCallStatus(() => client.GetObject(bucketName, key).Dispose());

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "새로 만든 버킷은 접근권한 블록 네 가지가 모두 켜진 상태인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPublicBlockDefault()
		{
			TestId = 7;
			var client = GetClient();
			// GetNewBucket(client)은 퍼블릭 액세스 차단 설정을 건드리지 않고 기본값 그대로 버킷을 만든다
			var bucketName = GetNewBucket(client);
			// 새 버킷은 네 가지 차단이 모두 켜진 상태로 만들어진다
			var config = client.GetPublicAccessBlock(bucketName).PublicAccessBlockConfiguration;
			Assert.True(config.BlockPublicAcls);
			Assert.True(config.IgnorePublicAcls);
			Assert.True(config.BlockPublicPolicy);
			Assert.True(config.RestrictPublicBuckets);
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "접근권한 블록을 일부 항목만 지정해 설정하면 나머지는 false로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutPublicBlockPartial()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 지정하지 않은 항목은 false 로 저장된다
			client.PutPublicAccessBlock(bucketName, new PublicAccessBlockConfiguration { BlockPublicAcls = true });
			var config = client.GetPublicAccessBlock(bucketName).PublicAccessBlockConfiguration;
			Assert.True(config.BlockPublicAcls);
			Assert.False(config.IgnorePublicAcls);
			Assert.False(config.BlockPublicPolicy);
			Assert.False(config.RestrictPublicBuckets);
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "접근권한 블록 설정이 없는 버킷에서도 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeletePublicBlockNotExist()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.DeletePublicAccessBlock(bucketName);
			var e = Assert.Throws<AggregateException>(() => client.GetPublicAccessBlock(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, GetErrorCode(e));
			// 설정이 없어도 삭제는 성공한다
			client.DeletePublicAccessBlock(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 접근권한 블록을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPublicBlockBucketNotExist()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			AggregateException e;
			e = Assert.Throws<AggregateException>(() => client.GetPublicAccessBlock(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => PutPublicBlock(client, bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => client.DeletePublicAccessBlock(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "다른 사용자가 접근권한 블록을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPublicBlockAltUser()
		{
			TestId = 11;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			PutPublicBlock(client, bucketName, blockPublicAcls: true);
			AggregateException e;
			e = Assert.Throws<AggregateException>(() => altClient.GetPublicAccessBlock(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => PutPublicBlock(altClient, bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => altClient.DeletePublicAccessBlock(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "[BlockPublicAcls] 퍼블릭 ACL은 거부되고 퍼블릭이 아닌 ACL은 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicAclsAllowPrivate()
		{
			TestId = 12;
			var key = "TestBlockPublicAclsAllowPrivate";
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.ObjectWriter);
			PutPublicBlock(client, bucketName, blockPublicAcls: true);
			client.PutObject(bucketName, key, body: key);

			// 퍼블릭 ACL 은 오브젝트 ACL 변경에서도 거부된다
			var e = Assert.Throws<AggregateException>(() => client.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			// 퍼블릭이 아닌 ACL 은 허용된다
			client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private);
			client.PutObject(bucketName, key + "2", body: key, acl: S3CannedACL.Private);
			client.PutBucketACL(bucketName, acl: S3CannedACL.Private);
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "버킷 정책에 따라 정책 상태(IsPublic)가 올바르게 판정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPolicyStatus()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutPublicBlock(client, bucketName);
			var resource = S3Utils.MakeArnResource($"{bucketName}/*");

			var e = Assert.Throws<AggregateException>(() => client.GetBucketPolicyStatus(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET_POLICY, GetErrorCode(e));

			// 모든 사용자에게 여는 정책은 퍼블릭으로 판정된다
			client.PutBucketPolicy(bucketName, S3Utils.MakeJsonPolicy("s3:GetObject", resource).ToString());
			Assert.True(client.GetBucketPolicyStatus(bucketName).PolicyStatus.IsPublic);

			// 특정 사용자에게만 여는 정책은 퍼블릭이 아니다
			var principal = new JObject() { { "CanonicalUser", Config.AltUser.UserId } };
			client.PutBucketPolicy(bucketName, S3Utils.MakeJsonPolicy("s3:GetObject", resource, principal).ToString());
			Assert.False(client.GetBucketPolicyStatus(bucketName).PolicyStatus.IsPublic);
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Policy")]
		[Trait(MainData.Explanation, "[BlockPublicPolicy] 퍼블릭 정책은 거부되고 특정 사용자에게만 여는 정책은 허용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBlockPublicPolicyAllowNonPublic()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutPublicBlock(client, bucketName, blockPublicPolicy: true);
			var resource = S3Utils.MakeArnResource($"{bucketName}/*");

			// 퍼블릭 정책은 거부되지만 특정 사용자에게만 여는 정책은 허용된다
			var e = Assert.Throws<AggregateException>(() =>
				client.PutBucketPolicy(bucketName, S3Utils.MakeJsonPolicy("s3:GetObject", resource).ToString()));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			var principal = new JObject() { { "CanonicalUser", Config.AltUser.UserId } };
			client.PutBucketPolicy(bucketName, S3Utils.MakeJsonPolicy("s3:GetObject", resource, principal).ToString());
			Assert.False(string.IsNullOrEmpty(client.GetBucketPolicy(bucketName).Policy));
		}

		[Fact]
		[Trait(MainData.Major, "Access")]
		[Trait(MainData.Minor, "Denied")]
		[Trait(MainData.Explanation, "[RestrictPublicBuckets] 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestRestrictPublicBuckets()
		{
			TestId = 15;
			var key = "TestRestrictPublicBuckets";
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutPublicBlock(client, bucketName);
			client.PutObject(bucketName, key, body: key);
			SetPublicReadPolicy(client, bucketName);

			// 같은 연결(keep-alive)을 계속 쓰면 AWS가 이전 판정(200)을 60초 넘게 그대로 돌려준다.
			// 새 연결에서는 변경이 바로 보이므로 확인할 때마다 클라이언트를 새로 만든다.
			// (python 은 응답 본문을 읽지 않아 요청마다 새 연결을 쓴다)

			// 퍼블릭 정책으로 누구나 읽을 수 있다
			WaitUntil(() => GetObjectStatus(GetPublicClient(), bucketName, key) == HttpStatusCode.OK);
			WaitUntil(() => GetObjectStatus(GetAltClient(), bucketName, key) == HttpStatusCode.OK);

			// RestrictPublicBuckets 를 켜면 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있다
			PutPublicBlock(client, bucketName, restrictPublicBuckets: true);
			WaitUntil(() => GetObjectStatus(GetPublicClient(), bucketName, key) == HttpStatusCode.Forbidden);
			WaitUntil(() => GetObjectStatus(GetAltClient(), bucketName, key) == HttpStatusCode.Forbidden);
			Assert.Equal(HttpStatusCode.OK, GetObjectStatus(client, bucketName, key));
		}
	}
}
