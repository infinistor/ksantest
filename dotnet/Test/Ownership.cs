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
		[Trait(MainData.Explanation, "소유권을 BucketOwnerEnforced로 변경하면 공개 객체의 퍼블릭 접근이 막히는지 확인")]
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

			// BucketOwnerEnforced 로 바꾸면 오브젝트 ACL 이 무시되어 퍼블릭 접근이 막힌다.
			// AWS 는 이미 맺은 연결에서는 변경 전 권한으로 계속 응답하므로 새 클라이언트로 확인한다.
			WaitUntil(() => GetCallStatus(() => GetPublicClient().GetObjectMetadata(bucketName, key)) == HttpStatusCode.Forbidden);
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

		private static void PutOwnership(S3Client client, string bucketName, ObjectOwnership ownership)
			=> client.PutBucketOwnershipControls(bucketName, new OwnershipControls
			{
				Rules = [new OwnershipControlsRule { ObjectOwnership = ownership }]
			});

		private static ObjectOwnership GetOwnership(S3Client client, string bucketName)
			=> client.GetBucketOwnershipControls(bucketName).OwnershipControls.Rules[0].ObjectOwnership;

		private static string ObjectOwnerId(S3Client client, string bucketName, string key)
			=> client.ListObjectsV2(bucketName, prefix: key, fetchOwner: true).S3Objects[0].Owner.Id;

		/// <summary>ACL 로 alt 사용자에게 쓰기 권한을 준 ObjectWriter 버킷을 만든다.</summary>
		private string CreateBucketAltWritable(S3Client client)
		{
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutBucketACL(bucketName,
				grantFullControl: $"id={Config.MainUser.UserId}",
				grantWrite: $"id={Config.AltUser.UserId}");
			return bucketName;
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "소유권을 지정하지 않고 만든 버킷의 소유권 설정이 BucketOwnerEnforced인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): 새 버킷의 소유권 기본값은 ObjectWriter 다.
		// SCP 등 ACL 로 권한을 주는 기존 사용 방식을 유지하려고 AWS 의 2023년 기본값(BucketOwnerEnforced)을 따르지 않는다.
		public void TestBucketOwnershipDefault()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 소유권을 지정하지 않고 만든 버킷은 BucketOwnerEnforced 이다
			Assert.Equal(ObjectOwnership.BucketOwnerEnforced, GetOwnership(client, bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 소유권을 모든 값으로 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketOwnershipAllValues()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			ObjectOwnership[] ownerships = [ObjectOwnership.ObjectWriter, ObjectOwnership.BucketOwnerPreferred, ObjectOwnership.BucketOwnerEnforced];
			foreach (var ownership in ownerships)
			{
				PutOwnership(client, bucketName, ownership);
				Assert.Equal(ownership, GetOwnership(client, bucketName));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "잘못된 값으로 버킷 소유권을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketOwnershipInvalid()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var e = Assert.Throws<AggregateException>(() => PutOwnership(client, bucketName, new ObjectOwnership("Invalid")));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷 소유권 설정을 삭제할 수 있고, 설정이 없어도 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteBucketOwnership()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerPreferred);
			client.DeleteBucketOwnershipControls(bucketName);
			var e = Assert.Throws<AggregateException>(() => client.GetBucketOwnershipControls(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.OWNERSHIP_CONTROLS_NOT_FOUND_ERROR, GetErrorCode(e));
			// 설정이 없어도 삭제는 성공한다
			client.DeleteBucketOwnershipControls(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 소유권 설정을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipBucketNotExist()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			AggregateException e;
			e = Assert.Throws<AggregateException>(() => client.GetBucketOwnershipControls(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => PutOwnership(client, bucketName, ObjectOwnership.ObjectWriter));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => client.DeleteBucketOwnershipControls(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "다른 사용자가 버킷 소유권 설정을 조회, 설정, 삭제하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipAltUser()
		{
			TestId = 13;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			AggregateException e;
			e = Assert.Throws<AggregateException>(() => altClient.GetBucketOwnershipControls(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => PutOwnership(altClient, bucketName, ObjectOwnership.ObjectWriter));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => altClient.DeleteBucketOwnershipControls(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "BucketOwnerEnforced 버킷을 ACL과 함께 만들면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCreateBucketOwnershipEnforcedWithAcl()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucketName();
			// BucketOwnerEnforced 버킷은 ACL 을 함께 지정해 만들 수 없다
			var e = Assert.Throws<AggregateException>(() =>
				client.PutBucket(bucketName, acl: S3CannedACL.PublicRead, objectOwnership: ObjectOwnership.BucketOwnerEnforced));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "잘못된 소유권 값으로 버킷을 만들면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCreateBucketInvalidOwnership()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucketName();
			var e = Assert.Throws<AggregateException>(() =>
				client.PutBucket(bucketName, objectOwnership: new ObjectOwnership("Invalid")));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "버킷 ACL에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced로 바꿀 수 없는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipEnforcedWithAclGrants()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = CreateBucketAltWritable(client);
			// 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없다
			var e = Assert.Throws<AggregateException>(() => PutOwnership(client, bucketName, ObjectOwnership.BucketOwnerEnforced));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, GetErrorCode(e));
			// ACL 을 초기화하면 바꿀 수 있다
			client.PutBucketACL(bucketName, acl: S3CannedACL.Private);
			PutOwnership(client, bucketName, ObjectOwnership.BucketOwnerEnforced);
			Assert.Equal(ObjectOwnership.BucketOwnerEnforced, GetOwnership(client, bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Owner")]
		[Trait(MainData.Explanation, "[ObjectWriter] 올린 사용자가 오브젝트 소유자가 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectOwnerObjectWriter()
		{
			TestId = 17;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketAltWritable(client);
			var key = "TestObjectOwnerObjectWriter";

			// ObjectWriter: 올린 사용자가 오브젝트 소유자가 되고, 버킷 소유자도 읽을 수 없다
			altClient.PutObject(bucketName, key, body: key);
			Assert.Equal(Config.AltUser.UserId, ObjectOwnerId(client, bucketName, key));
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			// bucket-owner-full-control 을 줘도 소유자는 올린 사용자다
			var key2 = key + "2";
			altClient.PutObject(bucketName, key2, body: key, acl: S3CannedACL.BucketOwnerFullControl);
			Assert.Equal(Config.AltUser.UserId, ObjectOwnerId(client, bucketName, key2));
			client.GetObject(bucketName, key2).Dispose();
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Owner")]
		[Trait(MainData.Explanation, "[BucketOwnerPreferred] bucket-owner-full-control로 올리면 버킷 소유자가 오브젝트 소유자가 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectOwnerBucketOwnerPreferred()
		{
			TestId = 18;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketAltWritable(client);
			PutOwnership(client, bucketName, ObjectOwnership.BucketOwnerPreferred);
			var key = "TestObjectOwnerBucketOwnerPreferred";

			// BucketOwnerPreferred: ACL 없이 올리면 올린 사용자가 소유자다
			altClient.PutObject(bucketName, key, body: key);
			Assert.Equal(Config.AltUser.UserId, ObjectOwnerId(client, bucketName, key));

			// bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 된다
			var key2 = key + "2";
			altClient.PutObject(bucketName, key2, body: key, acl: S3CannedACL.BucketOwnerFullControl);
			Assert.Equal(Config.MainUser.UserId, ObjectOwnerId(client, bucketName, key2));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Owner")]
		[Trait(MainData.Explanation, "[BucketOwnerEnforced] 누가 올려도 버킷 소유자가 오브젝트 소유자가 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectOwnerBucketOwnerEnforced()
		{
			TestId = 19;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var key = "TestObjectOwnerBucketOwnerEnforced";
			var resource = S3Utils.MakeArnResource($"{bucketName}/*");
			var principal = new JObject() { { "CanonicalUser", Config.AltUser.UserId } };
			client.PutBucketPolicy(bucketName, S3Utils.MakeJsonPolicy("s3:PutObject", resource, principal).ToString());

			// BucketOwnerEnforced: 누가 올려도 버킷 소유자가 소유자다
			WaitUntil(() => GetCallStatus(() => altClient.PutObject(bucketName, key, body: key)) == HttpStatusCode.OK);
			Assert.Equal(Config.MainUser.UserId, ObjectOwnerId(client, bucketName, key));

			// bucket-owner-full-control 은 허용되고 그 밖의 ACL 은 거부된다
			altClient.PutObject(bucketName, key + "2", body: key, acl: S3CannedACL.BucketOwnerFullControl);
			var e = Assert.Throws<AggregateException>(() =>
				altClient.PutObject(bucketName, key + "3", body: key, acl: S3CannedACL.Private));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Ownership")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "BucketOwnerEnforced 버킷은 ACL 조회만 가능하고 변경은 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketOwnershipEnforcedAcl()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client, ObjectOwnership.BucketOwnerEnforced);
			var key = "TestBucketOwnershipEnforcedAcl";
			client.PutObject(bucketName, key, body: key);

			// ACL 조회는 가능하고 소유자의 FULL_CONTROL 만 나온다
			GetACLResponse[] acls = [client.GetBucketACL(bucketName), client.GetObjectACL(bucketName, key)];
			foreach (var acl in acls)
			{
				var grants = acl.AccessControlList.Grants;
				Assert.Single(grants);
				Assert.Equal(Config.MainUser.UserId, grants[0].Grantee.CanonicalUser);
				Assert.Equal(S3Permission.FULL_CONTROL, grants[0].Permission);
			}

			// ACL 변경은 거부된다
			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: S3CannedACL.Private));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, GetErrorCode(e));
			e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key + "2", body: "a", acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, GetErrorCode(e));
			// 소유자의 bucket-owner-full-control 은 허용된다
			client.PutObject(bucketName, key + "3", body: "a", acl: S3CannedACL.BucketOwnerFullControl);
		}
	}
}
