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
	public class Grants : TestBase
	{
		public Grants(Xunit.Abstractions.ITestOutputHelper output) => Output = output;

		private const string AllUsers = "http://acs.amazonaws.com/groups/global/AllUsers";
		private const string AuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

		private S3Grant MainGrant(S3Permission permission) => new()
		{
			Permission = permission,
			Grantee = new S3Grantee() { CanonicalUser = Config.MainUser.UserId, DisplayName = Config.MainUser.DisplayName }
		};

		private S3Grant AltGrant(S3Permission permission) => new()
		{
			Permission = permission,
			Grantee = new S3Grantee() { CanonicalUser = Config.AltUser.UserId, DisplayName = Config.AltUser.DisplayName }
		};

		/// <summary>메인 유저의 FULL_CONTROL 권한과 AllUsers 그룹의 권한으로 구성된 acl 목록을 생성한다.</summary>
		private List<S3Grant> PublicAcl(params S3Permission[] permissions)
		{
			var grants = new List<S3Grant>() { MainGrant(S3Permission.FULL_CONTROL) };
			foreach (var permission in permissions)
				grants.Add(new S3Grant() { Permission = permission, Grantee = new S3Grantee() { URI = AllUsers } });
			return grants;
		}

		/// <summary>메인 유저의 FULL_CONTROL 권한과 AuthenticatedUsers 그룹의 권한으로 구성된 acl 목록을 생성한다.</summary>
		private List<S3Grant> AuthenticatedAcl(params S3Permission[] permissions)
		{
			var grants = new List<S3Grant>() { MainGrant(S3Permission.FULL_CONTROL) };
			foreach (var permission in permissions)
				grants.Add(new S3Grant() { Permission = permission, Grantee = new S3Grantee() { URI = AuthenticatedUsers } });
			return grants;
		}

		/// <summary>버킷에 서브유저 권한을 설정한 뒤 응답 acl이 올바른지 확인한다.</summary>
		private void CheckBucketPermission(S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var acl = CreateAltAcl(permission);

			client.PutBucketACL(bucketName, accessControlPolicy: acl);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(acl.Grants, response.AccessControlList.Grants);
		}

		/// <summary>오브젝트에 메인유저 권한을 설정한 뒤 응답 acl이 올바른지 확인한다.</summary>
		private void CheckObjectPermission(S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "testObjectPermission" + permission;

			var acl = new S3AccessControlList()
			{
				Owner = new Owner() { Id = Config.MainUser.UserId, DisplayName = Config.MainUser.DisplayName },
				Grants = [MainGrant(S3Permission.FULL_CONTROL), MainGrant(permission)]
			};

			client.PutObject(bucketName, key, body: key);
			client.PutObjectACL(bucketName, key, accessControlPolicy: acl);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(acl.Grants, response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "권한을 설정하지 않고 생성한 버킷의 기본 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclDefault()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "public-read로 생성한 버킷을 private로 변경할 경우 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclChanged()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.PublicRead);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(S3Permission.READ), response.AccessControlList.Grants);

			client.PutBucketACL(bucketName, acl: S3CannedACL.Private);

			response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "private로 생성한 버킷의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclPrivate()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.Private);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "public-read로 생성한 버킷의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclPublicRead()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.PublicRead);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(S3Permission.READ), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "public-read-write로 생성한 버킷의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclPublicRW()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(PublicAcl(S3Permission.READ, S3Permission.WRITE), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "authenticated-read로 생성한 버킷의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclAuthenticatedRead()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.AuthenticatedRead);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(AuthenticatedAcl(S3Permission.READ), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "권한을 설정하지 않고 생성한 오브젝트의 기본 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclDefault()
		{
			TestId = 7;
			var key = "TestObjectAclDefault";
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, key, body: key);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "public-read로 생성한 오브젝트를 private로 변경할 경우 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclChange()
		{
			TestId = 8;
			var key = "TestObjectAclChange";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicRead);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(S3Permission.READ), response.AccessControlList.Grants);

			client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private);

			response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "private로 생성한 오브젝트의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclPrivate()
		{
			TestId = 9;
			var key = "TestObjectAclPrivate";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.Private);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "public-read로 생성한 오브젝트의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclPublicRead()
		{
			TestId = 10;
			var key = "TestObjectAclPublicRead";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicRead);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(S3Permission.READ), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "public-read-write로 생성한 오브젝트의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclPublicRW()
		{
			TestId = 11;
			var key = "TestObjectAclPublicRW";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.PublicReadWrite);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(S3Permission.READ, S3Permission.WRITE), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "authenticated-read로 생성한 오브젝트의 acl 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclAuthenticatedRead()
		{
			TestId = 12;
			var key = "TestObjectAclAuthenticatedRead";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key, acl: S3CannedACL.AuthenticatedRead);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(AuthenticatedAcl(S3Permission.READ), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write] 서브유저가 업로드한 오브젝트를 bucket-owner-read로 " +
									 "변경했을때 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclBucketOwnerRead()
		{
			TestId = 13;
			var key = "TestObjectAclBucketOwnerRead";
			var mainClient = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketWithAcl(mainClient, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);

			altClient.PutObject(bucketName, key, body: key, acl: S3CannedACL.BucketOwnerRead);

			var response = altClient.GetObjectACL(bucketName, key);
			CheckGrants([AltGrant(S3Permission.FULL_CONTROL), MainGrant(S3Permission.READ)], response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[ownership:object-writer, bucket_acl:public-read-write] 서브유저가 업로드한 오브젝트를 " +
									 "bucket-owner-full-control로 변경했을때 소유자가 서브유저로 유지되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketObjectWriterObjectOwnerFullControl()
		{
			TestId = 14;
			var key = "TestBucketObjectWriterObjectOwnerFullControl";
			var mainClient = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketWithAcl(mainClient, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);

			altClient.PutObject(bucketName, key, body: key, acl: S3CannedACL.BucketOwnerFullControl);

			var response = mainClient.GetObjectACL(bucketName, key);
			CheckGrants([AltGrant(S3Permission.FULL_CONTROL), MainGrant(S3Permission.FULL_CONTROL)], response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[ownership:bucket-owner-preferred, bucket_acl:public-read-write] 서브유저가 업로드한 오브젝트를 " +
									 "bucket-owner-full-control로 변경했을때 소유자가 버킷 소유자로 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketOwnerEnforcedObjectOwnerFullControl()
		{
			TestId = 15;
			var key = "TestBucketOwnerEnforcedObjectOwnerFullControl";
			var mainClient = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketWithAcl(mainClient, ObjectOwnership.BucketOwnerPreferred, S3CannedACL.PublicReadWrite);

			altClient.PutObject(bucketName, key, body: key, acl: S3CannedACL.BucketOwnerFullControl);

			var response = mainClient.GetObjectACL(bucketName, key);
			CheckGrants(PublicAcl(), response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write] 오브젝트의 소유자를 서브유저가 변경해도 소유자가 유지되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclOwnerNotChange()
		{
			TestId = 16;
			var key = "TestObjectAclOwnerNotChange";
			var mainClient = GetClient();
			var altClient = GetAltClient();
			var bucketName = CreateBucketWithAcl(mainClient, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);

			mainClient.PutObject(bucketName, key, body: key);

			var acl1 = CreateAltAcl(S3Permission.FULL_CONTROL);
			mainClient.PutObjectACL(bucketName, key, accessControlPolicy: acl1);

			var acl2 = CreateAltAcl(S3Permission.READ_ACP);
			altClient.PutObjectACL(bucketName, key, accessControlPolicy: acl2);

			var response = altClient.GetObjectACL(bucketName, key);
			CheckGrants(acl2.Grants, response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Effect")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write] 권한정보를 변경한 오브젝트의 ContentType과 eTag가 변경되지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclChangeNotEffect()
		{
			TestId = 17;
			var key = "TestBucketAclChangeNotEffect";
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);

			client.PutObject(bucketName, key, body: key);

			var response = client.GetObject(bucketName, key);
			var contentType = response.Headers.ContentType;
			var eTag = response.ETag;

			var acl = CreateAltAcl(S3Permission.FULL_CONTROL);
			client.PutObjectACL(bucketName, key, accessControlPolicy: acl);

			response = client.GetObject(bucketName, key);
			Assert.Equal(contentType, response.Headers.ContentType);
			Assert.Equal(eTag, response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "private로 생성한 버킷에 private 권한을 중복 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclDuplicated()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.Private);

			var response = client.PutBucketACL(bucketName, acl: S3CannedACL.Private);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "버킷에 설정한 acl 정보가 올바르게 적용되는지 확인 : FULL_CONTROL")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPermissionFullControl()
		{
			TestId = 19;
			CheckBucketPermission(S3Permission.FULL_CONTROL);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "버킷에 설정한 acl 정보가 올바르게 적용되는지 확인 : WRITE")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPermissionWrite()
		{
			TestId = 20;
			CheckBucketPermission(S3Permission.WRITE);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "버킷에 설정한 acl 정보가 올바르게 적용되는지 확인 : WRITE_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPermissionWriteAcp()
		{
			TestId = 21;
			CheckBucketPermission(S3Permission.WRITE_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "버킷에 설정한 acl 정보가 올바르게 적용되는지 확인 : READ")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPermissionRead()
		{
			TestId = 22;
			CheckBucketPermission(S3Permission.READ);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "버킷에 설정한 acl 정보가 올바르게 적용되는지 확인 : READ_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketPermissionReadAcp()
		{
			TestId = 23;
			CheckBucketPermission(S3Permission.READ_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl 정보가 올바르게 적용되는지 확인 : FULL_CONTROL")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPermissionFullControl()
		{
			TestId = 24;
			CheckObjectPermission(S3Permission.FULL_CONTROL);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl 정보가 올바르게 적용되는지 확인 : WRITE")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPermissionWrite()
		{
			TestId = 25;
			CheckObjectPermission(S3Permission.WRITE);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl 정보가 올바르게 적용되는지 확인 : WRITE_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPermissionWriteAcp()
		{
			TestId = 26;
			CheckObjectPermission(S3Permission.WRITE_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl 정보가 올바르게 적용되는지 확인 : READ")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPermissionRead()
		{
			TestId = 27;
			CheckObjectPermission(S3Permission.READ);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl 정보가 올바르게 적용되는지 확인 : READ_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectPermissionReadAcp()
		{
			TestId = 28;
			CheckObjectPermission(S3Permission.READ_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 유저에게 권한을 부여하려고 하면 에러가 발생하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclGrantNonExistUser()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var grant = new S3Grant() { Permission = S3Permission.FULL_CONTROL, Grantee = new S3Grantee() { CanonicalUser = "Foo" } };
			var acl = AddBucketUserGrant(bucketName, grant);

			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, accessControlPolicy: acl));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		// AWS는 PutBucketAcl 본문에 <Owner>와 <AccessControlList>가 모두 있어야 하는데
		// (없으면 MalformedACLError), .NET SDK v4는 Grants가 비면 <AccessControlList>를 통째로 생략한다.
		// 빈 Grants를 서버에 그대로 보낼 방법이 없어 이 케이스를 검증할 수 없다(SDK 한계).
		[Fact(Skip = ".NET SDK v4 omits empty <AccessControlList> element; AWS rejects the body as MalformedACLError")]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷의 권한을 모두 제거한 뒤 소유자가 여전히 오브젝트를 업로드하고 권한을 복구할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclNoGrants()
		{
			TestId = 30;
			var key = "TestBucketAclNoGrants";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key);
			var response = client.GetBucketACL(bucketName);
			var oldGrants = response.AccessControlList.Grants;
			var policy = new S3AccessControlList()
			{
				Owner = response.AccessControlList.Owner,
				Grants = []
			};

			client.PutBucketACL(bucketName, accessControlPolicy: policy);

			client.PutObject(bucketName, key, body: key);

			var client2 = GetClient();
			client2.GetBucketACL(bucketName);
			client2.PutBucketACL(bucketName, acl: S3CannedACL.Private);

			policy.Grants = oldGrants;
			client2.PutBucketACL(bucketName, accessControlPolicy: policy);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Grant")]
		[Trait(MainData.Explanation, "버킷에 여러 권한을 한번에 설정했을때 모두 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclMultiGrants()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var acl = CreateAltAcl(S3Permission.READ, S3Permission.WRITE, S3Permission.READ_ACP,
									S3Permission.WRITE_ACP, S3Permission.FULL_CONTROL);

			client.PutBucketACL(bucketName, accessControlPolicy: acl);

			var response = client.GetBucketACL(bucketName);
			CheckGrants(acl.Grants, response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Grant")]
		[Trait(MainData.Explanation, "오브젝트에 여러 권한을 한번에 설정했을때 모두 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclMultiGrants()
		{
			TestId = 32;
			var key = "TestObjectAclMultiGrants";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var acl = CreateAltAcl(S3Permission.READ, S3Permission.WRITE, S3Permission.READ_ACP,
									S3Permission.WRITE_ACP, S3Permission.FULL_CONTROL);

			client.PutObject(bucketName, key, body: key);
			client.PutObjectACL(bucketName, key, accessControlPolicy: acl);

			var response = client.GetObjectACL(bucketName, key);
			CheckGrants(acl.Grants, response.AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 acl에서 소유자 정보가 누락될 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
		// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
		public void TestBucketAclRevokeAll()
		{
			TestId = 33;
			var key = "TestBucketAclRevokeAll";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key);
			var response = client.GetBucketACL(bucketName);

			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = new(), Grants = response.AccessControlList.Grants }));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "오브젝트의 acl에서 소유자 정보가 누락될 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
		// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
		public void TestObjectAclRevokeAll()
		{
			TestId = 34;
			var key = "TestObjectAclRevokeAll";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key);
			var response = client.GetObjectACL(bucketName, key);

			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = new(), Grants = response.AccessControlList.Grants }));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "권한 대상 유저의 아이디가 누락될 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclRevokeAllId()
		{
			TestId = 35;
			var key = "TestBucketAclRevokeAllId";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			client.PutObject(bucketName, key, body: key);
			var response = client.GetBucketACL(bucketName);

			var acl = new S3AccessControlList()
			{
				Owner = response.AccessControlList.Owner,
				Grants =
				[
					new()
					{
						Permission = S3Permission.FULL_CONTROL,
						Grantee = new S3Grantee() { CanonicalUser = null, DisplayName = Config.MainUser.DisplayName }
					}
				]
			};

			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, accessControlPolicy: acl));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_ACL_ERROR, GetErrorCode(e));
		}

		private const string LogDelivery = "http://acs.amazonaws.com/groups/s3/LogDelivery";

		/// <summary>버킷 소유자를 Owner 로 하는 AccessControlPolicy. grants 가 없으면 소유자 FULL_CONTROL. (python _owner_policy)</summary>
		private static S3AccessControlList OwnerPolicy(S3Client client, string bucketName, params S3Grant[] grants)
		{
			var ownerId = client.GetBucketACL(bucketName).AccessControlList.Owner.Id;
			return new S3AccessControlList()
			{
				Owner = new Owner() { Id = ownerId },
				Grants = grants.Length > 0 ? [.. grants] : [OwnerGrant(ownerId, S3Permission.FULL_CONTROL)]
			};
		}

		private static S3Grant OwnerGrant(string ownerId, S3Permission permission)
			=> new() { Permission = permission, Grantee = new S3Grantee() { CanonicalUser = ownerId } };

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 캔드 ACL을 버킷·오브젝트에 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclInvalidCanned()
		{
			TestId = 36;
			var key = "TestBucketAclInvalidCanned";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content");
			var invalid = new S3CannedACL("invalid");
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT, () => client.PutBucketACL(bucketName, acl: invalid));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT, () => client.PutObjectACL(bucketName, key, acl: invalid));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "캔드 ACL과 권한 부여 헤더를 함께 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 x-amz-acl 과 x-amz-grant-* 를 함께 보내면 둘을 합쳐 저장한다.
		// AWS 는 400 InvalidRequest 다.
		public void TestAclCannedWithGrantHeader()
		{
			TestId = 37;
			var key = "TestAclCannedWithGrantHeader";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content");
			var grant = $"uri=\"{AuthenticatedUsers}\"";
			var grantHeader = new List<KeyValuePair<string, string>>() { new("x-amz-grant-read", grant) };
			// 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
			var requests = new Action[]
			{
				() => client.PutBucketACL(bucketName, acl: S3CannedACL.Private, grantRead: grant),
				() => client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private, grantRead: grant),
				() => client.PutObject(bucketName, key + "2", body: "content", acl: S3CannedACL.Private, headerList: grantHeader),
			};
			foreach (var request in requests)
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST, request);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "캔드 ACL과 본문(AccessControlPolicy)을 함께 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 x-amz-acl 이 있으면 ACL 본문은 읽지 않고 캔드 ACL 을 적용한다.
		// AWS 는 400 UnexpectedContent 다.
		public void TestBucketAclCannedWithBody()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var policy = OwnerPolicy(client, bucketName);
			// 캔드 ACL 과 본문(AccessControlPolicy)은 함께 쓸 수 없다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.UNEXPECTED_CONTENT,
				() => client.PutBucketACL(bucketName, acl: S3CannedACL.Private, accessControlPolicy: policy));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "캔드 ACL, 권한 부여 헤더, 본문 없이 버킷 ACL을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclMissing()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			// 캔드 ACL, 권한 부여 헤더, 본문 중 아무것도 없다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MISSING_SECURITY_HEADER, () => client.PutBucketACL(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 권한으로 버킷 ACL을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclInvalidPermission()
		{
			TestId = 40;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var ownerId = client.GetBucketACL(bucketName).AccessControlList.Owner.Id;
			var policy = OwnerPolicy(client, bucketName, OwnerGrant(ownerId, new S3Permission("INVALID")));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_ACL_ERROR,
				() => client.PutBucketACL(bucketName, accessControlPolicy: policy));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "ACL로 버킷 소유자를 바꾸려 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 ACL 본문의 Owner 가 실제 소유자와 달라도 기존 소유자로 덮어써 받는다(소유자는 바뀌지 않는다).
		// AWS 는 403 AccessDenied 다.
		public void TestBucketAclChangeOwner()
		{
			TestId = 41;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var policy = OwnerPolicy(client, bucketName);
			policy.Owner = new Owner() { Id = Config.AltUser.UserId };
			// ACL 로 소유자를 바꿀 수 없다
			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED,
				() => client.PutBucketACL(bucketName, accessControlPolicy: policy));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 권한 부여 헤더로 버킷 ACL을 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclInvalidGrantHeader()
		{
			TestId = 42;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			// 형식 오류, 없는 사용자 ID, 없는 그룹 URI
			foreach (var grant in new[] { "invalid", "id=\"invalid\"", "uri=\"http://example.com/invalid\"" })
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT, () => client.PutBucketACL(bucketName, grantRead: grant));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 그룹 URI에 권한을 부여하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclInvalidGroupUri()
		{
			TestId = 43;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var grant = new S3Grant() { Permission = S3Permission.READ, Grantee = new S3Grantee() { URI = "http://example.com/invalid" } };
			var policy = OwnerPolicy(client, bucketName, grant);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => client.PutBucketACL(bucketName, accessControlPolicy: policy));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "권한 부여를 100개까지 설정할 수 있고 101개는 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclMaxGrants()
		{
			TestId = 44;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var ownerId = client.GetBucketACL(bucketName).AccessControlList.Owner.Id;
			S3Grant[] MakeGrants(int count)
			{
				var grants = new S3Grant[count];
				for (int i = 0; i < count; i++) grants[i] = OwnerGrant(ownerId, S3Permission.FULL_CONTROL);
				return grants;
			}
			// 권한 부여는 100개까지
			client.PutBucketACL(bucketName, accessControlPolicy: OwnerPolicy(client, bucketName, MakeGrants(100)));
			var policy = OwnerPolicy(client, bucketName, MakeGrants(101));
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_ACL_ERROR,
				() => client.PutBucketACL(bucketName, accessControlPolicy: policy));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 ACL을 조회, 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketAclBucketNotExist()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.GetBucketACL(bucketName));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.PutBucketACL(bucketName, acl: S3CannedACL.Private));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "다른 계정이 버킷·오브젝트의 ACL을 조회, 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestAclAccessDenied()
		{
			TestId = 46;
			var key = "TestAclAccessDenied";
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content");
			var requests = new Action[]
			{
				() => altClient.GetBucketACL(bucketName),
				() => altClient.PutBucketACL(bucketName, acl: S3CannedACL.Private),
				() => altClient.GetObjectACL(bucketName, key),
				() => altClient.PutObjectACL(bucketName, key, acl: S3CannedACL.Private),
			};
			foreach (var request in requests)
				AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, request);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 오브젝트의 ACL을 조회, 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectAclKeyNotExist()
		{
			TestId = 47;
			var key = "TestObjectAclKeyNotExist";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.GetObjectACL(bucketName, key));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버저닝을 쓰지 않는 버킷의 오브젝트 ACL을 null 버전으로 조회할 수 있고 잘못된 버전은 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectAclVersionId()
		{
			TestId = 48;
			var key = "TestObjectAclVersionId";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content");
			// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
			Assert.Single(client.GetObjectACL(bucketName, key, versionId: "null").AccessControlList.Grants);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT, () => client.GetObjectACL(bucketName, key, versionId: "invalid"));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "삭제 마커의 ACL을 조회, 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectAclDeleteMarker()
		{
			TestId = 49;
			var key = "TestObjectAclDeleteMarker";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var versionId = client.PutObject(bucketName, key, body: "content").VersionId;
			var markerId = client.DeleteObject(bucketName, key).VersionId;

			// 최신 버전이 삭제 마커이면 없는 키, 삭제 마커 버전을 지정하면 405
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => client.GetObjectACL(bucketName, key));
			AssertS3Error(HttpStatusCode.MethodNotAllowed, MainData.METHOD_NOT_ALLOWED,
				() => client.GetObjectACL(bucketName, key, versionId: markerId));
			AssertS3Error(HttpStatusCode.MethodNotAllowed, MainData.METHOD_NOT_ALLOWED,
				() => client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private, versionId: markerId));
			Assert.Single(client.GetObjectACL(bucketName, key, versionId: versionId).AccessControlList.Grants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "권한 부여 헤더만으로 업로드하면 지정한 권한만 남는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectWithGrantHeader()
		{
			TestId = 51;
			var key = "TestPutObjectWithGrantHeader";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var altId = Config.AltUser.UserId;
			// 권한 부여 헤더만 주면 소유자 FULL_CONTROL 없이 지정한 권한만 남는다
			client.PutObject(bucketName, key, body: "content", headerList: [new("x-amz-grant-read", $"id=\"{altId}\"")]);
			var grants = client.GetObjectACL(bucketName, key).AccessControlList.Grants;
			Assert.Single(grants);
			Assert.Equal(altId, grants[0].Grantee.CanonicalUser);
			Assert.Equal(S3Permission.READ, grants[0].Permission);
			Assert.Equal("content", GetBody(GetAltClient().GetObject(bucketName, key)));
		}
	}
}
