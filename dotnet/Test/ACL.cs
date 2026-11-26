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
	public partial class ACL : TestBase
	{
		public ACL(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketAndObject()
		{
			TestId = 1;
			var mainKey = "TestPrivateBucketAndObjectMain";
			var altKey = "TestPrivateBucketAndObjectAlt";
			var publicKey = "TestPrivateBucketAndObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.Private, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketPublicReadObject()
		{
			TestId = 2;
			var mainKey = "TestPrivateBucketPublicReadObjectMain";
			var altKey = "TestPrivateBucketPublicReadObjectAlt";
			var publicKey = "TestPrivateBucketPublicReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.PublicRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketPublicRWObject()
		{
			TestId = 3;
			var mainKey = "TestPrivateBucketPublicRWObjectMain";
			var altKey = "TestPrivateBucketPublicRWObjectAlt";
			var publicKey = "TestPrivateBucketPublicRWObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.PublicReadWrite, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketAuthenticatedReadObject()
		{
			TestId = 4;
			var mainKey = "TestPrivateBucketAuthenticatedReadObjectMain";
			var altKey = "TestPrivateBucketAuthenticatedReadObjectAlt";
			var publicKey = "TestPrivateBucketAuthenticatedReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.AuthenticatedRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketBucketOwnerReadObject()
		{
			TestId = 5;
			var mainKey = "TestPrivateBucketBucketOwnerReadObjectMain";
			var altKey = "TestPrivateBucketBucketOwnerReadObjectAlt";
			var publicKey = "TestPrivateBucketBucketOwnerReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketBucketOwnerReadObjectUploadAltUser()
		{
			TestId = 6;
			var mainKey = "TestPrivateBucketBucketOwnerReadObjectUploadAltUserMain";
			var altKey = "TestPrivateBucketBucketOwnerReadObjectUploadAltUserAlt";
			var publicKey = "TestPrivateBucketBucketOwnerReadObjectUploadAltUserPublic";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();
			client.PutBucketACL(bucketName, acl: S3CannedACL.Private);

			SucceedGetObject(altClient, bucketName, mainKey, mainKey);
			SucceedGetObject(client, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			FailedPutObject(altClient, bucketName, mainKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			SucceedPutObject(client, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPrivateBucketBucketOwnerFullControlObject()
		{
			TestId = 7;
			var mainKey = "TestPrivateBucketBucketOwnerFullControlObjectMain";
			var altKey = "TestPrivateBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "TestPrivateBucketBucketOwnerFullControlObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.Private, S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketPrivateObject()
		{
			TestId = 8;
			var mainKey = "TestPublicReadBucketPrivateObjectMain";
			var altKey = "TestPublicReadBucketPrivateObjectAlt";
			var publicKey = "TestPublicReadBucketPrivateObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.Private, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketAndObject()
		{
			TestId = 9;
			var mainKey = "TestPublicReadBucketAndObjectMain";
			var altKey = "TestPublicReadBucketAndObjectAlt";
			var publicKey = "TestPublicReadBucketAndObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.PublicRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketPublicRWObject()
		{
			TestId = 10;
			var mainKey = "TestPublicReadBucketPublicRWObjectMain";
			var altKey = "TestPublicReadBucketPublicRWObjectAlt";
			var publicKey = "TestPublicReadBucketPublicRWObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.PublicReadWrite, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketAuthenticatedReadObject()
		{
			TestId = 11;
			var mainKey = "TestPublicReadBucketAuthenticatedReadObjectMain";
			var altKey = "TestPublicReadBucketAuthenticatedReadObjectAlt";
			var publicKey = "TestPublicReadBucketAuthenticatedReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.AuthenticatedRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketBucketOwnerReadObject()
		{
			TestId = 12;
			var mainKey = "TestPublicReadBucketBucketOwnerReadObjectMain";
			var altKey = "TestPublicReadBucketBucketOwnerReadObjectAlt";
			var publicKey = "TestPublicReadBucketBucketOwnerReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicReadBucketBucketOwnerFullControlObject()
		{
			TestId = 13;
			var mainKey = "TestPublicReadBucketBucketOwnerFullControlObjectMain";
			var altKey = "TestPublicReadBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "TestPublicReadBucketBucketOwnerFullControlObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.PublicRead, S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPrivateObject()
		{
			TestId = 14;
			var mainKey = "TestPublicRWBucketPrivateObjectMain";
			var altKey = "TestPublicRWBucketPrivateObjectAlt";
			var altNewKey = "TestPublicRWBucketPrivateObjectAltNew";
			var publicKey = "TestPublicRWBucketPrivateObjectPublic";
			var publicNewKey = "TestPublicRWBucketPrivateObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.Private, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPrivateObjectByAltUser()
		{
			TestId = 15;
			var mainKey = "TestPublicRWBucketPrivateObjectByAltUserMain";
			var altKey = "TestPublicRWBucketPrivateObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketPrivateObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketPrivateObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.Private, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			FailedGetObject(client, bucketName, mainKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

			altClient.DeleteObject(bucketName, altKey);
			altClient.DeleteObject(bucketName, publicKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPublicReadObject()
		{
			TestId = 16;
			var mainKey = "TestPublicRWBucketPublicReadObjectMain";
			var altKey = "TestPublicRWBucketPublicReadObjectAlt";
			var altNewKey = "TestPublicRWBucketPublicReadObjectAltNew";
			var publicKey = "TestPublicRWBucketPublicReadObjectPublic";
			var publicNewKey = "TestPublicRWBucketPublicReadObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.PublicRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPublicReadObjectByAltUser()
		{
			TestId = 17;
			var mainKey = "TestPublicRWBucketPublicReadObjectByAltUserMain";
			var altKey = "TestPublicRWBucketPublicReadObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketPublicReadObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketPublicReadObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.PublicRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

			altClient.DeleteObject(bucketName, altKey);
			altClient.DeleteObject(bucketName, publicKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPublicRWObject()
		{
			TestId = 18;
			var mainKey = "TestPublicRWBucketPublicRWObjectMain";
			var altKey = "TestPublicRWBucketPublicRWObjectAlt";
			var altNewKey = "TestPublicRWBucketPublicRWObjectAltNew";
			var publicKey = "TestPublicRWBucketPublicRWObjectPublic";
			var publicNewKey = "TestPublicRWBucketPublicRWObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.PublicReadWrite, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketPublicRWObjectByAltUser()
		{
			TestId = 19;
			var mainKey = "TestPublicRWBucketPublicRWObjectByAltUserMain";
			var altKey = "TestPublicRWBucketPublicRWObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketPublicRWObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketPublicRWObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.PublicReadWrite, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketAuthenticatedReadObject()
		{
			TestId = 20;
			var mainKey = "TestPublicRWBucketAuthenticatedReadObjectMain";
			var altKey = "TestPublicRWBucketAuthenticatedReadObjectAlt";
			var altNewKey = "TestPublicRWBucketAuthenticatedReadObjectAltNew";
			var publicKey = "TestPublicRWBucketAuthenticatedReadObjectPublic";
			var publicNewKey = "TestPublicRWBucketAuthenticatedReadObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.AuthenticatedRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketAuthenticatedReadObjectByAltUser()
		{
			TestId = 21;
			var mainKey = "TestPublicRWBucketAuthenticatedReadObjectByAltUserMain";
			var altKey = "TestPublicRWBucketAuthenticatedReadObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketAuthenticatedReadObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketAuthenticatedReadObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.AuthenticatedRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketBucketOwnerReadObject()
		{
			TestId = 22;
			var mainKey = "TestPublicRWBucketBucketOwnerReadObjectMain";
			var altKey = "TestPublicRWBucketBucketOwnerReadObjectAlt";
			var altNewKey = "TestPublicRWBucketBucketOwnerReadObjectAltNew";
			var publicKey = "TestPublicRWBucketBucketOwnerReadObjectPublic";
			var publicNewKey = "TestPublicRWBucketBucketOwnerReadObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketBucketOwnerReadObjectByAltUser()
		{
			TestId = 23;
			var mainKey = "TestPublicRWBucketBucketOwnerReadObjectByAltUserMain";
			var altKey = "TestPublicRWBucketBucketOwnerReadObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketBucketOwnerReadObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketBucketOwnerReadObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketBucketOwnerFullControlObject()
		{
			TestId = 24;
			var mainKey = "TestPublicRWBucketBucketOwnerFullControlObjectMain";
			var altKey = "TestPublicRWBucketBucketOwnerFullControlObjectAlt";
			var altNewKey = "TestPublicRWBucketBucketOwnerFullControlObjectAltNew";
			var publicKey = "TestPublicRWBucketBucketOwnerFullControlObjectPublic";
			var publicNewKey = "TestPublicRWBucketBucketOwnerFullControlObjectPublicNew";
			var bucketName = SetupAclObjects(S3CannedACL.PublicReadWrite, S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketBucketOwnerFullControlObjectByAltUser()
		{
			TestId = 25;
			var mainKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserMain";
			var altKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserAlt";
			var publicKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserPublic";
			var publicNewKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserPublicNew";
			var bucketName = SetupAclObjectsByAlt(S3CannedACL.PublicReadWrite, S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			SucceedPutObject(altClient, bucketName, altKey, altKey);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred()
		{
			TestId = 26;
			var mainKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredMain";
			var altKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAlt";
			var altNewKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAltNew";
			var publicKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublic";
			var publicNewKey = "TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublicNew";
			var bucketName = SetupAclObjectsByAlt(ObjectOwnership.BucketOwnerPreferred, S3CannedACL.PublicReadWrite,
				S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(altClient, bucketName, altNewKey, altNewKey);
			SucceedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketPrivateObject()
		{
			TestId = 27;
			var mainKey = "TestAuthenticatedReadBucketPrivateObjectMain";
			var altKey = "TestAuthenticatedReadBucketPrivateObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketPrivateObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.Private, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketPublicReadObject()
		{
			TestId = 28;
			var mainKey = "TestAuthenticatedReadBucketPublicReadObjectMain";
			var altKey = "TestAuthenticatedReadBucketPublicReadObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketPublicReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.PublicRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketPublicRWObject()
		{
			TestId = 29;
			var mainKey = "TestAuthenticatedReadBucketPublicRWObjectMain";
			var altKey = "TestAuthenticatedReadBucketPublicRWObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketPublicRWObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.PublicReadWrite, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			SucceedGetObject(publicClient, bucketName, publicKey, publicKey);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketAndObject()
		{
			TestId = 30;
			var mainKey = "TestAuthenticatedReadBucketAndObjectMain";
			var altKey = "TestAuthenticatedReadBucketAndObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketAndObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.AuthenticatedRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			SucceedGetObject(altClient, bucketName, altKey, altKey);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketBucketOwnerReadObject()
		{
			TestId = 31;
			var mainKey = "TestAuthenticatedReadBucketBucketOwnerReadObjectMain";
			var altKey = "TestAuthenticatedReadBucketBucketOwnerReadObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketBucketOwnerReadObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.BucketOwnerRead, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Access")]
		public void TestAuthenticatedReadBucketBucketOwnerFullControlObject()
		{
			TestId = 32;
			var mainKey = "TestAuthenticatedReadBucketBucketOwnerFullControlObjectMain";
			var altKey = "TestAuthenticatedReadBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "TestAuthenticatedReadBucketBucketOwnerFullControlObjectPublic";
			var bucketName = SetupAclObjects(S3CannedACL.AuthenticatedRead, S3CannedACL.BucketOwnerFullControl, mainKey, altKey, publicKey);

			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();

			SucceedGetObject(client, bucketName, mainKey, mainKey);
			FailedGetObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedGetObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);

			SucceedPutObject(client, bucketName, mainKey, mainKey);
			FailedPutObject(altClient, bucketName, altKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedPutObject(publicClient, bucketName, publicKey, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "List")]
		public void TestPrivateBucketList()
		{
			TestId = 33;
			var keys = new List<string>() { "testPrivateBucketList1", "testPrivateBucketList2", "testPrivateBucketList3" };
			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();
			var bucketName = SetupAclBucket(S3CannedACL.Private, keys);

			SucceedListObjects(client, bucketName, keys);
			FailedListObjects(altClient, bucketName, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
			FailedListObjects(publicClient, bucketName, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "List")]
		public void TestPublicReadBucketList()
		{
			TestId = 34;
			var keys = new List<string>() { "testPublicReadBucketList1", "testPublicReadBucketList2", "testPublicReadBucketList3" };
			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();
			var bucketName = SetupAclBucket(S3CannedACL.PublicRead, keys);

			SucceedListObjects(client, bucketName, keys);
			SucceedListObjects(altClient, bucketName, keys);
			SucceedListObjects(publicClient, bucketName, keys);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "List")]
		public void TestPublicRWBucketList()
		{
			TestId = 35;
			var keys = new List<string>() { "testPublicRWBucketList1", "testPublicRWBucketList2", "testPublicRWBucketList3" };
			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();
			var bucketName = SetupAclBucket(S3CannedACL.PublicReadWrite, keys);

			SucceedListObjects(client, bucketName, keys);
			SucceedListObjects(altClient, bucketName, keys);
			SucceedListObjects(publicClient, bucketName, keys);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "List")]
		public void TestAuthenticatedReadBucketList()
		{
			TestId = 36;
			var keys = new List<string>() { "testAuthenticatedReadBucketList1", "testAuthenticatedReadBucketList2", "testAuthenticatedReadBucketList3" };
			var client = GetClient();
			var altClient = GetAltClient();
			var publicClient = GetPublicClient();
			var bucketName = SetupAclBucket(S3CannedACL.AuthenticatedRead, keys);

			SucceedListObjects(client, bucketName, keys);
			SucceedListObjects(altClient, bucketName, keys);
			FailedListObjects(publicClient, bucketName, HttpStatusCode.Forbidden, MainData.ACCESS_DENIED);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestBucketPermissionAltUserFullControl()
		{
			TestId = 37;
			var bucketName = SetupBucketPermission(S3Permission.FULL_CONTROL);
			var altClient = GetAltClient();

			CheckBucketAclAllowRead(altClient, bucketName);
			CheckBucketAclAllowReadACP(altClient, bucketName);
			CheckBucketAclAllowWrite(altClient, bucketName);
			CheckBucketAclAllowWriteACP(altClient, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestBucketPermissionAltUserRead()
		{
			TestId = 38;
			var bucketName = SetupBucketPermission(S3Permission.READ);
			var altClient = GetAltClient();

			CheckBucketAclAllowRead(altClient, bucketName);
			CheckBucketAclDenyReadACP(altClient, bucketName);
			CheckBucketAclDenyWrite(altClient, bucketName);
			CheckBucketAclDenyWriteACP(altClient, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestBucketPermissionAltUserReadAcp()
		{
			TestId = 39;
			var bucketName = SetupBucketPermission(S3Permission.READ_ACP);
			var altClient = GetAltClient();

			CheckBucketAclDenyRead(altClient, bucketName);
			CheckBucketAclAllowReadACP(altClient, bucketName);
			CheckBucketAclDenyWrite(altClient, bucketName);
			CheckBucketAclDenyWriteACP(altClient, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestBucketPermissionAltUserWrite()
		{
			TestId = 40;
			var bucketName = SetupBucketPermission(S3Permission.WRITE);
			var altClient = GetAltClient();

			CheckBucketAclDenyRead(altClient, bucketName);
			CheckBucketAclDenyReadACP(altClient, bucketName);
			CheckBucketAclAllowWrite(altClient, bucketName);
			CheckBucketAclDenyWriteACP(altClient, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestBucketPermissionAltUserWriteAcp()
		{
			TestId = 41;
			var bucketName = SetupBucketPermission(S3Permission.WRITE_ACP);
			var altClient = GetAltClient();

			CheckBucketAclDenyRead(altClient, bucketName);
			CheckBucketAclDenyReadACP(altClient, bucketName);
			CheckBucketAclDenyWrite(altClient, bucketName);
			CheckBucketAclAllowWriteACP(altClient, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestObjectPermissionAltUserFullControl()
		{
			TestId = 42;
			var key = "TestObjectPermissionAltUserFullControl";
			var bucketName = SetupObjectPermission(key, S3Permission.FULL_CONTROL);
			var altClient = GetAltClient();

			CheckObjectAclAllowRead(altClient, bucketName, key);
			CheckObjectAclAllowReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclAllowWriteACP(altClient, bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestObjectPermissionAltUserRead()
		{
			TestId = 43;
			var key = "TestObjectPermissionAltUserRead";
			var bucketName = SetupObjectPermission(key, S3Permission.READ);
			var altClient = GetAltClient();

			CheckObjectAclAllowRead(altClient, bucketName, key);
			CheckObjectAclDenyReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclDenyWriteACP(altClient, bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestObjectPermissionAltUserReadAcp()
		{
			TestId = 44;
			var key = "TestObjectPermissionAltUserReadAcp";
			var bucketName = SetupObjectPermission(key, S3Permission.READ_ACP);
			var altClient = GetAltClient();

			CheckObjectAclDenyRead(altClient, bucketName, key);
			CheckObjectAclAllowReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclDenyWriteACP(altClient, bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestObjectPermissionAltUserWrite()
		{
			TestId = 45;
			var key = "TestObjectPermissionAltUserWrite";
			var bucketName = SetupObjectPermission(key, S3Permission.WRITE);
			var altClient = GetAltClient();

			CheckObjectAclDenyRead(altClient, bucketName, key);
			CheckObjectAclDenyReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclDenyWriteACP(altClient, bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Permission")]
		public void TestObjectPermissionAltUserWriteAcp()
		{
			TestId = 46;
			var key = "TestObjectPermissionAltUserWriteAcp";
			var bucketName = SetupObjectPermission(key, S3Permission.WRITE_ACP);
			var altClient = GetAltClient();

			CheckObjectAclDenyRead(altClient, bucketName, key);
			CheckObjectAclDenyReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclAllowWriteACP(altClient, bucketName, key);
		}

		#region ACL 권한 범위 (AWS 실측 기준)

		private const string AllUsers = "http://acs.amazonaws.com/groups/global/AllUsers";
		private const string AuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

		private static void AssertDenied(Action call) => AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, call);

		/// <summary>ACL 응답을 (대상, 권한) 목록으로 줄인다. 대상은 main, alt, 그룹 URI 중 하나다.</summary>
		private List<(string, string)> GrantSet(GetACLResponse response)
		{
			var grants = new List<(string Target, string Permission)>();
			foreach (var grant in response.AccessControlList.Grants ?? [])
			{
				var grantee = grant.Grantee;
				var target = grantee.URI;
				if (string.IsNullOrEmpty(target))
				{
					if (grantee.CanonicalUser == Config.MainUser.UserId) target = "main";
					else if (grantee.CanonicalUser == Config.AltUser.UserId) target = "alt";
					else target = "other";
				}
				grants.Add((target, grant.Permission.Value));
			}
			grants.Sort((a, b) =>
			{
				var order = string.CompareOrdinal(a.Target, b.Target);
				return order != 0 ? order : string.CompareOrdinal(a.Permission, b.Permission);
			});
			return grants;
		}

		/// <summary>비공개 버킷의 오브젝트에 서브 사용자 권한 하나만 준다.</summary>
		private string SetupObjectGrant(string key, S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content");
			client.PutObjectACL(bucketName, key, accessControlPolicy: CreateAltAcl(permission));
			return bucketName;
		}

		/// <summary>.NET SDK는 키별 오류가 있으면 DeleteObjectsException을 던지므로 예외에서 응답을 꺼낸다.</summary>
		private static DeleteObjectsResponse DeleteObjectsWithErrors(S3Client client, string bucketName, List<KeyVersion> objects)
		{
			try { return client.DeleteObjects(bucketName, objects); }
			catch (AggregateException e) when (e.InnerException is DeleteObjectsException de) { return de.Response; }
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestBucketAclReadScope()
		{
			TestId = 47;
			var key = "TestBucketAclReadScope";
			var client = GetClient();
			var bucketName = SetupBucketPermission(S3Permission.READ);
			client.PutObject(bucketName, key, body: "content");
			var uploadId = client.InitiateMultipartUpload(bucketName, "multi").UploadId;

			var altClient = GetAltClient();
			// READ 는 목록 종류를 모두 연다
			altClient.HeadBucket(bucketName);
			Assert.Equal(1, altClient.ListObjectsV2(bucketName).KeyCount);
			Assert.Single(altClient.ListVersions(bucketName).Versions);
			Assert.Single(altClient.ListMultipartUploads(bucketName).MultipartUploads);
			// 목록 권한이 있으므로 없는 키는 404 다
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_KEY, () => altClient.GetObject(bucketName, "not-exist"));

			// 오브젝트 읽기·쓰기와 버킷 설정은 열지 않는다
			AssertDenied(() => altClient.GetObject(bucketName, key));
			AssertDenied(() => altClient.PutObject(bucketName, "new", body: "x"));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
			AssertDenied(() => altClient.InitiateMultipartUpload(bucketName, "new"));
			AssertDenied(() => altClient.ListParts(bucketName, "multi", uploadId));
			AssertDenied(() => altClient.GetBucketVersioning(bucketName));
			AssertDenied(() => altClient.GetBucketLocation(bucketName));
			var response = DeleteObjectsWithErrors(altClient, bucketName, [new KeyVersion() { Key = key }]);
			Assert.Empty(response.DeletedObjects ?? []);
			Assert.Equal(MainData.ACCESS_DENIED, response.DeleteErrors[0].Code);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestBucketAclWriteScope()
		{
			TestId = 48;
			var key = "TestBucketAclWriteScope";
			var client = GetClient();
			var bucketName = SetupBucketPermission(S3Permission.WRITE);
			client.PutObject(bucketName, key, body: "content");
			client.PutObject(bucketName, "other", body: "content");
			var uploadId = client.InitiateMultipartUpload(bucketName, "multi").UploadId;

			var altClient = GetAltClient();
			// WRITE 는 쓰기·덮어쓰기·삭제·멀티파트 시작을 연다(소유자가 올린 오브젝트도)
			altClient.PutObject(bucketName, "new", body: "x");
			altClient.PutObject(bucketName, key, body: "overwritten");
			altClient.DeleteObject(bucketName, key);
			var response = altClient.DeleteObjects(bucketName, [new KeyVersion() { Key = "other" }]);
			Assert.Equal(new List<string>() { "other" }, response.DeletedObjects.ConvertAll(d => d.Key));
			var newUploadId = altClient.InitiateMultipartUpload(bucketName, "alt-multi").UploadId;
			altClient.AbortMultipartUpload(bucketName, "alt-multi", newUploadId);
			Assert.Equal(new List<string>() { "new" }, GetKeys(client.ListObjectsV2(bucketName)));

			// 읽기·목록은 열지 않는다. 목록 권한이 없어 없는 키도 403 이다
			AssertS3Error(HttpStatusCode.Forbidden, null, () => altClient.HeadBucket(bucketName));
			AssertDenied(() => altClient.ListObjectsV2(bucketName));
			AssertDenied(() => altClient.ListMultipartUploads(bucketName));
			AssertDenied(() => altClient.GetObject(bucketName, "not-exist"));
			AssertDenied(() => altClient.GetBucketACL(bucketName));
			// 다른 사용자가 시작한 멀티파트는 중단할 수 없다
			AssertDenied(() => altClient.AbortMultipartUpload(bucketName, "multi", uploadId));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestBucketAclFullControlScope()
		{
			TestId = 49;
			var client = GetClient();
			var bucketName = SetupBucketPermission(S3Permission.FULL_CONTROL);
			var altClient = GetAltClient();
			Assert.Equal(0, altClient.ListObjectsV2(bucketName).KeyCount);
			altClient.PutObject(bucketName, "new", body: "x");
			altClient.DeleteObject(bucketName, "new");
			Assert.Equal(new List<(string, string)>() { ("alt", "FULL_CONTROL"), ("main", "FULL_CONTROL") },
				GrantSet(altClient.GetBucketACL(bucketName)));
			altClient.PutBucketACL(bucketName, accessControlPolicy: CreateAltAcl(S3Permission.FULL_CONTROL));

			// ACL 의 FULL_CONTROL 은 READ·WRITE·READ_ACP·WRITE_ACP 뿐이다. 버킷 설정과 삭제는 소유자만 한다
			AssertDenied(() => altClient.GetBucketVersioning(bucketName));
			AssertDenied(() => altClient.PutBucketVersioning(bucketName, status: VersionStatus.Enabled));
			AssertDenied(() => altClient.PutBucketTagging(bucketName, [new Tag() { Key = "a", Value = "b" }]));
			AssertDenied(() => altClient.GetBucketPolicy(bucketName));
			AssertDenied(() => altClient.DeleteBucket(bucketName));
			client.HeadBucket(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestObjectAclReadScope()
		{
			TestId = 50;
			var key = "TestObjectAclReadScope";
			var bucketName = SetupObjectGrant(key, S3Permission.READ);
			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			Assert.Equal(7, altClient.HeadObject(bucketName, key).ContentLength);
			// READ 는 본문과 메타데이터만 연다
			AssertDenied(() => altClient.GetObjectTagging(bucketName, key));
			AssertDenied(() => altClient.GetObjectAttributes(bucketName, key, [ObjectAttributes.ETag]));
			AssertDenied(() => altClient.GetObjectACL(bucketName, key));
			AssertDenied(() => altClient.PutObjectACL(bucketName, key, acl: S3CannedACL.Private));
			AssertDenied(() => altClient.PutObject(bucketName, key, body: "x"));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestObjectAclFullControlScope()
		{
			TestId = 51;
			var key = "TestObjectAclFullControlScope";
			var bucketName = SetupObjectGrant(key, S3Permission.FULL_CONTROL);
			var altClient = GetAltClient();
			Assert.Equal("content", GetBody(altClient.GetObject(bucketName, key)));
			Assert.Contains(("alt", "FULL_CONTROL"), GrantSet(altClient.GetObjectACL(bucketName, key)));
			// 오브젝트의 FULL_CONTROL 에는 쓰기가 없다. 덮어쓰기·삭제는 버킷 권한으로 판정한다
			AssertDenied(() => altClient.PutObject(bucketName, key, body: "x"));
			AssertDenied(() => altClient.DeleteObject(bucketName, key));
			AssertDenied(() => altClient.GetObjectTagging(bucketName, key));
			AssertDenied(() => altClient.PutObjectTagging(bucketName, key,
				new Tagging() { TagSet = [new Tag() { Key = "a", Value = "b" }] }));
			// WRITE_ACP 가 있으므로 ACL 은 바꿀 수 있다
			altClient.PutObjectACL(bucketName, key, acl: S3CannedACL.Private);
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Copy")]
		public void TestObjectAclNotCopied()
		{
			TestId = 52;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, "source", body: "content", acl: S3CannedACL.PublicRead);
			Assert.Equal(new List<(string, string)>() { (AllUsers, "READ"), ("main", "FULL_CONTROL") },
				GrantSet(client.GetObjectACL(bucketName, "source")));

			// 복사본은 원본의 ACL 을 물려받지 않는다
			client.CopyObject(bucketName, "source", bucketName, "copy");
			Assert.Equal(new List<(string, string)>() { ("main", "FULL_CONTROL") }, GrantSet(client.GetObjectACL(bucketName, "copy")));
			// 복사 요청에 준 ACL 이 적용된다
			client.CopyObject(bucketName, "source", bucketName, "canned", acl: S3CannedACL.AuthenticatedRead);
			Assert.Equal(new List<(string, string)>() { (AuthenticatedUsers, "READ"), ("main", "FULL_CONTROL") },
				GrantSet(client.GetObjectACL(bucketName, "canned")));
			// grant 헤더만 주면 소유자 전권은 붙지 않는다
			client.CopyObject(bucketName, "source", bucketName, "grant",
				headerList: [new("x-amz-grant-read", $"id=\"{Config.AltUser.UserId}\"")]);
			Assert.Equal(new List<(string, string)>() { ("alt", "READ") }, GrantSet(client.GetObjectACL(bucketName, "grant")));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Overwrite")]
		public void TestObjectAclResetOnOverwrite()
		{
			TestId = 53;
			var key = "TestObjectAclResetOnOverwrite";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content", acl: S3CannedACL.PublicRead);
			var publicClient = GetPublicClient();
			Assert.Equal("content", GetBody(publicClient.GetObject(bucketName, key)));

			// ACL 없이 덮어쓰면 비공개로 돌아간다
			client.PutObject(bucketName, key, body: "content");
			Assert.Equal(new List<(string, string)>() { ("main", "FULL_CONTROL") }, GrantSet(client.GetObjectACL(bucketName, key)));
			AssertDenied(() => GetPublicClient().GetObject(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Multipart")]
		public void TestObjectAclMultipart()
		{
			TestId = 54;
			var key = "TestObjectAclMultipart";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var uploadId = client.InitiateMultipartUpload(bucketName, key, headerList: [new("x-amz-acl", "public-read")]).UploadId;
			var etag = client.UploadPart(bucketName, key, uploadId, "content", 1).ETag;
			client.CompleteMultipartUpload(bucketName, key, uploadId, [new PartETag(1, etag)]);
			// 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트의 ACL 이 된다
			Assert.Equal(new List<(string, string)>() { (AllUsers, "READ"), ("main", "FULL_CONTROL") },
				GrantSet(client.GetObjectACL(bucketName, key)));
			var publicClient = GetPublicClient();
			Assert.Equal("content", GetBody(publicClient.GetObject(bucketName, key)));
			publicClient.HeadObject(bucketName, key);
			// public-read 는 ACL 조회와 태그 조회를 열지 않는다
			AssertDenied(() => publicClient.GetObjectACL(bucketName, key));
			AssertDenied(() => publicClient.GetObjectTagging(bucketName, key));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Version")]
		public void TestObjectAclPerVersion()
		{
			TestId = 55;
			var key = "TestObjectAclPerVersion";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var version1 = client.PutObject(bucketName, key, body: "1", acl: S3CannedACL.PublicRead).VersionId;
			var version2 = client.PutObject(bucketName, key, body: "2").VersionId;

			// ACL 은 버전마다 따로다
			Assert.Equal(new List<(string, string)>() { (AllUsers, "READ"), ("main", "FULL_CONTROL") },
				GrantSet(client.GetObjectACL(bucketName, key, versionId: version1)));
			Assert.Equal(new List<(string, string)>() { ("main", "FULL_CONTROL") },
				GrantSet(client.GetObjectACL(bucketName, key, versionId: version2)));
			var publicClient = GetPublicClient();
			Assert.Equal("1", GetBody(publicClient.GetObject(bucketName, key, versionId: version1)));
			AssertDenied(() => publicClient.GetObject(bucketName, key));
			AssertDenied(() => publicClient.GetObject(bucketName, key, versionId: version2));
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Canned")]
		public void TestAclCannedBucketOwner()
		{
			TestId = 56;
			var key = "TestAclCannedBucketOwner";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutObject(bucketName, key, body: "content", acl: S3CannedACL.PublicRead);
			// 버킷 소유자가 곧 오브젝트 소유자면 bucket-owner-* 는 소유자 전권만 남긴다
			foreach (var acl in new[] { S3CannedACL.BucketOwnerRead, S3CannedACL.BucketOwnerFullControl })
			{
				client.PutObjectACL(bucketName, key, acl: acl);
				Assert.Equal(new List<(string, string)>() { ("main", "FULL_CONTROL") }, GrantSet(client.GetObjectACL(bucketName, key)));
			}
			// 버킷에 주면 무시되고 비공개가 된다
			client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
			foreach (var acl in new[] { S3CannedACL.BucketOwnerRead, S3CannedACL.BucketOwnerFullControl })
			{
				client.PutBucketACL(bucketName, acl: acl);
				Assert.Equal(new List<(string, string)>() { ("main", "FULL_CONTROL") }, GrantSet(client.GetBucketACL(bucketName)));
			}
		}

		[Fact]
		[Trait(MainData.Major, "ACL")]
		[Trait(MainData.Minor, "Scope")]
		public void TestPublicRwBucketAnonymousScope()
		{
			TestId = 57;
			var key = "TestPublicRwBucketAnonymousScope";
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicReadWrite);
			client.PutObject(bucketName, key, body: "content");
			client.InitiateMultipartUpload(bucketName, "multi");

			var publicClient = GetPublicClient();
			Assert.Equal(1, publicClient.ListObjectsV2(bucketName).KeyCount);
			Assert.Single(publicClient.ListVersions(bucketName).Versions);
			Assert.Single(publicClient.ListMultipartUploads(bucketName).MultipartUploads);
			publicClient.PutObject(bucketName, "anonymous", body: "x");
			Assert.Equal("x", GetBody(publicClient.GetObject(bucketName, "anonymous")));

			// 버킷이 열려 있어도 비공개 오브젝트와 ACL 은 열리지 않는다
			AssertDenied(() => publicClient.GetObject(bucketName, key));
			AssertDenied(() => publicClient.GetBucketACL(bucketName));
			AssertDenied(() => publicClient.PutBucketACL(bucketName, acl: S3CannedACL.Private));
			// 익명 사용자는 멀티파트 업로드를 시작할 수 없다
			AssertDenied(() => publicClient.InitiateMultipartUpload(bucketName, "new"));
		}

		#endregion
	}
}
