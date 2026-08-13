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
	}
}
