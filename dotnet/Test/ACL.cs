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
			var mainKey = "testDefaultObjectPutGetMain";
			var altKey = "testDefaultObjectPutGetAlt";
			var publicKey = "testDefaultObjectPutGetPublic";
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
			var mainKey = "testPrivateBucketPublicObjectMain";
			var altKey = "testPrivateBucketPublicObjectAlt";
			var publicKey = "testPrivateBucketPublicObjectPublic";
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
			var mainKey = "testPrivateBucketPublicRWObjectMain";
			var altKey = "testPrivateBucketPublicRWObjectAlt";
			var publicKey = "testPrivateBucketPublicRWObjectPublic";
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
			var mainKey = "testPrivateBucketAuthenticatedObjectMain";
			var altKey = "testPrivateBucketAuthenticatedObjectAlt";
			var publicKey = "testPrivateBucketAuthenticatedObjectPublic";
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
			var mainKey = "testPrivateBucketBucketOwnerReadObjectMain";
			var altKey = "testPrivateBucketBucketOwnerReadObjectAlt";
			var publicKey = "testPrivateBucketBucketOwnerReadObjectPublic";
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
			var mainKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserMain";
			var altKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserAlt";
			var publicKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserPublic";
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
			var mainKey = "testPrivateBucketBucketOwnerFullControlObjectMain";
			var altKey = "testPrivateBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "testPrivateBucketBucketOwnerFullControlObjectPublic";
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
			var mainKey = "testPublicReadBucketPrivateObjectMain";
			var altKey = "testPublicReadBucketPrivateObjectAlt";
			var publicKey = "testPublicReadBucketPrivateObjectPublic";
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
			var mainKey = "testPublicReadBucketAndObjectMain";
			var altKey = "testPublicReadBucketAndObjectAlt";
			var publicKey = "testPublicReadBucketAndObjectPublic";
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
			var mainKey = "testPublicReadBucketPublicRWObjectMain";
			var altKey = "testPublicReadBucketPublicRWObjectAlt";
			var publicKey = "testPublicReadBucketPublicRWObjectPublic";
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
			var mainKey = "testPublicReadBucketAuthenticatedReadObjectMain";
			var altKey = "testPublicReadBucketAuthenticatedReadObjectAlt";
			var publicKey = "testPublicReadBucketAuthenticatedReadObjectPublic";
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
			var mainKey = "testPublicReadBucketBucketOwnerReadObjectMain";
			var altKey = "testPublicReadBucketBucketOwnerReadObjectAlt";
			var publicKey = "testPublicReadBucketBucketOwnerReadObjectPublic";
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
			var mainKey = "testPublicReadBucketBucketOwnerFullControlObjectMain";
			var altKey = "testPublicReadBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "testPublicReadBucketBucketOwnerFullControlObjectPublic";
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
			var mainKey = "testPublicRWBucketPrivateObjectMain";
			var altKey = "testPublicRWBucketPrivateObjectAlt";
			var altNewKey = "testPublicRWBucketPrivateObjectAltNew";
			var publicKey = "testPublicRWBucketPrivateObjectPublic";
			var publicNewKey = "testPublicRWBucketPrivateObjectPublicNew";
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
			var mainKey = "testPublicRWBucketPrivateObjectByAltUserMain";
			var altKey = "testPublicRWBucketPrivateObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketPrivateObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketPrivateObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketPublicReadObjectMain";
			var altKey = "testPublicRWBucketPublicReadObjectAlt";
			var altNewKey = "testPublicRWBucketPublicReadObjectAltNew";
			var publicKey = "testPublicRWBucketPublicReadObjectPublic";
			var publicNewKey = "testPublicRWBucketPublicReadObjectPublicNew";
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
			var mainKey = "testPublicRWBucketPublicReadObjectByAltUserMain";
			var altKey = "testPublicRWBucketPublicReadObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketPublicReadObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketPublicReadObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketPublicRWObjectMain";
			var altKey = "testPublicRWBucketPublicRWObjectAlt";
			var altNewKey = "testPublicRWBucketPublicRWObjectAltNew";
			var publicKey = "testPublicRWBucketPublicRWObjectPublic";
			var publicNewKey = "testPublicRWBucketPublicRWObjectPublicNew";
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
			var mainKey = "testPublicRWBucketPublicRWObjectByAltUserMain";
			var altKey = "testPublicRWBucketPublicRWObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketPublicRWObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketPublicRWObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketAuthenticatedReadObjectMain";
			var altKey = "testPublicRWBucketAuthenticatedReadObjectAlt";
			var altNewKey = "testPublicRWBucketAuthenticatedReadObjectAltNew";
			var publicKey = "testPublicRWBucketAuthenticatedReadObjectPublic";
			var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectPublicNew";
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
			var mainKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserMain";
			var altKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketBucketOwnerReadObjectMain";
			var altKey = "testPublicRWBucketBucketOwnerReadObjectAlt";
			var altNewKey = "testPublicRWBucketBucketOwnerReadObjectAltNew";
			var publicKey = "testPublicRWBucketBucketOwnerReadObjectPublic";
			var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectPublicNew";
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
			var mainKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserMain";
			var altKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectMain";
			var altKey = "testPublicRWBucketBucketOwnerFullControlObjectAlt";
			var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectAltNew";
			var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectPublic";
			var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectPublicNew";
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
			var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserMain";
			var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserAlt";
			var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublic";
			var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublicNew";
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
			var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredMain";
			var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAlt";
			var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAltNew";
			var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublic";
			var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublicNew";
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
			var mainKey = "testAuthenticatedReadBucketPrivateObjectMain";
			var altKey = "testAuthenticatedReadBucketPrivateObjectAlt";
			var publicKey = "testAuthenticatedReadBucketPrivateObjectPublic";
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
			var mainKey = "testAuthenticatedReadBucketPublicReadObjectMain";
			var altKey = "testAuthenticatedReadBucketPublicReadObjectAlt";
			var publicKey = "testAuthenticatedReadBucketPublicReadObjectPublic";
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
			var mainKey = "testAuthenticatedReadBucketPublicRWObjectMain";
			var altKey = "testAuthenticatedReadBucketPublicRWObjectAlt";
			var publicKey = "testAuthenticatedReadBucketPublicRWObjectPublic";
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
			var mainKey = "testAuthenticatedReadBucketAndObjectMain";
			var altKey = "testAuthenticatedReadBucketAndObjectAlt";
			var publicKey = "testAuthenticatedReadBucketAndObjectPublic";
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
			var mainKey = "testAuthenticatedReadBucketBucketOwnerReadObjectMain";
			var altKey = "testAuthenticatedReadBucketBucketOwnerReadObjectAlt";
			var publicKey = "testAuthenticatedReadBucketBucketOwnerReadObjectPublic";
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
			var mainKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectMain";
			var altKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectAlt";
			var publicKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectPublic";
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
			var key = "testObjectPermissionAltUserFullControl";
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
			var key = "testObjectPermissionAltUserRead";
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
			var key = "testObjectPermissionAltUserReadAcp";
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
			var key = "testObjectPermissionAltUserWrite";
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
			var key = "testObjectPermissionAltUserWriteAcp";
			var bucketName = SetupObjectPermission(key, S3Permission.WRITE_ACP);
			var altClient = GetAltClient();

			CheckObjectAclDenyRead(altClient, bucketName, key);
			CheckObjectAclDenyReadACP(altClient, bucketName, key);
			CheckObjectAclDenyWrite(altClient, bucketName, key);
			CheckObjectAclAllowWriteACP(altClient, bucketName, key);
		}
	}
}
