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
package org.example.test;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

public class ACL extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ACL Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ACL End");
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketAndObject() {
		var mainKey = "testDefaultObjectPutGetMain";
		var altKey = "testDefaultObjectPutGetAlt";
		var publicKey = "testDefaultObjectPutGetPublic";
		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.Private,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketPublicReadObject() {
		var mainKey = "testPrivateBucketPublicObjectMain";
		var altKey = "testPrivateBucketPublicObjectAlt";
		var publicKey = "testPrivateBucketPublicObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.PublicRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketPublicRWObject() {
		var mainKey = "testPrivateBucketPublicRWObjectMain";
		var altKey = "testPrivateBucketPublicRWObjectAlt";
		var publicKey = "testPrivateBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketAuthenticatedReadObject() {
		var mainKey = "testPrivateBucketAuthenticatedObjectMain";
		var altKey = "testPrivateBucketAuthenticatedObjectAlt";
		var publicKey = "testPrivateBucketAuthenticatedObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.AuthenticatedRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerReadObject() {
		var mainKey = "testPrivateBucketBucketOwnerReadObjectMain";
		var altKey = "testPrivateBucketBucketOwnerReadObjectAlt";
		var publicKey = "testPrivateBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerReadObjectUploadAltUser() {
		var mainKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserMain";
		var altKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserAlt";
		var publicKey = "testPrivateBucketBucketOwnerReadObjectUploadAltUserPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private, CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		failedGetObject(client, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, mainKey, mainKey);
		failedPutObject(client, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerFullControlObject() {
		var mainKey = "testPrivateBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPrivateBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testPrivateBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.Private,
				CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketPrivateObject() {
		var mainKey = "testPublicReadBucketPrivateObjectMain";
		var altKey = "testPublicReadBucketPrivateObjectAlt";
		var publicKey = "testPublicReadBucketPrivateObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead, CannedAccessControlList.Private,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketAndObject() {
		var mainKey = "testPublicReadBucketAndObjectMain";
		var altKey = "testPublicReadBucketAndObjectAlt";
		var publicKey = "testPublicReadBucketAndObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketPublicRWObject() {
		var mainKey = "testPublicReadBucketPublicRWObjectMain";
		var altKey = "testPublicReadBucketPublicRWObjectAlt";
		var publicKey = "testPublicReadBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicReadWrite,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketAuthenticatedReadObject() {
		var mainKey = "testPublicReadBucketAuthenticatedReadObjectMain";
		var altKey = "testPublicReadBucketAuthenticatedReadObjectAlt";
		var publicKey = "testPublicReadBucketAuthenticatedReadObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead, CannedAccessControlList.AuthenticatedRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketBucketOwnerReadObject() {
		var mainKey = "testPublicReadBucketBucketOwnerReadObjectMain";
		var altKey = "testPublicReadBucketBucketOwnerReadObjectAlt";
		var publicKey = "testPublicReadBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead, CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicReadBucketBucketOwnerFullControlObject() {
		var mainKey = "testPublicReadBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPublicReadBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testPublicReadBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicRead,
				CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPrivateObject() {
		var mainKey = "testPublicRWBucketPrivateObjectMain";
		var altKey = "testPublicRWBucketPrivateObjectAlt";
		var altNewKey = "testPublicRWBucketPrivateObjectAltNew";
		var publicKey = "testPublicRWBucketPrivateObjectPublic";
		var publicNewKey = "testPublicRWBucketPrivateObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPrivateObjectByAltUser() {
		var mainKey = "testPublicRWBucketPrivateObjectByAltUserMain";
		var altKey = "testPublicRWBucketPrivateObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketPrivateObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPrivateObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		failedGetObject(client, bucketName, mainKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

		altClient.deleteObject(bucketName, altKey);
		altClient.deleteObject(bucketName, publicKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicReadObject() {
		var mainKey = "testPublicRWBucketPublicReadObjectMain";
		var altKey = "testPublicRWBucketPublicReadObjectAlt";
		var altNewKey = "testPublicRWBucketPublicReadObjectAltNew";
		var publicKey = "testPublicRWBucketPublicReadObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketPublicReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketPublicReadObjectByAltUserAlt";
		var altNewKey = "testPublicRWBucketPublicReadObjectByAltUserAltNew";
		var publicKey = "testPublicRWBucketPublicReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

		altClient.deleteObject(bucketName, altKey);
		altClient.deleteObject(bucketName, publicKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicRWObject() {
		var mainKey = "testPublicRWBucketPublicRWObjectMain";
		var altKey = "testPublicRWBucketPublicRWObjectAlt";
		var altNewKey = "testPublicRWBucketPublicRWObjectAltNew";
		var publicKey = "testPublicRWBucketPublicRWObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicRWObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicReadWrite,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicRWObjectByAltUser() {
		var mainKey = "testPublicRWBucketPublicRWObjectByAltUserMain";
		var altKey = "testPublicRWBucketPublicRWObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketPublicRWObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPublicRWObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicReadWrite,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketAuthenticatedReadObject() {
		var mainKey = "testPublicRWBucketAuthenticatedReadObjectMain";
		var altKey = "testPublicRWBucketAuthenticatedReadObjectAlt";
		var altNewKey = "testPublicRWBucketAuthenticatedReadObjectAltNew";
		var publicKey = "testPublicRWBucketAuthenticatedReadObjectPublic";
		var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.AuthenticatedRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketAuthenticatedReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketAuthenticatedReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.AuthenticatedRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerReadObject() {
		var mainKey = "testPublicRWBucketBucketOwnerReadObjectMain";
		var altKey = "testPublicRWBucketBucketOwnerReadObjectAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerReadObjectAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerReadObjectPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerReadObjectByAltUser() {
		var mainKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserMain";
		var altKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObject() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectPublicNew";

		var bucketName = setupAclObjects(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObjectByAltUser() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserAlt";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred() {
		var mainKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredMain";
		var altKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAlt";
		var altNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredAltNew";
		var publicKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublic";
		var publicNewKey = "testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferredPublicNew";

		var bucketName = setupAclObjectsByAlt(ObjectOwnership.BucketOwnerPreferred,
				CannedAccessControlList.PublicReadWrite, CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(altClient, bucketName, altNewKey, altNewKey);
		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPrivateObject() {
		var mainKey = "testAuthenticatedReadBucketPrivateObjectMain";
		var altKey = "testAuthenticatedReadBucketPrivateObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPrivateObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.Private,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPublicReadObject() {
		var mainKey = "testAuthenticatedReadBucketPublicReadObjectMain";
		var altKey = "testAuthenticatedReadBucketPublicReadObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPublicReadObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead, CannedAccessControlList.PublicRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketPublicRWObject() {
		var mainKey = "testAuthenticatedReadBucketPublicRWObjectMain";
		var altKey = "testAuthenticatedReadBucketPublicRWObjectAlt";
		var publicKey = "testAuthenticatedReadBucketPublicRWObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead,
				CannedAccessControlList.PublicReadWrite,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketAndObject() {
		var mainKey = "testAuthenticatedReadBucketAndObjectMain";
		var altKey = "testAuthenticatedReadBucketAndObjectAlt";
		var publicKey = "testAuthenticatedReadBucketAndObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead,
				CannedAccessControlList.AuthenticatedRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketBucketOwnerReadObject() {
		var mainKey = "testAuthenticatedReadBucketBucketOwnerReadObjectMain";
		var altKey = "testAuthenticatedReadBucketBucketOwnerReadObjectAlt";
		var publicKey = "testAuthenticatedReadBucketBucketOwnerReadObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead,
				CannedAccessControlList.BucketOwnerRead,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testAuthenticatedReadBucketBucketOwnerFullControlObject() {
		var mainKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectMain";
		var altKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testAuthenticatedReadBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(CannedAccessControlList.AuthenticatedRead,
				CannedAccessControlList.BucketOwnerFullControl,
				mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		failedGetObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		failedPutObject(altClient, bucketName, altKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("List")
	public void testPrivateBucketList() {
		var keys = List.of("testPrivateBucketList1", "testPrivateBucketList2", "testPrivateBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(CannedAccessControlList.Private, keys);

		succeedListObjects(client, bucketName, keys);
		failedListObjects(altClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		failedListObjects(publicClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("List")
	public void testPublicReadBucketList() {
		var keys = List.of("testPublicReadBucketList1", "testPublicReadBucketList2", "testPublicReadBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(CannedAccessControlList.PublicRead, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		succeedListObjects(publicClient, bucketName, keys);
	}

	@Test
	@Tag("List")
	public void testPublicRWBucketList() {
		var keys = List.of("testPublicRWBucketList1", "testPublicRWBucketList2", "testPublicRWBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(CannedAccessControlList.PublicReadWrite, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		succeedListObjects(publicClient, bucketName, keys);
	}

	@Test
	@Tag("List")
	public void testAuthenticatedReadBucketList() {
		var keys = List.of("testAuthenticatedReadBucketList1",
				"testAuthenticatedReadBucketList2",
				"testAuthenticatedReadBucketList3");
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = setupAclBucket(CannedAccessControlList.AuthenticatedRead, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		failedListObjects(publicClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = setupBucketPermission(Permission.FullControl);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserRead() {
		var bucketName = setupBucketPermission(Permission.Read);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = setupBucketPermission(Permission.ReadAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWrite() {
		var bucketName = setupBucketPermission(Permission.Write);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = setupBucketPermission(Permission.WriteAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserFullControl() {
		var key = "testObjectPermissionAltUserFullControl";
		var bucketName = setupObjectPermission(key, Permission.FullControl);
		var altClient = getAltClient();

		checkObjectAclAllowRead(altClient, bucketName, key);
		checkObjectAclAllowReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclAllowWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserRead() {
		var key = "testObjectPermissionAltUserRead";
		var bucketName = setupObjectPermission(key, Permission.Read);
		var altClient = getAltClient();

		checkObjectAclAllowRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserReadAcp() {
		var key = "testObjectPermissionAltUserReadAcp";
		var bucketName = setupObjectPermission(key, Permission.ReadAcp);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclAllowReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserWrite() {
		var key = "testObjectPermissionAltUserWrite";
		var bucketName = setupObjectPermission(key, Permission.Write);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclDenyWriteACP(altClient, bucketName, key);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionAltUserWriteAcp() {
		var key = "testObjectPermissionAltUserWriteAcp";
		var bucketName = setupObjectPermission(key, Permission.WriteAcp);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclAllowWriteACP(altClient, bucketName, key);
	}
}
