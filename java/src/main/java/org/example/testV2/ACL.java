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
package org.example.testV2;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.Permission;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.jupiter.api.function.Executable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;

public class ACL extends TestBase {
	@Test
	@Tag("Access")
	public void testPrivateBucketAndObject() {
		var mainKey = "testDefaultObjectPutGetMain";
		var altKey = "testDefaultObjectPutGetAlt";
		var publicKey = "testDefaultObjectPutGetPublic";
		var bucketName = setupAclObjects(1, BucketCannedACL.PRIVATE, ObjectCannedACL.PRIVATE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(2, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(3, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ_WRITE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(4, BucketCannedACL.PRIVATE, ObjectCannedACL.AUTHENTICATED_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(5, BucketCannedACL.PRIVATE, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(6, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));

		succeedGetObject(altClient, bucketName, mainKey, mainKey);
		succeedGetObject(client, bucketName, altKey, altKey);
		failedGetObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		failedPutObject(altClient, bucketName, mainKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		succeedPutObject(client, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Access")
	public void testPrivateBucketBucketOwnerFullControlObject() {
		var mainKey = "testPrivateBucketBucketOwnerFullControlObjectMain";
		var altKey = "testPrivateBucketBucketOwnerFullControlObjectAlt";
		var publicKey = "testPrivateBucketBucketOwnerFullControlObjectPublic";

		var bucketName = setupAclObjects(7, BucketCannedACL.PRIVATE, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(8, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PRIVATE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(9, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(10, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ_WRITE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(11, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.AUTHENTICATED_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(12, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(13, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(14, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PRIVATE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(15, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PRIVATE, mainKey, altKey, publicKey);

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

		altClient.deleteObject(d -> d.bucket(bucketName).key(altKey));
		altClient.deleteObject(d -> d.bucket(bucketName).key(publicKey));
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicReadObject() {
		var mainKey = "testPublicRWBucketPublicReadObjectMain";
		var altKey = "testPublicRWBucketPublicReadObjectAlt";
		var altNewKey = "testPublicRWBucketPublicReadObjectAltNew";
		var publicKey = "testPublicRWBucketPublicReadObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectPublicNew";

		var bucketName = setupAclObjects(16, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PUBLIC_READ, mainKey, altKey, publicKey);

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
		var publicKey = "testPublicRWBucketPublicReadObjectByAltUserPublic";
		var publicNewKey = "testPublicRWBucketPublicReadObjectByAltUserPublicNew";

		var bucketName = setupAclObjectsByAlt(17, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PUBLIC_READ, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
		succeedGetObject(altClient, bucketName, altKey, altKey);
		succeedGetObject(publicClient, bucketName, publicKey, publicKey);

		succeedPutObject(client, bucketName, mainKey, mainKey);
		succeedPutObject(altClient, bucketName, altKey, altKey);
		failedPutObject(publicClient, bucketName, publicKey, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		succeedPutObject(publicClient, bucketName, publicNewKey, publicNewKey);

		altClient.deleteObject(d -> d.bucket(bucketName).key(altKey));
		altClient.deleteObject(d -> d.bucket(bucketName).key(publicKey));
	}

	@Test
	@Tag("Access")
	public void testPublicRWBucketPublicRWObject() {
		var mainKey = "testPublicRWBucketPublicRWObjectMain";
		var altKey = "testPublicRWBucketPublicRWObjectAlt";
		var altNewKey = "testPublicRWBucketPublicRWObjectAltNew";
		var publicKey = "testPublicRWBucketPublicRWObjectPublic";
		var publicNewKey = "testPublicRWBucketPublicRWObjectPublicNew";

		var bucketName = setupAclObjects(18, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PUBLIC_READ_WRITE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(19, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.PUBLIC_READ_WRITE, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
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

		var bucketName = setupAclObjects(20, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.AUTHENTICATED_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(21, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.AUTHENTICATED_READ, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
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

		var bucketName = setupAclObjects(22, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(23, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
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

		var bucketName = setupAclObjects(24, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjectsByAlt(25, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();

		succeedGetObject(client, bucketName, mainKey, mainKey);
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

		var bucketName = setupAclObjectsByAlt(26, ObjectOwnership.BUCKET_OWNER_PREFERRED, BucketCannedACL.PUBLIC_READ_WRITE, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(27, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.PRIVATE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(28, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.PUBLIC_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(29, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.PUBLIC_READ_WRITE, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(30, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.AUTHENTICATED_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(31, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.BUCKET_OWNER_READ, mainKey, altKey, publicKey);

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

		var bucketName = setupAclObjects(32, BucketCannedACL.AUTHENTICATED_READ, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, mainKey, altKey, publicKey);

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
		var bucketName = setupAclBucket(33, BucketCannedACL.PRIVATE, keys);

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
		var bucketName = setupAclBucket(34, BucketCannedACL.PUBLIC_READ, keys);

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
		var bucketName = setupAclBucket(35, BucketCannedACL.PUBLIC_READ_WRITE, keys);

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
		var bucketName = setupAclBucket(36, BucketCannedACL.AUTHENTICATED_READ, keys);

		succeedListObjects(client, bucketName, keys);
		succeedListObjects(altClient, bucketName, keys);
		failedListObjects(publicClient, bucketName, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = setupBucketPermission(37, Permission.FULL_CONTROL);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserRead() {
		var bucketName = setupBucketPermission(38, Permission.READ);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = setupBucketPermission(39, Permission.READ_ACP);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWrite() {
		var bucketName = setupBucketPermission(40, Permission.WRITE);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = setupBucketPermission(41, Permission.WRITE_ACP);
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
		var bucketName = setupObjectPermission(42, key, Permission.FULL_CONTROL);
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
		var bucketName = setupObjectPermission(43, key, Permission.READ);
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
		var bucketName = setupObjectPermission(44, key, Permission.READ_ACP);
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
		var bucketName = setupObjectPermission(45, key, Permission.WRITE);
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
		var bucketName = setupObjectPermission(46, key, Permission.WRITE_ACP);
		var altClient = getAltClient();

		checkObjectAclDenyRead(altClient, bucketName, key);
		checkObjectAclDenyReadACP(altClient, bucketName, key);
		checkObjectAclDenyWrite(altClient, bucketName, key);
		checkObjectAclAllowWriteACP(altClient, bucketName, key);
	}

	// region ACL 권한 범위 (AWS 실측 기준)

	private static void assertDenied(Executable executable) {
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, executable);
	}

	/** ACL 응답을 "대상 권한" 목록으로 줄인다. 대상은 main, alt, 그룹 URI 중 하나다. */
	private List<String> grantSet(List<Grant> grants) {
		var result = new ArrayList<String>();
		for (var grant : grants) {
			var grantee = grant.grantee();
			var target = grantee.uri();
			if (target == null || target.isEmpty()) {
				if (config.mainUser.id.equals(grantee.id()))
					target = "main";
				else if (config.altUser.id.equals(grantee.id()))
					target = "alt";
				else
					target = "other";
			}
			result.add(target + " " + grant.permissionAsString());
		}
		Collections.sort(result);
		return result;
	}

	/** 비공개 버킷의 오브젝트에 서브 사용자 권한 하나만 준다. */
	private String setupObjectGrant(String key, Permission permission, int testId) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, testId);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(createAltAcl(permission)));
		return bucketName;
	}

	@Test
	@Tag("Scope")
	public void testBucketAclReadScope() {
		var key = "testBucketAclReadScope";
		var client = getClient();
		var bucketName = setupBucketPermission(47, Permission.READ);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key("multi")).uploadId();

		var altClient = getAltClient();
		// READ 는 목록 종류를 모두 연다
		altClient.headBucket(h -> h.bucket(bucketName));
		assertEquals(1, altClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		assertEquals(1, altClient.listObjectVersions(l -> l.bucket(bucketName)).versions().size());
		assertEquals(1, altClient.listMultipartUploads(l -> l.bucket(bucketName)).uploads().size());
		// 목록 권한이 있으므로 없는 키는 404 다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> altClient.getObject(g -> g.bucket(bucketName).key("not-exist")));

		// 오브젝트 읽기·쓰기와 버킷 설정은 열지 않는다
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.createMultipartUpload(c -> c.bucket(bucketName).key("new")));
		assertDenied(() -> altClient.listParts(l -> l.bucket(bucketName).key("multi").uploadId(uploadId)));
		assertDenied(() -> altClient.getBucketVersioning(g -> g.bucket(bucketName)));
		assertDenied(() -> altClient.getBucketLocation(g -> g.bucket(bucketName)));
		var response = altClient
				.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(i -> i.key(key))));
		assertTrue(response.deleted().isEmpty());
		assertEquals(MainData.ACCESS_DENIED, response.errors().get(0).code());
	}

	@Test
	@Tag("Scope")
	public void testBucketAclWriteScope() {
		var key = "testBucketAclWriteScope";
		var client = getClient();
		var bucketName = setupBucketPermission(48, Permission.WRITE);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		client.putObject(p -> p.bucket(bucketName).key("other"), RequestBody.fromString("content"));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key("multi")).uploadId();

		var altClient = getAltClient();
		// WRITE 는 쓰기·덮어쓰기·삭제·멀티파트 시작을 연다(소유자가 올린 오브젝트도)
		altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x"));
		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("overwritten"));
		altClient.deleteObject(d -> d.bucket(bucketName).key(key));
		var response = altClient
				.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(i -> i.key("other"))));
		assertEquals(1, response.deleted().size());
		assertEquals("other", response.deleted().get(0).key());
		var newUploadId = altClient.createMultipartUpload(c -> c.bucket(bucketName).key("alt-multi")).uploadId();
		altClient.abortMultipartUpload(a -> a.bucket(bucketName).key("alt-multi").uploadId(newUploadId));
		assertEquals(List.of("new"), getKeys(client.listObjectsV2(l -> l.bucket(bucketName)).contents()));

		// 읽기·목록은 열지 않는다. 목록 권한이 없어 없는 키도 403 이다
		assertS3Error(HttpStatus.SC_FORBIDDEN, null, () -> altClient.headBucket(h -> h.bucket(bucketName)));
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.listMultipartUploads(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("not-exist")));
		assertDenied(() -> altClient.getBucketAcl(g -> g.bucket(bucketName)));
		// 다른 사용자가 시작한 멀티파트는 중단할 수 없다
		assertDenied(
				() -> altClient.abortMultipartUpload(a -> a.bucket(bucketName).key("multi").uploadId(uploadId)));
	}

	@Test
	@Tag("Scope")
	public void testBucketAclFullControlScope() {
		var client = getClient();
		var bucketName = setupBucketPermission(49, Permission.FULL_CONTROL);
		var altClient = getAltClient();
		assertEquals(0, altClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x"));
		altClient.deleteObject(d -> d.bucket(bucketName).key("new"));
		assertEquals(List.of("alt FULL_CONTROL", "main FULL_CONTROL"),
				grantSet(altClient.getBucketAcl(g -> g.bucket(bucketName)).grants()));
		altClient.putBucketAcl(
				p -> p.bucket(bucketName).accessControlPolicy(createAltAcl(Permission.FULL_CONTROL)));

		// ACL 의 FULL_CONTROL 은 READ·WRITE·READ_ACP·WRITE_ACP 뿐이다. 버킷 설정과 삭제는 소유자만 한다
		assertDenied(() -> altClient.getBucketVersioning(g -> g.bucket(bucketName)));
		assertDenied(() -> altClient.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED))));
		assertDenied(() -> altClient
				.putBucketTagging(p -> p.bucket(bucketName).tagging(t -> t.tagSet(s -> s.key("a").value("b")))));
		assertDenied(() -> altClient.getBucketPolicy(g -> g.bucket(bucketName)));
		assertDenied(() -> altClient.deleteBucket(d -> d.bucket(bucketName)));
		client.headBucket(h -> h.bucket(bucketName));
	}

	@Test
	@Tag("Scope")
	public void testObjectAclReadScope() {
		var key = "testObjectAclReadScope";
		var bucketName = setupObjectGrant(key, Permission.READ, 50);
		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertEquals(7L, altClient.headObject(h -> h.bucket(bucketName).key(key)).contentLength().longValue());
		// READ 는 본문과 메타데이터만 연다
		assertDenied(() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient
				.getObjectAttributes(g -> g.bucket(bucketName).key(key).objectAttributes(ObjectAttributes.E_TAG)));
		assertDenied(() -> altClient.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE)));
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("x")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Scope")
	public void testObjectAclFullControlScope() {
		var key = "testObjectAclFullControlScope";
		var bucketName = setupObjectGrant(key, Permission.FULL_CONTROL, 51);
		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertTrue(grantSet(altClient.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants())
				.contains("alt FULL_CONTROL"));
		// 오브젝트의 FULL_CONTROL 에는 쓰기가 없다. 덮어쓰기·삭제는 버킷 권한으로 판정한다
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("x")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.putObjectTagging(
				p -> p.bucket(bucketName).key(key).tagging(t -> t.tagSet(s -> s.key("a").value("b")))));
		// WRITE_ACP 가 있으므로 ACL 은 바꿀 수 있다
		altClient.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE));
	}

	@Test
	@Tag("Copy")
	public void testObjectAclNotCopied() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 52);
		client.putObject(p -> p.bucket(bucketName).key("source").acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("content"));
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key("source")).grants()));

		// 복사본은 원본의 ACL 을 물려받지 않는다
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey("source").destinationBucket(bucketName)
				.destinationKey("copy"));
		assertEquals(List.of("main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key("copy")).grants()));
		// 복사 요청에 준 ACL 이 적용된다
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey("source").destinationBucket(bucketName)
				.destinationKey("canned").acl(ObjectCannedACL.AUTHENTICATED_READ));
		assertEquals(List.of(MainData.AUTHENTICATED_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key("canned")).grants()));
		// grant 헤더만 주면 소유자 전권은 붙지 않는다
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey("source").destinationBucket(bucketName)
				.destinationKey("grant").grantRead("id=\"" + config.altUser.id + "\""));
		assertEquals(List.of("alt READ"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key("grant")).grants()));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectAclResetOnOverwrite() {
		var key = "testObjectAclResetOnOverwrite";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 53);
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("content"));
		var publicClient = getPublicClient();
		assertEquals("content", getBody(publicClient.getObject(g -> g.bucket(bucketName).key(key))));

		// ACL 없이 덮어쓰면 비공개로 돌아간다
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		assertEquals(List.of("main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants()));
		assertDenied(() -> getPublicClient().getObject(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Multipart")
	public void testObjectAclMultipart() {
		var key = "testObjectAclMultipart";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 54);
		var uploadId = client
				.createMultipartUpload(c -> c.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ))
				.uploadId();
		var eTag = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString("content")).eTag();
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadId)
				.multipartUpload(m -> m.parts(p -> p.eTag(eTag).partNumber(1))));
		// 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트의 ACL 이 된다
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants()));
		var publicClient = getPublicClient();
		assertEquals("content", getBody(publicClient.getObject(g -> g.bucket(bucketName).key(key))));
		publicClient.headObject(h -> h.bucket(bucketName).key(key));
		// public-read 는 ACL 조회와 태그 조회를 열지 않는다
		assertDenied(() -> publicClient.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> publicClient.getObjectTagging(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Version")
	public void testObjectAclPerVersion() {
		var key = "testObjectAclPerVersion";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 55);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var version1 = client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("1")).versionId();
		var version2 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("2"))
				.versionId();

		// ACL 은 버전마다 따로다
		assertEquals(List.of(MainData.ALL_USERS + " READ", "main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(version1)).grants()));
		assertEquals(List.of("main FULL_CONTROL"),
				grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(version2)).grants()));
		var publicClient = getPublicClient();
		assertEquals("1",
				getBody(publicClient.getObject(g -> g.bucket(bucketName).key(key).versionId(version1))));
		assertDenied(() -> publicClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> publicClient.getObject(g -> g.bucket(bucketName).key(key).versionId(version2)));
	}

	@Test
	@Tag("Canned")
	public void testAclCannedBucketOwner() {
		var key = "testAclCannedBucketOwner";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 56);
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("content"));
		// 버킷 소유자가 곧 오브젝트 소유자면 bucket-owner-* 는 소유자 전권만 남긴다
		for (var acl : List.of("bucket-owner-read", "bucket-owner-full-control")) {
			client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(acl));
			assertEquals(List.of("main FULL_CONTROL"),
					grantSet(client.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants()));
		}
		// 버킷에 주면 무시되고 비공개가 된다
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));
		for (var acl : List.of("bucket-owner-read", "bucket-owner-full-control")) {
			client.putBucketAcl(p -> p.bucket(bucketName).acl(acl));
			assertEquals(List.of("main FULL_CONTROL"),
					grantSet(client.getBucketAcl(g -> g.bucket(bucketName)).grants()));
		}
	}

	@Test
	@Tag("Scope")
	public void testPublicRwBucketAnonymousScope() {
		var key = "testPublicRwBucketAnonymousScope";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 57, BucketCannedACL.PUBLIC_READ_WRITE);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		client.createMultipartUpload(c -> c.bucket(bucketName).key("multi"));

		var publicClient = getPublicClient();
		assertEquals(1, publicClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		assertEquals(1, publicClient.listObjectVersions(l -> l.bucket(bucketName)).versions().size());
		assertEquals(1, publicClient.listMultipartUploads(l -> l.bucket(bucketName)).uploads().size());
		publicClient.putObject(p -> p.bucket(bucketName).key("anonymous"), RequestBody.fromString("x"));
		assertEquals("x", getBody(publicClient.getObject(g -> g.bucket(bucketName).key("anonymous"))));

		// 버킷이 열려 있어도 비공개 오브젝트와 ACL 은 열리지 않는다
		assertDenied(() -> publicClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> publicClient.getBucketAcl(g -> g.bucket(bucketName)));
		assertDenied(() -> publicClient.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE)));
		// 익명 사용자는 멀티파트 업로드를 시작할 수 없다
		assertDenied(() -> publicClient.createMultipartUpload(c -> c.bucket(bucketName).key("new")));
	}

	// endregion
}
