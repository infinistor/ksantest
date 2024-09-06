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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.example.Data.MainData;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

public class Grants extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Grants Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Grants End");
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclChanged() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);

		response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicReadWrite);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.AuthenticatedRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclDefault() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclDefault";

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclChange() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCanned";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read), response);

		client.setObjectAcl(bucketName, key, CannedAccessControlList.Private);

		response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedDuringCreate";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedPublicRW";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedAuthenticatedRead";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.AuthenticatedRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclBucketOwnerRead() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, CannedAccessControlList.PublicReadWrite);
		var key = "testObjectAclCannedBucketOwnerRead";

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerRead));
		var response = altClient.getObjectAcl(bucketName, key);

		var acl = new AccessControlList().withOwner(config.altUser.toOwner());
		acl.grantAllPermissions(config.altUser.toGrant(Permission.FullControl),
				config.mainUser.toGrant(Permission.Read));
		checkAcl(acl, response);
	}

	@Test
	@Tag("Object")
	public void testBucketObjectWriterBucketOwnerFullControl() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.ObjectWriter,
				CannedAccessControlList.PublicReadWrite);
		var key = "testBucketObjectWriterBucketOwnerFullControl";

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

		var response = mainClient.getObjectAcl(bucketName, key);
		checkAcl(createAcl(config.altUser.toOwner(), config.mainUser.toGrantee(), Permission.FullControl), response);
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerEnforcedBucketOwnerFullControl() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.BucketOwnerPreferred,
				CannedAccessControlList.PublicReadWrite);
		var key = "testBucketOwnerEnforcedBucketOwnerFullControl";

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

		var response = mainClient.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclOwnerNotChange() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, CannedAccessControlList.PublicReadWrite);
		var key = "testObjectAclOwnerNotChange";

		mainClient.putObject(bucketName, key, key);

		var acl = createAltACL(Permission.FullControl);
		mainClient.setObjectAcl(bucketName, key, acl);

		acl = createAltACL(Permission.ReadAcp);
		altClient.setObjectAcl(bucketName, key, acl);

		var response = altClient.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Effect")
	public void testBucketAclChangeNotEffect() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicReadWrite);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));

		var response = client.getObjectMetadata(bucketName, key);
		var contentType = response.getContentType();
		var eTag = response.getETag();

		var acl = createAltACL(Permission.FullControl);

		client.setObjectAcl(bucketName, key, acl);

		response = client.getObjectMetadata(bucketName, key);
		assertEquals(contentType, response.getContentType());
		assertEquals(eTag, response.getETag());
	}

	@Test
	@Tag("Permission")
	public void testBucketAclDuplicated() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.Private);
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionFullControl() {
		checkObjectACL(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWrite() {
		checkObjectACL(Permission.Write);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWriteAcp() {
		checkObjectACL(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionRead() {
		checkObjectACL(Permission.Read);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionReadAcp() {
		checkObjectACL(Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = bucketACLGrantUserId(Permission.FullControl);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);

		var client = getClient();

		var bucketACLResponse = client.getBucketAcl(bucketName);
		var ownerId = bucketACLResponse.getOwner().getId();

		assertEquals(config.mainUser.id, ownerId);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserRead() {
		var bucketName = bucketACLGrantUserId(Permission.Read);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = bucketACLGrantUserId(Permission.ReadAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWrite() {
		var bucketName = bucketACLGrantUserId(Permission.Write);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = bucketACLGrantUserId(Permission.WriteAcp);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclGrantNonExistUser() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var badUser = new CanonicalGrantee("Foo");
		var acl = addBucketUserGrant(bucketName, new Grant(badUser, Permission.FullControl));

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.INVALID_ARGUMENT, errorCode);
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclNoGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "foo";

		client.putObject(bucketName, key, key);
		var response = client.getBucketAcl(bucketName);
		var oldGrants = response.getGrantsAsList();
		var acl = new AccessControlList();
		acl.setOwner(response.getOwner());

		client.setBucketAcl(bucketName, acl);

		client.getObject(bucketName, key);

		assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, "A"));

		var client2 = getClient();
		client2.getBucketAcl(bucketName);
		client2.setBucketAcl(bucketName, CannedAccessControlList.Private);

		for (var MyGrant : oldGrants)
			acl.grantAllPermissions(MyGrant);
		client2.setBucketAcl(bucketName, acl);
	}

	@Test
	@Tag("Grant")
	public void testBucketAclMultiGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var acl = createAllAcl();

		client.setBucketAcl(bucketName, acl);

		var response = client.getBucketAcl(bucketName);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Grant")
	public void testObjectAclMultiGrants() {
		var key = "fooKey";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var acl = createAllAcl();

		client.putObject(
				new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
						.withAccessControlList(acl));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Delete")
	public void testBucketAclRevokeAll() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testBucketAclRevokeAll";

		client.putObject(bucketName, key, key);
		var response = client.getBucketAcl(bucketName);

		var acl1 = new AccessControlList();
		acl1.setOwner(new Owner());
		for (var Item : response.getGrantsAsList())
			acl1.grantAllPermissions(Item);

		assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl1));

		var acl2 = new AccessControlList();
		acl2.setOwner(response.getOwner());

		client.setBucketAcl(bucketName, acl2);

		var acl3 = new AccessControlList();
		acl3.setOwner(new Owner());

		assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl3));
	}

	@Test
	@Tag("Delete")
	public void testObjectAclRevokeAll() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);

		var acl1 = new AccessControlList();
		acl1.setOwner(new Owner());
		for (var Item : response.getGrantsAsList())
			acl1.grantAllPermissions(Item);

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl1));

		var acl2 = new AccessControlList();
		acl2.setOwner(response.getOwner());

		client.setObjectAcl(bucketName, key, acl2);

		var acl3 = new AccessControlList();
		acl3.setOwner(new Owner());

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl3));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPrivate() {
		var key1 = "testAccessBucketPrivateObjectPrivate";
		var key2 = "testAccessBucketPrivateObjectPrivate2";
		var newKey = "testAccessBucketPrivateObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, key2));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPublicRead() {
		var key1 = "testAccessBucketPrivateObjectPublicRead";
		var key2 = "testAccessBucketPrivateObjectPublicRead2";
		var newKey = "testAccessBucketPrivateObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPublicRW() {
		var key1 = "testAccessBucketPrivateObjectPublicRW";
		var key2 = "testAccessBucketPrivateObjectPublicRW2";
		var newKey = "testAccessBucketPrivateObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPrivate() {
		var key1 = "testAccessBucketPublicReadObjectPrivate";
		var key2 = "testAccessBucketPublicReadObjectPrivate2";
		var newKey = "testAccessBucketPublicReadObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead, CannedAccessControlList.Private);
		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(altClient3.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPublicRead() {
		var key1 = "testAccessBucketPublicReadObjectPublicRead";
		var key2 = "testAccessBucketPublicReadObjectPublicRead2";
		var newKey = "testAccessBucketPublicReadObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(altClient3.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPublicRW() {
		var key1 = "testAccessBucketPublicReadObjectPublicRW";
		var key2 = "testAccessBucketPublicReadObjectPublicRW2";
		var newKey = "testAccessBucketPublicReadObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "overwrite2"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(altClient3.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPrivate() {
		var key1 = "testAccessBucketPublicRWObjectPrivate";
		var key2 = "testAccessBucketPublicRWObjectPrivate2";
		var newKey = "testAccessBucketPublicRWObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.Private);
		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "overwrite2"));

		var objList = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(altClient.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objList);
		altClient.putObject(bucketName, newKey, "new-content");
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPublicRead() {
		var key1 = "testAccessBucketPublicRWObjectPublicRead";
		var key2 = "testAccessBucketPublicRWObjectPublicRead2";
		var newKey = "testAccessBucketPublicRWObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "overwrite2"));

		var objects = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objects);
		objects = getKeys(altClient.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objects);
		altClient.putObject(bucketName, newKey, "new-content");
	}

	@Test
	@Disabled("S3에서는 버킷의 권한이 public-read-write이더라도 오브젝트의 권한이 public-read로 설정되어있으면 업로드 불가능")
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPublicRW() {
		var key1 = "testAccessBucketPublicRWObjectPublicRW";
		var key2 = "testAccessBucketPublicRWObjectPublicRW2";
		var newKey = "testAccessBucketPublicRWObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "overwrite1"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "overwrite2"));

		var objects = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objects);
		objects = getKeys(altClient.listObjectsV2(bucketName).getObjectSummaries());
		assertEquals(List.of(key1, key2), objects);
		altClient.putObject(bucketName, newKey, "new-content");
	}
}
