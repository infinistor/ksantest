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
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Tag;

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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);

		response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPrivate() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.Private);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.AuthenticatedRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclDefault() {
		var key = "testObjectAclDefault";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclChange() {
		var key = "testObjectAclCanned";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

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
	public void testObjectAclPrivate() {
		var key = "testObjectAclCannedPrivate";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.Private));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRead() {
		var key = "testObjectAclCannedDuringCreate";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRW() {
		var key = "testObjectAclCannedPublicRW";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclAuthenticatedRead() {
		var key = "testObjectAclCannedAuthenticatedRead";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.AuthenticatedRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclBucketOwnerRead() {
		var key = "testObjectAclBucketOwnerRead";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.ObjectWriter,
				CannedAccessControlList.PublicReadWrite);

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
	public void testBucketObjectWriterObjectOwnerFullControl() {
		var key = "testBucketObjectWriterBucketOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.ObjectWriter,
				CannedAccessControlList.PublicReadWrite);

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

		var response = mainClient.getObjectAcl(bucketName, key);
		checkAcl(createAcl(config.altUser.toOwner(), config.mainUser.toGrantee(), Permission.FullControl), response);
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerEnforcedObjectOwnerFullControl() {
		var key = "testBucketOwnerEnforcedBucketOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.BucketOwnerPreferred,
				CannedAccessControlList.PublicReadWrite);

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

		var response = mainClient.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclOwnerNotChange() {
		var key = "testObjectAclOwnerNotChange";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.ObjectWriter,
				CannedAccessControlList.PublicReadWrite);

		mainClient.putObject(bucketName, key, key);

		var acl = createAltAcl(Permission.FullControl);
		mainClient.setObjectAcl(bucketName, key, acl);

		acl = createAltAcl(Permission.ReadAcp);
		altClient.setObjectAcl(bucketName, key, acl);

		var response = altClient.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Effect")
	public void testBucketAclChangeNotEffect() {
		var key = "testBucketAclChangeNotEffect";
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));

		var response = client.getObjectMetadata(bucketName, key);
		var contentType = response.getContentType();
		var eTag = response.getETag();

		var acl = createAltAcl(Permission.FullControl);

		client.setObjectAcl(bucketName, key, acl);

		response = client.getObjectMetadata(bucketName, key);
		assertEquals(contentType, response.getContentType());
		assertEquals(eTag, response.getETag());
	}

	@Test
	@Tag("Permission")
	public void testBucketAclDuplicated() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.Private);
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionFullControl() {
		checkBucketAcl(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWrite() {
		checkBucketAcl(Permission.Write);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWriteAcp() {
		checkBucketAcl(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionRead() {
		checkBucketAcl(Permission.Read);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionReadAcp() {
		checkBucketAcl(Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionFullControl() {
		checkObjectAcl(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWrite() {
		checkObjectAcl(Permission.Write);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWriteAcp() {
		checkObjectAcl(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionRead() {
		checkObjectAcl(Permission.Read);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionReadAcp() {
		checkObjectAcl(Permission.ReadAcp);
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclGrantNonExistUser() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		var badUser = new CanonicalGrantee("Foo");
		var acl = addBucketUserGrant(bucketName, new Grant(badUser, Permission.FullControl));

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclNoGrants() {
		var key = "testBucketAclNoGrants";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(bucketName, key, key);
		var response = client.getBucketAcl(bucketName);
		var oldGrants = response.getGrantsAsList();
		var acl = new AccessControlList();
		acl.setOwner(response.getOwner());

		client.setBucketAcl(bucketName, acl);

		client.getObject(bucketName, key);

		client.putObject(bucketName, key, "A");

		var client2 = getClient();
		client2.getBucketAcl(bucketName);
		client2.setBucketAcl(bucketName, CannedAccessControlList.Private);

		for (var myGrant : oldGrants)
			acl.grantAllPermissions(myGrant);
		client2.setBucketAcl(bucketName, acl);
	}

	@Test
	@Tag("Grant")
	public void testBucketAclMultiGrants() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var acl = createAcl(config.mainUser.toOwner(), config.altUser.toGrantee(), Permission.values());

		client.setBucketAcl(bucketName, acl);

		var response = client.getBucketAcl(bucketName);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Grant")
	public void testObjectAclMultiGrants() {
		var key = "testObjectAclMultiGrants";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var acl = createAcl(config.mainUser.toOwner(), config.altUser.toGrantee(), Permission.values());

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));
		client.setObjectAcl(bucketName, key, acl);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Delete")
	public void testBucketAclRevokeAll() {
		var key = "testBucketAclRevokeAll";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(bucketName, key, key);
		var response = client.getBucketAcl(bucketName);

		var acl1 = new AccessControlList().withOwner(new Owner());
		for (var Item : response.getGrantsAsList())
			acl1.grantAllPermissions(Item);

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl1));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_ACL_ERROR, e.getErrorCode());

		var acl2 = new AccessControlList();
		acl2.setOwner(response.getOwner());

		client.setBucketAcl(bucketName, acl2);

		var acl3 = new AccessControlList();
		acl3.setOwner(new Owner());

		e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl3));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_ACL_ERROR, e.getErrorCode());
	}

	@Test
	@Tag("Delete")
	public void testObjectAclRevokeAll() {
		var key = "testObjectAclRevokeAll";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);

		var acl1 = new AccessControlList();
		acl1.setOwner(new Owner());
		for (var Item : response.getGrantsAsList())
			acl1.grantAllPermissions(Item);

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl1));
		assertEquals(MainData.MALFORMED_ACL_ERROR, e.getErrorCode());

		var acl2 = new AccessControlList();
		acl2.setOwner(response.getOwner());

		client.setObjectAcl(bucketName, key, acl2);

		var acl3 = new AccessControlList();
		acl3.setOwner(new Owner());

		e = assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl3));
		assertEquals(MainData.MALFORMED_ACL_ERROR, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketAclRevokeAllId() {
		var key = "testBucketAclRevokeAllId";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		client.putObject(bucketName, key, key);

		var response = client.getBucketAcl(bucketName);

		var acl1 = new AccessControlList();
		acl1.setOwner(response.getOwner());

		var mainUser = config.mainUser;
		mainUser.id = null;
		acl1.grantAllPermissions(mainUser.toGrant(Permission.FullControl));

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, acl1));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}
}
