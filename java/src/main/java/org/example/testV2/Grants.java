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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.Permission;

public class Grants extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Grants V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Grants V2 End");
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclChanged() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ), response);

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));

		response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ, Permission.WRITE), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.AUTHENTICATED_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createAuthenticatedAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testObjectAclDefault";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedDuringCreate";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclChange() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var key = "testObjectAclCanned";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(Permission.READ), response);

		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE));

		response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedPublicRW";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ_WRITE),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(Permission.READ, Permission.WRITE), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclCannedAuthenticatedRead";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.AUTHENTICATED_READ),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createAuthenticatedAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclBucketOwnerRead() {
		var key = "foo";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_READ),
				RequestBody.fromString(key));
		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));

		checkAcl(AccessControlPolicy.builder().grants(List.of(
				config.altUser.toGrantV2(Permission.FULL_CONTROL),
				config.mainUser.toGrantV2(Permission.READ))).owner(config.altUser.toOwnerV2()).build(), response);
	}

	@Test
	@Tag("Object")
	public void testBucketObjectWriterBucketOwnerFullControl() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.OBJECT_WRITER,
				BucketCannedACL.PUBLIC_READ_WRITE);
		var key = "testBucketObjectWriterBucketOwnerFullControl";

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));

		var response = mainClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createAcl(config.altUser.toOwnerV2(), config.mainUser.toGranteeV2(), Permission.FULL_CONTROL),
				response);
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerEnforcedBucketOwnerFullControl() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, ObjectOwnership.BUCKET_OWNER_PREFERRED,
				BucketCannedACL.PUBLIC_READ_WRITE);
		var key = "testBucketOwnerEnforcedBucketOwnerFullControl";

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));

		var response = mainClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclOwnerNotChange() {
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, BucketCannedACL.PUBLIC_READ_WRITE);
		var key = "testObjectAclOwnerNotChange";

		mainClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var acl1 = createAltAcl(Permission.FULL_CONTROL);
		mainClient.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl1));

		var acl2 = createAltAcl(Permission.READ_ACP);
		altClient.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl2).build());

		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(acl2, response);
	}

	@Test
	@Tag("Effect")
	public void testBucketAclChangeNotEffect() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ_WRITE);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.headObject(g -> g.bucket(bucketName).key(key));
		var contentType = response.contentType();
		var eTag = response.eTag();

		var acl = createAltAcl(Permission.FULL_CONTROL);
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl));

		response = client.headObject(g -> g.bucket(bucketName).key(key));
		assertEquals(contentType, response.contentType());
		assertEquals(eTag, response.eTag());
	}

	@Test
	@Tag("Permission")
	public void testBucketAclDuplicated() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PRIVATE);
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionFullControl() {
		checkObjectACL(Permission.FULL_CONTROL);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWrite() {
		checkObjectACL(Permission.WRITE);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWriteAcp() {
		checkObjectACL(Permission.WRITE_ACP);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionRead() {
		checkObjectACL(Permission.READ);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionReadAcp() {
		checkObjectACL(Permission.READ_ACP);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = setupBucketPermission(Permission.FULL_CONTROL);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);

		var client = getClient();

		var bucketACLResponse = client.getBucketAcl(g -> g.bucket(bucketName));
		var ownerId = bucketACLResponse.owner().id();

		var mainUserId = config.mainUser.id;

		assertEquals(mainUserId, ownerId);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserRead() {
		var bucketName = setupBucketPermission(Permission.READ);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = setupBucketPermission(Permission.READ_ACP);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWrite() {
		var bucketName = setupBucketPermission(Permission.WRITE);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = setupBucketPermission(Permission.WRITE_ACP);
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

		var badUser = Grantee.builder().id("Foo").type(software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER)
				.build();
		var acl = addBucketUserGrant(bucketName,
				Grant.builder().grantee(badUser).permission(Permission.FULL_CONTROL).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclNoGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var oldGrants = response.grants();
		var acl = AccessControlPolicy.builder().owner(response.owner()).grants(List.of());

		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl.build()));

		assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("A")));

		var client2 = getClient();
		client2.getBucketAcl(g -> g.bucket(bucketName));
		client2.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));

		acl.grants(oldGrants);
		client2.putBucketAcl(
				p -> p.bucket(bucketName).accessControlPolicy(acl.build()).build());
	}

	@Test
	@Tag("Grant")
	public void testBucketAclMultiGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var acl = createAllAcl();

		client.putBucketAcl(c -> c.bucket(bucketName).accessControlPolicy(acl).build());

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(acl, response);
	}

	@Test
	@Tag("Grant")
	public void testObjectAclMultiGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectAclMultiGrants";
		var acl = createAllAcl();

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.putObjectAcl(c -> c.bucket(bucketName).key(key).accessControlPolicy(acl));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(acl, response);
	}

	@Test
	@Tag("Delete")
	public void testBucketAclRevokeAll() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testBucketAclRevokeAll";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName)
						.accessControlPolicy(a -> a.owner(o -> o.build()).grants(response.grants()))));
	}

	@Test
	@Tag("Access")
	public void testObjectAclRevokeAll() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		assertThrows(AwsServiceException.class, () -> client.putObjectAcl(
				p -> p.bucket(bucketName).key(key)
						.accessControlPolicy(a -> a.owner(o -> o.build()).grants(response.grants()))));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPrivate() {
		var key1 = "testAccessBucketPrivateObjectPrivate";
		var key2 = "testAccessBucketPrivateObjectPrivate2";
		var newKey = "testAccessBucketPrivateObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PRIVATE);

		var altClient = getAltClient();

		assertThrows(AwsServiceException.class, () -> altClient.getObject(g -> g.bucket(bucketName).key(key1)));
		assertThrows(AwsServiceException.class, () -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class, () -> altClient.listObjects(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class, () -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPublicRead() {
		var key1 = "testAccessBucketPrivateObjectPublicRead";
		var key2 = "testAccessBucketPrivateObjectPublicRead2";
		var newKey = "testAccessBucketPrivateObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ);
		var altClient = getAltClient();
		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));

		var body = getBody(response);
		assertEquals(key1, body);

		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class, () -> altClient2.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class, () -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
				RequestBody.fromString("alt-overwrite")));

		var altClient3 = getAltClient();
		assertThrows(AwsServiceException.class, () -> altClient3.listObjects(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class, () -> altClient3.listObjectsV2(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPrivateObjectPublicRW() {
		var key1 = "testAccessBucketPrivateObjectPublicRW";
		var key2 = "testAccessBucketPrivateObjectPublicRW2";
		var newKey = "testAccessBucketPrivateObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ_WRITE);
		var altClient = getAltClient();
		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));

		var body = getBody(response);
		assertEquals(key1, body);

		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient2.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var altClient3 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient3.listObjects(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class,
				() -> altClient3.listObjectsV2(l -> l.bucket(bucketName)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPrivate() {
		var key1 = "testAccessBucketPublicReadObjectPrivate";
		var key2 = "testAccessBucketPublicReadObjectPrivate2";
		var newKey = "testAccessBucketPublicReadObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PRIVATE);
		var altClient = getAltClient();

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key1)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient2.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(altClient3.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPublicRead() {
		var key1 = "testAccessBucketPublicReadObjectPublicRead";
		var key2 = "testAccessBucketPublicReadObjectPublicRead1";
		var newKey = "testAccessBucketPublicReadObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ,
				ObjectCannedACL.PUBLIC_READ);
		var altClient = getAltClient();

		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));
		var body = getBody(response);
		assertEquals(key1, body);

		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient2.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var altClient3 = getAltClient();
		var objList = getKeys(
				altClient3.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(
				altClient3.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicReadObjectPublicRW() {
		var key1 = "testAccessBucketPublicReadObjectPublicRW";
		var key2 = "testAccessBucketPublicReadObjectPublicRW2";
		var newKey = "testAccessBucketPublicReadObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ,
				ObjectCannedACL.PUBLIC_READ_WRITE);
		var altClient = getAltClient();

		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));
		var body = getBody(response);
		assertEquals(key1, body);

		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		var altClient2 = getAltClient();
		assertThrows(AwsServiceException.class,
				() -> altClient2.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var altClient3 = getAltClient();
		var objList = getKeys(
				altClient3.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(
				altClient3.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPrivate() {
		var key1 = "testAccessBucketPublicRWObjectPrivate";
		var key2 = "testAccessBucketPublicRWObjectPrivate2";
		var newKey = "testAccessBucketPublicRWObjectPrivateNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ_WRITE,
				ObjectCannedACL.PRIVATE);
		var altClient = getAltClient();

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key1)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var objList = getKeys(
				altClient.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		objList = getKeys(
				altClient.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objList);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}

	@Test
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPublicRead() {
		var key1 = "testAccessBucketPublicRWObjectPublicRead";
		var key2 = "testAccessBucketPublicRWObjectPublicRead2";
		var newKey = "testAccessBucketPublicRWObjectPublicReadNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ_WRITE,
				ObjectCannedACL.PUBLIC_READ);
		var altClient = getAltClient();

		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));
		var body = getBody(response);
		assertEquals(key1, body);
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var objects = getKeys(
				altClient.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objects);
		objects = getKeys(
				altClient.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objects);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}

	@Test
	@Disabled
	@Tag("Access")
	public void testAccessBucketPublicRWObjectPublicRW() {
		var key1 = "testAccessBucketPublicRWObjectPublicRW";
		var key2 = "testAccessBucketPublicRWObjectPublicRW2";
		var newKey = "testAccessBucketPublicRWObjectPublicRWNew";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PUBLIC_READ_WRITE,
				ObjectCannedACL.PUBLIC_READ_WRITE);
		var altClient = getAltClient();

		var response = altClient.getObject(g -> g.bucket(bucketName).key(key1));
		var body = getBody(response);
		assertEquals(key1, body);
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("overwrite")));

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key2),
						RequestBody.fromString("alt-overwrite")));

		var objects = getKeys(
				altClient.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objects);
		objects = getKeys(
				altClient.listObjectsV2(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key1, key2), objects);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}
}
