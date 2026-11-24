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
import java.util.Collections;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Type;

public class Grants extends TestBase {
	@Test
	@Tag("Bucket")
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclChanged() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 2, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ), response);

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));

		response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPrivate() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 3, BucketCannedACL.PRIVATE);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 4, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 5, BucketCannedACL.PUBLIC_READ_WRITE);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ, Permission.WRITE), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 6, BucketCannedACL.AUTHENTICATED_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createAuthenticatedAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclDefault() {
		var key = "testObjectAclDefault";
		var client = getClient();
		var bucketName = createBucket(client, 7);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclChange() {
		var key = "testObjectAclChange";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 8);

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
	public void testObjectAclPrivate() {
		var key = "testObjectAclPrivate";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 9);

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRead() {
		var key = "testObjectAclPublicRead";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 10);

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRW() {
		var key = "testObjectAclPublicRW";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 11);

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ_WRITE),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(Permission.READ, Permission.WRITE), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclAuthenticatedRead() {
		var key = "testObjectAclAuthenticatedRead";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 12);

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.AUTHENTICATED_READ),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createAuthenticatedAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclBucketOwnerRead() {
		var key = "testObjectAclBucketOwnerRead";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(mainClient, 13, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_READ),
				RequestBody.fromString(key));
		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));

		checkAcl(AccessControlPolicy.builder().grants(List.of(
				config.altUser.toGrantV2(Permission.FULL_CONTROL),
				config.mainUser.toGrantV2(Permission.READ))).owner(config.altUser.toOwnerV2()).build(), response);
	}

	@Test
	@Tag("Object")
	public void testBucketObjectWriterObjectOwnerFullControl() {
		var key = "testBucketObjectWriterObjectOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, 14, ObjectOwnership.OBJECT_WRITER, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));

		var response = mainClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createAcl(config.altUser.toOwnerV2(), config.mainUser.toGranteeV2(), Permission.FULL_CONTROL),
				response);
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerEnforcedObjectOwnerFullControl() {
		var key = "testBucketOwnerEnforcedObjectOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, 15, ObjectOwnership.BUCKET_OWNER_PREFERRED, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));

		var response = mainClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclOwnerNotChange() {
		var key = "testObjectAclOwnerNotChange";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(mainClient, 16, BucketCannedACL.PUBLIC_READ_WRITE);

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
		var key = "testBucketAclChangeNotEffect";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 17, BucketCannedACL.PUBLIC_READ_WRITE);

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
		var bucketName = createBucketCannedAcl(client, 18, BucketCannedACL.PRIVATE);
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionFullControl() {
		checkBucketAcl(19, Permission.FULL_CONTROL);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWrite() {
		checkBucketAcl(20, Permission.WRITE);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWriteAcp() {
		checkBucketAcl(21, Permission.WRITE_ACP);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionRead() {
		checkBucketAcl(22, Permission.READ);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionReadAcp() {
		checkBucketAcl(23, Permission.READ_ACP);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionFullControl() {
		checkObjectAcl(24, Permission.FULL_CONTROL);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWrite() {
		checkObjectAcl(25, Permission.WRITE);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWriteAcp() {
		checkObjectAcl(26, Permission.WRITE_ACP);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionRead() {
		checkObjectAcl(27, Permission.READ);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionReadAcp() {
		checkObjectAcl(28, Permission.READ_ACP);
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclGrantNonExistUser() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 29);

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
		var key = "testBucketAclNoGrants";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 30);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var oldGrants = response.grants();
		var acl = AccessControlPolicy.builder().owner(response.owner()).grants(List.of());

		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl.build()));

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("A"));

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
		var bucketName = createBucketCannedAcl(client, 31);
		var acl = createAcl(config.mainUser.toOwnerV2(), config.altUser.toGranteeV2(), Permission.READ,
				Permission.WRITE, Permission.READ_ACP, Permission.WRITE_ACP, Permission.FULL_CONTROL);

		client.putBucketAcl(c -> c.bucket(bucketName).accessControlPolicy(acl).build());

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(acl, response);
	}

	@Test
	@Tag("Grant")
	public void testObjectAclMultiGrants() {
		var key = "testObjectAclMultiGrants";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 32);
		var acl = createAcl(config.mainUser.toOwnerV2(), config.altUser.toGranteeV2(), Permission.READ,
				Permission.WRITE, Permission.READ_ACP, Permission.WRITE_ACP, Permission.FULL_CONTROL);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.putObjectAcl(c -> c.bucket(bucketName).key(key).accessControlPolicy(acl));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(acl, response);
	}

	@Test
	@Tag("Error")
	public void testBucketAclRevokeAllId() {
		var key = "testBucketAclRevokeAllId";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 35);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		var mainUser = config.mainUser;
		mainUser.id = null;
		var acl = AccessControlPolicy.builder().owner(response.owner())
				.grants(mainUser.toGrantV2(Permission.FULL_CONTROL));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl.build())));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_ACL_ERROR, e.awsErrorDetails().errorCode());
	}

	// region ACL API 검증
	static final String LOG_DELIVERY_GROUP = "http://acs.amazonaws.com/groups/s3/LogDelivery";

	/** 버킷 소유자를 Owner 로 하는 AccessControlPolicy. grants 가 없으면 소유자 FULL_CONTROL. */
	static AccessControlPolicy ownerPolicy(S3Client client, String bucketName, Grant... grants) {
		var ownerId = client.getBucketAcl(g -> g.bucket(bucketName)).owner().id();
		var list = grants.length > 0 ? List.of(grants) : List.of(ownerGrant(ownerId, Permission.FULL_CONTROL));
		return AccessControlPolicy.builder().owner(o -> o.id(ownerId)).grants(list).build();
	}

	static Grant ownerGrant(String ownerId, Permission permission) {
		return Grant.builder().grantee(g -> g.type(Type.CANONICAL_USER).id(ownerId)).permission(permission).build();
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclInvalidCanned() {
		var key = "testBucketAclInvalidCanned";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 36);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));

		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl("invalid")));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl("invalid")));
	}

	@Test
	@Tag("ERROR")
	public void testAclCannedWithGrantHeader() {
		var key = "testAclCannedWithGrantHeader";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 37);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var grant = "uri=\"" + MainData.AUTHENTICATED_USERS + "\"";

		// 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE).grantRead(grant)));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST, () -> client
				.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE).grantRead(grant)));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> client.putObject(
						p -> p.bucket(bucketName).key(key + "2").acl(ObjectCannedACL.PRIVATE).grantRead(grant),
						RequestBody.fromString("content")));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclCannedWithBody() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 38);
		var policy = ownerPolicy(client, bucketName);

		// 캔드 ACL 과 본문(AccessControlPolicy)은 함께 쓸 수 없다
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.UNEXPECTED_CONTENT, () -> client
				.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE).accessControlPolicy(policy)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclMissing() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 39);

		// 캔드 ACL, 권한 부여 헤더, 본문 중 아무것도 없다
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MISSING_SECURITY_HEADER,
				() -> client.putBucketAcl(p -> p.bucket(bucketName)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclInvalidPermission() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 40);
		var ownerId = client.getBucketAcl(g -> g.bucket(bucketName)).owner().id();
		var grant = Grant.builder().grantee(g -> g.type(Type.CANONICAL_USER).id(ownerId)).permission("INVALID")
				.build();
		var policy = ownerPolicy(client, bucketName, grant);

		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_ACL_ERROR,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(policy)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclChangeOwner() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 41);
		var policy = ownerPolicy(client, bucketName).toBuilder().owner(o -> o.id(config.altUser.id)).build();

		// ACL 로 소유자를 바꿀 수 없다
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(policy)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclInvalidGrantHeader() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 42);

		// 형식 오류, 없는 사용자 ID, 없는 그룹 URI
		for (var grant : List.of("invalid", "id=\"invalid\"", "uri=\"http://example.com/invalid\""))
			assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
					() -> client.putBucketAcl(p -> p.bucket(bucketName).grantRead(grant)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclInvalidGroupUri() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 43);
		var grant = Grant.builder().grantee(g -> g.type(Type.GROUP).uri("http://example.com/invalid"))
				.permission(Permission.READ).build();
		var policy = ownerPolicy(client, bucketName, grant);

		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(policy)));
	}

	@Test
	@Tag("Max")
	public void testBucketAclMaxGrants() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 44);
		var ownerId = client.getBucketAcl(g -> g.bucket(bucketName)).owner().id();
		var grant = ownerGrant(ownerId, Permission.FULL_CONTROL);

		// 권한 부여는 100개까지
		var maxPolicy = ownerPolicy(client, bucketName, Collections.nCopies(100, grant).toArray(new Grant[0]));
		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(maxPolicy));

		var overPolicy = ownerPolicy(client, bucketName, Collections.nCopies(101, grant).toArray(new Grant[0]));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_ACL_ERROR,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(overPolicy)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(45);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.getBucketAcl(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE)));
	}

	@Test
	@Tag("Access")
	public void testAclAccessDenied() {
		var key = "testAclAccessDenied";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client, 46);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));

		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.getBucketAcl(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE)));
	}

	@Test
	@Tag("ERROR")
	public void testObjectAclKeyNotExist() {
		var key = "testObjectAclKeyNotExist";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 47);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE)));
	}

	@Test
	@Tag("Version")
	public void testObjectAclVersionId() {
		var key = "testObjectAclVersionId";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 48);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));

		// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
		assertEquals(1,
				client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId("null")).grants().size());
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId("invalid")));
	}

	@Test
	@Tag("Version")
	public void testObjectAclDeleteMarker() {
		var key = "testObjectAclDeleteMarker";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 49);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"))
				.versionId();
		var markerId = client.deleteObject(d -> d.bucket(bucketName).key(key)).versionId();

		// 최신 버전이 삭제 마커이면 없는 키, 삭제 마커 버전을 지정하면 405
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(markerId)));
		assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED, () -> client.putObjectAcl(
				p -> p.bucket(bucketName).key(key).versionId(markerId).acl(ObjectCannedACL.PRIVATE)));
		assertEquals(1,
				client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(versionId)).grants().size());
	}

	@Test
	@Tag("Check")
	public void testPutObjectWithGrantHeader() {
		var key = "testPutObjectWithGrantHeader";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 51);
		var altId = config.altUser.id;

		// 권한 부여 헤더만 주면 소유자 FULL_CONTROL 없이 지정한 권한만 남는다
		client.putObject(p -> p.bucket(bucketName).key(key).grantRead("id=\"" + altId + "\""),
				RequestBody.fromString("content"));
		var grants = client.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants();
		assertEquals(1, grants.size());
		assertEquals(altId, grants.get(0).grantee().id());
		assertEquals(Permission.READ, grants.get(0).permission());
		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));
	}
	// endregion
}
