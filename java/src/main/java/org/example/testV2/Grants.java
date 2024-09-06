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
	// 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
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
	// [bucket:public-read] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createPublicAcl(Permission.READ, Permission.WRITE), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.AUTHENTICATED_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(createAuthenticatedAcl(Permission.READ), response);
	}

	@Test
	@Tag("Object")
	// 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
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
	// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가
	// 올바른지 확인
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
	// [object: public-read-write => alt-user-full-control => alt-user-read-acl]
	// 권한을 변경해도 소유주가 변경되지 않는지 확인
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
	// 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
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
	// [bucket:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	public void testBucketAclDuplicated() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PRIVATE);
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	public void testObjectPermissionFullControl() {
		checkObjectACL(Permission.FULL_CONTROL);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	public void testObjectPermissionWrite() {
		checkObjectACL(Permission.WRITE);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	public void testObjectPermissionWriteAcp() {
		checkObjectACL(Permission.WRITE_ACP);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	public void testObjectPermissionRead() {
		checkObjectACL(Permission.READ);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	public void testObjectPermissionReadAcp() {
		checkObjectACL(Permission.READ_ACP);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	public void testBucketPermissionAltUserFullControl() {
		var bucketName = bucketACLGrantUserId(Permission.FULL_CONTROL);
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
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	public void testBucketPermissionAltUserRead() {
		var bucketName = bucketACLGrantUserId(Permission.READ);
		var altClient = getAltClient();

		checkBucketAclAllowRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	public void testBucketPermissionAltUserReadAcp() {
		var bucketName = bucketACLGrantUserId(Permission.READ_ACP);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclAllowReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	public void testBucketPermissionAltUserWrite() {
		var bucketName = bucketACLGrantUserId(Permission.WRITE);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclAllowWrite(altClient, bucketName);
		checkBucketAclDenyWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	public void testBucketPermissionAltUserWriteAcp() {
		var bucketName = bucketACLGrantUserId(Permission.WRITE_ACP);
		var altClient = getAltClient();

		checkBucketAclDenyRead(altClient, bucketName);
		checkBucketAclDenyReadACP(altClient, bucketName);
		checkBucketAclDenyWrite(altClient, bucketName);
		checkBucketAclAllowWriteACP(altClient, bucketName);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	public void testBucketAclGrantNonExistUser() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var badUser = Grantee.builder().id("Foo").type(software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER)
				.build();
		var acl = addBucketUserGrant(bucketName,
				Grant.builder().grantee(badUser).permission(Permission.FULL_CONTROL).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
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
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
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
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
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
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
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
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
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
	// [bucket:private, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:private, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:private, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
