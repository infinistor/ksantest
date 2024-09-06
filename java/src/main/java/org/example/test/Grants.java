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
	// 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
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
	// [bucket:public-read] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.PublicReadWrite);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Bucket")
	// [bucket:authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.AuthenticatedRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	// 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
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
	// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
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
	// [ObjectWriter][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지
	// 확인
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
	// [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가
	// 올바른지 확인
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
	// [object: public-read-write => alt-user-full-control => alt-user-read-acl]
	// 권한을 변경해도 소유주가 변경되지 않는지 확인
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
	// 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
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
	// [bucket:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	public void testBucketAclDuplicated() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, CannedAccessControlList.Private);
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	public void testObjectPermissionFullControl() {
		checkObjectACL(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	public void testObjectPermissionWrite() {
		checkObjectACL(Permission.Write);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	public void testObjectPermissionWriteAcp() {
		checkObjectACL(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	public void testObjectPermissionRead() {
		checkObjectACL(Permission.Read);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	public void testObjectPermissionReadAcp() {
		checkObjectACL(Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
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
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
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
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
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
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
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
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
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
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
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
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
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
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
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
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
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
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
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
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	public void testObjectAclRevokeAll() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);

		// 소유주가 존재하지 않을 경우 실패
		var acl1 = new AccessControlList();
		acl1.setOwner(new Owner());
		for (var Item : response.getGrantsAsList())
			acl1.grantAllPermissions(Item);

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl1));

		// 소유주만 존재할 경우 성공
		var acl2 = new AccessControlList();
		acl2.setOwner(response.getOwner());

		client.setObjectAcl(bucketName, key, acl2);

		// 아무것도 없을 경우 실패
		var acl3 = new AccessControlList();
		acl3.setOwner(new Owner());

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, key, acl3));
	}

	@Test
	@Tag("Access")
	// [bucket:private, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:private, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:private, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:private] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:public-read] Acl 설정이 올바르게 동작하는지 확인
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
	// [bucket:public-read-write, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
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
