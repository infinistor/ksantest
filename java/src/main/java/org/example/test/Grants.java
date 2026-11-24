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
import java.util.List;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;

public class Grants extends TestBase {
	@Test
	@Tag("Bucket")
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclChanged() {
		var client = getClient();
		var bucketName = createBucket(client, 2, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicRead);

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
		var bucketName = createBucket(client, 3, ObjectOwnership.ObjectWriter, CannedAccessControlList.Private);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRead() {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclPublicRW() {
		var client = getClient();
		var bucketName = createBucket(client, 5, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Bucket")
	public void testBucketAclAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucket(client, 6, ObjectOwnership.ObjectWriter, CannedAccessControlList.AuthenticatedRead);

		var response = client.getBucketAcl(bucketName);
		checkAcl(createAuthenticatedAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclDefault() {
		var key = "testObjectAclDefault";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 7);

		client.putObject(bucketName, key, key);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclChange() {
		var key = "testObjectAclChange";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 8);

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
		var key = "testObjectAclPrivate";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 9);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.Private));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRead() {
		var key = "testObjectAclPublicRead";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 10);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclPublicRW() {
		var key = "testObjectAclPublicRW";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 11);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(createPublicAcl(Permission.Read, Permission.Write), response);
	}

	@Test
	@Tag("Object")
	public void testObjectAclAuthenticatedRead() {
		var key = "testObjectAclAuthenticatedRead";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 12);

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
		var bucketName = createBucket(mainClient, 13, ObjectOwnership.ObjectWriter,
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
		var key = "testBucketObjectWriterObjectOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, 14, ObjectOwnership.ObjectWriter,
				CannedAccessControlList.PublicReadWrite);

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

		var response = mainClient.getObjectAcl(bucketName, key);
		checkAcl(createAcl(config.altUser.toOwner(), config.mainUser.toGrantee(), Permission.FullControl), response);
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerEnforcedObjectOwnerFullControl() {
		var key = "testBucketOwnerEnforcedObjectOwnerFullControl";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(mainClient, 15, ObjectOwnership.BucketOwnerPreferred,
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
		var bucketName = createBucket(mainClient, 16, ObjectOwnership.ObjectWriter,
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
		var bucketName = createBucket(client, 17, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

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
		var bucketName = createBucket(client, 18, ObjectOwnership.ObjectWriter, CannedAccessControlList.Private);
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionFullControl() {
		checkBucketAcl(19, Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWrite() {
		checkBucketAcl(20, Permission.Write);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionWriteAcp() {
		checkBucketAcl(21, Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionRead() {
		checkBucketAcl(22, Permission.Read);
	}

	@Test
	@Tag("Permission")
	public void testBucketPermissionReadAcp() {
		checkBucketAcl(23, Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionFullControl() {
		checkObjectAcl(24, Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWrite() {
		checkObjectAcl(25, Permission.Write);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionWriteAcp() {
		checkObjectAcl(26, Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionRead() {
		checkObjectAcl(27, Permission.Read);
	}

	@Test
	@Tag("Permission")
	public void testObjectPermissionReadAcp() {
		checkObjectAcl(28, Permission.ReadAcp);
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclGrantNonExistUser() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 29);

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
		var bucketName = createBucketCannedAcl(client, 30);

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
		var bucketName = createBucketCannedAcl(client, 31);
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
		var bucketName = createBucketCannedAcl(client, 32);
		var acl = createAcl(config.mainUser.toOwner(), config.altUser.toGrantee(), Permission.values());

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));
		client.setObjectAcl(bucketName, key, acl);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Delete")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
	// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
	public void testBucketAclRevokeAll() {
		var key = "testBucketAclRevokeAll";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 33);

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
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
	// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
	public void testObjectAclRevokeAll() {
		var key = "testObjectAclRevokeAll";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 34);

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
		var bucketName = createBucketCannedAcl(client, 35);

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

	// region ACL API 검증
	static final String LOG_DELIVERY_GROUP = "http://acs.amazonaws.com/groups/s3/LogDelivery";

	@Test
	@Tag("ERROR")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 ACL 본문의 Owner 가 실제 소유자와 달라도 기존 소유자로 덮어써 받는다(소유자는 바뀌지 않는다).
	// AWS 는 403 AccessDenied 다.
	public void testBucketAclChangeOwner() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 41);
		var ownerId = client.getBucketAcl(bucketName).getOwner().getId();
		var acl = new AccessControlList();
		acl.setOwner(new Owner(config.altUser.id, null));
		acl.grantPermission(new CanonicalGrantee(ownerId), Permission.FullControl);

		// ACL 로 소유자를 바꿀 수 없다
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> client.setBucketAcl(bucketName, acl));
	}

	@Test
	@Tag("Max")
	public void testBucketAclMaxGrants() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 44);
		var owner = client.getBucketAcl(bucketName).getOwner();

		// 권한 부여는 100개까지
		var acl = new AccessControlList();
		acl.setOwner(new Owner(owner.getId(), null));
		for (int i = 0; i < 100; i++)
			acl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
		client.setBucketAcl(bucketName, acl);

		acl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
		assertEquals(101, acl.getGrantsAsList().size());
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_ACL_ERROR,
				() -> client.setBucketAcl(bucketName, acl));
	}

	@Test
	@Tag("ERROR")
	public void testBucketAclBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(45);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET, () -> client.getBucketAcl(bucketName));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.Private));
	}

	@Test
	@Tag("Access")
	public void testAclAccessDenied() {
		var key = "testAclAccessDenied";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client, 46);
		client.putObject(bucketName, key, "content");

		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> altClient.getBucketAcl(bucketName));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.setBucketAcl(bucketName, CannedAccessControlList.Private));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> altClient.getObjectAcl(bucketName, key));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.setObjectAcl(bucketName, key, CannedAccessControlList.Private));
	}

	@Test
	@Tag("ERROR")
	public void testObjectAclKeyNotExist() {
		var key = "testObjectAclKeyNotExist";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 47);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client.getObjectAcl(bucketName, key));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.Private));
	}

	@Test
	@Tag("Version")
	public void testObjectAclVersionId() {
		var key = "testObjectAclVersionId";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 48);
		client.putObject(bucketName, key, "content");

		// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
		assertEquals(1, client.getObjectAcl(bucketName, key, "null").getGrantsAsList().size());
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.getObjectAcl(bucketName, key, "invalid"));
	}

	@Test
	@Tag("Version")
	public void testObjectAclDeleteMarker() {
		var key = "testObjectAclDeleteMarker";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 49);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var versionId = client.putObject(bucketName, key, "content").getVersionId();
		client.deleteObject(bucketName, key);
		var markerId = client.listVersions(bucketName, key).getVersionSummaries().stream()
				.filter(v -> v.isDeleteMarker()).findFirst().orElseThrow().getVersionId();

		// 최신 버전이 삭제 마커이면 없는 키, 삭제 마커 버전을 지정하면 405
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client.getObjectAcl(bucketName, key));
		assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
				() -> client.getObjectAcl(bucketName, key, markerId));
		assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
				() -> client.setObjectAcl(bucketName, key, markerId, CannedAccessControlList.Private));
		assertEquals(1, client.getObjectAcl(bucketName, key, versionId).getGrantsAsList().size());
	}

	@Test
	@Tag("Check")
	public void testPutObjectWithGrantHeader() {
		var key = "testPutObjectWithGrantHeader";
		var content = "content";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 51);
		var altId = config.altUser.id;

		// 권한 부여 헤더만 주면 소유자 FULL_CONTROL 없이 지정한 권한만 남는다(SDK V1 은 ACL 을 x-amz-grant-* 헤더로 보낸다)
		var acl = new AccessControlList();
		acl.grantPermission(new CanonicalGrantee(altId), Permission.Read);
		var metadata = new ObjectMetadata();
		metadata.setContentLength(content.length());
		client.putObject(
				new PutObjectRequest(bucketName, key, createBody(content), metadata).withAccessControlList(acl));

		var grants = client.getObjectAcl(bucketName, key).getGrantsAsList();
		assertEquals(1, grants.size());
		assertEquals(altId, grants.get(0).getGrantee().getIdentifier());
		assertEquals(Permission.Read, grants.get(0).getPermission());
		assertEquals(content, getBody(getAltClient().getObject(bucketName, key).getObjectContent()));
	}
	// endregion
}
