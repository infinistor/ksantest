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
import software.amazon.awssdk.services.s3.model.Permission;

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
	// [bucketAcl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	public void testBucketAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		var user = config.mainUser.toGranteeV2();

		assertEquals(user.id(), response.owner().id());

		var getGrants = response.grants();
		var grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclCannedDuringCreate() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read => bucketAcl : private] 권한을 public-read로 생성한 버킷을
	// private로 변경할경우 올바르게 적용되는지 확인
	public void testBucketAclCanned() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);
		var user = config.mainUser.toGranteeV2();

		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		var getGrants = response.grants();
		var grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
		response = client.getBucketAcl(g -> g.bucket(bucketName));
		getGrants = response.grants();

		grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclCannedPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.WRITE).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의 acl정보가 올바른지
	// 확인
	public void testBucketAclCannedAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.AUTHENTICATED_READ);

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createAuthenticatedGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [objectAcl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인
	public void testObjectAclDefault() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		var getGrants = response.grants();
		var grants = List
				.of(Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	public void testObjectAclCannedDuringCreate() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("bar"));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var user = config.mainUser.toGranteeV2();

		var getGrants = response.grants();
		var grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read => objectAcl : private] 권한을 public-read로 생성한 오브젝트를
	// private로 변경할경우 올바르게 적용되는지 확인
	public void testObjectAclCanned() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("bar"));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);

		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE));
		response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		getGrants = response.grants();
		grants = List
				.of(Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지
	// 확인
	public void testObjectAclCannedPublicRW() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ_WRITE),
				RequestBody.fromString("bar"));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var user = config.mainUser.toGranteeV2();

		var getGrants = response.grants();
		var grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.READ).build(),
				Grant.builder().grantee(createPublicGrantee()).permission(Permission.WRITE).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [objectAcl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의 acl정보가
	// 올바른지 확인
	public void testObjectAclCannedAuthenticatedRead() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.AUTHENTICATED_READ),
				RequestBody.fromString("bar"));
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var user = config.mainUser.toGranteeV2();

		var getGrants = response.grants();
		var grants = List.of(Grant.builder().grantee(user).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(createAuthenticatedGrantee()).permission(Permission.READ).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write => objectAcl :
	// bucket-owner-read]"
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인
	public void testObjectAclCannedBucketOwnerRead() {
		var key = "foo";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var bucketACLResponse = mainClient.getBucketAcl(g -> g.bucket(bucketName));
		var owner = Grantee.builder()
				.id(bucketACLResponse.owner().id())
				.type(software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER)
				.build();

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_READ),
				RequestBody.fromString("bar"));
		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.altUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(owner).permission(Permission.READ).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write => objectAcl :
	// bucket-owner-full-control] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인
	public void testObjectAclCannedBucketOwnerFullControl() {
		var key = "foo";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, BucketCannedACL.PUBLIC_READ_WRITE);

		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var aclResponse = mainClient.getBucketAcl(g -> g.bucket(bucketName));
		var owner = Grantee.builder()
				.id(aclResponse.owner().id())
				.type(software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER)
				.build();

		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString("bar"));
		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var getGrants = response.grants();
		var grants = List.of(
				Grant.builder().grantee(config.altUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build(),
				Grant.builder().grantee(owner).permission(Permission.FULL_CONTROL).build());
		checkGrants(grants, getGrants);
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게
	// FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의
	// 소유자가 유지되는지 확인
	public void testObjectAclFullControlVerifyOwner() {
		var key = "foo";
		var mainClient = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(mainClient, BucketCannedACL.PUBLIC_READ_WRITE);

		mainClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var acl1 = createACL(Permission.FULL_CONTROL);
		mainClient.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl1));

		var acl2 = createACL(Permission.READ_ACP);
		altClient.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl2).build());

		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(config.mainUser.id, response.owner().id());
	}

	@Test
	@Tag("ETag")
	// [bucketAcl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인
	public void testObjectAclFullControlVerifyAttributes() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ_WRITE);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var contentType = response.response().contentType();
		var eTag = response.response().eTag();

		var acl = createACL(Permission.FULL_CONTROL);
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl));

		response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(contentType, response.response().contentType());
		assertEquals(eTag, response.response().eTag());
	}

	@Test
	@Tag("Permission")
	// [bucketAcl:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	public void testBucketAclCannedPrivateToPrivate() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	public void testObjectAcl() {
		checkObjectACL(Permission.FULL_CONTROL);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	public void testObjectAclWrite() {
		checkObjectACL(Permission.WRITE);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	public void testObjectAclWriteAcp() {
		checkObjectACL(Permission.WRITE_ACP);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	public void testObjectAclRead() {
		checkObjectACL(Permission.READ);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	public void testObjectAclReadAcp() {
		checkObjectACL(Permission.READ_ACP);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	public void testBucketAclGrantUserFullControl() {
		var bucketName = bucketACLGrantUserId(Permission.FULL_CONTROL);

		checkBucketACLGrantCanRead(bucketName);
		checkBucketACLGrantCanReadACP(bucketName);
		checkBucketACLGrantCanWrite(bucketName);
		checkBucketACLGrantCanWriteACP(bucketName);

		var client = getClient();

		var bucketACLResponse = client.getBucketAcl(g -> g.bucket(bucketName));
		var ownerId = bucketACLResponse.owner().id();

		var mainUserId = config.mainUser.id;

		assertEquals(mainUserId, ownerId);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	public void testBucketAclGrantUserRead() {
		var bucketName = bucketACLGrantUserId(Permission.READ);

		checkBucketACLGrantCanRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	public void testBucketAclGrantUserReadAcp() {
		var bucketName = bucketACLGrantUserId(Permission.READ_ACP);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCanReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	public void testBucketAclGrantUserWrite() {
		var bucketName = bucketACLGrantUserId(Permission.WRITE);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCanWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	public void testBucketAclGrantUserWriteAcp() {
		var bucketName = bucketACLGrantUserId(Permission.WRITE_ACP);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCanWriteACP(bucketName);
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

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
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
	@Tag("Header")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	public void testObjectHeaderAclGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "foo";
		var grants = createACL();

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
		client.putObjectAcl(c -> c.bucket(bucketName).key(key).accessControlPolicy(grants));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		var getGrants = response.grants();
		checkGrants(grants.grants(), getGrants);
	}

	@Test
	@Tag("Header")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	public void testBucketHeaderAclGrants() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var grants = createACL();

		client.putBucketAcl(c -> c.bucket(bucketName)
				.accessControlPolicy(grants)
				.build());

		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		var getGrants = response.grants();
		checkGrants(grants.grants(), getGrants);
	}

	@Test
	@Tag("Delete")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	public void testBucketAclRevokeAll() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		client.putObject(p -> p.bucket(bucketName).key("foo"), RequestBody.fromString("bar"));
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

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		assertThrows(AwsServiceException.class, () -> client.putObjectAcl(
				p -> p.bucket(bucketName).key(key)
						.accessControlPolicy(a -> a.owner(o -> o.build()).grants(response.grants()))));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과 오브젝트를 서브유저가
	// 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인
	public void testAccessBucketPrivateObjectPrivate() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PRIVATE);

		var altClient = getAltClient();

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key1)));
		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient.listObjects(l -> l.bucket(bucketName)));
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
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과 오브젝트를 서브유저가
	// 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjectsV2)
	public void testAccessBucketPrivateObjectV2Private() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PRIVATE);

		var altClient = getAltClient();

		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key1)));
		assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(key2)));
		assertThrows(AwsServiceException.class,
				() -> altClient.listObjects(l -> l.bucket(bucketName)));
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
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인
	public void testAccessBucketPrivateObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ);
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
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인(ListObjectsV2)
	public void testAccessBucketPrivateObjectV2PublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ);
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
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할
	// 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)
	public void testAccessBucketPrivateObjectPublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할
	// 수 있음을 확인(ListObjectsV2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 업로드불가)
	public void testAccessBucketPrivateObjectV2PublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:private] 메인유저가 public-read권한으로 생성한 버킷에서
	// private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인
	public void testAccessBucketPublicReadObjectPrivate() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		var objList = getKeys(
				altClient3.listObjects(l -> l.bucket(bucketName)).contents());
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read, private] 메인유저가 public-read권한으로
	// 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인
	public void testAccessBucketPublicReadObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read-write, private] 메인유저가
	// public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
	// 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 수정불가)
	public void testAccessBucketPublicReadObjectPublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AwsServiceException.class,
				() -> altClient2.putObject(p -> p.bucket(bucketName).key(newKey),
						RequestBody.fromString("new-content")));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:private] 메인유저가 public-read-write권한으로
	// 생성한 버킷에서
	// private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록 조회는 가능하나 다운로드 할 수 없음을 확인
	public void testAccessBucketPublicRWObjectPrivate() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		assertEquals(List.of(key2, key1), objList);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서
	// public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록 조회, 다운로드 가능함을 확인
	public void testAccessBucketPublicRWObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		assertEquals(List.of(key2, key1), objects);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}

	@Test
	@Disabled
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read-write, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서
	// public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 다운로드 가능함을 확인
	public void testAccessBucketPublicRWObjectPublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
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
		assertEquals(List.of(key2, key1), objects);
		altClient.putObject(p -> p.bucket(bucketName).key(newKey), RequestBody.fromString("new-content"));
	}
}
