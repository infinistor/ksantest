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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.example.Data.MainData;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;

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
		var bucketName = getNewBucket();

		var client = getClient();
		var response = client.getBucketAcl(bucketName);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		if (!StringUtils.isBlank(config.url))
			assertEquals(displayName, response.getOwner().getDisplayName());
		assertEquals(userId, response.getOwner().getId());

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclCannedDuringCreate() {
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
		var response = client.getBucketAcl(bucketName);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read => bucketAcl : private] 권한을 public-read로 생성한 버킷을
	// private로 변경할경우 올바르게 적용되는지 확인
	public void testBucketAclCanned() {
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
		var response = client.getBucketAcl(bucketName);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
		response = client.getBucketAcl(bucketName);
		getGrants = response.getGrantsAsList();

		myGrants.clear();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가 올바른지 확인
	public void testBucketAclCannedPublicRW() {
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var response = client.getBucketAcl(bucketName);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Write));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의 acl정보가 올바른지
	// 확인
	public void testBucketAclCannedAuthenticatedRead() {
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(
				new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.AuthenticatedRead));
		var response = client.getBucketAcl(bucketName);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AuthenticatedUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [objectAcl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인
	public void testObjectAclDefault() {
		var bucketName = getNewBucket();
		var client = getClient();

		var key = "foo";
		client.putObject(bucketName, key, "bar");
		var response = client.getObjectAcl(bucketName, key);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	public void testObjectAclCannedDuringCreate() {
		var bucketName = getNewBucket();
		var client = getClient();

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		var key = "foo";
		client.putObject(new PutObjectRequest(bucketName, key, createBody("bar"), metadata)
				.withCannedAcl(CannedAccessControlList.PublicRead));
		var response = client.getObjectAcl(bucketName, key);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read => objectAcl : private] 권한을 public-read로 생성한 오브젝트를
	// private로 변경할경우 올바르게 적용되는지 확인
	public void testObjectAclCanned() {
		var bucketName = getNewBucket();
		var client = getClient();

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		var key = "foo";
		client.putObject(new PutObjectRequest(bucketName, key, createBody("bar"), metadata)
				.withCannedAcl(CannedAccessControlList.PublicRead));
		var response = client.getObjectAcl(bucketName, key);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));

		client.setObjectAcl(bucketName, key, CannedAccessControlList.Private);
		response = client.getObjectAcl(bucketName, key);

		getGrants = response.getGrantsAsList();
		myGrants.clear();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지
	// 확인
	public void testObjectAclCannedPublicRW() {
		var bucketName = getNewBucket();
		var client = getClient();

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		var key = "foo";
		client.putObject(new PutObjectRequest(bucketName, key, createBody("bar"), metadata)
				.withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var response = client.getObjectAcl(bucketName, key);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Write));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [objectAcl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의 acl정보가
	// 올바른지 확인
	public void testObjectAclCannedAuthenticatedRead() {
		var bucketName = getNewBucket();
		var client = getClient();

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		var key = "foo";
		client.putObject(new PutObjectRequest(bucketName, key, createBody("bar"), metadata)
				.withCannedAcl(CannedAccessControlList.AuthenticatedRead));
		var response = client.getObjectAcl(bucketName, key);

		var displayName = config.mainUser.displayName;
		var userId = config.mainUser.userId;
		var user = new CanonicalGrantee(userId);
		user.setDisplayName(displayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AuthenticatedUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write => objectAcl :
	// bucket-owner-read]" +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인
	public void testObjectAclCannedBucketOwnerRead() {
		var bucketName = getNewBucketName();
		var mainClient = getClient();
		var altClient = getAltClient();
		var key = "foo";

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		mainClient.createBucket(
				new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		altClient.putObject(bucketName, key, "bar");

		var bucketACLResponse = mainClient.getBucketAcl(bucketName);
		var bucketOwnerId = bucketACLResponse.getOwner().getId();
		var bucketOwnerDisplayName = bucketACLResponse.getOwner().getDisplayName();
		var owner = new CanonicalGrantee(bucketOwnerId);
		owner.setDisplayName(bucketOwnerDisplayName);

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), metadata)
				.withCannedAcl(CannedAccessControlList.BucketOwnerRead));
		var response = altClient.getObjectAcl(bucketName, key);

		var altDisplayName = config.altUser.displayName;
		var altUserId = config.altUser.userId;
		var user = new CanonicalGrantee(altUserId);
		user.setDisplayName(altDisplayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(owner, Permission.Read));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write => objectAcl :
	// bucket-owner-full-control] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인
	public void testObjectAclCannedBucketOwnerFullControl() {
		var bucketName = getNewBucketName();
		var mainClient = getClient();
		var altClient = getAltClient();
		var key = "foo";

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		mainClient.createBucket(
				new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		altClient.putObject(bucketName, key, "bar");

		var bucketACLResponse = mainClient.getBucketAcl(bucketName);
		var bucketOwnerId = bucketACLResponse.getOwner().getId();
		var bucketOwnerDisplayName = bucketACLResponse.getOwner().getDisplayName();
		var owner = new CanonicalGrantee(bucketOwnerId);
		owner.setDisplayName(bucketOwnerDisplayName);

		altClient.putObject(new PutObjectRequest(bucketName, key, createBody(key), metadata)
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
		var response = altClient.getObjectAcl(bucketName, key);

		var altDisplayName = config.altUser.displayName;
		var altUserId = config.altUser.userId;
		var user = new CanonicalGrantee(altUserId);
		user.setDisplayName(altDisplayName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(owner, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게
	// FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의
	// 소유자가 유지되는지 확인
	public void testObjectAclFullControlVerifyOwner() {
		var bucketName = getNewBucketName();
		var mainClient = getClient();
		var altClient = getAltClient();
		var key = "foo";

		mainClient.createBucket(
				new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		mainClient.putObject(bucketName, key, "bar");

		var mainUserId = config.mainUser.userId;
		var mainDisplayName = config.mainUser.displayName;
		var mainUser = new CanonicalGrantee(mainUserId);
		mainUser.setDisplayName(mainDisplayName);

		var altUserId = config.altUser.userId;
		var altDisplayName = config.altUser.displayName;
		var altUser = new CanonicalGrantee(altUserId);
		altUser.setDisplayName(altDisplayName);

		var accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(mainUserId, mainDisplayName));
		accessControlList.grantPermission(altUser, Permission.FullControl);

		mainClient.setObjectAcl(bucketName, key, accessControlList);
		accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(mainUserId, mainDisplayName));
		accessControlList.grantPermission(altUser, Permission.ReadAcp);

		altClient.setObjectAcl(bucketName, key, accessControlList);

		var response = altClient.getObjectAcl(bucketName, key);
		assertEquals(mainUserId, response.getOwner().getId());
	}

	@Test
	@Tag("ETag")
	// [bucketAcl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인
	public void testObjectAclFullControlVerifyAttributes() {
		var bucketName = getNewBucketName();
		var mainClient = getClient();
		var key = "foo";

		mainClient.createBucket(
				new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var headers = new ObjectMetadata();
		headers.addUserMetadata("x-amz-meta-foo", "bar");
		headers.setContentType("text/plain");
		headers.setContentLength(3);

		mainClient.putObject(new PutObjectRequest(bucketName, key, createBody("bar"), headers));

		var response = mainClient.getObject(bucketName, key);
		var contentType = response.getObjectMetadata().getContentType();
		var eTag = response.getObjectMetadata().getETag();

		var altUserId = config.altUser.userId;
		var altDisplayName = config.altUser.displayName;
		var altUser = new CanonicalGrantee(altUserId);
		altUser.setDisplayName(altDisplayName);

		var accessControlList = addObjectUserGrant(bucketName, key, new Grant(altUser, Permission.FullControl));

		mainClient.setObjectAcl(bucketName, key, accessControlList);

		response = mainClient.getObject(bucketName, key);
		assertEquals(contentType, response.getObjectMetadata().getContentType());
		assertEquals(eTag, response.getObjectMetadata().getETag());
	}

	@Test
	@Tag("Permission")
	// [bucketAcl:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	public void testBucketAclCannedPrivateToPrivate() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	public void testObjectAcl() {
		checkObjectACL(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	public void testObjectAclWrite() {
		checkObjectACL(Permission.Write);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	public void testObjectAclWriteAcp() {
		checkObjectACL(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	public void testObjectAclRead() {
		checkObjectACL(Permission.Read);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	public void testObjectAclReadAcp() {
		checkObjectACL(Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	public void testBucketAclGrantUserFullControl() {
		var bucketName = bucketACLGrantUserId(Permission.FullControl);

		checkBucketACLGrantCanRead(bucketName);
		checkBucketACLGrantCanReadACP(bucketName);
		checkBucketACLGrantCanWrite(bucketName);
		checkBucketACLGrantCanWriteACP(bucketName);

		var client = getClient();

		var bucketACLResponse = client.getBucketAcl(bucketName);
		var ownerId = bucketACLResponse.getOwner().getId();
		var ownerDisplayName = bucketACLResponse.getOwner().getDisplayName();

		var mainUserId = config.mainUser.userId;
		var mainDisplayName = config.mainUser.displayName;

		assertEquals(mainUserId, ownerId);
		if (!StringUtils.isBlank(config.url))
			assertEquals(mainDisplayName, ownerDisplayName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	public void testBucketAclGrantUserRead() {
		var bucketName = bucketACLGrantUserId(Permission.Read);

		checkBucketACLGrantCanRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	public void testBucketAclGrantUserReadAcp() {
		var bucketName = bucketACLGrantUserId(Permission.ReadAcp);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCanReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	public void testBucketAclGrantUserWrite() {
		var bucketName = bucketACLGrantUserId(Permission.Write);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCanWrite(bucketName);
		checkBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	public void testBucketAclGrantUserWriteAcp() {
		var bucketName = bucketACLGrantUserId(Permission.WriteAcp);

		checkBucketACLGrantCantRead(bucketName);
		checkBucketACLGrantCantReadACP(bucketName);
		checkBucketACLGrantCantWrite(bucketName);
		checkBucketACLGrantCanWriteACP(bucketName);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	public void testBucketAclGrantNonExistUser() {
		var bucketName = getNewBucket();
		var client = getClient();

		var badUser = new CanonicalGrantee("Foo");
		var accessControlList = addBucketUserGrant(bucketName, new Grant(badUser, Permission.FullControl));

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, accessControlList));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.InvalidArgument, errorCode);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
	public void testBucketAclNoGrants() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "bar");
		var response = client.getBucketAcl(bucketName);
		var oldGrants = response.getGrantsAsList();
		var policy = new AccessControlList();
		policy.setOwner(response.getOwner());

		client.setBucketAcl(bucketName, policy);

		client.getObject(bucketName, key);

		assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, "A"));

		var client2 = getClient();
		client2.getBucketAcl(bucketName);
		client2.setBucketAcl(bucketName, CannedAccessControlList.Private);

		for (var MyGrant : oldGrants)
			policy.grantAllPermissions(MyGrant);
		client2.setBucketAcl(bucketName, policy);
	}

	@Test
	@Tag("Header")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	public void testObjectHeaderAclGrants() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "fooKey";

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);

		var altUser = new CanonicalGrantee(config.altUser.userId);
		altUser.setDisplayName(config.altUser.displayName);

		var grants = getGrantList();

		client.putObject(
				new PutObjectRequest(bucketName, key, createBody("bar"), metadata).withAccessControlList(grants));
		var response = client.getObjectAcl(bucketName, key);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(altUser, Permission.FullControl));
		myGrants.add(new Grant(altUser, Permission.Read));
		myGrants.add(new Grant(altUser, Permission.ReadAcp));
		myGrants.add(new Grant(altUser, Permission.Write));
		myGrants.add(new Grant(altUser, Permission.WriteAcp));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Header")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	public void testBucketHeaderAclGrants() {
		var bucketName = getNewBucketName();
		var client = getClient();

		var altUser = new CanonicalGrantee(config.altUser.userId);
		altUser.setDisplayName(config.altUser.displayName);

		var headers = getGrantList();

		client.createBucket(new CreateBucketRequest(bucketName).withAccessControlList(headers));
		var response = client.getBucketAcl(bucketName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(altUser, Permission.FullControl));
		myGrants.add(new Grant(altUser, Permission.Read));
		myGrants.add(new Grant(altUser, Permission.ReadAcp));
		myGrants.add(new Grant(altUser, Permission.Write));
		myGrants.add(new Grant(altUser, Permission.WriteAcp));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("Delete")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	public void testBucketAclRevokeAll() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, "foo", "bar");
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
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "bar");

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
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과 오브젝트를 서브유저가
	// 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인
	public void testAccessBucketPrivateObjectPrivate() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, key2));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, newKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과 오브젝트를 서브유저가
	// 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjectsV2)
	public void testAccessBucketPrivateObjectV2Private() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, key2));

		var altClient2 = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인
	public void testAccessBucketPrivateObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인(ListObjectsV2)
	public void testAccessBucketPrivateObjectV2PublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할
	// 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)
	public void testAccessBucketPrivateObjectPublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
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
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.Private,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();
		var response = altClient.getObject(bucketName, key1);

		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:private] 메인유저가 public-read권한으로 생성한 버킷에서
	// private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인
	public void testAccessBucketPublicReadObjectPrivate() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead,
				CannedAccessControlList.Private);
		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read, private] 메인유저가 public-read권한으로
	// 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인
	public void testAccessBucketPublicReadObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
	}

	@SuppressWarnings("resource")
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
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicRead,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);

		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		var altClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> altClient2.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient2.putObject(bucketName, key2, "baroverwrite"));

		var altClient3 = getAltClient();
		var objList = getKeys(altClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objList);
		assertThrows(AmazonServiceException.class, () -> altClient3.putObject(bucketName, newKey, "newcontent"));
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
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.Private);
		var altClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key1));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "baroverwrite"));

		var objList = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objList);
		altClient.putObject(bucketName, newKey, "newcontent");
	}

	@SuppressWarnings("resource")
	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서
	// public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록 조회, 다운로드 가능함을 확인
	public void testAccessBucketPublicRWObjectPublicRead() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicRead);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "baroverwrite"));

		var objects = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objects);
		altClient.putObject(bucketName, newKey, "newcontent");
	}

	@SuppressWarnings("resource")
	@Test
	@Ignore
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read-write, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서
	// public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 다운로드 가능함을 확인
	public void testAccessBucketPublicRWObjectPublicRW() {
		var key1 = "foo";
		var key2 = "bar";
		var newKey = "new";
		var bucketName = setupAccessTest(key1, key2, CannedAccessControlList.PublicReadWrite,
				CannedAccessControlList.PublicReadWrite);
		var altClient = getAltClient();

		var response = altClient.getObject(bucketName, key1);
		var body = getBody(response.getObjectContent());
		assertEquals(key1, body);
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key2));
		assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, "baroverwrite"));

		var objects = getKeys(altClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(List.of(key2, key1), objects);
		altClient.putObject(bucketName, newKey, "newcontent");
	}
}
