/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;

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

public class Grants extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Grants Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Grants End");
	}

	@Test
	@Tag("Bucket")
	// [bucket_acl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	public void test_bucket_acl_default()
	{
		var bucketName = getNewBucket();

		var client = getClient();
		var Response = client.getBucketAcl(bucketName);

		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		if (!StringUtils.isBlank(config.URL)) assertEquals(DisplayName, Response.getOwner().getDisplayName());
		assertEquals(UserId, Response.getOwner().getId());

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucket_acl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인
	public void test_bucket_acl_canned_during_create()
	{
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
		var Response = client.getBucketAcl(bucketName);

		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucket_acl : public-read => bucket_acl : private] 권한을 public-read로 생성한 버킷을 private로 변경할경우 올바르게 적용되는지 확인
	public void test_bucket_acl_canned()
	{
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));
		var Response = client.getBucketAcl(bucketName);

		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
		Response = client.getBucketAcl(bucketName);
		GetGrants = Response.getGrantsAsList();

		MyGrants.clear();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucket_acl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가 올바른지 확인
	public void test_bucket_acl_canned_publicreadwrite()
	{
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var Response = client.getBucketAcl(bucketName);

		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Write));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Bucket")
	// [bucket_acl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의 acl정보가 올바른지 확인
	public void test_bucket_acl_canned_authenticatedread()
	{
		var bucketName = getNewBucketName();

		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.AuthenticatedRead));
		var Response = client.getBucketAcl(bucketName);

		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AuthenticatedUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [object_acl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인
	public void test_object_acl_default()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Key = "foo";
		client.putObject(bucketName, Key, "bar");
		var Response = client.getObjectAcl(bucketName, Key);


		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [object_acl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	public void test_object_acl_canned_during_create()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		var Key = "foo";
		client.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead));
		var Response = client.getObjectAcl(bucketName, Key);


		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [object_acl : public-read => object_acl : private] 권한을 public-read로 생성한 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인
	public void test_object_acl_canned()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		var Key = "foo";
		client.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead));
		var Response = client.getObjectAcl(bucketName, Key);


		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));

		client.setObjectAcl(bucketName, Key, CannedAccessControlList.Private);
		Response = client.getObjectAcl(bucketName, Key);

		GetGrants = Response.getGrantsAsList();
		MyGrants.clear();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [object_acl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지 확인
	public void test_object_acl_canned_publicreadwrite()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		var Key = "foo";
		client.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var Response = client.getObjectAcl(bucketName, Key);


		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Write));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [object_acl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	public void test_object_acl_canned_authenticatedread()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		var Key = "foo";
		client.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.AuthenticatedRead));
		var Response = client.getObjectAcl(bucketName, Key);


		var DisplayName = config.mainUser.displayName;
		var UserId = config.mainUser.userId;
		var User = new CanonicalGrantee(UserId);
		User.setDisplayName(DisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AuthenticatedUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [bucket_acl: public-read-write] [object_acl : public-read-write => object_acl : bucket-owner-read]" +
	//"메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을 bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인
	public void test_object_acl_canned_bucketownerread()
	{
		var bucketName = getNewBucketName();
		var MainClient = getClient();
		var AltClient = getAltClient();
		var Key = "foo";

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		MainClient.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		AltClient.putObject(bucketName, Key, "bar");

		var BucketACLResponse = MainClient.getBucketAcl(bucketName);
		var BucketOwnerID = BucketACLResponse.getOwner().getId();
		var BucketOwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();
		var OwnerUser = new CanonicalGrantee(BucketOwnerID);
		OwnerUser.setDisplayName(BucketOwnerDisplayName);

		AltClient.putObject(new PutObjectRequest(bucketName, Key, createBody(Key), Metadata).withCannedAcl(CannedAccessControlList.BucketOwnerRead));
		var Response = AltClient.getObjectAcl(bucketName, Key);

		var AltDisplayName = config.altUser.displayName;
		var AltUserID = config.altUser.userId;
		var User = new CanonicalGrantee(AltUserID);
		User.setDisplayName(AltDisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(OwnerUser, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [bucket_acl: public-read-write] [object_acl : public-read-write => object_acl : bucket-owner-full-control] " +
	//"메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을 bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인
	public void test_object_acl_canned_bucketownerfullcontrol()
	{
		var bucketName = getNewBucketName();
		var MainClient = getClient();
		var AltClient = getAltClient();
		var Key = "foo";

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		MainClient.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		AltClient.putObject(bucketName, Key, "bar");

		var BucketACLResponse = MainClient.getBucketAcl(bucketName);
		var BucketOwnerID = BucketACLResponse.getOwner().getId();
		var BucketOwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();
		var OwnerUser = new CanonicalGrantee(BucketOwnerID);
		OwnerUser.setDisplayName(BucketOwnerDisplayName);

		AltClient.putObject(new PutObjectRequest(bucketName, Key, createBody(Key), Metadata).withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
		var Response = AltClient.getObjectAcl(bucketName, Key);

		var AltDisplayName = config.altUser.displayName;
		var AltUserID = config.altUser.userId;
		var User = new CanonicalGrantee(AltUserID);
		User.setDisplayName(AltDisplayName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(OwnerUser, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Object")
	// [bucket_acl: public-read-write] " +
	//"메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게 FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의 소유자가 유지되는지 확인
	public void test_object_acl_full_control_verify_owner()
	{
		var bucketName = getNewBucketName();
		var MainClient = getClient();
		var AltClient = getAltClient();
		var Key = "foo";

		MainClient.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		MainClient.putObject(bucketName, Key, "bar");

		var MainUserID = config.mainUser.userId;
		var MainDisplayName = config.mainUser.displayName;
		var MainUser = new CanonicalGrantee(MainUserID);
		MainUser.setDisplayName(MainDisplayName);

		var AltUserID = config.altUser.userId;
		var AltDisplayName = config.altUser.displayName;
		var AltUser = new CanonicalGrantee(AltUserID);
		AltUser.setDisplayName(AltDisplayName);


		var accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(MainUserID, MainDisplayName));
		accessControlList.grantPermission(AltUser, Permission.FullControl);

		MainClient.setObjectAcl(bucketName, Key, accessControlList);
		accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(MainUserID, MainDisplayName));
		accessControlList.grantPermission(AltUser, Permission.ReadAcp);

		AltClient.setObjectAcl(bucketName, Key, accessControlList);

		var Response = AltClient.getObjectAcl(bucketName, Key);
		assertEquals(MainUserID, Response.getOwner().getId());
	}

	@Test
	@Tag("ETag")
	// [bucket_acl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인
	public void test_object_acl_full_control_verify_attributes()
	{
		var bucketName = getNewBucketName();
		var MainClient = getClient();
		var Key = "foo";

		MainClient.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var Headers = new ObjectMetadata();
		Headers.addUserMetadata("x-amz-meta-foo", "bar");
		Headers.setContentType("text/plain");
		Headers.setContentLength(3);

		MainClient.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Headers));

		var Response = MainClient.getObject(bucketName, Key);
		var ContentType = Response.getObjectMetadata().getContentType();
		var ETag = Response.getObjectMetadata().getETag();

		var AltUserID = config.altUser.userId;
		var AltDisplayName = config.altUser.displayName;
		var AltUser = new CanonicalGrantee(AltUserID);
		AltUser.setDisplayName(AltDisplayName);

		var accessControlList = addObjectUserGrant(bucketName, Key, new Grant(AltUser, Permission.FullControl));

		MainClient.setObjectAcl(bucketName, Key, accessControlList);

		Response = MainClient.getObject(bucketName, Key);
		assertEquals(ContentType, Response.getObjectMetadata().getContentType());
		assertEquals(ETag, Response.getObjectMetadata().getETag());
	}

	@Test
	@Tag("Permission")
	// [bucket_acl:private] 기본생성한 버킷에 priavte 설정이 가능한지 확인
	public void test_bucket_acl_canned_private_to_private()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	public void test_object_acl()
	{
		CheckObjectACL(Permission.FullControl);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	public void test_object_acl_write()
	{
		CheckObjectACL(Permission.Write);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	public void test_object_acl_writeacp()
	{
		CheckObjectACL(Permission.WriteAcp);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	public void test_object_acl_read()
	{
		CheckObjectACL(Permission.Read);
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	public void test_object_acl_readacp()
	{
		CheckObjectACL(Permission.ReadAcp);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	public void test_bucket_acl_grant_userid_fullcontrol()
	{
		var bucketName = bucketACLGrantUserId(Permission.FullControl);

		CheckBucketACLGrantCanRead(bucketName);
		CheckBucketACLGrantCanReadACP(bucketName);
		CheckBucketACLGrantCanWrite(bucketName);
		CheckBucketACLGrantCanWriteACP(bucketName);

		var client = getClient();

		var BucketACLResponse = client.getBucketAcl(bucketName);
		var OwnerID = BucketACLResponse.getOwner().getId();
		var OwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();

		var MainUserID = config.mainUser.userId;
		var MainDisplayName = config.mainUser.displayName;

		assertEquals(MainUserID, OwnerID);
		if (!StringUtils.isBlank(config.URL)) assertEquals(MainDisplayName, OwnerDisplayName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	public void test_bucket_acl_grant_userid_read()
	{
		var bucketName = bucketACLGrantUserId(Permission.Read);

		CheckBucketACLGrantCanRead(bucketName);
		CheckBucketACLGrantCantReadACP(bucketName);
		CheckBucketACLGrantCantWrite(bucketName);
		CheckBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	public void test_bucket_acl_grant_userid_readacp()
	{
		var bucketName = bucketACLGrantUserId(Permission.ReadAcp);

		CheckBucketACLGrantCantRead(bucketName);
		CheckBucketACLGrantCanReadACP(bucketName);
		CheckBucketACLGrantCantWrite(bucketName);
		CheckBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	public void test_bucket_acl_grant_userid_write()
	{
		var bucketName = bucketACLGrantUserId(Permission.Write);

		CheckBucketACLGrantCantRead(bucketName);
		CheckBucketACLGrantCantReadACP(bucketName);
		CheckBucketACLGrantCanWrite(bucketName);
		CheckBucketACLGrantCantWriteACP(bucketName);
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	public void test_bucket_acl_grant_userid_writeacp()
	{
		var bucketName = bucketACLGrantUserId(Permission.WriteAcp);

		CheckBucketACLGrantCantRead(bucketName);
		CheckBucketACLGrantCantReadACP(bucketName);
		CheckBucketACLGrantCantWrite(bucketName);
		CheckBucketACLGrantCanWriteACP(bucketName);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	public void test_bucket_acl_grant_nonexist_user()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var BadUser = new CanonicalGrantee("_foo");
		var accessControlList = addBucketUserGrant(bucketName, new Grant(BadUser, Permission.FullControl));

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, accessControlList));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidArgument, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
	public void test_bucket_acl_no_grants()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";

		client.putObject(bucketName, Key, "bar");
		var Response = client.getBucketAcl(bucketName);
		var OldGrants = Response.getGrantsAsList();
		var Policy = new AccessControlList();
		Policy.setOwner(Response.getOwner());

		client.setBucketAcl(bucketName, Policy);

		client.getObject(bucketName, Key);

		assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, Key, "A"));

		var Client2 = getClient();
		Client2.getBucketAcl(bucketName);
		Client2.setBucketAcl(bucketName, CannedAccessControlList.Private);

		for(var MyGrant : OldGrants) Policy.grantAllPermissions(MyGrant);
		Client2.setBucketAcl(bucketName, Policy);
	}

	@Test
	@Tag("Header")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	public void test_object_header_acl_grants()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo_key";

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(3);

		var AltUser = new CanonicalGrantee(config.altUser.userId);
		AltUser.setDisplayName(config.altUser.displayName);

		var Grants = getGrantList(null, null);

		client.putObject(new PutObjectRequest(bucketName, Key, createBody("bar"), Metadata).withAccessControlList(Grants));
		var Response = client.getObjectAcl(bucketName, Key);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(AltUser, Permission.FullControl));
		MyGrants.add(new Grant(AltUser, Permission.Read));
		MyGrants.add(new Grant(AltUser, Permission.ReadAcp));
		MyGrants.add(new Grant(AltUser, Permission.Write));
		MyGrants.add(new Grant(AltUser, Permission.WriteAcp));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Header")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	public void test_bucket_header_acl_grants()
	{
		var bucketName = getNewBucketName();
		var client = getClient();

		var AltUser = new CanonicalGrantee(config.altUser.userId);
		AltUser.setDisplayName(config.altUser.displayName);

		var Headers = getGrantList(null, null);

		client.createBucket(new CreateBucketRequest(bucketName).withAccessControlList(Headers));
		var Response = client.getBucketAcl(bucketName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(AltUser, Permission.FullControl));
		MyGrants.add(new Grant(AltUser, Permission.Read));
		MyGrants.add(new Grant(AltUser, Permission.ReadAcp));
		MyGrants.add(new Grant(AltUser, Permission.Write));
		MyGrants.add(new Grant(AltUser, Permission.WriteAcp));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Delete")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	public void test_bucket_acl_revoke_all()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, "foo", "bar");
		var Response = client.getBucketAcl(bucketName);

		var ACL1 = new AccessControlList();
		ACL1.setOwner(new Owner());
		for (var Item : Response.getGrantsAsList()) ACL1.grantAllPermissions(Item);

		assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, ACL1));

		var ACL2 = new AccessControlList();
		ACL2.setOwner(Response.getOwner());

		client.setBucketAcl(bucketName, ACL2);

		var ACL3 = new AccessControlList();
		ACL3.setOwner(new Owner());

		assertThrows(AmazonServiceException.class, () -> client.setBucketAcl(bucketName, ACL3));
	}

	@Test
	@Tag("Delete")
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	public void test_object_acl_revoke_all()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";

		client.putObject(bucketName, Key, "bar");

		var Response = client.getObjectAcl(bucketName, Key);

		var ACL1 = new AccessControlList();
		ACL1.setOwner(new Owner());
		for (var Item : Response.getGrantsAsList()) ACL1.grantAllPermissions(Item);

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, Key, ACL1));

		var ACL2 = new AccessControlList();
		ACL2.setOwner(Response.getOwner());
		
		client.setObjectAcl(bucketName, Key, ACL2);

		var ACL3 = new AccessControlList();
		ACL3.setOwner(new Owner());

		assertThrows(AmazonServiceException.class, () -> client.setObjectAcl(bucketName, Key, ACL3));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인
	public void test_access_bucket_private_object_private()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

		var AltClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key1));
		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "barcontent"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjects_v2)
	public void test_access_bucket_private_objectv2_private()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

		var AltClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key1));
		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "barcontent"));

		var AltClient2 = getAltClient();

		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인
	public void test_access_bucket_private_object_publicread()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicRead);
		var AltClient = getAltClient();
		var Response = AltClient.getObject(bucketName, Key1);

		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인(ListObjects_v2)
	public void test_access_bucket_private_objectv2_publicread()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicRead);
		var AltClient = getAltClient();
		var Response = AltClient.getObject(bucketName, Key1);

		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private, public-read-write] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)
	public void test_access_bucket_private_object_publicreadwrite()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite);
		var AltClient = getAltClient();
		var Response = AltClient.getObject(bucketName, Key1);

		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient3.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:private, object_acl:private, public-read-write] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인(ListObjects_v2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)
	public void test_access_bucket_private_objectv2_publicreadwrite()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite);
		var AltClient = getAltClient();
		var Response = AltClient.getObject(bucketName, Key1);

		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient3.listObjectsV2(bucketName));
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:public-read, object_acl:private] 메인유저가 public-read권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인
	public void test_access_bucket_publicread_object_private()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.Private);
		var AltClient = getAltClient();

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key1));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		var ObjList = GetKeys(AltClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:public-read, object_acl:public-read, private] 메인유저가 public-read권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인
	public void test_access_bucket_publicread_object_publicread()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead);
		var AltClient = getAltClient();

		var Response = AltClient.getObject(bucketName, Key1);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		var ObjList = GetKeys(AltClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:public-read, object_acl:public-read-wirte, private] 메인유저가 public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 수정불가)
	public void test_access_bucket_publicread_object_publicreadwrite()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicReadWrite);
		var AltClient = getAltClient();

		var Response = AltClient.getObject(bucketName, Key1);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);

		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		var AltClient2 = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(bucketName, Key2, "baroverwrite"));

		var AltClient3 = getAltClient();
		var ObjList = GetKeys(AltClient3.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(bucketName, NewKey, "newcontent"));
	}

	@Test
	@Tag("Access")
	// [bucket_acl:public-read-write, object_acl:private] 메인유저가 public-read-write권한으로 생성한 버킷에서
	// private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록 조회는 가능하나 다운로드 할 수 없음을 확인
	public void test_access_bucket_publicreadwrite_object_private()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private);
		var AltClient = getAltClient();


		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key1));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key2, "baroverwrite"));

		var ObjList = GetKeys(AltClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		AltClient.putObject(bucketName, NewKey, "newcontent");
	}

	@Test
	@Tag("Access")
	// [bucket_acl:public-read-write, object_acl:public-read, private] 메인유저가 public-read-write권한으로 생성한 버킷에서
	// public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록 조회, 다운로드 가능함을 확인
	public void test_access_bucket_publicreadwrite_object_publicread()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicRead);
		var AltClient = getAltClient();

		var Response = AltClient.getObject(bucketName, Key1);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key2, "baroverwrite"));

		var ObjList = GetKeys(AltClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		AltClient.putObject(bucketName, NewKey, "newcontent");
	}

	@Test
	@Ignore
	@Tag("Access")
	// [bucket_acl:public-read-write, object_acl:public-read-write, private] 메인유저가 public-read-write권한으로 생성한 버킷에서
	// public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 다운로드 가능함을 확인
	public void test_access_bucket_publicreadwrite_object_publicreadwrite()
	{
		var Key1 = "foo";
		var Key2 = "bar";
		var NewKey = "new";
		var bucketName = setupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicReadWrite);
		var AltClient = getAltClient();

		var Response = AltClient.getObject(bucketName, Key1);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foocontent", Body);
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key1, "foooverwrite"));

		assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key2));
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key2, "baroverwrite"));

		var ObjList = GetKeys(AltClient.listObjects(bucketName).getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
		AltClient.putObject(bucketName, NewKey, "newcontent");
	}
}
