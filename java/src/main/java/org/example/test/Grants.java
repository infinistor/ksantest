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

import org.example.s3tests.MainData;
import org.junit.jupiter.api.Tag;
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
    @Tag("KSAN")
    //@Tag("[bucket_acl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인")
    public void test_bucket_acl_default()
    {
        var BucketName = GetNewBucket();
        
        var Client = GetClient();
        var Response = Client.getBucketAcl(BucketName);

        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
        var User = new CanonicalGrantee(UserId);
        User.setDisplayName(DisplayName);

        if (!StringUtils.isBlank(Config.URL)) assertEquals(DisplayName, Response.getOwner().getDisplayName());
        assertEquals(UserId, Response.getOwner().getId());

        var GetGrants = Response.getGrantsAsList();
        var MyGrants = new ArrayList<Grant>();
        MyGrants.add(new Grant(User, Permission.FullControl));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
    }

    @Test
    @Tag("Bucket")
    @Tag("KSAN")
    //@Tag("[bucket_acl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인")
    public void test_bucket_acl_canned_during_create()
    {
        var BucketName = GetNewBucketName();

        var Client = GetClient();
        Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicRead));
        var Response = Client.getBucketAcl(BucketName);

        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[bucket_acl : public-read => bucket_acl : private] 권한을 public-read로 생성한 버킷을 private로 변경할경우 올바르게 적용되는지 확인")
    public void test_bucket_acl_canned()
    {
        var BucketName = GetNewBucketName();

        var Client = GetClient();
        Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicRead));
        var Response = Client.getBucketAcl(BucketName);

        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
        var User = new CanonicalGrantee(UserId);
        User.setDisplayName(DisplayName);

        var GetGrants = Response.getGrantsAsList();
        var MyGrants = new ArrayList<Grant>();
        MyGrants.add(new Grant(User, Permission.FullControl));
        MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
        
        Client.setBucketAcl(BucketName, CannedAccessControlList.Private);
        Response = Client.getBucketAcl(BucketName);
        GetGrants = Response.getGrantsAsList();
        
        MyGrants.clear();
        MyGrants.add(new Grant(User, Permission.FullControl));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
    }

    @Test
    @Tag("Bucket")
    @Tag("KSAN")
    //@Tag("[bucket_acl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가 올바른지 확인")
    public void test_bucket_acl_canned_publicreadwrite()
    {
        var BucketName = GetNewBucketName();

        var Client = GetClient();
        Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        var Response = Client.getBucketAcl(BucketName);

        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[bucket_acl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의 acl정보가 올바른지 확인")
    public void test_bucket_acl_canned_authenticatedread()
    {
        var BucketName = GetNewBucketName();

        var Client = GetClient();
        Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.AuthenticatedRead));
        var Response = Client.getBucketAcl(BucketName);

        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[object_acl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인")
    public void test_object_acl_default()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var Key = "foo";
        Client.putObject(BucketName, Key, "bar");
        var Response = Client.getObjectAcl(BucketName, Key);


        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
        var User = new CanonicalGrantee(UserId);
        User.setDisplayName(DisplayName);

        var GetGrants = Response.getGrantsAsList();
        var MyGrants = new ArrayList<Grant>();
        MyGrants.add(new Grant(User, Permission.FullControl));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
    }

    @Test
    @Tag("Object")
    @Tag("KSAN")
    //@Tag("[object_acl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인")
    public void test_object_acl_canned_during_create()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        var Key = "foo";
        Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead));
        var Response = Client.getObjectAcl(BucketName, Key);


        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[object_acl : public-read => object_acl : private] 권한을 public-read로 생성한 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인")
    public void test_object_acl_canned()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        var Key = "foo";
        Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead));
        var Response = Client.getObjectAcl(BucketName, Key);


        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
        var User = new CanonicalGrantee(UserId);
        User.setDisplayName(DisplayName);

        var GetGrants = Response.getGrantsAsList();
        var MyGrants = new ArrayList<Grant>();
        MyGrants.add(new Grant(User, Permission.FullControl));
        MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
        
        Client.setObjectAcl(BucketName, Key, CannedAccessControlList.Private);
        Response = Client.getObjectAcl(BucketName, Key);

        GetGrants = Response.getGrantsAsList();
        MyGrants.clear();
        MyGrants.add(new Grant(User, Permission.FullControl));
        CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
    }

    @Test
    @Tag("Object")
    @Tag("KSAN")
    //@Tag("[object_acl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지 확인")
    public void test_object_acl_canned_publicreadwrite()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);
        
        var Key = "foo";
        Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        var Response = Client.getObjectAcl(BucketName, Key);


        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[object_acl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의 acl정보가 올바른지 확인")
    public void test_object_acl_canned_authenticatedread()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        var Key = "foo";
        Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Metadata).withCannedAcl(CannedAccessControlList.AuthenticatedRead));
        var Response = Client.getObjectAcl(BucketName, Key);


        var DisplayName = Config.MainUser.DisplayName;
        var UserId = Config.MainUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[bucket_acl: public-read-write] [object_acl : public-read-write => object_acl : bucket-owner-read]" +  
    //"메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을 bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인")
    public void test_object_acl_canned_bucketownerread()
    {
        var BucketName = GetNewBucketName();
        var MainClient = GetClient();
        var AltClient = GetAltClient();
        var Key = "foo";

		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        MainClient.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        AltClient.putObject(BucketName, Key, "bar");

        var BucketACLResponse = MainClient.getBucketAcl(BucketName);
        var BucketOwnerID = BucketACLResponse.getOwner().getId();
        var BucketOwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();
        var OwnerUser = new CanonicalGrantee(BucketOwnerID);
        OwnerUser.setDisplayName(BucketOwnerDisplayName);

        AltClient.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Key), Metadata).withCannedAcl(CannedAccessControlList.BucketOwnerRead));
        var Response = AltClient.getObjectAcl(BucketName, Key);

        var AltDisplayName = Config.AltUser.DisplayName;
        var AltUserID = Config.AltUser.UserID;
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
    @Tag("KSAN")
    //@Tag("[bucket_acl: public-read-write] [object_acl : public-read-write => object_acl : bucket-owner-full-control] " +
    //"메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을 bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인")
    public void test_object_acl_canned_bucketownerfullcontrol()
    {
        var BucketName = GetNewBucketName();
        var MainClient = GetClient();
        var AltClient = GetAltClient();
        var Key = "foo";

		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        MainClient.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        AltClient.putObject(BucketName, Key, "bar");

        var BucketACLResponse = MainClient.getBucketAcl(BucketName);
        var BucketOwnerID = BucketACLResponse.getOwner().getId();
        var BucketOwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();
        var OwnerUser = new CanonicalGrantee(BucketOwnerID);
        OwnerUser.setDisplayName(BucketOwnerDisplayName);
        
        AltClient.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Key), Metadata).withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
        var Response = AltClient.getObjectAcl(BucketName, Key);

        var AltDisplayName = Config.AltUser.DisplayName;
        var AltUserID = Config.AltUser.UserID;
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
    //@Tag("[bucket_acl: public-read-write] " + 
    //"메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게 FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의 소유자가 유지되는지 확인")
    public void test_object_acl_full_control_verify_owner()
    {
        var BucketName = GetNewBucketName();
        var MainClient = GetClient();
        var AltClient = GetAltClient();
        var Key = "foo";

        MainClient.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        MainClient.putObject(BucketName, Key, "bar");

        var MainUserID = Config.MainUser.UserID;
        var MainDisplayName = Config.MainUser.DisplayName;
        var MainUser = new CanonicalGrantee(MainUserID);
        MainUser.setDisplayName(MainDisplayName);

        var AltUserID = Config.AltUser.UserID;
        var AltDisplayName = Config.AltUser.DisplayName;
        var AltUser = new CanonicalGrantee(AltUserID);
        AltUser.setDisplayName(AltDisplayName);


        var accessControlList = new AccessControlList();
        accessControlList.setOwner(new Owner(MainUserID, MainDisplayName));
        accessControlList.grantPermission(AltUser, Permission.FullControl);
        
        MainClient.setObjectAcl(BucketName, Key, accessControlList);
        accessControlList = new AccessControlList();
        accessControlList.setOwner(new Owner(MainUserID, MainDisplayName));
        accessControlList.grantPermission(AltUser, Permission.ReadAcp);

        AltClient.setObjectAcl(BucketName, Key, accessControlList);

        var Response = AltClient.getObjectAcl(BucketName, Key);
        assertEquals(MainUserID, Response.getOwner().getId());
    }

    @Test
    @Tag("ETag")
    //@Tag("[bucket_acl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인")
    public void test_object_acl_full_control_verify_attributes()
    {
        var BucketName = GetNewBucketName();
        var MainClient = GetClient();
        var Key = "foo";

        MainClient.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

        var Headers = new ObjectMetadata();
        Headers.addUserMetadata("x-amz-meta-foo", "bar");
        Headers.setContentType("text/plain");
        Headers.setContentLength(3);

        MainClient.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Headers));

        var Response = MainClient.getObject(BucketName, Key);
        var ContentType = Response.getObjectMetadata().getContentType();
        var ETag = Response.getObjectMetadata().getETag();

        var AltUserID = Config.AltUser.UserID;
        var AltDisplayName = Config.AltUser.DisplayName;
        var AltUser = new CanonicalGrantee(AltUserID);
        AltUser.setDisplayName(AltDisplayName);

        var accessControlList = AddObjectUserGrant(BucketName, Key, new Grant(AltUser, Permission.FullControl));

        MainClient.setObjectAcl(BucketName, Key, accessControlList);

        Response = MainClient.getObject(BucketName, Key);
        assertEquals(ContentType, Response.getObjectMetadata().getContentType());
        assertEquals(ETag, Response.getObjectMetadata().getETag());
    }

    @Test
    @Tag("Permission")
    //@Tag("[bucket_acl:private] 기본생성한 버킷에 priavte 설정이 가능한지 확인")
    public void test_bucket_acl_canned_private_to_private()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.setBucketAcl(BucketName, CannedAccessControlList.Private);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL")
    public void test_object_acl()
    {
        CheckObjectACL(Permission.FullControl);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE")
    public void test_object_acl_write()
    {
        CheckObjectACL(Permission.Write);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP")
    public void test_object_acl_writeacp()
    {
        CheckObjectACL(Permission.WriteAcp);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ")
    public void test_object_acl_read()
    {
        CheckObjectACL(Permission.Read);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP")
    public void test_object_acl_readacp()
    {
        CheckObjectACL(Permission.ReadAcp);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL")
    public void test_bucket_acl_grant_userid_fullcontrol()
    {
        var BucketName = BucketACLGrantUserid(Permission.FullControl);

        CheckBucketACLGrantCanRead(BucketName);
        CheckBucketACLGrantCanReadACP(BucketName);
        CheckBucketACLGrantCanWrite(BucketName);
        CheckBucketACLGrantCanWriteACP(BucketName);

        var Client = GetClient();

        var BucketACLResponse = Client.getBucketAcl(BucketName);
        var OwnerID = BucketACLResponse.getOwner().getId();
        var OwnerDisplayName = BucketACLResponse.getOwner().getDisplayName();

        var MainUserID = Config.MainUser.UserID;
        var MainDisplayName = Config.MainUser.DisplayName;

        assertEquals(MainUserID, OwnerID);
        if (!StringUtils.isBlank(Config.URL)) assertEquals(MainDisplayName, OwnerDisplayName);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ")
    public void test_bucket_acl_grant_userid_read()
    {
        var BucketName = BucketACLGrantUserid(Permission.Read);

        CheckBucketACLGrantCanRead(BucketName);
        CheckBucketACLGrantCantReadACP(BucketName);
        CheckBucketACLGrantCantWrite(BucketName);
        CheckBucketACLGrantCantWriteACP(BucketName);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP")
    public void test_bucket_acl_grant_userid_readacp()
    {
        var BucketName = BucketACLGrantUserid(Permission.ReadAcp);

        CheckBucketACLGrantCantRead(BucketName);
        CheckBucketACLGrantCanReadACP(BucketName);
        CheckBucketACLGrantCantWrite(BucketName);
        CheckBucketACLGrantCantWriteACP(BucketName);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE")
    public void test_bucket_acl_grant_userid_write()
    {
        var BucketName = BucketACLGrantUserid(Permission.Write);

        CheckBucketACLGrantCantRead(BucketName);
        CheckBucketACLGrantCantReadACP(BucketName);
        CheckBucketACLGrantCanWrite(BucketName);
        CheckBucketACLGrantCantWriteACP(BucketName);
    }

    @Test
    @Tag("Permission")
    @Tag("KSAN")
    //@Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP")
    public void test_bucket_acl_grant_userid_writeacp()
    {
        var BucketName = BucketACLGrantUserid(Permission.WriteAcp);

        CheckBucketACLGrantCantRead(BucketName);
        CheckBucketACLGrantCantReadACP(BucketName);
        CheckBucketACLGrantCantWrite(BucketName);
        CheckBucketACLGrantCanWriteACP(BucketName);
    }

    @Test
    @Tag("ERROR")
    //@Tag("버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인")
    public void test_bucket_acl_grant_nonexist_user()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        var BadUser = new CanonicalGrantee("_foo");
        var accessControlList = AddBucketUserGrant(BucketName, new Grant(BadUser, Permission.FullControl));

        var e = assertThrows(AmazonServiceException.class, () -> Client.setBucketAcl(BucketName, accessControlList));
        var StatusCode = e.getStatusCode();
        var ErrorCode = e.getErrorCode();
        assertEquals(400, StatusCode);
        assertEquals(MainData.InvalidArgument, ErrorCode);
    }

    @Test
    @Tag("ERROR")
    //@Tag("버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인")
    public void test_bucket_acl_no_grants()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";

        Client.putObject(BucketName, Key, "bar");
        var Response = Client.getBucketAcl(BucketName);
        var OldGrants = Response.getGrantsAsList();
        var Policy = new AccessControlList();
        Policy.setOwner(Response.getOwner());

        Client.setBucketAcl(BucketName, Policy);
        
        Client.getObject(BucketName, Key);

        assertThrows(AmazonServiceException.class, () -> Client.putObject(BucketName, Key, "A"));

        var Client2 = GetClient();
        Client2.getBucketAcl(BucketName);
        Client2.setBucketAcl(BucketName, CannedAccessControlList.Private);

        for(var MyGrant : OldGrants) Policy.grantAllPermissions(MyGrant);
        Client2.setBucketAcl(BucketName, Policy);
    }

    @Test
    @Tag("Header")
    //@Tag("오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인")
    public void test_object_header_acl_grants()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo_key";

		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(3);

        var AltUser = new CanonicalGrantee(Config.AltUser.UserID);
        AltUser.setDisplayName(Config.AltUser.DisplayName);

        var Grants = GetGrantList(null, null);

        Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("bar"), Metadata).withAccessControlList(Grants));
        var Response = Client.getObjectAcl(BucketName, Key);
        
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
    //@Tag("버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인")
    public void test_bucket_header_acl_grants()
    {
        var BucketName = GetNewBucketName();
        var Client = GetClient();
        
        var AltUser = new CanonicalGrantee(Config.AltUser.UserID);
        AltUser.setDisplayName(Config.AltUser.DisplayName);

        var Headers = GetGrantList(null, null);

        Client.createBucket(new CreateBucketRequest(BucketName).withAccessControlList(Headers));
        var Response = Client.getBucketAcl(BucketName);

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
    //@Tag("버킷의 소유자정보를 포함한 모든 acl정보를 삭제할 경우 올바르게 적용되는지 확인")
    public void test_bucket_acl_revoke_all()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();

        Client.putObject(BucketName, "foo", "bar");
        var Response = Client.getBucketAcl(BucketName);
        var OldGrants = Response.getGrantsAsList();

        var Policy = new AccessControlList();
        Policy.setOwner(Response.getOwner());

        Client.setBucketAcl(BucketName, Policy);
        Response = Client.getBucketAcl(BucketName);

        assertEquals(0, Response.getGrantsAsList().size());

        for (var Item : OldGrants) Policy.grantAllPermissions(Item);
        Client.setBucketAcl(BucketName, Policy);
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인")
    public void test_access_bucket_private_object_private()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

        var AltClient = GetAltClient();

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key1));
        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient.listObjects(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "barcontent"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjects_v2)")
    public void test_access_bucket_private_objectv2_private()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.Private);

        var AltClient = GetAltClient();

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key1));
        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient.listObjectsV2(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "barcontent"));

        var AltClient2 = GetAltClient();

        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인")
    public void test_access_bucket_private_object_publicread()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicRead);
        var AltClient = GetAltClient();
        var Response = AltClient.getObject(BucketName, Key1);
        
        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient3.listObjects(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인(ListObjects_v2)")
    public void test_access_bucket_private_objectv2_publicread()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicRead);
        var AltClient = GetAltClient();
        var Response = AltClient.getObject(BucketName, Key1);

        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient3.listObjectsV2(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private, public-read-write] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)")
    public void test_access_bucket_private_object_publicreadwrite()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite);
        var AltClient = GetAltClient();
        var Response = AltClient.getObject(BucketName, Key1);

        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient3.listObjects(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:private, object_acl:private, public-read-write] 메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인(ListObjects_v2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)")
    public void test_access_bucket_private_objectv2_publicreadwrite()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.Private, CannedAccessControlList.PublicReadWrite);
        var AltClient = GetAltClient();
        var Response = AltClient.getObject(BucketName, Key1);

        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient3.listObjectsV2(BucketName));
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read, object_acl:private] 메인유저가 public-read권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인")
    public void test_access_bucket_publicread_object_private()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.Private);
        var AltClient = GetAltClient();

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key1));
        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        var ObjList = GetKeys(AltClient3.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read, object_acl:public-read, private] 메인유저가 public-read권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인")
    public void test_access_bucket_publicread_object_publicread()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead);
        var AltClient = GetAltClient();

        var Response = AltClient.getObject(BucketName, Key1);
        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        var ObjList = GetKeys(AltClient3.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read, object_acl:public-read-wirte, private] 메인유저가 public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 수정불가)")
    public void test_access_bucket_publicread_object_publicreadwrite()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicRead, CannedAccessControlList.PublicReadWrite);
        var AltClient = GetAltClient();

        var Response = AltClient.getObject(BucketName, Key1);
        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);

        assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key1, "foooverwrite"));

        var AltClient2 = GetAltClient();
        assertThrows(AmazonServiceException.class, () -> AltClient2.getObject(BucketName, Key2));
        assertThrows(AmazonServiceException.class, () -> AltClient2.putObject(BucketName, Key2, "baroverwrite"));

        var AltClient3 = GetAltClient();
        var ObjList = GetKeys(AltClient3.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        assertThrows(AmazonServiceException.class, () -> AltClient3.putObject(BucketName, NewKey, "newcontent"));
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read-write, object_acl:private] 메인유저가 public-read-write권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 업로드는 가능하지만 다운로드 할 수 없음을 확인")
    public void test_access_bucket_publicreadwrite_object_private()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.Private);
        var AltClient = GetAltClient();

        
        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key1));
        AltClient.putObject(BucketName, Key1, "barcontent");

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key2));
        AltClient.putObject(BucketName, Key2, "baroverwrite");

        var ObjList = GetKeys(AltClient.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        AltClient.putObject(BucketName, NewKey, "newcontent");
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read-write, object_acl:public-read, private] 메인유저가 public-read-write권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 업로드, 다운로드 모두 가능함을 확인")
    public void test_access_bucket_publicreadwrite_object_publicread()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicRead);
        var AltClient = GetAltClient();

        var Response = AltClient.getObject(BucketName, Key1);
        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);
        AltClient.putObject(BucketName, Key1, "barcontent");

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key2));
        AltClient.putObject(BucketName, Key2, "baroverwrite");

        var ObjList = GetKeys(AltClient.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        AltClient.putObject(BucketName, NewKey, "newcontent");
    }

    @Test
    @Tag("Access")
    //@Tag("[bucket_acl:public-read-write, object_acl:public-read-write, private] 메인유저가 public-read-write권한으로 생성한 버킷에서
    // public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나 업로드, 다운로드 모두 가능함을 확인")
    public void test_access_bucket_publicreadwrite_object_publicreadwrite()
    {
        var Key1 = "foo";
        var Key2 = "bar";
        var NewKey = "new";
        var BucketName = SetupAccessTest(Key1, Key2, CannedAccessControlList.PublicReadWrite, CannedAccessControlList.PublicReadWrite);
        var AltClient = GetAltClient();

        var Response = AltClient.getObject(BucketName, Key1);
        var Body = GetBody(Response.getObjectContent());
        assertEquals("foocontent", Body);
        AltClient.putObject(BucketName, Key1, "foooverwrite");

        assertThrows(AmazonServiceException.class, () -> AltClient.getObject(BucketName, Key2));
        // assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, Key2, "baroverwrite"));
        AltClient.putObject(BucketName, Key2, "baroverwrite");

        var ObjList = GetKeys(AltClient.listObjects(BucketName).getObjectSummaries());
        assertEquals(new ArrayList<>(Arrays.asList(new String[] { Key2, Key1 })), ObjList);
        AltClient.putObject(BucketName, NewKey, "newcontent");
    }
}
