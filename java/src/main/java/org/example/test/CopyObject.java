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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.MetadataDirective;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class CopyObject extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("CopyObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("CopyObject End");
	}

	@Test
	@Tag("Check")
	//오브젝트의 크기가 0일때 복사가 가능한지 확인
	public void test_object_copy_zero_size()
	{
		var Key = "foo123bar";
		var NewKey = "bar321foo";
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { Key })));
		var Client = GetClient();

		Client.putObject(BucketName, Key, "");

		Client.copyObject(BucketName, Key, BucketName, NewKey);

		var Response = Client.getObject(BucketName, NewKey);
		assertEquals(0, Response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Check")
	//동일한 버킷에서 오브젝트 복사가 가능한지 확인
	public void test_object_copy_same_bucket()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo123bar";
		var NewKey = "bar321foo";

		Client.putObject(BucketName, Key, "foo");

		Client.copyObject(BucketName, Key, BucketName, NewKey);

		var Response = Client.getObject(BucketName, NewKey);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foo", Body);
	}

	@Test
	@Tag("ContentType")
	//ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인
	public void test_object_copy_verify_contenttype()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo123bar";
		var NewKey = "bar321foo";
		var ContentType = "text/bla";
		var MetaData = new ObjectMetadata();
		MetaData.setContentType(ContentType);
		MetaData.setContentLength(3);

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody("foo"), MetaData));
		Client.copyObject(BucketName, Key, BucketName, NewKey);

		var Response = Client.getObject(BucketName, NewKey);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foo", Body);
		var ResponseContentType = Response.getObjectMetadata().getContentType();
		assertEquals(ContentType, ResponseContentType);
	}

	@Test
	@Tag("OverWrite")
	//복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인
	public void test_object_copy_to_itself()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo123bar";

		Client.putObject(BucketName, Key, "");

		var e = assertThrows(AmazonServiceException.class, () -> Client.copyObject(BucketName, Key, BucketName, Key));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidRequest, ErrorCode);
	}

	@Test
	@Tag("OverWrite")
	//복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	public void test_object_copy_to_itself_with_metadata()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo123bar";

		Client.putObject(BucketName, Key, "foo");

		var MetaData = new ObjectMetadata();
		MetaData.addUserMetadata("foo", "bar");

		Client.copyObject(new CopyObjectRequest(BucketName, Key, BucketName, Key).withNewObjectMetadata(MetaData).withMetadataDirective(MetadataDirective.REPLACE));
		var Response = Client.getObject(BucketName, Key);

		assertEquals(MetaData.getUserMetadata(), Response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Check")
	//다른 버킷으로 오브젝트 복사가 가능한지 확인
	public void test_object_copy_diff_bucket()
	{
		var BucketName1 = GetNewBucket();
		var BucketName2 = GetNewBucket();

		var Key1 = "foo123bar";
		var Key2 = "bar321foo";

		var Client = GetClient();
		Client.putObject(BucketName1, Key1, "foo");

		Client.copyObject(BucketName1, Key1, BucketName2, Key2);

		var Response = Client.getObject(BucketName2, Key2);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("foo", Body);
	}

	@Test
	@Tag("Check")
	//[bucket1:created main user, object:created main user / bucket2:created sub user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인
	public void test_object_copy_not_owned_bucket()
	{
		var Client = GetClient();
		var AltClient = GetAltClient();
		var BucketName1 = GetNewBucketName();
		var BucketName2 = GetNewBucketName();

		Client.createBucket(BucketName1);
		AltClient.createBucket(BucketName2);

		var Key1 = "foo123bar";
		var Key2 = "bar321foo";

		Client.putObject(BucketName1, Key1, "foo");

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.copyObject(BucketName1, Key1, BucketName2, Key2));
		var StatusCode = e.getStatusCode();

		assertEquals(403, StatusCode);
		AltClient.deleteBucket(BucketName2);
		DeleteBucketList(BucketName2);
	}

	@Test
	@Tag("Check")
	//[bucket_acl = main:full control,sub : full control | object_acl = default] 서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인
	public void test_object_copy_not_owned_object_bucket()
	{
		var Client = GetClient();
		var AltClient = GetAltClient();
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		var Key1 = "foo123bar";
		var Key2 = "bar321foo";

		Client.putObject(BucketName, Key1, "foo");

		var AltUserID = Config.AltUser.UserID;
		var myGrant = new Grant(new CanonicalGrantee(AltUserID), Permission.FullControl);
		var Grants = AddObjectUserGrant(BucketName, Key1, myGrant);
		Client.setObjectAcl(BucketName, Key1, Grants);

		Grants = AddBucketUserGrant(BucketName, myGrant);
		Client.setBucketAcl(BucketName, Grants);

		AltClient.getObject(BucketName, Key1);

		AltClient.copyObject(BucketName, Key1, BucketName, Key2);
	}

	@Test
	@Tag("OverWrite")
	//권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인
	public void test_object_copy_canned_acl()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var AltClient = GetAltClient();
		var Key1 = "foo123bar";
		var Key2 = "bar321foo";

		Client.putObject(BucketName, Key1, "foo");

		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName, Key2).withCannedAccessControlList(CannedAccessControlList.PublicRead));
		AltClient.getObject(BucketName, Key2);

		var MetaData = new ObjectMetadata();
		MetaData.addUserMetadata("x-amz-meta-abc", "def");


		Client.copyObject(new CopyObjectRequest(BucketName, Key2, BucketName, Key1).withCannedAccessControlList(CannedAccessControlList.PublicRead).withNewObjectMetadata(MetaData).withMetadataDirective(MetadataDirective.REPLACE));
		AltClient.getObject(BucketName, Key1);
	}

	@Test
	@Tag("Check")
	//크고 작은 용량의 오브젝트가 복사되는지 확인
	public void test_object_copy_retaining_metadata()
	{
		for (var size : new int[] { 3, 1024 * 1024 })
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var ContentType = "audio/ogg";

			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			var MetaData = new ObjectMetadata();
			MetaData.addUserMetadata("x-amz-meta-key1", "value1");
			MetaData.addUserMetadata("x-amz-meta-key2", "value2");
			MetaData.setContentType(ContentType);
			MetaData.setContentLength(size);

			Client.putObject(new PutObjectRequest(BucketName, Key1, CreateBody(Utils.RandomTextToLong(size)), MetaData));
			Client.copyObject(BucketName, Key1, BucketName, Key2);

			var Response = Client.getObject(BucketName, Key2);
			assertEquals(ContentType, Response.getObjectMetadata().getContentType());
			assertEquals(MetaData.getUserMetadata(), Response.getObjectMetadata().getUserMetadata());
			assertEquals(size, Response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("Check")
	//크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인
	public void test_object_copy_replacing_metadata()
	{
		for (var size : new int[] { 3, 1024 * 1024 })
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var ContentType = "audio/ogg";

			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			var MetaData = new ObjectMetadata();
			MetaData.addUserMetadata("x-amz-meta-key1", "value1");
			MetaData.addUserMetadata("x-amz-meta-key2", "value2");
			MetaData.setContentType(ContentType);
			MetaData.setContentLength(size);

			Client.putObject(new PutObjectRequest(BucketName, Key1, CreateBody(Utils.RandomTextToLong(size)), MetaData));

			MetaData = new ObjectMetadata();
			MetaData.addUserMetadata("x-amz-meta-key1", "value1");
			MetaData.addUserMetadata("x-amz-meta-key2", "value2");
			MetaData.setContentType(ContentType);

			Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName, Key2).withNewObjectMetadata(MetaData).withMetadataDirective(MetadataDirective.REPLACE));

			var Response = Client.getObject(BucketName, Key2);
			assertEquals(ContentType, Response.getObjectMetadata().getContentType());
			assertEquals(MetaData.getUserMetadata(), Response.getObjectMetadata().getUserMetadata());
			assertEquals(size, Response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("ERROR")
	//존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인
	public void test_object_copy_bucket_not_found()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.copyObject(BucketName + "-fake", "foo123bar", BucketName, "bar321foo"));
		var StatusCode = e.getStatusCode();
		assertEquals(404, StatusCode);
	}

	@Test
	@Tag("ERROR")
	//존재하지않는 오브젝트 복사 실패 확인
	public void test_object_copy_key_not_found()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.copyObject(BucketName, "foo123bar", BucketName, "bar321foo"));
		var StatusCode = e.getStatusCode();
		assertEquals(404, StatusCode);
	}

	@Test
	@Tag("Version")
	//버저닝된 오브젝트 복사 확인
	public void test_object_copy_versioned_bucket()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var Size = 1 * 5;
		var Data = Utils.RandomTextToLong(Size);
		var Key1 = "foo123bar";
		var Key2 = "bar321foo";
		var Key3 = "bar321foo2";
		Client.putObject(BucketName, Key1, Data);

		var Response = Client.getObject(BucketName, Key1);
		var VersionID = Response.getObjectMetadata().getVersionId();

		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName, Key2).withSourceVersionId(VersionID));
		Response = Client.getObject(BucketName, Key2);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());

		var VersionID2 = Response.getObjectMetadata().getVersionId();
		Client.copyObject(new CopyObjectRequest(BucketName, Key2, BucketName, Key3).withSourceVersionId(VersionID2));
		Response = Client.getObject(BucketName, Key3);
		Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());

		var BucketName2 = GetNewBucket();
		CheckConfigureVersioningRetry(BucketName2, BucketVersioningConfiguration.ENABLED);
		var Key4 = "bar321foo3";
		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName2, Key4).withSourceVersionId(VersionID));
		Response = Client.getObject(BucketName2, Key4);
		Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());

		var BucketName3 = GetNewBucket();
		CheckConfigureVersioningRetry(BucketName3, BucketVersioningConfiguration.ENABLED);
		var Key5 = "bar321foo4";
		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName3, Key5).withSourceVersionId(VersionID));
		Response = Client.getObject(BucketName3, Key5);
		Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());

		var Key6 = "foo123bar2";
		Client.copyObject(BucketName3, Key5, BucketName, Key6);
		Response = Client.getObject(BucketName, Key6);
		Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Version")
	//[버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인
	public void test_object_copy_versioned_url_encoding()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var SourceKey = "foo?bar";

		Client.putObject(BucketName, SourceKey, "");
		var Response = Client.getObject(BucketName, SourceKey);

		var DstKey = "bar&foo";
		Client.copyObject(new CopyObjectRequest(BucketName, SourceKey, BucketName, DstKey).withSourceVersionId(Response.getObjectMetadata().getVersionId()));
		Client.getObject(BucketName, DstKey);
	}

	@Test
	@Tag("Multipart")
	//[버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인
	public void test_object_copy_versioning_multipart_upload()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var Size = 50 * MainData.MB;
		var Key1 = "srcmultipart";
		var ContentType = "text/bla";

		var Key1MetaData = new ObjectMetadata();
		Key1MetaData.addUserMetadata("x-amz-meta-foo", "bar");
		Key1MetaData.setContentType(ContentType);

		var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size, Key1MetaData);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key1, UploadData.UploadId, UploadData.Parts));

		var Response = Client.getObjectMetadata(BucketName, Key1);
		var Key1Size = Response.getContentLength();
		var VersionID = Response.getVersionId();

		var Key2 = "dstmultipart";
		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName, Key2).withSourceVersionId(VersionID));
		Response = Client.getObjectMetadata(BucketName, Key2);
		var VersionID2 = Response.getVersionId();
		assertEquals(Key1Size, Response.getContentLength());
		assertEquals(Key1MetaData.getUserMetadata(), Response.getUserMetadata());
		assertEquals(ContentType, Response.getContentType());
		CheckContentUsingRange(BucketName, Key2, UploadData.GetBody(), MainData.MB);


		var Key3 = "dstmultipart2";
		Client.copyObject(new CopyObjectRequest(BucketName, Key2, BucketName, Key3).withSourceVersionId(VersionID2));
		Response = Client.getObjectMetadata(BucketName, Key3);
		assertEquals(Key1Size, Response.getContentLength());
		assertEquals(Key1MetaData.getUserMetadata(), Response.getUserMetadata());
		assertEquals(ContentType, Response.getContentType());
		CheckContentUsingRange(BucketName, Key3, UploadData.GetBody(), MainData.MB);

		var BucketName2 = GetNewBucket();
		CheckConfigureVersioningRetry(BucketName2, BucketVersioningConfiguration.ENABLED);
		var Key4 = "dstmultipart3";
		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName2, Key4).withSourceVersionId(VersionID));
		Response = Client.getObjectMetadata(BucketName2, Key4);
		assertEquals(Key1Size, Response.getContentLength());
		assertEquals(Key1MetaData.getUserMetadata(), Response.getUserMetadata());
		assertEquals(ContentType, Response.getContentType());
		CheckContentUsingRange(BucketName2, Key4, UploadData.GetBody(), MainData.MB);

		var BucketName3 = GetNewBucket();
		CheckConfigureVersioningRetry(BucketName3, BucketVersioningConfiguration.ENABLED);
		var Key5 = "dstmultipart4";
		Client.copyObject(new CopyObjectRequest(BucketName, Key1, BucketName3, Key5).withSourceVersionId(VersionID));
		Response = Client.getObjectMetadata(BucketName3, Key5);
		assertEquals(Key1Size, Response.getContentLength());
		assertEquals(Key1MetaData.getUserMetadata(), Response.getUserMetadata());
		assertEquals(ContentType, Response.getContentType());
		CheckContentUsingRange(BucketName3, Key5, UploadData.GetBody(), MainData.MB);

		var Key6 = "dstmultipart5";
		Client.copyObject(BucketName3, Key5, BucketName, Key6);
		Response = Client.getObjectMetadata(BucketName, Key6);
		assertEquals(Key1Size, Response.getContentLength());
		assertEquals(Key1MetaData.getUserMetadata(), Response.getUserMetadata());
		assertEquals(ContentType, Response.getContentType());
		CheckContentUsingRange(BucketName, Key6, UploadData.GetBody(), MainData.MB);
	}

	@Test
	@Tag("If Match")
	//ifmatch 값을 추가하여 오브젝트를 복사할 경우 성공확인
	public void test_copy_object_ifmatch_good()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var PutResponse = Client.putObject(BucketName, "foo", "bar");

		Client.copyObject(new CopyObjectRequest(BucketName, "foo", BucketName, "bar").withMatchingETagConstraint(PutResponse.getETag()));
		var GetResponse = Client.getObject(BucketName, "bar");
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("If Match")
	//ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인
	public void test_copy_object_ifmatch_failed()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		Client.putObject(BucketName, "foo", "bar");

		var Result = Client.copyObject(new CopyObjectRequest(BucketName, "foo", BucketName, "bar").withMatchingETagConstraint("ABC"));
		assertNull(Result);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_nor_src_to_nor_bucket_and_obj()
	{
		TestObjectCopy(false, false, false, false, 1024);
		TestObjectCopy(false, false, false, false, 256 * 1024);
		TestObjectCopy(false, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_nor_src_to_nor_bucket_encryption_obj()
	{
		TestObjectCopy(false, false, false, true, 1024);
		TestObjectCopy(false, false, false, true, 256 * 1024);
		TestObjectCopy(false, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_nor_src_to_encryption_bucket_nor_obj()
	{
		TestObjectCopy(false, false, true, false, 1024);
		TestObjectCopy(false, false, true, false, 256 * 1024);
		TestObjectCopy(false, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_nor_src_to_encryption_bucket_and_obj()
	{
		TestObjectCopy(false, false, true, true, 1024);
		TestObjectCopy(false, false, true, true, 256 * 1024);
		TestObjectCopy(false, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_src_to_nor_bucket_and_obj()
	{
		TestObjectCopy(true, false, false, false, 1024);
		TestObjectCopy(true, false, false, false, 256 * 1024);
		TestObjectCopy(true, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_src_to_nor_bucket_encryption_obj()
	{
		TestObjectCopy(true, false, false, true, 1024);
		TestObjectCopy(true, false, false, true, 256 * 1024);
		TestObjectCopy(true, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_src_to_encryption_bucket_nor_obj()
	{
		TestObjectCopy(true, false, true, false, 1024);
		TestObjectCopy(true, false, true, false, 256 * 1024);
		TestObjectCopy(true, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_src_to_encryption_bucket_and_obj()
	{
		TestObjectCopy(true, false, true, true, 1024);
		TestObjectCopy(true, false, true, true, 256 * 1024);
		TestObjectCopy(true, false, true, true, 1024 * 1024);
	}


	@Test
	@Tag("encryption")
	//[source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj()
	{
		TestObjectCopy(false, true, false, false, 1024);
		TestObjectCopy(false, true, false, false, 256 * 1024);
		TestObjectCopy(false, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj()
	{
		TestObjectCopy(false, true, false, true, 1024);
		TestObjectCopy(false, true, false, true, 256 * 1024);
		TestObjectCopy(false, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj()
	{
		TestObjectCopy(false, true, true, false, 1024);
		TestObjectCopy(false, true, true, false, 256 * 1024);
		TestObjectCopy(false, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj()
	{
		TestObjectCopy(false, true, true, true, 1024);
		TestObjectCopy(false, true, true, true, 256 * 1024);
		TestObjectCopy(false, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj()
	{
		TestObjectCopy(true, true, false, false, 1024);
		TestObjectCopy(true, true, false, false, 256 * 1024);
		TestObjectCopy(true, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj()
	{
		TestObjectCopy(true, true, false, true, 1024);
		TestObjectCopy(true, true, false, true, 256 * 1024);
		TestObjectCopy(true, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj()
	{
		TestObjectCopy(true, true, true, false, 1024);
		TestObjectCopy(true, true, true, false, 256 * 1024);
		TestObjectCopy(true, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인
	public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj()
	{
		TestObjectCopy(true, true, true, true, 1024);
		TestObjectCopy(true, true, true, true, 256 * 1024);
		TestObjectCopy(true, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	//일반 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void test_copy_to_normal_source()
	{
		var Size1 = 1024;
		var Size2 = 256 * 1024;
		var Size3 = 1024 * 1024;

		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size1);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size2);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size3);

		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size1);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size2);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size3);

		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size1);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size2);
		TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size3);
	}

	@Test
	@Tag("encryption")
	//SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void test_copy_to_sse_s3_source()
	{
		var Size1 = 1024;
		var Size2 = 256 * 1024;
		var Size3 = 1024 * 1024;

		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size1);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size2);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size3);

		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size1);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size2);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size3);

		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size1);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size2);
		TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size3);
	}

	@Test
	@Tag("encryption")
	//SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void test_copy_to_sse_c_source()
	{
		var Size1 = 1024;
		var Size2 = 256 * 1024;
		var Size3 = 1024 * 1024;

		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size1);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size2);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size3);

		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size1);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size2);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size3);

		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size1);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size2);
		TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size3);
	}

}
