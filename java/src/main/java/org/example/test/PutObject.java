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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.ObjectLockLegalHold;
import com.amazonaws.services.s3.model.ObjectLockLegalHoldStatus;
import com.amazonaws.services.s3.model.ObjectLockMode;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;

public class PutObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("PutObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("PutObject End");
	}

	@Test
	@Tag("PUT")
	// 오브젝트가 올바르게 생성되는지 확인
	public void test_bucket_list_distinct() {
		var BucketName1 = GetNewBucket();
		var BucketName2 = GetNewBucket();
		var Client = GetClient();

		Client.putObject(BucketName1, "asdf", "str");

		var Response = Client.listObjects(BucketName2);
		assertEquals(0, Response.getObjectSummaries().size());
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷에 오브젝트 업로드할 경우 실패 확인
	public void test_object_write_to_nonexist_bucket() {
		var KeyName = "foo";
		var BucketName = "whatchutalkinboutwillis";
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.putObject(BucketName, KeyName, KeyName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchBucket, ErrorCode);
	}

	@Test
	@Tag("Metadata")
	// 0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인
	public void test_object_head_zero_bytes() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var KeyName = "foo";
		Client.putObject(BucketName, KeyName, "");

		var Response = Client.getObjectMetadata(BucketName, KeyName);
		assertEquals(0, Response.getContentLength());
	}

	@Test
	@Tag("Metadata")
	// 업로드한 오브젝트의 ETag가 올바른지 확인
	public void test_object_write_check_etag() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var Response = Client.putObject(BucketName, "foo", "bar");
		assertEquals("37b51d194a7513e45b56f6524f2d51f2", Response.getETag());
	}

	@Test
	@Tag(" CacheControl")
	// 캐시(시간)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_cache_control() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var KeyName = "foo";
		var Body = "bar";
		var CacheControl = "public, max-age=14400";
		var Metadata = new ObjectMetadata();
		Metadata.setCacheControl(CacheControl);
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Body.length());

		Client.putObject(BucketName, KeyName, CreateBody(Body), Metadata);

		var Response = Client.getObjectMetadata(BucketName, KeyName);
		assertEquals(CacheControl, Response.getCacheControl());

		var Result = Client.getObject(BucketName, KeyName);
		assertEquals(Body, GetBody(Result.getObjectContent()));
	}

	@Test
	@Disabled("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	@Tag("Expires")
	// 캐시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_expires() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var KeyName = "foo";
		var Body = "bar";
		var Expires = GetTimeToAddSeconds(6000);
		var Metadata = new ObjectMetadata();
		Metadata.setExpirationTime(Expires);
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Body.length());

		Client.putObject(BucketName, KeyName, CreateBody(Body), Metadata);

		var Response = Client.getObjectMetadata(BucketName, KeyName);
		assertEquals(Expires, Response.getExpirationTime());
	}

	@Test
	@Tag("Update")
	// 오브젝트의 기본 작업을 모드 올바르게 할 수 있는지 확인(read, write, update, delete)
	public void test_object_write_read_update_read_delete() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var KeyName = "foo";
		var Body = "bar";

		// Write
		Client.putObject(BucketName, KeyName, Body);

		// Read
		var GetResponse = Client.getObject(BucketName, KeyName);
		var ResponseBody = GetBody(GetResponse.getObjectContent());
		assertEquals(Body, ResponseBody);

		// Update
		var Body2 = "soup";
		Client.putObject(BucketName, KeyName, Body2);

		// Read
		GetResponse = Client.getObject(BucketName, KeyName);
		ResponseBody = GetBody(GetResponse.getObjectContent());
		assertEquals(Body2, ResponseBody);

		// Delete
		Client.deleteObject(BucketName, KeyName);
	}

	@Test
	@Tag("Metadata")
	// 오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_good() {
		var MyMeta = "mymeta";
		var got = SetGetMetadata(MyMeta, null);
		assertEquals(MyMeta, got);
	}

	@Test
	@Tag("Metadata")
	// 오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_empty() {
		var got = SetGetMetadata("", null);
		assertEquals("", got);
	}

	@Test
	@Tag("Metadata")
	// 메타 데이터 업데이트가 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_overwrite_to_empty() {
		var BucketName = GetNewBucket();

		var MyMeta = "oldmata";
		var got = SetGetMetadata(MyMeta, BucketName);
		assertEquals(MyMeta, got);

		got = SetGetMetadata("", BucketName);
		assertEquals("", got);
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// 메타데이터에 올바르지 않는 문자열[EOF(\x04)를 사용할 경우 실패 확인
	public void test_object_set_get_non_utf8_metadata() {
		var Metadata = "\nmymeta";
		var e = SetGetMetadataUnreadable(Metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨앞에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_prefix() {
		var Metadata = "\nw";
		var e = SetGetMetadataUnreadable(Metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨뒤에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_suffix() {
		var Metadata = "h\n";
		var e = SetGetMetadataUnreadable(Metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Tag("Metadata")
	// 오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인
	public void test_object_metadata_replaced_on_put() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var KeyName = "foo";
		var Body = "bar";

		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-mata1", "bar");
		MetadataList.setContentType("text/plain");
		MetadataList.setContentLength(Body.length());

		Client.putObject(BucketName, KeyName, CreateBody(Body), MetadataList);
		Client.putObject(BucketName, KeyName, Body);

		var Response = Client.getObject(BucketName, KeyName);
		var got = Response.getObjectMetadata().getUserMetadata();
		assertEquals(0, got.size());
	}

	@Test
	@Tag("Incoding")
	// body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인
	public void test_object_write_file() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var KeyName = "foo";
		var Data_str = "bar";
		var Data = new String(Data_str.getBytes(), StandardCharsets.US_ASCII);

		Client.putObject(BucketName, KeyName, Data);

		var Response = Client.getObject(BucketName, KeyName);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data_str, Body);
	}

	@Test
	@Tag("SpecialKeyName")
	// 오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인
	public void test_bucket_create_special_key_names() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { " ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__", }));

		var BucketName = CreateObjects(KeyNames);

		var ObjectList = GetObjectList(BucketName, null);

		var Client = GetClient();

		for (var Name : KeyNames) {
			assertTrue(ObjectList.contains(Name));
			var Response = Client.getObject(BucketName, Name);
			var Body = GetBody(Response.getObjectContent());
			assertEquals(Name, Body);
			Client.setObjectAcl(BucketName, Name, CannedAccessControlList.Private);
		}
	}

	@Test
	@Tag("SpecialKeyName")
	// [_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지 확인
	public void test_bucket_list_special_prefix() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd" }));

		var BucketName = CreateObjects(KeyNames);

		var ObjectList = GetObjectList(BucketName, null);
		assertEquals(5, ObjectList.size());

		ObjectList = GetObjectList(BucketName, "_bla/");
		assertEquals(4, ObjectList.size());
	}

	@Test
	@Tag("Lock")
	// [버킷의 Lock옵션을 활성화] LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지 메타데이터를 통해
	// 확인
	public void test_object_lock_uploading_obj() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		var Body = "abc";
		var MD5 = Utils.GetMD5(Body);
		var MyDay = Calendar.getInstance();
		MyDay.set(2030, 1, 1, 0, 0, 0);

		var Metadata = new ObjectMetadata();
		Metadata.setContentMD5(MD5);
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Body.length());

		var PutResponse = Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Body), Metadata)
				.withObjectLockMode(ObjectLockMode.GOVERNANCE).withObjectLockRetainUntilDate(MyDay.getTime())
				.withObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON));

		var Response = Client.getObjectMetadata(BucketName, Key);
		assertEquals(ObjectLockMode.GOVERNANCE.toString(), Response.getObjectLockMode());
		assertEquals(MyDay.getTime(), Response.getObjectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), Response.getObjectLockLegalHoldStatus());

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		Client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(BucketName).withKey(Key).withLegalHold(LegalHold));
		Client.deleteVersion(new DeleteVersionRequest(BucketName, Key, PutResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Space")
	// 오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_infix_space() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "a a/", "b b/f1", "c/f 2", "d d/f 3" }));
		var BucketName = CreateObjectsToBody(KeyNames, "");
		var Client = GetClient();

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Space")
	// 오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_suffix_space() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "a /", "b /f1", "c/f2 ", "d /f3 " }));
		var BucketName = CreateObjectsToBody(KeyNames, "");
		var Client = GetClient();

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_2() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsToBodyV2(KeyNames, "");
		var Client = GetClientV2();

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion4] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_4() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsToBodyV4(KeyNames, "", true);
		var Client = GetClientV4(true);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion2] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_2() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsV2(KeyNames);
		var Client = GetClientV2();

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion4] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_4() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsV4(KeyNames, true);
		var Client = GetClientV4(true);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsV4(KeyNames, true);
		var Client = GetClientV4(true);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true]
	// 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding_and_disable_payload_signing() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsHttps(KeyNames, true, true);
		var Client = GetClientV4(true);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsV4(KeyNames, false);
		var Client = GetClientV4(false);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true]
	// 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding_and_disable_payload_signing() {
		var KeyNames = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var BucketName = CreateObjectsHttps(KeyNames, false, true);
		var Client = GetClientV4(false);

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());

		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Directory")
	// 폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인
	public void test_put_object_dir_and_file() {
		// file first
		var BucketName = GetNewBucket();
		var ObjectName = "aaa";
		var DirectoryName = "aaa/";
		var Client = GetClient();

		Client.putObject(BucketName, ObjectName, ObjectName);
		Client.putObject(BucketName, DirectoryName, "");

		var Response = Client.listObjects(BucketName);
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(2, Keys.size());

		// dir first
		var BucketName2 = GetNewBucket();

		Client.putObject(BucketName2, DirectoryName, "");
		Client.putObject(BucketName2, ObjectName, ObjectName);

		Response = Client.listObjects(BucketName2);
		Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(2, Keys.size());

		// etc
		var BucketName3 = GetNewBucket();
		var NewObjectName = "aaa/bbb/ccc";

		Client.putObject(BucketName3, ObjectName, ObjectName);
		Client.putObject(BucketName3, NewObjectName, NewObjectName);

		Response = Client.listObjects(BucketName3);
		Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(2, Keys.size());
	}

	@Test
	@Tag("PUT")
	// 오브젝트를 여러번 업로드 했을때 올바르게 반영되는지 확인
	public void test_object_twice() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var KeyName = "temp";
		var Dummy1 = Utils.RandomTextToLong(10 * MainData.KB);
		var Dummy2 = Utils.RandomTextToLong(1 * MainData.MB);

		Client.putObject(BucketName, KeyName, Dummy1);
		Client.putObject(BucketName, KeyName, Dummy2);

		var Response = Client.getObject(BucketName, KeyName);
		var Body = GetBody(Response.getObjectContent());

		assertEquals(Dummy2.length(), Body.length());
		assertTrue(Dummy2.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PUT")
	// 오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인
	public void test_object_emoji() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "test❤🍕🍔🚗";

		Client.putObject(BucketName, "asdf", Key);

		var Response = Client.listObjects(BucketName);
		assertEquals(1, Response.getObjectSummaries().size());
	}

	@Test
	@Tag("ACL")
	// acl 정보를 포함하여 업로드 가능한지 확인
	public void test_object_in_acl() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";
		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		
		var AltUserID = Config.AltUser.UserID;
		var AltDisplayName = Config.AltUser.DisplayName;

		var ACL = new AccessControlList();
		ACL.setOwner(new Owner(AltUserID, AltDisplayName));

		var AltUser = new CanonicalGrantee(AltUserID);
		AltUser.setDisplayName(AltDisplayName);
		ACL.grantPermission(AltUser, Permission.FullControl);

		var Request = new PutObjectRequest(BucketName, Key, CreateBody(Key), Metadata);
		Request.setAccessControlList(ACL);

		Client.putObject(Request);
	}
}
