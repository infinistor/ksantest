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
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.ObjectLockLegalHold;
import com.amazonaws.services.s3.model.ObjectLockLegalHoldStatus;
import com.amazonaws.services.s3.model.ObjectLockMode;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;

public class PutObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("PutObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("PutObject End");
	}

	@Test
	@Tag("PUT")
	// 오브젝트가 올바르게 생성되는지 확인
	public void test_bucket_list_distinct() {
		var bucketName1 = getNewBucket();
		var bucketName2 = getNewBucket();
		var client = getClient();

		client.putObject(bucketName1, "asdf", "str");

		var response = client.listObjects(bucketName2);
		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷에 오브젝트 업로드할 경우 실패 확인
	public void test_object_write_to_nonexist_bucket() {
		var key = "foo";
		var bucketName = "whatchutalkinboutwillis";
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, key));

		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchBucket, errorCode);
	}

	@Test
	@Tag("metadata")
	// 0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인
	public void test_object_head_zero_bytes() {
		var bucketName = getNewBucket();
		var client = getClient();

		var key = "foo";
		client.putObject(bucketName, key, "");

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(0, response.getContentLength());
	}

	@Test
	@Tag("metadata")
	// 업로드한 오브젝트의 ETag가 올바른지 확인
	public void test_object_write_check_etag() {
		var bucketName = getNewBucket();
		var client = getClient();

		var response = client.putObject(bucketName, "foo", "bar");
		assertEquals("37b51d194a7513e45b56f6524f2d51f2", response.getETag());
	}

	@Test
	@Tag("cacheControl")
	// 캐시(시간)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_cache_control() {
		var bucketName = getNewBucket();
		var client = getClient();

		var key = "foo";
		var body = "bar";
		var cacheControl = "public, max-age=14400";
		var metadata = new ObjectMetadata();
		metadata.setCacheControl(cacheControl);
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(cacheControl, response.getCacheControl());

		var Result = client.getObject(bucketName, key);
		assertEquals(body, getBody(Result.getObjectContent()));
	}

	@Test
	@Disabled("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	@Tag("Expires")
	// 캐시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_expires() {
		var bucketName = getNewBucket();
		var client = getClient();

		var key = "foo";
		var body = "bar";
		var Expires = getTimeToAddSeconds(6000);
		var metadata = new ObjectMetadata();
		metadata.setExpirationTime(Expires);
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(Expires, response.getExpirationTime());
	}

	@Test
	@Tag("Update")
	// 오브젝트의 기본 작업을 모드 올바르게 할 수 있는지 확인(read, write, update, delete)
	public void test_object_write_read_update_read_delete() {
		var bucketName = getNewBucket();
		var client = getClient();

		var key = "foo";
		var body = "bar";

		// Write
		client.putObject(bucketName, key, body);

		// Read
		var getResponse = client.getObject(bucketName, key);
		var responseBody = getBody(getResponse.getObjectContent());
		assertEquals(body, responseBody);

		// Update
		var body2 = "soup";
		client.putObject(bucketName, key, body2);

		// Read
		getResponse = client.getObject(bucketName, key);
		responseBody = getBody(getResponse.getObjectContent());
		assertEquals(body2, responseBody);

		// Delete
		client.deleteObject(bucketName, key);
	}

	@Test
	@Tag("metadata")
	// 오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_good() {
		var value = "my";
		var got = setGetMetadata(value, null);
		assertEquals(value, got);
	}

	@Test
	@Tag("metadata")
	// 오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_empty() {
		var got = setGetMetadata("", null);
		assertEquals("", got);
	}

	@Test
	@Tag("metadata")
	// 메타 데이터 업데이트가 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_overwrite_to_empty() {
		var bucketName = getNewBucket();

		var MyMeta = "oldmata";
		var got = setGetMetadata(MyMeta, bucketName);
		assertEquals(MyMeta, got);

		got = setGetMetadata("", bucketName);
		assertEquals("", got);
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	// 메타데이터에 올바르지 않는 문자열[EOF(\x04)를 사용할 경우 실패 확인
	public void test_object_set_get_non_utf8_metadata() {
		var metadata = "\nmymeta";
		var e = SetGetMetadataUnreadable(metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨앞에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_prefix() {
		var metadata = "\nw";
		var e = SetGetMetadataUnreadable(metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨뒤에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_suffix() {
		var metadata = "h\n";
		var e = SetGetMetadataUnreadable(metadata, null);
		assertTrue(ErrorCheck(e.getStatusCode()));
	}

	@Test
	@Tag("metadata")
	// 오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인
	public void test_object_metadata_replaced_on_put() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var body = "bar";

		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-mata1", "bar");
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);
		client.putObject(bucketName, key, body);

		var response = client.getObject(bucketName, key);
		var got = response.getObjectMetadata().getUserMetadata();
		assertEquals(0, got.size());
	}

	@Test
	@Tag("Incoding")
	// body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인
	public void test_object_write_file() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var data_str = "bar";
		var data = new String(data_str.getBytes(), StandardCharsets.US_ASCII);

		client.putObject(bucketName, key, data);

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(data_str, body);
	}

	@Test
	@Tag("SpecialKeyName")
	// 오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인
	public void test_bucket_create_special_key_names() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "-", "_", ".", "'", "("+")", "&", "$", "@", "=", ";", "/", ":", "+", "  ", ",", "?", "\\", "{"+"}", "^", "%", "`", "["+"]", "<" + ">", "~", "#", "|"}));
				
		var bucketName = createObjects(keys);

		var objects = getObjectList(bucketName, null);

		var client = getClient();

		for (var key : keys) {
			assertTrue(objects.contains(key));
			var response = client.getObject(bucketName, key);
			var body = getBody(response.getObjectContent());
			assertEquals(key, body);
			client.setObjectAcl(bucketName, key, CannedAccessControlList.Private);
		}
	}

	@Test
	@Tag("SpecialKeyName")
	// [_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지 확인
	public void test_bucket_list_special_prefix() {
		var keys = new ArrayList<>(Arrays.asList(new String[] { "_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd" }));

		var bucketName = createObjects(keys);

		var objects = getObjectList(bucketName, null);
		assertEquals(5, objects.size());

		objects = getObjectList(bucketName, "_bla/");
		assertEquals(4, objects.size());
	}

	@Test
	@Tag("Lock")
	// [버킷의 Lock옵션을 활성화] LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지 메타데이터를 통해
	// 확인
	public void test_object_lock_uploading_obj() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var key = "file1";
		var body = "abc";
		var md5 = Utils.getMD5(body);
		var days = Calendar.getInstance();
		days.set(2030, 1, 1, 0, 0, 0);

		var metadata = new ObjectMetadata();
		metadata.setContentMD5(md5);
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		var PutResponse = client.putObject(new PutObjectRequest(bucketName, key, createBody(body), metadata)
				.withObjectLockMode(ObjectLockMode.GOVERNANCE).withObjectLockRetainUntilDate(days.getTime())
				.withObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ObjectLockMode.GOVERNANCE.toString(), response.getObjectLockMode());
		assertEquals(days.getTime(), response.getObjectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), response.getObjectLockLegalHoldStatus());

		var legalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key).withLegalHold(legalHold));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, PutResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Space")
	// 오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_infix_space() {
		var keys = new ArrayList<>(Arrays.asList(new String[] { "a a/", "b b/f1", "c/f 2", "d d/f 3" }));
		var bucketName = createObjectsToBody(keys, "");
		var client = getClient();

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Space")
	// 오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_suffix_space() {
		var keys = new ArrayList<>(Arrays.asList(new String[] { "a /", "b /f1", "c/f2 ", "d /f3 " }));
		var bucketName = createObjectsToBody(keys, "");
		var client = getClient();

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_2() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsToBodyV2(keys, "");
		var client = getClientV2();

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion4] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_4() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsToBodyV4(keys, "", true);
		var client = getClientV4(true);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion2] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_2() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsV2(keys);
		var client = getClientV2();

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("SpecialCharacters")
	// [SignatureVersion4] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_4() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsV4(keys, true);
		var client = getClientV4(true);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsV4(keys, true);
		var client = getClientV4(true);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true]
	// 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding_and_disable_payload_signing() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsHttps(keys, true, true);
		var client = getClientV4(true);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsV4(keys, false);
		var client = getClientV4(false);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Encoding")
	// [SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true]
	// 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding_and_disable_payload_signing() {
		var keys = new ArrayList<>(
				Arrays.asList(new String[] { "!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(","(/","(/(", ")",")/",")/)", "*","*/","*/*", ":",":/",":/:", "[","[/","[/[", "]", "]/", "]/]" }));
		var bucketName = createObjectsHttps(keys, false, true);
		var client = getClientV4(false);

		var response = client.listObjects(bucketName);
		var Keys = getKeys(response.getObjectSummaries());

		assertEquals(keys, Keys);
	}

	@Test
	@Tag("Directory")
	// 폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인
	public void test_put_object_dir_and_file() {
		// file first
		var bucketName = getNewBucket();
		var key = "aaa";
		var directoryName = "aaa/";
		var client = getClient();

		client.putObject(bucketName, key, key);
		client.putObject(bucketName, directoryName, "");

		var response = client.listObjects(bucketName);
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());

		// dir first
		var bucketName2 = getNewBucket();

		client.putObject(bucketName2, directoryName, "");
		client.putObject(bucketName2, key, key);

		response = client.listObjects(bucketName2);
		keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());

		// etc
		var bucketName3 = getNewBucket();
		var newKey = "aaa/bbb/ccc";

		client.putObject(bucketName3, key, key);
		client.putObject(bucketName3, newKey, newKey);

		response = client.listObjects(bucketName3);
		keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());
	}

	@Test
	@Tag("Overwrite")
	// 오브젝트를 덮어쓰기 했을때 올바르게 반영되는지 확인
	public void test_object_overwrite() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "temp";
		var content1 = Utils.randomTextToLong(10 * MainData.KB);
		var content2 = Utils.randomTextToLong(1 * MainData.MB);

		client.putObject(bucketName, key, content1);
		client.putObject(bucketName, key, content2);

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());

		assertEquals(content2.length(), body.length());
		assertTrue(content2.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PUT")
	// 오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인
	public void test_object_emoji() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "test❤🍕🍔🚗";

		client.putObject(bucketName, "asdf", key);

		var response = client.listObjects(bucketName);
		assertEquals(1, response.getObjectSummaries().size());
	}
	
	@Test
	@Tag("metadata")
	// 메타데이터에 utf-8이 포함될 경우 올바르게 업로드 되는지 확인
	public void test_object_set_get_metadata_utf8() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var MetadataKey1 = "x-amz-meta-meta1";
		var MetadataKey2 = "x-amz-meta-meta2";
		var metadata1 = "utf-8";
		var metadata2 = "UTF-8";
		var ContentType = "text/plain; charset=UTF-8";
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata(MetadataKey1, metadata1);
		MetadataList.addUserMetadata(MetadataKey2, metadata2);
		MetadataList.setContentType(ContentType);

		client.putObject(bucketName, key, createBody("bar"), MetadataList);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata1, response.getUserMetaDataOf(MetadataKey1));
		assertEquals(metadata2, response.getUserMetaDataOf(MetadataKey2));

	}
}
