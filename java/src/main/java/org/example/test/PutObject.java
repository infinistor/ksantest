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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
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
	public void testBucketListDistinct() {
		var client = getClient();
		var bucketName1 = createBucket(client);
		var bucketName2 = createBucket(client);

		client.putObject(bucketName1, "asdf", "str");

		var response = client.listObjects(bucketName2);
		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("ERROR")
	public void testObjectWriteToNonExistBucket() {
		var key = "foo";
		var client = getClient();
		var bucketName = getNewBucketName();

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, key));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("metadata")
	public void testObjectHeadZeroBytes() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		client.putObject(bucketName, key, "");

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(0, response.getContentLength());
	}

	@Test
	@Tag("metadata")
	public void testObjectWriteCheckEtag() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.putObject(bucketName, "foo", "bar");
		assertEquals("37b51d194a7513e45b56f6524f2d51f2", response.getETag());
	}

	@Test
	@Tag("cacheControl")
	public void testObjectWriteCacheControl() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";
		var cacheControl = "public, max-age=14HttpStatus.SC_BAD_REQUEST";
		var metadata = new ObjectMetadata();
		metadata.setCacheControl(cacheControl);
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(cacheControl, response.getCacheControl());

		var result = client.getObject(bucketName, key);
		assertEquals(body, getBody(result.getObjectContent()));
	}

	@Test
	@Disabled("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	@Tag("Expires")
	public void testObjectWriteExpires() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";
		var expires = getTimeToAddSeconds(6000);
		var metadata = new ObjectMetadata();
		metadata.setExpirationTime(expires);
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(expires, response.getExpirationTime());
	}

	@Test
	@Tag("Update")
	public void testObjectWriteReadUpdateReadDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";

		client.putObject(bucketName, key, body);

		var getResponse = client.getObject(bucketName, key);
		var responseBody = getBody(getResponse.getObjectContent());
		assertEquals(body, responseBody);

		var body2 = "soup";
		client.putObject(bucketName, key, body2);

		getResponse = client.getObject(bucketName, key);
		responseBody = getBody(getResponse.getObjectContent());
		assertEquals(body2, responseBody);

		client.deleteObject(bucketName, key);
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataNoneToGood() {
		var value = "my";
		var got = setupMetadata(value, null);
		assertEquals(value, got);
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataNoneToEmpty() {
		var got = setupMetadata("", null);
		assertEquals("", got);
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataOverwriteToEmpty() {
		var bucketName = createBucket();

		var myMeta = "old_meta";
		var got = setupMetadata(myMeta, bucketName);
		assertEquals(myMeta, got);

		got = setupMetadata("", bucketName);
		assertEquals("", got);
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetNonUtf8Metadata() {
		var metadata = "\nmy_meta";
		var e = setGetMetadataUnreadable(metadata, null);
		assertTrue(errorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetMetadataEmptyToUnreadablePrefix() {
		var metadata = "\nw";
		var e = setGetMetadataUnreadable(metadata, null);
		assertTrue(errorCheck(e.getStatusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetMetadataEmptyToUnreadableSuffix() {
		var metadata = "h\n";
		var e = setGetMetadataUnreadable(metadata, null);
		assertTrue(errorCheck(e.getStatusCode()));
	}

	@Test
	@Tag("metadata")
	public void testObjectMetadataReplacedOnPut() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var body = "bar";

		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-meta1", "bar");
		metadata.setContentType("text/plain");
		metadata.setContentLength(body.length());

		client.putObject(bucketName, key, createBody(body), metadata);
		client.putObject(bucketName, key, body);

		var response = client.getObject(bucketName, key);
		var got = response.getObjectMetadata().getUserMetadata();
		assertEquals(0, got.size());
	}

	@Test
	@Tag("Encoding")
	public void testObjectWriteFile() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var dataStr = "bar";
		var data = new String(dataStr.getBytes(), StandardCharsets.US_ASCII);

		client.putObject(bucketName, key, data);

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(dataStr, body);
	}

	@Test
	@Tag("SpecialKeyName")
	public void testBucketCreateSpecialKeyNames() {
		var keys = List.of("!", "-", "_", ".", "'", "(" + ")", "&", "$", "@", "=", ";", "/", ":", "+", "  ", ",", "?",
				"{" + "}", "^", "%", "`", "[" + "]", "<" + ">", "~", "#", "|");
		var client = getClient();
		var bucketName = createObjects(client, keys);
		var objects = getObjectList(bucketName, null);

		for (var key : keys) {
			assertTrue(objects.contains(key));
			var response = client.getObject(bucketName, key);
			var body = getBody(response.getObjectContent());
			if (key.endsWith("/"))
				assertEquals("", body);
			else
				assertEquals(key, body);
		}
	}

	@Test
	@Tag("SpecialKeyName")
	public void testBucketListSpecialPrefix() {
		var keys = List.of("Bla/1", "Bla/2", "Bla/3", "Bla/4", "abcd");

		var bucketName = createObjects(keys);

		var objects = getObjectList(bucketName, null);
		assertEquals(5, objects.size());

		objects = getObjectList(bucketName, "Bla/");
		assertEquals(4, objects.size());
	}

	@Test
	@Tag("Lock")
	public void testObjectLockUploadingObj() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var key = "testObjectLockUploadingObj";
		var md5 = Utils.getMD5(key);
		var days = Calendar.getInstance();
		days.set(2030, 1, 1, 0, 0, 0);

		var metadata = new ObjectMetadata();
		metadata.setContentMD5(md5);
		metadata.setContentType("text/plain");
		metadata.setContentLength(key.length());

		var putResponse = client.putObject(new PutObjectRequest(bucketName, key, createBody(key), metadata)
				.withObjectLockMode(ObjectLockMode.GOVERNANCE).withObjectLockRetainUntilDate(days.getTime())
				.withObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ObjectLockMode.GOVERNANCE.toString(), response.getObjectLockMode());
		assertEquals(days.getTime(), response.getObjectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), response.getObjectLockLegalHoldStatus());

		var legalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key).withLegalHold(legalHold));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, putResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Space")
	public void testObjectInfixSpace() {
		var keys = List.of("a a/", "b b/f1", "c/f 2", "d d/f 3");
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Space")
	public void testObjectSuffixSpace() {
		var keys = List.of("a /", "b /f1", "c/f2 ", "d /f3 ");
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("SpecialCharacters")
	public void testPutEmptyObjectSignatureV2() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(", ")", ")/", ")/)",
				"*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV2();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("SpecialCharacters")
	public void testPutEmptyObjectSignatureV4() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV4(true, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("SpecialCharacters")
	public void testPutObjectSignatureV2() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV2();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("SpecialCharacters")
	public void testPutObjectSignatureV4() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV4(true, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectUseChunkEncoding() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV4(true, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectUseChunkEncodingAndDisablePayloadSigning() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientHttpsV4(true, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectNotChunkEncoding() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV4(false, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectNotChunkEncodingAndDisablePayloadSigning() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClientV4(false, true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(bucketName);
		var getKeys = getKeys(response.getObjectSummaries());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Directory")
	public void testPutObjectDirAndFile() {
		var key = "aaa";
		var directoryName = "aaa/";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, key);
		client.putObject(bucketName, directoryName, "");

		var response = client.listObjects(bucketName);
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());

		var bucketName2 = createBucket(client);

		client.putObject(bucketName2, directoryName, "");
		client.putObject(bucketName2, key, key);

		response = client.listObjects(bucketName2);
		keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());

		var bucketName3 = createBucket(client);
		var newKey = "aaa/bbb/ccc";

		client.putObject(bucketName3, key, key);
		client.putObject(bucketName3, newKey, newKey);

		response = client.listObjects(bucketName3);
		keys = getKeys(response.getObjectSummaries());
		assertEquals(2, keys.size());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
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
	public void testObjectEmoji() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "test❤🍕🍔🚗";

		client.putObject(bucketName, "asdf", key);

		var response = client.listObjects(bucketName);
		assertEquals(1, response.getObjectSummaries().size());
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataUtf8() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadataKey1 = "x-amz-meta-meta1";
		var metadataKey2 = "x-amz-meta-meta2";
		var metadata1 = "utf-8";
		var metadata2 = "UTF-8";
		var contentType = "text/plain; charset=UTF-8";
		var metadataList = new ObjectMetadata();
		metadataList.addUserMetadata(metadataKey1, metadata1);
		metadataList.addUserMetadata(metadataKey2, metadata2);
		metadataList.setContentType(contentType);

		client.putObject(bucketName, key, createBody("bar"), metadataList);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata1, response.getUserMetaDataOf(metadataKey1));
		assertEquals(metadata2, response.getUserMetaDataOf(metadataKey2));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyMaxLength() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = Utils.randomObjectName(MainData.MAX_KEY_LENGTH);
		var body = "test-max-length";

		var response = client.putObject(bucketName, key, body);
		assertNotNull(response.getETag());

		var getResponse = client.getObject(bucketName, key);
		assertEquals(body, getBody(getResponse.getObjectContent()));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyMinLength() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "a";
		var body = "test-min-length";

		var response = client.putObject(bucketName, key, body);
		assertNotNull(response.getETag());

		var getResponse = client.getObject(bucketName, key);
		assertEquals(body, getBody(getResponse.getObjectContent()));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyTooLong() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = Utils.randomObjectName(MainData.MAX_KEY_LENGTH + 1);
		var body = "test-too-long";

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, body));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.KEY_TOO_LONG, e.getErrorCode());
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeySpecialCharactersAtStart() {
		var client = getClient();
		var bucketName = createBucket(client);
		var specialChars = List.of("!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{",
				"}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`");

		for (var specialChar : specialChars) {
			// 최대 길이에서 특수문자 1자를 뺀 길이로 생성
			var remainingLength = MainData.MAX_KEY_LENGTH - specialChar.length();
			var key = specialChar + Utils.randomObjectName(remainingLength);
			var body = "test-body-" + specialChar;

			assertEquals(MainData.MAX_KEY_LENGTH, key.length());
			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeySpecialCharactersAtEnd() {
		var client = getClient();
		var bucketName = createBucket(client);
		var specialChars = List.of("!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{",
				"}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`");

		for (var specialChar : specialChars) {
			// 최대 길이에서 특수문자 1자를 뺀 길이로 생성
			var remainingLength = MainData.MAX_KEY_LENGTH - specialChar.length();
			var key = Utils.randomObjectName(remainingLength) + specialChar;
			var body = "test-body-" + specialChar;

			assertEquals(MainData.MAX_KEY_LENGTH, key.length());
			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyUnicodeCharacters() {
		var client = getClient();
		var bucketName = createBucket(client);
		var unicodeChars = List.of("한", "中", "日", "а", "α", "ع", "т", "ф");

		for (var unicodeChar : unicodeChars) {
			// 실제 바이트 길이 확인
			var singleCharBytes = unicodeChar.getBytes(StandardCharsets.UTF_8).length;
			var maxLength = 200 / singleCharBytes; // 200자 제한에 맞는 최대 문자 수

			System.out.println("문자: " + unicodeChar + ", 바이트: " + singleCharBytes + ", 최대길이: " + maxLength);

			// 안전하게 조금 작은 길이로 시도
			var safeLength = Math.max(1, maxLength - 1);
			var key = unicodeChar.repeat(safeLength);
			var body = "unicode-test-" + unicodeChar;

			var actualBytes = key.getBytes(StandardCharsets.UTF_8).length;
			System.out.println("키길이: " + key.length() + "자, 실제바이트: " + actualBytes);

			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyUnicodeCharactersTooLong() {
		var client = getClient();
		var bucketName = createBucket(client);
		var unicodeChars = List.of("한", "中", "日", "а", "α", "ع", "т", "ф");

		for (var unicodeChar : unicodeChars) {
			// 실제 바이트 길이 확인
			var singleCharBytes = unicodeChar.getBytes(StandardCharsets.UTF_8).length;
			var maxLength = 1024 / singleCharBytes; // 1024바이트 제한에 맞는 최대 문자 수

			// 1024바이트를 초과하는 길이로 시도
			var tooLongLength = maxLength + 1;
			var key = unicodeChar.repeat(tooLongLength);
			var body = "unicode-test-fail-" + unicodeChar;

			var actualBytes = key.getBytes(StandardCharsets.UTF_8).length;
			System.out.println("실패테스트 - 문자: " + unicodeChar + ", 키길이: " + key.length() + "자, 실제바이트: " + actualBytes);

			var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, body));
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
			assertEquals(MainData.KEY_TOO_LONG, e.getErrorCode());
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyWithLeadingAndTrailingSpaces() {
		var client = getClient();
		var bucketName = createBucket(client);
		var testCases = List.of(
				1, // " " + 1022자 + " " = 1024자
				2, // " " + 1020자 + " " = 1024자
				3, // " " + 1018자 + " " = 1024자
				5 // " " + 1014자 + " " = 1024자
		);

		for (var spaceCount : testCases) {
			var spaces = " ".repeat(spaceCount);
			var middleLength = MainData.MAX_KEY_LENGTH - (spaceCount * 2);
			var middle = Utils.randomObjectName(middleLength);
			var key = spaces + middle + spaces;
			var body = "space-test-" + spaceCount;

			assertEquals(MainData.MAX_KEY_LENGTH, key.length());
			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyWithConsecutiveSlashes() {
		var client = getClient();
		var bucketName = createBucket(client);
		var keys = List.of(
				"folder//double-slash",
				"folder///triple-slash",
				"//leading-double-slash",
				"trailing-double-slash//",
				"folder////multiple-slashes");

		for (var key : keys) {
			var body = "slash-test-" + key.replace("/", "-");

			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyBoundaryLengths() {
		var client = getClient();
		var bucketName = createBucket(client);
		var testCases = List.of(
				MainData.MAX_KEY_LENGTH - 1, // 1023
				MainData.MAX_KEY_LENGTH, // 1024
				500, // 중간 길이
				100, // 짧은 길이
				50 // 매우 짧은 길이
		);

		for (var length : testCases) {
			var key = Utils.randomObjectName(length);
			var body = "boundary-test-" + length;

			var response = client.putObject(bucketName, key, body);
			assertNotNull(response.getETag());

			var getResponse = client.getObject(bucketName, key);
			assertEquals(body, getBody(getResponse.getObjectContent()));
		}
	}

}
