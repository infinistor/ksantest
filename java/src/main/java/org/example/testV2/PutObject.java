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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.CheckSum;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;

public class PutObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("PutObject V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("PutObject V2 End");
	}

	@Test
	@Tag("PUT")
	public void testBucketListDistinct() {
		var client = getClient();
		var bucketName1 = createBucket(client);
		var bucketName2 = createBucket(client);

		client.putObject(p -> p.bucket(bucketName1).key("foo"), RequestBody.fromString("bar"));

		var response = client.listObjects(l -> l.bucket(bucketName2));
		assertEquals(0, response.contents().size());
	}

	@Test
	@Tag("ERROR")
	public void testObjectWriteToNonExistBucket() {
		var key = "foo";
		var client = getClient();
		var bucketName = getNewBucketName();

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(p -> p.bucket(bucketName).key(key),
				RequestBody.fromString("bar")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("metadata")
	public void testObjectHeadZeroBytes() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(0, response.contentLength());
	}

	@Test
	@Tag("metadata")
	public void testObjectWriteCheckEtag() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.putObject(p -> p.bucket(bucketName).key("foo"), RequestBody.fromString("bar"));
		assertEquals("37b51d194a7513e45b56f6524f2d51f2", response.eTag().replace("\"", ""));
	}

	@Test
	@Tag("cacheControl")
	public void testObjectWriteCacheControl() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";
		var cacheControl = "public, max-age=14HttpStatus.SC_BAD_REQUEST";
		var contentType = "text/plain";

		client.putObject(p -> p.bucket(bucketName).key(key).cacheControl(cacheControl).contentType(contentType)
				.contentLength((long) body.length()), RequestBody.fromString(body));

		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(cacheControl, headResponse.cacheControl());

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(body, getBody(getResponse));
	}

	@Test
	@Disabled("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	@Tag("Expires")
	public void testObjectWriteExpires() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";
		var expires = getTimeToAddSeconds(6000).toInstant();

		client.putObject(p -> p.bucket(bucketName).key(key).expires(expires).contentType("text/plain")
				.contentLength((long) body.length()), RequestBody.fromString(body));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(expires, response.expiresString());
	}

	@Test
	@Tag("Update")
	public void testObjectWriteReadUpdateReadDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var body = "bar";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var responseBody = getBody(getResponse);
		assertEquals(body, responseBody);

		var body2 = "soup";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body2));

		getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		responseBody = getBody(getResponse);
		assertEquals(body2, responseBody);

		client.deleteObject(d -> d.bucket(bucketName).key(key));
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataNoneToGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "my");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata), RequestBody.fromString(key));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataNoneToEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata), RequestBody.fromString(key));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataOverwriteToEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "my");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata), RequestBody.fromString(key));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());

		var metadata2 = new HashMap<String, String>();
		metadata2.put("meta1", "");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata2), RequestBody.fromString(key));

		response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata2, response.metadata());
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetNonUtf8Metadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "\nmy_meta");

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata),
						RequestBody.fromString("bar")));

		assertTrue(errorCheck(e.statusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetMetadataEmptyToUnreadablePrefix() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadataKey = "meta1";
		var metadata = new HashMap<String, String>();
		metadata.put(metadataKey, "\nasdf");

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata),
						RequestBody.fromString("bar")));

		assertTrue(errorCheck(e.statusCode()));
	}

	@Test
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("metadata")
	public void testObjectSetGetMetadataEmptyToUnreadableSuffix() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "asdf\n");

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata),
						RequestBody.fromString("bar")));

		assertTrue(errorCheck(e.statusCode()));
	}

	@Test
	@Tag("metadata")
	public void testObjectMetadataReplacedOnPut() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var body = "bar";

		var metadata = new HashMap<String, String>();
		metadata.put("meta1", "bar");

		client.putObject(p -> p.bucket(bucketName).key(key).contentType("text/plain").metadata(metadata),
				RequestBody.fromString(body));
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(0, response.response().metadata().size());
	}

	@Test
	@Tag("Encoding")
	public void testObjectWriteFile() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var dataStr = "bar";
		var data = new String(dataStr.getBytes(), StandardCharsets.US_ASCII);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals(dataStr, body);
	}

	@Test
	@Tag("SpecialKeyName")
	public void testBucketCreateSpecialKeyNames() {
		var keys = List.of("!", "-", "_", ".", "'", "(" + ")", "&", "$", "@", "=", ";", "/", ":", "+", "  ", ",", "?",
				"{" + "}", "^", "%", "`", "[" + "]", "<" + ">", "~", "#", "|");
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var objects = getObjectList(client, bucketName, null);

		for (var key : keys) {
			assertTrue(objects.contains(key));
			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var body = getBody(response);

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
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var objects = getObjectList(client, bucketName, null);
		assertEquals(5, objects.size());

		objects = getObjectList(client, bucketName, "Bla/");
		assertEquals(4, objects.size());
	}

	@Test
	@Tag("Lock")
	public void testObjectLockUploadingObj() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "testObjectLockUploadingObjV2";
		var md5 = Utils.getMD5(key);
		var days = Calendar.getInstance();
		days.set(2030, 1, 1, 0, 0, 0);

		var putResponse = client.putObject(p -> p
				.bucket(bucketName)
				.key(key)
				.contentMD5(md5)
				.contentType("text/plain")
				.contentLength((long) key.length())
				.objectLockMode(ObjectLockMode.GOVERNANCE)
				.objectLockRetainUntilDate(days.toInstant())
				.objectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON),
				RequestBody.fromString(key));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(ObjectLockMode.GOVERNANCE.toString(), response.objectLockMode().toString());
		assertEquals(days.toInstant(), response.objectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), response.objectLockLegalHoldStatus().toString());

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));
		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Space")
	public void testObjectInfixSpace() {
		var keys = List.of("a a/", "b b/f1", "c/f 2", "d d/f 3");
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Space")
	public void testObjectSuffixSpace() {
		var keys = List.of("a /", "b /f1", "c/f2 ", "d /f3 ");
		var client = getClient();
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("SpecialCharacters")
	public void testPutObjectSpecialCharacters() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClient(true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectSpecialCharactersUseChunkEncoding() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClient(true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectUseSpecialCharactersChunkEncodingAndDisablePayloadSigning() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClient(true);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectSpecialCharactersNotChunkEncoding() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClient(false);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Encoding")
	public void testPutObjectSpecialCharactersNotChunkEncodingAndDisablePayloadSigning() {
		var keys = List.of("!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
				")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]");
		var client = getClient(false);
		var bucketName = createObjects(client, keys);

		var response = client.listObjects(l -> l.bucket(bucketName));
		var getKeys = getKeys(response.contents());

		assertEquals(keys, getKeys);
	}

	@Test
	@Tag("Directory")
	public void testPutObjectDirAndFile() {
		var key = "aaa";
		var directoryName = "aaa/";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.putObject(p -> p.bucket(bucketName).key(directoryName), RequestBody.empty());

		var response = client.listObjects(l -> l.bucket(bucketName));
		var keys = getKeys(response.contents());
		assertEquals(2, keys.size());

		var bucketName2 = createBucket(client);

		client.putObject(p -> p.bucket(bucketName2).key(directoryName), RequestBody.empty());
		client.putObject(p -> p.bucket(bucketName2).key(key), RequestBody.fromString(key));

		response = client.listObjects(l -> l.bucket(bucketName2));
		keys = getKeys(response.contents());
		assertEquals(2, keys.size());

		var bucketName3 = createBucket(client);
		var newKey = "aaa/bbb/ccc";

		client.putObject(p -> p.bucket(bucketName3).key(key), RequestBody.fromString(key));
		client.putObject(p -> p.bucket(bucketName3).key(newKey), RequestBody.fromString(newKey));

		response = client.listObjects(l -> l.bucket(bucketName3));
		keys = getKeys(response.contents());
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

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content1));
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content2));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);

		assertEquals(content2.length(), body.length());
		assertTrue(content2.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PUT")
	public void testObjectEmoji() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "test❤🍕🍔🚗";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(1, response.contents().size());
	}

	@Test
	@Tag("metadata")
	public void testObjectSetGetMetadataUtf8() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var metadataKey1 = "meta1";
		var metadataKey2 = "meta2";
		var metadata1 = "utf-8";
		var metadata2 = "UTF-8";
		var contentType = "text/plain; charset=UTF-8";
		var metadata = new HashMap<String, String>();
		metadata.put(metadataKey1, metadata1);
		metadata.put(metadataKey2, metadata2);

		client.putObject(p -> p.bucket(bucketName).key(key).contentType(contentType).metadata(metadata),
				RequestBody.fromString("bar"));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata1, response.metadata().get(metadataKey1));
		assertEquals(metadata2, response.metadata().get(metadataKey2));
	}

	@Test
	@Tag("checksum")
	public void testPutObjectChecksumUseChunkEncoding() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption) {
		}

		var bucketName = createBucket();
		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED));

		// PutObject는 모든 체크섬 알고리즘 사용 가능
		var checksums = CheckSum.ALL_ALGORITHMS;

		for (var config : configs) {
			var client = getClient(true, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(true, config.requestOption, config.responseOption);

			for (var checksum : checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + checksum.name();
				var asyncKey = prefix + "/async/" + checksum.name();

				var response = client.putObject(
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(key), checksum, key),
						RequestBody.fromString(key));
				checksumCompare(checksum, key, response);

				var asyncResponse = asyncClient.putObject(
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(asyncKey), checksum, asyncKey),
						AsyncRequestBody.fromString(asyncKey));
				checksumCompare(checksum, asyncKey, asyncResponse.join());
			}
		}
	}

	@Test
	@Tag("checksum")
	public void testPutObjectChecksum() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption) {
		}

		var bucketName = createBucket();
		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED));

		// PutObject는 모든 체크섬 알고리즘 사용 가능
		var checksums = CheckSum.ALL_ALGORITHMS;

		for (var config : configs) {
			var client = getClient(false, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(false, config.requestOption, config.responseOption);

			for (var checksum : checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + checksum.name();
				var asyncKey = prefix + "/async/" + checksum.name();

				// 동기 클라이언트 체크섬 확인
				var response = client.putObject(
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(key), checksum, key),
						RequestBody.fromString(key));
				checksumCompare(checksum, key, response);

				// 비동기 클라이언트 체크섬 확인
				var asyncResponse = asyncClient.putObject(
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(asyncKey), checksum, asyncKey),
						AsyncRequestBody.fromString(asyncKey));
				checksumCompare(checksum, asyncKey, asyncResponse.join());
			}
		}
	}

	@Test
	@Tag("checksum")
	public void testPutObjectChecksumWithValue() {
		var client = getClient();
		var bucketName = createBucket(client);

		// 사전 계산한 체크섬 값을 직접 지정하여 업로드 성공 확인
		for (var checksum : CheckSum.ALL_ALGORITHMS) {
			var key = "precomputed/" + checksum.name();
			var value = CheckSum.calculateChecksum(checksum, key);

			var response = client.putObject(p -> {
				p.bucket(bucketName).key(key);
				CheckSum.setChecksum(p, checksum, value);
			}, RequestBody.fromString(key));
			checksumCompare(checksum, key, response);
		}
	}

	@Test
	@Tag("checksum-failure")
	public void testPutObjectChecksumFailure() {
		var client = getClient();
		var bucketName = createBucket(client);

		// 잘못된 체크섬 값을 지정할 경우 BadDigest 에러 확인
		for (var checksum : CheckSum.ALL_ALGORITHMS) {
			var key = "wrong-checksum/" + checksum.name();
			var wrongValue = CheckSum.calculateChecksum(checksum, key + "-wrong");

			var e = assertThrows(AwsServiceException.class,
					() -> client.putObject(p -> {
						p.bucket(bucketName).key(key);
						CheckSum.setChecksum(p, checksum, wrongValue);
					}, RequestBody.fromString(key)));
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
			assertEquals(MainData.BAD_DIGEST, e.awsErrorDetails().errorCode());
		}
	}

	@Test
	@Tag("IfMatch")
	// 일치하는 If-Match 조건으로 오브젝트 덮어쓰기 성공 확인
	public void testPutObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectIfMatchGood";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old")).eTag();

		client.putObject(p -> p.bucket(bucketName).key(key).ifMatch(eTag), RequestBody.fromString("new"));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("new", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	// 일치하지 않는 If-Match 조건으로 오브젝트 덮어쓰기 시 412 실패 확인
	public void testPutObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectIfMatchFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
						RequestBody.fromString("new")));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 존재하지 않는 키에 If-None-Match: * 조건으로 업로드 성공 확인
	public void testPutObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectIfNoneMatchGood";

		client.putObject(p -> p.bucket(bucketName).key(key).ifNoneMatch("*"), RequestBody.fromString("bar"));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("bar", getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 이미 존재하는 키에 If-None-Match: * 조건으로 업로드 시 412 실패 확인
	public void testPutObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectIfNoneMatchFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).ifNoneMatch("*"),
						RequestBody.fromString("new")));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
	public void testPutObjectIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectIfMatchAndIfNoneMatch";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old")).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).ifMatch(eTag).ifNoneMatch("*"),
						RequestBody.fromString("new")));
		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyMaxLength() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = Utils.randomObjectName(MainData.MAX_KEY_LENGTH);
		var body = "test-max-length";

		var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
		assertNotNull(response.eTag());

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(body, getBody(getResponse));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyMinLength() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "a";
		var body = "test-min-length";

		var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
		assertNotNull(response.eTag());

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(body, getBody(getResponse));
	}

	@Test
	@Tag("KeyLength")
	public void testPutObjectKeyTooLong() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = Utils.randomObjectName(MainData.MAX_KEY_LENGTH + 1);
		var body = "test-too-long";

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(p -> p.bucket(bucketName).key(key),
				RequestBody.fromString(body)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.KEY_TOO_LONG, e.awsErrorDetails().errorCode());
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
			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
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
			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
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

			// 안전하게 조금 작은 길이로 시도
			var safeLength = Math.max(1, maxLength - 1);
			var key = unicodeChar.repeat(safeLength);
			var body = "unicode-test-" + unicodeChar;

			var actualBytes = key.getBytes(StandardCharsets.UTF_8).length;

			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
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

			var e = assertThrows(AwsServiceException.class, () -> client.putObject(p -> p.bucket(bucketName).key(key),
					RequestBody.fromString(body)));
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
			assertEquals(MainData.KEY_TOO_LONG, e.awsErrorDetails().errorCode());
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
			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
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

			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
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

			var response = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
			assertNotNull(response.eTag());

			var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
			assertEquals(body, getBody(getResponse));
		}
	}
}
