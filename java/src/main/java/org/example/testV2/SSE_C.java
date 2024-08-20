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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.util.Base64;
import java.util.HashMap;

import org.example.Data.FormFile;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;

public class SSE_C extends TestBase {
	// cSpell:disable
	static final String SSE_KEY = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
	static final String SSE_KEY_MD5 = "DWygnHRtgiJ77HCm+1rvHw==";
	static final String SSE_ALGORITHM = "AES256";
	// cSpell:enable

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("SSE_C V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("SSE_C V2 End");
	}

	@Test
	@Tag("PutGet")
	// 1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testEncryptedTransfer1b() {
		testEncryptionSSECustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	// 1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testEncryptedTransfer1kb() {
		testEncryptionSSECustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	// 1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testEncryptedTransfer1MB() {
		testEncryptionSSECustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// 13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testEncryptedTransfer13b() {
		testEncryptionSSECustomerWrite(13);
	}

	@Test
	@Tag("metadata")
	// SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
	public void testEncryptionSseCMethodHead() {
		var key = "obj";
		var size = 1000;
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(size);

		client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var e = assertThrows(AwsServiceException.class, () -> client.headObject(h -> h.bucket(bucketName).key(key)));
		var statusCode = e.statusCode();
		assertEquals(400, statusCode);

		client.headObject(g -> g.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY).sseCustomerKeyMD5(SSE_KEY_MD5));
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
	public void testEncryptionSseCPresent() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		var statusCode = e.statusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void testEncryptionSseCOtherKey() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var sseB = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
		var sseBMd5 = "arxBvwY2V4SiOne6yppVPQ==";

		client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var e = assertThrows(AwsServiceException.class,
				() -> client
						.getObject(g -> g.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
								.sseCustomerKey(sseB).sseCustomerKeyMD5(sseBMd5)));
		var statusCode = e.statusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
	public void testEncryptionSseCInvalidMd5() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var e = assertThrows(AwsServiceException.class, () -> client
				.putObject(p -> p.bucket(bucketName).key(key)
						.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
						.sseCustomerKey(SSE_KEY)
						.sseCustomerKeyMD5("AAAAAAAAAAAAAAAAAAAAAA=="),
						RequestBody.fromString(data)));

		var statusCode = e.statusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 실패 확인
	public void testEncryptionSseCNoMd5() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY), RequestBody.fromString(data)));
		assertEquals(400, e.statusCode());
		assertEquals("InvalidArgument", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
	public void testEncryptionSseCNoKey() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).sseCustomerKeyMD5(SSE_KEY_MD5),
						RequestBody.fromString(data)));
		assertEquals(400, e.statusCode());
		assertEquals("InvalidArgument", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
	public void testEncryptionKeyNoSseC() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerKey(SSE_KEY).sseCustomerKeyMD5(SSE_KEY_MD5), RequestBody.fromString(data)));
		assertEquals(400, e.statusCode());
		assertEquals("InvalidArgument", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Multipart")
	// 멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
	public void testEncryptionSseCMultipartUpload() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var uploadData = setupSseCMultipartUpload(client, bucketName, key, size, metadata);

		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));

		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
		assertEquals(metadata, getResponse.response().metadata());
		assertEquals(SSE_ALGORITHM, getResponse.response().sseCustomerAlgorithm());

		var body = uploadData.getBody();
		checkContentUsingRangeEnc(client, bucketName, key, body, MainData.MB);
		checkContentUsingRangeEnc(client, bucketName, key, body, 10L * MainData.MB);
		checkContentUsingRandomRangeEnc(client, bucketName, key, body, size, 100);
	}

	@Test
	@Tag("Multipart")
	// SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void testEncryptionSseCMultipartBadDownload() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var sseGetKey = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
		var sseGetMd5 = "arxBvwY2V4SiOne6yppVPQ==";

		var uploadData = setupSseCMultipartUpload(client, bucketName, key, size, metadata);

		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));

		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client
				.getObject(g -> g.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
						.sseCustomerKey(SSE_KEY).sseCustomerKeyMD5(SSE_KEY_MD5));
		assertEquals(metadata, getResponse.response().metadata());
		assertEquals(SSE_ALGORITHM, getResponse.response().sseCustomerAlgorithm());

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(
						g -> g.bucket(bucketName).key(key).sseCustomerKey(sseGetKey).sseCustomerKeyMD5(sseGetMd5)));
		var statusCode = e.statusCode();
		assertEquals(400, statusCode);
		assertEquals("InvalidArgument", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Post")
	// Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
	public void testEncryptionSseCPostObjectAuthenticatedRequest() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var client = getClientHttps(false);
		var bucketName = createBucket(client);

		var contentType = "text/plain";
		var key = "foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		conditions.add(starts1);

		var acl = new JsonObject();
		acl.addProperty("acl", "private");
		conditions.add(acl);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(contentType);
		conditions.add(starts2);

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-server-side-encryption-customer-algorithm");
		starts3.add(SSE_ALGORITHM);
		conditions.add(starts3);

		var starts4 = new JsonArray();
		starts4.add("starts-with");
		starts4.add("$x-amz-server-side-encryption-customer-key");
		starts4.add(SSE_KEY);
		conditions.add(starts4);

		var starts5 = new JsonArray();
		starts5.add("starts-with");
		starts5.add("$x-amz-server-side-encryption-customer-key-md5");
		starts5.add(SSE_KEY_MD5);
		conditions.add(starts5);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);
		payload.put("x-amz-server-side-encryption-customer-algorithm", SSE_ALGORITHM);
		payload.put("x-amz-server-side-encryption-customer-key", SSE_KEY);
		payload.put("x-amz-server-side-encryption-customer-key-md5", SSE_KEY_MD5);

		var sendURL = getURL(bucketName);
		var result = NetUtils.postUpload(sendURL, payload, fileData);
		assertEquals(204, result.statusCode);

		var response = client.getObject(g -> g.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY));
		var body = getBody(response);
		assertEquals("bar", body);
		assertEquals(SSE_ALGORITHM, response.response().sseCustomerAlgorithm());
	}

	@Test
	@Tag("Get")
	// SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void testEncryptionSseCGetObjectMany() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		checkContentEnc(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	// SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void testEncryptionSseCRangeObjectMany() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		client.putObject(p -> p.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
		var body = getBody(response);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSE_ALGORITHM, response.response().sseCustomerAlgorithm());

		checkContentUsingRandomRangeEnc(client, bucketName, key, data, size, 50);
	}

	@Test
	@Tag("Multipart")
	// SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 multipart Copy 로 복사 가능한지 확인
	public void testSseCEncryptionMultipartCopyPartUpload() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;

		var uploadData = setupSseCMultipartUpload(client, bucketName, sourceKey, size, null);

		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));

		var listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = listResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(listResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(sourceKey)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
		assertEquals(SSE_ALGORITHM, getResponse.response().sseCustomerAlgorithm());

		// 멀티파트 복사
		var targetKey = "multipartEncCopy";
		var uploadData2 = multipartCopySseC(client, bucketName, sourceKey, bucketName, targetKey, size);
		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(targetKey).uploadId(uploadData2.uploadId)
						.multipartUpload(p -> p.parts(uploadData2.parts)));
		checkCopyContentSseC(client, bucketName, sourceKey, bucketName, targetKey);
	}

	@Test
	@Tag("Multipart")
	// SSE-C 설정하여 Multipart와 CopyPart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void testSseCEncryptionMultipartCopyMany() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, sourceKey, uploadData.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey1 = "my_multipart1";
		var uploadData2 = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey1, size, null);
		// 추가파츠 업로드
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(p -> p.parts(copyData1.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey1, copyData1.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey2 = "my_multipart2";
		var uploadData3 = multipartCopy(client, bucketName, targetKey1, bucketName, targetKey2, size * 2, null);
		// 추가파츠 업로드
		var copyData2 = multipartUpload(client, bucketName, targetKey2, size, uploadData3);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey2).uploadId(copyData2.uploadId)
				.multipartUpload(p -> p.parts(copyData2.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey2, copyData2.body.toString(), MainData.MB);
	}
}
