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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.util.Base64;
import java.util.HashMap;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.example.Data.FormFile;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SSE_C extends TestBase {

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("SSE_C Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("SSE_C End");
	}

	@Test
	@Tag("PutGet")
	public void testEncryptedTransfer1b() {
		testEncryptionSSECustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	public void testEncryptedTransfer1kb() {
		testEncryptionSSECustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	public void testEncryptedTransfer1MB() {
		testEncryptionSSECustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	public void testEncryptedTransfer13b() {
		testEncryptionSSECustomerWrite(13);
	}

	@Test
	@Tag("metadata")
	public void testEncryptionSseCMethodHead() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5(SSE_KEY_MD5);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));

		var e = assertThrows(AmazonServiceException.class, () -> client.getObjectMetadata(bucketName, key));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);

		client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
	}

	@Test
	@Tag("ERROR")
	public void testEncryptionSseCPresent() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5(SSE_KEY_MD5);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("ERROR")
	public void testEncryptionSseCOtherKey() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sseA = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5(SSE_KEY_MD5);

		var sseB = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sseA));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sseB)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("ERROR")
	public void testEncryptionSseCInvalidMd5() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("AAAAAAAAAAAAAAAAAAAAAA==");

		var e = assertThrows(AmazonServiceException.class, () -> client
				.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse)));

		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("ERROR")
	public void testEncryptionSseCNoMd5() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));
		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
		var body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	@Test
	@Tag("ERROR")
	public void testEncryptionSseCNoKey() {
		assertThrows(IllegalArgumentException.class,
				() -> new SSECustomerKey("").withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
	}

	@Test
	@Disabled("JAVA 에서는 algorithm값을 누락해도 기본값이 지정되어 있어 에러가 발생하지 않음")
	@Tag("ERROR")
	public void testEncryptionKeyNoSseC() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withMd5(SSE_KEY_MD5);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse)));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("Multipart")
	public void testEncryptionSseCMultipartUpload() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5(SSE_KEY_MD5);

		var uploadData = setupSseCMultipartUpload(client, bucketName, key, size, metadata);

		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		var body = uploadData.getBody();
		checkContentUsingRangeEnc(client, bucketName, key, body, MainData.MB, sse);
		checkContentUsingRangeEnc(client, bucketName, key, body, 10L * MainData.MB, sse);
		checkContentUsingRandomRangeEnc(client, bucketName, key, body, size, 100, sse);
	}

	@Test
	@Tag("Multipart")
	public void testEncryptionSseCMultipartBadDownload() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		metadata.setContentLength(size);

		var sseSet = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5(SSE_KEY_MD5);

		var sseGet = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		var uploadData = setupSseCMultipartUpload(client, bucketName, key, size, metadata);

		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sseSet));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sseGet)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Post")
	public void testEncryptionSseCPostObjectAuthenticatedRequest() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var client = getClientHttps();
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
		starts3.add("AES256");
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
		payload.put("x-amz-server-side-encryption-customer-algorithm", "AES256");
		payload.put("x-amz-server-side-encryption-customer-key", SSE_KEY);
		payload.put("x-amz-server-side-encryption-customer-key-md5", SSE_KEY_MD5);

		var sendURL = getURL(bucketName);
		var result = NetUtils.postUpload(sendURL, payload, fileData);
		assertEquals(204, result.statusCode);

		var response = client.getObject(new GetObjectRequest(bucketName, key)
				.withSSECustomerKey(new SSECustomerKey(SSE_KEY)));
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	@Test
	@Tag("Get")
	public void testEncryptionSseCGetObjectMany() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5(SSE_KEY_MD5);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));
		checkContentEnc(bucketName, key, data, 50, sse);
	}

	@Test
	@Tag("Get")
	public void testEncryptionSseCRangeObjectMany() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5(SSE_KEY_MD5);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));

		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());

		checkContentUsingRandomRangeEnc(client, bucketName, key, data, size, 50, sse);
	}

	@Test
	@Tag("Multipart")
	public void testSseCEncryptionMultipartCopyPartUpload() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5(SSE_KEY_MD5);

		var uploadData = setupSseCMultipartUpload(client, bucketName, sourceKey, size, metadata);

		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, sourceKey).withSSECustomerKey(sse));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		var targetKey = "multipartEncCopy";
		uploadData = multipartCopySseC(client, bucketName, sourceKey, bucketName, targetKey, size, metadata, sse);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey, uploadData.uploadId, uploadData.parts));
		checkCopyContentSseC(client, bucketName, sourceKey, bucketName, targetKey, sse);
	}

	@Test
	@Tag("Multipart")
	public void testSseCEncryptionMultipartCopyMany() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);
		var body = new StringBuilder();

		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "my_multipart1";
		uploadData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey1, size, metadata);
		uploadData = multipartUpload(client, bucketName, targetKey1, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey1, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "my_multipart2";
		uploadData = multipartCopy(client, bucketName, targetKey1, bucketName, targetKey2, size * 2, metadata);
		uploadData = multipartUpload(client, bucketName, targetKey2, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey2, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, targetKey2, body.toString(), MainData.MB);
	}
}
