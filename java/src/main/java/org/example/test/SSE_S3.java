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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class SSE_S3 extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("SSE_S3 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("SSE_S3 End");
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1b() {
		testEncryptionSseS3CustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1kb() {
		testEncryptionSseS3CustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1MB() {
		testEncryptionSseS3CustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer13b() {
		testEncryptionSseS3CustomerWrite(13);
	}

	@Test
	@Tag("Metadata")
	public void testSseS3EncryptionMethodHead() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "obj";
		var data = Utils.randomTextToLong(1000);
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");

		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		client.putObject(bucketName, key, createBody(data), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals("AES256", response.getSSEAlgorithm());
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadata));
		var uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, 5 * MainData.MB);
		var partETags = new ArrayList<PartETag>();
		int partNumber = 1;
		var data = new StringBuilder();
		for (var part : parts) {
			data.append(part);
			var partResPonse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(uploadId).withPartNumber(partNumber++).withInputStream(createBody(part))
					.withPartSize(part.length()));
			partETags.add(partResPonse.getPartETag());
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags));
		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());
		assertEquals(contentType, getResponse.getContentType());
		assertEquals("AES256", getResponse.getSSEAlgorithm());

		checkContentUsingRange(bucketName, key, data.toString(), MainData.MB);
		checkContentUsingRange(bucketName, key, data.toString(), 10L * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, data.toString(), 100);
	}

	@Test
	@Tag("encryption")
	public void testGetBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonS3Exception.class, () -> client.getBucketEncryption(bucketName));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("encryption")
	public void testPutBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));
	}

	@Test
	@Tag("encryption")
	public void testDeleteBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(1, response.getServerSideEncryptionConfiguration().getRules().size());

		client.deleteBucketEncryption(bucketName);

		var e = assertThrows(AmazonS3Exception.class, () -> client.getBucketEncryption(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("encryption")
	public void testPutBucketEncryptionAndObjectSetCheck() {
		var keys = List.of("for/bar", "test/");
		var client = getClient();
		var bucketName = createEmptyObjects(client, keys);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		for (var key : keys) {
			var getResponse = client.getObjectMetadata(bucketName, key);
			assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getSSEAlgorithm());
		}
	}

	@Test
	@Tag("CopyObject")
	public void testCopyObjectEncryption1kb() {
		testEncryptionSseS3Copy(1024);
	}

	@Test
	@Tag("CopyObject")
	public void testCopyObjectEncryption256kb() {
		testEncryptionSseS3Copy(256 * 1024);
	}

	@Test
	@Tag("CopyObject")
	public void testCopyObjectEncryption1mb() {
		testEncryptionSseS3Copy(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGet() {
		var client = getClient();
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetV4() {
		var client = getClientV4(true, true);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetUseChunkEncoding() {
		var client = getClientHttpsV4(true, false);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning() {
		var client = getClientHttpsV4(true, true);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetNotChunkEncoding() {
		var client = getClientHttpsV4(false, false);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning() {
		var client = getClientHttpsV4(false, true);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PresignedURL")
	public void testSseS3BucketPresignedUrlPutGet() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());

	}

	@Test
	@Tag("PresignedURL")
	public void testSseS3BucketPresignedUrlPutGetV4() {
		var client = getClientV4(true, true);
		var bucketName = createBucket(client);
		var key = "foo";

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	public void testSseS3GetObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		client.putObject(bucketName, key, data);
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	public void testSseS3RangeObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		client.putObject(bucketName, key, data);
		checkContentUsingRandomRange(bucketName, key, data, 100);
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartCopyPartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);

		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, sourceKey);
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());
		assertEquals(contentType, getResponse.getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getSSEAlgorithm());

		checkContentUsingRange(bucketName, sourceKey, uploadData.getBody(), MainData.MB);

		var targetKey = "multipartEncCopy";
		uploadData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey, size, metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey, uploadData.uploadId, uploadData.parts));

		checkCopyContentUsingRange(bucketName, sourceKey, bucketName, targetKey, MainData.MB);
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartCopyMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 10 * MainData.MB;
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

	@Test
	@Tag("Retroactive")
	public void testSseS3NotRetroactive() {
		var client = getClient();
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		var putKey = "put";
		var copyKey = "copy";
		var multiKey = "multi";

		client.putObject(bucketName, putKey, data);

		client.copyObject(bucketName, putKey, bucketName, copyKey);

		var uploadData = setupMultipartUpload(client, bucketName, multiKey, 5 * MainData.MB, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, multiKey, uploadData.uploadId, uploadData.parts));

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var getResponse = client.getObject(bucketName, putKey);
		var body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		getResponse = client.getObject(bucketName, copyKey);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		checkContentUsingRange(bucketName, multiKey, uploadData.body.toString(), MainData.MB);

		var putKey2 = "put2";
		var copyKey2 = "copy2";
		var multiKey2 = "multi2";
		var data2 = Utils.randomTextToLong(1000);
		client.putObject(bucketName, putKey2, data2);

		client.copyObject(bucketName, putKey, bucketName, copyKey2);

		var uploadData2 = setupMultipartUpload(client, bucketName, multiKey2, 5 * MainData.MB);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, multiKey2, uploadData2.uploadId, uploadData2.parts));

		client.deleteBucketEncryption(bucketName);

		getResponse = client.getObject(bucketName, putKey2);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data2.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());

		getResponse = client.getObject(bucketName, copyKey2);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());

		checkContentUsingRange(bucketName, multiKey2, uploadData2.body.toString(), MainData.MB);
	}
}
