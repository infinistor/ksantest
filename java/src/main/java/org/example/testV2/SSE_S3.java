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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

@SuppressWarnings("unchecked")
public class SSE_S3 extends TestBase {
	static final String SSE_ALGORITHM = "AES256";

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("SSE_S3 V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("SSE_S3 V2 End");
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1b() {
		testEncryptionSseS3Write(1);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1kb() {
		testEncryptionSseS3Write(1024);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer1MB() {
		testEncryptionSseS3Write(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3EncryptedTransfer13b() {
		testEncryptionSseS3Write(13);
	}

	@Test
	@Tag("Metadata")
	public void testSseS3EncryptionMethodHead() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "obj";
		var data = Utils.randomTextToLong(1000);
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata)
				.serverSideEncryption(ServerSideEncryption.AES256).build(), RequestBody.fromString(data));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());
		assertEquals(SSE_ALGORITHM, response.serverSideEncryptionAsString());
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var createMultiPartResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key)
				.serverSideEncryption(ServerSideEncryption.AES256).contentType(contentType).metadata(metadata));
		var uploadId = createMultiPartResponse.uploadId();

		var parts = Utils.generateRandomString(size, 5 * MainData.MB);
		var partETags = new ArrayList<CompletedPart>();
		int count = 1;
		var data = new StringBuilder();
		for (var part : parts) {
			var partNumber = count++;
			data.append(part);
			var partResPonse = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId)
					.partNumber(partNumber), RequestBody.fromString(part));
			partETags.add(CompletedPart.builder().partNumber(partNumber).eTag(partResPonse.eTag()).build());
		}

		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadId).multipartUpload(m -> m.parts(partETags)));
		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, getResponse.metadata());
		assertEquals(contentType, getResponse.contentType());
		assertEquals(SSE_ALGORITHM, getResponse.serverSideEncryptionAsString());

		checkContentUsingRange(bucketName, key, data.toString(), MainData.MB);
		checkContentUsingRange(bucketName, key, data.toString(), 10L * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, data.toString(), 100);
	}

	@Test
	@Tag("encryption")
	public void testGetBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		assertThrows(AwsServiceException.class, () -> client.getBucketEncryption(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("encryption")
	public void testPutBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));
	}

	@Test
	@Tag("encryption")
	public void testDeleteBucketEncryption() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(1, response.serverSideEncryptionConfiguration().rules().size());

		client.deleteBucketEncryption(d -> d.bucket(bucketName));

		assertThrows(AwsServiceException.class, () -> client.getBucketEncryption(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("encryption")
	public void testPutBucketEncryptionAndObjectSetCheck() {
		var keys = List.of("for/bar", "test/");
		var client = getClient();
		var bucketName = createEmptyObjects(client, keys);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		for (var key : keys) {
			var getResponse = client.headObject(g -> g.bucket(bucketName).key(key));
			assertEquals(ServerSideEncryption.AES256.toString(), getResponse.serverSideEncryptionAsString());
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

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var key = "bar";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));

		var getResponse = client.getObject(p -> p.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetUseChunkEncoding() {
		var client = getClientHttps(true);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var key = "bar";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));

		var getResponse = client.getObject(p -> p.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	public void testSseS3BucketPutGetNotChunkEncoding() {
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(1000);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var key = "bar";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));

		var getResponse = client.getObject(p -> p.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PresignedURL")
	public void testSseS3BucketPresignedUrlPutGet() {
		var client = getClient();
		var bucketName = createBucket(client);
		var signer = getS3Presigner();
		var key = "foo";

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var putURL = signer.presignPutObject(
				s -> s.signatureDuration(Duration.ofMinutes(10)).putObjectRequest(p -> p.bucket(bucketName).key(key)));
		var putResponse = putObject(putURL.url(), key);
		assertEquals(200, putResponse.getStatusLine().getStatusCode());

		var getURL = signer.presignGetObject(s -> s.signatureDuration(Duration.ofMinutes(10)).getObjectRequest(g -> g
				.bucket(bucketName).key(key)));
		var getResponse = getObject(getURL.url());
		assertEquals(200, getResponse.getStatusLine().getStatusCode());

	}

	@Test
	@Tag("PresignedURL")
	public void testSseS3BucketPresignedUrlPutGetV4() {
		var client = getClient(true);
		var bucketName = createBucket(client);
		var signer = getS3Presigner();
		var key = "foo";

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var putURL = signer.presignPutObject(s -> s.signatureDuration(Duration.ofMinutes(10)).putObjectRequest(p -> p
				.bucket(bucketName).key(key)));
		var putResponse = putObject(putURL.url(), key);
		assertEquals(200, putResponse.getStatusLine().getStatusCode());

		var getURL = signer.presignGetObject(s -> s.signatureDuration(Duration.ofMinutes(10)).getObjectRequest(g -> g
				.bucket(bucketName).key(key)));
		var getResponse = getObject(getURL.url());
		assertEquals(200, getResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	public void testSseS3GetObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));
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

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));
		checkContentUsingRandomRange(bucketName, key, data, 100);
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartCopyPartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var uploadData = setupSSEMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.headObject(h -> h.bucket(bucketName).key(sourceKey));
		assertEquals(metadata, getResponse.metadata());
		assertEquals(ServerSideEncryption.AES256.toString(), getResponse.serverSideEncryptionAsString());

		checkContentUsingRange(bucketName, sourceKey, uploadData.getBody(), MainData.MB);

		var targetKey = "multipartEncCopy";
		var copyData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey, size, metadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey).uploadId(copyData.uploadId)
				.multipartUpload(m -> m.parts(copyData.parts)));

		checkCopyContentUsingRange(bucketName, sourceKey, bucketName, targetKey, MainData.MB);
	}

	@Test
	@Tag("Multipart")
	public void testSseS3EncryptionMultipartCopyMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var sourceKey = "multipartEnc";
		var size = 10 * MainData.MB;
		var body = new StringBuilder();

		var uploadData = setupSSEMultipartUpload(client, bucketName, sourceKey, size, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "my_multipart1";
		var uploadData2 = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey1, size, null);
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(m -> m.parts(copyData1.parts)));

		body.append(uploadData2.body);

		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "my_multipart2";
		var uploadData3 = multipartCopy(client, bucketName, targetKey1, bucketName, targetKey2, size * 2, null);
		var copyData2 = multipartUpload(client, bucketName, targetKey2, size, uploadData3);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey2).uploadId(copyData2.uploadId)
				.multipartUpload(m -> m.parts(copyData2.parts)));

		body.append(uploadData3.body);

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

		client.putObject(p -> p.bucket(bucketName).key(putKey).build(), RequestBody.fromString(data));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(putKey).destinationBucket(bucketName)
				.destinationKey(copyKey));

		var uploadData = setupSSEMultipartUpload(client, bucketName, multiKey, 5 * MainData.MB, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(multiKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var getResponse = client.getObject(get -> get.bucket(bucketName).key(putKey));
		var body = getBody(getResponse);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		getResponse = client.getObject(get -> get.bucket(bucketName).key(copyKey));
		body = getBody(getResponse);
		assertEquals(data.length(), body.length());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		checkContentUsingRange(bucketName, multiKey, uploadData.body.toString(), MainData.MB);

		var putKey2 = "put2";
		var copyKey2 = "copy2";
		var multiKey2 = "multi2";
		var data2 = Utils.randomTextToLong(1000);
		client.putObject(p -> p.bucket(bucketName).key(putKey2).build(), RequestBody.fromString(data2));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(putKey2).destinationBucket(bucketName)
				.destinationKey(copyKey2));

		var uploadData2 = setupSSEMultipartUpload(client, bucketName, multiKey2, 5 * MainData.MB, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(multiKey2).uploadId(uploadData2.uploadId)
				.multipartUpload(m -> m.parts(uploadData2.parts)));

		client.deleteBucketEncryption(d -> d.bucket(bucketName));

		getResponse = client.getObject(get -> get.bucket(bucketName).key(putKey2));
		body = getBody(getResponse);
		assertEquals(data2.length(), body.length());
		assertTrue(data2.equals(body), MainData.NOT_MATCHED);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());

		getResponse = client.getObject(get -> get.bucket(bucketName).key(copyKey2));
		body = getBody(getResponse);
		assertEquals(data2.length(), body.length());
		assertTrue(data2.equals(body), MainData.NOT_MATCHED);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());

		checkContentUsingRange(bucketName, multiKey2, uploadData2.body.toString(), MainData.MB);
	}
}
