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
	// cSpell:disable
	static final String SSE_ALGORITHM = "AES256";

	// cSpell:enable
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
	// 1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testSseS3EncryptedTransfer1b() {
		testEncryptionSseS3CustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	// 1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testSseS3EncryptedTransfer1kb() {
		testEncryptionSseS3CustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	// 1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testSseS3EncryptedTransfer1MB() {
		testEncryptionSseS3CustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// 13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testSseS3EncryptedTransfer13b() {
		testEncryptionSseS3CustomerWrite(13);
	}

	@Test
	@Tag("Metadata")
	// SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	public void testSseS3EncryptionMethodHead() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var data = Utils.randomTextToLong(1000);
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata)
				.serverSideEncryption(ServerSideEncryption.AES256).build(), RequestBody.fromString(data));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());
		assertEquals(SSE_ALGORITHM, response.serverSideEncryptionAsString());
	}

	@Test
	@Tag("Multipart")
	// 멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	public void testSseS3EncryptionMultipartUpload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");

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
	// 버킷의 SSE-S3 설정 확인
	public void testGetBucketEncryption() {
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AwsServiceException.class, () -> client.getBucketEncryption(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("encryption")
	@Tag("KSAN")
	// 버킷의 SSE-S3 설정이 가능한지 확인
	public void testPutBucketEncryption() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));
	}

	@Test
	@Tag("encryption")
	// 버킷의 SSE-S3 설정 삭제가 가능한지 확인
	public void testDeleteBucketEncryption() {
		var bucketName = getNewBucket();
		var client = getClient();

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
	// 버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	public void testPutBucketEncryptionAndObjectSetCheck() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		var key = "foo";
		var data = Utils.randomTextToLong(1000);
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));

		var getHeadResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(ServerSideEncryption.AES256, getHeadResponse.serverSideEncryption());
	}

	@Test
	@Tag("CopyObject")
	// 버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	public void testCopyObjectEncryption1kb() {
		testEncryptionSseS3Copy(1024);
	}

	@Test
	@Tag("CopyObject")
	// 버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	public void testCopyObjectEncryption256kb() {
		testEncryptionSseS3Copy(256 * 1024);
	}

	@Test
	@Tag("CopyObject")
	// 버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	public void testCopyObjectEncryption1mb() {
		testEncryptionSseS3Copy(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// [버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	public void testSseS3BucketPutGet() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// [버킷에 SSE-S3 설정, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
	public void testSseS3BucketPutGetUseChunkEncoding() {
		var bucketName = getNewBucket();
		var client = getClientHttps(true);
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
	// [버킷에 SSE-S3 설정, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
	public void testSseS3BucketPutGetNotChunkEncoding() {
		var bucketName = getNewBucket();
		var client = getClientHttps(false);
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
	// [버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void testSseS3BucketPresignedUrlPutGet() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// [버킷에 SSE-S3 설정, resignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void testSseS3BucketPresignedUrlPutGetV4() {
		var bucketName = getNewBucket();
		var client = getClient(true);
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
	// SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void testSseS3GetObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void testSseS3RangeObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 multipart copy 로 복사 가능한지 확인
	public void testSseS3EncryptionMultipartCopyPartUpload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var sourceKey = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");

		// 멀티파트 업로드
		var uploadData = setupSSEMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		// 올바르게 업로드 되었는지 확인
		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.headObject(h -> h.bucket(bucketName).key(sourceKey));
		assertEquals(metadata, getResponse.metadata());
		assertEquals(contentType, getResponse.contentType());
		assertEquals(ServerSideEncryption.AES256.toString(), getResponse.serverSideEncryptionAsString());

		checkContentUsingRange(bucketName, sourceKey, uploadData.getBody(), MainData.MB);

		// 멀티파트 복사
		var targetKey = "multipartEncCopy";
		var copyData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey, size, metadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey).uploadId(copyData.uploadId)
				.multipartUpload(m -> m.parts(copyData.parts)));

		// 올바르게 복사 되었는지 확인
		checkCopyContentUsingRange(bucketName, sourceKey, bucketName, targetKey, MainData.MB);
	}

	@Test
	@Tag("Multipart")
	// SSE-S3 설정하여 Multipart와 Copy part를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void testSseS3EncryptionMultipartCopyMany() {
		var bucketName = getNewBucket();
		var client = getClient();
		var sourceKey = "multipartEnc";
		var size = 10 * MainData.MB;

		// 멀티파트 업로드
		var uploadData = setupSSEMultipartUpload(client, bucketName, sourceKey, size, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, sourceKey, uploadData.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey1 = "my_multipart1";
		var uploadData2 = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey1, size, null);
		// 추가파츠 업로드
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(m -> m.parts(copyData1.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey1, copyData1.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey2 = "my_multipart2";
		var uploadData3 = multipartCopy(client, bucketName, targetKey1, bucketName, targetKey2, size * 2, null);
		// 추가파츠 업로드
		var copyData2 = multipartUpload(client, bucketName, targetKey2, size, uploadData3);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey2).uploadId(copyData2.uploadId)
				.multipartUpload(m -> m.parts(copyData2.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey2, copyData2.body.toString(), MainData.MB);
	}

	@Test
	@Tag("Retroactive")
	// sse-s3설정은 소급적용 되지 않음을 확인
	public void testSseS3NotRetroactive() {
		var bucketName = getNewBucket();
		var client = getClient();
		var data = Utils.randomTextToLong(1000);

		var putKey = "put";
		var copyKey = "copy";
		var multiKey = "multi";

		// 일반 파일 업로드
		client.putObject(p -> p.bucket(bucketName).key(putKey).build(), RequestBody.fromString(data));

		// 일반 파일 복사
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(putKey).destinationBucket(bucketName)
				.destinationKey(copyKey));

		// 멀티파트 업로드
		var uploadData = setupSSEMultipartUpload(client, bucketName, multiKey, 5 * MainData.MB, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(multiKey).uploadId(uploadData.uploadId)
				.multipartUpload(m -> m.parts(uploadData.parts)));

		// SSE-S3 설정
		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(
				s -> s.rules(r -> r
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		// SSE-S3 설정이 올바르게 되었는지 확인
		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(ServerSideEncryption.AES256, response.serverSideEncryptionConfiguration().rules().get(0)
				.applyServerSideEncryptionByDefault().sseAlgorithm());

		// SSE-S3 설정이 적용되지 않은 파일들이 올바르게 복사되었는지 확인
		var getResponse = client.getObject(get -> get.bucket(bucketName).key(putKey));
		var body = getBody(getResponse);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		getResponse = client.getObject(get -> get.bucket(bucketName).key(copyKey));
		body = getBody(getResponse);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		checkContentUsingRange(bucketName, multiKey, uploadData.body.toString(), MainData.MB);

		var putKey2 = "put2";
		var copyKey2 = "copy2";
		var multiKey2 = "multi2";
		var data2 = Utils.randomTextToLong(1000);
		client.putObject(p -> p.bucket(bucketName).key(putKey2).build(), RequestBody.fromString(data2));

		// 일반 파일 복사
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(putKey2).destinationBucket(bucketName)
				.destinationKey(copyKey2));

		// 멀티파트 업로드
		var uploadData2 = setupSSEMultipartUpload(client, bucketName, multiKey2, 5 * MainData.MB, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(multiKey2).uploadId(uploadData2.uploadId)
				.multipartUpload(m -> m.parts(uploadData2.parts)));

		client.deleteBucketEncryption(d -> d.bucket(bucketName));

		getResponse = client.getObject(get -> get.bucket(bucketName).key(putKey2));
		body = getBody(getResponse);
		assertTrue(data2.equals(body), MainData.NOT_MATCHED);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());

		getResponse = client.getObject(get -> get.bucket(bucketName).key(copyKey2));
		body = getBody(getResponse);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(ServerSideEncryption.AES256.toString(),
				getResponse.response().serverSideEncryptionAsString());

		checkContentUsingRange(bucketName, multiKey2, uploadData2.body.toString(), MainData.MB);
	}
}
