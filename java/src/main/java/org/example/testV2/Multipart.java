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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Data.MultipartUploadV2Data;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.InvalidRequestException;

public class Multipart extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Multipart V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Multipart V2 End");
	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client
						.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
								.multipartUpload(p -> p.parts(uploadData.parts))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("Check")
	public void testMultipartUploadSmall() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(size, response.response().contentLength());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopySmall() {
		var sourceKey = "foo";
		var targetKey = "multipart";
		var size = 1;

		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 0);
		var targetBucketName = createBucket(client);

		var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		assertEquals(size, response.response().contentLength());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartCopyInvalidRange() {
		var client = getClient();
		var sourceKey = "source";
		var bucketName = createKeyWithRandomContent(client, sourceKey, 5);

		var targetKey = "dest";
		var response = client.createMultipartUpload(c -> c.bucket(bucketName).key(targetKey));
		var uploadId = response.uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(
						c -> c.sourceBucket(bucketName).sourceKey(sourceKey).destinationBucket(bucketName)
								.destinationKey(targetKey)
								.uploadId(uploadId).partNumber(1).copySourceRange("bytes=0-21")));

		assertTrue(e.statusCode() == HttpStatus.SC_BAD_REQUEST || e.statusCode() == 416);
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("Range")
	public void testMultipartCopyWithoutRange() {
		var client = getClient();
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 10);
		var targetBucketName = createBucket(client);
		var targetKey = "my_multipart_copy";

		var initResponse = client
				.createMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey));
		var uploadId = initResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();

		var copyResponse = client.uploadPartCopy(c -> c.sourceBucket(sourceBucketName).sourceKey(sourceKey)
				.destinationBucket(targetBucketName).destinationKey(targetKey).uploadId(uploadId).partNumber(1)
				.copySourceRange("bytes=0-9"));
		parts.add(CompletedPart.builder().partNumber(1).eTag(copyResponse.copyPartResult().eTag()).build());
		client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadId)
				.multipartUpload(p -> p.parts(parts)));

		var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		assertEquals(10, response.response().contentLength());

	}

	@Test
	@Tag("SpecialNames")
	public void testMultipartCopySpecialNames() {
		var sourceKeys = List.of(" ", "_", "__", "?versionId");
		var targetKey = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		for (var sourceKey : sourceKeys) {
			createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(
					c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
							.multipartUpload(p -> p.parts(uploadData.parts)));
			var response = client.headObject(g -> g.bucket(targetBucketName).key(targetKey));
			assertEquals(size, response.contentLength());
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey, MainData.MB);
		}

	}

	@Test
	@Tag("Put")
	public void testMultipartUpload() {
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		var listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = listResponse.keyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(listResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, getResponse.metadata());

		var body = uploadData.getBody();

		checkContentUsingRange(bucketName, key, body, MainData.MB);
		checkContentUsingRange(bucketName, key, body, 10L * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, body, 100);

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyVersioned() {
		var targetKey = "my_multipart";
		var sourceKey = "foo";
		var size = 15 * MainData.MB;
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);

		var versionIds = new ArrayList<String>();
		var listResponse = client.listObjectVersions(l -> l.bucket(sourceBucketName));
		for (var version : listResponse.versions())
			versionIds.add(version.versionId());

		for (var versionId : versionIds) {
			var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, null, 0,
					versionId);
			client.completeMultipartUpload(
					c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
							.multipartUpload(p -> p.parts(uploadData.parts)));
			var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
			assertEquals(size, response.response().contentLength());
			checkCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}

	}

	@Test
	@Tag("Duplicate")
	public void testMultipartUploadResendPart() {
		var key = "multipart";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		checkUploadMultipartResend(bucketName, key, size, List.of(0));
		checkUploadMultipartResend(bucketName, key, size, List.of(1));
		checkUploadMultipartResend(bucketName, key, size, List.of(2));
		checkUploadMultipartResend(bucketName, key, size, List.of(1, 2));
		checkUploadMultipartResend(bucketName, key, size, List.of(0, 1, 2, 3, 4, 5));

	}

	@Test
	@Tag("Put")
	public void testMultipartUploadMultipleSizes() {
		var key = "my_multipart";
		var client = getClient();
		var bucketName = createBucket(client);

		var sizeList = List.of(5 * MainData.MB, 5 * MainData.MB + 100 * MainData.KB,
				5 * MainData.MB + 600 * MainData.KB, 10 * MainData.MB, 10 * MainData.MB + 100 * MainData.KB,
				10 * MainData.MB + 600 * MainData.KB);

		for (var size : sizeList) {
			var uploadData = setupMultipartUpload(client, bucketName, key, size);
			client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
					.multipartUpload(p -> p.parts(uploadData.parts)));
		}

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyMultipleSizes() {
		var sourceKey = "source";
		var targetKey = "target";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 12 * MainData.MB);
		var targetBucketName = createBucket(client);

		var sizeList = List.of(5 * MainData.MB, 5 * MainData.MB + 100 * MainData.KB,
				5 * MainData.MB + 600 * MainData.KB, 10 * MainData.MB, 10 * MainData.MB + 100 * MainData.KB,
				10 * MainData.MB + 600 * MainData.KB);

		for (var size : sizeList) {
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(
					c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
							.multipartUpload(p -> p.parts(uploadData.parts)));
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);
		}

	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadSizeTooSmall() {
		var key = "multipart";
		var client = getClient();
		var bucketName = createBucket(client);
		var content = Utils.randomTextToLong(10 * MainData.KB);

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();

		for (int i = 0; i < 10; i++) {
			var partNumber = i + 1;
			var partResponse = client.uploadPart(
					u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(partNumber),
					RequestBody.fromString(content));
			parts.add(CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build());
		}

		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadId)
						.multipartUpload(p -> p.parts(parts))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.ENTITY_TOO_SMALL, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("Check")
	public void testMultipartUploadContents() {
		var bucketName = createBucket();
		doTestMultipartUploadContents(bucketName, "multipart", 3);

	}

	@Test
	@Tag("OverWrite")
	public void testMultipartUploadOverwriteExistingObject() {
		var key = "multipart";
		var partCount = 2;
		var client = getClient();
		var bucketName = createBucket(client);
		var content = Utils.randomTextToLong(5 * MainData.MB);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();
		var totalContent = new StringBuilder();

		for (int i = 0; i < partCount; i++) {
			var partNumber = i + 1;
			var partResponse = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId)
					.partNumber(partNumber), RequestBody.fromString(content));
			parts.add(CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build());
			totalContent.append(content);
		}

		client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadId).multipartUpload(p -> p.parts(parts)));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertTrue(totalContent.toString().equals(body), MainData.NOT_MATCHED);

	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUpload() {
		var key = "multipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(0, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(0, bytesUsed);

	}

	@Test
	@Tag("ERROR")
	public void testAbortMultipartUploadNotFound() {
		var key = "my_multipart";
		var client = getClient();
		var bucketName = createBucket(client);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var e = assertThrows(AwsServiceException.class,
				() -> client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId("nonexistent")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("List")
	public void testListMultipartUpload() {
		var key = "my_multipart";
		var key2 = "my_multipart2";
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData1 = setupMultipartUpload(client, bucketName, key, 5 * MainData.MB);
		var uploadData2 = setupMultipartUpload(client, bucketName, key, 6 * MainData.MB);
		var uploadData3 = setupMultipartUpload(client, bucketName, key2, 5 * MainData.MB);

		var uploadIds = List.of(uploadData1.uploadId, uploadData2.uploadId, uploadData3.uploadId);

		var response = client.listMultipartUploads(l -> l.bucket(bucketName));
		var getUploadIds = new ArrayList<String>();

		for (var uploadData : response.uploads()) {
			getUploadIds.add(uploadData.uploadId());
		}

		for (var uploadId : uploadIds) {
			assertTrue(getUploadIds.contains(uploadId));
		}

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadIds.get(0)));
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadIds.get(1)));
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key2).uploadId(uploadIds.get(2)));
	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadMissingPart() {
		var key = "my_multipart";
		var body = "test";
		var client = getClient();
		var bucketName = createBucket(client);

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var parts = new ArrayList<CompletedPart>();
		var partResponse = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString(body));
		parts.add(CompletedPart.builder().partNumber(9999).eTag(partResponse.eTag()).build());

		var e = assertThrows(AwsServiceException.class, () -> client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadId).multipartUpload(p -> p.parts(parts))));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_PART, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadIncorrectEtag() {
		var key = "my_multipart";
		var client = getClient();
		var bucketName = createBucket(client);

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var parts = new ArrayList<CompletedPart>();
		client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString("test"));
		parts.add(CompletedPart.builder().partNumber(1).eTag("ffffffffffffffffffffffffffffffff").build());

		var e = assertThrows(AwsServiceException.class, () -> client
				.completeMultipartUpload(
						c -> c.bucket(bucketName).key(key).uploadId(uploadId)
								.multipartUpload(p -> p.parts(parts))));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_PART, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("Overwrite")
	public void testAtomicMultipartUploadWrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));

		response = client.getObject(g -> g.bucket(bucketName).key(key));
		body = getBody(response);
		assertEquals("bar", body);

	}

	@Test
	@Tag("List")
	public void testMultipartUploadList() {
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);

		var response = client.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.parts());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUploadList() {
		var key = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

		var listResponse = client.listMultipartUploads(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.uploads().size());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyMany() {
		var sourceKey = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);
		var body = new StringBuilder();

		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "my_multipart1";
		var uploadData2 = multipartCopy(bucketName, sourceKey, bucketName, targetKey1, size, client, 0, null);
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(p -> p.parts(copyData1.parts)));
		body.append(copyData1.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "my_multipart2";
		var uploadData3 = multipartCopy(bucketName, targetKey1, bucketName, targetKey2, size * 2, client, 0, null);
		var copyData2 = multipartUpload(client, bucketName, targetKey2, size, uploadData3);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey2).uploadId(copyData2.uploadId)
				.multipartUpload(p -> p.parts(copyData2.parts)));

		body.append(copyData2.body);
		checkContentUsingRange(bucketName, targetKey2, body.toString(), MainData.MB);

	}

	@Test
	@Tag("List")
	public void testMultipartListParts() {
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 1 * MainData.MB);

		var index = 0;
		while (true) {
			var partNumber = index;
			var response = client.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
					.maxParts(10).partNumberMarker(partNumber));

			assertEquals(10, response.parts().size());
			partsETagCompare(uploadData.parts.subList(index, index + 10), response.parts());
			if (Boolean.TRUE.equals(response.isTruncated())) {
				index = response.nextPartNumberMarker();
			} else {
				break;
			}
		}

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
	}

	@Test
	@Tag("checksum")
	public void testMultipartUploadChecksumUseChunkEncoding() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption,
				ChecksumType checksumType,
				List<ChecksumAlgorithm> checksums) {
		}

		var bucketName = createBucket();

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.CRC64_NVME);

		var compositeChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256);

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums));

		for (var config : configs) {
			var client = getClient(true, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(true, config.requestOption, config.responseOption);

			for (var checksum : config.checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();
				var asyncKey = prefix + "/async/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();

				multipartUpload(client, bucketName, key, config.checksumType, checksum);
				multipartUpload(asyncClient, bucketName, asyncKey, config.checksumType, checksum);
			}
		}
	}

	@Test
	@Tag("checksum")
	public void testMultipartUploadChecksum() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption,
				ChecksumType checksumType,
				List<ChecksumAlgorithm> checksums) {
		}

		var bucketName = createBucket();

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.CRC64_NVME);

		var compositeChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256);

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums));

		for (var config : configs) {
			var client = getClient(false, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(false, config.requestOption, config.responseOption);

			for (var checksum : config.checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();
				var asyncKey = prefix + "/async/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();

				multipartUpload(client, bucketName, key, config.checksumType, checksum);

				var e = assertThrows(CompletionException.class,
						() -> multipartUpload(asyncClient, bucketName, asyncKey, config.checksumType, checksum));
				var e2 = (AwsServiceException) e.getCause();
				assertEquals(HttpStatus.SC_BAD_REQUEST, e2.statusCode());
				assertEquals(MainData.INVALID_REQUEST, e2.awsErrorDetails().errorCode());
			}
		}
	}

	@Test
	@Tag("checksum-failure")
	public void testMultipartUploadChecksumFailure() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption,
				ChecksumType checksumType,
				List<ChecksumAlgorithm> checksums) {
		}

		var bucketName = createBucket();

		// FULL_OBJECT 타입에서 지원되지 않는 체크섬 알고리즘
		var unsupportedFullObjectChecksums = List.of(
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256);

		// COMPOSITE 타입에서 지원되지 않는 체크섬 알고리즘
		var unsupportedCompositeChecksums = List.of(
				ChecksumAlgorithm.CRC64_NVME);

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, unsupportedFullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, unsupportedFullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, unsupportedFullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, unsupportedFullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, unsupportedCompositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, unsupportedCompositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, unsupportedCompositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, unsupportedCompositeChecksums));

		for (var config : configs) {
			var client = getClient(true, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(true, config.requestOption, config.responseOption);

			for (var checksum : config.checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();
				var asyncKey = prefix + "/async/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();

				var e = assertThrows(InvalidRequestException.class,
						() -> multipartUpload(client, bucketName, key, config.checksumType, checksum));
				assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
				assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());

				var e2 = assertThrows(CompletionException.class,
						() -> multipartUpload(asyncClient, bucketName, asyncKey, config.checksumType, checksum));
				var e3 = (AwsServiceException) e2.getCause();
				assertEquals(HttpStatus.SC_BAD_REQUEST, e3.statusCode());
				assertEquals(MainData.INVALID_REQUEST, e3.awsErrorDetails().errorCode());
			}
		}
	}

	@Test
	@Tag("checksum")
	public void testMultipartCopyChecksum() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption,
				ChecksumType checksumType,
				List<ChecksumAlgorithm> checksums) {
		}

		var bucketName = createBucket();

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.CRC64_NVME);

		var compositeChecksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256);

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.FULL_OBJECT, fullObjectChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED,
						ChecksumType.COMPOSITE, compositeChecksums),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED,
						ChecksumType.COMPOSITE, compositeChecksums));

		for (var config : configs) {
			var client = getClient(true, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(true, config.requestOption, config.responseOption);

			for (var checksum : config.checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				var key = prefix + "/sync/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();
				var asyncKey = prefix + "/async/" + config.checksumType().name().toLowerCase() + "/" + checksum.name();

				multipartUpload(client, bucketName, key, config.checksumType, checksum);
				multipartCopy(client, bucketName, key, bucketName, key, checksum);
				multipartUpload(asyncClient, bucketName, asyncKey, config.checksumType, checksum);
				multipartCopy(asyncClient, bucketName, asyncKey, bucketName, asyncKey, checksum);
			}
		}
	}

	@Test
	@Tag("checksum")
	public void testcreateMultipartUploadEmptyChecksumAlgorithm() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testMultipartUploadChecksumType";
		var checksumType = ChecksumType.FULL_OBJECT;

		var e = assertThrows(InvalidRequestException.class,
				() -> client.createMultipartUpload(c -> c
						.bucket(bucketName)
						.key(key)
						.checksumType(checksumType)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("checksum")
	public void testcreateMultipartUploadEmptyChecksumType() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testMultipartUploadChecksumType";
		var size = 10 * MainData.MB;
		var partSize = 5 * MainData.MB;
		var checksumType = ChecksumType.COMPOSITE;
		var checksum = ChecksumAlgorithm.CRC32;
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.checksumAlgorithm(checksum));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.checksumAlgorithm(checksum)
					.partNumber(uploadData.nextPartNumber()),
					RequestBody.fromString(Part));
			checksumCompare(checksum, Part, partResponse);
			uploadData.addPart(checksum, partResponse);
		}

		var completeResponse = client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadData.uploadId)
				.checksumType(checksumType)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		checksumCompare(checksum, uploadData, completeResponse);
	}

}
