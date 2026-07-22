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

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Data.MultipartUploadV2Data;
import org.example.Utility.CheckSum;
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
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;

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
		var bucketName = createBucket(client, 1);
		var key = "testMultipartUploadEmpty";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client
						.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
								.multipartUpload(p -> p.parts(uploadData.parts))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
	}

	@Test
	@Tag("Check")
	public void testMultipartUploadSmall() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var key = "testMultipartUploadSmall";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		var response = client.headObject(g -> g.bucket(bucketName).key(key));
		assertEquals(size, response.contentLength());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopySmall() {
		var sourceKey = "foo";
		var targetKey = "testMultipartCopySmall";
		var size = 1;

		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 3, sourceKey, 0);
		var targetBucketName = createBucket(client, 3);

		var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		var response = client.headObject(g -> g.bucket(targetBucketName).key(targetKey));
		assertEquals(size, response.contentLength());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartCopyInvalidRange() {
		var client = getClient();
		var sourceKey = "source";
		var bucketName = createKeyWithRandomContent(client, 4, sourceKey, 5);

		var targetKey = "testMultipartCopyInvalidRange";
		var response = client.createMultipartUpload(c -> c.bucket(bucketName).key(targetKey));
		var uploadId = response.uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(
						c -> c.sourceBucket(bucketName).sourceKey(sourceKey).destinationBucket(bucketName)
								.destinationKey(targetKey)
								.uploadId(uploadId).partNumber(1).copySourceRange("bytes=0-21")));

		assertTrue(e.statusCode() == HttpStatus.SC_BAD_REQUEST || e.statusCode() == 416);
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(targetKey).uploadId(uploadId));
	}

	@Test
	@Tag("Range")
	public void testMultipartCopyWithoutRange() {
		var client = getClient();
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(client, 5, sourceKey, 10);
		var targetBucketName = createBucket(client, 5);
		var targetKey = "testMultipartCopyWithoutRange";

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

		var response = client.headObject(g -> g.bucket(targetBucketName).key(targetKey));
		assertEquals(10, response.contentLength());
	}

	@Test
	@Tag("SpecialNames")
	public void testMultipartCopySpecialNames() {
		var sourceKeys = List.of(" ", "_", "__", "?versionId");
		var targetKey = "testMultipartCopySpecialNames";
		var size = 10 * MainData.MB;
		var client = getClient();
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

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
		var key = "testMultipartUpload";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");
		var client = getClient();
		var bucketName = createBucket(client, 7);

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
		var targetKey = "testMultipartCopyVersioned";
		var sourceKey = "foo";
		var size = 15 * MainData.MB;
		var client = getClient();
		var sourceBucketName = createBucket(client, 8);
		var targetBucketName = createBucket(client, 8);

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
			var response = client.headObject(g -> g.bucket(targetBucketName).key(targetKey));
			assertEquals(size, response.contentLength());
			checkCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}

	}

	@Test
	@Tag("Duplicate")
	public void testMultipartUploadResendPart() {
		var key = "testMultipartUploadResendPart";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 9);

		checkUploadMultipartResend(bucketName, key, size, List.of(0));
		checkUploadMultipartResend(bucketName, key, size, List.of(1));
		checkUploadMultipartResend(bucketName, key, size, List.of(2));
		checkUploadMultipartResend(bucketName, key, size, List.of(1, 2));
		checkUploadMultipartResend(bucketName, key, size, List.of(0, 1, 2, 3, 4, 5));

	}

	@Test
	@Tag("Put")
	public void testMultipartUploadMultipleSizes() {
		var key = "testMultipartUploadMultipleSizes";
		var client = getClient();
		var bucketName = createBucket(client, 10);

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
		var targetKey = "testMultipartCopyMultipleSizes";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 11, sourceKey, 12 * MainData.MB);
		var targetBucketName = createBucket(client, 11);

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
		var key = "testMultipartUploadSizeTooSmall";
		var client = getClient();
		var bucketName = createBucket(client, 12);
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

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));
	}

	@Test
	@Tag("Check")
	public void testMultipartUploadContents() {
		var bucketName = createBucket(13);
		doTestMultipartUploadContents(bucketName, "testMultipartUploadContents", 3);

	}

	@Test
	@Tag("OverWrite")
	public void testMultipartUploadOverwriteExistingObject() {
		var key = "testMultipartUploadOverwriteExistingObject";
		var partCount = 2;
		var client = getClient();
		var bucketName = createBucket(client, 14);
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
		var key = "testAbortMultipartUpload";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 15);

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
		var key = "testAbortMultipartUploadNotFound";
		var client = getClient();
		var bucketName = createBucket(client, 16);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var e = assertThrows(AwsServiceException.class,
				() -> client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId("nonexistent")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("List")
	public void testListMultipartUpload() {
		var key = "testListMultipartUpload";
		var key2 = "testListMultipartUpload2";
		var client = getClient();
		var bucketName = createBucket(client, 17);

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
		var key = "testMultipartUploadMissingPart";
		var body = "test";
		var client = getClient();
		var bucketName = createBucket(client, 18);

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

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadIncorrectEtag() {
		var key = "testMultipartUploadIncorrectEtag";
		var client = getClient();
		var bucketName = createBucket(client, 19);

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

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));
	}

	@Test
	@Tag("Overwrite")
	public void testAtomicMultipartUploadWrite() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		var key = "testAtomicMultipartUploadWrite";
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
		var key = "testMultipartUploadList";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 21);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);

		var response = client.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.parts());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUploadList() {
		var key = "testAbortMultipartUploadList";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 22);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

		var listResponse = client.listMultipartUploads(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.uploads().size());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyMany() {
		var sourceKey = "testMultipartCopyMany";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 23);
		var body = new StringBuilder();

		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "testMultipartCopyMany1";
		var uploadData2 = multipartCopy(bucketName, sourceKey, bucketName, targetKey1, size, client, 0, null);
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(p -> p.parts(copyData1.parts)));
		body.append(copyData1.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "testMultipartCopyMany2";
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
		var key = "testMultipartListParts";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 24);

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

		var bucketName = createBucket(25);

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = CheckSum.FULL_OBJECT_ALGORITHMS;
		var compositeChecksums = CheckSum.COMPOSITE_ALGORITHMS;

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

		var bucketName = createBucket(26);

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = CheckSum.FULL_OBJECT_ALGORITHMS;
		var compositeChecksums = CheckSum.COMPOSITE_ALGORITHMS;

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
				multipartUpload(asyncClient, bucketName, asyncKey, config.checksumType, checksum);
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

		var bucketName = createBucket(27);

		// FULL_OBJECT 타입에서 지원되지 않는 체크섬 알고리즘 (CRC 계열 이외 전체)
		var unsupportedFullObjectChecksums = List.of(
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256,
				ChecksumAlgorithm.MD5,
				ChecksumAlgorithm.SHA512,
				ChecksumAlgorithm.XXHASH64,
				ChecksumAlgorithm.XXHASH3,
				ChecksumAlgorithm.XXHASH128);

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

		var bucketName = createBucket(28);

		// FULL_OBJECT와 COMPOSITE 타입의 체크섬 알고리즘 정의
		var fullObjectChecksums = CheckSum.FULL_OBJECT_ALGORITHMS;
		var compositeChecksums = CheckSum.COMPOSITE_ALGORITHMS;

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
		var bucketName = createBucket(client, 29);
		var key = "testcreateMultipartUploadEmptyChecksumAlgorithm";
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
		var bucketName = createBucket(client, 30);
		var key = "testcreateMultipartUploadEmptyChecksumType";
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

	@Test
	@Tag("If Match")
	// 소스 오브젝트와 일치하는 copy-source-if-match 조건으로 UploadPartCopy 성공 확인
	public void testUploadPartCopyIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 31);
		var source = "testUploadPartCopyIfMatchGoodSource";
		var target = "testUploadPartCopyIfMatchGoodTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var partResponse = client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfMatch(eTag)
				.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1));

		client.completeMultipartUpload(c -> c.bucket(bucketName).key(target).uploadId(uploadId)
				.multipartUpload(p -> p.parts(CompletedPart.builder().partNumber(1)
						.eTag(partResponse.copyPartResult().eTag()).build())));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트와 일치하지 않는 copy-source-if-match 조건으로 UploadPartCopy 시 412 실패 확인
	public void testUploadPartCopyIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 32);
		var source = "testUploadPartCopyIfMatchFailedSource";
		var target = "testUploadPartCopyIfMatchFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfMatch("ABC")
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 UploadPartCopy 성공 확인
	public void testUploadPartCopyIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 33);
		var source = "testUploadPartCopyIfNoneMatchGoodSource";
		var target = "testUploadPartCopyIfNoneMatchGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var partResponse = client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfNoneMatch("ABC")
				.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1));

		client.completeMultipartUpload(c -> c.bucket(bucketName).key(target).uploadId(uploadId)
				.multipartUpload(p -> p.parts(CompletedPart.builder().partNumber(1)
						.eTag(partResponse.copyPartResult().eTag()).build())));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 UploadPartCopy 시 412 실패 확인
	public void testUploadPartCopyIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 34);
		var source = "testUploadPartCopyIfNoneMatchFailedSource";
		var target = "testUploadPartCopyIfNoneMatchFailedTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfNoneMatch(eTag)
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// UploadPartCopy 요청에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
	public void testUploadPartCopyIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 35);
		var source = "testUploadPartCopyIfMatchAndIfNoneMatchSource";
		var target = "testUploadPartCopyIfMatchAndIfNoneMatchTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)
						.overrideConfiguration(o -> o.putHeader("If-Match", "ABC").putHeader("If-None-Match", "DEF"))));

		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// UploadPartCopy 요청에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
	public void testUploadPartCopyIfMatchAndIfNoneMatchAny() {
		var client = getClient();
		var bucketName = createBucket(client, 36);
		var source = "testUploadPartCopyIfMatchAndIfNoneMatchAnySource";
		var target = "testUploadPartCopyIfMatchAndIfNoneMatchAnyTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)
						.overrideConfiguration(o -> o.putHeader("If-Match", "ABC").putHeader("If-None-Match", "*"))));

		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 성공 확인
	public void testUploadPartCopyIfModifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 37);
		var source = "testUploadPartCopyIfModifiedSinceGoodSource";
		var target = "testUploadPartCopyIfModifiedSinceGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var partResponse = client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfModifiedSince(days.toInstant())
				.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1));

		client.completeMultipartUpload(c -> c.bucket(bucketName).key(target).uploadId(uploadId)
				.multipartUpload(p -> p.parts(CompletedPart.builder().partNumber(1)
						.eTag(partResponse.copyPartResult().eTag()).build())));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 시 412 실패 확인
	public void testUploadPartCopyIfModifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 38);
		var source = "testUploadPartCopyIfModifiedSinceFailedSource";
		var target = "testUploadPartCopyIfModifiedSinceFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		// 미래 날짜는 RFC 7232에 따라 무시되므로 업로드 시간 + 1초를 지정하고 1초 대기
		var lastModified = client.headObject(h -> h.bucket(bucketName).key(source)).lastModified();
		var after = lastModified.plus(1, ChronoUnit.SECONDS);

		delay(1000);

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfModifiedSince(after)
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 성공 확인
	public void testUploadPartCopyIfUnmodifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 39);
		var source = "testUploadPartCopyIfUnmodifiedSinceGoodSource";
		var target = "testUploadPartCopyIfUnmodifiedSinceGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);

		var partResponse = client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfUnmodifiedSince(days.toInstant())
				.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1));

		client.completeMultipartUpload(c -> c.bucket(bucketName).key(target).uploadId(uploadId)
				.multipartUpload(p -> p.parts(CompletedPart.builder().partNumber(1)
						.eTag(partResponse.copyPartResult().eTag()).build())));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 시 412 실패 확인
	public void testUploadPartCopyIfUnmodifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 40);
		var source = "testUploadPartCopyIfUnmodifiedSinceFailedSource";
		var target = "testUploadPartCopyIfUnmodifiedSinceFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(target)).uploadId();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfUnmodifiedSince(days.toInstant())
						.destinationBucket(bucketName).destinationKey(target).uploadId(uploadId).partNumber(1)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("IfMatch")
	// 대상 오브젝트와 일치하는 If-Match 조건으로 CompleteMultipartUpload 덮어쓰기 성공 확인
	public void testCompleteMultipartUploadIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 41);
		var key = "testCompleteMultipartUploadIfMatchGood";
		var size = 5 * MainData.MB;

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old")).eTag();

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)).ifMatch(eTag));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(uploadData.getBody(), getBody(response));
	}

	@Test
	@Tag("IfMatch")
	// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 CompleteMultipartUpload 시 412 실패 확인
	public void testCompleteMultipartUploadIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 42);
		var key = "testCompleteMultipartUploadIfMatchFailed";
		var size = 5 * MainData.MB;

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old"));

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
						.uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))
						.ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ")));

		// CompleteMultipartUpload는 처리 시작 후 실패할 경우 200 OK 본문에 에러가 포함될 수 있음
		assertTrue(e.statusCode() == HttpStatus.SC_PRECONDITION_FAILED || e.statusCode() == HttpStatus.SC_OK,
				"statusCode: " + e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 존재하지 않는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 성공 확인
	public void testCompleteMultipartUploadIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 43);
		var key = "testCompleteMultipartUploadIfNoneMatchGood";
		var size = 5 * MainData.MB;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)).ifNoneMatch("*"));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(uploadData.getBody(), getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 이미 존재하는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 시 412 실패 확인
	public void testCompleteMultipartUploadIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 44);
		var key = "testCompleteMultipartUploadIfNoneMatchFailed";
		var size = 5 * MainData.MB;

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old"));

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
						.uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))
						.ifNoneMatch("*")));

		// CompleteMultipartUpload는 처리 시작 후 실패할 경우 200 OK 본문에 에러가 포함될 수 있음
		assertTrue(e.statusCode() == HttpStatus.SC_PRECONDITION_FAILED || e.statusCode() == HttpStatus.SC_OK,
				"statusCode: " + e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
	public void testCompleteMultipartUploadIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 45);
		var key = "testCompleteMultipartUploadIfMatchAndIfNoneMatch";
		var size = 5 * MainData.MB;

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old")).eTag();

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
						.uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))
						.ifMatch(eTag).ifNoneMatch(eTag)));
		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
	public void testCompleteMultipartUploadIfMatchAndIfNoneMatchAny() {
		var client = getClient();
		var bucketName = createBucket(client, 46);
		var key = "testCompleteMultipartUploadIfMatchAndIfNoneMatchAny";
		var size = 5 * MainData.MB;

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("old")).eTag();

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
						.uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))
						.ifMatch(eTag).ifNoneMatch("*")));
		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("Cancel")
	public void testMultipartUploadAbortDuringUpload() {
		var client = getClient();
		var bucketName = createBucket(client, 47);
		var key = "testMultipartUploadAbortDuringUpload";
		var partBody = Utils.randomTextToLong(5 * MainData.MB);

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));

		var e = assertThrows(NoSuchUploadException.class,
				() -> client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
						RequestBody.fromString(partBody)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.awsErrorDetails().errorCode());
	}

}
