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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class Multipart extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Multipart Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Multipart End");
	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());

	}

	@Test
	@Tag("Check")
	public void testMultipartUploadSmall() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		var response = client.getObject(bucketName, key);
		assertEquals(size, response.getObjectMetadata().getContentLength());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopySmall() {
		var sourceKey = "foo";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 0);
		var targetBucketName = createBucket(client);
		var targetKey = "multipart";
		var size = 10;

		var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));

		var response = client.getObject(targetBucketName, targetKey);
		assertEquals(size, response.getObjectMetadata().getContentLength());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartCopyInvalidRange() {
		var sourceKey = "source";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 5);

		var targetKey = "dest";
		var response = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(sourceBucketName, targetKey));
		var uploadId = response.getUploadId();

		var e = assertThrows(AmazonServiceException.class, () -> client.copyPart(new CopyPartRequest()
				.withSourceBucketName(sourceBucketName).withSourceKey(sourceKey)
				.withDestinationBucketName(sourceBucketName).withDestinationKey(targetKey).withUploadId(uploadId)
				.withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 21)));

		assertTrue(e.getStatusCode() == HttpStatus.SC_BAD_REQUEST || e.getStatusCode() == 416);
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());

	}

	@Test
	@Tag("Range")
	public void testMultipartCopyWithoutRange() {
		var sourceKey = "source";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 10);
		var targetBucketName = createBucket(client);
		var targetKey = "my_multipart_copy";

		var initResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucketName, targetKey));
		var uploadId = initResponse.getUploadId();
		var parts = new ArrayList<PartETag>();

		var copyResponse = client.copyPart(new CopyPartRequest().withSourceBucketName(sourceBucketName)
				.withSourceKey(sourceKey).withDestinationBucketName(targetBucketName).withDestinationKey(targetKey)
				.withUploadId(uploadId).withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 9));
		parts.add(new PartETag(1, copyResponse.getETag()));
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadId, parts));

		var response = client.getObject(targetBucketName, targetKey);
		assertEquals(10, response.getObjectMetadata().getContentLength());

	}

	@Test
	@Tag("SpecialNames")
	public void testMultipartCopySpecialNames() {
		var client = getClient();
		var keyNames = List.of(" ", "_", "__", "?versionId");
		var targetKey = "my_multipart";
		var size = 10 * MainData.MB;
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		for (var sourceKey : keyNames) {
			createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(
					new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
							uploadData.parts));
			var response = client.getObjectMetadata(targetBucketName, targetKey);
			assertEquals(size, response.getContentLength());
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey, MainData.MB);
		}

	}

	@Test
	@Tag("Put")
	public void testMultipartUpload() {
		var key = "my_multipart";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, key);
		assertEquals(contentType, getResponse.getContentType());
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());

		var body = uploadData.getBody();

		checkContentUsingRange(bucketName, key, body, MainData.MB);
		checkContentUsingRange(bucketName, key, body, 10L * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, body, 100);

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyVersioned() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var targetKey = "my_multipart";
		checkVersioning(sourceBucketName, BucketVersioningConfiguration.OFF);

		var sourceKey = "foo";
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);

		var size = 15 * MainData.MB;
		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);
		createKeyWithRandomContent(client, sourceKey, size, sourceBucketName);

		var versionIds = new ArrayList<String>();
		var listResponse = client.listVersions(sourceBucketName, null);
		for (var version : listResponse.getVersionSummaries())
			versionIds.add(version.getVersionId());

		for (var versionId : versionIds) {
			var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, null, 0,
					versionId);
			client.completeMultipartUpload(new CompleteMultipartUploadRequest(targetBucketName, targetKey,
					uploadData.uploadId, uploadData.parts));
			var response = client.getObject(targetBucketName, targetKey);
			assertEquals(size, response.getObjectMetadata().getContentLength());
			checkCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}

	}

	@Test
	@Tag("Duplicate")
	public void testMultipartUploadResendPart() {
		var bucketName = createBucket();
		var key = "multipart";
		var size = 50 * MainData.MB;

		checkUploadMultipartResend(bucketName, key, size, List.of(0));
		checkUploadMultipartResend(bucketName, key, size, List.of(1));
		checkUploadMultipartResend(bucketName, key, size, List.of(2));
		checkUploadMultipartResend(bucketName, key, size, List.of(1, 2));
		checkUploadMultipartResend(bucketName, key, size, List.of(0, 1, 2, 3, 4, 5));

	}

	@Test
	@Tag("Put")
	public void testMultipartUploadMultipleSizes() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "my_multipart";

		var size = 5 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 5 * MainData.MB + 100 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 5 * MainData.MB + 600 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB;
		uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB + 100 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB + 600 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopyMultipleSizes() {
		var sourceKey = "source";
		var targetKey = "target";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, sourceKey, 12 * MainData.MB);
		var targetBucketName = createBucket(client);

		var size = 5 * MainData.MB;
		var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
				null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 5 * MainData.MB + 100 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 5 * MainData.MB + 600 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB + 100 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB + 600 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadSizeTooSmall() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";

		var size = 1 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size, 10 * MainData.KB);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ENTITY_TOO_SMALL, e.getErrorCode());

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
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "multipart";
		var content = Utils.randomTextToLong(5 * MainData.MB);
		var partCount = 2;

		client.putObject(bucketName, key, content);

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();
		var parts = new ArrayList<PartETag>();
		var totalContent = new StringBuilder();

		for (int i = 0; i < partCount; i++) {
			var partNumber = i + 1;
			var partResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(uploadId).withInputStream(createBody(content)).withPartNumber(partNumber)
					.withPartSize(content.length()));
			parts.add(new PartETag(partNumber, partResponse.getETag()));
			totalContent.append(content);
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts));

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
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
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
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

		client.putObject(bucketName, key, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, "56788")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.getErrorCode());

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

		var response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		var getUploadIds = new ArrayList<String>();

		for (var uploadData : response.getMultipartUploads()) {
			getUploadIds.add(uploadData.getUploadId());
		}

		for (var uploadId : uploadIds) {
			assertTrue(getUploadIds.contains(uploadId));
		}

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadIds.get(0)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadIds.get(1)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key2, uploadIds.get(2)));

	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadMissingPart() {
		var key = "my_multipart";
		var body = "test";
		var client = getClient();
		var bucketName = createBucket(client);

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var parts = new ArrayList<PartETag>();
		var partResponse = client
				.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
						.withInputStream(createBody(body)).withPartNumber(1).withPartSize(body.length()));
		parts.add(new PartETag(9999, partResponse.getETag()));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts)));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_PART, e.getErrorCode());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadIncorrectEtag() {
		var key = "my_multipart";
		var client = getClient();
		var bucketName = createBucket(client);

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var parts = new ArrayList<PartETag>();
		client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
				.withInputStream(createBody("\\00")).withPartNumber(1).withPartSize("\\00".length()));
		parts.add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts)));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_PART, e.getErrorCode());

	}

	@Test
	@Tag("Overwrite")
	public void testAtomicMultipartUploadWrite() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, "bar");

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));

		response = client.getObject(bucketName, key);
		body = getBody(response.getObjectContent());
		assertEquals("bar", body);

	}

	@Test
	@Tag("List")
	public void testMultipartUploadList() {
		var key = "my_multipart";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);

		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);

		var response = client.listParts(new ListPartsRequest(bucketName, key, uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.getParts());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUploadList() {
		var key = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));

		var listResponse = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		assertEquals(0, listResponse.getMultipartUploads().size());

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
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "my_multipart1";
		uploadData = multipartCopy(bucketName, sourceKey, bucketName, targetKey1, size, client, 0, null);
		uploadData = multipartUpload(client, bucketName, targetKey1, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey1, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "my_multipart2";
		uploadData = multipartCopy(bucketName, targetKey1, bucketName, targetKey2, size * 2, client, 0, null);
		uploadData = multipartUpload(client, bucketName, targetKey2, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey2, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
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
			var response = client.listParts(
					new ListPartsRequest(bucketName, key, uploadData.uploadId)
							.withMaxParts(10)
							.withPartNumberMarker(partNumber));

			assertEquals(10, response.getParts().size());
			partsETagCompare(uploadData.parts.subList(partNumber, partNumber + 10), response.getParts());
			if (response.isTruncated()) {
				index = response.getNextPartNumberMarker();
			} else {
				break;
			}
		}

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}
}
