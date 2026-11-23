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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.junit.jupiter.api.function.Executable;

public class Multipart extends TestBase {
	@Test
	@Tag("ERROR")
	public void testMultipartUploadEmpty() {
		var client = getClient();
		var bucketName = createBucket(client, 1);
		var key = "testMultipartUploadEmpty";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}

	@Test
	@Tag("Check")
	public void testMultipartUploadSmall() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var key = "testMultipartUploadSmall";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(size, response.getContentLength());

	}

	@Test
	@Tag("Copy")
	public void testMultipartCopySmall() {
		var sourceKey = "foo";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 3, sourceKey, 0);
		var targetBucketName = createBucket(client, 3);
		var targetKey = "testMultipartCopySmall";
		var size = 10;

		var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
						uploadData.parts));

		var response = client.getObjectMetadata(targetBucketName, targetKey);
		assertEquals(size, response.getContentLength());

	}

	@Test
	@Tag("ERROR")
	public void testMultipartCopyInvalidRange() {
		var sourceKey = "source";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 4, sourceKey, 5);

		var targetKey = "testMultipartCopyInvalidRange";
		var response = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(sourceBucketName, targetKey));
		var uploadId = response.getUploadId();

		var e = assertThrows(AmazonServiceException.class, () -> client.copyPart(new CopyPartRequest()
				.withSourceBucketName(sourceBucketName).withSourceKey(sourceKey)
				.withDestinationBucketName(sourceBucketName).withDestinationKey(targetKey).withUploadId(uploadId)
				.withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 21)));

		assertTrue(e.getStatusCode() == HttpStatus.SC_BAD_REQUEST || e.getStatusCode() == 416);
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(sourceBucketName, targetKey, uploadId));
	}

	@Test
	@Tag("Range")
	public void testMultipartCopyWithoutRange() {
		var sourceKey = "source";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 5, sourceKey, 10);
		var targetBucketName = createBucket(client, 5);
		var targetKey = "testMultipartCopyWithoutRange";

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

		var response = client.getObjectMetadata(targetBucketName, targetKey);
		assertEquals(10, response.getContentLength());

	}

	@Test
	@Tag("SpecialNames")
	public void testMultipartCopySpecialNames() {
		var client = getClient();
		var keyNames = List.of(" ", "_", "__", "?versionId");
		var targetKey = "testMultipartCopySpecialNames";
		var size = 10 * MainData.MB;
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

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
		var key = "testMultipartUpload";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var client = getClient();
		var bucketName = createBucket(client, 7);

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
		var sourceBucketName = createBucket(client, 8);
		var targetBucketName = createBucket(client, 8);

		var targetKey = "testMultipartCopyVersioned";
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
			var response = client.getObjectMetadata(targetBucketName, targetKey);
			assertEquals(size, response.getContentLength());
			checkCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}

	}

	@Test
	@Tag("Duplicate")
	public void testMultipartUploadResendPart() {
		var bucketName = createBucket(9);
		var key = "testMultipartUploadResendPart";
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
		var bucketName = createBucket(client, 10);
		var key = "testMultipartUploadMultipleSizes";

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
		var targetKey = "testMultipartCopyMultipleSizes";
		var client = getClient();
		var sourceBucketName = createKeyWithRandomContent(client, 11, sourceKey, 12 * MainData.MB);
		var targetBucketName = createBucket(client, 11);

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
		var bucketName = createBucket(client, 12);
		var key = "testMultipartUploadSizeTooSmall";

		var size = 1 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size, 10 * MainData.KB);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ENTITY_TOO_SMALL, e.getErrorCode());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
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
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var key = "testMultipartUploadOverwriteExistingObject";
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
	@Tag("OverWrite")
	public void testPutObjectOverwriteMultipartUpload() {
		var key = "testPutObjectOverwriteMultipartUpload";
		var multipartSize = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 26);
		var content = Utils.randomTextToLong(1 * MainData.MB);

		var uploadData = setupMultipartUpload(client, bucketName, key, multipartSize);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		client.putObject(bucketName, key, content);

		var headResponse = client.getObjectMetadata(bucketName, key);
		assertEquals(content.length(), headResponse.getContentLength());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(content.length(), body.length());
		assertTrue(content.equals(body), MainData.NOT_MATCHED);

		checkContentUsingRange(bucketName, key, content, MainData.KB);
	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUpload() {
		var key = "testAbortMultipartUpload";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 15);

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
		var key = "testAbortMultipartUploadNotFound";
		var client = getClient();
		var bucketName = createBucket(client, 16);

		client.putObject(bucketName, key, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, "56788")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.getErrorCode());

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
		var key = "testMultipartUploadMissingPart";
		var body = "test";
		var client = getClient();
		var bucketName = createBucket(client, 18);

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

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testMultipartUploadIncorrectEtag() {
		var key = "testMultipartUploadIncorrectEtag";
		var client = getClient();
		var bucketName = createBucket(client, 19);

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

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));

	}

	@Test
	@Tag("Overwrite")
	public void testAtomicMultipartUploadWrite() {
		var key = "testAtomicMultipartUploadWrite";
		var client = getClient();
		var bucketName = createBucket(client, 20);

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
		var key = "testMultipartUploadList";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);

		var client = getClient();
		var bucketName = createBucket(client, 21);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);

		var response = client.listParts(new ListPartsRequest(bucketName, key, uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.getParts());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}

	@Test
	@Tag("Cancel")
	public void testAbortMultipartUploadList() {
		var key = "testAbortMultipartUploadList";
		var size = 10 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 22);

		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));

		var listResponse = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		assertEquals(0, listResponse.getMultipartUploads().size());

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
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		var targetKey1 = "testMultipartCopyMany1";
		uploadData = multipartCopy(bucketName, sourceKey, bucketName, targetKey1, size, client, 0, null);
		uploadData = multipartUpload(client, bucketName, targetKey1, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, targetKey1, uploadData.uploadId, uploadData.parts));

		body.append(uploadData.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		var targetKey2 = "testMultipartCopyMany2";
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
		var key = "testMultipartListParts";
		var size = 50 * MainData.MB;
		var client = getClient();
		var bucketName = createBucket(client, 24);

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

	@Test
	@Tag("Cancel")
	public void testMultipartUploadAbortDuringUpload() {
		var client = getClient();
		var bucketName = createBucket(client, 25);
		var key = "testMultipartUploadAbortDuringUpload";
		var partBody = Utils.randomTextToLong(5 * MainData.MB);

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
						.withUploadId(uploadId).withInputStream(createBody(partBody)).withPartNumber(1)
						.withPartSize(partBody.length())));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_UPLOAD, e.getErrorCode());
	}

	private static final String DIGITS = "0123456789";

	private record SmallUpload(String uploadId, List<PartETag> parts) {
	}

	private static UploadPartRequest partRequest(String bucketName, String key, String uploadId, int partNumber,
			String content) {
		return new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
				.withPartNumber(partNumber).withInputStream(createBody(content)).withPartSize(content.length());
	}

	/** 10바이트짜리 파트를 count 개 올리고 업로드 ID 와 파트 목록을 돌려준다. */
	private SmallUpload uploadSmallParts(AmazonS3 client, String bucketName, String key, int count) {
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		var parts = new ArrayList<PartETag>();
		for (var number = 1; number <= count; number++)
			parts.add(client.uploadPart(partRequest(bucketName, key, uploadId, number, DIGITS)).getPartETag());
		return new SmallUpload(uploadId, parts);
	}

	@Test
	@Tag("ERROR")
	public void testUploadPartInvalidPartNumber() {
		var key = "testUploadPartInvalidPartNumber";
		var client = getClient();
		var bucketName = createBucket(client, 49);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		// 파트 번호는 1~10000
		for (var partNumber : List.of(0, 10001)) {
			assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
					() -> client.uploadPart(partRequest(bucketName, key, uploadId, partNumber, "content")));
		}
		client.uploadPart(partRequest(bucketName, key, uploadId, 10000, "content"));
		var parts = client.listParts(new ListPartsRequest(bucketName, key, uploadId)).getParts();
		assertEquals(List.of(10000), parts.stream().map(part -> part.getPartNumber()).toList());
	}

	@Test
	@Tag("ERROR")
	public void testUploadPartNoSuchUpload() {
		var key = "testUploadPartNoSuchUpload";
		var client = getClient();
		var bucketName = createBucket(client, 50);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		// 없는 업로드 ID, 다른 키의 업로드 ID
		for (var target : List.of(List.of(key, "invalid"), List.of(key + "_other", uploadId))) {
			var targetKey = target.get(0);
			var targetUploadId = target.get(1);
			assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
					() -> client.uploadPart(partRequest(bucketName, targetKey, targetUploadId, 1, "content")));
			assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
					() -> client.listParts(new ListPartsRequest(bucketName, targetKey, targetUploadId)));
		}
	}

	@Test
	@Tag("ContentMD5")
	public void testUploadPartContentMd5Mismatch() {
		var key = "testUploadPartContentMd5Mismatch";
		var client = getClient();
		var bucketName = createBucket(client, 51);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.BAD_DIGEST,
				() -> client.uploadPart(
						partRequest(bucketName, key, uploadId, 1, "content").withMD5Digest(Utils.getMD5("other"))));
		assertEquals(0, client.listParts(new ListPartsRequest(bucketName, key, uploadId)).getParts().size());
	}

	@Test
	@Tag("Access")
	public void testMultipartAccessDenied() {
		var key = "testMultipartAccessDenied";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 52);
		var upload = uploadSmallParts(client, bucketName, key, 1);
		var uploadId = upload.uploadId();
		var denied = List.<Executable>of(
				() -> altClient.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key)),
				() -> altClient.uploadPart(partRequest(bucketName, key, uploadId, 2, "content")),
				() -> altClient.listParts(new ListPartsRequest(bucketName, key, uploadId)),
				() -> altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)),
				() -> altClient.completeMultipartUpload(
						new CompleteMultipartUploadRequest(bucketName, key, uploadId, upload.parts())),
				() -> altClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId)));
		for (var request : denied)
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, request);
		// 업로드는 그대로 남아 있다
		var parts = client.listParts(new ListPartsRequest(bucketName, key, uploadId)).getParts();
		assertEquals(List.of(1), parts.stream().map(part -> part.getPartNumber()).toList());
	}

	@Test
	@Tag("ERROR")
	public void testMultipartBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(53);
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "key")));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)));
	}

	@Test
	@Tag("List")
	public void testListPartsMaxParts() {
		var key = "testListPartsMaxParts";
		var client = getClient();
		var bucketName = createBucket(client, 55);
		var uploadId = uploadSmallParts(client, bucketName, key, 3).uploadId();

		var response = client.listParts(new ListPartsRequest(bucketName, key, uploadId));
		assertEquals(List.of(1, 2, 3), response.getParts().stream().map(part -> part.getPartNumber()).toList());
		assertEquals(false, response.isTruncated());
		assertEquals(1000, response.getMaxParts());

		response = client.listParts(new ListPartsRequest(bucketName, key, uploadId).withMaxParts(2));
		assertEquals(List.of(1, 2), response.getParts().stream().map(part -> part.getPartNumber()).toList());
		assertEquals(true, response.isTruncated());
		assertEquals(2, response.getNextPartNumberMarker());

		response = client.listParts(new ListPartsRequest(bucketName, key, uploadId).withPartNumberMarker(2));
		assertEquals(List.of(3), response.getParts().stream().map(part -> part.getPartNumber()).toList());
		assertEquals(false, response.isTruncated());

		response = client.listParts(new ListPartsRequest(bucketName, key, uploadId).withMaxParts(0));
		assertEquals(0, response.getParts().size());
		assertEquals(false, response.isTruncated());

		// 1000 보다 큰 값은 1000 으로 낮춘다
		assertEquals(1000,
				client.listParts(new ListPartsRequest(bucketName, key, uploadId).withMaxParts(1001)).getMaxParts());
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.listParts(new ListPartsRequest(bucketName, key, uploadId).withMaxParts(-1)));
	}

	@Test
	@Tag("ERROR")
	public void testCompleteMultipartUploadInvalidPartOrder() {
		var key = "testCompleteMultipartUploadInvalidPartOrder";
		var client = getClient();
		var bucketName = createBucket(client, 56);
		var upload = uploadSmallParts(client, bucketName, key, 2);
		// 파트 번호는 중복될 수 없다
		// (SDK 가 파트 목록을 번호순으로 정렬해 보내므로 순서가 뒤집힌 경우는 확인하지 않는다)
		var parts = List.of(upload.parts().get(0), upload.parts().get(0));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_PART_ORDER,
				() -> client.completeMultipartUpload(
						new CompleteMultipartUploadRequest(bucketName, key, upload.uploadId(), parts)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.getObjectMetadata(bucketName, key));
	}

	@Test
	@Tag("ERROR")
	public void testCompleteMultipartUploadNoParts() {
		var key = "testCompleteMultipartUploadNoParts";
		var client = getClient();
		var bucketName = createBucket(client, 57);
		var uploadId = uploadSmallParts(client, bucketName, key, 1).uploadId();
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML,
				() -> client.completeMultipartUpload(
						new CompleteMultipartUploadRequest(bucketName, key, uploadId, new ArrayList<PartETag>())));
	}

	@Test
	@Tag("Check")
	public void testCompleteMultipartUploadSkipPart() {
		var key = "testCompleteMultipartUploadSkipPart";
		var client = getClient();
		var bucketName = createBucket(client, 58);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		var bodies = List.of("a".repeat(5 * MainData.MB), "b".repeat(5 * MainData.MB), "c".repeat(10));
		var parts = new ArrayList<PartETag>();
		for (var index = 0; index < bodies.size(); index++)
			parts.add(client.uploadPart(partRequest(bucketName, key, uploadId, index + 1, bodies.get(index)))
					.getPartETag());

		// 올린 파트 중 일부만 골라 완료할 수 있다(SDK 는 ETag 를 따옴표 없이 보낸다)
		var response = client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId,
				List.of(parts.get(0), parts.get(2))));
		assertTrue(response.getETag().endsWith("-2"));
		assertEquals(bucketName, response.getBucketName());
		assertEquals(key, response.getKey());

		var getResponse = client.getObject(bucketName, key);
		assertEquals(bodies.get(0).length() + bodies.get(2).length(),
				getResponse.getObjectMetadata().getContentLength());
		assertTrue((bodies.get(0) + bodies.get(2)).equals(getBody(getResponse.getObjectContent())));
	}

	@Test
	@Tag("Check")
	public void testCompleteMultipartUploadTwice() {
		var key = "testCompleteMultipartUploadTwice";
		var client = getClient();
		var bucketName = createBucket(client, 59);
		var upload = uploadSmallParts(client, bucketName, key, 1);
		var uploadId = upload.uploadId();
		var first = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadId, upload.parts()));
		assertTrue(first.getETag().endsWith("-1"));

		// 완료한 업로드를 같은 내용으로 다시 완료하면 같은 결과로 성공한다
		var second = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadId, upload.parts()));
		assertEquals(first.getETag(), second.getETag());

		// 완료한 업로드에는 파트를 더 올리거나 목록을 볼 수 없고, 중단 요청은 성공한다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
				() -> client.uploadPart(partRequest(bucketName, key, uploadId, 2, "content")));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
				() -> client.listParts(new ListPartsRequest(bucketName, key, uploadId)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
		assertEquals(DIGITS, getBody(client.getObject(bucketName, key).getObjectContent()));
	}

	@Test
	@Tag("Check")
	public void testAbortMultipartUploadTwice() {
		var key = "testAbortMultipartUploadTwice";
		var client = getClient();
		var bucketName = createBucket(client, 60);
		var upload = uploadSmallParts(client, bucketName, key, 1);
		var uploadId = upload.uploadId();
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));

		// 이미 중단한 업로드를 다시 중단해도 성공한다
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
				() -> client.uploadPart(partRequest(bucketName, key, uploadId, 1, "content")));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
				() -> client.completeMultipartUpload(
						new CompleteMultipartUploadRequest(bucketName, key, uploadId, upload.parts())));
	}

	/** a/1 에 2개, a/2, b/1, c 에 1개씩 업로드를 만든다. */
	private void createUploads(AmazonS3 client, String bucketName) {
		for (var key : List.of("a/1", "a/1", "a/2", "b/1", "c"))
			client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
	}

	private static List<String> uploadKeys(MultipartUploadListing response) {
		return response.getMultipartUploads().stream().map(upload -> upload.getKey()).toList();
	}

	@Test
	@Tag("List")
	public void testListMultipartUploadsPrefixDelimiter() {
		var client = getClient();
		var bucketName = createBucket(client, 61);
		createUploads(client, bucketName);

		var response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		assertEquals(List.of("a/1", "a/1", "a/2", "b/1", "c"), uploadKeys(response));
		assertEquals(false, response.isTruncated());
		assertEquals(1000, response.getMaxUploads());

		response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withPrefix("a/"));
		assertEquals(List.of("a/1", "a/1", "a/2"), uploadKeys(response));

		response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withDelimiter("/"));
		assertEquals(List.of("c"), uploadKeys(response));
		assertEquals(List.of("a/", "b/"), response.getCommonPrefixes());
	}

	@Test
	@Tag("List")
	public void testListMultipartUploadsPagination() {
		var client = getClient();
		var bucketName = createBucket(client, 62);
		createUploads(client, bucketName);

		var first = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withMaxUploads(2));
		assertEquals(List.of("a/1", "a/1"), uploadKeys(first));
		assertEquals(true, first.isTruncated());
		assertEquals("a/1", first.getNextKeyMarker());
		assertEquals(first.getMultipartUploads().get(1).getUploadId(), first.getNextUploadIdMarker());

		var response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withMaxUploads(2)
				.withKeyMarker(first.getNextKeyMarker()).withUploadIdMarker(first.getNextUploadIdMarker()));
		assertEquals(List.of("a/2", "b/1"), uploadKeys(response));
		assertEquals(true, response.isTruncated());

		// 키 마커만 주면 그 키의 업로드는 모두 건너뛴다
		response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withKeyMarker("a/1"));
		assertEquals(List.of("a/2", "b/1", "c"), uploadKeys(response));
		assertEquals(false, response.isTruncated());
	}

	@Test
	@Tag("List")
	public void testListMultipartUploadsMaxUploads() {
		var client = getClient();
		var bucketName = createBucket(client, 63);
		createUploads(client, bucketName);

		var response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withMaxUploads(0));
		assertEquals(0, response.getMultipartUploads().size());
		assertEquals(false, response.isTruncated());

		// 1000 보다 큰 값은 1000 으로 낮춘다
		assertEquals(1000, client
				.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withMaxUploads(1001)).getMaxUploads());
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withMaxUploads(-1)));
	}

	@Test
	@Tag("ERROR")
	public void testUploadPartCopySourceNotExist() {
		var key = "testUploadPartCopySourceNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 64);
		client.putObject(bucketName, "source", DIGITS);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		var notExistBucketName = getNewBucketNameOnly(64);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.copyPart(new CopyPartRequest().withSourceBucketName(bucketName)
						.withSourceKey("not_exist").withDestinationBucketName(bucketName).withDestinationKey(key)
						.withUploadId(uploadId).withPartNumber(1)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.copyPart(new CopyPartRequest().withSourceBucketName(notExistBucketName)
						.withSourceKey("source").withDestinationBucketName(bucketName).withDestinationKey(key)
						.withUploadId(uploadId).withPartNumber(1)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_UPLOAD,
				() -> client.copyPart(new CopyPartRequest().withSourceBucketName(bucketName)
						.withSourceKey("source").withDestinationBucketName(bucketName).withDestinationKey(key)
						.withUploadId("invalid").withPartNumber(1)));
		assertEquals(0, client.listParts(new ListPartsRequest(bucketName, key, uploadId)).getParts().size());
	}

	@Test
	@Tag("PartNumber")
	public void testMultipartGetPartNumber() {
		var key = "testMultipartGetPartNumber";
		var client = getClient();
		var bucketName = createBucket(client, 66);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		var bodies = List.of("a".repeat(5 * MainData.MB), "b".repeat(10));
		var parts = new ArrayList<PartETag>();
		for (var index = 0; index < bodies.size(); index++)
			parts.add(client.uploadPart(partRequest(bucketName, key, uploadId, index + 1, bodies.get(index)))
					.getPartETag());
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts));
		long firstSize = bodies.get(0).length();
		long total = firstSize + bodies.get(1).length();

		// partNumber 를 주면 그 파트만 돌려주고 전체 파트 수를 알려준다
		var headResponse = client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withPartNumber(1));
		assertEquals(firstSize, headResponse.getContentLength());
		assertEquals(2, headResponse.getPartCount());
		assertEquals(List.of(0L, firstSize - 1), List.of(headResponse.getContentRange()));
		assertEquals(total, headResponse.getInstanceLength());

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(2));
		assertEquals(2, getResponse.getObjectMetadata().getPartCount());
		assertEquals(List.of(firstSize, total - 1), List.of(getResponse.getObjectMetadata().getContentRange()));
		assertEquals(total, getResponse.getObjectMetadata().getInstanceLength());
		assertEquals(bodies.get(1), getBody(getResponse.getObjectContent()));

		assertS3Error(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, MainData.INVALID_PART_NUMBER,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(3)));
	}
}
