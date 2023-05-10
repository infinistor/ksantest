/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
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
import java.util.Arrays;

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
	static public void BeforeAll() {
		System.out.println("Multipart Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Multipart End");
	}

	@Test
	@Tag("ERROR")
	// 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
	public void test_multipart_upload_empty() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.MalformedXML, errorCode);
	}

	@Test
	@Tag("Check")
	// 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
	public void test_multipart_upload_small() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		var response = client.getObject(bucketName, key);
		assertEquals(size, response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Copy")
	// 버킷a에서 버킷b로 멀티파트 복사 성공확인
	public void test_multipart_copy_small() {
		var sourceKey = "foo";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 0, null, null);

		var targetBucketName = getNewBucket();
		var targetKey = "multipart";
		var size = 1;
		var client = getClient();

		var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));

		var response = client.getObject(targetBucketName, targetKey);
		assertEquals(size, response.getObjectMetadata().getContentLength());
		checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey, MainData.MB);
	}

	@Test
	@Tag("ERROR")
	// 범위설정을 잘못한 멀티파트 복사 실패 확인
	public void test_multipart_copy_invalid_range() {
		var client = getClient();
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 5, null, client);

		var targetKey = "dest";
		var response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(sourceBucketName, targetKey));
		var uploadId = response.getUploadId();

		var e = assertThrows(AmazonServiceException.class, () -> client.copyPart(new CopyPartRequest()
				.withSourceBucketName(sourceBucketName).withSourceKey(sourceKey)
				.withDestinationBucketName(sourceBucketName).withDestinationKey(targetKey).withUploadId(uploadId)
				.withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 21)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertTrue(new ArrayList<>(Arrays.asList(new Integer[] { 400, 416 })).contains(statusCode));
		assertEquals(MainData.InvalidArgument, errorCode);
	}

	@Test
	@Tag("Range")
	// 범위를 지정한 멀티파트 복사 성공확인
	public void test_multipart_copy_without_range() {
		var client = getClient();
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 10, null, client);
		var targetBucketName = getNewBucket();
		var targetKey = "mymultipartcopy";

		var initResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucketName, targetKey));
		var uploadId = initResponse.getUploadId();
		var parts = new ArrayList<PartETag>();

		var CopyResponse = client.copyPart(new CopyPartRequest().withSourceBucketName(sourceBucketName)
				.withSourceKey(sourceKey).withDestinationBucketName(targetBucketName).withDestinationKey(targetKey)
				.withUploadId(uploadId).withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 9));
		parts.add(new PartETag(1, CopyResponse.getETag()));
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadId, parts));

		var response = client.getObject(targetBucketName, targetKey);
		assertEquals(10, response.getObjectMetadata().getContentLength());
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);
	}

	@Test
	@Tag("SpecialNames")
	// 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
	public void test_multipart_copy_special_names() {
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();

		var targetKey = "mymultipart";
		var size = 1;
		var client = getClient();

		for (var sourceKey : new String[] { " ", "_", "__", "?versionId" }) {
			createKeyWithRandomContent(sourceKey, 0, sourceBucketName, null);
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(
					new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId,
							uploadData.parts));
			var response = client.getObject(targetBucketName, targetKey);
			assertEquals(size, response.getObjectMetadata().getContentLength());
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey, MainData.MB);
		}
	}

	@Test
	@Tag("Put")
	// 멀티파트 업로드 확인
	public void test_multipart_upload() {
		var bucketName = getNewBucket();
		var key = "mymultipart";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, key);
		assertEquals(contentType, getResponse.getContentType());
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());

		var body = uploadData.getBody();

		checkContentUsingRange(bucketName, key, body, MainData.MB);
		checkContentUsingRange(bucketName, key, body, 10 * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, body, 100);
	}

	@Test
	@Tag("Copy")
	// 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
	public void test_multipart_copy_versioned() {
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();

		var targetKey = "mymultipart";
		checkVersioning(sourceBucketName, BucketVersioningConfiguration.OFF);

		var sourceKey = "foo";
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);

		var size = 15 * MainData.MB;
		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);
		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);
		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);

		var versionIds = new ArrayList<String>();
		var client = getClient();
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
			CheckCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}
	}

	@Test
	@Tag("Duplicate")
	// 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
	public void test_multipart_upload_resend_part() {
		var bucketName = getNewBucket();
		var key = "multipart";
		var size = 50 * MainData.MB;

		checkUploadMultipartResend(bucketName, key, size, new ArrayList<>(Arrays.asList(new Integer[] { 0 })));
		checkUploadMultipartResend(bucketName, key, size, new ArrayList<>(Arrays.asList(new Integer[] { 1 })));
		checkUploadMultipartResend(bucketName, key, size, new ArrayList<>(Arrays.asList(new Integer[] { 2 })));
		checkUploadMultipartResend(bucketName, key, size, new ArrayList<>(Arrays.asList(new Integer[] { 1, 2 })));
		checkUploadMultipartResend(bucketName, key, size,
				new ArrayList<>(Arrays.asList(new Integer[] { 0, 1, 2, 3, 4, 5 })));
	}

	@Test
	@Tag("Put")
	// 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
	public void test_multipart_upload_multiple_sizes() {
		var bucketName = getNewBucket();
		var key = "mymultipart";
		var client = getClient();

		var size = 5 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 5 * MainData.MB + 100 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 5 * MainData.MB + 600 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB;
		uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB + 100 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		size = 10 * MainData.MB + 600 * MainData.KB;
		uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

	}

	@Test
	@Tag("Copy")
	// 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
	public void test_multipart_copy_multiple_sizes() {
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 12 * MainData.MB, null, null);

		var targetBucketName = getNewBucket();
		var targetKey = "target";
		var client = getClient();

		var size = 5 * MainData.MB;
		var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 5 * MainData.MB + 100 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 5 * MainData.MB + 600 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB + 100 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);

		size = 10 * MainData.MB + 600 * MainData.KB;
		uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(targetBucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);
	}

	@Test
	@Tag("ERROR")
	// 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
	public void test_multipart_upload_size_too_small() {
		var bucketName = getNewBucket();
		var key = "multipart";
		var client = getClient();

		var size = 1 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size, 10 * MainData.KB, null, null);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.EntityTooSmall, errorCode);
	}

	@Test
	@Tag("Check")
	// 내용물을 채운 멀티파트 업로드 성공 확인
	public void test_multipart_upload_contents() {
		var bucketName = getNewBucket();
		doTestMultipartUploadContents(bucketName, "multipart", 3);
	}

	@Test
	@Tag("OverWrite")
	// 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
	public void test_multipart_upload_overwrite_existing_object() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var content = Utils.randomTextToLong(5 * MainData.MB);
		var NumParts = 2;

		client.putObject(bucketName, key, content);

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();
		var parts = new ArrayList<PartETag>();
		var totalContent = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var PartResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(uploadId).withInputStream(createBody(content)).withPartNumber(PartNumber)
					.withPartSize(content.length()));
			parts.add(new PartETag(PartNumber, PartResponse.getETag()));
			totalContent += content;
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts));

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertTrue(totalContent.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("Cancel")
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void test_abort_multipart_upload() {
		var bucketName = getNewBucket();
		var key = "multipart";
		var size = 10 * MainData.MB;
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));

		var HeadResponse = client.listObjectsV2(bucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(0, ObjectCount);
		var bytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(0, bytesUsed);
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않은 멀티파트 업로드 중단 실패 확인
	public void test_abort_multipart_upload_not_found() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "mymultipart";
		client.putObject(bucketName, key, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, "56788")));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchUpload, errorCode);
	}

	@Test
	@Tag("List")
	// 멀티파트 업로드 중인 목록 확인
	public void test_list_multipart_upload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "mymultipart";
		var key2 = "mymultipart2";

		var uploadIds = new ArrayList<>(Arrays.asList(
				new String[] { setupMultipartUpload(client, bucketName, key, 5 * MainData.MB, null).uploadId,
						setupMultipartUpload(client, bucketName, key, 6 * MainData.MB, null).uploadId,
						setupMultipartUpload(client, bucketName, key2, 5 * MainData.MB, null).uploadId, }));

		var response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		var getUploadIds = new ArrayList<String>();

		for (var uploadData : response.getMultipartUploads())
			getUploadIds.add(uploadData.getUploadId());

		for (var uploadId : uploadIds)
			assertTrue(getUploadIds.contains(uploadId));

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadIds.get(0)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadIds.get(1)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key2, uploadIds.get(2)));
	}

	@Test
	@Tag("ERROR")
	// 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
	public void test_multipart_upload_missing_part() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "mymultipart";
		var body = "test";

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var parts = new ArrayList<PartETag>();
		var partResponse = client
				.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
						.withInputStream(createBody(body)).withPartNumber(1).withPartSize(body.length()));
		parts.add(new PartETag(9999, partResponse.getETag()));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(400, statusCode);
		assertEquals(MainData.InvalidPart, errorCode);
	}

	@Test
	@Tag("ERROR")
	// 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
	public void test_multipart_upload_incorrect_etag() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "mymultipart";

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var parts = new ArrayList<PartETag>();
		client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
				.withInputStream(createBody("\\00")).withPartNumber(1).withPartSize("\\00".length()));
		parts.add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(400, statusCode);
		assertEquals(MainData.InvalidPart, errorCode);
	}

	@Test
	@Tag("Overwrite")
	// 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
	public void test_atomic_multipart_upload_write() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
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
	// 멀티파트 업로드 목록 확인
	public void test_multipart_upload_list() {
		var bucketName = getNewBucket();
		var key = "mymultipart";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);

		var response = client.listParts(new ListPartsRequest(bucketName, key, uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.getParts());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}

	@Test
	@Tag("Cancel")
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void test_abort_multipart_upload_list() {
		var bucketName = getNewBucket();
		var key = "mymultipart";
		var size = 10 * MainData.MB;
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));

		var listResponse = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		assertEquals(0, listResponse.getMultipartUploads().size());
	}

	@Test
	@Tag("Copy")
	// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
	public void test_multipart_copy_many() {
		var bucketName = getNewBucket();
		var sourceKey = "mymultipart";
		var size = 10 * MainData.MB;
		var client = getClient();
		var body = new StringBuilder();

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		uploadData = multipartCopy(bucketName, sourceKey, bucketName, TargetKey1, size, client, 0, null);
		// 추가파츠 업로드
		uploadData = MultipartUpload(client, bucketName, TargetKey1, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, TargetKey1, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, TargetKey1, body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		uploadData = multipartCopy(bucketName, TargetKey1, bucketName, TargetKey2, size * 2, client, 0, null);
		// 추가파츠 업로드
		uploadData = MultipartUpload(client, bucketName, TargetKey2, size, uploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, TargetKey2, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, TargetKey2, body.toString(), MainData.MB);
	}

	@Test
	@Tag("List")
	// 멀티파트 목록 확인
	public void test_multipart_list_parts() {
		var bucketName = getNewBucket();
		var key = "mymultipart";
		var contentType = "text/bla";
		var size = 50 * MainData.MB;
		var metadata = new ObjectMetadata();
		metadata.setContentType(contentType);
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 1 * MainData.MB, metadata, null);

		for (int i = 0; i < 41; i += 10) {
			var response = client.listParts(
					new ListPartsRequest(bucketName, key, uploadData.uploadId)
							.withMaxParts(10)
							.withPartNumberMarker(i));

			assertEquals(10, response.getParts().size());
			partsETagCompare(uploadData.parts.subList(i, i + 10), response.getParts());
		}
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadData.uploadId));
	}
}
