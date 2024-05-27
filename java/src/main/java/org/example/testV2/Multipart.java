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

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CompletedPart;

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
	// 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
	public void testMultipartUploadEmpty() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var size = 0;

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.MalformedXML, errorCode);
	}

	@Test
	@Tag("Check")
	// 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
	public void testMultipartUploadSmall() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var size = 1;

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(size, response.response().contentLength());
	}

	@Test
	@Tag("Copy")
	// 버킷a에서 버킷b로 멀티파트 복사 성공확인
	public void testMultipartCopySmall() {
		var sourceKey = "foo";
		var bucketName = createKeyWithRandomContent(sourceKey, 0, null, null);

		var targetBucketName = getNewBucket();
		var targetKey = "multipart";
		var size = 1;
		var client = getClient();

		var uploadData = multipartCopy(bucketName, sourceKey, targetBucketName, targetKey, size, client, 0, null);
		client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		assertEquals(size, response.response().contentLength());
	}

	@Test
	@Tag("ERROR")
	// 범위설정을 잘못한 멀티파트 복사 실패 확인
	public void testMultipartCopyInvalidRange() {
		var client = getClient();
		var sourceKey = "source";
		var bucketName = createKeyWithRandomContent(sourceKey, 5, null, client);

		var targetKey = "dest";
		var response = client.createMultipartUpload(c -> c.bucket(bucketName).key(targetKey));
		var uploadId = response.uploadId();

		var e = assertThrows(AwsServiceException.class,
				() -> client.uploadPartCopy(
						c -> c.sourceBucket(bucketName).sourceKey(sourceKey).destinationBucket(bucketName)
								.destinationKey(targetKey)
								.uploadId(uploadId).partNumber(1).copySourceRange("bytes=0-21")));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertTrue(statusCode == 400 || statusCode == 416);
		assertEquals(MainData.InvalidArgument, errorCode);
	}

	@Test
	@Tag("Range")
	// 범위를 지정한 멀티파트 복사 성공확인
	public void testMultipartCopyWithoutRange() {
		var client = getClient();
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 10, null, client);
		var targetBucketName = getNewBucket();
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
	// 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
	public void testMultipartCopySpecialNames() {
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();

		var targetKey = "my_multipart";
		var size = 1;
		var client = getClient();

		for (var sourceKey : new String[] { " ", "_", "__", "?versionId" }) {
			createKeyWithRandomContent(sourceKey, 0, sourceBucketName, null);
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
					.multipartUpload(p -> p.parts(uploadData.parts)));
			var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
			assertEquals(size, response.response().contentLength());
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey, MainData.MB);
		}
	}

	@Test
	@Tag("Put")
	// 멀티파트 업로드 확인
	public void testMultipartUpload() {
		var bucketName = getNewBucket();
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");
		var client = getClient();

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
	// 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
	public void testMultipartCopyVersioned() {
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var targetKey = "my_multipart";
		var sourceKey = "foo";
		var size = 15 * MainData.MB;

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);
		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);
		createKeyWithRandomContent(sourceKey, size, sourceBucketName, null);

		var versionIds = new ArrayList<String>();
		var client = getClient();
		var listResponse = client.listObjectVersions(l -> l.bucket(sourceBucketName));
		for (var version : listResponse.versions())
			versionIds.add(version.versionId());

		for (var versionId : versionIds) {
			var uploadData = multipartCopy(sourceBucketName, sourceKey, targetBucketName, targetKey, size, null, 0,
					versionId);
			client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
					.multipartUpload(p -> p.parts(uploadData.parts)));
			var response = client.getObject(g -> g.bucket(targetBucketName).key(targetKey));
			assertEquals(size, response.response().contentLength());
			checkCopyContent(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId);
		}
	}

	@Test
	@Tag("Duplicate")
	// 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
	public void testMultipartUploadResendPart() {
		var bucketName = getNewBucket();
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
	// 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
	public void testMultipartUploadMultipleSizes() {
		var bucketName = getNewBucket();
		var key = "my_multipart";
		var client = getClient();

		var sizeList = List.of(5 * MainData.MB, 5 * MainData.MB + 100 * MainData.KB,
				5 * MainData.MB + 600 * MainData.KB, 10 * MainData.MB, 10 * MainData.MB + 100 * MainData.KB,
				10 * MainData.MB + 600 * MainData.KB);

		for (var size : sizeList) {
			var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
			client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
					.multipartUpload(p -> p.parts(uploadData.parts)));
		}
	}

	@Test
	@Tag("Copy")
	// 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
	public void testMultipartCopyMultipleSizes() {
		var sourceKey = "source";
		var sourceBucketName = createKeyWithRandomContent(sourceKey, 12 * MainData.MB, null, null);

		var targetBucketName = getNewBucket();
		var targetKey = "target";
		var client = getClient();

		var sizeList = List.of(5 * MainData.MB, 5 * MainData.MB + 100 * MainData.KB,
				5 * MainData.MB + 600 * MainData.KB, 10 * MainData.MB, 10 * MainData.MB + 100 * MainData.KB,
				10 * MainData.MB + 600 * MainData.KB);

		for (var size : sizeList) {
			var uploadData = multipartCopy(client, sourceBucketName, sourceKey, targetBucketName, targetKey, size,
					null);
			client.completeMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey).uploadId(uploadData.uploadId)
					.multipartUpload(p -> p.parts(uploadData.parts)));
			checkCopyContentUsingRange(sourceBucketName, sourceKey, targetBucketName, targetKey);
		}
	}

	@Test
	@Tag("ERROR")
	// 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
	public void testMultipartUploadSizeTooSmall() {
		var bucketName = getNewBucket();
		var key = "multipart";
		var client = getClient();

		var size = 1 * MainData.MB;
		var uploadData = setupMultipartUpload(client, bucketName, key, size, 10 * MainData.KB, null, null);
		var e = assertThrows(AwsServiceException.class,
				() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts))));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(400, statusCode);
		assertEquals(MainData.EntityTooSmall, errorCode);
	}

	@Test
	@Tag("Check")
	// 내용물을 채운 멀티파트 업로드 성공 확인
	public void testMultipartUploadContents() {
		var bucketName = getNewBucket();
		doTestMultipartUploadContents(bucketName, "multipart", 3);
	}

	@Test
	@Tag("OverWrite")
	// 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
	public void testMultipartUploadOverwriteExistingObject() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart";
		var content = Utils.randomTextToLong(5 * MainData.MB);
		var partCount = 2;

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
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void testAbortMultipartUpload() {
		var bucketName = getNewBucket();
		var key = "multipart";
		var size = 10 * MainData.MB;
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

		var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		var objectCount = headResponse.keyCount();
		assertEquals(0, objectCount);
		var bytesUsed = getBytesUsed(headResponse);
		assertEquals(0, bytesUsed);
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않은 멀티파트 업로드 중단 실패 확인
	public void testAbortMultipartUploadNotFound() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "my_multipart";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var e = assertThrows(AwsServiceException.class,
				() -> client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId("nonexistent")));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchUpload, errorCode);
	}

	@Test
	@Tag("List")
	// 멀티파트 업로드 중인 목록 확인
	public void testListMultipartUpload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "my_multipart";
		var key2 = "my_multipart2";

		var uploadData1 = setupMultipartUpload(client, bucketName, key, 5 * MainData.MB, null);
		var uploadData2 = setupMultipartUpload(client, bucketName, key, 6 * MainData.MB, null);
		var uploadData3 = setupMultipartUpload(client, bucketName, key2, 5 * MainData.MB, null);

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
	// 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
	public void testMultipartUploadMissingPart() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "my_multipart";
		var body = "test";

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var parts = new ArrayList<CompletedPart>();
		var partResponse = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString(body));
		parts.add(CompletedPart.builder().partNumber(9999).eTag(partResponse.eTag()).build());

		var e = assertThrows(AwsServiceException.class, () -> client.completeMultipartUpload(
				c -> c.bucket(bucketName).key(key).uploadId(uploadId).multipartUpload(p -> p.parts(parts))));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertEquals(400, statusCode);
		assertEquals(MainData.InvalidPart, errorCode);
	}

	@Test
	@Tag("ERROR")
	// 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
	public void testMultipartUploadIncorrectEtag() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "my_multipart";

		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var parts = new ArrayList<CompletedPart>();
		client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString("test"));
		parts.add(CompletedPart.builder().partNumber(1).eTag("ffffffffffffffffffffffffffffffff").build());

		var e = assertThrows(AwsServiceException.class, () -> client
				.completeMultipartUpload(
						c -> c.bucket(bucketName).key(key).uploadId(uploadId).multipartUpload(p -> p.parts(parts))));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertEquals(400, statusCode);
		assertEquals(MainData.InvalidPart, errorCode);
	}

	@Test
	@Tag("Overwrite")
	// 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
	public void testAtomicMultipartUploadWrite() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// 멀티파트 업로드 목록 확인
	public void testMultipartUploadList() {
		var bucketName = getNewBucket();
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);

		var response = client.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
		partsETagCompare(uploadData.parts, response.parts());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
	}

	@Test
	@Tag("Cancel")
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void testAbortMultipartUploadList() {
		var bucketName = getNewBucket();
		var key = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, null);
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));

		var listResponse = client.listMultipartUploads(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.uploads().size());
	}

	@Test
	@Tag("Copy")
	// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
	public void testMultipartCopyMany() {
		var bucketName = getNewBucket();
		var sourceKey = "my_multipart";
		var size = 10 * MainData.MB;
		var client = getClient();

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, null);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, sourceKey, uploadData.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey1 = "my_multipart1";
		var uploadData2 = multipartCopy(bucketName, sourceKey, bucketName, targetKey1, size, client, 0, null);
		// 추가파츠 업로드
		var copyData1 = multipartUpload(client, bucketName, targetKey1, size, uploadData2);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey1).uploadId(copyData1.uploadId)
				.multipartUpload(p -> p.parts(copyData1.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey1, copyData1.body.toString(), MainData.MB);

		// 멀티파트 카피
		var targetKey2 = "my_multipart2";
		var uploadData3 = multipartCopy(bucketName, targetKey1, bucketName, targetKey2, size * 2, client, 0, null);
		// 추가파츠 업로드
		var copyData2 = multipartUpload(client, bucketName, targetKey2, size, uploadData3);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(targetKey2).uploadId(copyData2.uploadId)
				.multipartUpload(p -> p.parts(copyData2.parts)));

		// 업로드가 올바르게 되었는지 확인
		checkContentUsingRange(bucketName, targetKey2, copyData2.body.toString(), MainData.MB);
	}

	@Test
	@Tag("List")
	// 멀티파트 목록 확인
	public void testMultipartListParts() {
		var bucketName = getNewBucket();
		var key = "my_multipart";
		var size = 50 * MainData.MB;
		var metadata = new HashMap<String, String>();
		var client = getClient();

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 1 * MainData.MB, metadata, null);

		for (int i = 0; i < 41; i += 10) {
			var partNumber = i;
			var response = client.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
					.maxParts(10).partNumberMarker(partNumber));

			assertEquals(10, response.parts().size());
			partsETagCompare(uploadData.parts.subList(i, i + 10), response.parts());
		}
		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadData.uploadId));
	}
}
