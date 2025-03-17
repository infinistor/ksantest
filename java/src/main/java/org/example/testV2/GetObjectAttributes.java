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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;

public class GetObjectAttributes extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("GetObjectAttributes V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("GetObjectAttributes V2 End");
	}

	/**
	 * 기본 GetObjectAttributes 테스트
	 * 모든 속성을 요청하고 응답이 올바른지 확인
	 */
	@Test
	@Tag("Basic")
	public void testGetObjectAttributesBasic() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesBasic";

		// 객체 생성
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// GetObjectAttributes 요청
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.STORAGE_CLASS,
						ObjectAttributes.E_TAG));

		// 응답 검증
		assertNotNull(response);
		assertEquals(key.length(), response.objectSize());
		assertEquals(StorageClass.STANDARD, response.storageClass());
		assertNotNull(response.eTag());
	}

	/**
	 * 특정 속성만 요청하는 테스트
	 */
	@Test
	@Tag("SpecificAttributes")
	public void testGetObjectAttributesSpecificAttributes() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesSpecificAttributes";

		// 객체 생성
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// ObjectSize만 요청
		var sizeResponse = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE));

		assertNotNull(sizeResponse);
		assertEquals(key.length(), sizeResponse.objectSize());
		// 요청하지 않은 속성은 null이어야 함
		assertEquals(null, sizeResponse.checksum());

		// ETag만 요청
		var etagResponse = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.E_TAG));

		assertNotNull(etagResponse);
		assertNotNull(etagResponse.eTag());
		// 요청하지 않은 속성은 null이어야 함
		assertEquals(null, etagResponse.objectSize());
	}

	/**
	 * 멀티파트 업로드된 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Multipart")
	public void testGetObjectAttributesMultipart() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesMultipart";
		var size = 10 * MainData.MB;

		// 멀티파트 업로드 설정
		var uploadData = setupMultipartUpload(client, bucketName, key, size);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		// GetObjectAttributes 요청 (ObjectParts 포함)
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.STORAGE_CLASS,
						ObjectAttributes.E_TAG,
						ObjectAttributes.OBJECT_PARTS));

		// 응답 검증
		assertNotNull(response);
		assertEquals(size, response.objectSize());
		assertEquals(StorageClass.STANDARD, response.storageClass());
		assertNotNull(response.eTag());

		// 멀티파트 정보 검증
		assertNotNull(response.objectParts());
		assertTrue(response.objectParts().totalPartsCount() > 0);
		assertEquals(uploadData.parts.size(), response.objectParts().totalPartsCount());
	}

	/**
	 * 체크섬 알고리즘을 사용한 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Checksum")
	public void testGetObjectAttributesWithChecksum() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesWithChecksum";
		var checksumAlgorithm = ChecksumAlgorithm.SHA256;

		// 체크섬 알고리즘을 사용하여 객체 생성
		client.putObject(p -> p
				.bucket(bucketName)
				.key(key)
				.checksumAlgorithm(checksumAlgorithm),
				RequestBody.fromString(key));

		// GetObjectAttributes 요청 (Checksum 포함)
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.CHECKSUM));

		// 응답 검증
		assertNotNull(response);
		assertNotNull(response.checksum());
		// 체크섬 알고리즘 검증 - 체크섬 객체가 존재하는지만 확인
		assertNotNull(response.checksum());
	}

	/**
	 * 존재하지 않는 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	public void testGetObjectAttributesNonExistentObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesNonExistentObject";

		// 존재하지 않는 객체에 대한 요청
		var e = assertThrows(AwsServiceException.class, () -> client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE)));

		// 에러 검증
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	/**
	 * 존재하지 않는 버킷에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	public void testGetObjectAttributesNonExistentBucket() {
		var client = getClient();
		var bucketName = "non-existent-bucket-" + Utils.randomText(10).toLowerCase();
		var key = "testGetObjectAttributesNonExistentBucket";

		// 존재하지 않는 버킷에 대한 요청
		var e = assertThrows(AwsServiceException.class, () -> client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE)));

		// 에러 검증
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	/**
	 * 속성을 지정하지 않은 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	public void testGetObjectAttributesNoAttributes() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesNoAttributes";

		// 객체 생성
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// 속성을 지정하지 않은 요청
		var e = assertThrows(AwsServiceException.class, () -> client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)));

		// 에러 검증
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	/**
	 * 버전 ID를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Versioning")
	public void testGetObjectAttributesWithVersionId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesWithVersionId";

		// 버킷에 버전 관리 활성화
		checkConfigureVersioningRetry(bucketName,
				software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED);

		// 첫 번째 버전 생성
		var content1 = key + "-v1"; // 버전 구분을 위한 content
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content1));

		// 두 번째 버전 생성
		var content2 = key + "-v2"; // 버전 구분을 위한 content
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content2));

		// 버전 ID 목록 가져오기
		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName).prefix(key));
		var versions = listResponse.versions();
		assertEquals(2, versions.size());

		// 첫 번째 버전에 대한 GetObjectAttributes 요청
		var firstVersionId = versions.get(1).versionId(); // 첫 번째 버전은 리스트의 두 번째 항목
		var firstVersionResponse = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.versionId(firstVersionId)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE));

		// 두 번째 버전에 대한 GetObjectAttributes 요청
		var secondVersionId = versions.get(0).versionId(); // 두 번째 버전은 리스트의 첫 번째 항목
		var secondVersionResponse = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.versionId(secondVersionId)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE));

		// 응답 검증
		assertEquals(content1.length(), firstVersionResponse.objectSize());
		assertEquals(content2.length(), secondVersionResponse.objectSize());
	}

	/**
	 * 잘못된 버전 ID를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	public void testGetObjectAttributesInvalidVersionId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesInvalidVersionId";

		// 객체 생성
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// 잘못된 버전 ID로 요청
		var e = assertThrows(AwsServiceException.class, () -> client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.versionId("invalid-version-id")
				.objectAttributes(ObjectAttributes.OBJECT_SIZE)));

		// 에러 검증
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	/**
	 * 대용량 멀티파트 업로드 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("LargeMultipart")
	public void testGetObjectAttributesLargeMultipart() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesLargeMultipart";
		var size = 100 * MainData.MB; // 100MB
		var partSize = 5 * MainData.MB; // 5MB 파트 크기

		// 멀티파트 업로드 초기화
		var initResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();

		// 파트 업로드
		int partCount = size / partSize;
		for (int i = 0; i < partCount; i++) {
			var partNumber = i + 1;
			var partContent = Utils.randomTextToLong(partSize);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadId)
					.partNumber(partNumber),
					RequestBody.fromString(partContent));
			parts.add(CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build());
		}

		// 멀티파트 업로드 완료
		client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadId)
				.multipartUpload(p -> p.parts(parts)));

		// GetObjectAttributes 요청
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.OBJECT_PARTS));

		// 응답 검증
		assertNotNull(response);
		assertEquals(size, response.objectSize());
		assertNotNull(response.objectParts());
		assertEquals(partCount, response.objectParts().totalPartsCount());
		// 파트 크기 검증 - 객체 파트 정보가 존재하는지만 확인
		assertNotNull(response.objectParts());
	}

	/**
	 * 메타데이터가 있는 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Metadata")
	public void testGetObjectAttributesWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesWithMetadata";

		// 메타데이터 설정
		Map<String, String> metadata = new HashMap<>();
		metadata.put("custom-key1", "custom-value1");
		metadata.put("custom-key2", "custom-value2");

		// 메타데이터와 함께 객체 생성
		client.putObject(p -> p
				.bucket(bucketName)
				.key(key)
				.metadata(metadata),
				RequestBody.fromString(key));

		// GetObjectAttributes 요청
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.E_TAG));

		// 응답 검증
		assertNotNull(response);
		assertEquals(key.length(), response.objectSize());

		// 메타데이터는 GetObjectAttributes에서 반환되지 않음
		// HeadObject를 사용하여 메타데이터 확인
		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, headResponse.metadata());
	}

	/**
	 * SSE-S3 암호화된 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Encryption")
	public void testGetObjectAttributesWithSSES3() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesWithSSES3";

		// SSE-S3 암호화와 함께 객체 생성
		client.putObject(p -> p
				.bucket(bucketName)
				.key(key)
				.serverSideEncryption(software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256),
				RequestBody.fromString(key));

		// GetObjectAttributes 요청
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.E_TAG));

		// 응답 검증
		assertNotNull(response);
		assertEquals(key.length(), response.objectSize());

		// 암호화 정보는 GetObjectAttributes에서 반환되지 않음
		// HeadObject를 사용하여 암호화 정보 확인
		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256,
				headResponse.serverSideEncryption());
	}

	/**
	 * 비동기 클라이언트를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Async")
	public void testGetObjectAttributesAsync() {
		var client = getClient();
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesAsync";

		// 동기 클라이언트로 객체 생성
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// 비동기 클라이언트로 GetObjectAttributes 요청
		var response = asyncClient.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.E_TAG))
				.join();

		// 응답 검증
		assertNotNull(response);
		assertEquals(key.length(), response.objectSize());
		assertNotNull(response.eTag());
	}

	/**
	 * 비동기 클라이언트를 사용한 GetObjectAttributes 에러 테스트
	 */
	@Test
	@Tag("ERROR")
	public void testGetObjectAttributesAsyncError() {
		var asyncClient = getAsyncClient(true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var bucketName = getNewBucketNameOnly();
		var key = "testGetObjectAttributesAsyncError";

		// 존재하지 않는 버킷에 대한 비동기 요청
		var e = assertThrows(CompletionException.class, () -> asyncClient.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE))
				.join());

		// 에러 검증
		assertTrue(e.getCause() instanceof S3Exception);
		var s3Exception = (S3Exception) e.getCause();
		assertEquals(HttpStatus.SC_NOT_FOUND, s3Exception.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, s3Exception.awsErrorDetails().errorCode());
	}

	/**
	 * 모든 가능한 속성을 요청하는 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("AllAttributes")
	public void testGetObjectAttributesAllAttributes() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAttributesAllAttributes";
		var size = 10 * MainData.MB;
		var checksumType = ChecksumType.FULL_OBJECT;
		var checksumAlgorithm = ChecksumAlgorithm.CRC64_NVME;

		// 멀티파트 업로드 초기화
		var initResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.checksumType(checksumType)
				.checksumAlgorithm(checksumAlgorithm));
		var uploadId = initResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();

		// 파트 업로드
		var partContent = Utils.randomTextToLong(size);
		var partResponse = client.uploadPart(u -> u
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadId)
				.partNumber(1)
				.checksumAlgorithm(checksumAlgorithm),
				RequestBody.fromString(partContent));
		parts.add(CompletedPart.builder().partNumber(1).eTag(partResponse.eTag()).build());

		// 멀티파트 업로드 완료
		client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadId)
				.checksumType(checksumType)
				.multipartUpload(p -> p.parts(parts)));

		// 모든 속성을 요청
		var response = client.getObjectAttributes(g -> g
				.bucket(bucketName)
				.key(key)
				.objectAttributes(
						ObjectAttributes.OBJECT_SIZE,
						ObjectAttributes.STORAGE_CLASS,
						ObjectAttributes.E_TAG,
						ObjectAttributes.OBJECT_PARTS,
						ObjectAttributes.CHECKSUM));

		// 응답 검증
		assertNotNull(response);
		assertEquals(size, response.objectSize());
		assertEquals(StorageClass.STANDARD, response.storageClass());
		assertNotNull(response.eTag());
		assertNotNull(response.objectParts());
		assertEquals(1, response.objectParts().totalPartsCount());
		assertNotNull(response.checksum());
	}
}
