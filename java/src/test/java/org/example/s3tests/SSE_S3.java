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
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SSE-S3 암호화 기능을 테스트하는 클래스
 */
class SSE_S3 {

	org.example.test.SSE_S3 test = new org.example.test.SSE_S3();
	org.example.testV2.SSE_S3 testV2 = new org.example.testV2.SSE_S3();

	/**
	 * 테스트 정리 작업 수행
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3EncryptedTransfer1b() {
		test.testSseS3EncryptedTransfer1b();
		testV2.testSseS3EncryptedTransfer1b();
	}

	/**
	 * 1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3EncryptedTransfer1kb() {
		test.testSseS3EncryptedTransfer1kb();
		testV2.testSseS3EncryptedTransfer1kb();
	}

	/**
	 * 1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3EncryptedTransfer1MB() {
		test.testSseS3EncryptedTransfer1MB();
		testV2.testSseS3EncryptedTransfer1MB();
	}

	/**
	 * 13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3EncryptedTransfer13b() {
		test.testSseS3EncryptedTransfer13b();
		testV2.testSseS3EncryptedTransfer13b();
	}

	/**
	 * SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	 */
	@Test
	@Tag("Metadata")
	void testSseS3EncryptionMethodHead() {
		test.testSseS3EncryptionMethodHead();
		testV2.testSseS3EncryptionMethodHead();
	}

	/**
	 * 멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	 */
	@Test
	@Tag("Multipart")
	void testSseS3EncryptionMultipartUpload() {
		test.testSseS3EncryptionMultipartUpload();
		testV2.testSseS3EncryptionMultipartUpload();
	}

	/**
	 * 버킷의 SSE-S3 설정 확인
	 */
	@Test
	@Tag("encryption")
	void testGetBucketEncryption() {
		test.testGetBucketEncryption();
		testV2.testGetBucketEncryption();
	}

	/**
	 * 버킷의 SSE-S3 설정이 가능한지 확인
	 */
	@Test
	@Tag("encryption")
	void testPutBucketEncryption() {
		test.testPutBucketEncryption();
		testV2.testPutBucketEncryption();
	}

	/**
	 * 버킷의 SSE-S3 설정 삭제가 가능한지 확인
	 */
	@Test
	@Tag("encryption")
	void testDeleteBucketEncryption() {
		test.testDeleteBucketEncryption();
		testV2.testDeleteBucketEncryption();
	}

	/**
	 * 버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	 */
	@Test
	@Tag("encryption")
	void testPutBucketEncryptionAndObjectSetCheck() {
		test.testPutBucketEncryptionAndObjectSetCheck();
		testV2.testPutBucketEncryptionAndObjectSetCheck();
	}

	/**
	 * 버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	 */
	@Test
	@Tag("CopyObject")
	void testCopyObjectEncryption1kb() {
		test.testCopyObjectEncryption1kb();
		testV2.testCopyObjectEncryption1kb();
	}

	/**
	 * 버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	 */
	@Test
	@Tag("CopyObject")
	void testCopyObjectEncryption_256kb() {
		test.testCopyObjectEncryption256kb();
		testV2.testCopyObjectEncryption256kb();
	}

	/**
	 * 버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	 */
	@Test
	@Tag("CopyObject")
	void testCopyObjectEncryption1mb() {
		test.testCopyObjectEncryption1mb();
		testV2.testCopyObjectEncryption1mb();
	}

	/**
	 * [버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGet() {
		test.testSseS3BucketPutGet();
		testV2.testSseS3BucketPutGet();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGetV4() {
		test.testSseS3BucketPutGetV4();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGetUseChunkEncoding() {
		test.testSseS3BucketPutGetUseChunkEncoding();
		testV2.testSseS3BucketPutGetUseChunkEncoding();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning() {
		test.testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGetNotChunkEncoding() {
		test.testSseS3BucketPutGetNotChunkEncoding();
		testV2.testSseS3BucketPutGetNotChunkEncoding();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning() {
		test.testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning();
	}

	/**
	 * [버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PresignedURL")
	void testSseS3BucketPresignedUrlPutGet() {
		test.testSseS3BucketPresignedUrlPutGet();
		testV2.testSseS3BucketPresignedUrlPutGet();
	}

	/**
	 * [버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PresignedURL")
	void testSseS3BucketPresignedUrlPutGetV4() {
		test.testSseS3BucketPresignedUrlPutGetV4();
		testV2.testSseS3BucketPresignedUrlPutGetV4();
	}

	/**
	 * SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	 */
	@Test
	@Tag("Get")
	void testSseS3GetObjectMany() {
		test.testSseS3GetObjectMany();
		testV2.testSseS3GetObjectMany();
	}

	/**
	 * SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	 */
	@Test
	@Tag("Get")
	void testSseS3RangeObjectMany() {
		test.testSseS3RangeObjectMany();
		testV2.testSseS3RangeObjectMany();
	}

	/**
	 * SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
	 */
	@Test
	@Tag("Multipart")
	void testSseS3EncryptionMultipartCopyPartUpload() {
		test.testSseS3EncryptionMultipartCopyPartUpload();
		testV2.testSseS3EncryptionMultipartCopyPartUpload();
	}

	/**
	 * SSE-S3 설정하여 Multipart와 Copy Part를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	 */
	@Test
	@Tag("Multipart")
	void testSseS3EncryptionMultipartCopyMany() {
		test.testSseS3EncryptionMultipartCopyMany();
		testV2.testSseS3EncryptionMultipartCopyMany();
	}

	/**
	 * sse-s3설정은 소급적용 되지 않음을 확인
	 */
	@Test
	@Tag("PutGet")
	void testSseS3NotRetroactive() {
		test.testSseS3NotRetroactive();
		testV2.testSseS3NotRetroactive();
	}
}
