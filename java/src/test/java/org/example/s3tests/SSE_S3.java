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

class SSE_S3 {

	org.example.test.SSE_S3 Test = new org.example.test.SSE_S3();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1b() {
		Test.testSseS3EncryptedTransfer1b();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1kb() {
		Test.testSseS3EncryptedTransfer1kb();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1MB() {
		Test.testSseS3EncryptedTransfer1MB();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer13b() {
		Test.testSseS3EncryptedTransfer13b();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	void testSseS3EncryptionMethodHead() {
		Test.testSseS3EncryptionMethodHead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// @Tag("멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	void testSseS3EncryptionMultipartUpload() {
		Test.testSseS3EncryptionMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정 확인
	void testGetBucketEncryption() {
		Test.testGetBucketEncryption();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정이 가능한지 확인
	void testPutBucketEncryption() {
		Test.testPutBucketEncryption();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정 삭제가 가능한지 확인
	void testDeleteBucketEncryption() {
		Test.testDeleteBucketEncryption();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	void testPutBucketEncryptionAndObjectSetCheck() {
		Test.testPutBucketEncryptionAndObjectSetCheck();
	}

	@Test
	@Tag("KSAN")
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption1kb() {
		Test.testCopyObjectEncryption1kb();
	}

	@Test
	@Tag("KSAN")
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption_256kb() {
		Test.testCopyObjectEncryption256kb();
	}

	@Test
	@Tag("KSAN")
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption1mb() {
		Test.testCopyObjectEncryption1mb();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGet() {
		Test.testSseS3BucketPutGet();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetV4() {
		Test.testSseS3BucketPutGetV4();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드
	// 성공 확인
	void testSseS3BucketPutGetUseChunkEncoding() {
		Test.testSseS3BucketPutGetUseChunkEncoding();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true,
	// DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning() {
		Test.testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드
	// 성공 확인
	void testSseS3BucketPutGetNotChunkEncoding() {
		Test.testSseS3BucketPutGetNotChunkEncoding();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false,
	// DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning() {
		Test.testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning();
	}

	@Test
	@Tag("KSAN")
	@Tag("PresignedURL")
	// @Tag("[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	void testSseS3BucketPresignedUrlPutGet() {
		Test.testSseS3BucketPresignedUrlPutGet();
	}

	@Test
	@Tag("KSAN")
	@Tag("PresignedURL")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	void testSseS3BucketPresignedUrlPutGetV4() {
		Test.testSseS3BucketPresignedUrlPutGetV4();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	void testSseS3GetObjectMany() {
		Test.testSseS3GetObjectMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	void testSseS3RangeObjectMany() {
		Test.testSseS3RangeObjectMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
	void testSseS3EncryptionMultipartCopyPartUpload() {
		Test.testSseS3EncryptionMultipartCopyPartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// SSE-S3 설정하여 Multipart와 Copy Part를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	void testSseS3EncryptionMultipartCopyMany() {
		Test.testSseS3EncryptionMultipartCopyMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	//sse-s3설정은 소급적용 되지 않음을 확인
	void testSseS3NotRetroactive() {
		Test.testSseS3NotRetroactive();
	}
}
