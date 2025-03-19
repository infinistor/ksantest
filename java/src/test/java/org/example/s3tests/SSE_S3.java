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

	org.example.test.SSE_S3 test = new org.example.test.SSE_S3();
	org.example.testV2.SSE_S3 testV2 = new org.example.testV2.SSE_S3();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("PutGet")
	//1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1b() {
		test.testSseS3EncryptedTransfer1b();
		testV2.testSseS3EncryptedTransfer1b();
	}

	@Test
	@Tag("PutGet")
	//1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1kb() {
		test.testSseS3EncryptedTransfer1kb();
		testV2.testSseS3EncryptedTransfer1kb();
	}

	@Test
	@Tag("PutGet")
	//1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer1MB() {
		test.testSseS3EncryptedTransfer1MB();
		testV2.testSseS3EncryptedTransfer1MB();
	}

	@Test
	@Tag("PutGet")
	//13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testSseS3EncryptedTransfer13b() {
		test.testSseS3EncryptedTransfer13b();
		testV2.testSseS3EncryptedTransfer13b();
	}

	@Test
	@Tag("Metadata")
	//SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	void testSseS3EncryptionMethodHead() {
		test.testSseS3EncryptionMethodHead();
		testV2.testSseS3EncryptionMethodHead();
	}

	@Test
	@Tag("Multipart")
	//멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	void testSseS3EncryptionMultipartUpload() {
		test.testSseS3EncryptionMultipartUpload();
		testV2.testSseS3EncryptionMultipartUpload();
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정 확인
	void testGetBucketEncryption() {
		test.testGetBucketEncryption();
		testV2.testGetBucketEncryption();
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정이 가능한지 확인
	void testPutBucketEncryption() {
		test.testPutBucketEncryption();
		testV2.testPutBucketEncryption();
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정 삭제가 가능한지 확인
	void testDeleteBucketEncryption() {
		test.testDeleteBucketEncryption();
		testV2.testDeleteBucketEncryption();
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	void testPutBucketEncryptionAndObjectSetCheck() {
		test.testPutBucketEncryptionAndObjectSetCheck();
		testV2.testPutBucketEncryptionAndObjectSetCheck();
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption1kb() {
		test.testCopyObjectEncryption1kb();
		testV2.testCopyObjectEncryption1kb();
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption_256kb() {
		test.testCopyObjectEncryption256kb();
		testV2.testCopyObjectEncryption256kb();
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	void testCopyObjectEncryption1mb() {
		test.testCopyObjectEncryption1mb();
		testV2.testCopyObjectEncryption1mb();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGet() {
		test.testSseS3BucketPutGet();
		testV2.testSseS3BucketPutGet();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetV4() {
		test.testSseS3BucketPutGetV4();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드
	// 성공 확인
	void testSseS3BucketPutGetUseChunkEncoding() {
		test.testSseS3BucketPutGetUseChunkEncoding();
		testV2.testSseS3BucketPutGetUseChunkEncoding();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true,
	// DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning() {
		test.testSseS3BucketPutGetUseChunkEncodingAndDisablePayloadSigning();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드
	// 성공 확인
	void testSseS3BucketPutGetNotChunkEncoding() {
		test.testSseS3BucketPutGetNotChunkEncoding();
		testV2.testSseS3BucketPutGetNotChunkEncoding();
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false,
	// DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	void testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning() {
		test.testSseS3BucketPutGetNotChunkEncodingAndDisablePayloadSigning();
	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	void testSseS3BucketPresignedUrlPutGet() {
		test.testSseS3BucketPresignedUrlPutGet();
		testV2.testSseS3BucketPresignedUrlPutGet();
	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	void testSseS3BucketPresignedUrlPutGetV4() {
		test.testSseS3BucketPresignedUrlPutGetV4();
		testV2.testSseS3BucketPresignedUrlPutGetV4();
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	void testSseS3GetObjectMany() {
		test.testSseS3GetObjectMany();
		testV2.testSseS3GetObjectMany();
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	void testSseS3RangeObjectMany() {
		test.testSseS3RangeObjectMany();
		testV2.testSseS3RangeObjectMany();
	}

	@Test
	@Tag("Multipart")
	// SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
	void testSseS3EncryptionMultipartCopyPartUpload() {
		test.testSseS3EncryptionMultipartCopyPartUpload();
		testV2.testSseS3EncryptionMultipartCopyPartUpload();
	}

	@Test
	@Tag("Multipart")
	// SSE-S3 설정하여 Multipart와 Copy Part를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	void testSseS3EncryptionMultipartCopyMany() {
		test.testSseS3EncryptionMultipartCopyMany();
		testV2.testSseS3EncryptionMultipartCopyMany();
	}

	@Test
	@Tag("PutGet")
	//sse-s3설정은 소급적용 되지 않음을 확인
	void testSseS3NotRetroactive() {
		test.testSseS3NotRetroactive();
		testV2.testSseS3NotRetroactive();
	}
}
