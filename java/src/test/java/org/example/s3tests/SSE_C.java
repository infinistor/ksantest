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

import java.net.MalformedURLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class SSE_C {

	org.example.test.SSE_C Test = new org.example.test.SSE_C();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testEncryptedTransfer1b() {
		Test.testEncryptedTransfer1b();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testEncryptedTransfer1kb() {
		Test.testEncryptedTransfer1kb();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testEncryptedTransfer1MB() {
		Test.testEncryptedTransfer1MB();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	void testEncryptedTransfer13b() {
		Test.testEncryptedTransfer13b();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
	void testEncryptionSseCMethodHead() {
		Test.testEncryptionSseCMethodHead();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
	void testEncryptionSseCPresent() {
		Test.testEncryptionSseCPresent();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	void testEncryptionSseCOtherKey() {
		Test.testEncryptionSseCOtherKey();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
	void testEncryptionSseCInvalidMd5() {
		Test.testEncryptionSseCInvalidMd5();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 성공 확인
	void testEncryptionSseCNoMd5() {
		Test.testEncryptionSseCNoMd5();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
	void testEncryptionSseCNoKey() {
		Test.testEncryptionSseCNoKey();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA 에서는 algorithm값을 누락해도 기본값이 지정되어 있어 에러가 발생하지 않음")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
	void testEncryptionKeyNoSseC() {
		Test.testEncryptionKeyNoSseC();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// @Tag("멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
	void testEncryptionSseCMultipartUpload() {
		Test.testEncryptionSseCMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// @Tag("SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	void testEncryptionSseCMultipartBadDownload() {
		Test.testEncryptionSseCMultipartBadDownload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
	void testEncryptionSseCPostObjectAuthenticatedRequest() throws MalformedURLException {
		Test.testEncryptionSseCPostObjectAuthenticatedRequest();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	void testEncryptionSseCGetObjectMany() {
		Test.testEncryptionSseCGetObjectMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	void testEncryptionSseCRangeObjectMany() {
		Test.testEncryptionSseCRangeObjectMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
	void testSseCEncryptionMultipartCopyPartUpload() {
		Test.testSseCEncryptionMultipartCopyPartUpload();
	}
}
