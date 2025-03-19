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

class CSE {

	org.example.test.CSE test = new org.example.test.CSE();
	org.example.testV2.CSE testV2 = new org.example.testV2.CSE();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("PutGet")
	// [AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	void testCseEncryptedTransfer1b() {
		test.testCseEncryptedTransfer1b();
		testV2.testCseEncryptedTransfer1b();
	}

	@Test
	@Tag("PutGet")
	// [AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	void testCseEncryptedTransfer1kb() {
		test.testCseEncryptedTransfer1kb();
		testV2.testCseEncryptedTransfer1kb();
	}

	@Test
	@Tag("PutGet")
	// [AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	void testCseEncryptedTransfer1MB() {
		test.testCseEncryptedTransfer1MB();
		testV2.testCseEncryptedTransfer1MB();
	}

	@Test
	@Tag("PutGet")
	// [AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	void testCseEncryptedTransfer13b() {
		test.testCseEncryptedTransfer13b();
		testV2.testCseEncryptedTransfer13b();
	}

	@Test
	@Tag("Metadata")
	// [AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인
	void testCseEncryptionMethodHead() {
		test.testCseEncryptionMethodHead();
		testV2.testCseEncryptionMethodHead();
	}

	@Test
	@Tag("ERROR")
	// [AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치
	void testCseEncryptionNonDecryption() {
		test.testCseEncryptionNonDecryption();
		testV2.testCseEncryptionNonDecryption();
	}

	@Test
	@Tag("ERROR")
	// [AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인
	void testCseNonEncryptionDecryption() {
		test.testCseNonEncryptionDecryption();
		testV2.testCseNonEncryptionDecryption();
	}

	@Test
	@Tag("RangeRead")
	// [AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공
	void testCseEncryptionRangeRead() {
		test.testCseEncryptionRangeRead();
		testV2.testCseEncryptionRangeRead();
	}

	@Test
	@Tag("Multipart")
	// [AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인
	void testCseEncryptionMultipartUpload() {
		test.testCseEncryptionMultipartUpload();
		testV2.testCseEncryptionMultipartUpload();
	}

	@Test
	@Tag("Get")
	// CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	void testCseGetObjectMany() {
		test.testCseGetObjectMany();
		testV2.testCseGetObjectMany();
	}

	@Test
	@Tag("Get")
	// CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	void testCseRangeObjectMany() {
		test.testCseRangeObjectMany();
		testV2.testCseRangeObjectMany();
	}
}
