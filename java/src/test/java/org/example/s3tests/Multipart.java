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

class Multipart {

	org.example.test.Multipart Test = new org.example.test.Multipart();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("비어있는 오브젝트를 멀티파트로 업로드 실패 확인
	void testMultipartUploadEmpty() {
		Test.testMultipartUploadEmpty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
	void testMultipartUploadSmall() {
		Test.testMultipartUploadSmall();
	}

	@Test
	@Tag("KSAN")
	@Tag("Copy")
	// @Tag("버킷a에서 버킷b로 멀티파트 복사 성공확인
	void testMultipartCopySmall() {
		Test.testMultipartCopySmall();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("범위설정을 잘못한 멀티파트 복사 실패 확인
	void testMultipartCopyInvalidRange() {
		Test.testMultipartCopyInvalidRange();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("범위를 지정한 멀티파트 복사 성공확인
	void testMultipartCopyWithoutRange() {
		Test.testMultipartCopyWithoutRange();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialNames")
	// @Tag("특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
	void testMultipartCopySpecialNames() {
		Test.testMultipartCopySpecialNames();
	}

	@Test
	@Tag("KSAN")
	@Tag("Put")
	// @Tag("멀티파트 업로드 확인
	void testMultipartUpload() {
		Test.testMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Copy")
	// @Tag("버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
	void testMultipartCopyVersioned() {
		Test.testMultipartCopyVersioned();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// @Tag("멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
	void testMultipartUploadResendPart() {
		Test.testMultipartUploadResendPart();
	}

	@Test
	@Tag("KSAN")
	@Tag("Put")
	// @Tag("한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
	void testMultipartUploadMultipleSizes() {
		Test.testMultipartUploadMultipleSizes();
	}

	@Test
	@Tag("KSAN")
	@Tag("Copy")
	// @Tag("한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
	void testMultipartCopyMultipleSizes() {
		Test.testMultipartCopyMultipleSizes();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
	void testMultipartUploadSizeTooSmall() {
		Test.testMultipartUploadSizeTooSmall();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("내용물을 채운 멀티파트 업로드 성공 확인
	void testMultipartUploadContents() {
		Test.testMultipartUploadContents();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// @Tag("업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
	void testMultipartUploadOverwriteExistingObject() {
		Test.testMultipartUploadOverwriteExistingObject();
	}

	@Test
	@Tag("KSAN")
	@Tag("Cancel")
	// @Tag("멀티파트 업로드하는 도중 중단 성공 확인
	void testAbortMultipartUpload() {
		Test.testAbortMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("존재하지 않은 멀티파트 업로드 중단 실패 확인
	void testAbortMultipartUploadNotFound() {
		Test.testAbortMultipartUploadNotFound();
	}

	@Test
	@Tag("KSAN")
	@Tag("List")
	// @Tag("멀티파트 업로드 중인 목록 확인
	void testListMultipartUpload() {
		Test.testListMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
	void testMultipartUploadMissingPart() {
		Test.testMultipartUploadMissingPart();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
	void testMultipartUploadIncorrectEtag() {
		Test.testMultipartUploadIncorrectEtag();
	}

	@Test
	@Tag("KSAN")
	@Tag("Overwrite")
	// @Tag("버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
	void testAtomicMultipartUploadWrite() {
		Test.testAtomicMultipartUploadWrite();
	}

	@Test
	@Tag("KSAN")
	@Tag("List")
	// @Tag("멀티파트 업로드 목록 확인
	void testMultipartUploadList() {
		Test.testMultipartUploadList();
	}

	@Test
	@Tag("KSAN")
	@Tag("Cancel")
	// @Tag("멀티파트 업로드하는 도중 중단 성공 확인
	void testAbortMultipartUploadList() {
		Test.testAbortMultipartUploadList();
	}

	@Test
	@Tag("KSAN")
	@Tag("Copy")
	// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
	void testMultipartCopyMany() {
		Test.testMultipartCopyMany();
	}

	@Test
	@Tag("List")
	@Tag("KSAN")
	// 멀티파트 목록 확인
	void testMultipartListParts() {
		Test.testMultipartListParts();
	}
}
