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
 * 멀티파트 업로드 기능을 테스트하는 클래스
 */
class Multipart {

	org.example.test.Multipart test = new org.example.test.Multipart();
	org.example.testV2.Multipart testV2 = new org.example.testV2.Multipart();

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
	 * 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testMultipartUploadEmpty() {
		test.testMultipartUploadEmpty();
		testV2.testMultipartUploadEmpty();
	}

	/**
	 * 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
	 */
	@Test
	@Tag("Check")
	void testMultipartUploadSmall() {
		test.testMultipartUploadSmall();
		testV2.testMultipartUploadSmall();
	}

	/**
	 * 버킷a에서 버킷b로 멀티파트 복사 성공확인
	 */
	@Test
	@Tag("Copy")
	void testMultipartCopySmall() {
		test.testMultipartCopySmall();
		testV2.testMultipartCopySmall();
	}

	/**
	 * 범위설정을 잘못한 멀티파트 복사 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testMultipartCopyInvalidRange() {
		test.testMultipartCopyInvalidRange();
		testV2.testMultipartCopyInvalidRange();
	}

	/**
	 * 범위를 지정한 멀티파트 복사 성공확인
	 */
	@Test
	@Tag("Range")
	void testMultipartCopyWithoutRange() {
		test.testMultipartCopyWithoutRange();
		testV2.testMultipartCopyWithoutRange();
	}

	/**
	 * 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
	 */
	@Test
	@Tag("SpecialNames")
	void testMultipartCopySpecialNames() {
		test.testMultipartCopySpecialNames();
		testV2.testMultipartCopySpecialNames();
	}

	/**
	 * 멀티파트 업로드 확인
	 */
	@Test
	@Tag("Put")
	void testMultipartUpload() {
		test.testMultipartUpload();
		testV2.testMultipartUpload();
	}

	/**
	 * 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
	 */
	@Test
	@Tag("Copy")
	void testMultipartCopyVersioned() {
		test.testMultipartCopyVersioned();
		testV2.testMultipartCopyVersioned();
	}

	/**
	 * 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
	 */
	@Test
	@Tag("Duplicate")
	void testMultipartUploadResendPart() {
		test.testMultipartUploadResendPart();
		testV2.testMultipartUploadResendPart();
	}

	/**
	 * 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
	 */
	@Test
	@Tag("Put")
	void testMultipartUploadMultipleSizes() {
		test.testMultipartUploadMultipleSizes();
		testV2.testMultipartUploadMultipleSizes();
	}

	/**
	 * 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
	 */
	@Test
	@Tag("Copy")
	void testMultipartCopyMultipleSizes() {
		test.testMultipartCopyMultipleSizes();
		testV2.testMultipartCopyMultipleSizes();
	}

	/**
	 * 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testMultipartUploadSizeTooSmall() {
		test.testMultipartUploadSizeTooSmall();
		testV2.testMultipartUploadSizeTooSmall();
	}

	/**
	 * 내용물을 채운 멀티파트 업로드 성공 확인
	 */
	@Test
	@Tag("Check")
	void testMultipartUploadContents() {
		test.testMultipartUploadContents();
		testV2.testMultipartUploadContents();
	}

	/**
	 * 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
	 */
	@Test
	@Tag("OverWrite")
	void testMultipartUploadOverwriteExistingObject() {
		test.testMultipartUploadOverwriteExistingObject();
		testV2.testMultipartUploadOverwriteExistingObject();
	}

	/**
	 * 멀티파트 업로드하는 도중 중단 성공 확인
	 */
	@Test
	@Tag("Cancel")
	void testAbortMultipartUpload() {
		test.testAbortMultipartUpload();
		testV2.testAbortMultipartUpload();
	}

	/**
	 * 존재하지 않은 멀티파트 업로드 중단 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testAbortMultipartUploadNotFound() {
		test.testAbortMultipartUploadNotFound();
		testV2.testAbortMultipartUploadNotFound();
	}

	/**
	 * 멀티파트 업로드 중인 목록 확인
	 */
	@Test
	@Tag("List")
	void testListMultipartUpload() {
		test.testListMultipartUpload();
		testV2.testListMultipartUpload();
	}

	/**
	 * 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testMultipartUploadMissingPart() {
		test.testMultipartUploadMissingPart();
		testV2.testMultipartUploadMissingPart();
	}

	/**
	 * 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testMultipartUploadIncorrectEtag() {
		test.testMultipartUploadIncorrectEtag();
		testV2.testMultipartUploadIncorrectEtag();
	}

	/**
	 * 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
	 */
	@Test
	@Tag("Overwrite")
	void testAtomicMultipartUploadWrite() {
		test.testAtomicMultipartUploadWrite();
		testV2.testAtomicMultipartUploadWrite();
	}

	/**
	 * 멀티파트 업로드 목록 확인
	 */
	@Test
	@Tag("List")
	void testMultipartUploadList() {
		test.testMultipartUploadList();
		testV2.testMultipartUploadList();
	}

	/**
	 * 멀티파트 업로드하는 도중 중단 성공 확인
	 */
	@Test
	@Tag("Cancel")
	void testAbortMultipartUploadList() {
		test.testAbortMultipartUploadList();
		testV2.testAbortMultipartUploadList();
	}

	/**
	 * 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
	 */
	@Test
	@Tag("Copy")
	void testMultipartCopyMany() {
		test.testMultipartCopyMany();
		testV2.testMultipartCopyMany();
	}

	/**
	 * 멀티파트 목록 확인
	 */
	@Test
	@Tag("List")
	void testMultipartListParts() {
		test.testMultipartListParts();
		testV2.testMultipartListParts();
	}

	/**
	 * UseChunkEncoding을 사용하는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
	 */
	@Test
	@Tag("checksum")
	void testMultipartUploadChecksumUseChunkEncoding() {
		testV2.testMultipartUploadChecksumUseChunkEncoding();
	}

	/**
	 * UseChunkEncoding을 사용하지 않는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
	 */
	@Test
	@Tag("checksum")
	void testMultipartUploadChecksum() {
		testV2.testMultipartUploadChecksum();
	}

	/**
	 * 멀티파트 업로드 시 체크섬 계산 및 검증 실패 확인
	 */
	@Test
	@Tag("checksum-failure")
	void testMultipartUploadChecksumFailure() {
		testV2.testMultipartUploadChecksumFailure();
	}

	/**
	 * 멀티파트 업로드 시 체크섬 계산 및 검증 확인
	 */
	@Test
	@Tag("checksum")
	void testMultipartCopyChecksum() {
		testV2.testMultipartCopyChecksum();
	}

	/**
	 * 멀티파트 업로드 시 체크섬 알고리즘이 누락될 경우 에러 확인
	 */
	@Test
	@Tag("error")
	void testcreateMultipartUploadEmptyChecksumAlgorithm() {
		testV2.testcreateMultipartUploadEmptyChecksumAlgorithm();
	}

	/**
	 * 멀티파트 업로드 시 체크섬 타입이 누락될 경우 에러 확인
	 */
	@Test
	@Tag("error")
	void testcreateMultipartUploadEmptyChecksumType() {
		testV2.testcreateMultipartUploadEmptyChecksumType();
	}

}
