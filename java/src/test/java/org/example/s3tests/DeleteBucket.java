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
 * 버킷 삭제 기능 테스트
 */
class DeleteBucket {

	org.example.test.DeleteBucket test = new org.example.test.DeleteBucket();
	org.example.testV2.DeleteBucket testV2 = new org.example.testV2.DeleteBucket();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketDeleteNotExist() {
		test.testBucketDeleteNotExist();
		testV2.testBucketDeleteNotExist();
	}

	/**
	 * 내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketDeleteNonempty() {
		test.testBucketDeleteNonempty();
		testV2.testBucketDeleteNonempty();
	}

	/**
	 * 이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketCreateDelete() {
		test.testBucketCreateDelete();
		testV2.testBucketCreateDelete();
	}
}
