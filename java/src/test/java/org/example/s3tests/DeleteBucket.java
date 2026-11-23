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
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("DeleteBucket Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("DeleteBucket End");
	}


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

	/**
	 * 버저닝 버킷에 이전 버전과 삭제 마커가 남아 있으면 삭제에 실패하고, 모두 지우면 삭제되는지 확인
	 */
	@Test
	@Tag("Versioning")
	void testBucketDeleteVersioned() {
		test.testBucketDeleteVersioned();
		testV2.testBucketDeleteVersioned();
	}

	/**
	 * 완료되지 않은 멀티파트 업로드만 남아 있는 버킷을 삭제할 수 있는지 확인
	 */
	@Test
	@Tag("Multipart")
	void testBucketDeleteWithMultipartUpload() {
		test.testBucketDeleteWithMultipartUpload();
		testV2.testBucketDeleteWithMultipartUpload();
	}

	/**
	 * 다른 사용자가 버킷을 삭제하려 했을 경우 실패 확인
	 */
	@Test
	@Tag("Permission")
	void testBucketDeleteAltUser() {
		test.testBucketDeleteAltUser();
		testV2.testBucketDeleteAltUser();
	}

	/**
	 * 태그, CORS, 수명주기 설정이 남아 있어도 비어 있는 버킷은 삭제할 수 있는지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketDeleteWithConfigurations() {
		test.testBucketDeleteWithConfigurations();
		testV2.testBucketDeleteWithConfigurations();
	}

	/**
	 * 버킷을 삭제하고 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않은지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketDeleteRecreate() {
		test.testBucketDeleteRecreate();
		testV2.testBucketDeleteRecreate();
	}
}
