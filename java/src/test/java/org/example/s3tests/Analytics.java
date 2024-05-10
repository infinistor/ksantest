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

class Analytics {
	org.example.test.Analytics Test = new org.example.test.Analytics();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 가능한지 확인
	void testPutBucketAnalytics() {
		Test.testPutBucketAnalytics();
	}

	@Test
	@Tag("Get")
	// 버킷 분석 설정이 올바르게 적용되는지 확인
	void testGetBucketAnalytics() {
		Test.testGetBucketAnalytics();
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 여러개 가능한지 확인
	void testAddBucketAnalytics() {
		Test.testAddBucketAnalytics();
	}
	
	@Test
	@Tag("List")
	// 버킷 분석 설정이 목록으로 조회되는지 확인
	void testListBucketAnalytics() {
		Test.testListBucketAnalytics();
	}

	@Test
	@Tag("Delete")
	// 버킷 분석 설정이 삭제되는지 확인
	void testDeleteBucketAnalytics() {
		Test.testDeleteBucketAnalytics();
	}

	@Test
	@Tag("Error")
	// 버킷 분석 설정을 잘못 입력했을 때 에러가 발생하는지 확인
	void testPutBucketAnalyticsInvalid() {
		Test.testPutBucketAnalyticsInvalid();
	}
}
