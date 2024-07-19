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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;

class Website {
	org.example.test.Website test = new org.example.test.Website();
	org.example.testV2.Website testV2 = new org.example.testV2.Website();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷의 Website 설정 조회 확인
	void testWebsiteGetBuckets() {
		test.testWebsiteGetBuckets();
		testV2.testWebsiteGetBuckets();
	}

	@Test
	@Tag("Check")
	// 버킷의 Website 설정이 가능한지 확인
	void testWebsitePutBuckets() {
		test.testWebsitePutBuckets();
		testV2.testWebsitePutBuckets();
	}

	@Test
	@Tag("Delete")
	// 버킷의 Website 설정이 삭제가능한지 확인
	void testWebsiteDeleteBuckets() {
		test.testWebsiteDeleteBuckets();
		testV2.testWebsiteDeleteBuckets();
	}
}
