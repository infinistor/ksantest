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

class Cors {
	org.example.test.Cors test = new org.example.test.Cors();
	org.example.testV2.Cors testV2 = new org.example.testV2.Cors();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷의 cors정보 세팅 성공 확인
	void testSetCors() {
		test.testSetCors();
		testV2.testSetCors();
	}

	@Test
	@Tag("Post")
	// 버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
	void testCorsOriginResponse() {
		test.testCorsOriginResponse();
		testV2.testCorsOriginResponse();
	}

	@Test
	@Tag("Post")
	// 와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
	void testCorsOriginWildcard() {
		test.testCorsOriginWildcard();
		testV2.testCorsOriginWildcard();
	}

	@Test
	@Tag("Post")
	// cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
	void testCorsHeaderOption() {
		test.testCorsHeaderOption();
		testV2.testCorsHeaderOption();
	}
}