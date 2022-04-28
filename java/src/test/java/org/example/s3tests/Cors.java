/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Cors {
	org.example.test.Cors Test = new org.example.test.Cors();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("버킷의 cors정보 세팅 성공 확인
	public void test_set_cors() {
		Test.test_set_cors();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
	public void test_cors_origin_response() {
		Test.test_cors_origin_response();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
	public void test_cors_origin_wildcard() {
		Test.test_cors_origin_wildcard();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
	public void test_cors_header_option() {
		Test.test_cors_header_option();
	}
}