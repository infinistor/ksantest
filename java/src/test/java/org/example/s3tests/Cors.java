/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it
under the terms of
* the GNU General Public License as published by the Free Software
Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가
있습니다.
*/
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * CORS 테스트
 */
class Cors {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Cors Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Cors End");
	}

	org.example.test.Cors test = new org.example.test.Cors();
	org.example.testV2.Cors testV2 = new org.example.testV2.Cors();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷의 cors정보 세팅 성공 확인
	 */
	@Test
	@Tag("Check")
	void testSetCors() {
		test.testSetCors();
		testV2.testSetCors();
	}

	/**
	 * 버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
	 */
	@Test
	@Tag("Post")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 CORS 설정이 있는 버킷의
	// OPTIONS 는 Origin 이 없거나 규칙에 맞지 않아도 200 을 준다(2026-04 InfiniStor 와 같게 맞춤).
	// AWS 는 Origin 이 없으면 400, 규칙에 맞지 않으면 403 이다.
	void testCorsOriginResponse() {
		test.testCorsOriginResponse();
		testV2.testCorsOriginResponse();
	}

	/**
	 * 와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
	 */
	@Test
	@Tag("Post")
	void testCorsOriginWildcard() {
		test.testCorsOriginWildcard();
		testV2.testCorsOriginWildcard();
	}

	/**
	 * cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
	 */
	@Test
	@Tag("Post")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 허용하지 않은
	// Access-Control-Request-Headers 로 preflight 해도 200 을 준다(InfiniStor 와 같음). AWS 는 403 이다.
	void testCorsHeaderOption() {
		test.testCorsHeaderOption();
		testV2.testCorsHeaderOption();
	}

	/**
	 * cors 규칙의 모든 필드(ID, 허용 헤더, 노출 헤더, MaxAge)가 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Check")
	void testCorsAllFields() {
		test.testCorsAllFields();
		testV2.testCorsAllFields();
	}

	/**
	 * preflight 응답 헤더가 규칙대로 내려오고 허용되지 않은 헤더 요청은 거부되는지 확인
	 */
	@Test
	@Tag("Preflight")
	void testCorsPreflightHeaders() {
		test.testCorsPreflightHeaders();
		testV2.testCorsPreflightHeaders();
	}

	/**
	 * 여러 cors 규칙 중 먼저 선언된 규칙이 우선 적용되는지 확인
	 */
	@Test
	@Tag("Preflight")
	void testCorsRuleOrder() {
		test.testCorsRuleOrder();
		testV2.testCorsRuleOrder();
	}

	/**
	 * 오브젝트 GET/HEAD/preflight 요청에 cors 응답 헤더가 내려오는지 확인
	 */
	@Test
	@Tag("Object")
	void testCorsObjectRequest() {
		test.testCorsObjectRequest();
		testV2.testCorsObjectRequest();
	}

	/**
	 * 허용되지 않는 메서드로 cors 설정 시 실패하는지 확인 (SDK v1 은 AllowedMethods 가 enum 이라 잘못된 메서드를 표현할 수 없어 V2 만 실행)
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedMethod 가 GET·PUT·POST·DELETE·HEAD(대문자)가 아니면 400 InvalidRequest 다.
	void testCorsInvalidMethod() {
		testV2.testCorsInvalidMethod();
	}

	/**
	 * Origin 에 와일드카드가 둘 이상이면 cors 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedOrigin 에 * 가 둘 이상이면 400 InvalidRequest 다.
	void testCorsInvalidOriginWildcard() {
		test.testCorsInvalidOriginWildcard();
		testV2.testCorsInvalidOriginWildcard();
	}

	/**
	 * 허용 헤더에 와일드카드가 둘 이상이면 cors 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedHeader 에 * 가 둘 이상이면 400 InvalidRequest 다.
	void testCorsInvalidHeaderWildcard() {
		test.testCorsInvalidHeaderWildcard();
		testV2.testCorsInvalidHeaderWildcard();
	}

	/**
	 * 노출 헤더에 와일드카드를 쓰면 cors 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 ExposeHeader 에 * 가 있으면 400 InvalidRequest 다.
	void testCorsExposeHeaderWildcard() {
		test.testCorsExposeHeaderWildcard();
		testV2.testCorsExposeHeaderWildcard();
	}

	/**
	 * cors 규칙은 100개까지 허용되고 101개는 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 규칙이 101개 이상이면 400 InvalidRequest 다.
	void testCorsRuleLimit() {
		test.testCorsRuleLimit();
		testV2.testCorsRuleLimit();
	}

	/**
	 * cors 설정을 다시 넣으면 기존 설정을 덮어쓰는지 확인
	 */
	@Test
	@Tag("Check")
	void testCorsOverwrite() {
		test.testCorsOverwrite();
		testV2.testCorsOverwrite();
	}

	/**
	 * cors 설정이 없는 버킷에서도 삭제가 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testCorsDeleteNotExist() {
		test.testCorsDeleteNotExist();
		testV2.testCorsDeleteNotExist();
	}

	/**
	 * 존재하지 않는 버킷의 cors 설정 조회/저장/삭제가 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testCorsBucketNotExist() {
		test.testCorsBucketNotExist();
		testV2.testCorsBucketNotExist();
	}

	/**
	 * 권한이 없는 사용자의 cors 설정 조회/저장/삭제가 실패하는지 확인
	 */
	@Test
	@Tag("Permission")
	void testCorsAltUser() {
		test.testCorsAltUser();
		testV2.testCorsAltUser();
	}
}
