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

/**
 * 버킷의 웹사이트 기능을 테스트하는 클래스
 */
class Website {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Website Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Website End");
	}

	org.example.test.Website test = new org.example.test.Website();
	org.example.testV2.Website testV2 = new org.example.testV2.Website();

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
	 * 버킷의 Website 설정 조회 확인
	 */
	@Test
	@Tag("Check")
	void testWebsiteGetBuckets() {
		test.testWebsiteGetBuckets();
		testV2.testWebsiteGetBuckets();
	}

	/**
	 * 버킷의 Website 설정이 가능한지 확인
	 */
	@Test
	@Tag("Check")
	void testWebsitePutBuckets() {
		test.testWebsitePutBuckets();
		testV2.testWebsitePutBuckets();
	}

	/**
	 * 버킷의 Website 설정이 삭제가능한지 확인
	 */
	@Test
	@Tag("Delete")
	void testWebsiteDeleteBuckets() {
		test.testWebsiteDeleteBuckets();
		testV2.testWebsiteDeleteBuckets();
	}

	/**
	 * Website 설정 삭제 후 조회하면 NoSuchWebsiteConfiguration 에러가 발생하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testWebsiteGetAfterDelete() {
		test.testWebsiteGetAfterDelete();
		testV2.testWebsiteGetAfterDelete();
	}

	/**
	 * Website 설정이 없는 버킷에서 삭제해도 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testWebsiteDeleteNotConfigured() {
		test.testWebsiteDeleteNotConfigured();
		testV2.testWebsiteDeleteNotConfigured();
	}

	/**
	 * IndexDocument만으로 Website 설정이 가능한지 확인
	 */
	@Test
	@Tag("Check")
	void testWebsiteIndexDocumentOnly() {
		test.testWebsiteIndexDocumentOnly();
		testV2.testWebsiteIndexDocumentOnly();
	}

	/**
	 * Website 설정을 덮어쓰면 기존 설정이 완전히 대체되는지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testWebsiteOverwrite() {
		test.testWebsiteOverwrite();
		testV2.testWebsiteOverwrite();
	}

	/**
	 * RedirectAllRequestsTo(호스트, 프로토콜) 설정이 가능한지 확인
	 */
	@Test
	@Tag("Redirect")
	void testWebsiteRedirectAllRequests() {
		test.testWebsiteRedirectAllRequests();
		testV2.testWebsiteRedirectAllRequests();
	}

	/**
	 * RedirectAllRequestsTo에 호스트만 설정이 가능한지 확인
	 */
	@Test
	@Tag("Redirect")
	void testWebsiteRedirectAllRequestsHostOnly() {
		test.testWebsiteRedirectAllRequestsHostOnly();
		testV2.testWebsiteRedirectAllRequestsHostOnly();
	}

	/**
	 * RoutingRules 설정이 가능하고 조회 결과가 일치하는지 확인
	 */
	@Test
	@Tag("RoutingRule")
	void testWebsiteRoutingRules() {
		test.testWebsiteRoutingRules();
		testV2.testWebsiteRoutingRules();
	}

	/**
	 * RoutingRules를 최대 개수(50개)까지 설정 가능한지 확인
	 */
	@Test
	@Tag("RoutingRule")
	void testWebsiteRoutingRulesMax() {
		test.testWebsiteRoutingRulesMax();
		testV2.testWebsiteRoutingRulesMax();
	}

	/**
	 * RoutingRules가 최대 개수(50개)를 넘으면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRoutingRulesOverMax() {
		test.testWebsiteRoutingRulesOverMax();
		testV2.testWebsiteRoutingRulesOverMax();
	}

	/**
	 * RedirectAllRequestsTo 없이 IndexDocument를 빠뜨리면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteNoIndexDocument() {
		testV2.testWebsiteNoIndexDocument();
	}

	/**
	 * IndexDocument Suffix가 비어있으면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteEmptyIndexSuffix() {
		test.testWebsiteEmptyIndexSuffix();
		testV2.testWebsiteEmptyIndexSuffix();
	}

	/**
	 * IndexDocument Suffix에 '/'가 포함되면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteIndexSuffixWithSlash() {
		test.testWebsiteIndexSuffixWithSlash();
		testV2.testWebsiteIndexSuffixWithSlash();
	}

	/**
	 * RedirectAllRequestsTo와 IndexDocument를 함께 설정하면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRedirectAllWithIndexDocument() {
		test.testWebsiteRedirectAllWithIndexDocument();
		testV2.testWebsiteRedirectAllWithIndexDocument();
	}

	/**
	 * RedirectAllRequestsTo와 RoutingRules를 함께 설정하면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRedirectAllWithRoutingRules() {
		test.testWebsiteRedirectAllWithRoutingRules();
		testV2.testWebsiteRedirectAllWithRoutingRules();
	}

	/**
	 * RedirectAllRequestsTo의 Protocol이 http/https가 아니면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRedirectAllInvalidProtocol() {
		test.testWebsiteRedirectAllInvalidProtocol();
		testV2.testWebsiteRedirectAllInvalidProtocol();
	}

	/**
	 * RoutingRule에 ReplaceKeyWith와 ReplaceKeyPrefixWith를 함께 설정하면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRoutingRuleReplaceKeyAndPrefix() {
		test.testWebsiteRoutingRuleReplaceKeyAndPrefix();
		testV2.testWebsiteRoutingRuleReplaceKeyAndPrefix();
	}

	/**
	 * RoutingRule의 HttpRedirectCode가 3XX가 아니면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRoutingRuleInvalidRedirectCode() {
		test.testWebsiteRoutingRuleInvalidRedirectCode();
		testV2.testWebsiteRoutingRuleInvalidRedirectCode();
	}

	/**
	 * RoutingRule 조건의 HttpErrorCodeReturnedEquals가 4XX/5XX가 아니면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteRoutingRuleInvalidErrorCode() {
		test.testWebsiteRoutingRuleInvalidErrorCode();
		testV2.testWebsiteRoutingRuleInvalidErrorCode();
	}

	/**
	 * RoutingRule의 Redirect가 비어있어도 설정이 가능한지 확인
	 */
	@Test
	@Tag("RoutingRule")
	void testWebsiteRoutingRuleEmptyRedirect() {
		test.testWebsiteRoutingRuleEmptyRedirect();
		testV2.testWebsiteRoutingRuleEmptyRedirect();
	}

	/**
	 * 존재하지 않는 버킷에 Website 설정/조회/삭제 시 NoSuchBucket 에러가 발생하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteBucketNotFound() {
		test.testWebsiteBucketNotFound();
		testV2.testWebsiteBucketNotFound();
	}

	/**
	 * 다른 사용자가 Website 설정/조회/삭제할 수 없는지 확인
	 */
	@Test
	@Tag("Access")
	void testWebsiteAccessDenied() {
		test.testWebsiteAccessDenied();
		testV2.testWebsiteAccessDenied();
	}

	/**
	 * 웹사이트 엔드포인트에서 루트와 디렉터리 요청에 IndexDocument를 돌려주는지 확인
	 */
	@Test
	@Tag("Access")
	void testWebsiteAccessIndexDocument() {
		testV2.testWebsiteAccessIndexDocument();
	}

	/**
	 * 없는 객체를 요청하면 ErrorDocument 내용을 404로 돌려주는지 확인
	 */
	@Test
	@Tag("Access")
	void testWebsiteAccessErrorDocument() {
		testV2.testWebsiteAccessErrorDocument();
	}

	/**
	 * ErrorDocument가 없을 때 없는 객체를 요청하면 기본 404 NoSuchKey 페이지를 돌려주는지 확인
	 */
	@Test
	@Tag("Access")
	void testWebsiteAccessNoErrorDocument() {
		testV2.testWebsiteAccessNoErrorDocument();
	}

	/**
	 * RedirectAllRequestsTo 설정 시 모든 요청을 지정한 호스트로 301 리다이렉트하는지 확인
	 */
	@Test
	@Tag("Redirect")
	void testWebsiteAccessRedirectAllRequests() {
		testV2.testWebsiteAccessRedirectAllRequests();
	}

	/**
	 * KeyPrefixEquals 라우팅 규칙으로 접두어를 바꿔 리다이렉트하는지 확인
	 */
	@Test
	@Tag("RoutingRule")
	void testWebsiteAccessRoutingRuleKeyPrefix() {
		testV2.testWebsiteAccessRoutingRuleKeyPrefix();
	}

	/**
	 * HttpErrorCodeReturnedEquals 라우팅 규칙으로 404일 때만 리다이렉트하는지 확인
	 */
	@Test
	@Tag("RoutingRule")
	void testWebsiteAccessRoutingRuleErrorCode() {
		testV2.testWebsiteAccessRoutingRuleErrorCode();
	}

	/**
	 * 객체의 x-amz-website-redirect-location 메타데이터로 리다이렉트하는지 확인
	 */
	@Test
	@Tag("Redirect")
	void testWebsiteAccessObjectRedirectLocation() {
		testV2.testWebsiteAccessObjectRedirectLocation();
	}

	/**
	 * 공개되지 않은 버킷을 웹사이트로 접근하면 403 AccessDenied가 반환되는지 확인
	 */
	@Test
	@Tag("Access")
	void testWebsiteAccessNotPublic() {
		testV2.testWebsiteAccessNotPublic();
	}

	/**
	 * 웹사이트 설정이 없는 버킷에 접근하면 404 NoSuchWebsiteConfiguration이 반환되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testWebsiteAccessNoConfiguration() {
		testV2.testWebsiteAccessNoConfiguration();
	}
}
