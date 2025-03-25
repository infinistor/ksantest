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
 * 버킷 생성 기능을 테스트하는 클래스
 */
class PutBucket {

	org.example.test.PutBucket test = new org.example.test.PutBucket();
	org.example.testV2.PutBucket testV2 = new org.example.testV2.PutBucket();

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
	 * 생성한 버킷이 비어있는지 확인
	 */
	@Test
	@Tag("PUT")
	void testBucketListEmpty() {
		test.testBucketListEmpty();
		testV2.testBucketListEmpty();
	}

	/**
	 * 생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingBadStartsNonAlpha() {
		test.testBucketCreateNamingBadStartsNonAlpha();
		testV2.testBucketCreateNamingBadStartsNonAlpha();
	}

	/**
	 * 생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingBadShortOne() {
		test.testBucketCreateNamingBadShortOne();
		testV2.testBucketCreateNamingBadShortOne();
	}

	/**
	 * 생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingBadShortTwo() {
		test.testBucketCreateNamingBadShortTwo();
		testV2.testBucketCreateNamingBadShortTwo();
	}

	/**
	 * 생성할 버킷이름이 60자인 경우 버킷 생성 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodLong60() {
		test.testBucketCreateNamingGoodLong60();
		testV2.testBucketCreateNamingGoodLong60();
	}

	/**
	 * 생성할 버킷이름이 61자인 경우 버킷 생성 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodLong61() {
		test.testBucketCreateNamingGoodLong61();
		testV2.testBucketCreateNamingGoodLong61();
	}

	/**
	 * 생성할 버킷이름이 62자인 경우 버킷 생성 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodLong62() {
		test.testBucketCreateNamingGoodLong62();
		testV2.testBucketCreateNamingGoodLong62();
	}

	/**
	 * 생성할 버킷이름이 63자인 경우 버킷 생성 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodLong63() {
		test.testBucketCreateNamingGoodLong63();
		testV2.testBucketCreateNamingGoodLong63();
	}

	/**
	 * 생성할 버킷이름이 64자인 경우 버킷 생성 실패
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodLong64() {
		test.testBucketCreateNamingGoodLong64();
		testV2.testBucketCreateNamingGoodLong64();
	}

	/**
	 * 생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingBadIp() {
		test.testBucketCreateNamingBadIp();
		testV2.testBucketCreateNamingBadIp();
	}

	/**
	 * 생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsUnderscore() {
		test.testBucketCreateNamingDnsUnderscore();
		testV2.testBucketCreateNamingDnsUnderscore();
	}

	/**
	 * 생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsLong() {
		test.testBucketCreateNamingDnsLong();
		testV2.testBucketCreateNamingDnsLong();
	}

	/**
	 * 생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsDashAtEnd() {
		test.testBucketCreateNamingDnsDashAtEnd();
		testV2.testBucketCreateNamingDnsDashAtEnd();
	}

	/**
	 * 생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsDotDot() {
		test.testBucketCreateNamingDnsDotDot();
		testV2.testBucketCreateNamingDnsDotDot();
	}

	/**
	 * 생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsDotDash() {
		test.testBucketCreateNamingDnsDotDash();
		testV2.testBucketCreateNamingDnsDotDash();
	}

	/**
	 * 생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingDnsDashDot() {
		test.testBucketCreateNamingDnsDashDot();
		testV2.testBucketCreateNamingDnsDashDot();
	}

	/**
	 * 버킷 중복 생성시 실패 확인
	 */
	@Test
	@Tag("Duplicate")
	void testBucketCreateExists() {
		test.testBucketCreateExists();
		testV2.testBucketCreateExists();
	}

	/**
	 * [다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인
	 */
	@Test
	@Tag("Duplicate")
	void testBucketCreateExistsNonowner() {
		test.testBucketCreateExistsNonowner();
		testV2.testBucketCreateExistsNonowner();
	}

	/**
	 * 생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodStartsAlpha() {
		test.testBucketCreateNamingGoodStartsAlpha();
		testV2.testBucketCreateNamingGoodStartsAlpha();
	}

	/**
	 * 생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodStartsDigit() {
		test.testBucketCreateNamingGoodStartsDigit();
		testV2.testBucketCreateNamingGoodStartsDigit();
	}

	/**
	 * 생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodContainsPeriod() {
		test.testBucketCreateNamingGoodContainsPeriod();
		testV2.testBucketCreateNamingGoodContainsPeriod();
	}

	/**
	 * 생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인
	 */
	@Test
	@Tag("CreationRules")
	void testBucketCreateNamingGoodContainsHyphen() {
		test.testBucketCreateNamingGoodContainsHyphen();
		testV2.testBucketCreateNamingGoodContainsHyphen();
	}

	/**
	 * 버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)
	 */
	@Test
	@Tag("Duplicate")
	void testBucketRecreateNotOverriding() {
		test.testBucketRecreateNotOverriding();
		testV2.testBucketRecreateNotOverriding();
	}

	/**
	 * 버킷의 location 정보 조회
	 */
	@Test
	@Tag("location")
	void testGetBucketLocation() {
		test.testGetBucketLocation();
		testV2.testGetBucketLocation();
	}
}
