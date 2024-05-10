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

class PutBucket {

	org.example.test.PutBucket Test = new org.example.test.PutBucket();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("PUT")
	// 생성한 버킷이 비어있는지 확인
	void testBucketListEmpty() {
		Test.testBucketListEmpty();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingBadStartsNonAlpha() {
		Test.testBucketCreateNamingBadStartsNonAlpha();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인
	void testBucketCreateNamingBadShortOne() {
		Test.testBucketCreateNamingBadShortOne();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인
	void testBucketCreateNamingBadShortTwo() {
		Test.testBucketCreateNamingBadShortTwo();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 60자인 경우 버킷 생성 확인
	void testBucketCreateNamingGoodLong60() {
		Test.testBucketCreateNamingGoodLong60();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 61자인 경우 버킷 생성 확인
	void testBucketCreateNamingGoodLong61() {
		Test.testBucketCreateNamingGoodLong61();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 62자인 경우 버킷 생성 확인
	void testBucketCreateNamingGoodLong62() {
		Test.testBucketCreateNamingGoodLong62();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 63자인 경우 버킷 생성 확인
	void testBucketCreateNamingGoodLong63() {
		Test.testBucketCreateNamingGoodLong63();
	}

	@Test
	@Tag("CreationRules")
	//생성할 버킷이름이 64자인 경우 버킷 생성 실패
	void testBucketCreateNamingGoodLong64() {
		Test.testBucketCreateNamingGoodLong64();
	}


	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 버킷이름의 길이 긴 경우 버킷 목록을 읽어올 수 있는지 확인
	void testBucketListLongName() {
		Test.testBucketListLongName();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingBadIp() {
		Test.testBucketCreateNamingBadIp();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingDnsUnderscore() {
		Test.testBucketCreateNamingDnsUnderscore();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인
	void testBucketCreateNamingDnsLong() {
		Test.testBucketCreateNamingDnsLong();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인
	void testBucketCreateNamingDnsDashAtEnd() {
		Test.testBucketCreateNamingDnsDashAtEnd();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingDnsDotDot() {
		Test.testBucketCreateNamingDnsDotDot();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingDnsDotDash() {
		Test.testBucketCreateNamingDnsDotDash();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인
	void testBucketCreateNamingDnsDashDot() {
		Test.testBucketCreateNamingDnsDashDot();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// 버킷 중복 생성시 실패 확인
	void testBucketCreateExists() {
		Test.testBucketCreateExists();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// [다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인
	void testBucketCreateExistsNonowner() {
		Test.testBucketCreateExistsNonowner();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인
	void testBucketCreateNamingGoodStartsAlpha() {
		Test.testBucketCreateNamingGoodStartsAlpha();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인
	void testBucketCreateNamingGoodStartsDigit() {
		Test.testBucketCreateNamingGoodStartsDigit();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인
	void testBucketCreateNamingGoodContainsPeriod() {
		Test.testBucketCreateNamingGoodContainsPeriod();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인
	void testBucketCreateNamingGoodContainsHyphen() {
		Test.testBucketCreateNamingGoodContainsHyphen();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// 버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)
	void testBucketRecreateNotOverriding() {
		Test.testBucketRecreateNotOverriding();
	}

	@Test
	@Tag("KSAN")
	@Tag("location")
	// 버킷의 location 정보 조회
	void testGetBucketLocation() {
		Test.testGetBucketLocation();
	}
}
