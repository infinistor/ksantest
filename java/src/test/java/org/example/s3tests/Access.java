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

class Access {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Access Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Access End");
	}

	org.example.test.Access test = new org.example.test.Access();
	org.example.testV2.Access testV2 = new org.example.testV2.Access();

	/**
	 * 테스트 완료 후 정리 작업을 수행합니다.
	 *
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * BlockPublicAcls와 BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
	 */
	@Test
	@Tag("Denied")
	void testBlockPublicAclAndPolicy() {
		test.testBlockPublicAclAndPolicy();
		testV2.testBlockPublicAclAndPolicy();
	}

	/**
	 * BlockPublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
	 */
	@Test
	@Tag("Denied")
	void testBlockPublicAcls() {
		test.testBlockPublicAcls();
		testV2.testBlockPublicAcls();
	}

	/**
	 * BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
	 */
	@Test
	@Tag("Denied")
	void testBlockPublicPolicy() {
		test.testBlockPublicPolicy();
		testV2.testBlockPublicPolicy();
	}

	/**
	 * 버킷의 접근 권한 블록 삭제 기능을 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testDeletePublicBlock() {
		test.testDeletePublicBlock();
		testV2.testDeletePublicBlock();
	}

	/**
	 * IgnorePublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
	 */
	@Test
	@Tag("Denied")
	void testIgnorePublicAcls() {
		test.testIgnorePublicAcls();
		testV2.testIgnorePublicAcls();
	}

	/**
	 * 버킷의 접근 권한 블록 설정 기능을 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testPutPublicBlock() {
		test.testPutPublicBlock();
		testV2.testPutPublicBlock();
	}

	/**
	 * 새로 만든 버킷은 네 가지 퍼블릭 액세스 차단이 모두 켜져 있는지 확인
	 */
	@Test
	@Tag("Check")
	void testPublicBlockDefault() {
		test.testPublicBlockDefault();
		testV2.testPublicBlockDefault();
	}

	/**
	 * 퍼블릭 액세스 차단 설정시 지정하지 않은 항목은 false로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testPutPublicBlockPartial() {
		test.testPutPublicBlockPartial();
		testV2.testPutPublicBlockPartial();
	}

	/**
	 * 퍼블릭 액세스 차단 설정이 없는 버킷에서 조회는 실패하고 삭제는 성공하는지 확인
	 */
	@Test
	@Tag("Check")
	void testDeletePublicBlockNotExist() {
		test.testDeletePublicBlockNotExist();
		testV2.testDeletePublicBlockNotExist();
	}

	/**
	 * 존재하지 않는 버킷의 퍼블릭 액세스 차단 설정을 조회, 설정, 삭제할 경우 실패 확인
	 */
	@Test
	@Tag("Error")
	void testPublicBlockBucketNotExist() {
		test.testPublicBlockBucketNotExist();
		testV2.testPublicBlockBucketNotExist();
	}

	/**
	 * 다른 사용자가 버킷의 퍼블릭 액세스 차단 설정을 조회, 설정, 삭제할 경우 실패 확인
	 */
	@Test
	@Tag("Permission")
	void testPublicBlockAltUser() {
		test.testPublicBlockAltUser();
		testV2.testPublicBlockAltUser();
	}

	/**
	 * BlockPublicAcls 설정시 퍼블릭 ACL은 거부되고 퍼블릭이 아닌 ACL은 허용되는지 확인
	 */
	@Test
	@Tag("Denied")
	void testBlockPublicAclsAllowPrivate() {
		test.testBlockPublicAclsAllowPrivate();
		testV2.testBlockPublicAclsAllowPrivate();
	}

	/**
	 * 버킷 정책이 퍼블릭인지 여부를 GetBucketPolicyStatus로 올바르게 판정하는지 확인
	 */
	@Test
	@Tag("Policy")
	void testBucketPolicyStatus() {
		test.testBucketPolicyStatus();
		testV2.testBucketPolicyStatus();
	}

	/**
	 * BlockPublicPolicy 설정시 퍼블릭 정책은 거부되고 특정 사용자에게만 여는 정책은 허용되는지 확인
	 */
	@Test
	@Tag("Policy")
	void testBlockPublicPolicyAllowNonPublic() {
		test.testBlockPublicPolicyAllowNonPublic();
		testV2.testBlockPublicPolicyAllowNonPublic();
	}

	/**
	 * RestrictPublicBuckets 설정시 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있는지 확인
	 */
	@Test
	@Tag("Denied")
	void testRestrictPublicBuckets() {
		test.testRestrictPublicBuckets();
		testV2.testRestrictPublicBuckets();
	}
}
