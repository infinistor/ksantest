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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷의 로깅 기능을 테스트하는 클래스
 */
class Logging {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Logging Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Logging End");
	}

	org.example.test.Logging test = new org.example.test.Logging();
	org.example.testV2.Logging testV2 = new org.example.testV2.Logging();

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
	 * 버킷에 로깅 설정 조회 가능한지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingGet() {
		test.testLoggingGet();
		testV2.testLoggingGet();
	}

	/**
	 * 버킷에 로깅 설정 가능한지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingSet() {
		test.testLoggingSet();
		testV2.testLoggingSet();
	}

	/**
	 * 버킷에 설정한 로깅 정보 조회가 가능한지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingSetGet() {
		test.testLoggingSetGet();
		testV2.testLoggingSetGet();
	}

	/**
	 * 버킷의 로깅에 Prefix가 설정되는지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingPrefix() {
		test.testLoggingPrefix();
		testV2.testLoggingPrefix();
	}

	/**
	 * 버저닝 설정된 버킷의 로깅이 설정되는지 확인
	 */
	@Test
	@Tag("Versioning")
	void testLoggingVersioning() {
		test.testLoggingVersioning();
		testV2.testLoggingVersioning();
	}

	/**
	 * SSE-s3설정된 버킷의 로깅이 설정되는지 확인
	 */
	@Test
	@Tag("Encryption")
	void testLoggingEncryption() {
		test.testLoggingEncryption();
		testV2.testLoggingEncryption();
	}

	/**
	 * 존재하지 않는 버킷에 로깅 설정 실패 확인
	 */
	@Test
	@Tag("Error")
	void testLoggingBucketNotFound() {
		test.testLoggingBucketNotFound();
		testV2.testLoggingBucketNotFound();
	}

	/**
	 * 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
	 */
	@Test
	@Tag("Error")
	void testLoggingTargetBucketNotFound() {
		test.testLoggingTargetBucketNotFound();
		testV2.testLoggingTargetBucketNotFound();
	}

	/**
	 * 빈 설정을 넣으면 버킷의 로깅이 해제되는지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingDisable() {
		test.testLoggingDisable();
		testV2.testLoggingDisable();
	}

	/**
	 * 로깅 설정을 다시 넣으면 기존 설정을 덮어쓰는지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingOverwrite() {
		test.testLoggingOverwrite();
		testV2.testLoggingOverwrite();
	}

	/**
	 * 로그 오브젝트 키 형식을 SimplePrefix 로 설정하고 조회되는지 확인
	 */
	@Test
	@Tag("KeyFormat")
	void testLoggingSimplePrefix() {
		test.testLoggingSimplePrefix();
		testV2.testLoggingSimplePrefix();
	}

	/**
	 * 로그 오브젝트 키 형식을 PartitionedPrefix(EventTime/DeliveryTime)로 설정하고 조회되는지 확인
	 */
	@Test
	@Tag("KeyFormat")
	void testLoggingPartitionedPrefix() {
		test.testLoggingPartitionedPrefix();
		testV2.testLoggingPartitionedPrefix();
	}

	/**
	 * 소스 버킷 자신을 타깃 버킷으로 지정할 수 있는지 확인
	 */
	@Test
	@Tag("Put/Get")
	void testLoggingSameBucket() {
		test.testLoggingSameBucket();
		testV2.testLoggingSameBucket();
	}

	/**
	 * 타깃 버킷의 소유자가 다를때 로깅 설정 실패 확인
	 */
	@Test
	@Tag("Error")
	void testLoggingTargetBucketOtherOwner() {
		test.testLoggingTargetBucketOtherOwner();
		testV2.testLoggingTargetBucketOtherOwner();
	}

	/**
	 * 존재하지 않는 버킷의 로깅 설정 조회 실패 확인
	 */
	@Test
	@Tag("Error")
	void testLoggingGetBucketNotExist() {
		test.testLoggingGetBucketNotExist();
		testV2.testLoggingGetBucketNotExist();
	}

	/**
	 * 권한이 없는 사용자의 로깅 설정 조회/저장 실패 확인
	 */
	@Test
	@Tag("Permission")
	void testLoggingAltUser() {
		test.testLoggingAltUser();
		testV2.testLoggingAltUser();
	}
}
