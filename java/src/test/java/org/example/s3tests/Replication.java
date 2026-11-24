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
 * 버킷의 복제 기능을 테스트하는 클래스
 */
class Replication {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Replication Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Replication End");
	}


	org.example.test.Replication test = new org.example.test.Replication();
	org.example.testV2.Replication testV2 = new org.example.testV2.Replication();

	/**
	 * 테스트 정리 작업 수행
	 * 
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷의 Replication 설정이 되는지 확인(put/get/delete)
	 */
	@Test
	@Tag("Check")
	void testReplicationSet() {
		test.testReplicationSet();
		testV2.testReplicationSet();
	}

	/**
	 * 원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationInvalidSourceBucketName() {
		test.testReplicationInvalidSourceBucketName();
		testV2.testReplicationInvalidSourceBucketName();
	}

	/**
	 * 원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationInvalidSourceBucketVersioning() {
		test.testReplicationInvalidSourceBucketVersioning();
		testV2.testReplicationInvalidSourceBucketVersioning();
	}

	/**
	 * 대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationInvalidTargetBucketName() {
		test.testReplicationInvalidTargetBucketName();
		testV2.testReplicationInvalidTargetBucketName();
	}

	/**
	 * 대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationInvalidTargetBucketVersioning() {
		test.testReplicationInvalidTargetBucketVersioning();
		testV2.testReplicationInvalidTargetBucketVersioning();
	}

	/**
	 * 버킷에 복제 설정을 하고 버저닝 설정을 중단 했을 때 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationBucketVersioningSuspend() {
		test.testReplicationBucketVersioningSuspend();
		testV2.testReplicationBucketVersioningSuspend();
	}

	/**
	 * 복제 설정이 없는 버킷의 조회는 실패하고 삭제는 성공하는지 확인
	 */
	@Test
	@Tag("Check")
	void testReplicationGetNotExist() {
		test.testReplicationGetNotExist();
		testV2.testReplicationGetNotExist();
	}

	/**
	 * 태그 필터를 가진 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testReplicationTagFilter() {
		test.testReplicationTagFilter();
		testV2.testReplicationTagFilter();
	}

	/**
	 * 접두어와 태그를 묶은 And 필터를 가진 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testReplicationAndFilter() {
		test.testReplicationAndFilter();
		testV2.testReplicationAndFilter();
	}

	/**
	 * 빈 접두어 필터를 가진 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testReplicationEmptyFilter() {
		test.testReplicationEmptyFilter();
		testV2.testReplicationEmptyFilter();
	}

	/**
	 * 구버전(V1) 스키마의 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testReplicationV1Schema() {
		test.testReplicationV1Schema();
		testV2.testReplicationV1Schema();
	}

	/**
	 * 여러 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Check")
	void testReplicationMultipleRules() {
		test.testReplicationMultipleRules();
		testV2.testReplicationMultipleRules();
	}

	/**
	 * 복제 규칙의 Priority 가 중복되면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationDuplicatePriority() {
		test.testReplicationDuplicatePriority();
		testV2.testReplicationDuplicatePriority();
	}

	/**
	 * 복제 규칙의 ID 가 중복되면 실패하는지 확인 (SDK v1 은 규칙을 ID 키의 Map 으로 관리해 중복 ID 를 표현할 수 없어 V2 만 실행)
	 */
	@Test
	@Tag("ERROR")
	void testReplicationDuplicateId() {
		testV2.testReplicationDuplicateId();
	}

	/**
	 * Filter 를 쓰는 규칙에 Priority 나 DeleteMarkerReplication 이 없으면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationFilterWithoutPriority() {
		test.testReplicationFilterWithoutPriority();
		testV2.testReplicationFilterWithoutPriority();
	}

	/**
	 * 삭제 마커 복제를 켠 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("DeleteMarker")
	void testReplicationDeleteMarkerEnabled() {
		test.testReplicationDeleteMarkerEnabled();
		testV2.testReplicationDeleteMarkerEnabled();
	}

	/**
	 * 태그 필터가 있는 규칙에서 삭제 마커 복제를 켜면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationDeleteMarkerWithTagFilter() {
		test.testReplicationDeleteMarkerWithTagFilter();
		testV2.testReplicationDeleteMarkerWithTagFilter();
	}

	/**
	 * 비활성(Disabled) 복제 규칙이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Check")
	void testReplicationDisabledRule() {
		test.testReplicationDisabledRule();
		testV2.testReplicationDisabledRule();
	}

	/**
	 * 원본 버킷 자신을 대상으로 지정하면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationSameBucket() {
		test.testReplicationSameBucket();
		testV2.testReplicationSameBucket();
	}

	/**
	 * 대상 버킷이 ARN 형식이 아니면 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationInvalidDestinationArn() {
		test.testReplicationInvalidDestinationArn();
		testV2.testReplicationInvalidDestinationArn();
	}

	/**
	 * 존재하지 않는 버킷의 복제 설정 조회/삭제가 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testReplicationBucketNotExist() {
		test.testReplicationBucketNotExist();
		testV2.testReplicationBucketNotExist();
	}

	/**
	 * 권한이 없는 사용자의 복제 설정 조회/저장/삭제가 실패하는지 확인
	 */
	@Test
	@Tag("Permission")
	void testReplicationAltUser() {
		test.testReplicationAltUser();
		testV2.testReplicationAltUser();
	}
}
