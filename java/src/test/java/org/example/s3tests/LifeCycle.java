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
 * 버킷의 수명 주기(Lifecycle) 관리 기능을 테스트하는 클래스
 */
class LifeCycle {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("LifeCycle Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("LifeCycle End");
	}

	org.example.test.LifeCycle test = new org.example.test.LifeCycle();
	org.example.testV2.LifeCycle testV2 = new org.example.testV2.LifeCycle();

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
	 * 버킷의 Lifecycle 규칙을 추가 가능한지 확인
	 */
	@Test
	@Tag("Check")
	void testLifecycleSet() {
		test.testLifecycleSet();
		testV2.testLifecycleSet();
	}

	/**
	 * 버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Get")
	void testLifecycleGet() {
		test.testLifecycleGet();
		testV2.testLifecycleGet();
	}

	/**
	 * ID 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인
	 */
	@Test
	@Tag("Check")
	void testLifecycleGetNoId() {
		test.testLifecycleGetNoId();
		testV2.testLifecycleGetNoId();
	}

	/**
	 * 버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인
	 */
	@Test
	@Tag("Version")
	void testLifecycleExpirationVersioningEnabled() {
		test.testLifecycleExpirationVersioningEnabled();
		testV2.testLifecycleExpirationVersioningEnabled();
	}

	/**
	 * 버킷에 Lifecycle 규칙을 설정할때 ID의 길이가 너무 길면 실패하는지 확인
	 */
	@Test
	@Tag("Check")
	void testLifecycleIdTooLong() {
		test.testLifecycleIdTooLong();
		testV2.testLifecycleIdTooLong();
	}

	/**
	 * 버킷에 Lifecycle 규칙을 설정할때 같은 ID로 규칙을 여러개 설정할경우 실패하는지 확인
	 */
	@Test
	@Tag("Duplicate")
	void testLifecycleSameId() {
		test.testLifecycleSameId();
		testV2.testLifecycleSameId();
	}

	/**
	 * 버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleInvalidStatus() {
		test.testLifecycleInvalidStatus();
		testV2.testLifecycleInvalidStatus();
	}

	/**
	 * 버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인
	 */
	@Test
	@Tag("Date")
	void testLifecycleSetDate() {
		test.testLifecycleSetDate();
		testV2.testLifecycleSetDate();
	}

	/**
	 * 버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleSetInvalidDate() {
		test.testLifecycleSetInvalidDate();
		testV2.testLifecycleSetInvalidDate();
	}

	/**
	 * 버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인
	 */
	@Test
	@Tag("Version")
	void testLifecycleSetNoncurrent() {
		test.testLifecycleSetNoncurrent();
		testV2.testLifecycleSetNoncurrent();
	}

	/**
	 * 버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Version")
	void testLifecycleNoncurrentExpiration() {
		test.testLifecycleNoncurrentExpiration();
		testV2.testLifecycleNoncurrentExpiration();
	}

	/**
	 * DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
	 */
	@Test
	@Tag("DeleteMarker")
	void testLifecycleSetDeleteMarker() {
		test.testLifecycleSetDeleteMarker();
		testV2.testLifecycleSetDeleteMarker();
	}

	/**
	 * Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleSetFilter() {
		test.testLifecycleSetFilter();
		testV2.testLifecycleSetFilter();
	}

	/**
	 * Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleSetEmptyFilter() {
		test.testLifecycleSetEmptyFilter();
		testV2.testLifecycleSetEmptyFilter();
	}

	/**
	 * DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("DeleteMarker")
	void testLifecycleDeleteMarkerExpiration() {
		test.testLifecycleDeleteMarkerExpiration();
		testV2.testLifecycleDeleteMarkerExpiration();
	}

	/**
	 * AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
	 */
	@Test
	@Tag("Multipart")
	void testLifecycleSetMultipart() {
		test.testLifecycleSetMultipart();
		testV2.testLifecycleSetMultipart();
	}

	/**
	 * AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Multipart")
	void testLifecycleMultipartExpiration() {
		test.testLifecycleMultipartExpiration();
		testV2.testLifecycleMultipartExpiration();
	}

	/**
	 * 버킷의 Lifecycle 규칙을 삭제 가능한지 확인
	 */
	@Test
	@Tag("Delete")
	void testLifecycleDelete() {
		test.testLifecycleDelete();
		testV2.testLifecycleDelete();
	}

	/**
	 * Lifecycle 규칙에 0일을 설정할때 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleSetExpirationZero() {
		test.testLifecycleSetExpirationZero();
		testV2.testLifecycleSetExpirationZero();
	}

	/**
	 * Lifecycle 규칙을 적용할 경우 오브젝트의 만료기한이 설정되는지 확인
	 */
	@Test
	@Tag("metadata")
	void testLifecycleSetExpiration() {
		test.testLifecycleSetExpiration();
		// testV2.testLifecycleSetExpiration();
	}

	/**
	 * Lifecycle 설정이 없는 버킷을 조회하면 NoSuchLifecycleConfiguration 인지 확인
	 */
	@Test
	@Tag("Get")
	void testLifecycleGetNotExist() {
		test.testLifecycleGetNotExist();
		testV2.testLifecycleGetNotExist();
	}

	/**
	 * Lifecycle 설정이 없어도 삭제가 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testLifecycleDeleteNotExist() {
		test.testLifecycleDeleteNotExist();
		testV2.testLifecycleDeleteNotExist();
	}

	/**
	 * 존재하지 않는 버킷의 Lifecycle 조회·설정·삭제가 NoSuchBucket 으로 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleBucketNotExist() {
		test.testLifecycleBucketNotExist();
		testV2.testLifecycleBucketNotExist();
	}

	/**
	 * 다른 사용자의 Lifecycle 조회·설정·삭제가 AccessDenied 로 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleAccessDenied() {
		test.testLifecycleAccessDenied();
		testV2.testLifecycleAccessDenied();
	}

	/**
	 * 동작이 하나도 없는 규칙이 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleNoAction() {
		test.testLifecycleNoAction();
		testV2.testLifecycleNoAction();
	}

	/**
	 * Filter 도 Prefix 도 없는 규칙이 거부되는지 확인 (SDK v1 은 Filter 가 없으면 빈 Prefix 를 넣어 보내므로 V2만)
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleNoFilter() {
		testV2.testLifecycleNoFilter();
	}

	/**
	 * 규칙이 없는 Lifecycle 설정이 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleEmptyRules() {
		test.testLifecycleEmptyRules();
		testV2.testLifecycleEmptyRules();
	}

	/**
	 * 만료에 Days 와 Date 를 함께 주면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleDaysAndDate() {
		test.testLifecycleDaysAndDate();
		testV2.testLifecycleDaysAndDate();
	}

	/**
	 * 만료일이 음수이면 거부되는지 확인 (SDK v1 은 -1 을 미설정으로 다루므로 V2만)
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleNegativeDays() {
		testV2.testLifecycleNegativeDays();
	}

	/**
	 * 스토리지 클래스 전환 규칙이 그대로 저장·조회되는지 확인
	 */
	@Test
	@Tag("Transition")
	void testLifecycleTransition() {
		test.testLifecycleTransition();
		testV2.testLifecycleTransition();
	}

	/**
	 * 전환할 수 없는 스토리지 클래스가 거부되는지 확인
	 */
	@Test
	@Tag("Transition")
	void testLifecycleTransitionInvalidStorageClass() {
		test.testLifecycleTransitionInvalidStorageClass();
		testV2.testLifecycleTransitionInvalidStorageClass();
	}

	/**
	 * 만료일이 전환일보다 크지 않으면 거부되는지 확인
	 */
	@Test
	@Tag("Transition")
	void testLifecycleExpirationBeforeTransition() {
		test.testLifecycleExpirationBeforeTransition();
		testV2.testLifecycleExpirationBeforeTransition();
	}

	/**
	 * 이전 버전 전환 규칙이 그대로 저장·조회되는지 확인
	 */
	@Test
	@Tag("Transition")
	void testLifecycleNoncurrentTransition() {
		test.testLifecycleNoncurrentTransition();
		testV2.testLifecycleNoncurrentTransition();
	}

	/**
	 * 이전 버전 만료일이 0이면 거부되는지 확인
	 */
	@Test
	@Tag("Version")
	void testLifecycleNoncurrentZeroDays() {
		test.testLifecycleNoncurrentZeroDays();
		testV2.testLifecycleNoncurrentZeroDays();
	}

	/**
	 * 보존할 이전 버전 수(NewerNoncurrentVersions)가 저장되고 1~100 밖의 값은 거부되는지 확인
	 */
	@Test
	@Tag("Version")
	void testLifecycleNewerNoncurrentVersions() {
		test.testLifecycleNewerNoncurrentVersions();
		testV2.testLifecycleNewerNoncurrentVersions();
	}

	/**
	 * ExpiredObjectDeleteMarker 를 Days 와 함께 주면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleDeleteMarkerWithDays() {
		test.testLifecycleDeleteMarkerWithDays();
		testV2.testLifecycleDeleteMarkerWithDays();
	}

	/**
	 * ExpiredObjectDeleteMarker 를 태그 필터와 함께 주면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleDeleteMarkerWithTag() {
		test.testLifecycleDeleteMarkerWithTag();
		testV2.testLifecycleDeleteMarkerWithTag();
	}

	/**
	 * AbortIncompleteMultipartUpload 를 태그·크기 필터와 함께 주면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleAbortMultipartWithFilter() {
		test.testLifecycleAbortMultipartWithFilter();
		testV2.testLifecycleAbortMultipartWithFilter();
	}

	/**
	 * AbortIncompleteMultipartUpload 의 일수가 0이면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testLifecycleAbortMultipartZeroDays() {
		test.testLifecycleAbortMultipartZeroDays();
		testV2.testLifecycleAbortMultipartZeroDays();
	}

	/**
	 * 태그 필터가 그대로 저장·조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleTagFilter() {
		test.testLifecycleTagFilter();
		testV2.testLifecycleTagFilter();
	}

	/**
	 * And 필터(접두어·태그·크기)가 그대로 저장·조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleAndFilter() {
		test.testLifecycleAndFilter();
		testV2.testLifecycleAndFilter();
	}

	/**
	 * 조건이 하나뿐이거나 태그 키가 중복된 And 필터가 거부되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleAndFilterInvalid() {
		test.testLifecycleAndFilterInvalid();
		testV2.testLifecycleAndFilterInvalid();
	}

	/**
	 * 오브젝트 크기 필터가 그대로 저장·조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleObjectSizeFilter() {
		test.testLifecycleObjectSizeFilter();
		testV2.testLifecycleObjectSizeFilter();
	}

	/**
	 * 올바르지 않은 오브젝트 크기 필터가 거부되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleObjectSizeFilterInvalid() {
		test.testLifecycleObjectSizeFilterInvalid();
		testV2.testLifecycleObjectSizeFilterInvalid();
	}

	/**
	 * Filter 대신 Prefix 를 쓰는 예전 형식이 저장·조회되고 Prefix 와 Filter 를 함께 주면 거부되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testLifecycleLegacyPrefix() {
		test.testLifecycleLegacyPrefix();
		testV2.testLifecycleLegacyPrefix();
	}

	/**
	 * 규칙을 1000개까지 넣을 수 있고 1001개는 거부되는지 확인
	 */
	@Test
	@Tag("Max")
	void testLifecycleMaxRules() {
		test.testLifecycleMaxRules();
		testV2.testLifecycleMaxRules();
	}

	/**
	 * 만료 규칙에 걸리는 오브젝트에만 x-amz-expiration 헤더가 오는지 확인
	 */
	@Test
	@Tag("metadata")
	void testLifecycleExpirationHeader() throws java.io.IOException {
		test.testLifecycleExpirationHeader();
		testV2.testLifecycleExpirationHeader();
	}
}
