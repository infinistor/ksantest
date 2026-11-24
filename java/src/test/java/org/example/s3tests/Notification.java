package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷의 알람 기능을 테스트하는 클래스
 */
class Notification {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Notification Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Notification End");
	}


	org.example.test.Notification test = new org.example.test.Notification();
	org.example.testV2.Notification testV2 = new org.example.testV2.Notification();

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
	 * 버킷에 알람 설정이 없는지 확인
	 */
	@Test
	@Tag("Get")
	void testNotificationGetEmpty() {
		test.testNotificationGetEmpty();
		testV2.testNotificationGetEmpty();
	}

	/**
	 * 버킷에 알람 설정이 가능한지 확인
	 */
	@Test
	@Tag("Put")
	void testNotificationPut() {
		test.testNotificationPut();
		testV2.testNotificationPut();
	}

	/**
	 * 버킷에 알람 설정이 되어있는지 확인
	 */
	@Test
	@Tag("Get")
	void testNotificationGet() {
		test.testNotificationGet();
		testV2.testNotificationGet();
	}

	/**
	 * 버킷에 알람 설정이 삭제되는지 확인
	 */
	@Test
	@Tag("Delete")
	void testNotificationDelete() {
		test.testNotificationDelete();
		testV2.testNotificationDelete();
	}

	/**
	 * 형식이 잘못된 Lambda ARN으로 알람 설정 시 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testNotificationPutInvalidArn() {
		test.testNotificationPutInvalidArn();
		testV2.testNotificationPutInvalidArn();
	}

	/**
	 * 버킷에 EventBridge 알람 설정, 조회, 삭제가 가능한지 확인
	 */
	@Test
	@Tag("EventBridge")
	void testNotificationEventBridge() {
		test.testNotificationEventBridge();
		testV2.testNotificationEventBridge();
	}

	/**
	 * 버킷에 SQS 큐 알람 설정, 조회, 삭제가 가능한지 확인
	 */
	@Test
	@Tag("Queue")
	void testNotificationQueue() {
		test.testNotificationQueue();
		testV2.testNotificationQueue();
	}

	/**
	 * 버킷에 SNS 토픽 알람 설정, 조회, 삭제가 가능한지 확인
	 */
	@Test
	@Tag("Topic")
	void testNotificationTopic() {
		test.testNotificationTopic();
		testV2.testNotificationTopic();
	}

	/**
	 * 접두어/접미어 필터가 있는 알람 설정이 저장되고 조회되는지 확인
	 */
	@Test
	@Tag("Filter")
	void testNotificationFilter() {
		test.testNotificationFilter();
		testV2.testNotificationFilter();
	}

	/**
	 * 잘못된 이벤트 이름으로 알람 설정 시 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testNotificationInvalidEvent() {
		test.testNotificationInvalidEvent();
		testV2.testNotificationInvalidEvent();
	}

	/**
	 * 잘못된 필터 이름으로 알람 설정 시 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testNotificationInvalidFilterName() {
		test.testNotificationInvalidFilterName();
		testV2.testNotificationInvalidFilterName();
	}

	/**
	 * 같은 이벤트를 공유하는 설정은 필터가 겹치면 실패하고 겹치지 않으면 성공하는지 확인
	 */
	@Test
	@Tag("Error")
	void testNotificationOverlappingEvent() {
		test.testNotificationOverlappingEvent();
		testV2.testNotificationOverlappingEvent();
	}

	/**
	 * 같은 Id 를 가진 알람 설정이 실패하는지 확인 (SDK v1 은 설정을 Id 키의 Map 으로 관리해 중복 Id 를 표현할 수 없어 V2 만 실행)
	 */
	@Test
	@Tag("Error")
	void testNotificationDuplicateId() {
		testV2.testNotificationDuplicateId();
	}

	/**
	 * 존재하지 않는 버킷의 알람 설정 조회/저장이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testNotificationBucketNotExist() {
		test.testNotificationBucketNotExist();
		testV2.testNotificationBucketNotExist();
	}

	/**
	 * 권한이 없는 사용자의 알람 설정 조회/저장이 실패하는지 확인
	 */
	@Test
	@Tag("Permission")
	void testNotificationAltUser() {
		test.testNotificationAltUser();
		testV2.testNotificationAltUser();
	}
}
