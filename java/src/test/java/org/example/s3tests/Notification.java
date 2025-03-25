package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷의 알람 기능을 테스트하는 클래스
 */
class Notification {

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
}
