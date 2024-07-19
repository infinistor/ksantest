package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

public class Notification {

	org.example.test.Notification test = new org.example.test.Notification();
	org.example.testV2.Notification testV2 = new org.example.testV2.Notification();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 없는지 확인
	public void testNotificationGetEmpty() {
		test.testNotificationGetEmpty();
		testV2.testNotificationGetEmpty();
	}

	@Test
	@Tag("Put")
	// 버킷에 알람 설정이 가능한지 확인
	public void testNotificationPut() {
		test.testNotificationPut();
		testV2.testNotificationPut();
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 되어있는지 확인
	public void testNotificationGet() {
		test.testNotificationGet();
		testV2.testNotificationGet();
	}

	@Test
	@Tag("Delete")
	// 버킷에 알람 설정이 삭제되는지 확인
	public void testNotificationDelete() {
		test.testNotificationDelete();
		testV2.testNotificationDelete();
	}
}
