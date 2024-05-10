package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

public class Notification {

	org.example.test.Notification Test = new org.example.test.Notification();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 없는지 확인
	public void testNotificationGetEmpty() {
		Test.testNotificationGetEmpty();
	}

	@Test
	@Tag("Put")
	// 버킷에 알람 설정이 가능한지 확인
	public void testNotificationPut() {
		Test.testNotificationPut();
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 되어있는지 확인
	public void testNotificationGet() {
		Test.testNotificationGet();
	}

	@Test
	@Tag("Delete")
	// 버킷에 알람 설정이 삭제되는지 확인
	public void testNotificationDelete() {
		Test.testNotificationDelete();
	}
}
