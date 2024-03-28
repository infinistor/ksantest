package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

public class Notification {

	org.example.test.Notification Test = new org.example.test.Notification();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 없는지 확인
	public void test_notification_get_empty() {
		Test.test_notification_get_empty();
	}

	@Test
	@Tag("Put")
	// 버킷에 알람 설정이 가능한지 확인
	public void test_notification_put() {
		Test.test_notification_put();
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 되어있는지 확인
	public void test_notification_get() {
		Test.test_notification_get();
	}

	@Test
	@Tag("Delete")
	// 버킷에 알람 설정이 삭제되는지 확인
	public void test_notification_delete() {
		Test.test_notification_delete();
	}
}
