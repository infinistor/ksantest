package org.example.s3tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

public class Payment {
	
	org.example.test.Payment Test = new org.example.test.Payment();

	@org.junit.jupiter.api.AfterEach
	public void Clear() {
		Test.Clear();
	}

	
	@Test
	@Tag("Put")
	// 버킷 과금 설정이 가능한지 확인
	public void test_put_bucket_request_payment() {
		Test.test_put_bucket_request_payment();
	}

	
	@Test
	@Tag("Get")
	// 버킷 과금 설정 조회 확인
	public void test_get_bucket_request_payment() {
		Test.test_get_bucket_request_payment();
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정이 올바르게 적용되는지 확인
	public void test_set_get_bucket_request_payment() {
		Test.test_set_get_bucket_request_payment();
	}
}
