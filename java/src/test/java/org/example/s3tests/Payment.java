package org.example.s3tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;

class Payment {
	
	org.example.test.Payment Test = new org.example.test.Payment();
	
	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	
	@Test
	@Tag("Put")
	// 버킷 과금 설정이 가능한지 확인
	void testPutBucketRequestPayment() {
		Test.testPutBucketRequestPayment();
	}

	
	@Test
	@Tag("Get")
	// 버킷 과금 설정 조회 확인
	void testGetBucketRequestPayment() {
		Test.testGetBucketRequestPayment();
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정이 올바르게 적용되는지 확인
	void testSetGetBucketRequestPayment() {
		Test.testSetGetBucketRequestPayment();
	}
}
