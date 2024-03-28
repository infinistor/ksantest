package org.example.s3tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

public class Accelerate {
	org.example.test.Accelerate Test = new org.example.test.Accelerate();

	@org.junit.jupiter.api.AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("Put")
	// 버킷 가속 설정이 가능한지 확인
	public void test_put_bucket_accelerate() {
		Test.test_put_bucket_accelerate();
	}

	@Test
	@Tag("Get")
	// 버킷 가속 설정이 올바르게 적용되는지 확인
	public void test_get_bucket_accelerate() {
		Test.test_get_bucket_accelerate();
	}

	@Test
	@Tag("Change")
	// 버킷 가속 설정이 변경되는지 확인
	public void test_change_bucket_accelerate() {
		Test.test_change_bucket_accelerate();
	}

	
	@Test
	@Tag("Error")
	// 버킷 가속 설정을 잘못 입력했을 때 에러가 발생하는지 확인
	public void test_put_bucket_accelerate_invalid() {
		Test.test_put_bucket_accelerate_invalid();
	}
}
