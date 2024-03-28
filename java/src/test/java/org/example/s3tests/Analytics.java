package org.example.s3tests;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Analytics {
	org.example.test.Analytics Test = new org.example.test.Analytics();

	@org.junit.jupiter.api.AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 가능한지 확인
	public void test_put_bucket_analytics() {
		Test.test_put_bucket_analytics();
	}

	@Test
	@Tag("Get")
	// 버킷 분석 설정이 올바르게 적용되는지 확인
	public void test_get_bucket_analytics() {
		Test.test_get_bucket_analytics();
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 여러개 가능한지 확인
	public void test_add_bucket_analytics() {
		Test.test_add_bucket_analytics();
	}
	
	@Test
	@Tag("List")
	// 버킷 분석 설정이 목록으로 조회되는지 확인
	public void test_list_bucket_analytics() {
		Test.test_list_bucket_analytics();
	}

	@Test
	@Tag("Delete")
	// 버킷 분석 설정이 삭제되는지 확인
	public void test_delete_bucket_analytics() {
		Test.test_delete_bucket_analytics();
	}

	@Test
	@Tag("Error")
	// 버킷 분석 설정을 잘못 입력했을 때 에러가 발생하는지 확인
	public void test_put_bucket_analytics_invalid() {
		Test.test_put_bucket_analytics_invalid();
	}
}
