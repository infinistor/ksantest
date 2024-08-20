package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class Metrics {
	org.example.test.Metrics test = new org.example.test.Metrics();
	org.example.testV2.Metrics testV2 = new org.example.testV2.Metrics();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("List")
	// 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
	void testMetrics() {
		test.testMetrics();
		testV2.testMetrics();
	}

	@Test
	@Tag("Put")
	// 버킷에 Metrics를 설정할 수 있는지 확인
	void testPutMetrics() {
		test.testPutMetrics();
		testV2.testPutMetrics();
	}

	@Test
	@Tag("Check")
	// 버킷에 Metrics 설정이 되었는지 확인
	void testCheckMetrics() {
		test.testCheckMetrics();
		testV2.testCheckMetrics();
	}

	@Test
	@Tag("Get")
	// 버킷에 설정된 Metrics를 조회할 수 있는지 확인
	void testGetMetrics() {
		test.testGetMetrics();
		testV2.testGetMetrics();
	}

	@Test
	@Tag("Delete")
	// 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
	void testDeleteMetrics() {
		test.testDeleteMetrics();
		testV2.testDeleteMetrics();
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
	void testGetMetricsNotExist() {
		test.testGetMetricsNotExist();
		testV2.testGetMetricsNotExist();
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
	void testDeleteMetricsNotExist() {
		test.testDeleteMetricsNotExist();
		testV2.testDeleteMetricsNotExist();
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 Metrics를 설정하려고 할 경우 실패하는지 확인
	void testPutMetricsNotExist() {
		test.testPutMetricsNotExist();
		testV2.testPutMetricsNotExist();
	}

	@Test
	@Tag("Error")
	// Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
	void testPutMetricsEmptyId() {
		test.testPutMetricsEmptyId();
		testV2.testPutMetricsEmptyId();
	}

	@Test
	@Tag("Error")
	// Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
	void testPutMetricsNoId() {
		test.testPutMetricsNoId();
		testV2.testPutMetricsNoId();
	}

	@Test
	@Tag("Error")
	// Metrics 아이디를 중복으로 설정하려고 할 경우 실패하는지 확인
	void testPutMetricsDuplicateId() {
		test.testPutMetricsDuplicateId();
		testV2.testPutMetricsDuplicateId();
	}

	@Test
	@Tag("Filter")
	// 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
	void testMetricsPrefix() {
		test.testMetricsPrefix();
		testV2.testMetricsPrefix();
	}

	@Test
	@Tag("Filter")
	// Metrics 설정에 태그를 적용할 수 있는지 확인
	void testMetricsTag() {
		test.testMetricsTag();
		testV2.testMetricsTag();
	}

	@Test
	@Tag("Filter")
	// Metrics 설정에 필터를 적용할 수 있는지 확인
	void testMetricsFilter() {
		test.testMetricsFilter();
		testV2.testMetricsFilter();
	}
}
