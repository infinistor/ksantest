package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷의 메트릭스 기능을 테스트하는 클래스
 */
class Metrics {
	org.example.test.Metrics test = new org.example.test.Metrics();
	org.example.testV2.Metrics testV2 = new org.example.testV2.Metrics();

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
	 * 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
	 */
	@Test
	@Tag("List")
	void testMetrics() {
		test.testMetrics();
		testV2.testMetrics();
	}

	/**
	 * 버킷에 Metrics를 설정할 수 있는지 확인
	 */
	@Test
	@Tag("Put")
	void testPutMetrics() {
		test.testPutMetrics();
		testV2.testPutMetrics();
	}

	/**
	 * 버킷에 Metrics 설정이 되었는지 확인
	 */
	@Test
	@Tag("Check")
	void testCheckMetrics() {
		test.testCheckMetrics();
		testV2.testCheckMetrics();
	}

	/**
	 * 버킷에 설정된 Metrics를 조회할 수 있는지 확인
	 */
	@Test
	@Tag("Get")
	void testGetMetrics() {
		test.testGetMetrics();
		testV2.testGetMetrics();
	}

	/**
	 * 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
	 */
	@Test
	@Tag("Delete")
	void testDeleteMetrics() {
		test.testDeleteMetrics();
		testV2.testDeleteMetrics();
	}

	/**
	 * 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testGetMetricsNotExist() {
		test.testGetMetricsNotExist();
		testV2.testGetMetricsNotExist();
	}

	/**
	 * 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testDeleteMetricsNotExist() {
		test.testDeleteMetricsNotExist();
		testV2.testDeleteMetricsNotExist();
	}

	/**
	 * 존재하지 않은 Metrics를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutMetricsNotExist() {
		test.testPutMetricsNotExist();
		testV2.testPutMetricsNotExist();
	}

	/**
	 * Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutMetricsEmptyId() {
		test.testPutMetricsEmptyId();
		testV2.testPutMetricsEmptyId();
	}

	/**
	 * Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutMetricsNoId() {
		test.testPutMetricsNoId();
		testV2.testPutMetricsNoId();
	}

	/**
	 * Metrics 아이디를 중복으로 설정하려고 할 경우 덮어쓰기 확인
	 */
	@Test
	@Tag("Overwrite")
	void testPutMetricsDuplicateId() {
		test.testPutMetricsDuplicateId();
		testV2.testPutMetricsDuplicateId();
	}

	/**
	 * 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testMetricsPrefix() {
		test.testMetricsPrefix();
		testV2.testMetricsPrefix();
	}

	/**
	 * Metrics 설정에 태그를 적용할 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testMetricsTag() {
		test.testMetricsTag();
		testV2.testMetricsTag();
	}

	/**
	 * Metrics 설정에 필터를 적용할 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testMetricsFilter() {
		test.testMetricsFilter();
		testV2.testMetricsFilter();
	}
}
