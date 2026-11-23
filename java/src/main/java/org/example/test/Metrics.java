/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.metrics.MetricsAndOperator;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsFilter;
import com.amazonaws.services.s3.model.metrics.MetricsFilterPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsPrefixPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsTagPredicate;

public class Metrics extends TestBase {
	@Test
	@Tag("List")
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getMetricsConfigurationList());
	}

	@Test
	@Tag("Put")
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var metrics = new MetricsConfiguration().withId("metrics-id");

		client.setBucketMetricsConfiguration(bucketName, metrics);
	}

	@Test
	@Tag("Check")
	public void testCheckMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 3);
		var metrics = new MetricsConfiguration().withId("metrics-id");

		client.setBucketMetricsConfiguration(bucketName, metrics);
		var response = client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName));
		assertEquals(1, response.getMetricsConfigurationList().size());
	}

	@Test
	@Tag("Get")
	public void testGetMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var metricId = "metrics-id";
		var metrics = new MetricsConfiguration().withId(metricId);

		client.setBucketMetricsConfiguration(bucketName, metrics);
		var response = client.getBucketMetricsConfiguration(bucketName, metricId);
		assertEquals(metricId, response.getMetricsConfiguration().getId());
	}

	@Test
	@Tag("Delete")
	public void testDeleteMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		var metricId = "metrics-id";
		var metrics = new MetricsConfiguration().withId(metricId);

		client.setBucketMetricsConfiguration(bucketName, metrics);
		client.deleteBucketMetricsConfiguration(bucketName, metricId);
	}

	@Test
	@Tag("Error")
	public void testGetMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketMetricsConfiguration(bucketName, "metrics-id"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketMetricsConfiguration(bucketName, "metrics-id"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(8);
		var metrics = new MetricsConfiguration().withId("metrics-id");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketMetricsConfiguration(bucketName, metrics));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsEmptyId() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		var metrics = new MetricsConfiguration().withId("");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketMetricsConfiguration(bucketName, metrics));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_CONFIGURATION_ID, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		var metrics = new MetricsConfiguration();

		assertThrows(IllegalArgumentException.class, () -> client.setBucketMetricsConfiguration(bucketName, metrics));
	}

	@Test
	@Tag("Overwrite")
	public void testPutMetricsDuplicateId() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var metricId = "metrics-id";
		var metrics1 = new MetricsConfiguration().withId(metricId)
				.withFilter(new MetricsFilter(new MetricsPrefixPredicate("test1")));
		var metrics2 = new MetricsConfiguration().withId(metricId)
				.withFilter(new MetricsFilter(new MetricsPrefixPredicate("test2")));

		client.setBucketMetricsConfiguration(bucketName, metrics1);
		var response = client.getBucketMetricsConfiguration(bucketName, metricId);
		assertEquals("test1",
				((MetricsPrefixPredicate) response.getMetricsConfiguration().getFilter().getPredicate()).getPrefix());

		client.setBucketMetricsConfiguration(bucketName, metrics2);
		response = client.getBucketMetricsConfiguration(bucketName, metricId);
		assertEquals("test2",
				((MetricsPrefixPredicate) response.getMetricsConfiguration().getFilter().getPredicate()).getPrefix());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var prefix = "test";
		var metrics = new MetricsConfiguration().withId("metrics-id")
				.withFilter(new MetricsFilter(new MetricsPrefixPredicate(prefix)));

		client.setBucketMetricsConfiguration(bucketName, metrics);
		var response = client.getBucketMetricsConfiguration(bucketName, "metrics-id");
		assertEquals(prefix,
				((MetricsPrefixPredicate) response.getMetricsConfiguration().getFilter().getPredicate()).getPrefix());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsTag() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		var tag = new com.amazonaws.services.s3.model.Tag("key", "value");
		var metrics = new MetricsConfiguration().withId("metrics-id")
				.withFilter(new MetricsFilter(new MetricsTagPredicate(tag)));

		client.setBucketMetricsConfiguration(bucketName, metrics);
		var response = client.getBucketMetricsConfiguration(bucketName, "metrics-id");
		assertEquals(tag.getKey(), ((MetricsTagPredicate) response.getMetricsConfiguration().getFilter().getPredicate())
				.getTag().getKey());
		assertEquals(tag.getValue(),
				((MetricsTagPredicate) response.getMetricsConfiguration().getFilter().getPredicate()).getTag()
						.getValue());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var prefix = "test";
		var filters = new ArrayList<MetricsFilterPredicate>();
		var tag = new com.amazonaws.services.s3.model.Tag("key", "value");
		filters.add(new MetricsPrefixPredicate(prefix));
		filters.add(new MetricsTagPredicate(tag));
		var metrics = new MetricsConfiguration().withId("metrics-id")
				.withFilter(new MetricsFilter(new MetricsAndOperator(filters)));

		client.setBucketMetricsConfiguration(bucketName, metrics);
		var response = client.getBucketMetricsConfiguration(bucketName, "metrics-id");
		var and = (MetricsAndOperator) response.getMetricsConfiguration().getFilter().getPredicate();

		// AWS는 And 연산자 내 조건 순서를 보장하지 않으므로 타입으로 구분하여 검증
		String actualPrefix = null;
		com.amazonaws.services.s3.model.Tag actualTag = null;
		for (var operand : and.getOperands()) {
			if (operand instanceof MetricsPrefixPredicate metricsPrefixPredicate)
				actualPrefix = metricsPrefixPredicate.getPrefix();
			else if (operand instanceof MetricsTagPredicate metricsTagPredicate)
				actualTag = metricsTagPredicate.getTag();
		}

		assertEquals(prefix, actualPrefix);
		assertNotNull(actualTag);
		assertEquals(tag.getKey(), actualTag.getKey());
		assertEquals(tag.getValue(), actualTag.getValue());
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AmazonServiceException.class, executable);
		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	@Test
	@Tag("Max")
	public void testPutMetricsIdLength() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		// ID 는 64자까지 허용한다
		var metricId = "a".repeat(64);
		client.setBucketMetricsConfiguration(bucketName, new MetricsConfiguration().withId(metricId));
		var response = client.getBucketMetricsConfiguration(bucketName, metricId);
		assertEquals(metricId, response.getMetricsConfiguration().getId());

		assertError(() -> client.setBucketMetricsConfiguration(bucketName,
				new MetricsConfiguration().withId("a".repeat(65))), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Error")
	public void testPutMetricsInvalidId() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		// ID 에는 영숫자와 - _ . 만 쓸 수 있다
		assertError(() -> client.setBucketMetricsConfiguration(bucketName, new MetricsConfiguration().withId("a b/c")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		assertNull(client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName))
				.getMetricsConfigurationList());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsInvalidFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 18);

		// And 안에 조건이 하나뿐이면 거부된다
		// (And 없이 Prefix 와 Tag 를 함께 주는 요청은 SDK v1 모델로 만들 수 없어 확인하지 않는다)
		var filters = new ArrayList<MetricsFilterPredicate>();
		filters.add(new MetricsPrefixPredicate("test"));
		var metrics = new MetricsConfiguration().withId("metrics-id")
				.withFilter(new MetricsFilter(new MetricsAndOperator(filters)));
		assertError(() -> client.setBucketMetricsConfiguration(bucketName, metrics), HttpStatus.SC_BAD_REQUEST,
				MainData.MALFORMED_XML);
	}

	@Test
	@Tag("List")
	public void testListMetricsPagination() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var ids = new ArrayList<String>();
		for (int i = 0; i < 101; i++)
			ids.add(String.format("metrics-%03d", i));
		for (var metricId : ids)
			client.setBucketMetricsConfiguration(bucketName, new MetricsConfiguration().withId(metricId));

		// 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
		var response = client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName));
		assertEquals(100, response.getMetricsConfigurationList().size());
		assertTrue(response.isTruncated());
		var listed = new ArrayList<String>();
		response.getMetricsConfigurationList().forEach(c -> listed.add(c.getId()));

		response = client.listBucketMetricsConfigurations(new ListBucketMetricsConfigurationsRequest()
				.withBucketName(bucketName).withContinuationToken(response.getNextContinuationToken()));
		assertEquals(1, response.getMetricsConfigurationList().size());
		assertFalse(response.isTruncated());
		response.getMetricsConfigurationList().forEach(c -> listed.add(c.getId()));
		Collections.sort(listed);
		assertEquals(ids, listed);
	}

	@Test
	@Tag("List")
	public void testListMetricsInvalidToken() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		assertError(() -> client.listBucketMetricsConfigurations(new ListBucketMetricsConfigurationsRequest()
				.withBucketName(bucketName).withContinuationToken("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_CONTINUATION_TOKEN);
	}

	@Test
	@Tag("Error")
	public void testMetricsBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(21);

		assertError(() -> client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName)),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.getBucketMetricsConfiguration(bucketName, "metrics-id"), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketMetricsConfiguration(bucketName, "metrics-id"), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
	}
}
