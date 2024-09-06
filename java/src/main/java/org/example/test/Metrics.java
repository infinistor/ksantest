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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.metrics.MetricsAndOperator;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsFilter;
import com.amazonaws.services.s3.model.metrics.MetricsFilterPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsPrefixPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsTagPredicate;

public class Metrics extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Metrics Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Metrics End");
	}

	@Test
	@Tag("List")
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getMetricsConfigurationList());
	}

	@Test
	@Tag("Put")
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metrics = new MetricsConfiguration().withId("metrics-id");

		client.setBucketMetricsConfiguration(bucketName, metrics);
	}

	@Test
	@Tag("Check")
	public void testCheckMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
		var metricId = "metrics-id";
		var metrics = new MetricsConfiguration().withId(metricId);

		client.setBucketMetricsConfiguration(bucketName, metrics);
		client.deleteBucketMetricsConfiguration(bucketName, metricId);
	}

	@Test
	@Tag("Error")
	public void testGetMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketMetricsConfiguration(bucketName, "metrics-id"));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketMetricsConfiguration(bucketName, "metrics-id"));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly();
		var metrics = new MetricsConfiguration().withId("metrics-id");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketMetricsConfiguration(bucketName, metrics));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsEmptyId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metrics = new MetricsConfiguration().withId("");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketMetricsConfiguration(bucketName, metrics));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_CONFIGURATION_ID, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metrics = new MetricsConfiguration();

		assertThrows(IllegalArgumentException.class, () -> client.setBucketMetricsConfiguration(bucketName, metrics));
	}

	@Test
	@Tag("Overwrite")
	public void testPutMetricsDuplicateId() {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		assertEquals(prefix, ((MetricsPrefixPredicate) and.getOperands().get(0)).getPrefix());
		assertEquals(tag.getKey(), ((MetricsTagPredicate) and.getOperands().get(1)).getTag().getKey());
		assertEquals(tag.getValue(), ((MetricsTagPredicate) and.getOperands().get(1)).getTag().getValue());
	}
}
