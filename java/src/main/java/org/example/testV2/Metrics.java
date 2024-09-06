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
package org.example.testV2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class Metrics extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Metrics V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Metrics V2 End");
	}

	@Test
	@Tag("List")
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(0, response.metricsConfigurationList().size());
	}

	@Test
	@Tag("Put")
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id")));
	}

	@Test
	@Tag("Check")
	public void testCheckMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id")));
		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.metricsConfigurationList().size());
	}

	@Test
	@Tag("Get")
	public void testGetMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id(metricId).metricsConfiguration(m -> m.id(metricId)));
		var response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals(metricId, response.metricsConfiguration().id());
	}

	@Test
	@Tag("Delete")
	public void testDeleteMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id(metricId).metricsConfiguration(m -> m.id(metricId)));
		client.deleteBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
	}

	@Test
	@Tag("Error")
	public void testGetMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id("metrics-id")));
		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteBucketMetricsConfiguration(l -> l.bucket(bucketName).id("metrics-id")));
		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketMetricsConfiguration(
						l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id"))));
		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsEmptyId() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketMetricsConfiguration(
						l -> l.bucket(bucketName).id("").metricsConfiguration(m -> m.id(""))));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_CONFIGURATION_ID, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client);

		assertThrows(software.amazon.awssdk.core.exception.SdkClientException.class,
				() -> client.putBucketMetricsConfiguration(l -> l.bucket(bucketName)));
	}

	@Test
	@Tag("Overwrite")
	public void testPutMetricsDuplicateId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";

		client.putBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.prefix("test1"))));
		var response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals("test1", response.metricsConfiguration().filter().prefix());

		client.putBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.prefix("test2"))));
		response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals("test2", response.metricsConfiguration().filter().prefix());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsPrefix() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";
		var prefix = "test";

		client.putBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.prefix(prefix))));
		var response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals(prefix, response.metricsConfiguration().filter().prefix());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsTag() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";
		var tag = software.amazon.awssdk.services.s3.model.Tag.builder().key("key").value("value").build();

		client.putBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.tag(tag))));
		var response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals(tag.key(), response.metricsConfiguration().filter().tag().key());
		assertEquals(tag.value(), response.metricsConfiguration().filter().tag().value());
	}

	@Test
	@Tag("Filtering")
	public void testMetricsFilter() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metricId = "metrics-id";
		var prefix = "test";
		var tag = software.amazon.awssdk.services.s3.model.Tag.builder().key("key").value("value").build();

		client.putBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.and(a -> a.prefix(prefix).tags(tag)))));
		var response = client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
		assertEquals(prefix, response.metricsConfiguration().filter().and().prefix());
		assertEquals(tag.key(), response.metricsConfiguration().filter().and().tags().get(0).key());
		assertEquals(tag.value(), response.metricsConfiguration().filter().and().tags().get(0).value());
	}
}
