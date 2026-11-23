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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;

public class Metrics extends TestBase {
	@Test
	@Tag("List")
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(0, response.metricsConfigurationList().size());
	}

	@Test
	@Tag("Put")
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 2);

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id")));
	}

	@Test
	@Tag("Check")
	public void testCheckMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id")));
		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.metricsConfigurationList().size());
	}

	@Test
	@Tag("Get")
	public void testGetMetrics() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
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
		var bucketName = createBucket(client, 5);
		var metricId = "metrics-id";

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id(metricId).metricsConfiguration(m -> m.id(metricId)));
		client.deleteBucketMetricsConfiguration(l -> l.bucket(bucketName).id(metricId));
	}

	@Test
	@Tag("Error")
	public void testGetMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketMetricsConfiguration(l -> l.bucket(bucketName).id("metrics-id")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteMetricsNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteBucketMetricsConfiguration(l -> l.bucket(bucketName).id("metrics-id")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(8);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketMetricsConfiguration(
						l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id"))));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsEmptyId() {
		var client = getClient();
		var bucketName = createBucket(client, 9);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketMetricsConfiguration(
						l -> l.bucket(bucketName).id("").metricsConfiguration(m -> m.id(""))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_CONFIGURATION_ID, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client, 10);

		assertThrows(software.amazon.awssdk.core.exception.SdkClientException.class,
				() -> client.putBucketMetricsConfiguration(l -> l.bucket(bucketName)));
	}

	@Test
	@Tag("Overwrite")
	public void testPutMetricsDuplicateId() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
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
		var bucketName = createBucket(client, 12);
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
		var bucketName = createBucket(client, 13);
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
		var bucketName = createBucket(client, 14);
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

	private static void putMetrics(S3Client client, String bucketName, String metricId) {
		client.putBucketMetricsConfiguration(
				p -> p.bucket(bucketName).id(metricId).metricsConfiguration(m -> m.id(metricId)));
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AwsServiceException.class, executable);
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Max")
	public void testPutMetricsIdLength() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		// ID 는 64자까지 허용한다
		var metricId = "a".repeat(64);
		putMetrics(client, bucketName, metricId);
		var response = client.getBucketMetricsConfiguration(g -> g.bucket(bucketName).id(metricId));
		assertEquals(metricId, response.metricsConfiguration().id());

		assertError(() -> putMetrics(client, bucketName, "a".repeat(65)), HttpStatus.SC_BAD_REQUEST,
				MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Error")
	public void testPutMetricsInvalidId() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		// ID 에는 영숫자와 - _ . 만 쓸 수 있다
		assertError(() -> putMetrics(client, bucketName, "a b/c"), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		assertEquals(0,
				client.listBucketMetricsConfigurations(l -> l.bucket(bucketName)).metricsConfigurationList().size());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsIdMismatch() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		// 쿼리의 id 와 본문의 Id 가 다르면 거부된다
		assertError(() -> client.putBucketMetricsConfiguration(
				p -> p.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("other-id"))),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		assertEquals(0,
				client.listBucketMetricsConfigurations(l -> l.bucket(bucketName)).metricsConfigurationList().size());
	}

	@Test
	@Tag("Error")
	public void testPutMetricsInvalidFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var metricId = "metrics-id";
		var tag = software.amazon.awssdk.services.s3.model.Tag.builder().key("key").value("value").build();

		// And 안에 조건이 하나뿐이면 거부된다
		assertError(() -> client.putBucketMetricsConfiguration(p -> p.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.and(a -> a.prefix("test"))))),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		// And 없이 Prefix 와 Tag 를 함께 줄 수 없다
		assertError(() -> client.putBucketMetricsConfiguration(p -> p.bucket(bucketName).id(metricId)
				.metricsConfiguration(m -> m.id(metricId).filter(f -> f.prefix("test").tag(tag)))),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
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
			putMetrics(client, bucketName, metricId);

		// 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(100, response.metricsConfigurationList().size());
		assertTrue(response.isTruncated());
		var listed = new ArrayList<String>();
		response.metricsConfigurationList().forEach(c -> listed.add(c.id()));

		var token = response.nextContinuationToken();
		response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName).continuationToken(token));
		assertEquals(1, response.metricsConfigurationList().size());
		assertFalse(response.isTruncated());
		response.metricsConfigurationList().forEach(c -> listed.add(c.id()));
		Collections.sort(listed);
		assertEquals(ids, listed);
	}

	@Test
	@Tag("List")
	public void testListMetricsInvalidToken() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		assertError(() -> client.listBucketMetricsConfigurations(
				l -> l.bucket(bucketName).continuationToken("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_CONTINUATION_TOKEN);
	}

	@Test
	@Tag("Error")
	public void testMetricsBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(21);

		assertError(() -> client.listBucketMetricsConfigurations(l -> l.bucket(bucketName)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
		assertError(() -> client.getBucketMetricsConfiguration(g -> g.bucket(bucketName).id("metrics-id")),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketMetricsConfiguration(d -> d.bucket(bucketName).id("metrics-id")),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
	}
}
