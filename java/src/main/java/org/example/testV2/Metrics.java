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
	// 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketMetricsConfigurations(l -> l.bucket(bucketName));
		assertEquals(0, response.metricsConfigurationList().size());
	}

	@Test
	@Tag("Put")
	// 버킷에 Metrics를 설정할 수 있는지 확인
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketMetricsConfiguration(
				l -> l.bucket(bucketName).id("metrics-id").metricsConfiguration(m -> m.id("metrics-id")));
	}

	@Test
	@Tag("Check")
	// 버킷에 Metrics 설정이 되었는지 확인
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
	// 버킷에 설정된 Metrics를 조회할 수 있는지 확인
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
	// 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
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
	// 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
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
	// 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
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
	// 존재하지 않은 버킷에 Metrics를 설정하려고 할 경우 실패하는지 확인
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
	// Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
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
	// Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client);

		assertThrows(software.amazon.awssdk.core.exception.SdkClientException.class,
				() -> client.putBucketMetricsConfiguration(l -> l.bucket(bucketName)));
	}

	@Test
	@Tag("Override")
	// Metrics 아이디를 중복으로 설정하려고 할 경우 재설정되는지 확인
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
	// 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
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
	// Metrics 설정에 태그를 적용할 수 있는지 확인
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
	// Metrics 설정에 필터를 적용할 수 있는지 확인
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
