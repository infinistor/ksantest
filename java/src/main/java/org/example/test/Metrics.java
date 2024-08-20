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
	// 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
	public void testMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketMetricsConfigurations(
				new ListBucketMetricsConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getMetricsConfigurationList());
	}

	@Test
	@Tag("Put")
	// 버킷에 Metrics를 설정할 수 있는지 확인
	public void testPutMetrics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metrics = new MetricsConfiguration().withId("metrics-id");

		client.setBucketMetricsConfiguration(bucketName, metrics);
	}

	@Test
	@Tag("Check")
	// 버킷에 Metrics 설정이 되었는지 확인
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
	// 버킷에 설정된 Metrics를 조회할 수 있는지 확인
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
	// 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
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
	// 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
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
	// 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
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
	// 존재하지 않은 버킷에 Metrics를 설정하려고 할 경우 실패하는지 확인
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
	// Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
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
	// Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
	public void testPutMetricsNoId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var metrics = new MetricsConfiguration();

		assertThrows(IllegalArgumentException.class, () -> client.setBucketMetricsConfiguration(bucketName, metrics));
	}

	@Test
	@Tag("Override")
	// Metrics 아이디를 중복으로 설정하려고 할 경우 재설정되는지 확인
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
	@Tag("Filter")
	// 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
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
	@Tag("Filter")
	// Metrics 설정에 태그를 적용할 수 있는지 확인
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
	@Tag("Filter")
	// 옵션을 포함한 Metrics 설정이 올바르게 적용되는지 확인
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
