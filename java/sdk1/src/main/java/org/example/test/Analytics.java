package org.example.test;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.analytics.AnalyticsExportDestination;
import com.amazonaws.services.s3.model.analytics.AnalyticsS3BucketDestination;
import com.amazonaws.services.s3.model.analytics.StorageClassAnalysis;
import com.amazonaws.services.s3.model.analytics.StorageClassAnalysisDataExport;

public class Analytics extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Analytics Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Analytics End");
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 가능한지 확인
	public void test_put_bucket_analytics() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));
	}

	@Test
	@Tag("Get")
	// 버킷 분석 설정이 올바르게 적용되는지 확인
	public void test_get_bucket_analytics() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));

		var response = client.getBucketAnalyticsConfiguration(bucketName, "test");
		assertEquals("test", response.getAnalyticsConfiguration().getId());
		assertEquals("V_1", response.getAnalyticsConfiguration().getStorageClassAnalysis().getDataExport()
				.getOutputSchemaVersion());
		assertEquals("arn:aws:s3:::" + targetBucketName, response.getAnalyticsConfiguration().getStorageClassAnalysis()
				.getDataExport().getDestination().getS3BucketDestination().getBucketArn());
		assertEquals("CSV", response.getAnalyticsConfiguration().getStorageClassAnalysis().getDataExport()
				.getDestination().getS3BucketDestination().getFormat());
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 여러개 가능한지 확인
	public void test_add_bucket_analytics() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));
		var response = client.listBucketAnalyticsConfigurations(new ListBucketAnalyticsConfigurationsRequest().withBucketName(bucketName));
		assertEquals(1, response.getAnalyticsConfigurationList().size());

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test2")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));
		response = client.listBucketAnalyticsConfigurations(new ListBucketAnalyticsConfigurationsRequest().withBucketName(bucketName));
		assertEquals(2, response.getAnalyticsConfigurationList().size());
	}

	@Test
	@Tag("List")
	// 버킷 분석 설정이 목록으로 조회되는지 확인
	public void test_list_bucket_analytics() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));
		var response = client.listBucketAnalyticsConfigurations(new ListBucketAnalyticsConfigurationsRequest().withBucketName(bucketName));
		assertEquals(1, response.getAnalyticsConfigurationList().size());
		assertEquals("test", response.getAnalyticsConfigurationList().get(0).getId());
		assertEquals("V_1", response.getAnalyticsConfigurationList().get(0).getStorageClassAnalysis().getDataExport()
				.getOutputSchemaVersion());
		assertEquals("arn:aws:s3:::" + targetBucketName, response.getAnalyticsConfigurationList().get(0)
				.getStorageClassAnalysis().getDataExport().getDestination().getS3BucketDestination().getBucketArn());
		assertEquals("CSV", response.getAnalyticsConfigurationList().get(0).getStorageClassAnalysis().getDataExport()
				.getDestination().getS3BucketDestination().getFormat());
	}

	@Test
	@Tag("Delete")
	// 버킷 분석 설정이 삭제되는지 확인
	public void test_delete_bucket_analytics() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
				.withStorageClassAnalysis(new StorageClassAnalysis()
						.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
								.withDestination(new AnalyticsExportDestination()
										.withS3BucketDestination(new AnalyticsS3BucketDestination()
												.withBucketArn("arn:aws:s3:::" + targetBucketName)
												.withFormat("CSV"))))));
		client.deleteBucketAnalyticsConfiguration(bucketName, "test");
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketAnalyticsConfiguration(bucketName, "test"));
		assertEquals(404, e.getStatusCode());
		assertEquals("NoSuchConfiguration", e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 버킷 분석 설정을 잘못 입력했을 때 에러가 발생하는지 확인
	public void test_put_bucket_analytics_invalid() {
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
						.withStorageClassAnalysis(new StorageClassAnalysis()
								.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_2")
										.withDestination(new AnalyticsExportDestination()
												.withS3BucketDestination(new AnalyticsS3BucketDestination()
														.withBucketArn("arn:aws:s3:::" + targetBucketName)
														.withFormat("CSV")))))));
		assertEquals(400, e.getStatusCode());
		assertEquals("MalformedXML", e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("")
						.withStorageClassAnalysis(new StorageClassAnalysis()
								.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
										.withDestination(new AnalyticsExportDestination()
												.withS3BucketDestination(new AnalyticsS3BucketDestination()
														.withBucketArn("arn:aws:s3:::" + targetBucketName)
														.withFormat("CSV")))))));
		assertEquals(400, e.getStatusCode());
		assertEquals("InvalidConfigurationId", e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAnalyticsConfiguration(bucketName, new AnalyticsConfiguration().withId("test")
						.withStorageClassAnalysis(new StorageClassAnalysis()
								.withDataExport(new StorageClassAnalysisDataExport().withOutputSchemaVersion("V_1")
										.withDestination(new AnalyticsExportDestination()
												.withS3BucketDestination(new AnalyticsS3BucketDestination()
														.withBucketArn("arn:aws:s3:::" + targetBucketName)
														.withFormat("JSON")))))));
		assertEquals(400, e.getStatusCode());
		assertEquals("MalformedXML", e.getErrorCode());
	}
}
