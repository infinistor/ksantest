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
	public static void beforeAll() {
		System.out.println("Analytics Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Analytics End");
	}

	@Test
	@Tag("Put")
	// 버킷 분석 설정이 가능한지 확인
	public void testPutBucketAnalytics() {
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
	public void testGetBucketAnalytics() {
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
	public void testAddBucketAnalytics() {
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
	public void testListBucketAnalytics() {
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
	public void testDeleteBucketAnalytics() {
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
	public void testPutBucketAnalyticsInvalid() {
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
