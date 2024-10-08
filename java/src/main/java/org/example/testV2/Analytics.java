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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hc.core5.http.HttpStatus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class Analytics extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Analytics V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Analytics V2 End");
	}

	@Test
	@Tag("Put")
	public void testPutBucketAnalytics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));
	}

	@Test
	@Tag("Get")
	public void testGetBucketAnalytics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));

		var response = client.getBucketAnalyticsConfiguration(g -> g.bucket(bucketName).id("test"));
		assertEquals("test", response.analyticsConfiguration().id());
		assertEquals("V_1", response.analyticsConfiguration().storageClassAnalysis().dataExport()
				.outputSchemaVersion());
		assertEquals("arn:aws:s3:::" + targetBucketName, response.analyticsConfiguration().storageClassAnalysis()
				.dataExport().destination().s3BucketDestination().bucket());
		assertEquals("CSV", response.analyticsConfiguration().storageClassAnalysis().dataExport()
				.destination().s3BucketDestination().format());
	}

	@Test
	@Tag("Put")
	public void testAddBucketAnalytics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));
		var response = client.listBucketAnalyticsConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.analyticsConfigurationList().size());

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test2")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));
		response = client.listBucketAnalyticsConfigurations(l -> l.bucket(bucketName));
		assertEquals(2, response.analyticsConfigurationList().size());
	}

	@Test
	@Tag("List")
	public void testListBucketAnalytics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));
		var response = client.listBucketAnalyticsConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.analyticsConfigurationList().size());
		assertEquals("test", response.analyticsConfigurationList().get(0).id());
		assertEquals("V_1", response.analyticsConfigurationList().get(0).storageClassAnalysis().dataExport()
				.outputSchemaVersion());
		assertEquals("arn:aws:s3:::" + targetBucketName, response.analyticsConfigurationList().get(0)
				.storageClassAnalysis().dataExport().destination().s3BucketDestination().bucket());
		assertEquals("CSV", response.analyticsConfigurationList().get(0).storageClassAnalysis().dataExport()
				.destination().s3BucketDestination().format());
	}

	@Test
	@Tag("Delete")
	public void testDeleteBucketAnalytics() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
				.analyticsConfiguration(analytics -> analytics.id("test")
						.storageClassAnalysis(storage -> storage
								.dataExport(data -> data.outputSchemaVersion("V_1")
										.destination(destination -> destination
												.s3BucketDestination(s3 -> s3
														.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))))));
		client.deleteBucketAnalyticsConfiguration(d -> d.bucket(bucketName).id("test"));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketAnalyticsConfiguration(g -> g.bucket(bucketName).id("test")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals("NoSuchConfiguration", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketAnalyticsInvalid() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
						.analyticsConfiguration(analytics -> analytics.id("test")
								.storageClassAnalysis(storage -> storage
										.dataExport(data -> data.outputSchemaVersion("V_2")
												.destination(destination -> destination
														.s3BucketDestination(s3 -> s3
																.bucket("arn:aws:s3:::" + targetBucketName)
																.format("CSV"))))))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("MalformedXML", e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
						.analyticsConfiguration(analytics -> analytics.id("")
								.storageClassAnalysis(storage -> storage
										.dataExport(data -> data.outputSchemaVersion("V_1")
												.destination(destination -> destination
														.s3BucketDestination(s3 -> s3
																.bucket("arn:aws:s3:::" + targetBucketName)
																.format("CSV"))))))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("InvalidConfigurationId", e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAnalyticsConfiguration(p -> p.bucket(bucketName)
						.analyticsConfiguration(analytics -> analytics.id("test")
								.storageClassAnalysis(storage -> storage
										.dataExport(data -> data.outputSchemaVersion("V_1")
												.destination(destination -> destination
														.s3BucketDestination(s3 -> s3
																.bucket("arn:aws:s3:::" + targetBucketName)
																.format("JSON"))))))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("MalformedXML", e.awsErrorDetails().errorCode());
	}
}
