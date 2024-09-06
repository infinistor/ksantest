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

import org.example.Data.MainData;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.WebsiteConfiguration;

import org.junit.jupiter.api.Tag;

public class Website extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Website V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Website V2 End");
	}

	@Test
	@Tag("Check")
	public void testWebsiteGetBuckets() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_WEBSITE_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testWebsitePutBuckets() {
		var client = getClient();
		var bucketName = createBucket(client);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("400"))
				.indexDocument(d -> d.suffix("a"))
				.build();

		client.putBucketWebsite(g -> g.bucket(bucketName).websiteConfiguration(webConfig));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals(webConfig.errorDocument(), response.errorDocument());
		assertEquals(webConfig.indexDocument(), response.indexDocument());
	}

	@Test
	@Tag("Delete")
	public void testWebsiteDeleteBuckets() {
		var client = getClient();
		var bucketName = createBucket(client);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("400"))
				.indexDocument(d -> d.suffix("a"))
				.build();

		client.putBucketWebsite(g -> g.bucket(bucketName).websiteConfiguration(webConfig));
		client.deleteBucketWebsite(d -> d.bucket(bucketName));
	}
}
