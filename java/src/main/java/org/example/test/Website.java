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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;

public class Website extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("Website Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("Website End");
	}

	@Test
	@Tag("Check")
	//버킷의 Website 설정 조회 확인
	public void testWebsiteGetBuckets()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertNull(response);
	}

	@Test
	@Tag("Check")
	//버킷의 Website 설정이 가능한지 확인
	public void testWebsitePutBuckets()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setErrorDocument("400");
		webConfig.setIndexDocumentSuffix("a");

		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals(webConfig.getErrorDocument(), response.getErrorDocument());
	}

	@Test
	@Tag("Delete")
	//버킷의 Website 설정이 삭제가능한지 확인
	public void testWebsiteDeleteBuckets()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setErrorDocument("400");
		webConfig.setIndexDocumentSuffix("a");

		client.setBucketWebsiteConfiguration(bucketName, webConfig);
		client.deleteBucketWebsiteConfiguration(bucketName);
	}
}
