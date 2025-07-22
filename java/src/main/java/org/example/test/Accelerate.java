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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketAccelerateStatus;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
public class Accelerate extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Accelerate Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Accelerate End");
	}

	@Test
	@Tag("Put")
	public void testPutBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));
	}

	@Test
	@Tag("Get")
	public void testGetBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));

		var response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals(BucketAccelerateStatus.Enabled, response.getStatus());
	}

	@Test
	@Tag("Change")
	public void testChangeBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));

		var response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals(BucketAccelerateStatus.Enabled, response.getStatus());

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Suspended)));

		response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals(BucketAccelerateStatus.Suspended, response.getStatus());
	}

	@Test
	@Tag("Error")
	public void testPutBucketAccelerateInvalid() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client
						.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
								.withAccelerateConfiguration(new BucketAccelerateConfiguration("Invalid"))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}
}
