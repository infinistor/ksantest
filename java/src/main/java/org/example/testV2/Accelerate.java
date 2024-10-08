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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.apache.hc.core5.http.HttpStatus;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketAccelerateStatus;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
public class Accelerate extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Accelerate V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Accelerate V2 End");
	}

	@Test
	@Tag("Put")
	public void testPutBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketAccelerateConfiguration(
				p -> p.bucket(bucketName).accelerateConfiguration(a -> a.status("Enabled")));
	}

	@Test
	@Tag("Get")
	public void testGetBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketAccelerateConfiguration(p -> p.bucket(bucketName)
				.accelerateConfiguration(a -> a.status(BucketAccelerateStatus.ENABLED)));

		var response = client.getBucketAccelerateConfiguration(g -> g
				.bucket(bucketName));
		assertEquals(BucketAccelerateStatus.ENABLED, response.status());
	}

	@Test
	@Tag("Change")
	public void testChangeBucketAccelerate() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketAccelerateConfiguration(p -> p
				.bucket(bucketName)
				.accelerateConfiguration(a -> a.status(BucketAccelerateStatus.ENABLED)));

		var response = client.getBucketAccelerateConfiguration(g -> g
				.bucket(bucketName));
		assertEquals(BucketAccelerateStatus.ENABLED, response.status());

		client.putBucketAccelerateConfiguration(p -> p
				.bucket(bucketName)
				.accelerateConfiguration(a -> a.status(BucketAccelerateStatus.SUSPENDED)));

		response = client.getBucketAccelerateConfiguration(g -> g
				.bucket(bucketName));
		assertEquals(BucketAccelerateStatus.SUSPENDED, response.status());
	}

	@Test
	@Tag("Error")
	public void testPutBucketAccelerateInvalid() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAccelerateConfiguration(p -> p
						.bucket(bucketName)
						.accelerateConfiguration(a -> a.status("Invalid"))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("MalformedXML", e.awsErrorDetails().errorCode());
	}
}
