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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;

public class Logging extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Logging Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Logging End");
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 조회 가능한지 확인
	public void testLoggingGet() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketLogging(g -> g.bucket(bucketName));
		assertNull(response.loggingEnabled());
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 가능한지 확인
	public void testLoggingSet() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketLogging(p -> p.bucket(sourceBucketName)
				.bucketLoggingStatus(c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(""))));
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
	public void testLoggingSetGet() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketLogging(p -> p.bucket(sourceBucketName)
				.bucketLoggingStatus(c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(""))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals("", response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Put/Get")
	// 버킷의 로깅에 Prefix가 설정되는지 확인
	public void testLoggingPrefix() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
				c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(prefix, response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Versioning")
	// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
	public void testLoggingVersioning() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
				c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(prefix, response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Encryption")
	// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
	public void testLoggingEncryption() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		client.putBucketEncryption(p -> p.bucket(sourceBucketName)
				.serverSideEncryptionConfiguration(s -> s.rules(ServerSideEncryptionRule.builder()
						.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256))
						.build())));

		client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
				c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(prefix, response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Error")
	// 존재하지 않는 버킷에 로깅 설정 실패 확인
	public void testLoggingBucketNotFound() {
		var sourceBucketName = getNewBucketNameOnly();
		var targetBucketName = getNewBucketNameOnly();
		var prefix = "logs/";
		var client = getClient();

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
			c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix)))));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
	public void testLoggingTargetBucketNotFound() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = getNewBucketNameOnly();

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
			c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix)))));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.awsErrorDetails().errorCode());
	}

}
