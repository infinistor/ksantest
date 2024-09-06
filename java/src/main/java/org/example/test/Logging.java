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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;

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
	public void testLoggingGet() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.getBucketLoggingConfiguration(bucketName);
		assertNull(response.getLogFilePrefix());
		assertNull(response.getDestinationBucketName());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSet() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, ""));
		client.setBucketLoggingConfiguration(request);
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSetGet() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, ""));
		client.setBucketLoggingConfiguration(request);

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		assertEquals("", response.getLogFilePrefix());
		assertEquals(targetBucketName, response.getDestinationBucketName());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingPrefix() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		client.setBucketLoggingConfiguration(request);

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		assertEquals(prefix, response.getLogFilePrefix());
		assertEquals(targetBucketName, response.getDestinationBucketName());
	}

	@Test
	@Tag("Versioning")
	public void testLoggingVersioning() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		client.setBucketLoggingConfiguration(request);

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		assertEquals(prefix, response.getLogFilePrefix());
		assertEquals(targetBucketName, response.getDestinationBucketName());
	}

	@Test
	@Tag("Encryption")
	public void testLoggingEncryption() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));
		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(sourceBucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		client.setBucketLoggingConfiguration(request);

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		assertEquals(prefix, response.getLogFilePrefix());
		assertEquals(targetBucketName, response.getDestinationBucketName());
	}

	@Test
	@Tag("Error")
	public void testLoggingBucketNotFound() {
		var sourceBucketName = getNewBucketNameOnly();
		var targetBucketName = getNewBucketNameOnly();
		var prefix = "logs/";
		var client = getClient();

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(request));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testLoggingTargetBucketNotFound() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = getNewBucketNameOnly();

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(request));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.getErrorCode());
	}

}
