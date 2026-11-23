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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.TargetObjectKeyFormat;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.PartitionedPrefix;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SimplePrefix;

public class Logging extends TestBase {
	@Test
	@Tag("Put/Get")
	public void testLoggingGet() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.getBucketLoggingConfiguration(bucketName);
		assertNull(response.getLogFilePrefix());
		assertNull(response.getDestinationBucketName());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSet() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 2);
		var targetBucketName = createBucket(client, 2);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, ""));
		client.setBucketLoggingConfiguration(request);
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSetGet() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);

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
		var sourceBucketName = createBucket(client, 4);
		var targetBucketName = createBucket(client, 4);

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
		var sourceBucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);

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
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

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
		var sourceBucketName = getNewBucketNameOnly(7);
		var targetBucketName = getNewBucketNameOnly(7);
		var prefix = "logs/";
		var client = getClient();

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(request));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testLoggingTargetBucketNotFound() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 8);
		var targetBucketName = getNewBucketNameOnly(8);

		var request = new SetBucketLoggingConfigurationRequest(sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(request));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.getErrorCode());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingDisable() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 9);
		var targetBucketName = createBucket(client, 9);

		putLogging(client, sourceBucketName, new BucketLoggingConfiguration(targetBucketName, "logs/"));
		assertTrue(client.getBucketLoggingConfiguration(sourceBucketName).isLoggingEnabled());

		// 빈 설정을 넣으면 로깅이 해제된다
		putLogging(client, sourceBucketName, new BucketLoggingConfiguration());
		assertFalse(client.getBucketLoggingConfiguration(sourceBucketName).isLoggingEnabled());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingOverwrite() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 10);
		var targetBucketName = createBucket(client, 10);
		var newTargetBucketName = createBucket(client, 10);

		putLogging(client, sourceBucketName, new BucketLoggingConfiguration(targetBucketName, "logs/"));
		putLogging(client, sourceBucketName, new BucketLoggingConfiguration(newTargetBucketName, "other/"));

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		assertEquals(newTargetBucketName, response.getDestinationBucketName());
		assertEquals("other/", response.getLogFilePrefix());
	}

	@Test
	@Tag("KeyFormat")
	public void testLoggingSimplePrefix() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 11);
		var targetBucketName = createBucket(client, 11);

		putLogging(client, sourceBucketName, new BucketLoggingConfiguration(targetBucketName, "logs/")
				.withTargetObjectKeyFormat(new TargetObjectKeyFormat(new SimplePrefix())));

		var response = client.getBucketLoggingConfiguration(sourceBucketName);
		var keyFormat = response.getTargetObjectKeyFormat();
		assertNotNull(keyFormat.getSimplePrefix());
		assertNull(keyFormat.getPartitionedPrefix());
	}

	@Test
	@Tag("KeyFormat")
	public void testLoggingPartitionedPrefix() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 12);
		var targetBucketName = createBucket(client, 12);

		for (var dateSource : List.of("EventTime", "DeliveryTime")) {
			putLogging(client, sourceBucketName, new BucketLoggingConfiguration(targetBucketName, "logs/")
					.withTargetObjectKeyFormat(new TargetObjectKeyFormat(
							new PartitionedPrefix().withPartitionDateSource(dateSource))));

			var response = client.getBucketLoggingConfiguration(sourceBucketName);
			var keyFormat = response.getTargetObjectKeyFormat();
			assertEquals(dateSource, keyFormat.getPartitionedPrefix().getPartitionDateSourceAsString());
			assertNull(keyFormat.getSimplePrefix());
		}
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSameBucket() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		// 소스 버킷 자신을 타깃으로 지정할 수 있다
		putLogging(client, bucketName, new BucketLoggingConfiguration(bucketName, "self/"));

		var response = client.getBucketLoggingConfiguration(bucketName);
		assertEquals(bucketName, response.getDestinationBucketName());
		assertEquals("self/", response.getLogFilePrefix());
	}

	@Test
	@Tag("Error")
	public void testLoggingTargetBucketOtherOwner() {
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucketName = createBucket(client, 14);
		var targetBucketName = createBucket(altClient, 14);

		try {
			// 소스와 타깃 버킷의 소유자가 다르면 거부된다
			var e = assertThrows(AmazonServiceException.class, () -> putLogging(client, sourceBucketName,
					new BucketLoggingConfiguration(targetBucketName, "logs/")));
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
			assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.getErrorCode());
		} finally {
			altClient.deleteBucket(targetBucketName);
			deleteBucketList(targetBucketName);
		}
	}

	@Test
	@Tag("Error")
	public void testLoggingGetBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(15);

		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketLoggingConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testLoggingAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucketName = createBucket(client, 16);
		var targetBucketName = createBucket(client, 16);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getBucketLoggingConfiguration(sourceBucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> putLogging(altClient, sourceBucketName,
				new BucketLoggingConfiguration(targetBucketName, "logs/")));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	/** 로깅 설정을 저장한다. 빈 BucketLoggingConfiguration 을 넣으면 로깅이 해제된다. */
	private void putLogging(AmazonS3 client, String sourceBucketName, BucketLoggingConfiguration loggingConfig) {
		client.setBucketLoggingConfiguration(
				new SetBucketLoggingConfigurationRequest(sourceBucketName, loggingConfig));
	}
}
