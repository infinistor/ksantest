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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.LoggingEnabled;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;
import software.amazon.awssdk.services.s3.model.SimplePrefix;

public class Logging extends TestBase {
	@Test
	@Tag("Put/Get")
	public void testLoggingGet() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.getBucketLogging(g -> g.bucket(bucketName));
		assertNull(response.loggingEnabled());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSet() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 2);
		var targetBucketName = createBucket(client, 2);

		client.putBucketLogging(p -> p.bucket(sourceBucketName)
				.bucketLoggingStatus(c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(""))));
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSetGet() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);

		client.putBucketLogging(p -> p.bucket(sourceBucketName)
				.bucketLoggingStatus(c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(""))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals("", response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingPrefix() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 4);
		var targetBucketName = createBucket(client, 4);

		client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
				c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(prefix, response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Versioning")
	public void testLoggingVersioning() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
				c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix))));

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(prefix, response.loggingEnabled().targetPrefix());
		assertEquals(targetBucketName, response.loggingEnabled().targetBucket());
	}

	@Test
	@Tag("Encryption")
	public void testLoggingEncryption() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

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
	public void testLoggingBucketNotFound() {
		var sourceBucketName = getNewBucketNameOnly(7);
		var targetBucketName = getNewBucketNameOnly(7);
		var prefix = "logs/";
		var client = getClient();

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
			c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix)))));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testLoggingTargetBucketNotFound() {
		var prefix = "logs/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 8);
		var targetBucketName = getNewBucketNameOnly(8);

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketLogging(p -> p.bucket(sourceBucketName).bucketLoggingStatus(
			c -> c.loggingEnabled(l -> l.targetBucket(targetBucketName).targetPrefix(prefix)))));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingDisable() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 9);
		var targetBucketName = createBucket(client, 9);

		putLogging(client, sourceBucketName, loggingEnabled(targetBucketName, "logs/").build());
		assertNotNull(client.getBucketLogging(g -> g.bucket(sourceBucketName)).loggingEnabled());

		// 빈 설정을 넣으면 로깅이 해제된다
		putLogging(client, sourceBucketName, null);
		assertNull(client.getBucketLogging(g -> g.bucket(sourceBucketName)).loggingEnabled());
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingOverwrite() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 10);
		var targetBucketName = createBucket(client, 10);
		var newTargetBucketName = createBucket(client, 10);

		putLogging(client, sourceBucketName, loggingEnabled(targetBucketName, "logs/").build());
		putLogging(client, sourceBucketName, loggingEnabled(newTargetBucketName, "other/").build());

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		assertEquals(newTargetBucketName, response.loggingEnabled().targetBucket());
		assertEquals("other/", response.loggingEnabled().targetPrefix());
	}

	@Test
	@Tag("KeyFormat")
	public void testLoggingSimplePrefix() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 11);
		var targetBucketName = createBucket(client, 11);

		putLogging(client, sourceBucketName, loggingEnabled(targetBucketName, "logs/")
				.targetObjectKeyFormat(f -> f.simplePrefix(SimplePrefix.builder().build())).build());

		var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
		var keyFormat = response.loggingEnabled().targetObjectKeyFormat();
		assertNotNull(keyFormat.simplePrefix());
		assertNull(keyFormat.partitionedPrefix());
	}

	@Test
	@Tag("KeyFormat")
	public void testLoggingPartitionedPrefix() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 12);
		var targetBucketName = createBucket(client, 12);

		for (var dateSource : List.of("EventTime", "DeliveryTime")) {
			putLogging(client, sourceBucketName, loggingEnabled(targetBucketName, "logs/")
					.targetObjectKeyFormat(f -> f.partitionedPrefix(p -> p.partitionDateSource(dateSource))).build());

			var response = client.getBucketLogging(g -> g.bucket(sourceBucketName));
			var keyFormat = response.loggingEnabled().targetObjectKeyFormat();
			assertEquals(dateSource, keyFormat.partitionedPrefix().partitionDateSourceAsString());
			assertNull(keyFormat.simplePrefix());
		}
	}

	@Test
	@Tag("Put/Get")
	public void testLoggingSameBucket() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		// 소스 버킷 자신을 타깃으로 지정할 수 있다
		putLogging(client, bucketName, loggingEnabled(bucketName, "self/").build());

		var response = client.getBucketLogging(g -> g.bucket(bucketName));
		assertEquals(bucketName, response.loggingEnabled().targetBucket());
		assertEquals("self/", response.loggingEnabled().targetPrefix());
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
			var e = assertThrows(AwsServiceException.class,
					() -> putLogging(client, sourceBucketName, loggingEnabled(targetBucketName, "logs/").build()));
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
			assertEquals(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, e.awsErrorDetails().errorCode());
		} finally {
			altClient.deleteBucket(d -> d.bucket(targetBucketName));
			deleteBucketList(targetBucketName);
		}
	}

	@Test
	@Tag("Error")
	public void testLoggingGetBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(15);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketLogging(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testLoggingAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucketName = createBucket(client, 16);
		var targetBucketName = createBucket(client, 16);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getBucketLogging(g -> g.bucket(sourceBucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> putLogging(altClient, sourceBucketName, loggingEnabled(targetBucketName, "logs/").build()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	private static LoggingEnabled.Builder loggingEnabled(String targetBucketName, String prefix) {
		return LoggingEnabled.builder().targetBucket(targetBucketName).targetPrefix(prefix);
	}

	/** 로깅 설정을 저장한다. loggingEnabled 가 null 이면 빈 설정을 넣어 로깅을 해제한다. */
	private void putLogging(S3Client client, String sourceBucketName, LoggingEnabled loggingEnabled) {
		client.putBucketLogging(
				p -> p.bucket(sourceBucketName).bucketLoggingStatus(s -> s.loggingEnabled(loggingEnabled)));
	}
}
