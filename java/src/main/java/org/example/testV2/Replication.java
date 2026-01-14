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

import org.apache.hc.core5.http.HttpStatus;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Destination;
import software.amazon.awssdk.services.s3.model.ReplicationConfiguration;
import software.amazon.awssdk.services.s3.model.ReplicationRule;
import software.amazon.awssdk.services.s3.model.ReplicationRuleFilter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Replication V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Replication V2 End");
	}

	@Test
	@Tag("Check")
	public void testReplicationSet() {
		var prefix = "test/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled").priority(1)
						.destination(Destination.builder().bucket(targetBucketARN).build())
						.filter(ReplicationRuleFilter.builder().prefix(prefix).build())
						.deleteMarkerReplication(d -> d.status("Disabled"))
						.build())
				.build();

		client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config));
		var getConfig = client.getBucketReplication(get -> get.bucket(sourceBucketName));
		replicationConfigCompare(config, getConfig.replicationConfiguration());

		client.deleteBucketReplication(delete -> delete.bucket(sourceBucketName));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketReplication(get -> get.bucket(sourceBucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidSourceBucketName() {
		var client = getClient();
		var sourceBucketName = getNewBucketNameOnly();
		var targetBucketName = getNewBucketNameOnly();

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled")
						.destination(Destination.builder().bucket(targetBucketARN).build()).build())
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals("NoSuchBucket", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidSourceBucketVersioning() {
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled")
						.destination(Destination.builder().bucket(targetBucketARN).build()).build())
				.build();

		var e = assertThrows(
				AwsServiceException.class,
				() -> client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("InvalidRequest", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidTargetBucketName() {
		var prefix = "test/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = getNewBucketNameOnly();

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled").priority(1)
						.destination(Destination.builder().bucket(targetBucketARN).build())
						.filter(ReplicationRuleFilter.builder().prefix(prefix).build())
						.deleteMarkerReplication(d -> d.status("Disabled"))
						.build())
				.build();
		var e = assertThrows(
				AwsServiceException.class,
				() -> client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("InvalidRequest", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidTargetBucketVersioning() {
		var prefix = "test/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		checkConfigureVersioningRetry(
				sourceBucketName,
				BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled").priority(1)
						.destination(Destination.builder().bucket(targetBucketARN).build())
						.filter(ReplicationRuleFilter.builder().prefix(prefix).build())
						.deleteMarkerReplication(d -> d.status("Disabled"))
						.build())
				.build();
		var e = assertThrows(
				AwsServiceException.class,
				() -> client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals("InvalidRequest", e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationBucketVersioningSuspend() {
		var prefix = "test/";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		// 원본, 대상 버킷 버저닝 설정
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		// 원본 버킷 복제 설정
		var config = ReplicationConfiguration.builder().role("arn:aws:iam::635518764071:role/replication")
				.rules(ReplicationRule.builder().status("Enabled").priority(1)
						.destination(Destination.builder().bucket(targetBucketARN).build())
						.filter(ReplicationRuleFilter.builder().prefix(prefix).build())
						.deleteMarkerReplication(d -> d.status("Disabled"))
						.build())
				.build();

		client.putBucketReplication(p -> p.bucket(sourceBucketName).replicationConfiguration(config));

		// 원본 버킷 버저닝 중단 실패 확인
		var e1 = assertThrows(
				AwsServiceException.class,
				() -> checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.SUSPENDED));
		assertEquals(HttpStatus.SC_CONFLICT, e1.statusCode());
		assertEquals("InvalidBucketState", e1.awsErrorDetails().errorCode());
	}
}