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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.ReplicationDestinationConfig;
import com.amazonaws.services.s3.model.ReplicationRule;
import com.amazonaws.services.s3.model.replication.ReplicationFilter;
import com.amazonaws.services.s3.model.replication.ReplicationPrefixPredicate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Replication Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Replication End");
	}

	@Test
	@Tag("Check")
	// @Tag("버킷의 Replication 설정이 되는지 확인(put/get/delete)")
	public void testReplicationSet() {
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningConfiguration.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		var rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination);
		var config = new BucketReplicationConfiguration();
		config.setRoleARN("arn:aws:iam::635518764071:role/aws_replication_test");
		config.addRule("rule1", rule);

		client.setBucketReplicationConfiguration(sourceBucketName, config);
		var getConfig = client.getBucketReplicationConfiguration(sourceBucketName);
		replicationConfigCompare(config, getConfig);

		client.deleteBucketReplicationConfiguration(sourceBucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketReplicationConfiguration(sourceBucketName));
		assertEquals(404, e.getStatusCode());
	}

	@Test
	@Tag("ERROR")
	// @Tag("원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
	public void testReplicationInvalidSourceBucketName() {

		var sourceBucketName = getNewBucketNameOnly();
		var targetBucketName = getNewBucketNameOnly();
		var client = getClient();
		var prefix = "test1";

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN("arn:aws:iam::635518764071:role/aws_replication_test");
		config.addRule("rule1", rule);

		var e = assertThrows(AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(404, e.getStatusCode());
		assertEquals("NoSuchBucket", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// @Tag("원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
	public void testReplicationInvalidSourceBucketVersioning() {

		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();
		var prefix = "test1";

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(
				targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN("arn:aws:iam::635518764071:role/aws_replication_test");
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(400, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// @Tag("대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")
	public void testReplicationInvalidTargetBucketName() {

		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucketNameOnly();
		var client = getClient();
		var prefix = "test1";

		checkConfigureVersioningRetry(
				sourceBucketName,
				BucketVersioningConfiguration.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(
				targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN("arn:aws:iam::635518764071:role/aws_replication_test");
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(400, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// @Tag("대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")
	public void testReplicationInvalidTargetBucketVersioning() {

		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();
		var prefix = "test1";

		checkConfigureVersioningRetry(
				sourceBucketName,
				BucketVersioningConfiguration.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN("arn:aws:iam::635518764071:role/aws_replication_test");
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(400, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}
}