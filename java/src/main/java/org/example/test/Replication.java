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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteMarkerReplication;
import com.amazonaws.services.s3.model.ReplicationDestinationConfig;
import com.amazonaws.services.s3.model.ReplicationRule;
import com.amazonaws.services.s3.model.replication.ReplicationAndOperator;
import com.amazonaws.services.s3.model.replication.ReplicationFilter;
import com.amazonaws.services.s3.model.replication.ReplicationFilterPredicate;
import com.amazonaws.services.s3.model.replication.ReplicationPrefixPredicate;
import com.amazonaws.services.s3.model.replication.ReplicationTagPredicate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase {
	@Test
	@Tag("Check")
	public void testReplicationSet() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 1);
		var targetBucketName = createBucket(client, 1);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningConfiguration.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		var rule = new ReplicationRule().withStatus("Enabled").withDestinationConfig(destination);
		var config = new BucketReplicationConfiguration();
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		client.setBucketReplicationConfiguration(sourceBucketName, config);
		var getConfig = client.getBucketReplicationConfiguration(sourceBucketName);
		replicationConfigCompare(config, getConfig);

		client.deleteBucketReplicationConfiguration(sourceBucketName);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketReplicationConfiguration(sourceBucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidSourceBucketName() {
		var prefix = "test1";
		var client = getClient();
		var sourceBucketName = getNewBucketNameOnly(2);
		var targetBucketName = getNewBucketNameOnly(2);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		var e = assertThrows(AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals("NoSuchBucket", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidSourceBucketVersioning() {
		var prefix = "test1";
		var client = getClient();
		var sourceBucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		ReplicationDestinationConfig destination = new ReplicationDestinationConfig().withBucketARN(
				targetBucketARN);
		ReplicationRule rule = new ReplicationRule()
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)));
		BucketReplicationConfiguration config = new BucketReplicationConfiguration();
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidTargetBucketName() {
		var prefix = "test1";
		var client = getClient();
		var sourceBucketName = createBucket(client, 4);
		var targetBucketName = getNewBucketNameOnly(4);

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
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidTargetBucketVersioning() {
		var prefix = "test1";
		var client = getClient();
		var sourceBucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);

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
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		var e = assertThrows(
				AmazonS3Exception.class,
				() -> client.setBucketReplicationConfiguration(sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals("InvalidRequest", e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationBucketVersioningSuspend() {
		var prefix = "test1";
		var client = getClient();
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

		// 원본, 대상 버킷 버저닝 설정
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningConfiguration.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		// 원본 버킷 복제 설정
		var destination = new ReplicationDestinationConfig().withBucketARN(targetBucketARN);
		var rule = new ReplicationRule()
				.withPriority(1)
				.withStatus("Enabled")
				.withDestinationConfig(destination)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate(prefix)))
				.withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Disabled"));
		var config = new BucketReplicationConfiguration();
		config.setRoleARN(replicationRole());
		config.addRule("rule1", rule);

		client.setBucketReplicationConfiguration(sourceBucketName, config);

		// 원본 버킷 버저닝 중단 실패 확인
		var e1 = assertThrows(
				AmazonS3Exception.class,
				() -> checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.SUSPENDED));
		assertEquals(HttpStatus.SC_CONFLICT, e1.getStatusCode());
		assertEquals("InvalidBucketState", e1.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testReplicationGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketReplicationConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.REPLICATION_CONFIGURATION_NOT_FOUND_ERROR, e.getErrorCode());

		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketReplicationConfiguration(bucketName);
	}

	@Test
	@Tag("Filter")
	public void testReplicationTagFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 8);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withFilter(new ReplicationFilter(new ReplicationTagPredicate(new com.amazonaws.services.s3.model.Tag("key1", "value1"))))));

		var predicate = (ReplicationTagPredicate) getReplicationRule(client, sourceBucketName, "rule1").getFilter()
				.getPredicate();
		assertEquals("key1", predicate.getTag().getKey());
		assertEquals("value1", predicate.getTag().getValue());
	}

	@Test
	@Tag("Filter")
	public void testReplicationAndFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 9);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];
		List<ReplicationFilterPredicate> operands = List.of(new ReplicationPrefixPredicate("test/"),
				new ReplicationTagPredicate(new com.amazonaws.services.s3.model.Tag("key1", "value1")),
				new ReplicationTagPredicate(new com.amazonaws.services.s3.model.Tag("key2", "value2")));

		putReplication(client, sourceBucketName, replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withFilter(new ReplicationFilter(new ReplicationAndOperator(operands)))));

		var predicate = (ReplicationAndOperator) getReplicationRule(client, sourceBucketName, "rule1").getFilter()
				.getPredicate();
		String prefix = null;
		var tags = new ArrayList<String>();
		for (var operand : predicate.getOperands()) {
			if (operand instanceof ReplicationPrefixPredicate prefixPredicate)
				prefix = prefixPredicate.getPrefix();
			else if (operand instanceof ReplicationTagPredicate tagPredicate)
				tags.add(tagPredicate.getTag().getKey() + "=" + tagPredicate.getTag().getValue());
		}
		assertEquals("test/", prefix);
		assertEquals(List.of("key1=value1", "key2=value2"), tags);
	}

	@Test
	@Tag("Filter")
	public void testReplicationEmptyFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 10);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 빈 접두어는 버킷 전체를 대상으로 한다
		putReplication(client, sourceBucketName, replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("")))));

		var filter = getReplicationRule(client, sourceBucketName, "rule1").getFilter();
		assertNotNull(filter);
		if (filter.getPredicate() != null) {
			var predicate = (ReplicationPrefixPredicate) filter.getPredicate();
			assertTrue(predicate.getPrefix() == null || predicate.getPrefix().isEmpty());
		}
	}

	@Test
	@Tag("Filter")
	public void testReplicationV1Schema() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 11);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 구버전(V1) 스키마: Filter/Priority/DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다
		putReplication(client, sourceBucketName, replicationConfig().addRule("v1-rule", new ReplicationRule()
				.withStatus("Enabled").withPrefix("v1/")
				.withDestinationConfig(new ReplicationDestinationConfig().withBucketARN(targetBucketARN))));

		var rule = getReplicationRule(client, sourceBucketName, "v1-rule");
		assertNotNull(rule);
		assertEquals("v1/", rule.getPrefix());
		assertEquals("Enabled", rule.getStatus());
		assertEquals(targetBucketARN, rule.getDestinationConfig().getBucketARN());
		assertNull(rule.getFilter());
	}

	@Test
	@Tag("Check")
	public void testReplicationMultipleRules() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 12);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationConfig()
				.addRule("rule-1", replicationRule(targetBucketARN).withPriority(1)
						.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("a/"))))
				.addRule("rule-2", replicationRule(targetBucketARN).withPriority(2)
						.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("b/")))));

		var rules = client.getBucketReplicationConfiguration(sourceBucketName).getRules();
		assertEquals(2, rules.size());
		assertEquals(1, rules.get("rule-1").getPriority());
		assertEquals("a/", ((ReplicationPrefixPredicate) rules.get("rule-1").getFilter().getPredicate()).getPrefix());
		assertEquals(2, rules.get("rule-2").getPriority());
		assertEquals("b/", ((ReplicationPrefixPredicate) rules.get("rule-2").getFilter().getPredicate()).getPrefix());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationDuplicatePriority() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 13);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];
		var config = replicationConfig()
				.addRule("rule-1", replicationRule(targetBucketARN).withPriority(1)
						.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("a/"))))
				.addRule("rule-2", replicationRule(targetBucketARN).withPriority(1)
						.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("b/"))));

		var e = assertThrows(AmazonServiceException.class, () -> putReplication(client, sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationFilterWithoutPriority() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 15);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// Filter 를 쓰는 V2 스키마에서는 Priority 와 DeleteMarkerReplication 이 필수다
		var withoutPriority = replicationConfig().addRule("rule1", new ReplicationRule().withStatus("Enabled")
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("test/")))
				.withDestinationConfig(new ReplicationDestinationConfig().withBucketARN(targetBucketARN))
				.withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Disabled")));
		var e = assertThrows(AmazonServiceException.class,
				() -> putReplication(client, sourceBucketName, withoutPriority));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());

		var withoutDeleteMarker = replicationConfig().addRule("rule1", new ReplicationRule().withStatus("Enabled")
				.withPriority(1)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("test/")))
				.withDestinationConfig(new ReplicationDestinationConfig().withBucketARN(targetBucketARN)));
		e = assertThrows(AmazonServiceException.class,
				() -> putReplication(client, sourceBucketName, withoutDeleteMarker));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("DeleteMarker")
	public void testReplicationDeleteMarkerEnabled() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 16);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Enabled"))));

		var rule = getReplicationRule(client, sourceBucketName, "rule1");
		assertEquals("Enabled", rule.getDeleteMarkerReplication().getStatus());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationDeleteMarkerWithTagFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 17);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 태그 필터가 있으면 삭제 마커 복제를 켤 수 없다
		var config = replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withFilter(new ReplicationFilter(new ReplicationTagPredicate(new com.amazonaws.services.s3.model.Tag("key1", "value1"))))
				.withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Enabled")));
		var e = assertThrows(AmazonServiceException.class, () -> putReplication(client, sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testReplicationDisabledRule() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 18);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName,
				replicationConfig().addRule("rule1", replicationRule(targetBucketARN).withStatus("Disabled")));

		var rule = getReplicationRule(client, sourceBucketName, "rule1");
		assertEquals("Disabled", rule.getStatus());
	}

	@Test
	@Tag("Check")
	public void testReplicationStorageClass() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 19);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationConfig().addRule("rule1", replicationRule(targetBucketARN)
				.withDestinationConfig(new ReplicationDestinationConfig().withBucketARN(targetBucketARN)
						.withStorageClass("STANDARD_IA"))));

		var destination = getReplicationRule(client, sourceBucketName, "rule1").getDestinationConfig();
		assertEquals(targetBucketARN, destination.getBucketARN());
		assertEquals("STANDARD_IA", destination.getStorageClass());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationSameBucket() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 20);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		var config = replicationConfig().addRule("rule1", replicationRule("arn:aws:s3:::" + sourceBucketName));

		var e = assertThrows(AmazonServiceException.class, () -> putReplication(client, sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidDestinationArn() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 21);
		var targetBucketName = createBucket(client, 21);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningConfiguration.ENABLED);
		var config = replicationConfig().addRule("rule1", replicationRule(targetBucketName));

		// 대상 버킷은 ARN 형식이어야 한다
		var e = assertThrows(AmazonServiceException.class, () -> putReplication(client, sourceBucketName, config));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(22);

		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketReplicationConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.deleteBucketReplicationConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testReplicationAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var buckets = setupReplicationBuckets(client, 23);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];
		var config = replicationConfig().addRule("rule1", replicationRule(targetBucketARN));
		putReplication(client, sourceBucketName, config);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getBucketReplicationConfiguration(sourceBucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> putReplication(altClient, sourceBucketName, config));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.deleteBucketReplicationConfiguration(sourceBucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	/** 복제 역할 ARN. 계정 아이디는 설정의 AccountId(없으면 UserID)를 쓴다. */
	private String replicationRole() {
		return "arn:aws:iam::" + config.mainUser.getExpectedOwnerId() + ":role/replication";
	}

	/** 버저닝이 켜진 원본·대상 버킷을 만들고 {원본 버킷명, 대상 버킷 ARN}을 돌려준다. */
	private String[] setupReplicationBuckets(AmazonS3 client, int testId) {
		var sourceBucketName = createBucket(client, testId);
		var targetBucketName = createBucket(client, testId);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningConfiguration.ENABLED);
		return new String[] { sourceBucketName, "arn:aws:s3:::" + targetBucketName };
	}

	/** 기본 복제 규칙(Enabled, Priority 1, 접두어 test/, 삭제 마커 복제 Disabled). 필요한 항목만 덮어쓴다. */
	private static ReplicationRule replicationRule(String targetBucketARN) {
		return new ReplicationRule().withStatus("Enabled").withPriority(1)
				.withFilter(new ReplicationFilter(new ReplicationPrefixPredicate("test/")))
				.withDestinationConfig(new ReplicationDestinationConfig().withBucketARN(targetBucketARN))
				.withDeleteMarkerReplication(new DeleteMarkerReplication().withStatus("Disabled"));
	}

	private BucketReplicationConfiguration replicationConfig() {
		return new BucketReplicationConfiguration().withRoleARN(replicationRole());
	}

	private static void putReplication(AmazonS3 client, String sourceBucketName,
			BucketReplicationConfiguration config) {
		client.setBucketReplicationConfiguration(sourceBucketName, config);
	}

	private static ReplicationRule getReplicationRule(AmazonS3 client, String sourceBucketName, String ruleId) {
		return client.getBucketReplicationConfiguration(sourceBucketName).getRule(ruleId);
	}
}
