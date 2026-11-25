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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteMarkerReplication;
import software.amazon.awssdk.services.s3.model.Destination;
import software.amazon.awssdk.services.s3.model.ReplicationConfiguration;
import software.amazon.awssdk.services.s3.model.ReplicationRule;
import software.amazon.awssdk.services.s3.model.ReplicationRuleFilter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Replication extends TestBase {
	@Test
	@Tag("Check")
	public void testReplicationSet() {
		var prefix = "test/";
		var client = getClient();
		var sourceBucketName = createBucket(client, 1);
		var targetBucketName = createBucket(client, 1);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role(replicationRole())
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
		var sourceBucketName = getNewBucketNameOnly(2);
		var targetBucketName = getNewBucketNameOnly(2);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role(replicationRole())
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
		var sourceBucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role(replicationRole())
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
		var sourceBucketName = createBucket(client, 4);
		var targetBucketName = getNewBucketNameOnly(4);

		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role(replicationRole())
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
		var sourceBucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);

		checkConfigureVersioningRetry(
				sourceBucketName,
				BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		var config = ReplicationConfiguration.builder().role(replicationRole())
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
		var sourceBucketName = createBucket(client, 6);
		var targetBucketName = createBucket(client, 6);

		// 원본, 대상 버킷 버저닝 설정
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		String targetBucketARN = "arn:aws:s3:::" + targetBucketName;

		// 원본 버킷 복제 설정
		var config = ReplicationConfiguration.builder().role(replicationRole())
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

	@Test
	@Tag("Check")
	public void testReplicationGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketReplication(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.REPLICATION_CONFIGURATION_NOT_FOUND_ERROR, e.awsErrorDetails().errorCode());

		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketReplication(d -> d.bucket(bucketName));
	}

	@Test
	@Tag("Filter")
	public void testReplicationTagFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 8);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationRule(targetBucketARN)
				.filter(f -> f.tag(t -> t.key("key1").value("value1"))).build());

		var filter = getReplicationRules(client, sourceBucketName).get(0).filter();
		assertEquals("key1", filter.tag().key());
		assertEquals("value1", filter.tag().value());
		assertNull(filter.prefix());
		assertNull(filter.and());
	}

	@Test
	@Tag("Filter")
	public void testReplicationAndFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 9);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];
		var tags = List.of(software.amazon.awssdk.services.s3.model.Tag.builder().key("key1").value("value1").build(),
				software.amazon.awssdk.services.s3.model.Tag.builder().key("key2").value("value2").build());

		putReplication(client, sourceBucketName, replicationRule(targetBucketARN)
				.filter(f -> f.and(a -> a.prefix("test/").tags(tags))).build());

		var filter = getReplicationRules(client, sourceBucketName).get(0).filter();
		assertEquals("test/", filter.and().prefix());
		assertEquals(tags, filter.and().tags());
		assertNull(filter.prefix());
		assertNull(filter.tag());
	}

	@Test
	@Tag("Filter")
	public void testReplicationEmptyFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 10);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 빈 접두어는 버킷 전체를 대상으로 한다
		putReplication(client, sourceBucketName, replicationRule(targetBucketARN).filter(f -> f.prefix("")).build());

		var filter = getReplicationRules(client, sourceBucketName).get(0).filter();
		assertNotNull(filter);
		assertTrue(filter.prefix() == null || filter.prefix().isEmpty());
		assertNull(filter.tag());
		assertNull(filter.and());
	}

	@Test
	@Tag("Filter")
	public void testReplicationV1Schema() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 11);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 구버전(V1) 스키마: Filter/Priority/DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다
		putReplication(client, sourceBucketName, ReplicationRule.builder().id("v1-rule").status("Enabled")
				.prefix("v1/").destination(d -> d.bucket(targetBucketARN)).build());

		var rule = getReplicationRules(client, sourceBucketName).get(0);
		assertEquals("v1-rule", rule.id());
		assertEquals("v1/", rule.prefix());
		assertEquals("Enabled", rule.statusAsString());
		assertEquals(targetBucketARN, rule.destination().bucket());
		assertNull(rule.filter());
	}

	@Test
	@Tag("Check")
	public void testReplicationMultipleRules() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 12);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).id("rule-1").priority(1).filter(f -> f.prefix("a/")).build(),
				replicationRule(targetBucketARN).id("rule-2").priority(2).filter(f -> f.prefix("b/")).build());

		var rules = getReplicationRules(client, sourceBucketName);
		assertEquals(2, rules.size());
		assertEquals("rule-1", rules.get(0).id());
		assertEquals(1, rules.get(0).priority());
		assertEquals("a/", rules.get(0).filter().prefix());
		assertEquals("rule-2", rules.get(1).id());
		assertEquals(2, rules.get(1).priority());
		assertEquals("b/", rules.get(1).filter().prefix());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationDuplicatePriority() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 13);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		var e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).id("rule-1").priority(1).filter(f -> f.prefix("a/")).build(),
				replicationRule(targetBucketARN).id("rule-2").priority(1).filter(f -> f.prefix("b/")).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationDuplicateId() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 14);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		var e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).id("rule-1").priority(1).filter(f -> f.prefix("a/")).build(),
				replicationRule(targetBucketARN).id("rule-1").priority(2).filter(f -> f.prefix("b/")).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// KSAN 알려진 실패(일부러 다르게 동작, 당분간 유지): Filter 가 있는데 Priority 나 DeleteMarkerReplication 이 없어도 받는다.
	// 이전부터 받아 온 형태라 검증을 넣으면 기존 설정이 거부될 수 있어 일단 넣지 않는다. AWS 는 400 InvalidRequest 다.
	public void testReplicationFilterWithoutPriority() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 15);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// Filter 를 쓰는 V2 스키마에서는 Priority 와 DeleteMarkerReplication 이 필수다
		var e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).priority(null).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).deleteMarkerReplication((DeleteMarkerReplication) null).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("DeleteMarker")
	public void testReplicationDeleteMarkerEnabled() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 16);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).deleteMarkerReplication(d -> d.status("Enabled")).build());

		var rules = getReplicationRules(client, sourceBucketName);
		assertEquals("Enabled", rules.get(0).deleteMarkerReplication().statusAsString());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationDeleteMarkerWithTagFilter() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 17);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		// 태그 필터가 있으면 삭제 마커 복제를 켤 수 없다
		var e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule(targetBucketARN).filter(f -> f.tag(t -> t.key("key1").value("value1")))
						.deleteMarkerReplication(d -> d.status("Enabled")).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testReplicationDisabledRule() {
		var client = getClient();
		var buckets = setupReplicationBuckets(client, 18);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];

		putReplication(client, sourceBucketName, replicationRule(targetBucketARN).status("Disabled").build());

		var rules = getReplicationRules(client, sourceBucketName);
		assertEquals("Disabled", rules.get(0).statusAsString());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationSameBucket() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 20);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);

		var e = assertThrows(AwsServiceException.class, () -> putReplication(client, sourceBucketName,
				replicationRule("arn:aws:s3:::" + sourceBucketName).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationInvalidDestinationArn() {
		var client = getClient();
		var sourceBucketName = createBucket(client, 21);
		var targetBucketName = createBucket(client, 21);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		// 대상 버킷은 ARN 형식이어야 한다
		var e = assertThrows(AwsServiceException.class,
				() -> putReplication(client, sourceBucketName, replicationRule(targetBucketName).build()));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testReplicationBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(22);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketReplication(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.deleteBucketReplication(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testReplicationAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var buckets = setupReplicationBuckets(client, 23);
		var sourceBucketName = buckets[0];
		var targetBucketARN = buckets[1];
		var rule = replicationRule(targetBucketARN).build();
		putReplication(client, sourceBucketName, rule);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getBucketReplication(g -> g.bucket(sourceBucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> putReplication(altClient, sourceBucketName, rule));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.deleteBucketReplication(d -> d.bucket(sourceBucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	/** 복제 역할 ARN. 계정 아이디는 설정의 AccountId(없으면 UserID)를 쓴다. */
	private String replicationRole() {
		return "arn:aws:iam::" + config.mainUser.getExpectedOwnerId() + ":role/replication";
	}

	/** 버저닝이 켜진 원본·대상 버킷을 만들고 {원본 버킷명, 대상 버킷 ARN}을 돌려준다. */
	private String[] setupReplicationBuckets(S3Client client, int testId) {
		var sourceBucketName = createBucket(client, testId);
		var targetBucketName = createBucket(client, testId);
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);
		return new String[] { sourceBucketName, "arn:aws:s3:::" + targetBucketName };
	}

	/** 기본 복제 규칙(Enabled, Priority 1, 접두어 test/, 삭제 마커 복제 Disabled). 필요한 항목만 덮어쓴다. */
	private static ReplicationRule.Builder replicationRule(String targetBucketARN) {
		return ReplicationRule.builder().status("Enabled").priority(1)
				.filter(f -> f.prefix("test/"))
				.destination(d -> d.bucket(targetBucketARN))
				.deleteMarkerReplication(d -> d.status("Disabled"));
	}

	private void putReplication(S3Client client, String sourceBucketName, ReplicationRule... rules) {
		client.putBucketReplication(p -> p.bucket(sourceBucketName)
				.replicationConfiguration(c -> c.role(replicationRole()).rules(rules)));
	}

	private static List<ReplicationRule> getReplicationRules(S3Client client, String sourceBucketName) {
		return client.getBucketReplication(g -> g.bucket(sourceBucketName)).replicationConfiguration().rules();
	}
}
