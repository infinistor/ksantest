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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;

public class LifeCycle extends TestBase {
	@Test
	@Tag("Check")
	public void testLifecycleSet() {
		var client = getClient();
		var bucketName = createBucket(client, 1);
		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(1))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().id("rule2").expiration(e -> e.days(2))
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.DISABLED).build());

		client.putBucketLifecycleConfiguration(
				p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("Get")
	public void testLifecycleGet() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(31))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().id("rule2").expiration(e -> e.days(120))
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
		var response = client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName));
		prefixLifecycleConfigurationCheck(rules, response.rules());
	}

	@Test
	@Tag("Check")
	public void testLifecycleGetNoId() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().expiration(e -> e.days(31))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().expiration(e -> e.days(120))
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
		var response = client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName));
		var getRules = response.rules();

		for (int i = 0; i < rules.size(); i++) {
			assertNotNull(getRules.get(i).id());
			assertEquals(rules.get(i).expiration().date(), getRules.get(i).expiration().date());
			assertEquals(rules.get(i).expiration().days(), getRules.get(i).expiration().days());
			assertEquals(rules.get(i).filter().prefix(), getRules.get(i).filter().prefix());
			assertEquals(rules.get(i).status(), getRules.get(i).status());
		}
	}

	@Test
	@Tag("Version")
	public void testLifecycleExpirationVersioningEnabled() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var key = "test1/a";
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		createMultipleVersions(client, bucketName, key, 1, true);
		client.deleteObject(d -> d.bucket(bucketName).key(key));

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(1))
				.filter(f -> f.prefix("expire1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = response.versions();
		var deleteMarkers = response.deleteMarkers();
		assertEquals(1, versions.size());
		assertEquals(1, deleteMarkers.size());
	}

	@Test
	@Tag("Check")
	public void testLifecycleIdTooLong() {
		var client = getClient();
		var bucketName = createBucket(client, 5);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id(Utils.randomTextToLong(256))
				.expiration(e -> e.days(2))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketLifecycleConfiguration(
						p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Duplicate")
	public void testLifecycleSameId() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(1))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(2))
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.DISABLED).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketLifecycleConfiguration(
						p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleInvalidStatus() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(2))
				.filter(f -> f.prefix("test1/"))
				.status("invalid").build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketLifecycleConfiguration(
						p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Date")
	public void testLifecycleSetDate() {
		var client = getClient();
		var bucketName = createBucket(client, 8);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder()
				.id("rule1")
				.expiration(e -> e
						.date(new Calendar.Builder()
								.setDate(2099, 10, 10)
								.setTimeZone(TimeZone.getTimeZone("GMT"))
								.build()
								.toInstant()))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleSetInvalidDate() {
		var client = getClient();
		var bucketName = createBucket(client, 9);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder()
				.id("rule1")
				.expiration(e -> e
						.date(new Calendar.Builder()
								.setDate(2099, 10, 10)
								.build()
								.toInstant()))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketLifecycleConfiguration(
						p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Version")
	public void testLifecycleSetNoncurrent() {
		var client = getClient();
		var bucketName = createObjects(client, 10, "past/foo", "future/bar");

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.noncurrentVersionExpiration(e -> e.noncurrentDays(2))
				.filter(f -> f.prefix("past/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().id("rule2")
				.noncurrentVersionExpiration(e -> e.noncurrentDays(3))
				.filter(f -> f.prefix("future/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("Version")
	public void testLifecycleNoncurrentExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 11);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		createMultipleVersions(client, bucketName, "test1/a", 3, true);
		createMultipleVersions(client, bucketName, "test2/abc", 3, false);

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = response.versions();

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.noncurrentVersionExpiration(e -> e.noncurrentDays(2))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));

		assertEquals(6, versions.size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testLifecycleSetDeleteMarker() {
		var client = getClient();
		var bucketName = createBucket(client, 12);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.expiredObjectDeleteMarker(true))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));

	}

	@Test
	@Tag("Filter")
	public void testLifecycleSetFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.expiration(e -> e.expiredObjectDeleteMarker(true))
				.filter(f -> f.prefix("foo"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(
				p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("Filter")
	public void testLifecycleSetEmptyFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 14);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.expiration(e -> e.expiredObjectDeleteMarker(true))
				.filter(f -> f.prefix(""))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("DeleteMarker")
	public void testLifecycleDeleteMarkerExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		createMultipleVersions(client, bucketName, "test1/a", 1, true);
		createMultipleVersions(client, bucketName, "test2/abc", 1, false);
		client.deleteObject(d -> d.bucket(bucketName).key("test1/a"));
		client.deleteObject(d -> d.bucket(bucketName).key("test2/abc"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, response.versions().size());
		assertEquals(2, response.deleteMarkers().size());

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.noncurrentVersionExpiration(e -> e.noncurrentDays(1))
				.expiration(e -> e.expiredObjectDeleteMarker(true))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	@Test
	@Tag("Multipart")
	public void testLifecycleSetMultipart() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED)
				.abortIncompleteMultipartUpload(a -> a.daysAfterInitiation(2)).build());
		rules.add(LifecycleRule.builder().id("rule2")
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.ENABLED)
				.abortIncompleteMultipartUpload(a -> a.daysAfterInitiation(3)).build());

		client.putBucketLifecycleConfiguration(
				p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));

	}

	@Test
	@Tag("Multipart")
	public void testLifecycleMultipartExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		var keyNames = List.of("test1/a", "test2/b");

		for (var Key : keyNames) {
			client.createMultipartUpload(b -> b.bucket(bucketName).key(Key));
		}

		var listResponse = client.listMultipartUploads(l -> l.bucket(bucketName));
		var uploads = listResponse.uploads();

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED)
				.abortIncompleteMultipartUpload(a -> a.daysAfterInitiation(2)).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
		assertEquals(2, uploads.size());

		for (var upload : uploads) {
			client.abortMultipartUpload(a -> a.bucket(bucketName).key(upload.key()).uploadId(upload.uploadId()));
		}
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 18);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(1))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());
		rules.add(LifecycleRule.builder().id("rule2").expiration(e -> e.days(2))
				.filter(f -> f.prefix("test2/"))
				.status(ExpirationStatus.DISABLED).build());

		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
		client.deleteBucketLifecycle(d -> d.bucket(bucketName));
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleSetExpirationZero() {
		var client = getClient();
		var bucketName = createBucket(client, 19);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1").expiration(e -> e.days(0))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketLifecycleConfiguration(
						p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Disabled("Java SDK V2에서는 expires값을 재대로 가져오지 못함")
	@Test
	@Tag("metadata")
	public void testLifecycleSetExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		var rules = new ArrayList<LifecycleRule>();
		rules.add(LifecycleRule.builder().id("rule1")
				.expiration(e -> e.days(1))
				.filter(f -> f.prefix("test1/"))
				.status(ExpirationStatus.ENABLED).build());

		client.putBucketLifecycleConfiguration(
				p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));

		var key = "test1/a";
		var content = "test";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		var expiredTime = getExpiredDate(response.lastModified(), 1);
		assertEquals(expiredTime, response.expiresString());

		var response2 = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(expiredTime, response2.response().expiresString());
	}

	/** ID rule1, 전체 대상(Prefix ""), Enabled 인 규칙. 필요한 항목을 덧붙여 쓴다. */
	private static LifecycleRule.Builder rule() {
		return LifecycleRule.builder().id("rule1").status(ExpirationStatus.ENABLED).filter(f -> f.prefix(""));
	}

	private static void putRules(S3Client client, String bucketName, List<LifecycleRule> rules) {
		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName).lifecycleConfiguration(c -> c.rules(rules)));
	}

	private static void putRules(S3Client client, String bucketName, LifecycleRule rule) {
		putRules(client, bucketName, List.of(rule));
	}

	private static void assertPutRulesError(S3Client client, String bucketName, List<LifecycleRule> rules,
			int statusCode, String errorCode) {
		var e = assertThrows(AwsServiceException.class, () -> putRules(client, bucketName, rules));
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	private static void assertPutRulesError(S3Client client, String bucketName, LifecycleRule rule, int statusCode,
			String errorCode) {
		assertPutRulesError(client, bucketName, List.of(rule), statusCode, errorCode);
	}

	private static List<LifecycleRule> getRules(S3Client client, String bucketName) {
		return client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName)).rules();
	}

	private static software.amazon.awssdk.services.s3.model.Tag tag(String key, String value) {
		return software.amazon.awssdk.services.s3.model.Tag.builder().key(key).value(value).build();
	}

	@Test
	@Tag("Get")
	public void testLifecycleGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 21);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_LIFECYCLE_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		// 설정이 없어도 삭제는 성공한다
		var response = client.deleteBucketLifecycle(d -> d.bucket(bucketName));
		assertEquals(HttpStatus.SC_NO_CONTENT, response.sdkHttpResponse().statusCode());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(23);
		var rule = rule().expiration(e -> e.days(1)).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		assertPutRulesError(client, bucketName, rule, HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);

		e = assertThrows(AwsServiceException.class, () -> client.deleteBucketLifecycle(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 24);
		var rule = rule().expiration(e -> e.days(1)).build();
		putRules(client, bucketName, rule);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getBucketLifecycleConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		assertPutRulesError(altClient, bucketName, rule, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);

		e = assertThrows(AwsServiceException.class, () -> altClient.deleteBucketLifecycle(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		// 설정은 그대로 남아 있다
		assertEquals(1, getRules(client, bucketName).size());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleNoAction() {
		var client = getClient();
		var bucketName = createBucket(client, 25);

		assertPutRulesError(client, bucketName, rule().build(), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleNoFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 26);

		// Filter 도 Prefix 도 없는 규칙은 거부된다
		var rule = LifecycleRule.builder().id("rule1").status(ExpirationStatus.ENABLED).expiration(e -> e.days(1))
				.build();
		assertPutRulesError(client, bucketName, rule, HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleEmptyRules() {
		var client = getClient();
		var bucketName = createBucket(client, 27);

		assertPutRulesError(client, bucketName, new ArrayList<LifecycleRule>(), HttpStatus.SC_BAD_REQUEST,
				MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDaysAndDate() {
		var client = getClient();
		var bucketName = createBucket(client, 28);

		var rule = rule().expiration(e -> e.days(1).date(Instant.parse("2099-11-10T00:00:00Z"))).build();
		assertPutRulesError(client, bucketName, rule, HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleNegativeDays() {
		var client = getClient();
		var bucketName = createBucket(client, 29);

		var rule = rule().expiration(e -> e.days(-1)).build();
		assertPutRulesError(client, bucketName, rule, HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Transition")
	public void testLifecycleTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 30);

		putRules(client, bucketName, rule()
				.transitions(t -> t.days(30).storageClass("STANDARD_IA"), t -> t.days(60).storageClass("GLACIER"))
				.expiration(e -> e.days(90)).build());

		var rules = getRules(client, bucketName);
		assertEquals(1, rules.size());
		var transitions = new ArrayList<>(rules.get(0).transitions());
		transitions.sort((a, b) -> a.days().compareTo(b.days()));
		assertEquals(2, transitions.size());
		assertEquals(30, transitions.get(0).days().intValue());
		assertEquals("STANDARD_IA", transitions.get(0).storageClassAsString());
		assertEquals(60, transitions.get(1).days().intValue());
		assertEquals("GLACIER", transitions.get(1).storageClassAsString());
		assertEquals(90, rules.get(0).expiration().days().intValue());
	}

	@Test
	@Tag("Transition")
	public void testLifecycleTransitionInvalidStorageClass() {
		var client = getClient();
		var bucketName = createBucket(client, 31);

		// STANDARD 로는 전환할 수 없다
		assertPutRulesError(client, bucketName,
				rule().transitions(t -> t.days(30).storageClass("STANDARD")).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		// 없는 스토리지 클래스
		assertPutRulesError(client, bucketName,
				rule().transitions(t -> t.days(30).storageClass("INVALID")).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Transition")
	public void testLifecycleExpirationBeforeTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 32);

		// 만료일은 전환일보다 커야 한다
		assertPutRulesError(client, bucketName,
				rule().transitions(t -> t.days(30).storageClass("STANDARD_IA")).expiration(e -> e.days(30)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		putRules(client, bucketName,
				rule().transitions(t -> t.days(30).storageClass("STANDARD_IA")).expiration(e -> e.days(31)).build());
	}

	@Test
	@Tag("Transition")
	public void testLifecycleNoncurrentTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 34);

		putRules(client, bucketName,
				rule().noncurrentVersionTransitions(t -> t.noncurrentDays(30).storageClass("STANDARD_IA")).build());

		var transitions = getRules(client, bucketName).get(0).noncurrentVersionTransitions();
		assertEquals(1, transitions.size());
		assertEquals(30, transitions.get(0).noncurrentDays().intValue());
		assertEquals("STANDARD_IA", transitions.get(0).storageClassAsString());
	}

	@Test
	@Tag("Version")
	public void testLifecycleNoncurrentZeroDays() {
		var client = getClient();
		var bucketName = createBucket(client, 35);

		assertPutRulesError(client, bucketName,
				rule().noncurrentVersionExpiration(e -> e.noncurrentDays(0)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Version")
	public void testLifecycleNewerNoncurrentVersions() {
		var client = getClient();
		var bucketName = createBucket(client, 36);

		putRules(client, bucketName,
				rule().noncurrentVersionExpiration(e -> e.noncurrentDays(1).newerNoncurrentVersions(5)).build());

		var expiration = getRules(client, bucketName).get(0).noncurrentVersionExpiration();
		assertEquals(1, expiration.noncurrentDays().intValue());
		assertEquals(5, expiration.newerNoncurrentVersions().intValue());

		// 보존할 버전 수는 1~100 만 허용한다
		for (var count : List.of(0, 101)) {
			assertPutRulesError(client, bucketName,
					rule().noncurrentVersionExpiration(e -> e.noncurrentDays(1).newerNoncurrentVersions(count))
							.build(),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		}
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDeleteMarkerWithDays() {
		var client = getClient();
		var bucketName = createBucket(client, 37);

		// ExpiredObjectDeleteMarker 는 Days 와 함께 쓸 수 없다
		assertPutRulesError(client, bucketName,
				rule().expiration(e -> e.days(1).expiredObjectDeleteMarker(true)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDeleteMarkerWithTag() {
		var client = getClient();
		var bucketName = createBucket(client, 38);

		assertPutRulesError(client, bucketName,
				rule().filter(f -> f.tag(tag("key", "value"))).expiration(e -> e.expiredObjectDeleteMarker(true))
						.build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAbortMultipartWithFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 39);

		// AbortIncompleteMultipartUpload 는 태그·크기 필터와 함께 쓸 수 없다
		var filters = List.of(
				LifecycleRuleFilter.builder().tag(tag("key", "value")).build(),
				LifecycleRuleFilter.builder().objectSizeGreaterThan(10L).build());
		for (var filter : filters) {
			assertPutRulesError(client, bucketName,
					rule().filter(filter).abortIncompleteMultipartUpload(a -> a.daysAfterInitiation(1)).build(),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
		}
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAbortMultipartZeroDays() {
		var client = getClient();
		var bucketName = createBucket(client, 40);

		assertPutRulesError(client, bucketName,
				rule().abortIncompleteMultipartUpload(a -> a.daysAfterInitiation(0)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleTagFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 41);

		putRules(client, bucketName,
				rule().filter(f -> f.tag(tag("key", "value"))).expiration(e -> e.days(1)).build());

		var filter = getRules(client, bucketName).get(0).filter();
		assertEquals("key", filter.tag().key());
		assertEquals("value", filter.tag().value());
		assertNull(filter.prefix());
		assertNull(filter.and());
	}

	@Test
	@Tag("Filter")
	public void testLifecycleAndFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 42);
		var tags = List.of(tag("key1", "value1"), tag("key2", "value2"));

		putRules(client, bucketName, rule()
				.filter(f -> f.and(a -> a.prefix("test1/").tags(tags).objectSizeGreaterThan(10L)
						.objectSizeLessThan(100L)))
				.expiration(e -> e.days(1)).build());

		var actual = getRules(client, bucketName).get(0).filter().and();
		assertEquals("test1/", actual.prefix());
		tagCompare(tags, actual.tags());
		assertEquals(10L, actual.objectSizeGreaterThan().longValue());
		assertEquals(100L, actual.objectSizeLessThan().longValue());
	}

	@Test
	@Tag("Filter")
	public void testLifecycleAndFilterInvalid() {
		var client = getClient();
		var bucketName = createBucket(client, 43);

		// And 안에 조건이 하나뿐이면 거부된다
		assertPutRulesError(client, bucketName,
				rule().filter(f -> f.and(a -> a.prefix("test1/"))).expiration(e -> e.days(1)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		// And 안의 태그 키는 중복될 수 없다
		assertPutRulesError(client, bucketName,
				rule().filter(f -> f.and(a -> a.prefix("test1/").tags(tag("key", "1"), tag("key", "2"))))
						.expiration(e -> e.days(1)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleObjectSizeFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 44);

		putRules(client, bucketName,
				rule().filter(f -> f.objectSizeGreaterThan(500L)).expiration(e -> e.days(1)).build());
		var filter = getRules(client, bucketName).get(0).filter();
		assertEquals(500L, filter.objectSizeGreaterThan().longValue());
		assertNull(filter.objectSizeLessThan());
		assertNull(filter.prefix());

		putRules(client, bucketName,
				rule().filter(f -> f.objectSizeLessThan(500L)).expiration(e -> e.days(1)).build());
		filter = getRules(client, bucketName).get(0).filter();
		assertEquals(500L, filter.objectSizeLessThan().longValue());
		assertNull(filter.objectSizeGreaterThan());
		assertNull(filter.prefix());
	}

	@Test
	@Tag("Filter")
	public void testLifecycleObjectSizeFilterInvalid() {
		var client = getClient();
		var bucketName = createBucket(client, 45);

		// ObjectSizeLessThan 은 1 이상이어야 한다
		assertPutRulesError(client, bucketName,
				rule().filter(f -> f.objectSizeLessThan(0L)).expiration(e -> e.days(1)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
		// ObjectSizeLessThan 은 ObjectSizeGreaterThan 보다 커야 한다
		assertPutRulesError(client, bucketName,
				rule().filter(f -> f.and(a -> a.objectSizeGreaterThan(100L).objectSizeLessThan(100L)))
						.expiration(e -> e.days(1)).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("Filter")
	@SuppressWarnings("deprecation")
	public void testLifecycleLegacyPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 46);

		// Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식도 그대로 저장·조회된다
		var legacy = LifecycleRule.builder().id("rule1").status(ExpirationStatus.ENABLED).prefix("legacy/")
				.expiration(e -> e.days(1));
		putRules(client, bucketName, legacy.build());
		var rules = getRules(client, bucketName);
		assertEquals("legacy/", rules.get(0).prefix());
		assertNull(rules.get(0).filter());

		// Prefix 와 Filter 를 함께 줄 수는 없다
		assertPutRulesError(client, bucketName, legacy.filter(f -> f.prefix("test1/")).build(),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	private static List<LifecycleRule> makeRules(int count) {
		var rules = new ArrayList<LifecycleRule>();
		for (int i = 0; i < count; i++) {
			var prefix = "test" + i + "/";
			rules.add(LifecycleRule.builder().id("rule" + i).status(ExpirationStatus.ENABLED)
					.filter(f -> f.prefix(prefix)).expiration(e -> e.days(1)).build());
		}
		return rules;
	}

	@Test
	@Tag("Max")
	public void testLifecycleMaxRules() {
		var client = getClient();
		var bucketName = createBucket(client, 47);

		// 규칙은 1000개까지 넣을 수 있다
		putRules(client, bucketName, makeRules(1000));
		assertEquals(1000, getRules(client, bucketName).size());
		assertPutRulesError(client, bucketName, makeRules(1001), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("metadata")
	public void testLifecycleExpirationHeader() {
		var client = getClient();
		var bucketName = createBucket(client, 49);
		putRules(client, bucketName, LifecycleRule.builder().id("rule1").status(ExpirationStatus.ENABLED)
				.filter(f -> f.prefix("test1/")).expiration(e -> e.days(1)).build());

		// 만료 규칙에 걸리는 오브젝트는 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려준다
		var key = "test1/testLifecycleExpirationHeader";
		var ruleId = "rule-id=\"rule1\"";
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("test"));
		assertTrue(putResponse.expiration().contains(ruleId));
		assertTrue(putResponse.expiration().contains("expiry-date="));
		assertTrue(client.headObject(h -> h.bucket(bucketName).key(key)).expiration().contains(ruleId));
		try (var getResponse = client.getObject(g -> g.bucket(bucketName).key(key))) {
			assertTrue(getResponse.response().expiration().contains(ruleId));
		} catch (java.io.IOException e) {
			throw new java.io.UncheckedIOException(e);
		}

		// 규칙에 걸리지 않는 오브젝트에는 헤더가 없다
		var other = "test2/testLifecycleExpirationHeader";
		assertNull(client.putObject(p -> p.bucket(bucketName).key(other), RequestBody.fromString("test"))
				.expiration());
		assertNull(client.headObject(h -> h.bucket(bucketName).key(other)).expiration());
	}
}
