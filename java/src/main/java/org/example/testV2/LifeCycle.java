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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import software.amazon.awssdk.services.s3.model.LifecycleRule;

public class LifeCycle extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("LifeCycle V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("LifeCycle V2 End");
	}

	@Test
	@Tag("Check")
	public void testLifecycleSet() {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createObjects(client, "past/foo", "future/bar");

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

		var keyNames = List.of("test1/a", "test2/b");
		var uploadIds = new ArrayList<String>();

		for (var Key : keyNames) {
			var response = client.createMultipartUpload(b -> b.bucket(bucketName).key(Key));
			uploadIds.add(response.uploadId());
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
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		assertEquals(expiredTime, response.expires());

		var response2 = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(expiredTime, response2.response().expires());
	}
}
