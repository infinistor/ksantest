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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortIncompleteMultipartUpload;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.NoncurrentVersionExpiration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.NoncurrentVersionTransition;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleAndOperator;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilterPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleObjectSizeGreaterThanPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleObjectSizeLessThanPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;

public class LifeCycle extends TestBase {
	@Test
	@Tag("Check")
	public void testLifecycleSet() {
		var client = getClient();
		var bucketName = createBucket(client, 1);
		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withId("rule2").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);

		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
	}

	@Test
	@Tag("Get")
	public void testLifecycleGet() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(31)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withId("rule2").withExpirationInDays(120)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);

		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
		var response = client.getBucketLifecycleConfiguration(bucketName);
		checkPrefixLifecycleConfiguration(rules, response.getRules());
	}

	@Test
	@Tag("Check")
	public void testLifecycleGetNoId() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withExpirationInDays(31)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withExpirationInDays(120)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);

		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
		var response = client.getBucketLifecycleConfiguration(bucketName);
		var getRules = response.getRules();

		for (int i = 0; i < rules.size(); i++) {
			assertNotNull(getRules.get(i).getId());
			assertEquals(rules.get(i).getExpirationDate(), getRules.get(i).getExpirationDate());
			assertEquals(rules.get(i).getExpirationInDays(), getRules.get(i).getExpirationInDays());
			assertEquals(((LifecyclePrefixPredicate) rules.get(i).getFilter().getPredicate()).getPrefix(),
					((LifecyclePrefixPredicate) getRules.get(i).getFilter().getPredicate()).getPrefix());
			assertEquals(rules.get(i).getStatus(), getRules.get(i).getStatus());
		}
	}

	@Test
	@Tag("Version")
	public void testLifecycleExpirationVersioningEnabled() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var key = "test1/a";
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		createMultipleVersions(client, bucketName, key, 1, true);
		client.deleteObject(bucketName, key);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("expire1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);

		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		var response = client.listVersions(bucketName, null);
		var versions = getVersions(response.getVersionSummaries());
		var deleteMarkers = getDeleteMarkers(response.getVersionSummaries());
		assertEquals(1, versions.size());
		assertEquals(1, deleteMarkers.size());
	}

	@Test
	@Tag("Check")
	public void testLifecycleIdTooLong() {
		var client = getClient();
		var bucketName = createBucket(client, 5);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId(Utils.randomTextToLong(256)).withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketLifecycleConfiguration(bucketName, myLifeCycle));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("Duplicate")
	public void testLifecycleSameId() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withId("rule1").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketLifecycleConfiguration(bucketName, myLifeCycle));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleInvalidStatus() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus("invalid"));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketLifecycleConfiguration(bucketName, myLifeCycle));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Date")
	public void testLifecycleSetDate() {
		var client = getClient();
		var bucketName = createBucket(client, 8);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withExpirationDate(new Calendar.Builder().setDate(2099, 10, 10)
						.setTimeZone(TimeZone.getTimeZone("GMT")).build().getTime())
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleSetInvalidDate() {
		var client = getClient();
		var bucketName = createBucket(client, 9);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withExpirationDate(new Calendar.Builder().setDate(2099, 10, 10).build().getTime())
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketLifecycleConfiguration(bucketName, myLifeCycle));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("Version")
	public void testLifecycleSetNoncurrent() {
		var client = getClient();
		var bucketName = createObjects(client, 10, "past/foo", "future/bar");

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(2))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("past/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withId("rule2")
				.withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(3))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("future/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
	}

	@Test
	@Tag("Version")
	public void testLifecycleNoncurrentExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 11);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		createMultipleVersions(client, bucketName, "test1/a", 3, true);
		createMultipleVersions(client, bucketName, "test2/abc", 3, false);

		var response = client.listVersions(bucketName, null);
		var initVersions = response.getVersionSummaries();

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(2))
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		assertEquals(6, initVersions.size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testLifecycleSetDeleteMarker() {
		var client = getClient();
		var bucketName = createBucket(client, 12);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

	}

	@Test
	@Tag("Filter")
	public void testLifecycleSetFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("foo")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

	}

	@Test
	@Tag("Filter")
	public void testLifecycleSetEmptyFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 14);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

	}

	@Test
	@Tag("DeleteMarker")
	public void testLifecycleDeleteMarkerExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		createMultipleVersions(client, bucketName, "test1/a", 1, true);
		createMultipleVersions(client, bucketName, "test2/abc", 1, false);
		client.deleteObject(bucketName, "test1/a");
		client.deleteObject(bucketName, "test2/abc");

		var response = client.listVersions(bucketName, null);
		var totalVersions = response.getVersionSummaries();

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(1))
				.withExpiredObjectDeleteMarker(true)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		assertEquals(4, totalVersions.size());
	}

	@Test
	@Tag("Multipart")
	public void testLifecycleSetMultipart() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(2)));
		rules.add(new Rule().withId("rule2")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(3)));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
	}

	@Test
	@Tag("Multipart")
	public void testLifecycleMultipartExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		var keyNames = List.of("test1/a", "test2/b");

		for (var key : keyNames) {
			client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		}

		var listResponse = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		var initUploads = listResponse.getMultipartUploads();

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED)
				.withAbortIncompleteMultipartUpload(new AbortIncompleteMultipartUpload().withDaysAfterInitiation(2)));

		var myLifeCycle = new BucketLifecycleConfiguration(
				rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		assertEquals(2, initUploads.size());

		for (var upload : initUploads) {
			client.abortMultipartUpload(
					new AbortMultipartUploadRequest(bucketName, upload.getKey(), upload.getUploadId()));
		}
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 18);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));
		rules.add(new Rule().withId("rule2").withExpirationInDays(2)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test2/")))
				.withStatus(BucketLifecycleConfiguration.DISABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);

		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);
		client.deleteBucketLifecycleConfiguration(bucketName);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleSetExpirationZero() {
		var client = getClient();
		var bucketName = createBucket(client, 19);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(0)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/")))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketLifecycleConfiguration(bucketName, myLifeCycle));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("metadata")
	public void testLifecycleSetExpiration() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId("rule1").withExpirationInDays(1)
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		var key = "test1/a";
		var content = "test";

		client.putObject(bucketName, key, content);

		var response = client.getObjectMetadata(bucketName, key);
		var expiredTime = getExpiredDate(response.getLastModified(), 1);
		assertEquals(expiredTime.getTime(), response.getExpirationTime().getTime());

		var response2 = client.getObject(bucketName, key);
		assertEquals(expiredTime.getTime(), response2.getObjectMetadata().getExpirationTime().getTime());
	}

	/** ID rule1, 전체 대상(Prefix ""), Enabled 인 규칙. 필요한 항목을 덧붙여 쓴다. */
	private static Rule rule() {
		return new Rule().withId("rule1").withStatus(BucketLifecycleConfiguration.ENABLED)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("")));
	}

	private static LifecycleFilter filter(LifecycleFilterPredicate predicate) {
		return new LifecycleFilter(predicate);
	}

	private static LifecycleFilter andFilter(LifecycleFilterPredicate... predicates) {
		return new LifecycleFilter(new LifecycleAndOperator(List.of(predicates)));
	}

	private static LifecycleTagPredicate tag(String key, String value) {
		return new LifecycleTagPredicate(new com.amazonaws.services.s3.model.Tag(key, value));
	}

	private static Transition transition(int days, String storageClass) {
		return new Transition().withDays(days).withStorageClass(storageClass);
	}

	private static void putRules(AmazonS3 client, String bucketName, List<Rule> rules) {
		client.setBucketLifecycleConfiguration(bucketName, new BucketLifecycleConfiguration(rules));
	}

	private static void putRules(AmazonS3 client, String bucketName, Rule rule) {
		putRules(client, bucketName, List.of(rule));
	}

	private static void assertPutRulesError(AmazonS3 client, String bucketName, List<Rule> rules, int statusCode,
			String errorCode) {
		var e = assertThrows(AmazonServiceException.class, () -> putRules(client, bucketName, rules));
		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	private static void assertPutRulesError(AmazonS3 client, String bucketName, Rule rule, int statusCode,
			String errorCode) {
		assertPutRulesError(client, bucketName, List.of(rule), statusCode, errorCode);
	}

	@Test
	@Tag("Get")
	public void testLifecycleGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 21);

		// SDK v1 은 404 응답을 null 로 돌려준다
		assertNull(client.getBucketLifecycleConfiguration(bucketName));
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketLifecycleConfiguration(bucketName);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(23);

		// SDK v1 의 조회는 404 를 null 로 바꾸므로 설정·삭제만 확인한다
		assertPutRulesError(client, bucketName, rule().withExpirationInDays(1), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketLifecycleConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 24);
		putRules(client, bucketName, rule().withExpirationInDays(1));

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getBucketLifecycleConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		assertPutRulesError(altClient, bucketName, rule().withExpirationInDays(1), HttpStatus.SC_FORBIDDEN,
				MainData.ACCESS_DENIED);

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.deleteBucketLifecycleConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		// 설정은 그대로 남아 있다
		assertEquals(1, client.getBucketLifecycleConfiguration(bucketName).getRules().size());
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleNoAction() {
		var client = getClient();
		var bucketName = createBucket(client, 25);

		assertPutRulesError(client, bucketName, rule(), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleEmptyRules() {
		var client = getClient();
		var bucketName = createBucket(client, 27);

		assertPutRulesError(client, bucketName, new ArrayList<Rule>(), HttpStatus.SC_BAD_REQUEST,
				MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDaysAndDate() {
		var client = getClient();
		var bucketName = createBucket(client, 28);

		var rule = rule().withExpirationInDays(1).withExpirationDate(new Calendar.Builder().setDate(2099, 10, 10)
				.setTimeZone(TimeZone.getTimeZone("GMT")).build().getTime());
		assertPutRulesError(client, bucketName, rule, HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Transition")
	public void testLifecycleTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 30);

		putRules(client, bucketName, rule().addTransition(transition(30, "STANDARD_IA"))
				.addTransition(transition(60, "GLACIER")).withExpirationInDays(90));

		var rules = client.getBucketLifecycleConfiguration(bucketName).getRules();
		assertEquals(1, rules.size());
		var transitions = new ArrayList<>(rules.get(0).getTransitions());
		transitions.sort((a, b) -> Integer.compare(a.getDays(), b.getDays()));
		assertEquals(2, transitions.size());
		assertEquals(30, transitions.get(0).getDays());
		assertEquals("STANDARD_IA", transitions.get(0).getStorageClassAsString());
		assertEquals(60, transitions.get(1).getDays());
		assertEquals("GLACIER", transitions.get(1).getStorageClassAsString());
		assertEquals(90, rules.get(0).getExpirationInDays());
	}

	@Test
	@Tag("Transition")
	public void testLifecycleTransitionInvalidStorageClass() {
		var client = getClient();
		var bucketName = createBucket(client, 31);

		// STANDARD 로는 전환할 수 없다
		assertPutRulesError(client, bucketName, rule().addTransition(transition(30, "STANDARD")),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		// 없는 스토리지 클래스
		assertPutRulesError(client, bucketName, rule().addTransition(transition(30, "INVALID")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Transition")
	public void testLifecycleExpirationBeforeTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 32);

		// 만료일은 전환일보다 커야 한다
		assertPutRulesError(client, bucketName,
				rule().addTransition(transition(30, "STANDARD_IA")).withExpirationInDays(30),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		putRules(client, bucketName, rule().addTransition(transition(30, "STANDARD_IA")).withExpirationInDays(31));
	}

	@Test
	@Tag("Transition")
	public void testLifecycleNoncurrentTransition() {
		var client = getClient();
		var bucketName = createBucket(client, 34);

		putRules(client, bucketName, rule().addNoncurrentVersionTransition(
				new NoncurrentVersionTransition().withDays(30).withStorageClass("STANDARD_IA")));

		var transitions = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0)
				.getNoncurrentVersionTransitions();
		assertEquals(1, transitions.size());
		assertEquals(30, transitions.get(0).getDays());
		assertEquals("STANDARD_IA", transitions.get(0).getStorageClassAsString());
	}

	@Test
	@Tag("Version")
	public void testLifecycleNoncurrentZeroDays() {
		var client = getClient();
		var bucketName = createBucket(client, 35);

		assertPutRulesError(client, bucketName,
				rule().withNoncurrentVersionExpiration(new NoncurrentVersionExpiration().withDays(0)),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Version")
	public void testLifecycleNewerNoncurrentVersions() {
		var client = getClient();
		var bucketName = createBucket(client, 36);

		putRules(client, bucketName, rule().withNoncurrentVersionExpiration(
				new NoncurrentVersionExpiration().withDays(1).withNewerNoncurrentVersions(5)));

		var expiration = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0)
				.getNoncurrentVersionExpiration();
		assertEquals(1, expiration.getDays());
		assertEquals(5, expiration.getNewerNoncurrentVersions());

		// 보존할 버전 수는 1~100 만 허용한다
		for (var count : List.of(0, 101)) {
			assertPutRulesError(client, bucketName,
					rule().withNoncurrentVersionExpiration(
							new NoncurrentVersionExpiration().withDays(1).withNewerNoncurrentVersions(count)),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		}
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDeleteMarkerWithDays() {
		var client = getClient();
		var bucketName = createBucket(client, 37);

		// ExpiredObjectDeleteMarker 는 Days 와 함께 쓸 수 없다
		assertPutRulesError(client, bucketName, rule().withExpirationInDays(1).withExpiredObjectDeleteMarker(true),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleDeleteMarkerWithTag() {
		var client = getClient();
		var bucketName = createBucket(client, 38);

		assertPutRulesError(client, bucketName,
				rule().withFilter(filter(tag("key", "value"))).withExpiredObjectDeleteMarker(true),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAbortMultipartWithFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 39);

		// AbortIncompleteMultipartUpload 는 태그·크기 필터와 함께 쓸 수 없다
		var filters = List.of(filter(tag("key", "value")), filter(new LifecycleObjectSizeGreaterThanPredicate(10L)));
		for (var filter : filters) {
			assertPutRulesError(client, bucketName,
					rule().withFilter(filter).withAbortIncompleteMultipartUpload(
							new AbortIncompleteMultipartUpload().withDaysAfterInitiation(1)),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
		}
	}

	@Test
	@Tag("ERROR")
	public void testLifecycleAbortMultipartZeroDays() {
		var client = getClient();
		var bucketName = createBucket(client, 40);

		assertPutRulesError(client, bucketName,
				rule().withAbortIncompleteMultipartUpload(
						new AbortIncompleteMultipartUpload().withDaysAfterInitiation(0)),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleTagFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 41);

		putRules(client, bucketName, rule().withFilter(filter(tag("key", "value"))).withExpirationInDays(1));

		var predicate = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0).getFilter()
				.getPredicate();
		var actual = assertInstanceOf(LifecycleTagPredicate.class, predicate).getTag();
		assertEquals("key", actual.getKey());
		assertEquals("value", actual.getValue());
	}

	@Test
	@Tag("Filter")
	public void testLifecycleAndFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 42);

		putRules(client, bucketName, rule()
				.withFilter(andFilter(new LifecyclePrefixPredicate("test1/"), tag("key1", "value1"),
						tag("key2", "value2"), new LifecycleObjectSizeGreaterThanPredicate(10L),
						new LifecycleObjectSizeLessThanPredicate(100L)))
				.withExpirationInDays(1));

		var predicate = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0).getFilter()
				.getPredicate();
		var and = assertInstanceOf(LifecycleAndOperator.class, predicate);

		// And 안의 조건 순서는 보장되지 않으므로 타입으로 구분해 확인한다
		String prefix = null;
		Long greaterThan = null;
		Long lessThan = null;
		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		for (var operand : and.getOperands()) {
			if (operand instanceof LifecyclePrefixPredicate p)
				prefix = p.getPrefix();
			else if (operand instanceof LifecycleTagPredicate p)
				tags.add(p.getTag());
			else if (operand instanceof LifecycleObjectSizeGreaterThanPredicate p)
				greaterThan = p.getObjectSizeGreaterThan();
			else if (operand instanceof LifecycleObjectSizeLessThanPredicate p)
				lessThan = p.getObjectSizeLessThan();
		}
		assertEquals("test1/", prefix);
		tagCompare(List.of(new com.amazonaws.services.s3.model.Tag("key1", "value1"),
				new com.amazonaws.services.s3.model.Tag("key2", "value2")), tags);
		assertEquals(Long.valueOf(10), greaterThan);
		assertEquals(Long.valueOf(100), lessThan);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleAndFilterInvalid() {
		var client = getClient();
		var bucketName = createBucket(client, 43);

		// And 안에 조건이 하나뿐이면 거부된다
		assertPutRulesError(client, bucketName,
				rule().withFilter(andFilter(new LifecyclePrefixPredicate("test1/"))).withExpirationInDays(1),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
		// And 안의 태그 키는 중복될 수 없다
		assertPutRulesError(client, bucketName,
				rule().withFilter(andFilter(new LifecyclePrefixPredicate("test1/"), tag("key", "1"), tag("key", "2")))
						.withExpirationInDays(1),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleObjectSizeFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 44);

		putRules(client, bucketName,
				rule().withFilter(filter(new LifecycleObjectSizeGreaterThanPredicate(500L))).withExpirationInDays(1));
		var predicate = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0).getFilter()
				.getPredicate();
		assertEquals(Long.valueOf(500), assertInstanceOf(LifecycleObjectSizeGreaterThanPredicate.class, predicate)
				.getObjectSizeGreaterThan());

		putRules(client, bucketName,
				rule().withFilter(filter(new LifecycleObjectSizeLessThanPredicate(500L))).withExpirationInDays(1));
		predicate = client.getBucketLifecycleConfiguration(bucketName).getRules().get(0).getFilter().getPredicate();
		assertEquals(Long.valueOf(500),
				assertInstanceOf(LifecycleObjectSizeLessThanPredicate.class, predicate).getObjectSizeLessThan());
	}

	@Test
	@Tag("Filter")
	public void testLifecycleObjectSizeFilterInvalid() {
		var client = getClient();
		var bucketName = createBucket(client, 45);

		// ObjectSizeLessThan 은 1 이상이어야 한다
		assertPutRulesError(client, bucketName,
				rule().withFilter(filter(new LifecycleObjectSizeLessThanPredicate(0L))).withExpirationInDays(1),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
		// ObjectSizeLessThan 은 ObjectSizeGreaterThan 보다 커야 한다
		assertPutRulesError(client, bucketName,
				rule().withFilter(andFilter(new LifecycleObjectSizeGreaterThanPredicate(100L),
						new LifecycleObjectSizeLessThanPredicate(100L))).withExpirationInDays(1),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("Filter")
	public void testLifecycleLegacyPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 46);

		// Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식도 그대로 저장·조회된다
		// (Prefix 와 Filter 를 함께 주는 요청은 SDK v1 이 보내기 전에 막으므로 확인하지 않는다)
		putRules(client, bucketName, new Rule().withId("rule1").withStatus(BucketLifecycleConfiguration.ENABLED)
				.withPrefix("legacy/").withExpirationInDays(1));
		var rules = client.getBucketLifecycleConfiguration(bucketName).getRules();
		assertEquals("legacy/", rules.get(0).getPrefix());
		assertNull(rules.get(0).getFilter());
	}

	private static List<Rule> makeRules(int count) {
		var rules = new ArrayList<Rule>();
		for (int i = 0; i < count; i++) {
			rules.add(new Rule().withId("rule" + i).withStatus(BucketLifecycleConfiguration.ENABLED)
					.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test" + i + "/")))
					.withExpirationInDays(1));
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
		assertEquals(1000, client.getBucketLifecycleConfiguration(bucketName).getRules().size());
		assertPutRulesError(client, bucketName, makeRules(1001), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("metadata")
	public void testLifecycleExpirationHeader() throws java.io.IOException {
		var client = getClient();
		var bucketName = createBucket(client, 49);
		putRules(client, bucketName, new Rule().withId("rule1").withStatus(BucketLifecycleConfiguration.ENABLED)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("test1/"))).withExpirationInDays(1));

		// 만료 규칙에 걸리는 오브젝트는 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려준다
		var key = "test1/testLifecycleExpirationHeader";
		var putResponse = client.putObject(bucketName, key, "test");
		assertEquals("rule1", putResponse.getExpirationTimeRuleId());
		assertNotNull(putResponse.getExpirationTime());
		assertEquals("rule1", client.getObjectMetadata(bucketName, key).getExpirationTimeRuleId());
		try (var getResponse = client.getObject(bucketName, key)) {
			assertEquals("rule1", getResponse.getObjectMetadata().getExpirationTimeRuleId());
		}

		// 규칙에 걸리지 않는 오브젝트에는 헤더가 없다
		var other = "test2/testLifecycleExpirationHeader";
		assertNull(client.putObject(bucketName, other, "test").getExpirationTimeRuleId());
		assertNull(client.getObjectMetadata(bucketName, other).getExpirationTimeRuleId());
	}
}
