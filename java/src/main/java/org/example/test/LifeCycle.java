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
import com.amazonaws.services.s3.model.AbortIncompleteMultipartUpload;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.NoncurrentVersionExpiration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;

public class LifeCycle extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("LifeCycle Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("LifeCycle End");
	}

	@Test
	@Tag("Check")
	public void testLifecycleSet() {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);
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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createObjects(client, "past/foo", "future/bar");

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

		var keyNames = List.of("test1/a", "test2/b");

		var uploadIds = new ArrayList<String>();

		for (var key : keyNames) {
			var response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
			uploadIds.add(response.getUploadId());
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
	}

	@Test
	@Tag("Delete")
	public void testLifecycleDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
		var bucketName = createBucket(client);

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
}
