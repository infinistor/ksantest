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

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import com.amazonaws.AmazonServiceException;

public class PutBucket extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("PutBucket Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("PutBucket End");
	}

	@Test
	@Tag("PUT")
	public void testBucketListEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listObjects(bucketName);

		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadStartsNonAlpha() {
		var bucketName = getNewBucketName();
		checkBadBucketName("_" + bucketName);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadShortOne() {
		checkBadBucketName("a");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadShortTwo() {
		checkBadBucketName("aa");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodLong60() {
		testBucketCreateNamingGoodLong(60);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodLong61() {
		testBucketCreateNamingGoodLong(61);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodLong62() {
		testBucketCreateNamingGoodLong(62);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodLong63() {
		testBucketCreateNamingGoodLong(63);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodLong64() {
		testBucketCreateNamingBadLong(64);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketListLongName() {
		var bucketName = getNewBucketName(61);
		var client = getClient();
		client.createBucket(bucketName);
		var response = client.listObjects(bucketName);

		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadIp() {
		checkBadBucketName("192.168.11.123");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsUnderscore() {
		checkBadBucketName("fooBar");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsLong() {
		var prefix = getPrefix();
		var addLength = 63 - prefix.length();
		prefix = Utils.randomText(addLength);
		checkGoodBucketName(prefix, null);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsDashAtEnd() {
		checkBadBucketName("foo-");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsDotDot() {
		checkBadBucketName("foo..bar");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsDotDash() {
		checkBadBucketName("foo.-bar");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsDashDot() {
		checkBadBucketName("foo-.bar");
	}

	@Test
	@Tag("Duplicate")
	public void testBucketCreateExists() {
		var bucketName = getNewBucketName();
		var client = getClient();

		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client.createBucket(bucketName));

		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.BUCKET_ALREADY_OWNED_BY_YOU, e.getErrorCode());
	}

	@Test
	@Tag("Duplicate")
	public void testBucketCreateExistsNonowner() {
		var bucketName = getNewBucketName();
		var client = getClient();
		var altClient = getAltClient();

		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.createBucket(bucketName));

		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.BUCKET_ALREADY_EXISTS, e.getErrorCode());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodStartsAlpha() {
		checkGoodBucketName("foo", "a" + getPrefix());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodStartsDigit() {
		checkGoodBucketName("foo", "0" + getPrefix());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodContainsPeriod() {
		checkGoodBucketName("aaa.111", null);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodContainsHyphen() {
		checkGoodBucketName("aaa-111", null);
	}

	@Test
	@Tag("Duplicate")
	public void testBucketRecreateNotOverriding() {
		var keys = List.of("my_key1", "my_key2");

		var bucketName = createObjects(keys);

		var objects = getObjectList(bucketName, null);
		assertEquals(keys, objects);

		var client = getClient();
		assertThrows(AmazonServiceException.class, () -> client.createBucket(bucketName));

		objects = getObjectList(bucketName, null);
		assertEquals(keys, objects);
	}

	@Test
	@Tag("location")
	public void testGetBucketLocation() {
		var client = getClient();
		var bucketName = createBucket(client);
		client.getBucketLocation(bucketName);
	}
}
