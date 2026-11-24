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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class PutBucket extends TestBase {
	@Test
	@Tag("PUT")
	public void testBucketListEmpty() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.listObjects(l -> l.bucket(bucketName));

		assertEquals(0, response.contents().size());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadStartsNonAlpha() {
		var bucketName = getNewBucketNameOnly(2);
		checkBadBucketName("_" + bucketName);
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
	public void testBucketCreateNamingBadIp() {
		checkBadBucketName("127.0.0.1");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsUnderscore() {
		checkBadBucketName("foo_bar");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingDnsLong() {
		var prefix = getPrefix();
		var addLength = 63 - prefix.length();
		checkGoodBucketName(prefix + Utils.randomText(addLength));
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
		var bucketName = getNewBucketName(17);
		var client = getClient();

		client.createBucket(c -> c.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> client.createBucket(c -> c.bucket(bucketName)));

		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.BUCKET_ALREADY_OWNED_BY_YOU, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Duplicate")
	public void testBucketCreateExistsNonowner() {
		var bucketName = getNewBucketName(18);
		var client = getClient();
		var altClient = getAltClient();

		client.createBucket(c -> c.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> altClient.createBucket(c -> c.bucket(bucketName)));

		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.BUCKET_ALREADY_EXISTS, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodStartsAlpha() {
		checkGoodBucketName(Utils.getNewBucketName("a", getSuiteId(), 19));
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodStartsDigit() {
		checkGoodBucketName(Utils.getNewBucketName("0", getSuiteId(), 20));
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodContainsPeriod() {
		checkGoodBucketName(getNewBucketNameOnly(21).replace('-', '.'));
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingGoodContainsHyphen() {
		checkGoodBucketName(getNewBucketNameOnly(22));
	}

	@Test
	@Tag("Duplicate")
	public void testBucketRecreateNotOverriding() {
		var keys = List.of("my_key1", "my_key2");
		var client = getClient();
		var bucketName = createObjects(client, 23, keys);

		var objects = getObjectList(client, bucketName, null);
		assertEquals(keys, objects);

		assertThrows(AwsServiceException.class, () -> client.createBucket(c -> c.bucket(bucketName)));

		objects = getObjectList(client, bucketName, null);
		assertEquals(keys, objects);
	}

	@Test
	@Tag("location")
	public void testGetBucketLocation() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		client.getBucketLocation(g -> g.bucket(bucketName));
	}

	/** 버킷 생성이 주어진 에러로 실패하는지 확인한다. 혹시 만들어지면 남지 않게 바로 지운다. */
	private void assertCreateError(Consumer<CreateBucketRequest.Builder> request, String bucketName, int statusCode,
			String errorCode) {
		var client = getClient();
		var e = assertThrows(AwsServiceException.class, () -> {
			client.createBucket(request);
			client.deleteBucket(d -> d.bucket(bucketName));
		});
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	private void assertCreateError(String bucketName, int statusCode, String errorCode) {
		assertCreateError(c -> c.bucket(bucketName), bucketName, statusCode, errorCode);
	}

	/** 이름을 length 자로 자르고 끝에 남은 '-', '.' 를 떼어낸다. */
	private static String trimBucketName(String bucketName, int length) {
		if (bucketName.length() > length)
			bucketName = bucketName.substring(0, length);
		return bucketName.replaceAll("[-.]+$", "");
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadUppercase() {
		// SDK 가 요청을 보내기 전에 이름을 검사해 막는다
		checkBadBucketName(getNewBucketNameOnly(25).toUpperCase());
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadPunycodePrefix() {
		// xn-- 로 시작하는 이름은 쓸 수 없다
		var bucketName = trimBucketName("xn--" + getNewBucketNameOnly(26), 63);
		assertCreateError(bucketName, HttpStatus.SC_BAD_REQUEST, MainData.INVALID_BUCKET_NAME);
	}

	@Test
	@Tag("CreationRules")
	public void testBucketCreateNamingBadDotAtEdge() {
		// SDK 가 요청을 보내기 전에 이름을 검사해 막는다
		var bucketName = trimBucketName(getNewBucketNameOnly(27), 60);
		checkBadBucketName("." + bucketName);
		checkBadBucketName(bucketName + ".");
	}

	@Test
	@Tag("location")
	public void testGetBucketLocationNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(30);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketLocation(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("location")
	public void testGetBucketLocationAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 31);

		var e = assertThrows(AwsServiceException.class, () -> altClient.getBucketLocation(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ACL")
	public void testBucketCreateAclWithGrant() {
		// 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
		var bucketName = getNewBucketNameOnly(32);
		assertCreateError(c -> c.bucket(bucketName).acl("private").grantRead("uri=\"" + MainData.ALL_USERS + "\""),
				bucketName, HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ACL")
	public void testBucketCreateInvalidAcl() {
		var bucketName = getNewBucketNameOnly(33);
		assertCreateError(c -> c.bucket(bucketName).acl("invalid"), bucketName, HttpStatus.SC_BAD_REQUEST,
				MainData.INVALID_ARGUMENT);
	}
}
