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
	// 생성한 버킷이 비어있는지 확인
	public void testBucketListEmpty() {
		var bucketName = getNewBucket();
		var client = getClient();

		var response = client.listObjects(bucketName);

		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingBadStartsNonAlpha() {
		var bucketName = getNewBucketName();
		checkBadBucketName("_" + bucketName);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingBadShortOne() {
		checkBadBucketName("a");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingBadShortTwo() {
		checkBadBucketName("aa");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 60자인 경우 버킷 생성 확인
	public void testBucketCreateNamingGoodLong60() {
		testBucketCreateNamingGoodLong(60);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 61자인 경우 버킷 생성 확인
	public void testBucketCreateNamingGoodLong61() {
		testBucketCreateNamingGoodLong(61);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 62자인 경우 버킷 생성 확인
	public void testBucketCreateNamingGoodLong62() {
		testBucketCreateNamingGoodLong(62);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 63자인 경우 버킷 생성 확인
	public void testBucketCreateNamingGoodLong63() {
		testBucketCreateNamingGoodLong(63);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 64자인 경우 버킷 생성 실패
	public void testBucketCreateNamingGoodLong64() {
		testBucketCreateNamingBadLong(64);
	}

	@Test
	@Tag("CreationRules")
	// 버킷이름의 길이 긴 경우 버킷 목록을 읽어올 수 있는지 확인
	public void testBucketListLongName() {
		var bucketName = getNewBucketName(61);
		var client = getClient();
		client.createBucket(bucketName);
		var response = client.listObjects(bucketName);

		assertEquals(0, response.getObjectSummaries().size());
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingBadIp() {
		checkBadBucketName("192.168.11.123");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingDnsUnderscore() {
		checkBadBucketName("fooBar");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인
	public void testBucketCreateNamingDnsLong() {
		var prefix = getPrefix();
		var addLength = 63 - prefix.length();
		prefix = Utils.randomText(addLength);
		checkGoodBucketName(prefix, null);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingDnsDashAtEnd() {
		checkBadBucketName("foo-");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingDnsDotDot() {
		checkBadBucketName("foo..bar");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingDnsDotDash() {
		checkBadBucketName("foo.-bar");
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void testBucketCreateNamingDnsDashDot() {
		checkBadBucketName("foo-.bar");
	}

	@Test
	@Tag("Duplicate")
	// 버킷 중복 생성시 실패 확인
	public void testBucketCreateExists() {
		var bucketName = getNewBucketName();
		var client = getClient();

		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client.createBucket(bucketName));

		assertEquals(409, e.getStatusCode());
		assertEquals(MainData.BucketAlreadyOwnedByYou, e.getErrorCode());
	}

	@Test
	@Tag("Duplicate")
	// [다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인
	public void testBucketCreateExistsNonowner() {
		var bucketName = getNewBucketName();
		var client = getClient();
		var altClient = getAltClient();

		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.createBucket(bucketName));

		assertEquals(409, e.getStatusCode());
		assertEquals(MainData.BucketAlreadyExists, e.getErrorCode());
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인
	public void testBucketCreateNamingGoodStartsAlpha() {
		checkGoodBucketName("foo", "a" + getPrefix());
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인
	public void testBucketCreateNamingGoodStartsDigit() {
		checkGoodBucketName("foo", "0" + getPrefix());
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인
	public void testBucketCreateNamingGoodContainsPeriod() {
		checkGoodBucketName("aaa.111", null);
	}

	@Test
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인
	public void testBucketCreateNamingGoodContainsHyphen() {
		checkGoodBucketName("aaa-111", null);
	}

	@Test
	@Tag("Duplicate")
	// 버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)
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
	// 버킷의 location 정보 조회
	public void testGetBucketLocation() {
		var bucketName = getNewBucket();
		var client = getClient();
		client.getBucketLocation(bucketName);
	}
}
