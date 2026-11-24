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
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class ListBuckets {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("ListBuckets Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("ListBuckets End");
	}


	org.example.test.ListBuckets test = new org.example.test.ListBuckets();
	org.example.testV2.ListBuckets testV2 = new org.example.testV2.ListBuckets();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 여러개의 버킷 생성해서 목록 조회 확인
	 */
	@Test
	@Tag("Get")
	void testBucketsCreateThenList() {
		test.testBucketsCreateThenList();
		testV2.testBucketsCreateThenList();
	}

	/**
	 * 존재하지 않는 사용자가 버킷목록 조회시 에러 확인
	 */
	@Test
	@Tag("ERROR")
	void testListBucketsInvalidAuth() {
		test.testListBucketsInvalidAuth();
		testV2.testListBucketsInvalidAuth();
	}

	/**
	 * 로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인
	 */
	@Test
	@Tag("ERROR")
	void testListBucketsBadAuth() {
		test.testListBucketsBadAuth();
		testV2.testListBucketsBadAuth();
	}

	/**
	 * 버킷의 메타데이터를 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Metadata")
	void testHeadBucket() {
		test.testHeadBucket();
		testV2.testHeadBucket();
	}

	/**
	 * 버킷 목록 조회시 Prefix를 이용한 필터링 확인
	 */
	@Test
	@Tag("Prefix")
	void testListBucketsPrefix() {
		test.testListBucketsPrefix();
		testV2.testListBucketsPrefix();
	}

	/**
	 * 버킷 목록 조회시 MaxBuckets를 이용한 필터링 확인
	 */
	@Test
	@Tag("MaxBuckets")
	void testListBucketsMaxBuckets() {
		test.testListBucketsMaxBuckets();
		testV2.testListBucketsMaxBuckets();
	}

	/**
	 * 버킷 목록 조회시 ContinuationToken를 이용한 필터링 확인
	 */
	@Test
	@Tag("ContinuationToken")
	void testListBucketsContinuationToken() {
		test.testListBucketsContinuationToken();
		testV2.testListBucketsContinuationToken();
	}

	/**
	 * 존재하지 않는 버킷의 메타데이터를 조회할 경우 404 확인
	 */
	@Test
	@Tag("Metadata")
	void testHeadBucketNotExist() {
		test.testHeadBucketNotExist();
		testV2.testHeadBucketNotExist();
	}

	/**
	 * 다른 사용자의 버킷 메타데이터를 조회할 경우 403 확인
	 */
	@Test
	@Tag("Permission")
	void testHeadBucketAltUser() {
		test.testHeadBucketAltUser();
		testV2.testHeadBucketAltUser();
	}

	/**
	 * 버킷 목록 조회시 일치하는 버킷이 없는 Prefix를 지정하면 빈 목록을 돌려주는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testListBucketsPrefixNotMatch() {
		test.testListBucketsPrefixNotMatch();
		testV2.testListBucketsPrefixNotMatch();
	}

	/**
	 * 버킷 목록 조회시 허용 범위를 벗어난 MaxBuckets를 지정할 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testListBucketsMaxBucketsInvalid() {
		test.testListBucketsMaxBucketsInvalid();
		testV2.testListBucketsMaxBucketsInvalid();
	}

	/**
	 * 버킷 목록을 ContinuationToken으로 끝까지 넘기면 전체 목록과 순서까지 같은지 확인
	 */
	@Test
	@Tag("ContinuationToken")
	void testListBucketsPaging() {
		test.testListBucketsPaging();
		testV2.testListBucketsPaging();
	}

	/**
	 * 버킷 목록 조회시 버킷 이름, 생성일, 소유자 정보가 올바른지 확인
	 */
	@Test
	@Tag("Get")
	void testListBucketsOwner() {
		test.testListBucketsOwner();
		testV2.testListBucketsOwner();
	}

	/**
	 * 다른 사용자의 버킷 목록에는 내 버킷이 나오지 않는지 확인
	 */
	@Test
	@Tag("Permission")
	void testListBucketsAltUser() {
		test.testListBucketsAltUser();
		testV2.testListBucketsAltUser();
	}
}