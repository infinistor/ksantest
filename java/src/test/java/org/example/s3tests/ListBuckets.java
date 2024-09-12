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

	org.example.test.ListBuckets test = new org.example.test.ListBuckets();
	org.example.testV2.ListBuckets testV2 = new org.example.testV2.ListBuckets();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Get")
	//여러개의 버킷 생성해서 목록 조회 확인
	void testBucketsCreateThenList() {
		test.testBucketsCreateThenList();
		testV2.testBucketsCreateThenList();
	}

	@Test
	@Tag("ERROR")
	//존재하지 않는 사용자가 버킷목록 조회시 에러 확인
	void testListBucketsInvalidAuth() {
		test.testListBucketsInvalidAuth();
		testV2.testListBucketsInvalidAuth();
	}

	@Test
	@Tag("ERROR")
	//로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인
	void testListBucketsBadAuth() {
		test.testListBucketsBadAuth();
		testV2.testListBucketsBadAuth();
	}

	@Test
	@Tag("Metadata")
	//버킷의 메타데이터를 가져올 수 있는지 확인
	void testHeadBucket() {
		test.testHeadBucket();
		testV2.testHeadBucket();
	}
}
