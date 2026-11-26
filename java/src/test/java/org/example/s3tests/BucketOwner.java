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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
class BucketOwner {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("BucketOwner Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("BucketOwner End");
	}

	org.example.testV2.BucketOwner testV2 = new org.example.testV2.BucketOwner();

	/**
	 * 테스트 완료 후 정리 작업을 수행합니다.
	 *
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		testV2.clear(testInfo);
	}

	/**
	 * 올바른 소유자 아이디로 HeadBucket이 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testBucketOwnerHeadBucket() {
		testV2.testBucketOwnerHeadBucket();
	}

	/**
	 * 잘못된 소유자 아이디로 HeadBucket을 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerHeadBucketMismatch() {
		testV2.testBucketOwnerHeadBucketMismatch();
	}

	/**
	 * 올바른 소유자 아이디로 ListObjectsV2가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testBucketOwnerListObjectsV2() {
		testV2.testBucketOwnerListObjectsV2();
	}

	/**
	 * 잘못된 소유자 아이디로 ListObjectsV2를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerListObjectsV2Mismatch() {
		testV2.testBucketOwnerListObjectsV2Mismatch();
	}

	/**
	 * 올바른 소유자 아이디로 오브젝트 업로드가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testBucketOwnerPutObject() {
		testV2.testBucketOwnerPutObject();
	}

	/**
	 * 잘못된 소유자 아이디로 오브젝트 업로드를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerPutObjectMismatch() {
		testV2.testBucketOwnerPutObjectMismatch();
	}

	/**
	 * 올바른 소유자 아이디로 오브젝트 다운로드가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testBucketOwnerGetObject() {
		testV2.testBucketOwnerGetObject();
	}

	/**
	 * 잘못된 소유자 아이디로 오브젝트 다운로드를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerGetObjectMismatch() {
		testV2.testBucketOwnerGetObjectMismatch();
	}

	/**
	 * 올바른 소유자 아이디로 오브젝트 삭제가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testBucketOwnerDeleteObject() {
		testV2.testBucketOwnerDeleteObject();
	}

	/**
	 * 잘못된 소유자 아이디로 오브젝트 삭제를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerDeleteObjectMismatch() {
		testV2.testBucketOwnerDeleteObjectMismatch();
	}

	/**
	 * 올바른 소스/대상 소유자 아이디로 오브젝트 복사가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testSourceBucketOwnerCopyObject() {
		testV2.testSourceBucketOwnerCopyObject();
	}

	/**
	 * 잘못된 소스 소유자 아이디로 오브젝트 복사를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testSourceBucketOwnerCopyObjectMismatch() {
		testV2.testSourceBucketOwnerCopyObjectMismatch();
	}

	/**
	 * 잘못된 대상 소유자 아이디로 오브젝트 복사를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerCopyObjectMismatch() {
		testV2.testBucketOwnerCopyObjectMismatch();
	}

	/**
	 * 올바른 소스/대상 소유자 아이디로 파트 복사가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testSourceBucketOwnerUploadPartCopy() {
		testV2.testSourceBucketOwnerUploadPartCopy();
	}

	/**
	 * 잘못된 소스 소유자 아이디로 파트 복사를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testSourceBucketOwnerUploadPartCopyMismatch() {
		testV2.testSourceBucketOwnerUploadPartCopyMismatch();
	}

	/**
	 * 잘못된 대상 소유자 아이디로 파트 복사를 실패하는지 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testBucketOwnerUploadPartCopyMismatch() {
		testV2.testBucketOwnerUploadPartCopyMismatch();
	}

	/**
	 * ListObjects/ListObjectVersions/ListMultipartUploads가 소유자 아이디를 확인하는지 테스트
	 */
	@Test
	@Tag("List")
	void testBucketOwnerListOperations() {
		testV2.testBucketOwnerListOperations();
	}

	/**
	 * 버킷 위치/ACL/버저닝 API가 소유자 아이디를 확인하고, 다르면 설정이 바뀌지 않는지 테스트
	 */
	@Test
	@Tag("Bucket")
	void testBucketOwnerBucketConfig() {
		testV2.testBucketOwnerBucketConfig();
	}

	/**
	 * 버킷 태그/CORS 설정·조회·삭제가 소유자 아이디를 확인하는지 테스트
	 */
	@Test
	@Tag("Bucket")
	void testBucketOwnerBucketTaggingCors() {
		testV2.testBucketOwnerBucketTaggingCors();
	}

	/**
	 * 설정이 없는 버킷에서 소유자가 맞으면 404, 다르면 403을 반환하는지 테스트
	 */
	@Test
	@Tag("Priority")
	void testBucketOwnerNotConfigured() {
		testV2.testBucketOwnerNotConfigured();
	}

	/**
	 * HeadObject/GetObjectAcl/GetObjectAttributes가 소유자 아이디를 확인하는지 테스트
	 */
	@Test
	@Tag("Object")
	void testBucketOwnerObjectRead() {
		testV2.testBucketOwnerObjectRead();
	}

	/**
	 * 오브젝트 태그 설정·조회·삭제가 소유자 아이디를 확인하는지 테스트
	 */
	@Test
	@Tag("Object")
	void testBucketOwnerObjectTagging() {
		testV2.testBucketOwnerObjectTagging();
	}

	/**
	 * 없는 키를 조회할 때 소유자가 맞으면 404, 다르면 403을 반환하는지 테스트
	 */
	@Test
	@Tag("Priority")
	void testBucketOwnerKeyNotExist() {
		testV2.testBucketOwnerKeyNotExist();
	}

	/**
	 * 잘못된 소유자 아이디로 DeleteObjects를 하면 하나도 지워지지 않는지 테스트
	 */
	@Test
	@Tag("Object")
	void testBucketOwnerDeleteObjects() {
		testV2.testBucketOwnerDeleteObjects();
	}

	/**
	 * 멀티파트 업로드의 각 단계가 소유자 아이디를 확인하는지 테스트
	 */
	@Test
	@Tag("Multipart")
	void testBucketOwnerMultipart() {
		testV2.testBucketOwnerMultipart();
	}

	/**
	 * 잘못된 소유자 아이디로 버킷 삭제를 실패하고, 올바른 아이디로는 삭제되는지 테스트
	 */
	@Test
	@Tag("Bucket")
	void testBucketOwnerDeleteBucket() {
		testV2.testBucketOwnerDeleteBucket();
	}

	/**
	 * 버킷이 없으면 소유자 아이디와 상관없이 404를 반환하는지 테스트
	 */
	@Test
	@Tag("Priority")
	void testBucketOwnerBucketNotExist() {
		testV2.testBucketOwnerBucketNotExist();
	}

	/**
	 * 다른 사용자는 소유자 아이디가 맞아도 접근할 수 없는지 테스트
	 */
	@Test
	@Tag("Access")
	void testBucketOwnerOtherUser() {
		testV2.testBucketOwnerOtherUser();
	}
}
