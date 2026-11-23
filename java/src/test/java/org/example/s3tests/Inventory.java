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

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 버킷 인벤토리 기능 테스트
 */
class Inventory {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Inventory Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Inventory End");
	}

	org.example.test.Inventory test = new org.example.test.Inventory();
	org.example.testV2.Inventory testV2 = new org.example.testV2.Inventory();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인
	 */
	@Test
	@Tag("List")
	void testListBucketInventory() {
		test.testListBucketInventory();
		testV2.testListBucketInventory();
	}

	/**
	 * 버킷에 인벤토리를 설정할 수 있는지 확인
	 */
	@Test
	@Tag("Put")
	void testPutBucketInventory() {
		test.testPutBucketInventory();
		testV2.testPutBucketInventory();
	}

	/**
	 * 버킷에 인벤토리 설정이 되었는지 확인
	 */
	@Test
	@Tag("Check")
	void testCheckBucketInventory() {
		test.testCheckBucketInventory();
		testV2.testCheckBucketInventory();
	}

	/**
	 * 버킷에 설정된 인벤토리를 조회할 수 있는지 확인
	 */
	@Test
	@Tag("Get")
	void testGetBucketInventory() {
		test.testGetBucketInventory();
		testV2.testGetBucketInventory();
	}

	/**
	 * 버킷에 설정된 인벤토리를 삭제할 수 있는지 확인
	 */
	@Test
	@Tag("Delete")
	void testDeleteBucketInventory() {
		test.testDeleteBucketInventory();
		testV2.testDeleteBucketInventory();
	}

	/**
	 * 존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testGetBucketInventoryNotExist() {
		test.testGetBucketInventoryNotExist();
		testV2.testGetBucketInventoryNotExist();
	}

	/**
	 * 존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testDeleteBucketInventoryNotExist() {
		test.testDeleteBucketInventoryNotExist();
		testV2.testDeleteBucketInventoryNotExist();
	}

	/**
	 * 존재하지 않은 버킷에 인벤토리를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryNotExist() {
		test.testPutBucketInventoryNotExist();
		testV2.testPutBucketInventoryNotExist();
	}

	/**
	 * 인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryIdNotExist() {
		test.testPutBucketInventoryIdNotExist();
		testV2.testPutBucketInventoryIdNotExist();
	}

	/**
	 * 인벤토리 아이디가 중복되는 경우 덮어쓰기 되는지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testPutBucketInventoryIdDuplicate(){
		test.testPutBucketInventoryIdDuplicate();
		testV2.testPutBucketInventoryIdDuplicate();
	}

	/**
	 * 타깃 버킷이 존재하지 않아도 설정이 저장되는지 확인 (AWS 는 타깃 버킷 존재 여부를 확인하지 않음)
	 */
	@Test
	@Tag("Put")
	void testPutBucketInventoryTargetNotExist() {
		test.testPutBucketInventoryTargetNotExist();
		testV2.testPutBucketInventoryTargetNotExist();
	}

	/**
	 * 지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidFormat() {
		test.testPutBucketInventoryInvalidFormat();
		testV2.testPutBucketInventoryInvalidFormat();
	}

	/**
	 * 올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidFrequency() {
		test.testPutBucketInventoryInvalidFrequency();
		testV2.testPutBucketInventoryInvalidFrequency();
	}

	/**
	 * 대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidCase() {
		test.testPutBucketInventoryInvalidCase();
		testV2.testPutBucketInventoryInvalidCase();
	}

	/**
	 * 접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Put")
	void testPutBucketInventoryPrefix() {
		test.testPutBucketInventoryPrefix();
		testV2.testPutBucketInventoryPrefix();
	}

	/**
	 * 옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Put")
	void testPutBucketInventoryOptional() {
		test.testPutBucketInventoryOptional();
		testV2.testPutBucketInventoryOptional();
	}

	/**
	 * 올바르지 않은 옵션을 포함한 인벤토리를 설정하려고 할 경우 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidOptional() {
		test.testPutBucketInventoryInvalidOptional();
		testV2.testPutBucketInventoryInvalidOptional();
	}

	/**
	 * 인벤토리 ID 는 64자까지 허용되고 65자는 거부되는지 확인
	 */
	@Test
	@Tag("Max")
	void testPutBucketInventoryIdLength() {
		test.testPutBucketInventoryIdLength();
		testV2.testPutBucketInventoryIdLength();
	}

	/**
	 * 허용하지 않는 문자가 든 인벤토리 ID 가 거부되는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidId() {
		test.testPutBucketInventoryInvalidId();
		testV2.testPutBucketInventoryInvalidId();
	}

	/**
	 * 쿼리의 id 와 본문의 Id 가 다르면 거부되는지 확인 (SDK v1 은 두 값을 따로 줄 수 없으므로 V2만)
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryIdMismatch() {
		testV2.testPutBucketInventoryIdMismatch();
	}

	/**
	 * 비활성(IsEnabled=false) 인벤토리가 그대로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testPutBucketInventoryDisabled() {
		test.testPutBucketInventoryDisabled();
		testV2.testPutBucketInventoryDisabled();
	}

	/**
	 * 올바르지 않은 IncludedObjectVersions 가 거부되는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidVersions() {
		test.testPutBucketInventoryInvalidVersions();
		testV2.testPutBucketInventoryInvalidVersions();
	}

	/**
	 * 인벤토리의 형식·주기·대상 버전이 그대로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testPutBucketInventoryFormats() {
		test.testPutBucketInventoryFormats();
		testV2.testPutBucketInventoryFormats();
	}

	/**
	 * 인벤토리 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testPutBucketInventoryDestination() {
		test.testPutBucketInventoryDestination();
		testV2.testPutBucketInventoryDestination();
	}

	/**
	 * 대상 버킷이 ARN 형식이 아니면 거부되는지 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketInventoryInvalidDestinationArn() {
		test.testPutBucketInventoryInvalidDestinationArn();
		testV2.testPutBucketInventoryInvalidDestinationArn();
	}

	/**
	 * 인벤토리 설정 목록이 100개씩 나뉘어 조회되는지 확인
	 */
	@Test
	@Tag("List")
	void testListBucketInventoryPagination() {
		test.testListBucketInventoryPagination();
		testV2.testListBucketInventoryPagination();
	}

	/**
	 * 올바르지 않은 ContinuationToken 으로 인벤토리 목록을 조회하면 실패하는지 확인
	 */
	@Test
	@Tag("List")
	void testListBucketInventoryInvalidToken() {
		test.testListBucketInventoryInvalidToken();
		testV2.testListBucketInventoryInvalidToken();
	}

	/**
	 * 존재하지 않는 버킷의 인벤토리 목록·조회·삭제가 NoSuchBucket 으로 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testBucketInventoryBucketNotExist() {
		test.testBucketInventoryBucketNotExist();
		testV2.testBucketInventoryBucketNotExist();
	}
}
