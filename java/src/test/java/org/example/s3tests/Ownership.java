package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷과 오브젝트의 소유권 기능을 테스트하는 클래스
 */
class Ownership {

	org.example.test.Ownership test = new org.example.test.Ownership();
	org.example.testV2.Ownership testV2 = new org.example.testV2.Ownership();

	/**
	 * 테스트 정리 작업 수행
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷의 소유권 조회 확인
	 */
	@Test
	@Tag("Get")
	void testGetBucketOwnership() {
		test.testGetBucketOwnership();
		testV2.testGetBucketOwnership();
	}

	/**
	 * 버킷을 생성할때 소유권 설정 확인
	 */
	@Test
	@Tag("Put")
	void testCreateBucketWithOwnership() {
		test.testCreateBucketWithOwnership();
		testV2.testCreateBucketWithOwnership();
	}

	/**
	 * 버킷의 소유권 변경 확인
	 */
	@Test
	@Tag("Put")
	void testChangeBucketOwnership() {
		test.testChangeBucketOwnership();
		testV2.testChangeBucketOwnership();
	}

	/**
	 * [BucketOwnerEnforced] 버킷 ACL 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testBucketOwnershipDenyACL() {
		test.testBucketOwnershipDenyACL();
		testV2.testBucketOwnershipDenyACL();
	}

	/**
	 * [BucketOwnerEnforced] 오브젝트 ACL 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testBucketOwnershipDenyObjectACL() {
		test.testBucketOwnershipDenyObjectACL();
		testV2.testBucketOwnershipDenyObjectACL();
	}

	/**
	 * ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경해도 접근 가능한지 확인
	 */
	@Test
	@Tag("Check")
	void testObjectOwnershipDenyChange() {
		test.testObjectOwnershipDenyChange();
		testV2.testObjectOwnershipDenyChange();
	}

	/**
	 * ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경할경우 ACL 설정이 실패하는지 확인
	 */
	@Test
	@Tag("Error")
	void testObjectOwnershipDenyACL() {
		test.testObjectOwnershipDenyACL();
		testV2.testObjectOwnershipDenyACL();
	}
}
