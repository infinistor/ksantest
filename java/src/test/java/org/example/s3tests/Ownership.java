package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * 버킷과 오브젝트의 소유권 기능을 테스트하는 클래스
 */
class Ownership {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Ownership Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Ownership End");
	}


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
	 * ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경하면 퍼블릭 접근이 막히는지 확인
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

	/**
	 * 소유권을 지정하지 않고 만든 버킷의 소유권이 BucketOwnerEnforced인지 확인
	 */
	@Test
	@Tag("Get")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): 새 버킷의 소유권 기본값은 ObjectWriter 다.
	// SCP 등 ACL 로 권한을 주는 기존 사용 방식을 유지하려고 AWS 의 2023년 기본값(BucketOwnerEnforced)을 따르지 않는다.
	void testBucketOwnershipDefault() {
		test.testBucketOwnershipDefault();
		testV2.testBucketOwnershipDefault();
	}

	/**
	 * 버킷의 소유권을 ObjectWriter, BucketOwnerPreferred, BucketOwnerEnforced로 변경할 수 있는지 확인
	 */
	@Test
	@Tag("Put")
	void testPutBucketOwnershipAllValues() {
		test.testPutBucketOwnershipAllValues();
		testV2.testPutBucketOwnershipAllValues();
	}

	/**
	 * 버킷의 소유권을 잘못된 값으로 설정할 경우 실패 확인
	 */
	@Test
	@Tag("Error")
	void testPutBucketOwnershipInvalid() {
		test.testPutBucketOwnershipInvalid();
		testV2.testPutBucketOwnershipInvalid();
	}

	/**
	 * 버킷의 소유권 설정을 삭제하면 조회는 실패하고 다시 삭제해도 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testDeleteBucketOwnership() {
		test.testDeleteBucketOwnership();
		testV2.testDeleteBucketOwnership();
	}

	/**
	 * 존재하지 않는 버킷의 소유권을 조회, 설정, 삭제할 경우 실패 확인
	 */
	@Test
	@Tag("Error")
	void testBucketOwnershipBucketNotExist() {
		test.testBucketOwnershipBucketNotExist();
		testV2.testBucketOwnershipBucketNotExist();
	}

	/**
	 * 다른 사용자가 버킷의 소유권을 조회, 설정, 삭제할 경우 실패 확인
	 */
	@Test
	@Tag("Permission")
	void testBucketOwnershipAltUser() {
		test.testBucketOwnershipAltUser();
		testV2.testBucketOwnershipAltUser();
	}

	/**
	 * BucketOwnerEnforced 버킷을 ACL과 함께 생성할 경우 실패 확인
	 */
	@Test
	@Tag("Error")
	void testCreateBucketOwnershipEnforcedWithAcl() {
		test.testCreateBucketOwnershipEnforcedWithAcl();
		testV2.testCreateBucketOwnershipEnforcedWithAcl();
	}

	/**
	 * 잘못된 소유권 값으로 버킷을 생성할 경우 실패 확인
	 */
	@Test
	@Tag("Error")
	void testCreateBucketInvalidOwnership() {
		test.testCreateBucketInvalidOwnership();
		testV2.testCreateBucketInvalidOwnership();
	}

	/**
	 * 버킷 ACL에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced로 변경할 수 없는지 확인
	 */
	@Test
	@Tag("Error")
	void testBucketOwnershipEnforcedWithAclGrants() {
		test.testBucketOwnershipEnforcedWithAclGrants();
		testV2.testBucketOwnershipEnforcedWithAclGrants();
	}

	/**
	 * ObjectWriter 버킷에서는 올린 사용자가 오브젝트 소유자가 되는지 확인
	 */
	@Test
	@Tag("Owner")
	void testObjectOwnerObjectWriter() {
		test.testObjectOwnerObjectWriter();
		testV2.testObjectOwnerObjectWriter();
	}

	/**
	 * BucketOwnerPreferred 버킷에서는 bucket-owner-full-control로 올린 오브젝트만 버킷 소유자의 소유가 되는지 확인
	 */
	@Test
	@Tag("Owner")
	void testObjectOwnerBucketOwnerPreferred() {
		test.testObjectOwnerBucketOwnerPreferred();
		testV2.testObjectOwnerBucketOwnerPreferred();
	}

	/**
	 * BucketOwnerEnforced 버킷에서는 누가 올려도 버킷 소유자가 오브젝트 소유자가 되는지 확인
	 */
	@Test
	@Tag("Owner")
	void testObjectOwnerBucketOwnerEnforced() {
		test.testObjectOwnerBucketOwnerEnforced();
		testV2.testObjectOwnerBucketOwnerEnforced();
	}

	/**
	 * BucketOwnerEnforced 버킷에서 ACL 조회는 가능하고 ACL 변경은 거부되는지 확인
	 */
	@Test
	@Tag("Error")
	void testBucketOwnershipEnforcedAcl() {
		test.testBucketOwnershipEnforcedAcl();
		testV2.testBucketOwnershipEnforcedAcl();
	}
}
