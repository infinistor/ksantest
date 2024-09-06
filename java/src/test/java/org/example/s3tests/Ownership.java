package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class Ownership {

	org.example.test.Ownership test = new org.example.test.Ownership();
	org.example.testV2.Ownership testV2 = new org.example.testV2.Ownership();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Get")
	// 버킷의 오너십 조회 확인
	void testGetBucketOwnership() {
		test.testGetBucketOwnership();
		testV2.testGetBucketOwnership();
	}

	@Test
	@Tag("Put")
	// 버킷을 생성할때 오너십 설정 확인
	void testCreateBucketWithOwnership() {
		test.testCreateBucketWithOwnership();
		testV2.testCreateBucketWithOwnership();
	}

	@Test
	@Tag("Put")
	// 버킷의 오너십 변경 확인
	void testChangeBucketOwnership() {
		test.testChangeBucketOwnership();
		testV2.testChangeBucketOwnership();
	}

	@Test
	@Tag("Error")
	// 오너십 설정된 버킷에서 버킷 ACL 설정이 실패하는지 확인
	void testBucketOwnershipDenyACL() {
		test.testBucketOwnershipDenyACL();
		testV2.testBucketOwnershipDenyACL();
	}

	@Test
	@Tag("Error")
	// 오너십 설정된 버킷에서 오브젝트 ACL 설정이 실패하는지 확인
	void testBucketOwnershipDenyObjectACL() {
		test.testBucketOwnershipDenyObjectACL();
		testV2.testBucketOwnershipDenyObjectACL();
	}

	@Test
	@Tag("Check")
	// ACL 설정된 오브젝트에 오너십을 변경해도 접근 가능한지 확인
	void testObjectOwnershipDenyChange() {
		test.testObjectOwnershipDenyChange();
		testV2.testObjectOwnershipDenyChange();
	}

	@Test
	@Tag("Error")
	// ACL 설정된 오브젝트에 오너십을 변경할경우 ACL 설정이 실패하는지 확인
	void testObjectOwnershipDenyACL() {
		test.testObjectOwnershipDenyACL();
		testV2.testObjectOwnershipDenyACL();
	}
}
