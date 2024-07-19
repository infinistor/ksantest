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

class Access {

	org.example.test.Access test = new org.example.test.Access();
	org.example.testV2.Access testV2 = new org.example.testV2.Access();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 설정 확인
	void testPutPublicBlock() {
		test.testPutPublicBlock();
		testV2.testPutPublicBlock();
	}

	@Test
	@Tag("Denied")
	// [접근권한 설정에 public 무시 설정] 버킷의 권한설정 실패 확인
	void testBlockPublicPutBucketAcls() {
		test.testBlockPublicPutBucketAcls();
		testV2.testBlockPublicPutBucketAcls();
	}

	@Test
	@Tag("Denied")
	// [접근권한 설정에 public 무시 설정] 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인
	void testBlockPublicObjectCannedAcls() {
		test.testBlockPublicObjectCannedAcls();
		testV2.testBlockPublicObjectCannedAcls();
	}

	@Test
	@Tag("Denied")
	// [접근권한설정에 정책으로 설정한 public 권한 무시를 설정] 버킷의 정책을 추가하려고 할때 실패 확인
	void testBlockPublicPolicy() {
		test.testBlockPublicPolicy();
		testV2.testBlockPublicPolicy();
	}

	@Test
	@Tag("Denied")
	// [접근권한블록에 ACL로 설정한 public 권한 무시를 설정] 오브젝트 권한을 public-read로 설정할 경우 접근되지 않음을 확인
	void testIgnorePublicAcls() {
		test.testIgnorePublicAcls();
		testV2.testIgnorePublicAcls();
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 삭제 확인
	void testDeletePublicBlock() {
		test.testDeletePublicBlock();
		testV2.testDeletePublicBlock();
	}
}