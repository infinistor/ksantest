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

class ACL {
	org.example.test.ACL test = new org.example.test.ACL();
	org.example.testV2.ACL testV2 = new org.example.testV2.ACL();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = private] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketAndObject() {
		test.testPrivateBucketAndObject();
		testV2.testPrivateBucketAndObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = public-read] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketPublicReadObject() {
		test.testPrivateBucketPublicReadObject();
		testV2.testPrivateBucketPublicReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = public-read-write] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketPublicRWObject() {
		test.testPrivateBucketPublicRWObject();
		testV2.testPrivateBucketPublicRWObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = authenticated-read] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketAuthenticatedReadObject() {
		test.testPrivateBucketAuthenticatedReadObject();
		testV2.testPrivateBucketAuthenticatedReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketBucketOwnerReadObject() {
		test.testPrivateBucketBucketOwnerReadObject();
		testV2.testPrivateBucketBucketOwnerReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = private, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인
	void testPrivateBucketBucketOwnerFullControlObject() {
		test.testPrivateBucketBucketOwnerFullControlObject();
		testV2.testPrivateBucketBucketOwnerFullControlObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = private] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketPrivateObject() {
		test.testPublicReadBucketPrivateObject();
		testV2.testPublicReadBucketPrivateObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = public-read] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketAndObject() {
		test.testPublicReadBucketAndObject();
		testV2.testPublicReadBucketAndObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketPublicRWObject() {
		test.testPublicReadBucketPublicRWObject();
		testV2.testPublicReadBucketPublicRWObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketAuthenticatedReadObject() {
		test.testPublicReadBucketAuthenticatedReadObject();
		testV2.testPublicReadBucketAuthenticatedReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketBucketOwnerReadObject() {
		test.testPublicReadBucketBucketOwnerReadObject();
		testV2.testPublicReadBucketBucketOwnerReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인
	void testPublicReadBucketBucketOwnerFullControlObject() {
		test.testPublicReadBucketBucketOwnerFullControlObject();
		testV2.testPublicReadBucketBucketOwnerFullControlObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = private] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPrivateObject() {
		test.testPublicRWBucketPrivateObject();
		testV2.testPublicRWBucketPrivateObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = private, AltUser] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPrivateObjectByAltUser() {
		test.testPublicRWBucketPrivateObjectByAltUser();
		testV2.testPublicRWBucketPrivateObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = public-read] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPublicReadObject() {
		test.testPublicRWBucketPublicReadObject();
		testV2.testPublicRWBucketPublicReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = public-read, AltUser] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPublicReadObjectByAltUser() {
		test.testPublicRWBucketPublicReadObjectByAltUser();
		testV2.testPublicRWBucketPublicReadObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = public-read-write] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPublicRWObject() {
		test.testPublicRWBucketPublicRWObject();
		testV2.testPublicRWBucketPublicRWObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = public-read-write, AltUser]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketPublicRWObjectByAltUser() {
		test.testPublicRWBucketPublicRWObjectByAltUser();
		testV2.testPublicRWBucketPublicRWObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = authenticated-read] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketAuthenticatedReadObject() {
		test.testPublicRWBucketAuthenticatedReadObject();
		testV2.testPublicRWBucketAuthenticatedReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = authenticated-read, AltUser]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketAuthenticatedReadObjectByAltUser() {
		test.testPublicRWBucketAuthenticatedReadObjectByAltUser();
		testV2.testPublicRWBucketAuthenticatedReadObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketBucketOwnerReadObject() {
		test.testPublicRWBucketBucketOwnerReadObject();
		testV2.testPublicRWBucketBucketOwnerReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = bucket-owner-read, AltUser]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketBucketOwnerReadObjectByAltUser() {
		test.testPublicRWBucketBucketOwnerReadObjectByAltUser();
		testV2.testPublicRWBucketBucketOwnerReadObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = bucket-owner-full-control]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketBucketOwnerFullControlObject() {
		test.testPublicRWBucketBucketOwnerFullControlObject();
		testV2.testPublicRWBucketBucketOwnerFullControlObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, Object = bucket-owner-full-control, AltUser]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketBucketOwnerFullControlObjectByAltUser() {
		test.testPublicRWBucketBucketOwnerFullControlObjectByAltUser();
		testV2.testPublicRWBucketBucketOwnerFullControlObjectByAltUser();
	}

	@Test
	@Tag("Access")
	// [Bucket = public-read-write, BucketOwnerPreferred,
	// Object = bucket-owner-full-control, AltUser]
	// 오브젝트에 접근 가능한지 확인
	void testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred() {
		test.testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred();
		testV2.testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = private] 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketPrivateObject() {
		test.testAuthenticatedReadBucketPrivateObject();
		testV2.testAuthenticatedReadBucketPrivateObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = public-read] 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketPublicReadObject() {
		test.testAuthenticatedReadBucketPublicReadObject();
		testV2.testAuthenticatedReadBucketPublicReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketPublicRWObject() {
		test.testAuthenticatedReadBucketPublicRWObject();
		testV2.testAuthenticatedReadBucketPublicRWObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketAndObject() {
		test.testAuthenticatedReadBucketAndObject();
		testV2.testAuthenticatedReadBucketAndObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketBucketOwnerReadObject() {
		test.testAuthenticatedReadBucketBucketOwnerReadObject();
		testV2.testAuthenticatedReadBucketBucketOwnerReadObject();
	}

	@Test
	@Tag("Access")
	// [Bucket = authenticated-read, Object = bucket-owner-full-control]
	// 오브젝트에 접근 가능한지 확인
	void testAuthenticatedReadBucketBucketOwnerFullControlObject() {
		test.testAuthenticatedReadBucketBucketOwnerFullControlObject();
		testV2.testAuthenticatedReadBucketBucketOwnerFullControlObject();
	}

	@Test
	@Tag("List")
	// [Bucket = private] 오브젝트 목록 조회가 가능한지 확인
	void testPrivateBucketList() {
		test.testPrivateBucketList();
		testV2.testPrivateBucketList();
	}

	@Test
	@Tag("List")
	// [Bucket = public-read] 오브젝트 목록 조회가 가능한지 확인
	void testPublicReadBucketList() {
		test.testPublicReadBucketList();
		testV2.testPublicReadBucketList();
	}

	@Test
	@Tag("List")
	// [Bucket = public-read-write] 오브젝트 목록 조회가 가능한지 확인
	void testPublicRWBucketList() {
		test.testPublicRWBucketList();
		testV2.testPublicRWBucketList();
	}

	@Test
	@Tag("List")
	// [Bucket = authenticated-read] 오브젝트 목록 조회가 가능한지 확인
	void testAuthenticatedReadBucketList() {
		test.testAuthenticatedReadBucketList();
		testV2.testAuthenticatedReadBucketList();
	}

	@Test
	@Tag("Permission")
	// [Bucket = FullControl] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인
	void testBucketPermissionAltUserFullControl() {
		test.testBucketPermissionAltUserFullControl();
		testV2.testBucketPermissionAltUserFullControl();
	}

	@Test
	@Tag("Permission")
	// [Bucket = Read] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인
	void testBucketPermissionAltUserRead() {
		test.testBucketPermissionAltUserRead();
		testV2.testBucketPermissionAltUserRead();
	}

	@Test
	@Tag("Permission")
	// [Bucket = ReadAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인
	void testBucketPermissionAltUserReadAcp() {
		test.testBucketPermissionAltUserReadAcp();
		testV2.testBucketPermissionAltUserReadAcp();
	}

	@Test
	@Tag("Permission")
	// [Bucket = Write] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인
	void testBucketPermissionAltUserWrite() {
		test.testBucketPermissionAltUserWrite();
		testV2.testBucketPermissionAltUserWrite();
	}

	@Test
	@Tag("Permission")
	// [Bucket = WriteAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인
	void testBucketPermissionAltUserWriteAcp() {
		test.testBucketPermissionAltUserWriteAcp();
		testV2.testBucketPermissionAltUserWriteAcp();
	}

	@Test
	@Tag("Permission")
	// [Object = FullControl] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인
	void testObjectPermissionAltUserFullControl() {
		test.testObjectPermissionAltUserFullControl();
		testV2.testObjectPermissionAltUserFullControl();
	}

	@Test
	@Tag("Permission")
	// [Object = Read] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인
	void testObjectPermissionAltUserRead() {
		test.testObjectPermissionAltUserRead();
		testV2.testObjectPermissionAltUserRead();
	}

	@Test
	@Tag("Permission")
	// [Object = ReadAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인
	void testObjectPermissionAltUserReadAcp() {
		test.testObjectPermissionAltUserReadAcp();
		testV2.testObjectPermissionAltUserReadAcp();
	}

	@Test
	@Tag("Permission")
	// [Object = Write] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인
	void testObjectPermissionAltUserWrite() {
		test.testObjectPermissionAltUserWrite();
		testV2.testObjectPermissionAltUserWrite();
	}

	@Test
	@Tag("Permission")
	// [Object = WriteAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인
	void testObjectPermissionAltUserWriteAcp() {
		test.testObjectPermissionAltUserWriteAcp();
		testV2.testObjectPermissionAltUserWriteAcp();
	}
}
