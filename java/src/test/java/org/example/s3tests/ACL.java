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

	/**
	 * 테스트 완료 후 정리 작업을 수행합니다.
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * [Bucket = private, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketAndObject() {
		test.testPrivateBucketAndObject();
		testV2.testPrivateBucketAndObject();
	}

	/**
	 * [Bucket = private, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketPublicReadObject() {
		test.testPrivateBucketPublicReadObject();
		testV2.testPrivateBucketPublicReadObject();
	}

	/**
	 * [Bucket = private, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketPublicRWObject() {
		test.testPrivateBucketPublicRWObject();
		testV2.testPrivateBucketPublicRWObject();
	}

	/**
	 * [Bucket = private, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketAuthenticatedReadObject() {
		test.testPrivateBucketAuthenticatedReadObject();
		testV2.testPrivateBucketAuthenticatedReadObject();
	}

	/**
	 * [Bucket = private, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketBucketOwnerReadObject() {
		test.testPrivateBucketBucketOwnerReadObject();
		testV2.testPrivateBucketBucketOwnerReadObject();
	}

	/**
	 * [Bucket = private, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPrivateBucketBucketOwnerFullControlObject() {
		test.testPrivateBucketBucketOwnerFullControlObject();
		testV2.testPrivateBucketBucketOwnerFullControlObject();
	}

	/**
	 * [Bucket = public-read, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketPrivateObject() {
		test.testPublicReadBucketPrivateObject();
		testV2.testPublicReadBucketPrivateObject();
	}

	/**
	 * [Bucket = public-read, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketAndObject() {
		test.testPublicReadBucketAndObject();
		testV2.testPublicReadBucketAndObject();
	}

	/**
	 * [Bucket = public-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketPublicRWObject() {
		test.testPublicReadBucketPublicRWObject();
		testV2.testPublicReadBucketPublicRWObject();
	}

	/**
	 * [Bucket = public-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketAuthenticatedReadObject() {
		test.testPublicReadBucketAuthenticatedReadObject();
		testV2.testPublicReadBucketAuthenticatedReadObject();
	}

	/**
	 * [Bucket = public-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketBucketOwnerReadObject() {
		test.testPublicReadBucketBucketOwnerReadObject();
		testV2.testPublicReadBucketBucketOwnerReadObject();
	}

	/**
	 * [Bucket = public-read, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicReadBucketBucketOwnerFullControlObject() {
		test.testPublicReadBucketBucketOwnerFullControlObject();
		testV2.testPublicReadBucketBucketOwnerFullControlObject();
	}

	/**
	 * [Bucket = public-read-write, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPrivateObject() {
		test.testPublicRWBucketPrivateObject();
		testV2.testPublicRWBucketPrivateObject();
	}

	/**
	 * [Bucket = public-read-write, Object = private, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPrivateObjectByAltUser() {
		test.testPublicRWBucketPrivateObjectByAltUser();
		testV2.testPublicRWBucketPrivateObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPublicReadObject() {
		test.testPublicRWBucketPublicReadObject();
		testV2.testPublicRWBucketPublicReadObject();
	}

	/**
	 * [Bucket = public-read-write, Object = public-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPublicReadObjectByAltUser() {
		test.testPublicRWBucketPublicReadObjectByAltUser();
		testV2.testPublicRWBucketPublicReadObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPublicRWObject() {
		test.testPublicRWBucketPublicRWObject();
		testV2.testPublicRWBucketPublicRWObject();
	}

	/**
	 * [Bucket = public-read-write, Object = public-read-write, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketPublicRWObjectByAltUser() {
		test.testPublicRWBucketPublicRWObjectByAltUser();
		testV2.testPublicRWBucketPublicRWObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketAuthenticatedReadObject() {
		test.testPublicRWBucketAuthenticatedReadObject();
		testV2.testPublicRWBucketAuthenticatedReadObject();
	}

	/**
	 * [Bucket = public-read-write, Object = authenticated-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketAuthenticatedReadObjectByAltUser() {
		test.testPublicRWBucketAuthenticatedReadObjectByAltUser();
		testV2.testPublicRWBucketAuthenticatedReadObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketBucketOwnerReadObject() {
		test.testPublicRWBucketBucketOwnerReadObject();
		testV2.testPublicRWBucketBucketOwnerReadObject();
	}

	/**
	 * [Bucket = public-read-write, Object = bucket-owner-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketBucketOwnerReadObjectByAltUser() {
		test.testPublicRWBucketBucketOwnerReadObjectByAltUser();
		testV2.testPublicRWBucketBucketOwnerReadObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketBucketOwnerFullControlObject() {
		test.testPublicRWBucketBucketOwnerFullControlObject();
		testV2.testPublicRWBucketBucketOwnerFullControlObject();
	}

	/**
	 * [Bucket = public-read-write, Object = bucket-owner-full-control, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketBucketOwnerFullControlObjectByAltUser() {
		test.testPublicRWBucketBucketOwnerFullControlObjectByAltUser();
		testV2.testPublicRWBucketBucketOwnerFullControlObjectByAltUser();
	}

	/**
	 * [Bucket = public-read-write, BucketOwnerPreferred, Object = bucket-owner-full-control, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred() {
		test.testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred();
		testV2.testPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred();
	}

	/**
	 * [Bucket = authenticated-read, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketPrivateObject() {
		test.testAuthenticatedReadBucketPrivateObject();
		testV2.testAuthenticatedReadBucketPrivateObject();
	}

	/**
	 * [Bucket = authenticated-read, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketPublicReadObject() {
		test.testAuthenticatedReadBucketPublicReadObject();
		testV2.testAuthenticatedReadBucketPublicReadObject();
	}

	/**
	 * [Bucket = authenticated-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketPublicRWObject() {
		test.testAuthenticatedReadBucketPublicRWObject();
		testV2.testAuthenticatedReadBucketPublicRWObject();
	}

	/**
	 * [Bucket = authenticated-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketAndObject() {
		test.testAuthenticatedReadBucketAndObject();
		testV2.testAuthenticatedReadBucketAndObject();
	}

	/**
	 * [Bucket = authenticated-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketBucketOwnerReadObject() {
		test.testAuthenticatedReadBucketBucketOwnerReadObject();
		testV2.testAuthenticatedReadBucketBucketOwnerReadObject();
	}

	/**
	 * [Bucket = authenticated-read, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Access")
	void testAuthenticatedReadBucketBucketOwnerFullControlObject() {
		test.testAuthenticatedReadBucketBucketOwnerFullControlObject();
		testV2.testAuthenticatedReadBucketBucketOwnerFullControlObject();
	}

	/**
	 * [Bucket = private] 오브젝트 목록 조회가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("List")
	void testPrivateBucketList() {
		test.testPrivateBucketList();
		testV2.testPrivateBucketList();
	}

	/**
	 * [Bucket = public-read] 오브젝트 목록 조회가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("List")
	void testPublicReadBucketList() {
		test.testPublicReadBucketList();
		testV2.testPublicReadBucketList();
	}

	/**
	 * [Bucket = public-read-write] 오브젝트 목록 조회가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("List")
	void testPublicRWBucketList() {
		test.testPublicRWBucketList();
		testV2.testPublicRWBucketList();
	}

	/**
	 * [Bucket = authenticated-read] 오브젝트 목록 조회가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("List")
	void testAuthenticatedReadBucketList() {
		test.testAuthenticatedReadBucketList();
		testV2.testAuthenticatedReadBucketList();
	}

	/**
	 * [Bucket = FullControl] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionAltUserFullControl() {
		test.testBucketPermissionAltUserFullControl();
		testV2.testBucketPermissionAltUserFullControl();
	}

	/**
	 * [Bucket = Read] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionAltUserRead() {
		test.testBucketPermissionAltUserRead();
		testV2.testBucketPermissionAltUserRead();
	}

	/**
	 * [Bucket = ReadAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionAltUserReadAcp() {
		test.testBucketPermissionAltUserReadAcp();
		testV2.testBucketPermissionAltUserReadAcp();
	}

	/**
	 * [Bucket = Write] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionAltUserWrite() {
		test.testBucketPermissionAltUserWrite();
		testV2.testBucketPermissionAltUserWrite();
	}

	/**
	 * [Bucket = WriteAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionAltUserWriteAcp() {
		test.testBucketPermissionAltUserWriteAcp();
		testV2.testBucketPermissionAltUserWriteAcp();
	}

	/**
	 * [Object = FullControl] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionAltUserFullControl() {
		test.testObjectPermissionAltUserFullControl();
		testV2.testObjectPermissionAltUserFullControl();
	}

	/**
	 * [Object = Read] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionAltUserRead() {
		test.testObjectPermissionAltUserRead();
		testV2.testObjectPermissionAltUserRead();
	}

	/**
	 * [Object = ReadAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionAltUserReadAcp() {
		test.testObjectPermissionAltUserReadAcp();
		testV2.testObjectPermissionAltUserReadAcp();
	}

	/**
	 * [Object = Write] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionAltUserWrite() {
		test.testObjectPermissionAltUserWrite();
		testV2.testObjectPermissionAltUserWrite();
	}

	/**
	 * [Object = WriteAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionAltUserWriteAcp() {
		test.testObjectPermissionAltUserWriteAcp();
		testV2.testObjectPermissionAltUserWriteAcp();
	}
}
