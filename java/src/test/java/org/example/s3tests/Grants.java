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

class Grants {

	org.example.test.Grants test = new org.example.test.Grants();
	org.example.testV2.Grants testV2 = new org.example.testV2.Grants();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	void testBucketAclDefault() {
		test.testBucketAclDefault();
		testV2.testBucketAclDefault();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인
	void testBucketAclCannedDuringCreate() {
		test.testBucketAclCannedDuringCreate();
		testV2.testBucketAclCannedDuringCreate();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read => bucketAcl : private] 권한을 public-read로 생성한
	// 버킷을 private로 변경할경우 올바르게 적용되는지 확인
	void testBucketAclCanned() {
		test.testBucketAclCanned();
		testV2.testBucketAclCanned();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가
	// 올바른지 확인
	void testBucketAclCannedPublicRW() {
		test.testBucketAclCannedPublicRW();
		testV2.testBucketAclCannedPublicRW();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의
	// acl정보가 올바른지 확인
	void testBucketAclCannedAuthenticatedRead() {
		test.testBucketAclCannedAuthenticatedRead();
		testV2.testBucketAclCannedAuthenticatedRead();
	}

	@Test
	@Tag("Object")
	// [objectAcl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인
	void testObjectAclDefault() {
		test.testObjectAclDefault();
		testV2.testObjectAclDefault();
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclCannedDuringCreate() {
		test.testObjectAclCannedDuringCreate();
		testV2.testObjectAclCannedDuringCreate();
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read => objectAcl : private] 권한을 public-read로 생성한
	// 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인
	void testObjectAclCanned() {
		test.testObjectAclCanned();
		testV2.testObjectAclCanned();
	}

	@Test
	@Tag("Object")
	// [objectAcl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의
	// acl정보가 올바른지 확인
	void testObjectAclCannedPublicRW() {
		test.testObjectAclCannedPublicRW();
		testV2.testObjectAclCannedPublicRW();
	}

	@Test
	@Tag("Object")
	// [objectAcl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의
	// acl정보가 올바른지 확인
	void testObjectAclCannedAuthenticatedRead() {
		test.testObjectAclCannedAuthenticatedRead();
		testV2.testObjectAclCannedAuthenticatedRead();
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write =>
	// objectAcl : bucket-owner-read]" +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인
	void testObjectAclCannedBucketOwnerRead() {
		test.testObjectAclCannedBucketOwnerRead();
		testV2.testObjectAclCannedBucketOwnerRead();
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write =>
	// objectAcl : bucket-owner-full-control] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인
	void testObjectAclCannedBucketOwnerFullControl() {
		test.testObjectAclCannedBucketOwnerFullControl();
		testV2.testObjectAclCannedBucketOwnerFullControl();
	}

	@Test
	@Tag("Object")
	// [bucketAcl: public-read-write] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게
	// FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의
	// 소유자가 유지되는지 확인
	void testObjectAclFullControlVerifyOwner() {
		test.testObjectAclFullControlVerifyOwner();
		testV2.testObjectAclFullControlVerifyOwner();
	}

	@Test
	@Tag("ETag")
	// [bucketAcl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인
	void testObjectAclFullControlVerifyAttributes() {
		test.testObjectAclFullControlVerifyAttributes();
		testV2.testObjectAclFullControlVerifyAttributes();
	}

	@Test
	@Tag("Permission")
	// [bucketAcl:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	void testBucketAclCannedPrivateToPrivate() {
		test.testBucketAclCannedPrivateToPrivate();
		testV2.testBucketAclCannedPrivateToPrivate();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	void testObjectAcl() {
		test.testObjectAcl();
		testV2.testObjectAcl();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	void testObjectAclWrite() {
		test.testObjectAclWrite();
		testV2.testObjectAclWrite();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	void testObjectAclWriteAcp() {
		test.testObjectAclWriteAcp();
		testV2.testObjectAclWriteAcp();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	void testObjectAclRead() {
		test.testObjectAclRead();
		testV2.testObjectAclRead();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	void testObjectAclReadAcp() {
		test.testObjectAclReadAcp();
		testV2.testObjectAclReadAcp();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	void testBucketAclGrantUserFullControl() {
		test.testBucketAclGrantUserFullControl();
		testV2.testBucketAclGrantUserFullControl();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	void testBucketAclGrantUserRead() {
		test.testBucketAclGrantUserRead();
		testV2.testBucketAclGrantUserRead();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	void testBucketAclGrantUserReadAcp() {
		test.testBucketAclGrantUserReadAcp();
		testV2.testBucketAclGrantUserReadAcp();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	void testBucketAclGrantUserWrite() {
		test.testBucketAclGrantUserWrite();
		testV2.testBucketAclGrantUserWrite();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	void testBucketAclGrantUserWriteAcp() {
		test.testBucketAclGrantUserWriteAcp();
		testV2.testBucketAclGrantUserWriteAcp();
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	void testBucketAclGrantNonExistUser() {
		test.testBucketAclGrantNonExistUser();
		testV2.testBucketAclGrantNonExistUser();
	}

	@Test
	@Tag("ERROR")
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
	void testBucketAclNoGrants() {
		test.testBucketAclNoGrants();
		testV2.testBucketAclNoGrants();
	}

	@Test
	@Tag("Header")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	void testObjectHeaderAclGrants() {
		test.testObjectHeaderAclGrants();
		testV2.testObjectHeaderAclGrants();
	}

	@Test
	@Tag("Header")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	void testBucketHeaderAclGrants() {
		test.testBucketHeaderAclGrants();
		testV2.testBucketHeaderAclGrants();
	}

	@Test
	@Tag("Delete")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	void testBucketAclRevokeAll() {
		test.testBucketAclRevokeAll();
		testV2.testBucketAclRevokeAll();
	}

	@Test
	@Tag("Delete")
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	void testObjectAclRevokeAll() {
		test.testObjectAclRevokeAll();
		testV2.testObjectAclRevokeAll();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인
	void testAccessBucketPrivateObjectPrivate() {
		test.testAccessBucketPrivateObjectPrivate();
		testV2.testAccessBucketPrivateObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjectsV2)
	void testAccessBucketPrivateObjectV2Private() {
		test.testAccessBucketPrivateObjectV2Private();
		testV2.testAccessBucketPrivateObjectV2Private();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인
	void testAccessBucketPrivateObjectPublicRead() {
		test.testAccessBucketPrivateObjectPublicRead();
		testV2.testAccessBucketPrivateObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인(ListObjectsV2)
	void testAccessBucketPrivateObjectV2PublicRead() {
		test.testAccessBucketPrivateObjectV2PublicRead();
		testV2.testAccessBucketPrivateObjectV2PublicRead();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가
	// private권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
	// 오브젝트는 다운로드만 할 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 업로드불가)
	void testAccessBucketPrivateObjectPublicRW() {
		test.testAccessBucketPrivateObjectPublicRW();
		testV2.testAccessBucketPrivateObjectPublicRW();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가
	// private권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
	// 오브젝트는 다운로드만 할 수 있음을 확인(ListObjectsV2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이
	// public-read-write로 설정되어있어도 업로드불가)
	void testAccessBucketPrivateObjectV2PublicRW() {
		test.testAccessBucketPrivateObjectV2PublicRW();
		testV2.testAccessBucketPrivateObjectV2PublicRW();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:private] 메인유저가 public-read권한으로 생성한
	// 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인
	void testAccessBucketPublicReadObjectPrivate() {
		test.testAccessBucketPublicReadObjectPrivate();
		testV2.testAccessBucketPublicReadObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read, private] 메인유저가
	// public-read권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드
	// 할 수 있음을 확인
	void testAccessBucketPublicReadObjectPublicRead() {
		test.testAccessBucketPublicReadObjectPublicRead();
		testV2.testAccessBucketPublicReadObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read-write, private] 메인유저가
	// public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
	// 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 수정불가)
	void testAccessBucketPublicReadObjectPublicRW() {
		test.testAccessBucketPublicReadObjectPublicRW();
		testV2.testAccessBucketPublicReadObjectPublicRW();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나
	// 업로드는 가능하지만 다운로드 할 수 없음을 확인
	void testAccessBucketPublicRWObjectPrivate() {
		test.testAccessBucketPublicRWObjectPrivate();
		testV2.testAccessBucketPublicRWObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
	// 읽거나 업로드, 다운로드 모두 가능함을 확인
	void testAccessBucketPublicRWObjectPublicRead() {
		test.testAccessBucketPublicRWObjectPublicRead();
		testV2.testAccessBucketPublicRWObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read-write, private]
	// 메인유저가 public-read-write권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는
	// 오브젝트 목록을 읽거나 업로드, 다운로드 모두 가능함을 확인
	void testAccessBucketPublicRWObjectPublicRW() {
		test.testAccessBucketPublicRWObjectPublicRW();
		testV2.testAccessBucketPublicRWObjectPublicRW();
	}
}
