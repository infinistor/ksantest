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

	org.example.test.Grants Test = new org.example.test.Grants();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Bucket")
	// [bucketAcl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	void testBucketAclDefault() {
		Test.testBucketAclDefault();
	}

	@Test
	@Tag("KSAN")
	@Tag("Bucket")
	// [bucketAcl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인
	void testBucketAclCannedDuringCreate() {
		Test.testBucketAclCannedDuringCreate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Bucket")
	// [bucketAcl : public-read => bucketAcl : private] 권한을 public-read로 생성한
	// 버킷을 private로 변경할경우 올바르게 적용되는지 확인
	void testBucketAclCanned() {
		Test.testBucketAclCanned();
	}

	@Test
	@Tag("KSAN")
	@Tag("Bucket")
	// [bucketAcl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가
	// 올바른지 확인
	void testBucketAclCannedPublicRW() {
		Test.testBucketAclCannedPublicRW();
	}

	@Test
	@Tag("KSAN")
	@Tag("Bucket")
	// [bucketAcl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의
	// acl정보가 올바른지 확인
	void testBucketAclCannedAuthenticatedRead() {
		Test.testBucketAclCannedAuthenticatedRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [objectAcl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인
	void testObjectAclDefault() {
		Test.testObjectAclDefault();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [objectAcl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclCannedDuringCreate() {
		Test.testObjectAclCannedDuringCreate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [objectAcl : public-read => objectAcl : private] 권한을 public-read로 생성한
	// 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인
	void testObjectAclCanned() {
		Test.testObjectAclCanned();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [objectAcl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의
	// acl정보가 올바른지 확인
	void testObjectAclCannedPublicRW() {
		Test.testObjectAclCannedPublicRW();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [objectAcl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의
	// acl정보가 올바른지 확인
	void testObjectAclCannedAuthenticatedRead() {
		Test.testObjectAclCannedAuthenticatedRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write =>
	// objectAcl : bucket-owner-read]" +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인
	void testObjectAclCannedBucketOwnerRead() {
		Test.testObjectAclCannedBucketOwnerRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [bucketAcl: public-read-write] [objectAcl : public-read-write =>
	// objectAcl : bucket-owner-full-control] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
	// bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인
	void testObjectAclCannedBucketOwnerFullControl() {
		Test.testObjectAclCannedBucketOwnerFullControl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// [bucketAcl: public-read-write] " +
	// "메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게
	// FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의
	// 소유자가 유지되는지 확인
	void testObjectAclFullControlVerifyOwner() {
		Test.testObjectAclFullControlVerifyOwner();
	}

	@Test
	@Tag("KSAN")
	@Tag("ETag")
	// [bucketAcl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인
	void testObjectAclFullControlVerifyAttributes() {
		Test.testObjectAclFullControlVerifyAttributes();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// [bucketAcl:private] 기본생성한 버킷에 private 설정이 가능한지 확인
	void testBucketAclCannedPrivateToPrivate() {
		Test.testBucketAclCannedPrivateToPrivate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	void testObjectAcl() {
		Test.testObjectAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	void testObjectAclWrite() {
		Test.testObjectAclWrite();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	void testObjectAclWriteAcp() {
		Test.testObjectAclWriteAcp();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	void testObjectAclRead() {
		Test.testObjectAclRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	void testObjectAclReadAcp() {
		Test.testObjectAclReadAcp();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	void testBucketAclGrantUserFullControl() {
		Test.testBucketAclGrantUserFullControl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	void testBucketAclGrantUserRead() {
		Test.testBucketAclGrantUserRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	void testBucketAclGrantUserReadAcp() {
		Test.testBucketAclGrantUserReadAcp();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	void testBucketAclGrantUserWrite() {
		Test.testBucketAclGrantUserWrite();
	}

	@Test
	@Tag("KSAN")
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	void testBucketAclGrantUserWriteAcp() {
		Test.testBucketAclGrantUserWriteAcp();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	void testBucketAclGrantNonExistUser() {
		Test.testBucketAclGrantNonExistUser();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
	void testBucketAclNoGrants() {
		Test.testBucketAclNoGrants();
	}

	@Test
	@Tag("KSAN")
	@Tag("Header")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	void testObjectHeaderAclGrants() {
		Test.testObjectHeaderAclGrants();
	}

	@Test
	@Tag("KSAN")
	@Tag("Header")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	void testBucketHeaderAclGrants() {
		Test.testBucketHeaderAclGrants();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	void testBucketAclRevokeAll() {
		Test.testBucketAclRevokeAll();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	void testObjectAclRevokeAll() {
		Test.testObjectAclRevokeAll();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인
	void testAccessBucketPrivateObjectPrivate() {
		Test.testAccessBucketPrivateObjectPrivate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private] 메인유저가 private권한으로 생성한 버킷과
	// 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjectsV2)
	void testAccessBucketPrivateObjectV2Private() {
		Test.testAccessBucketPrivateObjectV2Private();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인
	void testAccessBucketPrivateObjectPublicRead() {
		Test.testAccessBucketPrivateObjectPublicRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read] 메인유저가 private권한으로
	// 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
	// 확인(ListObjectsV2)
	void testAccessBucketPrivateObjectV2PublicRead() {
		Test.testAccessBucketPrivateObjectV2PublicRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가
	// private권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
	// 오브젝트는 다운로드만 할 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 업로드불가)
	void testAccessBucketPrivateObjectPublicRW() {
		Test.testAccessBucketPrivateObjectPublicRW();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:private, objectAcl:private, public-read-write] 메인유저가
	// private권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
	// 오브젝트는 다운로드만 할 수 있음을 확인(ListObjectsV2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이
	// public-read-write로 설정되어있어도 업로드불가)
	void testAccessBucketPrivateObjectV2PublicRW() {
		Test.testAccessBucketPrivateObjectV2PublicRW();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:private] 메인유저가 public-read권한으로 생성한
	// 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인
	void testAccessBucketPublicReadObjectPrivate() {
		Test.testAccessBucketPublicReadObjectPrivate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read, private] 메인유저가
	// public-read권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드
	// 할 수 있음을 확인
	void testAccessBucketPublicReadObjectPublicRead() {
		Test.testAccessBucketPublicReadObjectPublicRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read, objectAcl:public-read-write, private] 메인유저가
	// public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
	// 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로
	// 설정되어있어도 수정불가)
	void testAccessBucketPublicReadObjectPublicRW() {
		Test.testAccessBucketPublicReadObjectPublicRW();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나
	// 업로드는 가능하지만 다운로드 할 수 없음을 확인
	void testAccessBucketPublicRWObjectPrivate() {
		Test.testAccessBucketPublicRWObjectPrivate();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read, private] 메인유저가
	// public-read-write권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
	// 읽거나 업로드, 다운로드 모두 가능함을 확인
	void testAccessBucketPublicRWObjectPublicRead() {
		Test.testAccessBucketPublicRWObjectPublicRead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Access")
	// [bucketAcl:public-read-write, objectAcl:public-read-write, private]
	// 메인유저가 public-read-write권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는
	// 오브젝트 목록을 읽거나 업로드, 다운로드 모두 가능함을 확인
	void testAccessBucketPublicRWObjectPublicRW() {
		Test.testAccessBucketPublicRWObjectPublicRW();
	}
}
