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
	// 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	void testBucketAclDefault() {
		test.testBucketAclDefault();
		testV2.testBucketAclDefault();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
	void testBucketAclChanged() {
		test.testBucketAclChanged();
		testV2.testBucketAclChanged();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read] 생성한 버킷의 acl정보가 올바른지 확인
	void testBucketAclPublicRead() {
		test.testBucketAclPublicRead();
		testV2.testBucketAclPublicRead();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
	void testBucketAclPublicRW() {
		test.testBucketAclPublicRW();
		testV2.testBucketAclPublicRW();
	}

	@Test
	@Tag("Bucket")
	// [bucketAcl : authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
	void testBucketAclAuthenticatedRead() {
		test.testBucketAclAuthenticatedRead();
		testV2.testBucketAclAuthenticatedRead();
	}

	@Test
	@Tag("Object")
	// 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclDefault() {
		test.testObjectAclDefault();
		testV2.testObjectAclDefault();
	}

	@Test
	@Tag("Object")
	// [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
	void testObjectAclChange() {
		test.testObjectAclChange();
		testV2.testObjectAclChange();
	}

	@Test
	@Tag("Object")
	// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclPublicRead() {
		test.testObjectAclPublicRead();
		testV2.testObjectAclPublicRead();
	}

	@Test
	@Tag("Object")
	// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclPublicRW() {
		test.testObjectAclPublicRW();
		testV2.testObjectAclPublicRW();
	}

	@Test
	@Tag("Object")
	// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclAuthenticatedRead() {
		test.testObjectAclAuthenticatedRead();
		testV2.testObjectAclAuthenticatedRead();
	}

	@Test
	@Tag("Object")
	// [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	void testObjectAclBucketOwnerRead() {
		test.testObjectAclBucketOwnerRead();
		testV2.testObjectAclBucketOwnerRead();
	}

	@Test
	@Tag("Object")
	// [ObjectWriter][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지
	// 확인
	void testBucketObjectWriterBucketOwnerFullControl() {
		test.testBucketObjectWriterBucketOwnerFullControl();
		testV2.testBucketObjectWriterBucketOwnerFullControl();
	}

	@Test
	@Tag("Object")
	// [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가
	// 올바른지 확인
	void testBucketOwnerEnforcedBucketOwnerFullControl() {
		test.testBucketOwnerEnforcedBucketOwnerFullControl();
		testV2.testBucketOwnerEnforcedBucketOwnerFullControl();
	}

	@Test
	@Tag("Object")
	// [object: public-read-write => alt-user-full-control => alt-user-read-acl]
	// 권한을 변경해도 소유주가 변경되지 않는지 확인
	void testObjectAclOwnerNotChange() {
		test.testObjectAclOwnerNotChange();
		testV2.testObjectAclOwnerNotChange();
	}

	@Test
	@Tag("Effect")
	// 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
	void testBucketAclChangeNotEffect() {
		test.testBucketAclChangeNotEffect();
		testV2.testBucketAclChangeNotEffect();
	}

	@Test
	@Tag("Permission")
	// [bucket:private] 버킷에 ACL 중복 설정이 가능한지 확인
	void testBucketAclDuplicated() {
		test.testBucketAclDuplicated();
		testV2.testBucketAclDuplicated();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	void testObjectPermissionFullControl() {
		test.testObjectPermissionFullControl();
		testV2.testObjectPermissionFullControl();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	void testObjectPermissionWrite() {
		test.testObjectPermissionWrite();
		testV2.testObjectPermissionWrite();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	void testObjectPermissionWriteAcp() {
		test.testObjectPermissionWriteAcp();
		testV2.testObjectPermissionWriteAcp();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	void testObjectPermissionRead() {
		test.testObjectPermissionRead();
		testV2.testObjectPermissionRead();
	}

	@Test
	@Tag("Permission")
	// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	void testObjectAclPermissionReadAcp() {
		test.testObjectPermissionReadAcp();
		testV2.testObjectPermissionReadAcp();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL
	void testBucketPermissionAltUserFullControl() {
		test.testBucketPermissionAltUserFullControl();
		testV2.testBucketPermissionAltUserFullControl();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ
	void testBucketPermissionAltUserRead() {
		test.testBucketPermissionAltUserRead();
		testV2.testBucketPermissionAltUserRead();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP
	void testBucketPermissionAltUserReadAcp() {
		test.testBucketPermissionAltUserReadAcp();
		testV2.testBucketPermissionAltUserReadAcp();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE
	void testBucketPermissionAltUserWrite() {
		test.testBucketPermissionAltUserWrite();
		testV2.testBucketPermissionAltUserWrite();
	}

	@Test
	@Tag("Permission")
	// 메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP
	void testBucketPermissionAltUserWriteAcp() {
		test.testBucketPermissionAltUserWriteAcp();
		testV2.testBucketPermissionAltUserWriteAcp();
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
	@Tag("Grant")
	// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	void testBucketAclMultiGrants() {
		test.testBucketAclMultiGrants();
		testV2.testBucketAclMultiGrants();
	}

	@Test
	@Tag("Grant")
	// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	void testObjectAclMultiGrants() {
		test.testObjectAclMultiGrants();
		testV2.testObjectAclMultiGrants();
	}

	@Test
	@Tag("Error")
	// 버킷의 acl 설정이 누락될 경우 실패함을 확인
	void testBucketAclRevokeAll() {
		test.testBucketAclRevokeAll();
		testV2.testBucketAclRevokeAll();
	}

	@Test
	@Tag("Error")
	// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	void testObjectAclRevokeAll() {
		test.testObjectAclRevokeAll();
		testV2.testObjectAclRevokeAll();
	}

	@Test
	@Tag("Access")
	// [bucket:private, object:private] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPrivateObjectPrivate() {
		test.testAccessBucketPrivateObjectPrivate();
		testV2.testAccessBucketPrivateObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucket:private, object:private, public-read] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPrivateObjectPublicRead() {
		test.testAccessBucketPrivateObjectPublicRead();
		testV2.testAccessBucketPrivateObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucket:private, object:private, public-read-write] Acl 설정이 올바르게 동작하는지
	// 확인
	void testAccessBucketPrivateObjectPublicRW() {
		test.testAccessBucketPrivateObjectPublicRW();
		testV2.testAccessBucketPrivateObjectPublicRW();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read, object:private] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicReadObjectPrivate() {
		test.testAccessBucketPublicReadObjectPrivate();
		testV2.testAccessBucketPublicReadObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read, object:public-read] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicReadObjectPublicRead() {
		test.testAccessBucketPublicReadObjectPublicRead();
		testV2.testAccessBucketPublicReadObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicReadObjectPublicRW() {
		test.testAccessBucketPublicReadObjectPublicRW();
		testV2.testAccessBucketPublicReadObjectPublicRW();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read-write, object:private] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicRWObjectPrivate() {
		test.testAccessBucketPublicRWObjectPrivate();
		testV2.testAccessBucketPublicRWObjectPrivate();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read-write, object:public-read] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicRWObjectPublicRead() {
		test.testAccessBucketPublicRWObjectPublicRead();
		testV2.testAccessBucketPublicRWObjectPublicRead();
	}

	@Test
	@Tag("Access")
	// [bucket:public-read-write, object:public-read-write] Acl 설정이 올바르게 동작하는지 확인
	void testAccessBucketPublicRWObjectPublicRW() {
		test.testAccessBucketPublicRWObjectPublicRW();
		testV2.testAccessBucketPublicRWObjectPublicRW();
	}
}
