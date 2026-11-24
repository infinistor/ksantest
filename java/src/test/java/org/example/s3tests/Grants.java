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

/**
 * 버킷과 오브젝트의 권한 관리 테스트
 */
class Grants {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Grants Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Grants End");
	}


	org.example.test.Grants test = new org.example.test.Grants();
	org.example.testV2.Grants testV2 = new org.example.testV2.Grants();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclDefault() {
		test.testBucketAclDefault();
		testV2.testBucketAclDefault();
	}

	/**
	 * [bucket : public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclChanged() {
		test.testBucketAclChanged();
		testV2.testBucketAclChanged();
	}

	/**
	 * [bucket : private] 생성한 버킷의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclPrivate() {
		test.testBucketAclPrivate();
		testV2.testBucketAclPrivate();
	}

	/**
	 * [bucket : public-read] 생성한 버킷의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclPublicRead() {
		test.testBucketAclPublicRead();
		testV2.testBucketAclPublicRead();
	}

	/**
	 * [bucket : public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclPublicRW() {
		test.testBucketAclPublicRW();
		testV2.testBucketAclPublicRW();
	}

	/**
	 * [bucket : authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Bucket")
	void testBucketAclAuthenticatedRead() {
		test.testBucketAclAuthenticatedRead();
		testV2.testBucketAclAuthenticatedRead();
	}

	/**
	 * 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclDefault() {
		test.testObjectAclDefault();
		testV2.testObjectAclDefault();
	}

	/**
	 * [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclChange() {
		test.testObjectAclChange();
		testV2.testObjectAclChange();
	}

	/**
	 * [object:private] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclPrivate() {
		test.testObjectAclPrivate();
		testV2.testObjectAclPrivate();
	}

	/**
	 * [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclPublicRead() {
		test.testObjectAclPublicRead();
		testV2.testObjectAclPublicRead();
	}

	/**
	 * [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclPublicRW() {
		test.testObjectAclPublicRW();
		testV2.testObjectAclPublicRW();
	}

	/**
	 * [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclAuthenticatedRead() {
		test.testObjectAclAuthenticatedRead();
		testV2.testObjectAclAuthenticatedRead();
	}

	/**
	 * [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclBucketOwnerRead() {
		test.testObjectAclBucketOwnerRead();
		testV2.testObjectAclBucketOwnerRead();
	}

	/**
	 * [ObjectWriter][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testBucketObjectWriterObjectOwnerFullControl() {
		test.testBucketObjectWriterObjectOwnerFullControl();
		testV2.testBucketObjectWriterObjectOwnerFullControl();
	}

	/**
	 * [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
	 */
	@Test
	@Tag("Object")
	void testBucketOwnerEnforcedObjectOwnerFullControl() {
		test.testBucketOwnerEnforcedObjectOwnerFullControl();
		testV2.testBucketOwnerEnforcedObjectOwnerFullControl();
	}

	/**
	 * [object: public-read-write => alt-user-full-control => alt-user-read-acl] 권한을 변경해도 소유주가 변경되지 않는지 확인
	 */
	@Test
	@Tag("Object")
	void testObjectAclOwnerNotChange() {
		test.testObjectAclOwnerNotChange();
		testV2.testObjectAclOwnerNotChange();
	}

	/**
	 * 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
	 */
	@Test
	@Tag("Effect")
	void testBucketAclChangeNotEffect() {
		test.testBucketAclChangeNotEffect();
		testV2.testBucketAclChangeNotEffect();
	}

	/**
	 * [bucket:private] 버킷에 ACL 중복 설정이 가능한지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testBucketAclDuplicated() {
		test.testBucketAclDuplicated();
		testV2.testBucketAclDuplicated();
	}

	/**
	 * 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionFullControl() {
		test.testBucketPermissionFullControl();
		testV2.testBucketPermissionFullControl();
	}

	/**
	 * 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionWrite() {
		test.testBucketPermissionWrite();
		testV2.testBucketPermissionWrite();
	}

	/**
	 * 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionWriteAcp() {
		test.testBucketPermissionWriteAcp();
		testV2.testBucketPermissionWriteAcp();
	}

	/**
	 * 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionRead() {
		test.testBucketPermissionRead();
		testV2.testBucketPermissionRead();
	}

	/**
	 * 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	 */
	@Test
	@Tag("Permission")
	void testBucketPermissionReadAcp() {
		test.testBucketPermissionReadAcp();
		testV2.testBucketPermissionReadAcp();
	}

	/**
	 * 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionFullControl() {
		test.testObjectPermissionFullControl();
		testV2.testObjectPermissionFullControl();
	}

	/**
	 * 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionWrite() {
		test.testObjectPermissionWrite();
		testV2.testObjectPermissionWrite();
	}

	/**
	 * 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionWriteAcp() {
		test.testObjectPermissionWriteAcp();
		testV2.testObjectPermissionWriteAcp();
	}

	/**
	 * 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
	 */
	@Test
	@Tag("Permission")
	void testObjectPermissionRead() {
		test.testObjectPermissionRead();
		testV2.testObjectPermissionRead();
	}

	/**
	 * 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
	 */
	@Test
	@Tag("Permission")
	void testObjectAclPermissionReadAcp() {
		test.testObjectPermissionReadAcp();
		testV2.testObjectPermissionReadAcp();
	}

	/**
	 * 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclGrantNonExistUser() {
		test.testBucketAclGrantNonExistUser();
		testV2.testBucketAclGrantNonExistUser();
	}

	/**
	 * 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclNoGrants() {
		test.testBucketAclNoGrants();
		testV2.testBucketAclNoGrants();
	}

	/**
	 * 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
	 */
	@Test
	@Tag("Grant")
	void testBucketAclMultiGrants() {
		test.testBucketAclMultiGrants();
		testV2.testBucketAclMultiGrants();
	}

	/**
	 * 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
	 */
	@Test
	@Tag("Grant")
	void testObjectAclMultiGrants() {
		test.testObjectAclMultiGrants();
		testV2.testObjectAclMultiGrants();
	}

	/**
	 * 버킷의 acl 설정이 누락될 경우 실패함을 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
	// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
	void testBucketAclRevokeAll() {
		test.testBucketAclRevokeAll();
		testV2.testBucketAclRevokeAll();
	}

	/**
	 * 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
	 */
	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
	// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
	void testObjectAclRevokeAll() {
		test.testObjectAclRevokeAll();
		testV2.testObjectAclRevokeAll();
	}

	/**
	 * 버킷의 acl 설정에 Id가 누락될 경우 실패함을 확인
	 */
	@Test
	@Tag("Error")
	void testBucketAclRevokeAllId() {
		test.testBucketAclRevokeAllId();
		testV2.testBucketAclRevokeAllId();
	}

	/**
	 * 잘못된 canned ACL 로 버킷·오브젝트 ACL 을 설정하면 InvalidArgument 인지 확인
	 * SDK V1은 ACL을 열거형으로만 받아 잘못된 값을 보낼 수 없으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclInvalidCanned() {
		testV2.testBucketAclInvalidCanned();
	}

	/**
	 * canned ACL 과 권한 부여 헤더를 함께 주면 InvalidRequest 인지 확인
	 * SDK V1은 canned ACL과 권한 부여 헤더 중 하나만 보내므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testAclCannedWithGrantHeader() {
		testV2.testAclCannedWithGrantHeader();
	}

	/**
	 * canned ACL 과 본문(AccessControlPolicy)을 함께 주면 UnexpectedContent 인지 확인
	 * SDK V1은 canned ACL과 본문 중 하나만 보내므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclCannedWithBody() {
		testV2.testBucketAclCannedWithBody();
	}

	/**
	 * canned ACL, 권한 부여 헤더, 본문이 모두 없으면 MissingSecurityHeader 인지 확인
	 * SDK V1은 ACL 없는 요청을 클라이언트에서 막으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclMissing() {
		testV2.testBucketAclMissing();
	}

	/**
	 * 없는 권한을 부여하면 MalformedACLError 인지 확인
	 * SDK V1은 권한을 열거형으로만 받아 잘못된 값을 보낼 수 없으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclInvalidPermission() {
		testV2.testBucketAclInvalidPermission();
	}

	/**
	 * ACL 의 Owner 를 다른 사용자로 바꾸면 AccessDenied 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclChangeOwner() {
		test.testBucketAclChangeOwner();
		testV2.testBucketAclChangeOwner();
	}

	/**
	 * 올바르지 않은 권한 부여 헤더(형식 오류, 없는 ID, 없는 그룹)가 InvalidArgument 인지 확인
	 * SDK V1은 버킷 ACL에 권한 부여 헤더를 임의 문자열로 보낼 수 없으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclInvalidGrantHeader() {
		testV2.testBucketAclInvalidGrantHeader();
	}

	/**
	 * 없는 그룹 URI 에 권한을 부여하면 InvalidArgument 인지 확인
	 * SDK V1은 그룹을 열거형으로만 받아 잘못된 URI를 보낼 수 없으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclInvalidGroupUri() {
		testV2.testBucketAclInvalidGroupUri();
	}

	/**
	 * 권한 부여 100개는 저장되고 101개는 MalformedACLError 인지 확인
	 */
	@Test
	@Tag("Max")
	void testBucketAclMaxGrants() {
		test.testBucketAclMaxGrants();
		testV2.testBucketAclMaxGrants();
	}

	/**
	 * 없는 버킷의 ACL 조회·설정이 NoSuchBucket 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketAclBucketNotExist() {
		test.testBucketAclBucketNotExist();
		testV2.testBucketAclBucketNotExist();
	}

	/**
	 * 다른 사용자의 버킷·오브젝트 ACL 조회·설정이 AccessDenied 인지 확인
	 */
	@Test
	@Tag("Access")
	void testAclAccessDenied() {
		test.testAclAccessDenied();
		testV2.testAclAccessDenied();
	}

	/**
	 * 없는 오브젝트의 ACL 조회·설정이 NoSuchKey 인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjectAclKeyNotExist() {
		test.testObjectAclKeyNotExist();
		testV2.testObjectAclKeyNotExist();
	}

	/**
	 * null 버전의 ACL 은 조회되고 올바르지 않은 버전 ID 는 InvalidArgument 인지 확인
	 */
	@Test
	@Tag("Version")
	void testObjectAclVersionId() {
		test.testObjectAclVersionId();
		testV2.testObjectAclVersionId();
	}

	/**
	 * 최신 버전이 삭제 마커이면 NoSuchKey, 삭제 마커 버전을 지정하면 MethodNotAllowed 인지 확인
	 */
	@Test
	@Tag("Version")
	void testObjectAclDeleteMarker() {
		test.testObjectAclDeleteMarker();
		testV2.testObjectAclDeleteMarker();
	}

	/**
	 * 권한 부여 헤더로 업로드하면 지정한 권한만 남고 대상 사용자가 읽을 수 있는지 확인
	 */
	@Test
	@Tag("Check")
	void testPutObjectWithGrantHeader() {
		test.testPutObjectWithGrantHeader();
		testV2.testPutObjectWithGrantHeader();
	}
}
