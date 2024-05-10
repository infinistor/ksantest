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
	org.example.test.ACL Test = new org.example.test.ACL();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지
	// 확인
	void testObjectRawGet() {
		Test.testObjectRawGet();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	void testObjectRawGetBucketGone() {
		Test.testObjectRawGetBucketGone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트를 삭제할때 에러 확인
	void testObjectDeleteKeyBucketGone() {
		Test.testObjectDeleteKeyBucketGone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	void testObjectRawGetObjectGone() {
		Test.testObjectRawGetObjectGone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawGetBucketAcl() {
		Test.testObjectRawGetBucketAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에
	// 접근할때 에러확인
	void testObjectRawGetObjectAcl() {
		Test.testObjectRawGetObjectAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 공용 버킷의 공용
	// 오브젝트에 접근 가능한지 확인
	void testObjectRawAuthenticated() {
		Test.testObjectRawAuthenticated();
	}

	@Test
	@Tag("KSAN")
	@Tag("Header")
	// [Bucket_ACL = priavte, Object_ACL = priavte] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고
	// 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인
	void testObjectRawResponseHeaders() {
		Test.testObjectRawResponseHeaders();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawAuthenticatedBucketAcl() {
		Test.testObjectRawAuthenticatedBucketAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawAuthenticatedObjectAcl() {
		Test.testObjectRawAuthenticatedObjectAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	void testObjectRawAuthenticatedBucketGone() {
		Test.testObjectRawAuthenticatedBucketGone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	void testObjectRawAuthenticatedObjectGone() {
		Test.testObjectRawAuthenticatedObjectGone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의
	// 공용 오브젝트에 URL 형식으로 접근 가능한지 확인
	void testObjectRawGetXAmzExpiresNotExpired() {
		Test.testObjectRawGetXAmzExpiresNotExpired();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용
	// 오브젝트에 URL 형식으로 접근 실패 확인
	void testObjectRawGetXAmzExpiresOutRangeZero() {
		Test.testObjectRawGetXAmzExpiresOutRangeZero();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인 유효주기가 만료된 사용자가 공용
	// 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	void testObjectRawGetXAmzExpiresOutPositiveRange() {
		Test.testObjectRawGetXAmzExpiresOutPositiveRange();
	}

	@Test
	@Tag("KSAN")
	@Tag("Update")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를
	// 권한없는 사용자가 업데이트하려고 할때 실패 확인
	void testObjectAnonPut() {
		Test.testObjectAnonPut();
	}

	@Test
	@Tag("KSAN")
	@Tag("Update")
	// [Bucket_ACL = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는
	// 사용자가 업데이트했을때 올바르게 적용 되는지 확인
	void testObjectAnonPutWriteAccess() {
		Test.testObjectAnonPutWriteAccess();
	}

	@Test
	@Tag("KSAN")
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드
	void testObjectPutAuthenticated() {
		Test.testObjectPutAuthenticated();
	}

	@Test
	@Tag("KSAN")
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트
	// 업데이트 실패 확인
	void testObjectRawPutAuthenticatedExpired() {
		Test.testObjectRawPutAuthenticatedExpired();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testAclPrivateBucketPublicReadObject() {
		Test.testAclPrivateBucketPublicReadObject();
	}
}
