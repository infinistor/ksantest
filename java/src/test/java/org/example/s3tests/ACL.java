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
import org.junit.jupiter.api.Disabled;
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
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지 확인
	void testObjectRawGet() {
		test.testObjectRawGet();
		testV2.testObjectRawGet();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	void testObjectRawGetBucketGone() {
		test.testObjectRawGetBucketGone();
		testV2.testObjectRawGetBucketGone();
	}

	@Test
	@Tag("Delete")
	// [Bucket = public-read, Object = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트를 삭제할때 에러 확인
	void testObjectDeleteKeyBucketGone() {
		test.testObjectDeleteKeyBucketGone();
		testV2.testObjectDeleteKeyBucketGone();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	void testObjectRawGetObjectGone() {
		test.testObjectRawGetObjectGone();
		testV2.testObjectRawGetObjectGone();
	}

	@Test
	@Tag("Get")
	// [Bucket = private, Object = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawGetBucketAcl() {
		test.testObjectRawGetBucketAcl();
		testV2.testObjectRawGetBucketAcl();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에
	// 접근할때 에러확인
	void testObjectRawGetObjectAcl() {
		test.testObjectRawGetObjectAcl();
		testV2.testObjectRawGetObjectAcl();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 로그인한 사용자가 공용 버킷의 공용
	// 오브젝트에 접근 가능한지 확인
	void testObjectRawAuthenticated() {
		test.testObjectRawAuthenticated();
		testV2.testObjectRawAuthenticated();
	}

	@Test
	@Tag("Header")
	// [Bucket = private, Object = private] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고
	// 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인
	void testObjectRawResponseHeaders() {
		test.testObjectRawResponseHeaders();
		testV2.testObjectRawResponseHeaders();
	}

	@Test
	@Tag("Get")
	// [Bucket = private, Object = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawAuthenticatedBucketAcl() {
		test.testObjectRawAuthenticatedBucketAcl();
		testV2.testObjectRawAuthenticatedBucketAcl();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근
	// 가능한지 확인
	void testObjectRawAuthenticatedObjectAcl() {
		test.testObjectRawAuthenticatedObjectAcl();
		testV2.testObjectRawAuthenticatedObjectAcl();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	void testObjectRawAuthenticatedBucketGone() {
		test.testObjectRawAuthenticatedBucketGone();
		testV2.testObjectRawAuthenticatedBucketGone();
	}

	@Test
	@Tag("Get")
	// [Bucket = public-read, Object = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	void testObjectRawAuthenticatedObjectGone() {
		test.testObjectRawAuthenticatedObjectGone();
		testV2.testObjectRawAuthenticatedObjectGone();
	}

	@Test
	@Tag("Post")
	// [Bucket = public-read, Object = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의
	// 공용 오브젝트에 URL 형식으로 접근 가능한지 확인
	void testObjectRawGetXAmzExpiresNotExpired() {
		test.testObjectRawGetXAmzExpiresNotExpired();
		testV2.testObjectRawGetXAmzExpiresNotExpired();
	}

	@Disabled("Expires가 60초 미만일경우 서버에서 60초로 강제로 고정하므로 테스트에 시간 소요가 길어 테스트에서 제외합니다.")
	@Test
	@Tag("Post")
	// [Bucket = public-read, Object = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용
	// 오브젝트에 URL 형식으로 접근 실패 확인
	void testObjectRawGetXAmzExpiresOutRangeZero() {
		test.testObjectRawGetXAmzExpiresOutRangeZero();
		testV2.testObjectRawGetXAmzExpiresOutRangeZero();
	}

	@Disabled("Expires가 60초 미만일경우 서버에서 60초로 강제로 고정하므로 테스트에 시간 소요가 길어 테스트에서 제외합니다.")
	@Test
	@Tag("Post")
	// [Bucket = public-read, Object = public-read] 로그인 유효주기가 만료된 사용자가 공용
	// 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	void testObjectRawGetXAmzExpiresOutPositiveRange() {
		test.testObjectRawGetXAmzExpiresOutPositiveRange();
		testV2.testObjectRawGetXAmzExpiresOutPositiveRange();
	}

	@Test
	@Tag("Update")
	// [Bucket = Default, Object = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를
	// 권한없는 사용자가 업데이트하려고 할때 실패 확인
	void testObjectAnonPut() {
		test.testObjectAnonPut();
		testV2.testObjectAnonPut();
	}

	@Test
	@Tag("Update")
	// [Bucket = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는
	// 사용자가 업데이트했을때 올바르게 적용 되는지 확인
	void testObjectAnonPutWriteAccess() {
		test.testObjectAnonPutWriteAccess();
		testV2.testObjectAnonPutWriteAccess();
	}

	@Test
	@Tag("Default")
	// [Bucket = Default, Object = Default] 로그인한 사용자가 버켓을 만들고 업로드
	void testObjectPutAuthenticated() {
		test.testObjectPutAuthenticated();
		testV2.testObjectPutAuthenticated();
	}

	@Disabled("Expires가 60초 미만일경우 서버에서 60초로 강제로 고정하므로 테스트에 시간 소요가 길어 테스트에서 제외합니다.")
	@Test
	@Tag("Default")
	// [Bucket = Default, Object = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트 업데이트 실패 확인
	void testObjectRawPutAuthenticatedExpired() {
		test.testObjectRawPutAuthenticatedExpired();
		testV2.testObjectRawPutAuthenticatedExpired();
	}

	@Test
	@Tag("Get")
	// [Bucket = private, Object = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	void testAclPrivateBucketPublicReadObject() {
		test.testAclPrivateBucketPublicReadObject();
		testV2.testAclPrivateBucketPublicReadObject();
	}
}
