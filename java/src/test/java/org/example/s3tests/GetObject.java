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
 * 오브젝트 다운로드 기능 테스트
 */
class GetObject {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("GetObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("GetObject End");
	}


	org.example.test.GetObject test = new org.example.test.GetObject();
	org.example.testV2.GetObject testV2 = new org.example.testV2.GetObject();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjectReadNotExist() {
		test.testObjectReadNotExist();
		testV2.testObjectReadNotExist();
	}

	/**
	 * 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
	 */
	@Test
	@Tag("IfMatch")
	void testGetObjectIfMatchGood() {
		test.testGetObjectIfMatchGood();
		testV2.testGetObjectIfMatchGood();
	}

	/**
	 * 오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
	 */
	@Test
	@Tag("IfMatch")
	void testGetObjectIfMatchFailed() {
		test.testGetObjectIfMatchFailed();
		testV2.testGetObjectIfMatchFailed();
	}

	/**
	 * 오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
	 */
	@Test
	@Tag("IfNoneMatch")
	void testGetObjectIfNoneMatchGood() {
		test.testGetObjectIfNoneMatchGood();
		testV2.testGetObjectIfNoneMatchGood();
	}

	/**
	 * 오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
	 */
	@Test
	@Tag("IfNoneMatch")
	void testGetObjectIfNoneMatchFailed() {
		test.testGetObjectIfNoneMatchFailed();
		testV2.testGetObjectIfNoneMatchFailed();
	}

	/**
	 * [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifModifiedSince)보다 이후에 수정된 오브젝트를 조회 성공
	 */
	@Test
	@Tag("IfModifiedSince")
	void testGetObjectIfModifiedSinceGood() {
		test.testGetObjectIfModifiedSinceGood();
		testV2.testGetObjectIfModifiedSinceGood();
	}

	/**
	 * [지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifModifiedSince)보다 이전에 수정된 오브젝트 조회 실패
	 */
	@Test
	@Tag("IfModifiedSince")
	void testGetObjectIfModifiedSinceFailed() {
		test.testGetObjectIfModifiedSinceFailed();
		testV2.testGetObjectIfModifiedSinceFailed();
	}

	/**
	 * [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 실패
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testGetObjectIfUnmodifiedSinceGood() {
		test.testGetObjectIfUnmodifiedSinceGood();
		testV2.testGetObjectIfUnmodifiedSinceGood();
	}

	/**
	 * [지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 성공
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testGetObjectIfUnmodifiedSinceFailed() {
		test.testGetObjectIfUnmodifiedSinceFailed();
		testV2.testGetObjectIfUnmodifiedSinceFailed();
	}

	/**
	 * If-Match(일치)와 If-Unmodified-Since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 성공하는지 확인
	 */
	@Test
	@Tag("IfMatch")
	void testGetObjectIfMatchWithIfUnmodifiedSince() {
		testV2.testGetObjectIfMatchWithIfUnmodifiedSince();
	}

	/**
	 * If-None-Match(불일치)와 If-Modified-Since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 304가 반환되는지
	 * 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testGetObjectIfNoneMatchWithIfModifiedSince() {
		testV2.testGetObjectIfNoneMatchWithIfModifiedSince();
	}

	/**
	 * If-Match와 If-None-Match에 동일한 ETag를 지정하면 304가 반환되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testGetObjectIfMatchAndIfNoneMatch() {
		testV2.testGetObjectIfMatchAndIfNoneMatch();
	}

	/**
	 * If-Match와 If-None-Match: * 를 함께 지정하면 304가 반환되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testGetObjectIfMatchAndIfNoneMatchAny() {
		testV2.testGetObjectIfMatchAndIfNoneMatchAny();
	}

	/**
	 * HeadObject에서 일치하는 If-Match 조건으로 성공 확인
	 */
	@Test
	@Tag("IfMatch")
	void testHeadObjectIfMatchGood() {
		testV2.testHeadObjectIfMatchGood();
	}

	/**
	 * HeadObject에서 일치하지 않는 If-Match 조건으로 412 실패 확인
	 */
	@Test
	@Tag("IfMatch")
	void testHeadObjectIfMatchFailed() {
		testV2.testHeadObjectIfMatchFailed();
	}

	/**
	 * HeadObject에서 일치하는 If-None-Match 조건으로 304 반환 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testHeadObjectIfNoneMatchGood() {
		testV2.testHeadObjectIfNoneMatchGood();
	}

	/**
	 * HeadObject에서 일치하지 않는 If-None-Match 조건으로 성공 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testHeadObjectIfNoneMatchFailed() {
		testV2.testHeadObjectIfNoneMatchFailed();
	}

	/**
	 * HeadObject에서 오브젝트 업로드 이전 시간의 If-Modified-Since 조건으로 성공 확인
	 */
	@Test
	@Tag("IfModifiedSince")
	void testHeadObjectIfModifiedSinceGood() {
		testV2.testHeadObjectIfModifiedSinceGood();
	}

	/**
	 * HeadObject에서 오브젝트 업로드 이후 시간의 If-Modified-Since 조건으로 304 반환 확인
	 */
	@Test
	@Tag("IfModifiedSince")
	void testHeadObjectIfModifiedSinceFailed() {
		testV2.testHeadObjectIfModifiedSinceFailed();
	}

	/**
	 * HeadObject에서 오브젝트 업로드 이전 시간의 If-Unmodified-Since 조건으로 412 실패 확인
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testHeadObjectIfUnmodifiedSinceGood() {
		testV2.testHeadObjectIfUnmodifiedSinceGood();
	}

	/**
	 * HeadObject에서 오브젝트 업로드 이후 시간의 If-Unmodified-Since 조건으로 성공 확인
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testHeadObjectIfUnmodifiedSinceFailed() {
		testV2.testHeadObjectIfUnmodifiedSinceFailed();
	}

	/**
	 * 지정한 범위로 오브젝트 다운로드가 가능한지 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestResponseCode() {
		test.testRangedRequestResponseCode();
		testV2.testRangedRequestResponseCode();
	}

	/**
	 * 지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
	 */
	@Test
	@Tag("Range")
	void testRangedBigRequestResponseCode() {
		test.testRangedBigRequestResponseCode();
		testV2.testRangedBigRequestResponseCode();
	}

	/**
	 * 특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestSkipLeadingBytesResponseCode() {
		test.testRangedRequestSkipLeadingBytesResponseCode();
		testV2.testRangedRequestSkipLeadingBytesResponseCode();
	}

	/**
	 * 끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestReturnTrailingBytesResponseCode() {
		testV2.testRangedRequestReturnTrailingBytesResponseCode();
	}

	/**
	 * 오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestInvalidRange() {
		test.testRangedRequestInvalidRange();
		testV2.testRangedRequestInvalidRange();
	}

	/**
	 * 비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestEmptyObject() {
		test.testRangedRequestEmptyObject();
		testV2.testRangedRequestEmptyObject();
	}

	/**
	 * 같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
	 */
	@Test
	@Tag("Get")
	void testGetObjectMany() {
		test.testGetObjectMany();
		testV2.testGetObjectMany();
	}

	/**
	 * 같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	 */
	@Test
	@Tag("Get")
	void testRangeObjectMany() {
		test.testRangeObjectMany();
		testV2.testRangeObjectMany();
	}

	/**
	 * 모든 체크섬 알고리즘으로 업로드한 오브젝트의 Range 다운로드 내용 확인
	 */
	@Test
	@Tag("Range")
	@Tag("checksum")
	void testRangeGetChecksum() {
		testV2.testRangeGetChecksum();
	}

	/**
	 * GetObject의 반환헤더값을 설정하여 업로드 할 경우 적용되었는지 확인
	 */
	@Test
	@Tag("Header")
	void testObjectResponseHeaders() {
		test.testObjectResponseHeaders();
		testV2.testObjectResponseHeaders();
	}

	/**
	 * 멀티파트로 업로드 된 오브젝트를 다운로드 할때 파트 번호를 지정하여 다운로드 가능한지 확인
	 */
	@Test
	@Tag("Range")
	void testMultipartObjectRange() {
		testV2.testMultipartObjectRange();
	}

	/**
	 * GetObject에서 파일을 읽지 않고 버려도 무시되는지 확인
	 */
	@Test
	@Tag("Get")
	void testGetObjectIgnore() {
		test.testGetObjectIgnore();
		testV2.testGetObjectIgnore();
	}

	/**
	 * 삭제한 파일 GetObject 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAfterDelete() {
		test.testGetObjectAfterDelete();
		testV2.testGetObjectAfterDelete();
	}

	/**
	 * 버저닝한 버킷에서 삭제한 파일 GetObject 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAfterDeleteVersioning() {
		test.testGetObjectAfterDeleteVersioning();
		testV2.testGetObjectAfterDeleteVersioning();
	}

	/**
	 * 버저닝한 버킷에서 DeleteMarker로 GetObject 실패 확인
	 */
	@Test
	@Tag("Versioning")
	void testGetObjectDeleteMarker() {
		test.testGetObjectDeleteMarker();
		testV2.testGetObjectDeleteMarker();
	}

	/**
	 * 없는 버킷의 오브젝트를 조회하면 404 NoSuchBucket인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectBucketNotExist() {
		test.testGetObjectBucketNotExist();
		testV2.testGetObjectBucketNotExist();
	}

	/**
	 * 없는 오브젝트를 HeadObject하면 404인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testHeadObjectNotExist() {
		test.testHeadObjectNotExist();
		testV2.testHeadObjectNotExist();
	}

	/**
	 * 권한이 없는 사용자와 익명 사용자의 조회가 403 AccessDenied로 거부되는지 확인
	 */
	@Test
	@Tag("Access")
	void testGetObjectAccessDenied() {
		test.testGetObjectAccessDenied();
		testV2.testGetObjectAccessDenied();
	}

	/**
	 * 해석할 수 없는 Range는 무시하고 전체를 200으로 돌려주는지 확인
	 * SDK V1은 Range를 숫자 구간으로만 받아 형식이 다른 값을 보낼 수 없으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("Range")
	void testRangedRequestIgnored() {
		testV2.testRangedRequestIgnored();
	}

	/**
	 * Range가 오브젝트 끝을 넘으면 잘라서 돌려주고 시작이 크기 이상이면 416 InvalidRange인지 확인
	 */
	@Test
	@Tag("Range")
	void testRangedRequestBeyondEnd() {
		test.testRangedRequestBeyondEnd();
		testV2.testRangedRequestBeyondEnd();
	}

	/**
	 * HeadObject에 Range를 주면 206과 Content-Range를 돌려주는지 확인
	 * SDK V1의 HeadObject 요청은 Range를 지원하지 않으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("Range")
	void testHeadObjectRange() {
		testV2.testHeadObjectRange();
	}

	/**
	 * 멀티파트가 아닌 오브젝트를 partNumber=1로 조회하면 전체를 돌려주고 2 이상은 416 InvalidPartNumber인지 확인
	 */
	@Test
	@Tag("PartNumber")
	void testGetObjectPartNumber() {
		test.testGetObjectPartNumber();
		testV2.testGetObjectPartNumber();
	}

	/**
	 * 범위를 벗어난 partNumber와 Range 동시 지정이 400으로 거부되는지 확인
	 */
	@Test
	@Tag("PartNumber")
	void testGetObjectPartNumberInvalid() {
		test.testGetObjectPartNumberInvalid();
		testV2.testGetObjectPartNumberInvalid();
	}

	/**
	 * 버저닝을 쓰지 않는 버킷에서 null 버전 조회가 성공하고 잘못된 버전 ID는 400인지 확인
	 */
	@Test
	@Tag("Version")
	void testGetObjectVersionIdUnversioned() {
		test.testGetObjectVersionIdUnversioned();
		testV2.testGetObjectVersionIdUnversioned();
	}

	/**
	 * 버저닝 버킷에 없는 null 버전을 조회하면 404 NoSuchVersion인지 확인
	 */
	@Test
	@Tag("Version")
	void testGetObjectNullVersionNotExist() {
		test.testGetObjectNullVersionNotExist();
		testV2.testGetObjectNullVersionNotExist();
	}

	/**
	 * 익명 요청에 response-* 재정의를 쓰면 400 InvalidRequest로 거부되는지 확인
	 */
	@Test
	@Tag("Header")
	void testGetObjectResponseHeadersAnonymous() {
		test.testGetObjectResponseHeadersAnonymous();
		testV2.testGetObjectResponseHeadersAnonymous();
	}

	/**
	 * HeadObject에도 response-* 재정의가 적용되는지 확인
	 * SDK V1의 HeadObject 요청은 response-* 재정의를 지원하지 않으므로 V2만 테스트한다.
	 */
	@Test
	@Tag("Header")
	void testHeadObjectResponseHeaders() {
		testV2.testHeadObjectResponseHeaders();
	}

	/**
	 * Get/Head 응답의 기본 헤더(Accept-Ranges, Content-Length, ETag, Last-Modified)를 확인
	 */
	@Test
	@Tag("Header")
	void testGetObjectDefaultHeaders() {
		test.testGetObjectDefaultHeaders();
		testV2.testGetObjectDefaultHeaders();
	}

	/**
	 * If-Match: * 는 통과하고 If-None-Match: * 는 304인지 확인
	 */
	@Test
	@Tag("IfMatch")
	void testGetObjectIfMatchWildcard() {
		test.testGetObjectIfMatchWildcard();
		testV2.testGetObjectIfMatchWildcard();
	}
}
