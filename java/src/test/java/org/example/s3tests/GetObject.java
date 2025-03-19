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

class GetObject {

	org.example.test.GetObject test = new org.example.test.GetObject();
	org.example.testV2.GetObject testV2 = new org.example.testV2.GetObject();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
	void testObjectReadNotExist() {
		test.testObjectReadNotExist();
		testV2.testObjectReadNotExist();
	}

	@Test
	@Tag("IfMatch")
	// 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
	void testGetObjectIfMatchGood() {
		test.testGetObjectIfMatchGood();
		testV2.testGetObjectIfMatchGood();
	}

	@Test
	@Tag("IfMatch")
	// 오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
	void testGetObjectIfMatchFailed() {
		test.testGetObjectIfMatchFailed();
		testV2.testGetObjectIfMatchFailed();
	}

	@Test
	@Tag("IfNoneMatch")
	// 오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
	void testGetObjectIfNoneMatchGood() {
		test.testGetObjectIfNoneMatchGood();
		testV2.testGetObjectIfNoneMatchGood();
	}

	@Test
	@Tag("IfNoneMatch")
	// 오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
	void testGetObjectIfNoneMatchFailed() {
		test.testGetObjectIfNoneMatchFailed();
		testV2.testGetObjectIfNoneMatchFailed();
	}

	@Test
	@Tag("IfModifiedSince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifModifiedSince)보다 이후에 수정된 오브젝트를 조회 성공
	void testGetObjectIfModifiedSinceGood() {
		test.testGetObjectIfModifiedSinceGood();
		testV2.testGetObjectIfModifiedSinceGood();
	}

	@Test
	@Tag("IfModifiedSince")
	// [지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifModifiedSince)보다 이전에 수정된 오브젝트 조회 실패
	void testGetObjectIfModifiedSinceFailed() {
		test.testGetObjectIfModifiedSinceFailed();
		testV2.testGetObjectIfModifiedSinceFailed();
	}

	@Test
	@Tag("IfUnmodifiedSince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회
	// 실패
	void testGetObjectIfUnmodifiedSinceGood() {
		test.testGetObjectIfUnmodifiedSinceGood();
		testV2.testGetObjectIfUnmodifiedSinceGood();
	}

	@Test
	@Tag("IfUnmodifiedSince")
	// [지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회
	// 성공
	void testGetObjectIfUnmodifiedSinceFailed() {
		test.testGetObjectIfUnmodifiedSinceFailed();
		testV2.testGetObjectIfUnmodifiedSinceFailed();
	}

	@Test
	@Tag("Range")
	// 지정한 범위로 오브젝트 다운로드가 가능한지 확인
	void testRangedRequestResponseCode() {
		test.testRangedRequestResponseCode();
		testV2.testRangedRequestResponseCode();
	}

	@Test
	@Tag("Range")
	// 지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
	void testRangedBigRequestResponseCode() {
		test.testRangedBigRequestResponseCode();
		testV2.testRangedBigRequestResponseCode();
	}

	@Test
	@Tag("Range")
	// 특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
	void testRangedRequestSkipLeadingBytesResponseCode() {
		test.testRangedRequestSkipLeadingBytesResponseCode();
		testV2.testRangedRequestSkipLeadingBytesResponseCode();
	}

	@Test
	@Tag("Range")
	// 끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
	void testRangedRequestReturnTrailingBytesResponseCode() {
		test.testRangedRequestReturnTrailingBytesResponseCode();
		testV2.testRangedRequestReturnTrailingBytesResponseCode();
	}

	@Test
	@Tag("Range")
	// 오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
	void testRangedRequestInvalidRange() {
		test.testRangedRequestInvalidRange();
		testV2.testRangedRequestInvalidRange();
	}

	@Test
	@Tag("Range")
	// 비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
	void testRangedRequestEmptyObject() {
		test.testRangedRequestEmptyObject();
		testV2.testRangedRequestEmptyObject();
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
	void testGetObjectMany() {
		test.testGetObjectMany();
		testV2.testGetObjectMany();
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	void testRangeObjectMany() {
		test.testRangeObjectMany();
		testV2.testRangeObjectMany();
	}

	@Test
	@Tag("Header")
	// GetObject의 반환헤더값을 설정하여 업로드 할 경우 적용되었는지 확인
	void testObjectResponseHeaders() {
		test.testObjectResponseHeaders();
		testV2.testObjectResponseHeaders();
	}

	// 멀티파트로 업로드 된 오브젝트를 다운로드 할때 파트 번호를 지정하여 다운로드 가능한지 확인
	@Test
	@Tag("Range")
	void testMultipartObjectRange() {
		testV2.testMultipartObjectRange();
	}

}
