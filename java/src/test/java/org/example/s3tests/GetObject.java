/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class GetObject {

	org.example.test.GetObject Test = new org.example.test.GetObject();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
	public void test_object_read_not_exist() {
		Test.test_object_read_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifmatch")
	// @Tag("존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
	public void test_get_object_ifmatch_good() {
		Test.test_get_object_ifmatch_good();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifmatch")
	// @Tag("오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
	public void test_get_object_ifmatch_failed() {
		Test.test_get_object_ifmatch_failed();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifnonematch")
	// @Tag("오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
	public void test_get_object_ifnonematch_good() {
		Test.test_get_object_ifnonematch_good();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifnonematch")
	// @Tag("오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
	public void test_get_object_ifnonematch_failed() {
		Test.test_get_object_ifnonematch_failed();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifmodifiedsince")
	// @Tag("[지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공
	public void test_get_object_ifmodifiedsince_good() {
		Test.test_get_object_ifmodifiedsince_good();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifmodifiedsince")
	// @Tag("[지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패
	public void test_get_object_ifmodifiedsince_failed() {
		Test.test_get_object_ifmodifiedsince_failed();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifunmodifiedsince")
	// @Tag("[지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회
	// 실패
	public void test_get_object_ifunmodifiedsince_good() {
		Test.test_get_object_ifunmodifiedsince_good();
	}

	@Test
	@Tag("KSAN")
	@Tag("Ifunmodifiedsince")
	// @Tag("[지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회
	// 성공
	public void test_get_object_ifunmodifiedsince_failed() {
		Test.test_get_object_ifunmodifiedsince_failed();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("지정한 범위로 오브젝트 다운로드가 가능한지 확인
	public void test_ranged_request_response_code() {
		Test.test_ranged_request_response_code();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
	public void test_ranged_big_request_response_code() {
		Test.test_ranged_big_request_response_code();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
	public void test_ranged_request_skip_leading_bytes_response_code() {
		Test.test_ranged_request_skip_leading_bytes_response_code();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
	public void test_ranged_request_return_trailing_bytes_response_code() {
		Test.test_ranged_request_return_trailing_bytes_response_code();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
	public void test_ranged_request_invalid_range() {
		Test.test_ranged_request_invalid_range();
	}

	@Test
	@Tag("KSAN")
	@Tag("Range")
	// @Tag("비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
	public void test_ranged_request_empty_object() {
		Test.test_ranged_request_empty_object();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_get_object_many() {
		Test.test_get_object_many();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_range_object_many() {
		Test.test_range_object_many();
	}
}
