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

public class LifeCycle {
	org.example.test.LifeCycle Test = new org.example.test.LifeCycle();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 버킷의 Lifecycle 규칙을 추가 가능한지 확인
	public void test_lifecycle_set() {
		Test.test_lifecycle_set();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// 버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인
	public void test_lifecycle_get() {
		Test.test_lifecycle_get();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// ID 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인
	public void test_lifecycle_get_no_id() {
		Test.test_lifecycle_get_no_id();
	}

	@Test
	@Tag("KSAN")
	@Tag("Version")
	// 버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인
	public void test_lifecycle_expiration_versioning_enabled() {
		Test.test_lifecycle_expiration_versioning_enabled();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 버킷에 Lifecycle 규칙을 설정할때 ID의 길이가 너무 길면 실패하는지 확인
	public void test_lifecycle_id_too_long() {
		Test.test_lifecycle_id_too_long();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// 버킷에 Lifecycle 규칙을 설정할때 같은 ID로 규칙을 여러개 설정할경우 실패하는지 확인
	public void test_lifecycle_same_id() {
		Test.test_lifecycle_same_id();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인
	public void test_lifecycle_invalid_status() {
		Test.test_lifecycle_invalid_status();
	}

	@Test
	@Tag("KSAN")
	@Tag("Date")
	// 버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인
	public void test_lifecycle_set_date() {
		Test.test_lifecycle_set_date();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인
	public void test_lifecycle_set_invalid_date() {
		Test.test_lifecycle_set_invalid_date();
	}

	@Test
	@Tag("KSAN")
	@Tag("Version")
	// 버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인
	public void test_lifecycle_set_noncurrent() {
		Test.test_lifecycle_set_noncurrent();
	}

	@Test
	@Tag("KSAN")
	@Tag("Version")
	// 버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인
	public void test_lifecycle_noncur_expiration() {
		Test.test_lifecycle_noncur_expiration();
	}

	@Test
	@Tag("KSAN")
	@Tag("DeleteMarker")
	// DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
	public void test_lifecycle_set_deletemarker() {
		Test.test_lifecycle_set_deletemarker();
	}

	@Test
	@Tag("KSAN")
	@Tag("Filter")
	// Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인
	public void test_lifecycle_set_filter() {
		Test.test_lifecycle_set_filter();
	}

	@Test
	@Tag("KSAN")
	@Tag("Filter")
	// Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인
	public void test_lifecycle_set_empty_filter() {
		Test.test_lifecycle_set_empty_filter();
	}

	@Test
	@Tag("KSAN")
	@Tag("DeleteMarker")
	// DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
	public void test_lifecycle_deletemarker_expiration() {
		Test.test_lifecycle_deletemarker_expiration();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
	public void test_lifecycle_set_multipart() {
		Test.test_lifecycle_set_multipart();

	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
	public void test_lifecycle_multipart_expiration() {
		Test.test_lifecycle_multipart_expiration();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// @Tag("버킷의 Lifecycle 규칙을 삭제 가능한지 확인
	public void test_lifecycle_delete() {
		Test.test_lifecycle_delete();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// Lifecycle 규칙에 0일을 설정할때 실패하는지 확인
	public void test_lifecycle_set_expiration_zero(){
		Test.test_lifecycle_set_expiration_zero();
	}

	@Test
	@Tag("KSAN")
	@Tag("metadata")
	// Lifecycle 규칙을 적용할 경우 오브젝트의 만료기한이 설정되는지 확인
	public void test_lifecycle_set_expiration(){
		Test.test_lifecycle_set_expiration();
	}
}
