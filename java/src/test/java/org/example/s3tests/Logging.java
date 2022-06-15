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

public class Logging {
	org.example.test.Logging Test = new org.example.test.Logging();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 조회 가능한지 확인
	public void test_logging_get() {
		Test.test_logging_get();
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 가능한지 확인
	public void test_logging_set() {
		Test.test_logging_set();
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
	public void test_logging_set_get() {
		Test.test_logging_set_get();
	}

	@Test
	@Tag("Put/Get")
	// 버킷의 로깅에 Prefix가 설정되는지 확인
	public void test_logging_prefix() {
		Test.test_logging_prefix();
	}
	
	@Test
	@Tag("Versioning")
	// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
	public void test_logging_versioning(){
		Test.test_logging_versioning();
	}
	
	@Test
	@Tag("Encryption")
	// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
	public void test_logging_encryption(){
		Test.test_logging_encryption();
	}

	@Test
	@Tag("Error")
	// 존재하지 않는 버킷에 로깅 설정 실패 확인
	public void test_logging_bucket_not_found() {
		Test.test_logging_bucket_not_found();
	}

	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
	public void test_logging_target_bucket_not_found() {
		Test.test_logging_target_bucket_not_found();
	}
}
