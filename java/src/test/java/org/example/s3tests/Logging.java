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

class Logging {
	org.example.test.Logging test = new org.example.test.Logging();
	org.example.testV2.Logging testV2 = new org.example.testV2.Logging();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 조회 가능한지 확인
	void testLoggingGet() {
		test.testLoggingGet();
		testV2.testLoggingGet();
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 가능한지 확인
	void testLoggingSet() {
		test.testLoggingSet();
		testV2.testLoggingSet();
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
	void testLoggingSetGet() {
		test.testLoggingSetGet();
		testV2.testLoggingSetGet();
	}

	@Test
	@Tag("Put/Get")
	// 버킷의 로깅에 Prefix가 설정되는지 확인
	void testLoggingPrefix() {
		test.testLoggingPrefix();
		testV2.testLoggingPrefix();
	}
	
	@Test
	@Tag("Versioning")
	// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
	void testLoggingVersioning(){
		test.testLoggingVersioning();
		testV2.testLoggingVersioning();
	}
	
	@Test
	@Tag("Encryption")
	// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
	void testLoggingEncryption(){
		test.testLoggingEncryption();
		testV2.testLoggingEncryption();
	}

	@Test
	@Tag("Error")
	// 존재하지 않는 버킷에 로깅 설정 실패 확인
	void testLoggingBucketNotFound() {
		test.testLoggingBucketNotFound();
		testV2.testLoggingBucketNotFound();
	}

	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
	void testLoggingTargetBucketNotFound() {
		test.testLoggingTargetBucketNotFound();
		testV2.testLoggingTargetBucketNotFound();
	}
}
