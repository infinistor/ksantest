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

class Replication {

	org.example.test.Replication Test = new org.example.test.Replication();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("버킷의 Replication 설정이 되는지 확인(put/get/delete)
	void testReplicationSet() {
		Test.testReplicationSet();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
	void testReplicationInvalidSourceBucketName() {
		Test.testReplicationInvalidSourceBucketName();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
	void testReplicationInvalidSourceBucketVersioning() {
		Test.testReplicationInvalidSourceBucketVersioning();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
	void testReplicationInvalidTargetBucketName() {
		Test.testReplicationInvalidTargetBucketName();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
	void testReplicationInvalidTargetBucketVersioning() {
		Test.testReplicationInvalidTargetBucketVersioning();
	}

}