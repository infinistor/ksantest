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
 * 오브젝트 삭제 기능 테스트
 */
class DeleteObjects {

	org.example.test.DeleteObjects test = new org.example.test.DeleteObjects();
	org.example.testV2.DeleteObjects testV2 = new org.example.testV2.DeleteObjects();

	/**
	 * 테스트 정리 작업 수행
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
	 */
	@Test
	@Tag("ListObject")
	void testMultiObjectDelete() {
		test.testMultiObjectDelete();
		testV2.testMultiObjectDelete();
	}

	/**
	 * 버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)
	 */
	@Test
	@Tag("ListObjectsV2")
	void testMultiObjectV2Delete() {
		test.testMultiObjectV2Delete();
		testV2.testMultiObjectV2Delete();
	}

	/**
	 * 버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제
	 */
	@Test
	@Tag("Versioning")
	void testMultiObjectDeleteVersions() {
		test.testMultiObjectDeleteVersions();
		testV2.testMultiObjectDeleteVersions();
	}

	/**
	 * quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
	 */
	@Test
	@Tag("quiet")
	void testMultiObjectDeleteQuiet() {
		test.testMultiObjectDeleteQuiet();
		testV2.testMultiObjectDeleteQuiet();
	}

	/**
	 * 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	 */
	@Test
	@Tag("Directory")
	void testDirectoryDelete() {
		test.testDirectoryDelete();
		testV2.testDirectoryDelete();
	}

	/**
	 * 버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	 */
	@Test
	@Tag("versioning")
	void testDirectoryDeleteVersions() {
		test.testDirectoryDeleteVersions();
		testV2.testDirectoryDeleteVersions();
	}

	/**
	 * 삭제한 오브젝트가 재대로 삭제 되었는지 확인
	 */
	@Test
	@Tag("DeleteObjects")
	void testDeleteObjects() {
		test.testDeleteObjects();
		testV2.testDeleteObjects();
	}

	/**
	 * 버저닝 된 버켓에서 버전 정보를 포함한 삭제가 정상 동작하는지 확인
	 */
	@Test
	@Tag("versioning")
	void testDeleteObjectsWithVersioning() {
		test.testDeleteObjectsWithVersioning();
		testV2.testDeleteObjectsWithVersioning();
	}
}
