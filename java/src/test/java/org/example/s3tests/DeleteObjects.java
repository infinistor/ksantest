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
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("DeleteObjects Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("DeleteObjects End");
	}


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

	/**
	 * 버저닝된 버킷에서 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
	 */
	@Test
	@Tag("versioning")
	void testDeleteObjectsWithVersioningDeleteMarker() {
		test.testDeleteObjectsWithVersioningDeleteMarker();
		testV2.testDeleteObjectsWithVersioningDeleteMarker();
	}

	/**
	 * 버저닝된 버킷에서 여러 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
	 */
	@Test
	@Tag("versioning")
	void testVersioningMultiObjectDeleteWithMarker() {
		test.testVersioningMultiObjectDeleteWithMarker();
		testV2.testVersioningMultiObjectDeleteWithMarker();
	}

	/**
	 * 버저닝된 버킷에서 존재하지 않는 오브젝트를 반복 삭제할 경우 DeleteMarker가 생성되는지 확인
	 */
	@Test
	@Tag("versioning")
	void testVersioningMultiObjectDeleteWithMarkerCreate() {
		test.testVersioningMultiObjectDeleteWithMarkerCreate();
		testV2.testVersioningMultiObjectDeleteWithMarkerCreate();
	}

	/**
	 * 버저닝된 버킷에서 존재하지 않는 여러개의 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
	 */
	@Test
	@Tag("versioning")
	void testVersioningMultiObjectDeleteWithMarkerCreateObjects() {
		test.testVersioningMultiObjectDeleteWithMarkerCreateObjects();
		testV2.testVersioningMultiObjectDeleteWithMarkerCreateObjects();
	}

	/**
	 * 일치하는 If-Match 조건으로 오브젝트 삭제 성공 확인
	 */
	@Test
	@Tag("IfMatch")
	void testDeleteObjectIfMatchGood() {
		testV2.testDeleteObjectIfMatchGood();
	}

	/**
	 * 일치하지 않는 If-Match 조건으로 오브젝트 삭제 시 412 실패 확인
	 */
	@Test
	@Tag("IfMatch")
	void testDeleteObjectIfMatchFailed() {
		testV2.testDeleteObjectIfMatchFailed();
	}

	/**
	 * If-Match: * 조건으로 존재하는 오브젝트 삭제 성공 확인
	 */
	@Test
	@Tag("IfMatch")
	void testDeleteObjectIfMatchAny() {
		testV2.testDeleteObjectIfMatchAny();
	}

	/**
	 * If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testDeleteObjectIfMatchAndIfNoneMatch() {
		testV2.testDeleteObjectIfMatchAndIfNoneMatch();
	}

	/**
	 * If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testDeleteObjectIfMatchAndIfNoneMatchAny() {
		testV2.testDeleteObjectIfMatchAndIfNoneMatchAny();
	}

	/**
	 * 모든 오브젝트의 ETag 조건이 일치하는 DeleteObjects 성공 확인
	 */
	@Test
	@Tag("IfMatch")
	void testDeleteObjectsIfMatchGood() {
		testV2.testDeleteObjectsIfMatchGood();
	}

	/**
	 * ETag 조건이 일치하지 않는 오브젝트만 삭제에 실패(PreconditionFailed)하는지 확인
	 */
	@Test
	@Tag("IfMatch")
	void testDeleteObjectsIfMatchMixed() {
		testV2.testDeleteObjectsIfMatchMixed();
	}

	/**
	 * If-Match와 If-None-Match를 함께 지정하면 DeleteObjects가 501로 거부되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testDeleteObjectsIfMatchAndIfNoneMatch() {
		testV2.testDeleteObjectsIfMatchAndIfNoneMatch();
	}

	/**
	 * If-Match와 If-None-Match: * 를 함께 지정하면 DeleteObjects가 501로 거부되는지 확인
	 */
	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	void testDeleteObjectsIfMatchAndIfNoneMatchAny() {
		testV2.testDeleteObjectsIfMatchAndIfNoneMatchAny();
	}

	/**
	 * 없는 키를 삭제해도 성공하는지 확인
	 */
	@Test
	@Tag("Check")
	void testDeleteObjectNotExist() {
		test.testDeleteObjectNotExist();
		testV2.testDeleteObjectNotExist();
	}

	/**
	 * 없는 버킷에 삭제를 요청하면 404 NoSuchBucket인지 확인
	 */
	@Test
	@Tag("ERROR")
	void testDeleteObjectBucketNotExist() {
		test.testDeleteObjectBucketNotExist();
		testV2.testDeleteObjectBucketNotExist();
	}

	/**
	 * 권한이 없는 사용자의 삭제가 거부되는지 확인(DeleteObjects는 키마다 AccessDenied)
	 */
	@Test
	@Tag("Access")
	void testDeleteObjectAccessDenied() {
		test.testDeleteObjectAccessDenied();
		testV2.testDeleteObjectAccessDenied();
	}

	/**
	 * 버저닝을 쓰지 않는 버킷에서 null 버전 삭제가 성공하고 잘못된 버전 ID는 400인지 확인
	 */
	@Test
	@Tag("Version")
	void testDeleteObjectVersionIdUnversioned() {
		test.testDeleteObjectVersionIdUnversioned();
		testV2.testDeleteObjectVersionIdUnversioned();
	}

	/**
	 * DeleteObjects로 없는 키를 지워도 삭제된 것으로 응답하는지 확인
	 */
	@Test
	@Tag("Check")
	void testDeleteObjectsNotExist() {
		test.testDeleteObjectsNotExist();
		testV2.testDeleteObjectsNotExist();
	}

	/**
	 * 삭제 목록이 빈 DeleteObjects가 400 MalformedXML로 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testDeleteObjectsEmpty() {
		test.testDeleteObjectsEmpty();
		testV2.testDeleteObjectsEmpty();
	}

	/**
	 * DeleteObjects가 1000개까지 허용되고 1001개는 400 MalformedXML인지 확인
	 */
	@Test
	@Tag("Max")
	void testDeleteObjectsMaxKeys() {
		test.testDeleteObjectsMaxKeys();
		testV2.testDeleteObjectsMaxKeys();
	}

	/**
	 * DeleteObjects에 같은 키를 두 번 넣어도 한 번만 응답하는지 확인
	 */
	@Test
	@Tag("Check")
	void testDeleteObjectsDuplicateKeys() {
		test.testDeleteObjectsDuplicateKeys();
		testV2.testDeleteObjectsDuplicateKeys();
	}

	/**
	 * DeleteObjects에 잘못된 버전 ID를 주면 키마다 NoSuchVersion을 돌려주는지 확인
	 */
	@Test
	@Tag("Version")
	void testDeleteObjectsInvalidVersionId() {
		test.testDeleteObjectsInvalidVersionId();
		testV2.testDeleteObjectsInvalidVersionId();
	}

	/**
	 * 버저닝 버킷에서 없는 키를 지워도 삭제 마커가 생기는지 확인
	 */
	@Test
	@Tag("Version")
	void testDeleteObjectVersioningNotExist() {
		test.testDeleteObjectVersioningNotExist();
		testV2.testDeleteObjectVersioningNotExist();
	}

	/**
	 * 버저닝 버킷에서 잘못된 버전 ID 삭제는 400이고 없는 null 버전 삭제는 성공하는지 확인
	 */
	@Test
	@Tag("Version")
	void testDeleteObjectVersioningInvalidVersionId() {
		test.testDeleteObjectVersioningInvalidVersionId();
		testV2.testDeleteObjectVersioningInvalidVersionId();
	}

	/**
	 * 삭제 마커를 지우면 이전 버전이 다시 보이는지 확인
	 */
	@Test
	@Tag("Version")
	void testDeleteObjectRemoveDeleteMarker() {
		test.testDeleteObjectRemoveDeleteMarker();
		testV2.testDeleteObjectRemoveDeleteMarker();
	}
}
