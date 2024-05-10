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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class Versioning {

	org.example.test.Versioning Test = new org.example.test.Versioning();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("버킷의 버저닝 옵션 변경 가능 확인
	void testVersioningBucketCreateSuspend() {
		Test.testVersioningBucketCreateSuspend();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
	@Tag("Object")
	// @Tag("버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인
	void testVersioningObjCreateReadRemoveHead() {
		Test.testVersioningObjCreateReadRemoveHead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인
	void testVersioningObjPlainNullVersionRemoval() {
		Test.testVersioningObjPlainNullVersionRemoval();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("[버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인
	void testVersioningObjPlainNullVersionOverwrite() {
		Test.testVersioningObjPlainNullVersionOverwrite();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("[버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인
	void testVersioningObjPlainNullVersionOverwriteSuspended() {
		Test.testVersioningObjPlainNullVersionOverwriteSuspended();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("버전관리를 일시중단했을때 올바르게 동작하는지 확인
	void testVersioningObjSuspendVersions() {
		Test.testVersioningObjSuspendVersions();

	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("오브젝트하나의 여러버전을 모두 삭제 가능한지 확인
	void testVersioningObjCreateVersionsRemoveAll() {
		Test.testVersioningObjCreateVersionsRemoveAll();
	}

	@Test
	@Tag("KSAN")
	@Tag("Object")
	// @Tag("이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인
	void testVersioningObjCreateVersionsRemoveSpecialNames() {
		Test.testVersioningObjCreateVersionsRemoveSpecialNames();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	//// @Tag("오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인
	void testVersioningObjCreateOverwriteMultipart() {
		Test.testVersioningObjCreateOverwriteMultipart();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	//// @Tag("오브젝트의 해당 버전 정보가 올바른지 확인
	void testVersioningObjListMarker() {
		Test.testVersioningObjListMarker();
	}

	@Test
	@Tag("KSAN")
	@Tag("Copy")
	//// @Tag("오브젝트의 버전별 복사가 가능한지 화인
	void testVersioningCopyObjVersion() {
		Test.testVersioningCopyObjVersion();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	//// @Tag("버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인
	void testVersioningMultiObjectDelete() {
		Test.testVersioningMultiObjectDelete();
	}

	@Test
	@Tag("KSAN")
	@Tag("DeleteMarker")
	//// @Tag("버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인
	void testVersioningMultiObjectDeleteWithMarker() {
		Test.testVersioningMultiObjectDeleteWithMarker();
	}

	@Test
	@Tag("KSAN")
	@Tag("DeleteMarker")
	//// @Tag("존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인
	void testVersioningMultiObjectDeleteWithMarkerCreate() {
		Test.testVersioningMultiObjectDeleteWithMarkerCreate();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	//// @Tag("오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인
	void testVersionedObjectAcl() {
		Test.testVersionedObjectAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	// @Tag("버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인
	void testVersionedObjectAclNoVersionSpecified() {
		Test.testVersionedObjectAclNoVersionSpecified();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	//// @Tag("오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인
	void testVersionedConcurrentObjectCreateAndRemove() {
		Test.testVersionedConcurrentObjectCreateAndRemove();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인
	void testVersioningBucketAtomicUploadReturnVersionId() {
		Test.testVersioningBucketAtomicUploadReturnVersionId();
	}

	@Test
	@Tag("KSAN")
	@Tag("MultiPart")
	// @Tag("버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인
	void testVersioningBucketMultipartUploadReturnVersionId() {
		Test.testVersioningBucketMultipartUploadReturnVersionId();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인
	void testVersioningGetObjectHead() {
		Test.testVersioningGetObjectHead();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
	void testVersioningLatest() {
		Test.testVersioningLatest();
	}
}
