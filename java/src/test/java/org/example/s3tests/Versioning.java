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

	org.example.test.Versioning test = new org.example.test.Versioning();
	org.example.testV2.Versioning testV2 = new org.example.testV2.Versioning();

	@AfterEach
	public void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷의 버저닝 옵션 변경 가능 확인
	void testVersioningBucketCreateSuspend() {
		test.testVersioningBucketCreateSuspend();
		testV2.testVersioningBucketCreateSuspend();
	}

	@Test
	@Disabled("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
	@Tag("Object")
	// 버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인
	void testVersioningObjCreateReadRemoveHead() {
		test.testVersioningObjCreateReadRemoveHead();
		testV2.testVersioningObjCreateReadRemoveHead();
	}

	@Test
	@Tag("Object")
	// 버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인
	void testVersioningObjPlainNullVersionRemoval() {
		test.testVersioningObjPlainNullVersionRemoval();
		testV2.testVersioningObjPlainNullVersionRemoval();
	}

	@Test
	@Tag("Object")
	// [버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인
	void testVersioningObjPlainNullVersionOverwrite() {
		test.testVersioningObjPlainNullVersionOverwrite();
		testV2.testVersioningObjPlainNullVersionOverwrite();
	}

	@Test
	@Tag("Object")
	// [버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인
	void testVersioningObjPlainNullVersionOverwriteSuspended() {
		test.testVersioningObjPlainNullVersionOverwriteSuspended();
		testV2.testVersioningObjPlainNullVersionOverwriteSuspended();
	}

	@Test
	@Tag("Object")
	// 버전관리를 일시중단했을때 올바르게 동작하는지 확인
	void testVersioningObjSuspendVersions() {
		test.testVersioningObjSuspendVersions();
		testV2.testVersioningObjSuspendVersions();

	}

	@Test
	@Tag("Object")
	// 오브젝트하나의 여러버전을 모두 삭제 가능한지 확인
	void testVersioningObjCreateVersionsRemoveAll() {
		test.testVersioningObjCreateVersionsRemoveAll();
		testV2.testVersioningObjCreateVersionsRemoveAll();
	}

	@Test
	@Tag("Object")
	// 이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인
	void testVersioningObjCreateVersionsRemoveSpecialNames() {
		test.testVersioningObjCreateVersionsRemoveSpecialNames();
		testV2.testVersioningObjCreateVersionsRemoveSpecialNames();
	}

	@Test
	@Tag("Multipart")
	// 오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인
	void testVersioningObjCreateOverwriteMultipart() {
		test.testVersioningObjCreateOverwriteMultipart();
		testV2.testVersioningObjCreateOverwriteMultipart();
	}

	@Test
	@Tag("Check")
	// 오브젝트의 해당 버전 정보가 올바른지 확인
	void testVersioningObjListMarker() {
		test.testVersioningObjListMarker();
		testV2.testVersioningObjListMarker();
	}

	@Test
	@Tag("Copy")
	// 오브젝트의 버전별 복사가 가능한지 화인
	void testVersioningCopyObjVersion() {
		test.testVersioningCopyObjVersion();
		testV2.testVersioningCopyObjVersion();
	}

	@Test
	@Tag("Delete")
	// 버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인
	void testVersioningMultiObjectDelete() {
		test.testVersioningMultiObjectDelete();
		testV2.testVersioningMultiObjectDelete();
	}

	@Test
	@Tag("DeleteMarker")
	// 버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인
	void testVersioningMultiObjectDeleteWithMarker() {
		test.testVersioningMultiObjectDeleteWithMarker();
		testV2.testVersioningMultiObjectDeleteWithMarker();
	}

	@Test
	@Tag("DeleteMarker")
	// 존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인
	void testVersioningMultiObjectDeleteWithMarkerCreate() {
		test.testVersioningMultiObjectDeleteWithMarkerCreate();
		testV2.testVersioningMultiObjectDeleteWithMarkerCreate();
	}

	@Test
	@Tag("ACL")
	// 오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인
	void testVersionedObjectAcl() {
		test.testVersionedObjectAcl();
		testV2.testVersionedObjectAcl();
	}

	@Test
	@Tag("ACL")
	// 버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인
	void testVersionedObjectAclNoVersionSpecified() {
		test.testVersionedObjectAclNoVersionSpecified();
		testV2.testVersionedObjectAclNoVersionSpecified();
	}

	@Test
	@Tag("Check")
	// 오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인
	void testVersionedConcurrentObjectCreateAndRemove() {
		test.testVersionedConcurrentObjectCreateAndRemove();
		testV2.testVersionedConcurrentObjectCreateAndRemove();
	}

	@Test
	@Tag("Check")
	// 버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인
	void testVersioningBucketAtomicUploadReturnVersionId() {
		test.testVersioningBucketAtomicUploadReturnVersionId();
		testV2.testVersioningBucketAtomicUploadReturnVersionId();
	}

	@Test
	@Tag("MultiPart")
	// 버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인
	void testVersioningBucketMultipartUploadReturnVersionId() {
		test.testVersioningBucketMultipartUploadReturnVersionId();
		testV2.testVersioningBucketMultipartUploadReturnVersionId();
	}

	@Test
	@Tag("Metadata")
	// 업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인
	void testVersioningGetObjectHead() {
		test.testVersioningGetObjectHead();
		testV2.testVersioningGetObjectHead();
	}

	@Test
	@Tag("Delete")
	// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
	void testVersioningLatest() {
		test.testVersioningLatest();
		testV2.testVersioningLatest();
	}
}
