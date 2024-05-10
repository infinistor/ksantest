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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
class CopyObject {
	org.example.test.CopyObject Test = new org.example.test.CopyObject();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 오브젝트의 크기가 0일때 복사가 가능한지 확인
	void testObjectCopyZeroSize() {
		Test.testObjectCopyZeroSize();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 동일한 버킷에서 오브젝트 복사가 가능한지 확인
	void testObjectCopySameBucket() {
		Test.testObjectCopySameBucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("ContentType")
	// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인
	void testObjectCopyVerifyContentType() {
		Test.testObjectCopyVerifyContentType();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인
	void testObjectCopyToItself() {
		Test.testObjectCopyToItself();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	void testObjectCopyToItselfWithMetadata() {
		Test.testObjectCopyToItselfWithMetadata();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 다른 버킷으로 오브젝트 복사가 가능한지 확인
	void testObjectCopyDiffBucket() {
		Test.testObjectCopyDiffBucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// [bucket1:created main user, object:created main user / bucket2:created sub
	// user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인
	void testObjectCopyNotOwnedBucket() {
		Test.testObjectCopyNotOwnedBucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// [bucketAcl = main:full control,sub : full control | objectAcl = default]
	// 서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인
	void testObjectCopyNotOwnedObjectBucket() {
		Test.testObjectCopyNotOwnedObjectBucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인
	void testObjectCopyCannedAcl() {
		Test.testObjectCopyCannedAcl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 크고 작은 용량의 오브젝트가 복사되는지 확인
	void testObjectCopyRetainingMetadata() {
		Test.testObjectCopyRetainingMetadata();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인
	void testObjectCopyReplacingMetadata() {
		Test.testObjectCopyReplacingMetadata();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인
	void testObjectCopyBucketNotFound() {
		Test.testObjectCopyBucketNotFound();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 존재하지않는 오브젝트 복사 실패 확인
	void testObjectCopyKeyNotFound() {
		Test.testObjectCopyKeyNotFound();
	}

	@Test
	@Tag("KSAN")
	@Tag("Version")
	// 버저닝된 오브젝트 복사 확인
	void testObjectCopyVersionedBucket() {
		Test.testObjectCopyVersionedBucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("Version")
	// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인
	void testObjectCopyVersionedUrlEncoding() {
		Test.testObjectCopyVersionedUrlEncoding();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인
	void testObjectCopyVersioningMultipartUpload() {
		Test.testObjectCopyVersioningMultipartUpload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Imatch")
	// ifmatch 값을 추가하여 오브젝트를 복사할 경우 성공확인
	void testCopyObjectIfmatchGood() {
		Test.testCopyObjectIfMatchGood();
	}

	@Test
	@Tag("KSAN")
	@Tag("Imatch")
	// ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인
	void testCopyObjectIfmatchFailed() {
		Test.testCopyObjectIfMatchFailed();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	void testCopyNorSrcToNorBucketAndObj() {
		Test.testCopyNorSrcToNorBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	void testCopyNorSrcToNorBucketEncryptionObj() {
		Test.testCopyNorSrcToNorBucketEncryptionObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyNorSrcToEncryptionBucketNorObj() {
		Test.testCopyNorSrcToEncryptionBucketNorObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyNorSrcToEncryptionBucketAndObj() {
		Test.testCopyNorSrcToEncryptionBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionSrcToNorBucketAndObj() {
		Test.testCopyEncryptionSrcToNorBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		Test.testCopyEncryptionSrcToNorBucketEncryptionObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		Test.testCopyEncryptionSrcToEncryptionBucketNorObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		Test.testCopyEncryptionSrcToEncryptionBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source bucket : encryption, source obj : normal, dest bucket : normal, dest
	// obj : normal] 오브젝트 복사 성공 확인
	void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		Test.testCopyEncryptionBucketNorObjToNorBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		Test.testCopyEncryptionBucketNorObjToNorBucketEncryptionObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		Test.testCopyEncryptionBucketNorObjToEncryptionBucketNorObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		Test.testCopyEncryptionBucketNorObjToEncryptionBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		Test.testCopyEncryptionBucketAndObjToNorBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		Test.testCopyEncryptionBucketAndObjToNorBucketEncryptionObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		Test.testCopyEncryptionBucketAndObjToEncryptionBucketNorObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		Test.testCopyEncryptionBucketAndObjToEncryptionBucketAndObj();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// 일반 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToNormalSource() {
		Test.testCopyToNormalSource();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToSseS3Source() {
		Test.testCopyToSseS3Source();
	}

	@Test
	@Tag("KSAN")
	@Tag("encryption")
	// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToSseCSource() {
		Test.testCopyToSseCSource();
	}

	@Test
	@Tag("ERROR")
	// 삭제된 오브젝트 복사 실패 확인
	void testCopyToDeletedObject() {
		Test.testCopyToDeletedObject();
	}

	@Test
	@Tag("ERROR")
	// 버저닝된 버킷에서 삭제된 오브젝트 복사 실패 확인
	void testCopyToDeleteMarkerObject() {
		Test.testCopyToDeleteMarkerObject();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 추가 가능한지 확인(Versioning 설정)
	void testObjectVersioningCopyToItselfWithMetadata() {
		Test.testObjectVersioningCopyToItselfWithMetadata();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인
	void testObjectCopyToItselfWithMetadataOverwrite() {
		Test.testObjectCopyToItselfWithMetadataOverwrite();
	}

	@Test
	@Tag("KSAN")
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인(Versioning 설정)
	void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		Test.testObjectVersioningCopyToItselfWithMetadataOverwrite();
	}
}
