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
	org.example.test.CopyObject test = new org.example.test.CopyObject();
	org.example.testV2.CopyObject testV2 = new org.example.testV2.CopyObject();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 오브젝트의 크기가 0일때 복사가 가능한지 확인
	void testObjectCopyZeroSize() {
		test.testObjectCopyZeroSize();
		testV2.testObjectCopyZeroSize();
	}

	@Test
	@Tag("Check")
	// 동일한 버킷에서 오브젝트 복사가 가능한지 확인
	void testObjectCopySameBucket() {
		test.testObjectCopySameBucket();
		testV2.testObjectCopySameBucket();
	}

	@Test
	@Tag("ContentType")
	// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인
	void testObjectCopyVerifyContentType() {
		test.testObjectCopyVerifyContentType();
		testV2.testObjectCopyVerifyContentType();
	}

	@Test
	@Tag("OverWrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인
	void testObjectCopyToItself() {
		test.testObjectCopyToItself();
		testV2.testObjectCopyToItself();
	}

	@Test
	@Tag("OverWrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	void testObjectCopyToItselfWithMetadata() {
		test.testObjectCopyToItselfWithMetadata();
		testV2.testObjectCopyToItselfWithMetadata();
	}

	@Test
	@Tag("Check")
	// 다른 버킷으로 오브젝트 복사가 가능한지 확인
	void testObjectCopyDiffBucket() {
		test.testObjectCopyDiffBucket();
		testV2.testObjectCopyDiffBucket();
	}

	@Test
	@Tag("Check")
	// [bucket1:created main user, object:created main user / bucket2:created sub
	// user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인
	void testObjectCopyNotOwnedBucket() {
		test.testObjectCopyNotOwnedBucket();
		testV2.testObjectCopyNotOwnedBucket();
	}

	@Test
	@Tag("Check")
	// 다른유저의 버킷의 오브젝트를 권한이 충분할 경우 복사 가능한지 확인
	void testObjectCopyNotOwnedObjectBucket() {
		test.testObjectCopyNotOwnedObjectBucket();
		testV2.testObjectCopyNotOwnedObjectBucket();
	}

	@Test
	@Tag("OverWrite")
	// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인
	// 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인
	void testObjectCopyCannedAcl() {
		test.testObjectCopyCannedAcl();
		testV2.testObjectCopyCannedAcl();
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트가 복사되는지 확인
	void testObjectCopyRetainingMetadata() {
		test.testObjectCopyRetainingMetadata();
		testV2.testObjectCopyRetainingMetadata();
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인
	void testObjectCopyReplacingMetadata() {
		test.testObjectCopyReplacingMetadata();
		testV2.testObjectCopyReplacingMetadata();
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인
	void testObjectCopyBucketNotFound() {
		test.testObjectCopyBucketNotFound();
		testV2.testObjectCopyBucketNotFound();
	}

	@Test
	@Tag("ERROR")
	// 존재하지않는 오브젝트 복사 실패 확인
	void testObjectCopyKeyNotFound() {
		test.testObjectCopyKeyNotFound();
		testV2.testObjectCopyKeyNotFound();
	}

	@Test
	@Tag("Version")
	// 버저닝된 오브젝트 복사 확인
	void testObjectCopyVersionedBucket() {
		test.testObjectCopyVersionedBucket();
		testV2.testObjectCopyVersionedBucket();
	}

	@Test
	@Tag("Version")
	// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인
	void testObjectCopyVersionedUrlEncoding() {
		test.testObjectCopyVersionedUrlEncoding();
		testV2.testObjectCopyVersionedUrlEncoding();
	}

	@Test
	@Tag("Multipart")
	// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인
	void testObjectCopyVersioningMultipartUpload() {
		test.testObjectCopyVersioningMultipartUpload();
		testV2.testObjectCopyVersioningMultipartUpload();
	}

	@Test
	@Tag("IfMatch")
	// ifMatch 값을 추가하여 오브젝트를 복사할 경우 성공확인
	void testCopyObjectIfMatchGood() {
		test.testCopyObjectIfMatchGood();
		testV2.testCopyObjectIfMatchGood();
	}

	@Test
	@Tag("IfMatch")
	// ifMatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인
	void testCopyObjectIfMatchFailed() {
		test.testCopyObjectIfMatchFailed();
		testV2.testCopyObjectIfMatchFailed();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	void testCopyNorSrcToNorBucketAndObj() {
		test.testCopyNorSrcToNorBucketAndObj();
		testV2.testCopyNorSrcToNorBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	void testCopyNorSrcToNorBucketEncryptionObj() {
		test.testCopyNorSrcToNorBucketEncryptionObj();
		testV2.testCopyNorSrcToNorBucketEncryptionObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyNorSrcToEncryptionBucketNorObj() {
		test.testCopyNorSrcToEncryptionBucketNorObj();
		testV2.testCopyNorSrcToEncryptionBucketNorObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyNorSrcToEncryptionBucketAndObj() {
		test.testCopyNorSrcToEncryptionBucketAndObj();
		testV2.testCopyNorSrcToEncryptionBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionSrcToNorBucketAndObj() {
		test.testCopyEncryptionSrcToNorBucketAndObj();
		testV2.testCopyEncryptionSrcToNorBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		test.testCopyEncryptionSrcToNorBucketEncryptionObj();
		testV2.testCopyEncryptionSrcToNorBucketEncryptionObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		test.testCopyEncryptionSrcToEncryptionBucketNorObj();
		testV2.testCopyEncryptionSrcToEncryptionBucketNorObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		test.testCopyEncryptionSrcToEncryptionBucketAndObj();
		testV2.testCopyEncryptionSrcToEncryptionBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source bucket : encryption, source obj : normal, dest bucket : normal, dest
	// obj : normal] 오브젝트 복사 성공 확인
	void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		test.testCopyEncryptionBucketNorObjToNorBucketAndObj();
		testV2.testCopyEncryptionBucketNorObjToNorBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		test.testCopyEncryptionBucketNorObjToNorBucketEncryptionObj();
		testV2.testCopyEncryptionBucketNorObjToNorBucketEncryptionObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		test.testCopyEncryptionBucketNorObjToEncryptionBucketNorObj();
		testV2.testCopyEncryptionBucketNorObjToEncryptionBucketNorObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		test.testCopyEncryptionBucketNorObjToEncryptionBucketAndObj();
		testV2.testCopyEncryptionBucketNorObjToEncryptionBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		test.testCopyEncryptionBucketAndObjToNorBucketAndObj();
		testV2.testCopyEncryptionBucketAndObjToNorBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		test.testCopyEncryptionBucketAndObjToNorBucketEncryptionObj();
		testV2.testCopyEncryptionBucketAndObjToNorBucketEncryptionObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		test.testCopyEncryptionBucketAndObjToEncryptionBucketNorObj();
		testV2.testCopyEncryptionBucketAndObjToEncryptionBucketNorObj();
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		test.testCopyEncryptionBucketAndObjToEncryptionBucketAndObj();
		testV2.testCopyEncryptionBucketAndObjToEncryptionBucketAndObj();
	}

	@Test
	@Tag("encryption")
	// 일반 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToNormalSource() {
		test.testCopyToNormalSource();
		testV2.testCopyToNormalSource();
	}

	@Test
	@Tag("encryption")
	// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToSseS3Source() {
		test.testCopyToSseS3Source();
		testV2.testCopyToSseS3Source();
	}

	@Test
	@Tag("encryption")
	// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	void testCopyToSseCSource() {
		test.testCopyToSseCSource();
		testV2.testCopyToSseCSource();
	}

	@Test
	@Tag("ERROR")
	// 삭제된 오브젝트 복사 실패 확인
	void testCopyToDeletedObject() {
		test.testCopyToDeletedObject();
		testV2.testCopyToDeletedObject();
	}

	@Test
	@Tag("ERROR")
	// 버저닝된 버킷에서 삭제된 오브젝트 복사 실패 확인
	void testCopyToDeleteMarkerObject() {
		test.testCopyToDeleteMarkerObject();
		testV2.testCopyToDeleteMarkerObject();
	}

	@Test
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 추가 가능한지 확인(Versioning 설정)
	void testObjectVersioningCopyToItselfWithMetadata() {
		test.testObjectVersioningCopyToItselfWithMetadata();
		testV2.testObjectVersioningCopyToItselfWithMetadata();
	}

	@Test
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인
	void testObjectCopyToItselfWithMetadataOverwrite() {
		test.testObjectCopyToItselfWithMetadataOverwrite();
		testV2.testObjectCopyToItselfWithMetadataOverwrite();
	}

	@Test
	@Tag("OverWrite")
	// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인(Versioning 설정)
	void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		test.testObjectVersioningCopyToItselfWithMetadataOverwrite();
		testV2.testObjectVersioningCopyToItselfWithMetadataOverwrite();
	}

	@Test
	@Tag("ERROR")
	// sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락하면 오류가 발생하는지 확인
	void testCopyRevokeSseAlgorithm() {
		test.testCopyRevokeSseAlgorithm();
		testV2.testCopyRevokeSseAlgorithm();
	}

	@Test
	@Tag("checksum")
	// UseChunkEncoding을 사용하는 오브젝트 복사 시 체크섬 계산 및 검증 확인
	void testCopyObjectChecksumUseChunkEncoding() {
		testV2.testCopyObjectChecksumUseChunkEncoding();
	}
}
