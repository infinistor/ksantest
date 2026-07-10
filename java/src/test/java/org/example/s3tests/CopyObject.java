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

	/**
	 * 테스트 완료 후 정리 작업을 수행합니다.
	 * 
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 오브젝트의 크기가 0일때 복사가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyZeroSize() {
		test.testObjectCopyZeroSize();
		testV2.testObjectCopyZeroSize();
	}

	/**
	 * 동일한 버킷에서 오브젝트 복사가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopySameBucket() {
		test.testObjectCopySameBucket();
		testV2.testObjectCopySameBucket();
	}

	/**
	 * ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인하는 테스트
	 */
	@Test
	@Tag("ContentType")
	void testObjectCopyVerifyContentType() {
		test.testObjectCopyVerifyContentType();
		testV2.testObjectCopyVerifyContentType();
	}

	/**
	 * 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러를 확인하는 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectCopyToItself() {
		test.testObjectCopyToItself();
		testV2.testObjectCopyToItself();
	}

	/**
	 * 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인하는
	 * 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectCopyToItselfWithMetadata() {
		test.testObjectCopyToItselfWithMetadata();
		testV2.testObjectCopyToItselfWithMetadata();
	}

	/**
	 * 다른 버킷으로 오브젝트 복사가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyDiffBucket() {
		test.testObjectCopyDiffBucket();
		testV2.testObjectCopyDiffBucket();
	}

	/**
	 * [bucket1:created main user, object:created main user / bucket2:created sub
	 * user]
	 * 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyNotOwnedBucket() {
		test.testObjectCopyNotOwnedBucket();
		testV2.testObjectCopyNotOwnedBucket();
	}

	/**
	 * 다른유저의 버킷의 오브젝트를 권한이 충분할 경우 복사 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyNotOwnedObjectBucket() {
		test.testObjectCopyNotOwnedObjectBucket();
		testV2.testObjectCopyNotOwnedObjectBucket();
	}

	/**
	 * 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인하는 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectCopyCannedAcl() {
		test.testObjectCopyCannedAcl();
		testV2.testObjectCopyCannedAcl();
	}

	/**
	 * 크고 작은 용량의 오브젝트가 복사되는지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyRetainingMetadata() {
		test.testObjectCopyRetainingMetadata();
		testV2.testObjectCopyRetainingMetadata();
	}

	/**
	 * 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인하는 테스트
	 */
	@Test
	@Tag("Check")
	void testObjectCopyReplacingMetadata() {
		test.testObjectCopyReplacingMetadata();
		testV2.testObjectCopyReplacingMetadata();
	}

	/**
	 * 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패를 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testObjectCopyBucketNotFound() {
		test.testObjectCopyBucketNotFound();
		testV2.testObjectCopyBucketNotFound();
	}

	/**
	 * 존재하지않는 오브젝트 복사 실패를 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testObjectCopyKeyNotFound() {
		test.testObjectCopyKeyNotFound();
		testV2.testObjectCopyKeyNotFound();
	}

	/**
	 * 버저닝된 오브젝트 복사를 확인하는 테스트
	 */
	@Test
	@Tag("Version")
	void testObjectCopyVersioningBucket() {
		test.testObjectCopyVersioningBucket();
		testV2.testObjectCopyVersioningBucket();
	}

	/**
	 * [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("Version")
	void testObjectCopyVersioningUrlEncoding() {
		test.testObjectCopyVersioningUrlEncoding();
		testV2.testObjectCopyVersioningUrlEncoding();
	}

	/**
	 * [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사를 확인하는 테스트
	 */
	@Test
	@Tag("Multipart")
	void testObjectCopyVersioningMultipartUpload() {
		test.testObjectCopyVersioningMultipartUpload();
		testV2.testObjectCopyVersioningMultipartUpload();
	}

	/**
	 * ifMatch 값을 추가하여 오브젝트를 복사할 경우 성공을 확인하는 테스트
	 */
	@Test
	@Tag("IfMatch")
	void testCopyObjectIfMatchGood() {
		test.testCopyObjectIfMatchGood();
		testV2.testCopyObjectIfMatchGood();
	}

	/**
	 * ifMatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패를 확인하는 테스트
	 */
	@Test
	@Tag("IfMatch")
	void testCopyObjectIfMatchFailed() {
		test.testCopyObjectIfMatchFailed();
		testV2.testCopyObjectIfMatchFailed();
	}

	/**
	 * 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 복사 성공 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testCopyObjectIfNoneMatchGood() {
		testV2.testCopyObjectIfNoneMatchGood();
	}

	/**
	 * 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 복사 시 412 실패 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testCopyObjectIfNoneMatchFailed() {
		testV2.testCopyObjectIfNoneMatchFailed();
	}

	/**
	 * 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 복사 성공 확인
	 */
	@Test
	@Tag("IfModifiedSince")
	void testCopyObjectIfModifiedSinceGood() {
		testV2.testCopyObjectIfModifiedSinceGood();
	}

	/**
	 * 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 복사 시 412 실패 확인
	 */
	@Test
	@Tag("IfModifiedSince")
	void testCopyObjectIfModifiedSinceFailed() {
		testV2.testCopyObjectIfModifiedSinceFailed();
	}

	/**
	 * 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 복사 성공 확인
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testCopyObjectIfUnmodifiedSinceGood() {
		testV2.testCopyObjectIfUnmodifiedSinceGood();
	}

	/**
	 * 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 복사 시 412 실패 확인
	 */
	@Test
	@Tag("IfUnmodifiedSince")
	void testCopyObjectIfUnmodifiedSinceFailed() {
		testV2.testCopyObjectIfUnmodifiedSinceFailed();
	}

	/**
	 * copy-source-if-match(일치)와 copy-source-if-unmodified-since(불일치)를 함께 사용할 경우
	 * ETag 조건이 우선되어 복사에 성공하는지 확인
	 */
	@Test
	@Tag("IfMatch")
	void testCopyObjectIfMatchWithIfUnmodifiedSince() {
		testV2.testCopyObjectIfMatchWithIfUnmodifiedSince();
	}

	/**
	 * copy-source-if-none-match(불일치)와 copy-source-if-modified-since(일치)를 함께 사용할 경우
	 * ETag 조건이 우선되어 412가 반환되는지 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testCopyObjectIfNoneMatchWithIfModifiedSince() {
		testV2.testCopyObjectIfNoneMatchWithIfModifiedSince();
	}

	/**
	 * 대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 복사 성공 확인
	 */
	@Test
	@Tag("IfMatch")
	void testCopyObjectDestinationIfMatchGood() {
		testV2.testCopyObjectDestinationIfMatchGood();
	}

	/**
	 * 대상 오브젝트와 일치하지 않는 If-Match 조건으로 덮어쓰기 복사 시 412 실패 확인
	 */
	@Test
	@Tag("IfMatch")
	void testCopyObjectDestinationIfMatchFailed() {
		testV2.testCopyObjectDestinationIfMatchFailed();
	}

	/**
	 * 존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testCopyObjectDestinationIfNoneMatchGood() {
		testV2.testCopyObjectDestinationIfNoneMatchGood();
	}

	/**
	 * 이미 존재하는 대상 키에 If-None-Match: * 조건으로 복사 시 412 실패 확인
	 */
	@Test
	@Tag("IfNoneMatch")
	void testCopyObjectDestinationIfNoneMatchFailed() {
		testV2.testCopyObjectDestinationIfNoneMatchFailed();
	}

	/**
	 * [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을
	 * 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyNorSrcToNorBucketAndObj() {
		test.testCopyNorSrcToNorBucketAndObj();
		testV2.testCopyNorSrcToNorBucketAndObj();
	}

	/**
	 * [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyNorSrcToNorBucketEncryptionObj() {
		test.testCopyNorSrcToNorBucketEncryptionObj();
		testV2.testCopyNorSrcToNorBucketEncryptionObj();
	}

	/**
	 * [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyNorSrcToEncryptionBucketNorObj() {
		test.testCopyNorSrcToEncryptionBucketNorObj();
		testV2.testCopyNorSrcToEncryptionBucketNorObj();
	}

	/**
	 * [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyNorSrcToEncryptionBucketAndObj() {
		test.testCopyNorSrcToEncryptionBucketAndObj();
		testV2.testCopyNorSrcToEncryptionBucketAndObj();
	}

	/**
	 * [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionSrcToNorBucketAndObj() {
		test.testCopyEncryptionSrcToNorBucketAndObj();
		testV2.testCopyEncryptionSrcToNorBucketAndObj();
	}

	/**
	 * [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		test.testCopyEncryptionSrcToNorBucketEncryptionObj();
		testV2.testCopyEncryptionSrcToNorBucketEncryptionObj();
	}

	/**
	 * [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		test.testCopyEncryptionSrcToEncryptionBucketNorObj();
		testV2.testCopyEncryptionSrcToEncryptionBucketNorObj();
	}

	/**
	 * [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	 * 오브젝트 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		test.testCopyEncryptionSrcToEncryptionBucketAndObj();
		testV2.testCopyEncryptionSrcToEncryptionBucketAndObj();
	}

	/**
	 * [source bucket : encryption, source obj : normal, dest bucket : normal, dest
	 * obj : normal] 오브젝트 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		test.testCopyEncryptionBucketNorObjToNorBucketAndObj();
		testV2.testCopyEncryptionBucketNorObjToNorBucketAndObj();
	}

	/**
	 * [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		test.testCopyEncryptionBucketNorObjToNorBucketEncryptionObj();
		testV2.testCopyEncryptionBucketNorObjToNorBucketEncryptionObj();
	}

	/**
	 * [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		test.testCopyEncryptionBucketNorObjToEncryptionBucketNorObj();
		testV2.testCopyEncryptionBucketNorObjToEncryptionBucketNorObj();
	}

	/**
	 * [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		test.testCopyEncryptionBucketNorObjToEncryptionBucketAndObj();
		testV2.testCopyEncryptionBucketNorObjToEncryptionBucketAndObj();
	}

	/**
	 * [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사
	 * 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		test.testCopyEncryptionBucketAndObjToNorBucketAndObj();
		testV2.testCopyEncryptionBucketAndObjToNorBucketAndObj();
	}

	/**
	 * [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		test.testCopyEncryptionBucketAndObjToNorBucketEncryptionObj();
		testV2.testCopyEncryptionBucketAndObjToNorBucketEncryptionObj();
	}

	/**
	 * [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	 * 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		test.testCopyEncryptionBucketAndObjToEncryptionBucketNorObj();
		testV2.testCopyEncryptionBucketAndObjToEncryptionBucketNorObj();
	}

	/**
	 * [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	 * 오브젝트 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		test.testCopyEncryptionBucketAndObjToEncryptionBucketAndObj();
		testV2.testCopyEncryptionBucketAndObjToEncryptionBucketAndObj();
	}

	/**
	 * 일반 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyToNormalSource() {
		test.testCopyToNormalSource();
		testV2.testCopyToNormalSource();
	}

	/**
	 * SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
	 */
	@Test
	@Tag("encryption")
	void testCopyToSseS3Source() {
		test.testCopyToSseS3Source();
		testV2.testCopyToSseS3Source();
	}

	/**
	 * SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
	 * SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
	 */
	@Test
	@Tag("encryption")
	void testCopyToSseCSource() {
		testV2.testCopyToSseCSource();
	}

	/**
	 * 삭제된 오브젝트 복사 실패를 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testCopyToDeletedObject() {
		test.testCopyToDeletedObject();
		testV2.testCopyToDeletedObject();
	}

	/**
	 * 버저닝된 버킷에서 삭제된 오브젝트 복사 실패를 확인하는 테스트
	 */
	@Test
	@Tag("ERROR")
	void testCopyToDeleteMarkerObject() {
		test.testCopyToDeleteMarkerObject();
		testV2.testCopyToDeleteMarkerObject();
	}

	/**
	 * 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 추가 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectVersioningCopyToItselfWithMetadata() {
		test.testObjectVersioningCopyToItselfWithMetadata();
		testV2.testObjectVersioningCopyToItselfWithMetadata();
	}

	/**
	 * copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectCopyToItselfWithMetadataOverwrite() {
		test.testObjectCopyToItselfWithMetadataOverwrite();
		testV2.testObjectCopyToItselfWithMetadataOverwrite();
	}

	/**
	 * 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
	 */
	@Test
	@Tag("OverWrite")
	void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		test.testObjectVersioningCopyToItselfWithMetadataOverwrite();
		testV2.testObjectVersioningCopyToItselfWithMetadataOverwrite();
	}

	/**
	 * sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락하면 오류가 발생하는지 확인하는 테스트
	 * SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
	 */
	@Test
	@Tag("ERROR")
	void testCopyRevokeSseAlgorithm() {
		testV2.testCopyRevokeSseAlgorithm();
	}

	/**
	 * UseChunkEncoding을 사용하는 오브젝트 복사 시 체크섬 계산 및 검증을 확인하는 테스트
	 */
	@Test
	@Tag("checksum")
	void testCopyObjectChecksumUseChunkEncoding() {
		testV2.testCopyObjectChecksumUseChunkEncoding();
	}

	/**
	 * 메타데이터와 태그가 복사되는지 확인하는 테스트
	 */
	@Test
	@Tag("metadata")
	void testCopyObjectMetadataAndTags() {
		test.testCopyObjectMetadataAndTags();
		testV2.testCopyObjectMetadataAndTags();
	}
}
