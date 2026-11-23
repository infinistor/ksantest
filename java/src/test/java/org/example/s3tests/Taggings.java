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

import java.net.MalformedURLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 버킷과 오브젝트의 태그 기능을 테스트하는 클래스
 */
class Taggings {
	@org.junit.jupiter.api.BeforeAll
	static void beforeAll() {
		System.out.println("Taggings Start");
	}

	@org.junit.jupiter.api.AfterAll
	static void afterAll() {
		System.out.println("Taggings End");
	}


	org.example.test.Taggings test = new org.example.test.Taggings();
	org.example.testV2.Taggings testV2 = new org.example.testV2.Taggings();

	/**
	 * 테스트 정리 작업 수행
	 * 
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * 버킷에 사용자 추가 태그값을 설정할경우 성공확인
	 */
	@Test
	@Tag("Check")
	void testSetTagging() {
		test.testSetTagging();
		testV2.testSetTagging();
	}

	/**
	 * 오브젝트에 태그 설정이 올바르게 적용되는지 확인
	 */
	@Test
	@Tag("Check")
	void testGetObjTagging() {
		test.testGetObjTagging();
		testV2.testGetObjTagging();
	}

	/**
	 * 오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인
	 */
	@Test
	@Tag("Check")
	void testGetObjHeadTagging() {
		test.testGetObjHeadTagging();
		testV2.testGetObjHeadTagging();
	}

	/**
	 * 추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)
	 */
	@Test
	@Tag("Max")
	void testPutMaxTags() {
		test.testPutMaxTags();
		testV2.testPutMaxTags();
	}

	/**
	 * 추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인
	 */
	@Test
	@Tag("Overflow")
	void testPutExcessTags() {
		test.testPutExcessTags();
		testV2.testPutExcessTags();
	}

	/**
	 * 태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인
	 */
	@Test
	@Tag("Max")
	void testPutMaxSizeTags() {
		test.testPutMaxSizeTags();
		testV2.testPutMaxSizeTags();
	}

	/**
	 * 태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인
	 */
	@Test
	@Tag("Overflow")
	void testPutExcessKeyTags() {
		test.testPutExcessKeyTags();
		testV2.testPutExcessKeyTags();
	}

	/**
	 * 태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인
	 */
	@Test
	@Tag("Overflow")
	void testPutExcessValTags() {
		test.testPutExcessValTags();
		testV2.testPutExcessValTags();
	}

	/**
	 * 오브젝트의 태그목록을 덮어쓰기 가능한지 확인
	 */
	@Test
	@Tag("Overwrite")
	void testPutModifyTags() {
		test.testPutModifyTags();
		testV2.testPutModifyTags();
	}

	/**
	 * 오브젝트의 태그를 삭제 가능한지 확인
	 */
	@Test
	@Tag("Delete")
	void testPutDeleteTags() {
		test.testPutDeleteTags();
		testV2.testPutDeleteTags();
	}

	/**
	 * 헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인
	 */
	@Test
	@Tag("PutObject")
	void testPutObjWithTags() {
		test.testPutObjWithTags();
		testV2.testPutObjWithTags();
	}

	/**
	 * 로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인
	 * 
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Post")
	void testPostObjectTagsAuthenticatedRequest() throws MalformedURLException {
		test.testPostObjectTagsAuthenticatedRequest();
		testV2.testPostObjectTagsAuthenticatedRequest();
	}

	/**
	 * 업로드시 오브젝트의 태그 정보를 빈 값으로 올릴 경우 성공 확인
	 */
	@Test
	@Tag("Check")
	void testGetObjNonTagging() {
		test.testGetObjNonTagging();
		testV2.testGetObjNonTagging();
	}

	/**
	 * x-amz-tagging 헤더에 URL 인코딩이 필요한 문자(공백, /, +, =)가 든 태그로 업로드하면 디코딩된 값으로 저장되는지 확인
	 */
	@Test
	@Tag("PutObject")
	void testPutObjWithTagsEncoded() {
		testV2.testPutObjWithTagsEncoded();
	}

	/**
	 * x-amz-tagging 헤더에 URL 인코딩이 필요한 문자가 든 태그로 멀티파트 업로드하면 디코딩된 값으로 저장되는지 확인
	 */
	@Test
	@Tag("Multipart")
	void testMultipartUploadWithTagsEncoded() {
		testV2.testMultipartUploadWithTagsEncoded();
	}

	/**
	 * 버킷 태그를 50개까지 넣을 수 있고 51개는 거부되는지 확인
	 */
	@Test
	@Tag("Max")
	void testBucketTaggingMax() {
		test.testBucketTaggingMax();
		testV2.testBucketTaggingMax();
	}

	/**
	 * 버킷 태그의 키가 중복되면 거부되는지 확인 (SDK v1 의 TagSet 은 키 중복을 표현할 수 없으므로 V2만)
	 */
	@Test
	@Tag("ERROR")
	void testBucketTaggingDuplicateKey() {
		testV2.testBucketTaggingDuplicateKey();
	}

	/**
	 * 올바르지 않은 버킷 태그(aws: 접두어, 길이 초과, 허용하지 않는 문자)가 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketTaggingInvalidTag() {
		test.testBucketTaggingInvalidTag();
		testV2.testBucketTaggingInvalidTag();
	}

	/**
	 * 값이 빈 버킷 태그가 그대로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketTaggingEmptyValue() {
		test.testBucketTaggingEmptyValue();
		testV2.testBucketTaggingEmptyValue();
	}

	/**
	 * 빈 태그 목록을 넣으면 버킷 태그가 지워지고, 태그가 없어도 삭제가 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testBucketTaggingEmptySet() {
		test.testBucketTaggingEmptySet();
		testV2.testBucketTaggingEmptySet();
	}

	/**
	 * 존재하지 않는 버킷의 태그 조회·설정·삭제가 NoSuchBucket 으로 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketTaggingBucketNotExist() {
		test.testBucketTaggingBucketNotExist();
		testV2.testBucketTaggingBucketNotExist();
	}

	/**
	 * 오브젝트 태그의 키가 중복되면 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjTaggingDuplicateKey() {
		test.testObjTaggingDuplicateKey();
		testV2.testObjTaggingDuplicateKey();
	}

	/**
	 * 올바르지 않은 오브젝트 태그가 거부되는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjTaggingInvalidTag() {
		test.testObjTaggingInvalidTag();
		testV2.testObjTaggingInvalidTag();
	}

	/**
	 * 허용되는 특수문자와 빈 값이 든 오브젝트 태그가 그대로 저장되는지 확인
	 */
	@Test
	@Tag("Check")
	void testObjTaggingSpecialCharacters() {
		test.testObjTaggingSpecialCharacters();
		testV2.testObjTaggingSpecialCharacters();
	}

	/**
	 * 빈 태그 목록을 넣으면 오브젝트 태그가 지워지고, 태그가 없어도 삭제가 성공하는지 확인
	 */
	@Test
	@Tag("Delete")
	void testObjTaggingEmptySet() {
		test.testObjTaggingEmptySet();
		testV2.testObjTaggingEmptySet();
	}

	/**
	 * 존재하지 않는 오브젝트의 태그 조회·설정·삭제가 NoSuchKey 로 실패하는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testObjTaggingKeyNotExist() {
		test.testObjTaggingKeyNotExist();
		testV2.testObjTaggingKeyNotExist();
	}

	/**
	 * 태그 11개를 헤더로 주고 업로드하면 실패하고 오브젝트가 만들어지지 않는지 확인
	 */
	@Test
	@Tag("Overflow")
	void testPutObjWithExcessTags() {
		test.testPutObjWithExcessTags();
		testV2.testPutObjWithExcessTags();
	}

	/**
	 * 올바르지 않은 태그를 헤더로 주고 업로드하면 실패하고 오브젝트가 만들어지지 않는지 확인
	 */
	@Test
	@Tag("ERROR")
	void testPutObjWithInvalidTags() {
		test.testPutObjWithInvalidTags();
		testV2.testPutObjWithInvalidTags();
	}

	/**
	 * 버전을 지정한 태그 설정·조회·삭제가 그 버전에만 적용되는지 확인
	 */
	@Test
	@Tag("Version")
	void testObjTaggingVersion() {
		test.testObjTaggingVersion();
		testV2.testObjTaggingVersion();
	}

	/**
	 * 올바르지 않은 버전 ID 로 태그를 요청하면 InvalidArgument 인지 확인
	 */
	@Test
	@Tag("Version")
	void testObjTaggingInvalidVersion() {
		test.testObjTaggingInvalidVersion();
		testV2.testObjTaggingInvalidVersion();
	}

	/**
	 * 최신 버전이 삭제 마커이면 태그 요청이 MethodNotAllowed 인지 확인
	 */
	@Test
	@Tag("Version")
	void testObjTaggingDeleteMarker() {
		test.testObjTaggingDeleteMarker();
		testV2.testObjTaggingDeleteMarker();
	}
}
