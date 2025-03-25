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
 * POST 방식의 오브젝트 업로드 기능을 테스트하는 클래스
 */
class Post {

	org.example.test.Post test = new org.example.test.Post();
	org.example.testV2.Post testV2 = new org.example.testV2.Post();

	/**
	 * 테스트 정리 작업 수행
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	/**
	 * post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectAnonymousRequest() throws MalformedURLException {
		test.testPostObjectAnonymousRequest();
		testV2.testPostObjectAnonymousRequest();
	}

	/**
	 * post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectAuthenticatedRequest() throws MalformedURLException {
		test.testPostObjectAuthenticatedRequest();
		testV2.testPostObjectAuthenticatedRequest();
	}

	/**
	 * content-type 헤더 정보 없이 post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectAuthenticatedNoContentType() throws MalformedURLException {
		test.testPostObjectAuthenticatedNoContentType();
		testV2.testPostObjectAuthenticatedNoContentType();
	}

	/**
	 * [PostKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectAuthenticatedRequestBadAccessKey() throws MalformedURLException {
		test.testPostObjectAuthenticatedRequestBadAccessKey();
		testV2.testPostObjectAuthenticatedRequestBadAccessKey();
	}

	/**
	 * [성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("StatusCode")
	void testPostObjectSetSuccessCode() throws MalformedURLException {
		test.testPostObjectSetSuccessCode();
		testV2.testPostObjectSetSuccessCode();
	}

	/**
	 * [성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("StatusCode")
	void testPostObjectSetInvalidSuccessCode() throws MalformedURLException {
		test.testPostObjectSetInvalidSuccessCode();
		testV2.testPostObjectSetInvalidSuccessCode();
	}

	/**
	 * post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectUploadLargerThanChunk() throws MalformedURLException {
		test.testPostObjectUploadLargerThanChunk();
		testV2.testPostObjectUploadLargerThanChunk();
	}

	/**
	 * [오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectSetKeyFromFilename() throws MalformedURLException {
		test.testPostObjectSetKeyFromFilename();
		testV2.testPostObjectSetKeyFromFilename();
	}

	/**
	 * post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectIgnoredHeader() throws MalformedURLException {
		test.testPostObjectIgnoredHeader();
		testV2.testPostObjectIgnoredHeader();
	}

	/**
	 * [헤더정보에 대소문자를 섞어서 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectCaseInsensitiveConditionFields() throws MalformedURLException {
		test.testPostObjectCaseInsensitiveConditionFields();
		testV2.testPostObjectCaseInsensitiveConditionFields();
	}

	/**
	 * [오브젝트 이름에 '\'를 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectEscapedFieldValues() throws MalformedURLException {
		test.testPostObjectEscapedFieldValues();
		testV2.testPostObjectEscapedFieldValues();
	}

	/**
	 * [redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Upload")
	void testPostObjectSuccessRedirectAction() throws MalformedURLException {
		test.testPostObjectSuccessRedirectAction();
		testV2.testPostObjectSuccessRedirectAction();
	}

	/**
	 * [SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectInvalidSignature() throws MalformedURLException {
		test.testPostObjectInvalidSignature();
		testV2.testPostObjectInvalidSignature();
	}

	/**
	 * [PostKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectInvalidAccessKey() throws MalformedURLException {
		test.testPostObjectInvalidAccessKey();
		testV2.testPostObjectInvalidAccessKey();
	}

	/**
	 * [로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectInvalidDateFormat() throws MalformedURLException {
		test.testPostObjectInvalidDateFormat();
		testV2.testPostObjectInvalidDateFormat();
	}

	/**
	 * [오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectNoKeySpecified() throws MalformedURLException {
		test.testPostObjectNoKeySpecified();
		testV2.testPostObjectNoKeySpecified();
	}

	/**
	 * [signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectMissingSignature() throws MalformedURLException {
		test.testPostObjectMissingSignature();
		testV2.testPostObjectMissingSignature();
	}

	/**
	 * [policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectMissingPolicyCondition() throws MalformedURLException {
		test.testPostObjectMissingPolicyCondition();
		testV2.testPostObjectMissingPolicyCondition();
	}

	/**
	 * [사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("Metadata")
	void testPostObjectUserSpecifiedHeader() throws MalformedURLException {
		test.testPostObjectUserSpecifiedHeader();
		testV2.testPostObjectUserSpecifiedHeader();
	}

	/**
	 * [사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectRequestMissingPolicySpecifiedField() throws MalformedURLException {
		test.testPostObjectRequestMissingPolicySpecifiedField();
		testV2.testPostObjectRequestMissingPolicySpecifiedField();
	}

	/**
	 * [policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectConditionIsCaseSensitive() throws MalformedURLException {
		test.testPostObjectConditionIsCaseSensitive();
		testV2.testPostObjectConditionIsCaseSensitive();
	}

	/**
	 * [policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectExpiresIsCaseSensitive() throws MalformedURLException {
		test.testPostObjectExpiresIsCaseSensitive();
		testV2.testPostObjectExpiresIsCaseSensitive();
	}

	/**
	 * [policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectExpiredPolicy() throws MalformedURLException {
		test.testPostObjectExpiredPolicy();
		testV2.testPostObjectExpiredPolicy();
	}

	/**
	 * [사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectInvalidRequestFieldValue() throws MalformedURLException {
		test.testPostObjectInvalidRequestFieldValue();
		testV2.testPostObjectInvalidRequestFieldValue();
	}

	/**
	 * [policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectMissingExpiresCondition() throws MalformedURLException {
		test.testPostObjectMissingExpiresCondition();
		testV2.testPostObjectMissingExpiresCondition();
	}

	/**
	 * [policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectMissingConditionsList() throws MalformedURLException {
		test.testPostObjectMissingConditionsList();
		testV2.testPostObjectMissingConditionsList();
	}

	/**
	 * [policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectUploadSizeLimitExceeded() throws MalformedURLException {
		test.testPostObjectUploadSizeLimitExceeded();
		testV2.testPostObjectUploadSizeLimitExceeded();
	}

	/**
	 * [policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectMissingContentLengthArgument() throws MalformedURLException {
		test.testPostObjectMissingContentLengthArgument();
		testV2.testPostObjectMissingContentLengthArgument();
	}

	/**
	 * [policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력) post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectInvalidContentLengthArgument() throws MalformedURLException {
		test.testPostObjectInvalidContentLengthArgument();
		testV2.testPostObjectInvalidContentLengthArgument();
	}

	/**
	 * [policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectUploadSizeBelowMinimum() throws MalformedURLException {
		test.testPostObjectUploadSizeBelowMinimum();
		testV2.testPostObjectUploadSizeBelowMinimum();
	}

	/**
	 * [policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("ERROR")
	void testPostObjectEmptyConditions() throws MalformedURLException {
		test.testPostObjectEmptyConditions();
		testV2.testPostObjectEmptyConditions();
	}

	/**
	 * PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	 * @throws UnsupportedOperationException 지원하지 않는 작업을 수행할 경우 발생하는 예외
	 */
	@Test
	@Tag("PresignedURL")
	void testPresignedUrlPutGet() throws UnsupportedOperationException {
		test.testPresignedUrlPutGet();
		testV2.testPresignedUrlPutGet();
	}

	/**
	 * [SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	 */
	@Test
	@Tag("PresignedURL")
	void testPresignedUrlPutGetV4() {
		test.testPresignedUrlPutGetV4();
	}

	/**
	 * SignatureVersion4로 오브젝트 업로드 성공 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("signV4")
	void testPutObjectV4() throws MalformedURLException {
		test.testPutObjectV4();
		testV2.testPutObjectV4();
	}

	/**
	 * [SignatureVersion4] post 방식으로 내용을 암호화 하여 오브젝트 업로드 성공 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("signV4")
	void testPutObjectChunkedV4() throws MalformedURLException {
		test.testPutObjectChunkedV4();
		testV2.testPutObjectChunkedV4();
	}

	/**
	 * [SignatureVersion4] post 방식으로 오브젝트 다운로드 성공 확인
	 * @throws MalformedURLException URL 형식이 잘못된 경우 발생하는 예외
	 */
	@Test
	@Tag("signV4")
	void testGetObjectV4() throws MalformedURLException {
		test.testGetObjectV4();
		testV2.testGetObjectV4();
	}
}
