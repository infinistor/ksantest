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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class GetObjectAttributes {
	org.example.testV2.GetObjectAttributes testV2 = new org.example.testV2.GetObjectAttributes();

	@AfterEach
	void clear(TestInfo testInfo) {
		testV2.clear(testInfo);
	}

	/**
	 * 기본 GetObjectAttributes 테스트
	 * 모든 속성을 요청하고 응답이 올바른지 확인
	 */
	@Test
	@Tag("Basic")
	void testGetObjectAttributesBasic() {
		testV2.testGetObjectAttributesBasic();
	}

	/**
	 * 특정 속성만 요청하는 테스트
	 */
	@Test
	@Tag("SpecificAttributes")
	void testGetObjectAttributesSpecificAttributes() {
		testV2.testGetObjectAttributesSpecificAttributes();
	}

	/**
	 * 멀티파트 업로드된 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Multipart")
	void testGetObjectAttributesMultipart() {
		testV2.testGetObjectAttributesMultipart();
	}

	/**
	 * 체크섬 알고리즘을 사용한 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Checksum")
	void testGetObjectAttributesWithChecksum() {
		testV2.testGetObjectAttributesWithChecksum();
	}

	/**
	 * 존재하지 않는 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAttributesNonExistentObject() {
		testV2.testGetObjectAttributesNonExistentObject();
	}

	/**
	 * 존재하지 않는 버킷에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAttributesNonExistentBucket() {
		testV2.testGetObjectAttributesNonExistentBucket();
	}

	/**
	 * 속성을 지정하지 않은 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAttributesNoAttributes() {
		testV2.testGetObjectAttributesNoAttributes();
	}

	/**
	 * 버전 ID를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Versioning")
	void testGetObjectAttributesWithVersionId() {
		testV2.testGetObjectAttributesWithVersionId();
	}

	/**
	 * 잘못된 버전 ID를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAttributesInvalidVersionId() {
		testV2.testGetObjectAttributesInvalidVersionId();
	}

	/**
	 * 대용량 멀티파트 업로드 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("LargeMultipart")
	void testGetObjectAttributesLargeMultipart() {
		testV2.testGetObjectAttributesLargeMultipart();
	}

	/**
	 * 메타데이터가 있는 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Metadata")
	void testGetObjectAttributesWithMetadata() {
		testV2.testGetObjectAttributesWithMetadata();
	}

	/**
	 * SSE-S3 암호화된 객체에 대한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Encryption")
	void testGetObjectAttributesWithSSES3() {
		testV2.testGetObjectAttributesWithSSES3();
	}

	/**
	 * 비동기 클라이언트를 사용한 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("Async")
	void testGetObjectAttributesAsync() {
		testV2.testGetObjectAttributesAsync();
	}

	/**
	 * 비동기 클라이언트를 사용한 GetObjectAttributes 에러 테스트
	 */
	@Test
	@Tag("ERROR")
	void testGetObjectAttributesAsyncError() {
		testV2.testGetObjectAttributesAsyncError();
	}

	/**
	 * 모든 가능한 속성을 요청하는 GetObjectAttributes 테스트
	 */
	@Test
	@Tag("AllAttributes")
	void testGetObjectAttributesAllAttributes() {
		testV2.testGetObjectAttributesAllAttributes();
	}
}