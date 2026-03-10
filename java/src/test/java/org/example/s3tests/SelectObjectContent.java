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

/**
 * S3 SelectObjectContent API 테스트
 * 오브젝트 내용에 SQL을 실행하여 결과를 조회하는 기능 검증
 */
class SelectObjectContent {

	org.example.testV2.SelectObjectContent testV2 = new org.example.testV2.SelectObjectContent();

	@AfterEach
	void clear(TestInfo testInfo) {
		testV2.clear(testInfo);
	}

	/**
	 * CSV 오브젝트에 대해 SelectObjectContent 기본 동작 확인
	 */
	@Test
	@Tag("Select")
	void testSelectObjectContentCsvBasic() {
		testV2.testSelectObjectContentCsvBasic();
	}

	/**
	 * CSV 오브젝트에 WHERE 조건 적용
	 */
	@Test
	@Tag("Select")
	void testSelectObjectContentCsvWithWhere() {
		testV2.testSelectObjectContentCsvWithWhere();
	}

	/**
	 * CSV 오브젝트 LIMIT 적용
	 */
	@Test
	@Tag("Select")
	void testSelectObjectContentCsvLimit() {
		testV2.testSelectObjectContentCsvLimit();
	}

	/**
	 * JSON 오브젝트에 대해 SelectObjectContent 기본 동작 확인
	 */
	@Test
	@Tag("Select")
	void testSelectObjectContentJsonBasic() {
		testV2.testSelectObjectContentJsonBasic();
	}

	/**
	 * 존재하지 않는 버킷에 SelectObjectContent 요청 시 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testSelectObjectContentNonExistentBucket() {
		testV2.testSelectObjectContentNonExistentBucket();
	}

	/**
	 * 존재하지 않는 오브젝트에 SelectObjectContent 요청 시 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testSelectObjectContentNonExistentObject() {
		testV2.testSelectObjectContentNonExistentObject();
	}

	/**
	 * 빈 CSV 오브젝트에 SelectObjectContent (헤더만 있는 경우)
	 */
	@Test
	@Tag("Select")
	void testSelectObjectContentCsvEmptyRows() {
		testV2.testSelectObjectContentCsvEmptyRows();
	}
}
