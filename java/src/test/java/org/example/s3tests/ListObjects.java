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
 * 버킷의 오브젝트 목록 조회 기능을 테스트하는 클래스
 */
class ListObjects {

	org.example.test.ListObjects test = new org.example.test.ListObjects();
	org.example.testV2.ListObjects testV2 = new org.example.testV2.ListObjects();

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
	 * 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Check")
	void testBucketListMany() {
		test.testBucketListMany();
		testV2.testBucketListMany();
	}

	/**
	 * 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterBasic() {
		test.testBucketListDelimiterBasic();
		testV2.testBucketListDelimiterBasic();
	}

	/**
	 * 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Encoding")
	void testBucketListEncodingBasic() {
		test.testBucketListEncodingBasic();
		testV2.testBucketListEncodingBasic();
	}

	/**
	 * 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListDelimiterPrefix() {
		test.testBucketListDelimiterPrefix();
		testV2.testBucketListDelimiterPrefix();
	}

	/**
	 * 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListDelimiterPrefixEndsWithDelimiter() {
		test.testBucketListDelimiterPrefixEndsWithDelimiter();
		testV2.testBucketListDelimiterPrefixEndsWithDelimiter();
	}

	/**
	 * 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterAlt() {
		test.testBucketListDelimiterAlt();
		testV2.testBucketListDelimiterAlt();
	}

	/**
	 * [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListDelimiterPrefixUnderscore() {
		test.testBucketListDelimiterPrefixUnderscore();
		testV2.testBucketListDelimiterPrefixUnderscore();
	}

	/**
	 * 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterPercentage() {
		test.testBucketListDelimiterPercentage();
		testV2.testBucketListDelimiterPercentage();
	}

	/**
	 * 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterWhitespace() {
		test.testBucketListDelimiterWhitespace();
		testV2.testBucketListDelimiterWhitespace();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterDot() {
		test.testBucketListDelimiterDot();
		testV2.testBucketListDelimiterDot();
	}

	/**
	 * 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterUnreadable() {
		test.testBucketListDelimiterUnreadable();
		testV2.testBucketListDelimiterUnreadable();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterEmpty() {
		test.testBucketListDelimiterEmpty();
		testV2.testBucketListDelimiterEmpty();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterNone() {
		test.testBucketListDelimiterNone();
		testV2.testBucketListDelimiterNone();
	}

	/**
	 * [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterNotExist() {
		test.testBucketListDelimiterNotExist();
		testV2.testBucketListDelimiterNotExist();
	}

	/**
	 * 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListDelimiterNotSkipSpecial() {
		test.testBucketListDelimiterNotSkipSpecial();
		testV2.testBucketListDelimiterNotSkipSpecial();
	}

	/**
	 * [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixBasic() {
		test.testBucketListPrefixBasic();
		testV2.testBucketListPrefixBasic();
	}

	/**
	 * 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixAlt() {
		test.testBucketListPrefixAlt();
		testV2.testBucketListPrefixAlt();
	}

	/**
	 * 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixEmpty() {
		test.testBucketListPrefixEmpty();
		testV2.testBucketListPrefixEmpty();
	}

	/**
	 * 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixNone() {
		test.testBucketListPrefixNone();
		testV2.testBucketListPrefixNone();
	}

	/**
	 * [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixNotExist() {
		test.testBucketListPrefixNotExist();
		testV2.testBucketListPrefixNotExist();
	}

	/**
	 * 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListPrefixUnreadable() {
		test.testBucketListPrefixUnreadable();
		testV2.testBucketListPrefixUnreadable();
	}

	/**
	 * 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListPrefixDelimiterBasic() {
		test.testBucketListPrefixDelimiterBasic();
		testV2.testBucketListPrefixDelimiterBasic();
	}

	/**
	 * [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListPrefixDelimiterAlt() {
		test.testBucketListPrefixDelimiterAlt();
		testV2.testBucketListPrefixDelimiterAlt();
	}

	/**
	 * [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListPrefixDelimiterPrefixNotExist() {
		test.testBucketListPrefixDelimiterPrefixNotExist();
		testV2.testBucketListPrefixDelimiterPrefixNotExist();
	}

	/**
	 * [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListPrefixDelimiterDelimiterNotExist() {
		test.testBucketListPrefixDelimiterDelimiterNotExist();
		testV2.testBucketListPrefixDelimiterDelimiterNotExist();
	}

	/**
	 * [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListPrefixDelimiterPrefixDelimiterNotExist() {
		test.testBucketListPrefixDelimiterPrefixDelimiterNotExist();
		testV2.testBucketListPrefixDelimiterPrefixDelimiterNotExist();
	}

	/**
	 * 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListMaxKeysOne() {
		test.testBucketListMaxKeysOne();
		testV2.testBucketListMaxKeysOne();
	}

	/**
	 * 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListMaxKeysZero() {
		test.testBucketListMaxKeysZero();
		testV2.testBucketListMaxKeysZero();
	}

	/**
	 * [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListMaxKeysNone() {
		test.testBucketListMaxKeysNone();
		testV2.testBucketListMaxKeysNone();
	}

	/**
	 * 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListMarkerNone() {
		test.testBucketListMarkerNone();
		testV2.testBucketListMarkerNone();
	}

	/**
	 * 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListMarkerEmpty() {
		test.testBucketListMarkerEmpty();
		testV2.testBucketListMarkerEmpty();
	}

	/**
	 * 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListMarkerUnreadable() {
		test.testBucketListMarkerUnreadable();
		testV2.testBucketListMarkerUnreadable();
	}

	/**
	 * [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListMarkerNotInList() {
		test.testBucketListMarkerNotInList();
		testV2.testBucketListMarkerNotInList();
	}

	/**
	 * [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListMarkerAfterList() {
		test.testBucketListMarkerAfterList();
		testV2.testBucketListMarkerAfterList();
	}

	/**
	 * ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	 */
	@Test
	@Tag("Metadata")
	void testBucketListReturnData() {
		test.testBucketListReturnData();
		testV2.testBucketListReturnData();
	}

	/**
	 * 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	 */
	@Test
	@Tag("ACL")
	void testBucketListObjectsAnonymous() {
		test.testBucketListObjectsAnonymous();
		testV2.testBucketListObjectsAnonymous();
	}

	/**
	 * 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	 */
	@Test
	@Tag("ACL")
	void testBucketListObjectsAnonymousFail() {
		test.testBucketListObjectsAnonymousFail();
		testV2.testBucketListObjectsAnonymousFail();
	}

	/**
	 * 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketNotExist() {
		test.testBucketNotExist();
		testV2.testBucketNotExist();
	}

	/**
	 * delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListFilteringAll() {
		test.testBucketListFilteringAll();
		testV2.testBucketListFilteringAll();
	}

	/**
	 * versioning 활성화 버킷에서 오브젝트 목록을 가져올때 버전정보가 포함되어 있는지 확인
	 */
	@Test
	@Tag("Versioning")
	void testBucketListVersioning() {
		test.testBucketListVersioning();
		testV2.testBucketListVersioning();
	}
}
