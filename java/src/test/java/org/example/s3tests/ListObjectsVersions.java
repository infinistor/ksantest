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
 * 버킷의 버전 관리된 오브젝트 목록 조회 기능을 테스트하는 클래스
 */
class ListObjectsVersions {

	org.example.test.ListObjectsVersions test = new org.example.test.ListObjectsVersions();
	org.example.testV2.ListObjectsVersions testV2 = new org.example.testV2.ListObjectsVersions();

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
	void testBucketListVersionsMany() {
		test.testBucketListVersionsMany();
		testV2.testBucketListVersionsMany();
	}

	/**
	 * 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterBasic() {
		test.testBucketListVersionsDelimiterBasic();
		testV2.testBucketListVersionsDelimiterBasic();
	}

	/**
	 * 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
	 */
	@Test
	@Tag("Encoding")
	void testBucketListVersionsEncodingBasic() {
		test.testBucketListVersionsEncodingBasic();
		testV2.testBucketListVersionsEncodingBasic();
	}

	/**
	 * 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListVersionsDelimiterPrefix() {
		test.testBucketListVersionsDelimiterPrefix();
		testV2.testBucketListVersionsDelimiterPrefix();
	}

	/**
	 * 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListVersionsDelimiterPrefixEndsWithDelimiter() {
		test.testBucketListVersionsDelimiterPrefixEndsWithDelimiter();
		testV2.testBucketListVersionsDelimiterPrefixEndsWithDelimiter();
	}

	/**
	 * 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterAlt() {
		test.testBucketListVersionsDelimiterAlt();
		testV2.testBucketListVersionsDelimiterAlt();
	}

	/**
	 * [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testBucketListVersionsDelimiterPrefixUnderscore() {
		test.testBucketListVersionsDelimiterPrefixUnderscore();
		testV2.testBucketListVersionsDelimiterPrefixUnderscore();
	}

	/**
	 * 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterPercentage() {
		test.testBucketListVersionsDelimiterPercentage();
		testV2.testBucketListVersionsDelimiterPercentage();
	}

	/**
	 * 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterWhitespace() {
		test.testBucketListVersionsDelimiterWhitespace();
		testV2.testBucketListVersionsDelimiterWhitespace();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterDot() {
		test.testBucketListVersionsDelimiterDot();
		testV2.testBucketListVersionsDelimiterDot();
	}

	/**
	 * 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterUnreadable() {
		test.testBucketListVersionsDelimiterUnreadable();
		testV2.testBucketListVersionsDelimiterUnreadable();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterEmpty() {
		test.testBucketListVersionsDelimiterEmpty();
		testV2.testBucketListVersionsDelimiterEmpty();
	}

	/**
	 * 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterNone() {
		test.testBucketListVersionsDelimiterNone();
		testV2.testBucketListVersionsDelimiterNone();
	}

	/**
	 * [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterNotExist() {
		test.testBucketListVersionsDelimiterNotExist();
		testV2.testBucketListVersionsDelimiterNotExist();
	}

	/**
	 * 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
	 */
	@Test
	@Tag("Delimiter")
	void testBucketListVersionsDelimiterNotSkipSpecial() {
		test.testBucketListVersionsDelimiterNotSkipSpecial();
		testV2.testBucketListVersionsDelimiterNotSkipSpecial();
	}

	/**
	 * [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixBasic() {
		test.testBucketListVersionsPrefixBasic();
		testV2.testBucketListVersionsPrefixBasic();
	}

	/**
	 * 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixAlt() {
		test.testBucketListVersionsPrefixAlt();
		testV2.testBucketListVersionsPrefixAlt();
	}

	/**
	 * 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixEmpty() {
		test.testBucketListVersionsPrefixEmpty();
		testV2.testBucketListVersionsPrefixEmpty();
	}

	/**
	 * 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixNone() {
		test.testBucketListVersionsPrefixNone();
		testV2.testBucketListVersionsPrefixNone();
	}

	/**
	 * [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixNotExist() {
		test.testBucketListVersionsPrefixNotExist();
		testV2.testBucketListVersionsPrefixNotExist();
	}

	/**
	 * 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	 */
	@Test
	@Tag("Prefix")
	void testBucketListVersionsPrefixUnreadable() {
		test.testBucketListVersionsPrefixUnreadable();
		testV2.testBucketListVersionsPrefixUnreadable();
	}

	/**
	 * 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListVersionsPrefixDelimiterBasic() {
		test.testBucketListVersionsPrefixDelimiterBasic();
		testV2.testBucketListVersionsPrefixDelimiterBasic();
	}

	/**
	 * [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListVersionsPrefixDelimiterAlt() {
		test.testBucketListVersionsPrefixDelimiterAlt();
		testV2.testBucketListVersionsPrefixDelimiterAlt();
	}

	/**
	 * [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListVersionsPrefixDelimiterPrefixNotExist() {
		test.testBucketListVersionsPrefixDelimiterPrefixNotExist();
		testV2.testBucketListVersionsPrefixDelimiterPrefixNotExist();
	}

	/**
	 * [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListVersionsPrefixDelimiterDelimiterNotExist() {
		test.testBucketListVersionsPrefixDelimiterDelimiterNotExist();
		testV2.testBucketListVersionsPrefixDelimiterDelimiterNotExist();
	}

	/**
	 * [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	 */
	@Test
	@Tag("PrefixAndDelimiter")
	void testBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist() {
		test.testBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist();
		testV2.testBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist();
	}

	/**
	 * 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListVersionsMaxKeysOne() {
		test.testBucketListVersionsMaxKeysOne();
		testV2.testBucketListVersionsMaxKeysOne();
	}

	/**
	 * 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListVersionsMaxKeysZero() {
		test.testBucketListVersionsMaxKeysZero();
		testV2.testBucketListVersionsMaxKeysZero();
	}

	/**
	 * [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("MaxKeys")
	void testBucketListVersionsMaxKeysNone() {
		test.testBucketListVersionsMaxKeysNone();
		testV2.testBucketListVersionsMaxKeysNone();
	}

	/**
	 * 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListVersionsMarkerNone() {
		test.testBucketListVersionsMarkerNone();
		testV2.testBucketListVersionsMarkerNone();
	}

	/**
	 * 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListVersionsMarkerEmpty() {
		test.testBucketListVersionsMarkerEmpty();
		testV2.testBucketListVersionsMarkerEmpty();
	}

	/**
	 * 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListVersionsMarkerUnreadable() {
		test.testBucketListVersionsMarkerUnreadable();
		testV2.testBucketListVersionsMarkerUnreadable();
	}

	/**
	 * [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListVersionsMarkerNotInList() {
		test.testBucketListVersionsMarkerNotInList();
		testV2.testBucketListVersionsMarkerNotInList();
	}

	/**
	 * [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	 */
	@Test
	@Tag("Marker")
	void testBucketListVersionsMarkerAfterList() {
		test.testBucketListVersionsMarkerAfterList();
		testV2.testBucketListVersionsMarkerAfterList();
	}

	/**
	 * ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	 */
	@Test
	@Tag("Metadata")
	void testBucketListVersionsReturnData() {
		test.testBucketListVersionsReturnData();
		testV2.testBucketListVersionsReturnData();
	}

	/**
	 * 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	 */
	@Test
	@Tag("ACL")
	void testBucketListVersionsObjectsAnonymous() {
		test.testBucketListVersionsObjectsAnonymous();
		testV2.testBucketListVersionsObjectsAnonymous();
	}

	/**
	 * 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	 */
	@Test
	@Tag("ACL")
	void testBucketListVersionsObjectsAnonymousFail() {
		test.testBucketListVersionsObjectsAnonymousFail();
		testV2.testBucketListVersionsObjectsAnonymousFail();
	}

	/**
	 * 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	 */
	@Test
	@Tag("ERROR")
	void testBucketListVersionsNotExist() {
		test.testBucketListVersionsNotExist();
		testV2.testBucketListVersionsNotExist();
	}

	/**
	 * delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Filtering")
	void testVersioningBucketListFilteringAll() {
		test.testVersioningBucketListFilteringAll();
		testV2.testVersioningBucketListFilteringAll();
	}

	/**
	 * 단일 오브젝트의 버전 목록을 올바르게 가져오는지 확인
	 */
	@Test
	@Tag("Object")
	void testVersioningObjListMarker() {
		test.testVersioningObjListMarker();
		testV2.testVersioningObjListMarker();
	}
}
