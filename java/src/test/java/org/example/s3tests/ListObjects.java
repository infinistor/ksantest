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

class ListObjects {

	org.example.test.ListObjects Test = new org.example.test.ListObjects();

	@AfterEach
	public void clear(TestInfo testInfo) {
		Test.clear(testInfo);
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
	void testBucketListMany() {
		Test.testBucketListMany();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	void testBucketListDelimiterBasic() {
		Test.testBucketListDelimiterBasic();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
	void testBucketListEncodingBasic() {
		Test.testBucketListEncodingBasic();
	}

	@Test
	@Tag("KSAN")
	@Tag("Filtering")
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	void testBucketListDelimiterPrefix() {
		Test.testBucketListDelimiterPrefix();
	}

	@Test
	@Tag("KSAN")
	@Tag("Filtering")
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	void testBucketListDelimiterPrefixEndsWithDelimiter() {
		Test.testBucketListDelimiterPrefixEndsWithDelimiter();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
	void testBucketListDelimiterAlt() {
		Test.testBucketListDelimiterAlt();
	}

	@Test
	@Tag("KSAN")
	@Tag("Filtering")
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	void testBucketListDelimiterPrefixUnderscore() {
		Test.testBucketListDelimiterPrefixUnderscore();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
	void testBucketListDelimiterPercentage() {
		Test.testBucketListDelimiterPercentage();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
	void testBucketListDelimiterWhitespace() {
		Test.testBucketListDelimiterWhitespace();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
	void testBucketListDelimiterDot() {
		Test.testBucketListDelimiterDot();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
	void testBucketListDelimiterUnreadable() {
		Test.testBucketListDelimiterUnreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
	void testBucketListDelimiterEmpty() {
		Test.testBucketListDelimiterEmpty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
	void testBucketListDelimiterNone() {
		Test.testBucketListDelimiterNone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	void testBucketListDelimiterNotExist() {
		Test.testBucketListDelimiterNotExist();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
	void testBucketListDelimiterNotSkipSpecial() {
		Test.testBucketListDelimiterNotSkipSpecial();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
	void testBucketListPrefixBasic() {
		Test.testBucketListPrefixBasic();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
	void testBucketListPrefixAlt() {
		Test.testBucketListPrefixAlt();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
	void testBucketListPrefixEmpty() {
		Test.testBucketListPrefixEmpty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
	void testBucketListPrefixNone() {
		Test.testBucketListPrefixNone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	void testBucketListPrefixNotExist() {
		Test.testBucketListPrefixNotExist();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	void testBucketListPrefixUnreadable() {
		Test.testBucketListPrefixUnreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	void testBucketListPrefixDelimiterBasic() {
		Test.testBucketListPrefixDelimiterBasic();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	void testBucketListPrefixDelimiterAlt() {
		Test.testBucketListPrefixDelimiterAlt();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	void testBucketListPrefixDelimiterPrefixNotExist() {
		Test.testBucketListPrefixDelimiterPrefixNotExist();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	void testBucketListPrefixDelimiterDelimiterNotExist() {
		Test.testBucketListPrefixDelimiterDelimiterNotExist();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인
	void testBucketListPrefixDelimiterPrefixDelimiterNotExist() {
		Test.testBucketListPrefixDelimiterPrefixDelimiterNotExist();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
	void testBucketListMaxKeysOne() {
		Test.testBucketListMaxKeysOne();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
	void testBucketListMaxKeysZero() {
		Test.testBucketListMaxKeysZero();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
	void testBucketListMaxKeysNone() {
		Test.testBucketListMaxKeysNone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Marker")
	// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	void testBucketListMarkerNone() {
		Test.testBucketListMarkerNone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Marker")
	// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
	void testBucketListMarkerEmpty() {
		Test.testBucketListMarkerEmpty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Marker")
	// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	void testBucketListMarkerUnreadable() {
		Test.testBucketListMarkerUnreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("Marker")
	// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
	void testBucketListMarkerNotInList() {
		Test.testBucketListMarkerNotInList();
	}

	@Test
	@Tag("KSAN")
	@Tag("Marker")
	// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지
	// 확인
	void testBucketListMarkerAfterList() {
		Test.testBucketListMarkerAfterList();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	void testBucketListReturnData() {
		Test.testBucketListReturnData();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	void testBucketListObjectsAnonymous() {
		Test.testBucketListObjectsAnonymous();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	void testBucketListObjectsAnonymousFail() {
		Test.testBucketListObjectsAnonymousFail();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	void testBucketNotExist() {
		Test.testBucketNotExist();
	}

	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	void testBucketListFilteringAll() {
		Test.testBucketListFilteringAll();
	}
}
