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

class ListObjectsV2 {

	org.example.test.ListObjectsV2 test = new org.example.test.ListObjectsV2();
	org.example.testV2.ListObjectsV2 testV2 = new org.example.testV2.ListObjectsV2();

	@AfterEach
	void clear(TestInfo testInfo) {
		test.clear(testInfo);
		testV2.clear(testInfo);
	}

	@Test
	@Tag("Check")
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
	void testBucketListV2Many() {
		test.testBucketListV2Many();
		testV2.testBucketListV2Many();
	}

	@Test
	@Tag("KeyCount")
	// ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
	void testBasicKeyCount() {
		test.testBasicKeyCount();
		testV2.testBasicKeyCount();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterBasic() {
		test.testBucketListV2DelimiterBasic();
		testV2.testBucketListV2DelimiterBasic();
	}

	@Test
	@Tag("Encoding")
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
	void testBucketListV2EncodingBasic() {
		test.testBucketListV2EncodingBasic();
		testV2.testBucketListV2EncodingBasic();
	}

	@Test
	@Tag("Filtering")
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterPrefix() {
		test.testBucketListV2DelimiterPrefix();
		testV2.testBucketListV2DelimiterPrefix();
	}

	@Test
	@Tag("Filtering")
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterPrefixEndsWithDelimiter() {
		test.testBucketListV2DelimiterPrefixEndsWithDelimiter();
		testV2.testBucketListV2DelimiterPrefixEndsWithDelimiter();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterAlt() {
		test.testBucketListV2DelimiterAlt();
		testV2.testBucketListV2DelimiterAlt();
	}

	@Test
	@Tag("Filtering")
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterPrefixUnderscore() {
		test.testBucketListV2DelimiterPrefixUnderscore();
		testV2.testBucketListV2DelimiterPrefixUnderscore();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterPercentage() {
		test.testBucketListV2DelimiterPercentage();
		testV2.testBucketListV2DelimiterPercentage();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterWhitespace() {
		test.testBucketListV2DelimiterWhitespace();
		testV2.testBucketListV2DelimiterWhitespace();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterDot() {
		test.testBucketListV2DelimiterDot();
		testV2.testBucketListV2DelimiterDot();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterUnreadable() {
		test.testBucketListV2DelimiterUnreadable();
		testV2.testBucketListV2DelimiterUnreadable();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterEmpty() {
		test.testBucketListV2DelimiterEmpty();
		testV2.testBucketListV2DelimiterEmpty();
	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterNone() {
		test.testBucketListV2DelimiterNone();
		testV2.testBucketListV2DelimiterNone();
	}

	@Test
	@Tag("FetchOwner")
	// [권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	void testBucketListV2FetchOwnerNotEmpty() {
		test.testBucketListV2FetchOwnerNotEmpty();
		testV2.testBucketListV2FetchOwnerNotEmpty();
	}

	@Test
	@Tag("FetchOwner")
	// @Tag( "[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지
	// 확인(ListObjectsV2)
	void testBucketListV2FetchOwnerDefaultEmpty() {
		test.testBucketListV2FetchOwnerDefaultEmpty();
		testV2.testBucketListV2FetchOwnerDefaultEmpty();
	}

	@Test
	@Tag("FetchOwner")
	// [권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	void testBucketListV2FetchOwnerEmpty() {
		test.testBucketListV2FetchOwnerEmpty();
		testV2.testBucketListV2FetchOwnerEmpty();
	}

	@Test
	@Tag("Delimiter")
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	void testBucketListV2DelimiterNotExist() {
		test.testBucketListV2DelimiterNotExist();
		testV2.testBucketListV2DelimiterNotExist();
	}

	@Test
	@Tag("Prefix")
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixBasic() {
		test.testBucketListV2PrefixBasic();
		testV2.testBucketListV2PrefixBasic();
	}

	@Test
	@Tag("Prefix")
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
	void testBucketListV2PrefixAlt() {
		test.testBucketListV2PrefixAlt();
		testV2.testBucketListV2PrefixAlt();
	}

	@Test
	@Tag("Prefix")
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixEmpty() {
		test.testBucketListV2PrefixEmpty();
		testV2.testBucketListV2PrefixEmpty();
	}

	@Test
	@Tag("Prefix")
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixNone() {
		test.testBucketListV2PrefixNone();
		testV2.testBucketListV2PrefixNone();
	}

	@Test
	@Tag("Prefix")
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixNotExist() {
		test.testBucketListV2PrefixNotExist();
		testV2.testBucketListV2PrefixNotExist();
	}

	@Test
	@Tag("Prefix")
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixUnreadable() {
		test.testBucketListV2PrefixUnreadable();
		testV2.testBucketListV2PrefixUnreadable();
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixDelimiterBasic() {
		test.testBucketListV2PrefixDelimiterBasic();
		testV2.testBucketListV2PrefixDelimiterBasic();
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixDelimiterAlt() {
		test.testBucketListV2PrefixDelimiterAlt();
		testV2.testBucketListV2PrefixDelimiterAlt();
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
	void testBucketListV2PrefixDelimiterPrefixNotExist() {
		test.testBucketListV2PrefixDelimiterPrefixNotExist();
		testV2.testBucketListV2PrefixDelimiterPrefixNotExist();
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	void testBucketListV2PrefixDelimiterDelimiterNotExist() {
		test.testBucketListV2PrefixDelimiterDelimiterNotExist();
		testV2.testBucketListV2PrefixDelimiterDelimiterNotExist();
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인(ListObjectsV2)
	void testBucketListV2PrefixDelimiterPrefixDelimiterNotExist() {
		test.testBucketListV2PrefixDelimiterPrefixDelimiterNotExist();
		testV2.testBucketListV2PrefixDelimiterPrefixDelimiterNotExist();
	}

	@Test
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	void testBucketListV2MaxKeysOne() {
		test.testBucketListV2MaxKeysOne();
		testV2.testBucketListV2MaxKeysOne();
	}

	@Test
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
	void testBucketListV2MaxKeysZero() {
		test.testBucketListV2MaxKeysZero();
		testV2.testBucketListV2MaxKeysZero();
	}

	@Test
	@Tag("MaxKeys")
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	void testBucketListV2MaxKeysNone() {
		test.testBucketListV2MaxKeysNone();
		testV2.testBucketListV2MaxKeysNone();
	}

	@Test
	@Tag("ContinuationToken")
	// 오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
	void testBucketListV2ContinuationToken() {
		test.testBucketListV2ContinuationToken();
		testV2.testBucketListV2ContinuationToken();
	}

	@Test
	@Tag("ContinuationTokenAndStartAfter")
	// 오브젝트 목록을 가져올때 StartAfter와 토큰이 재대로 동작하는지 확인
	void testBucketListV2BothContinuationTokenStartAfter() {
		test.testBucketListV2BothContinuationTokenStartAfter();
		testV2.testBucketListV2BothContinuationTokenStartAfter();
	}

	@Test
	@Tag("StartAfter")
	// startAfter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	void testBucketListV2StartAfterUnreadable() {
		test.testBucketListV2StartAfterUnreadable();
		testV2.testBucketListV2StartAfterUnreadable();
	}

	@Test
	@Tag("StartAfter")
	// [startAfter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startAfter보다 정렬순서가 낮은 오브젝트는 존재하는 환경]
	// startAfter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	void testBucketListV2StartAfterNotInList() {
		test.testBucketListV2StartAfterNotInList();
		testV2.testBucketListV2StartAfterNotInList();
	}

	@Test
	@Tag("StartAfter")
	// [startAfter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startAfter를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
	void testBucketListV2StartAfterAfterList() {
		test.testBucketListV2StartAfterAfterList();
		testV2.testBucketListV2StartAfterAfterList();
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
	void testBucketListV2ObjectsAnonymous() {
		test.testBucketListV2ObjectsAnonymous();
		testV2.testBucketListV2ObjectsAnonymous();
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
	void testBucketListV2ObjectsAnonymousFail() {
		test.testBucketListV2ObjectsAnonymousFail();
		testV2.testBucketListV2ObjectsAnonymousFail();
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
	void testBucketV2NotExist() {
		test.testBucketV2NotExist();
		testV2.testBucketV2NotExist();
	}

	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	void testBucketListV2FilteringAll() {
		test.testBucketListV2FilteringAll();
		testV2.testBucketListV2FilteringAll();
	}
	
}
