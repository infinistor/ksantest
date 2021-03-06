/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ListObjectsV2 {

	org.example.test.ListObjectsV2 Test = new org.example.test.ListObjectsV2();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_many() {
		Test.test_bucket_listv2_many();
	}

	@Test
	@Tag("KSAN")
	@Tag("KeyCount")
	// ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
	public void test_basic_key_count() {
		Test.test_basic_key_count();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_basic() {
		Test.test_bucket_listv2_delimiter_basic();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
	public void test_bucket_listv2_encoding_basic() {
		Test.test_bucket_listv2_encoding_basic();
	}

	@Test
	@Tag("KSAN")
	@Tag("DelimiterandPrefix")
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix() {
		Test.test_bucket_listv2_delimiter_prefix();
	}

	@Test
	@Tag("KSAN")
	@Tag("DelimiterandPrefix")
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix_ends_with_delimiter() {
		Test.test_bucket_listv2_delimiter_prefix_ends_with_delimiter();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_alt() {
		Test.test_bucket_listv2_delimiter_alt();
	}

	@Test
	@Tag("KSAN")
	@Tag("DelimiterandPrefix")
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix_underscore() {
		Test.test_bucket_listv2_delimiter_prefix_underscore();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_percentage() {
		Test.test_bucket_listv2_delimiter_percentage();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_whitespace() {
		Test.test_bucket_listv2_delimiter_whitespace();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_dot() {
		Test.test_bucket_listv2_delimiter_dot();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_unreadable() {
		Test.test_bucket_listv2_delimiter_unreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_empty() {
		Test.test_bucket_listv2_delimiter_empty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_none() {
		Test.test_bucket_listv2_delimiter_none();
	}

	@Test
	@Tag("KSAN")
	@Tag("Fetchowner")
	// [권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_notempty() {
		Test.test_bucket_listv2_fetchowner_notempty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Fetchowner")
	// @Tag( "[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지
	// 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_defaultempty() {
		Test.test_bucket_listv2_fetchowner_defaultempty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Fetchowner")
	// [권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_empty() {
		Test.test_bucket_listv2_fetchowner_empty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delimiter")
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_not_exist() {
		Test.test_bucket_listv2_delimiter_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_basic() {
		Test.test_bucket_listv2_prefix_basic();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_alt() {
		Test.test_bucket_listv2_prefix_alt();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_empty() {
		Test.test_bucket_listv2_prefix_empty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_none() {
		Test.test_bucket_listv2_prefix_none();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_not_exist() {
		Test.test_bucket_listv2_prefix_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("Prefix")
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_unreadable() {
		Test.test_bucket_listv2_prefix_unreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_basic() {
		Test.test_bucket_listv2_prefix_delimiter_basic();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_alt() {
		Test.test_bucket_listv2_prefix_delimiter_alt();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_prefix_not_exist() {
		Test.test_bucket_listv2_prefix_delimiter_prefix_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_delimiter_not_exist() {
		Test.test_bucket_listv2_prefix_delimiter_delimiter_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist() {
		Test.test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_one() {
		Test.test_bucket_listv2_maxkeys_one();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_zero() {
		Test.test_bucket_listv2_maxkeys_zero();
	}

	@Test
	@Tag("KSAN")
	@Tag("MaxKeys")
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_none() {
		Test.test_bucket_listv2_maxkeys_none();
	}

	@Test
	@Tag("KSAN")
	@Tag("Continuationtoken")
	// 오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
	public void test_bucket_listv2_continuationtoken() {
		Test.test_bucket_listv2_continuationtoken();
	}

	@Test
	@Tag("KSAN")
	@Tag("ContinuationtokenAndStartAfter")
	// 오브젝트 목록을 가져올때 Startafter와 토큰이 재대로 동작하는지 확인
	public void test_bucket_listv2_both_continuationtoken_startafter() {
		Test.test_bucket_listv2_both_continuationtoken_startafter();
	}

	@Test
	@Tag("KSAN")
	@Tag("StartAfter")
	// startafter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	public void test_bucket_listv2_startafter_unreadable() {
		Test.test_bucket_listv2_startafter_unreadable();
	}

	@Test
	@Tag("KSAN")
	@Tag("StartAfter")
	// [startafter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startafter보다 정렬순서가 낮은 오브젝트는 존재하는 환경]
	// startafter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void test_bucket_listv2_startafter_not_in_list() {
		Test.test_bucket_listv2_startafter_not_in_list();
	}

	@Test
	@Tag("KSAN")
	@Tag("StartAfter")
	// [startafter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startafter를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
	public void test_bucket_listv2_startafter_after_list() {
		Test.test_bucket_listv2_startafter_after_list();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_objects_anonymous() {
		Test.test_bucket_listv2_objects_anonymous();
	}

	@Test
	@Tag("KSAN")
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
	public void test_bucket_listv2_objects_anonymous_fail() {
		Test.test_bucket_listv2_objects_anonymous_fail();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
	public void test_bucketv2_notexist() {
		Test.test_bucketv2_notexist();
	}

}
