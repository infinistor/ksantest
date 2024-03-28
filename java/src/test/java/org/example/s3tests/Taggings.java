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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Taggings {

	org.example.test.Taggings Test = new org.example.test.Taggings();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("버킷에 사용자 추가 태그값을 설정할경우 성공확인
	public void test_set_tagging() {
		Test.test_set_tagging();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("오브젝트에 태그 설정이 올바르게 적용되는지 확인
	public void test_get_obj_tagging() {
		Test.test_get_obj_tagging();
	}

	@Test
	@Tag("KSAN")
	@Tag("Check")
	// @Tag("오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인
	public void test_get_obj_head_tagging() {
		Test.test_get_obj_head_tagging();
	}

	@Test
	@Tag("KSAN")
	@Tag("Max")
	// @Tag("추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)
	public void test_put_max_tags() {
		Test.test_put_max_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Overflow")
	// @Tag("추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인
	public void test_put_excess_tags() {
		Test.test_put_excess_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Max")
	// @Tag("태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인
	public void test_put_max_kvsize_tags() {
		Test.test_put_max_kvsize_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Overflow")
	// @Tag("태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인
	public void test_put_excess_key_tags() {
		Test.test_put_excess_key_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Overflow")
	// @Tag("태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인
	public void test_put_excess_val_tags() {
		Test.test_put_excess_val_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Overwrite")
	// @Tag("오브젝트의 태그목록을 덮어쓰기 가능한지 확인
	public void test_put_modify_tags() {
		Test.test_put_modify_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	// @Tag("오브젝트의 태그를 삭제 가능한지 확인
	public void test_put_delete_tags() {
		Test.test_put_delete_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutObject")
	// @Tag("헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인
	public void test_put_obj_with_tags() {
		Test.test_put_obj_with_tags();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인
	public void test_post_object_tags_authenticated_request() throws MalformedURLException {
		Test.test_post_object_tags_authenticated_request();
	}

	
	@Test
	@Tag("Check")
	// 업로드시 오브젝트의 태그 정보를 빈 값으로 올릴 경우 성공 확인
	public void test_get_obj_non_tagging() {
		Test.test_get_obj_non_tagging();
	}
}
