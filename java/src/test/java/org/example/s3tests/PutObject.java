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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class PutObject {

	org.example.test.PutObject Test = new org.example.test.PutObject();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("PUT")
	// @Tag("오브젝트가 올바르게 생성되는지 확인
	public void test_bucket_list_distinct() {
		Test.test_bucket_list_distinct();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("존재하지 않는 버킷에 오브젝트 업로드할 경우 실패 확인
	public void test_object_write_to_nonexist_bucket() {
		Test.test_object_write_to_nonexist_bucket();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인
	public void test_object_head_zero_bytes() {
		Test.test_object_head_zero_bytes();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("업로드한 오브젝트의 ETag가 올바른지 확인
	public void test_object_write_check_etag() {
		Test.test_object_write_check_etag();
	}

	@Test
	@Tag("KSAN")
	@Tag("CacheControl")
	// @Tag("캐시(시간)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_cache_control() {
		Test.test_object_write_cache_control();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	@Tag("Expires")
	// @Tag("캐시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void test_object_write_expires() {
		Test.test_object_write_expires();
	}

	@Test
	@Tag("KSAN")
	@Tag("Update")
	// @Tag("오브젝트의 기본 작업을 모드 올바르게 할 수 있는지 확인(read, write, update, delete)
	public void test_object_write_read_update_read_delete() {
		Test.test_object_write_read_update_read_delete();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_good() {
		Test.test_object_set_get_metadata_none_to_good();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_none_to_empty() {
		Test.test_object_set_get_metadata_none_to_empty();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("메타 데이터 업데이트가 올바르게 적용되었는지 확인
	public void test_object_set_get_metadata_overwrite_to_empty() {
		Test.test_object_set_get_metadata_overwrite_to_empty();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// @Tag("메타데이터에 올바르지 않는 문자열[EOF(\x04)를 사용할 경우 실패 확인
	public void test_object_set_get_non_utf8_metadata() {
		Test.test_object_set_get_non_utf8_metadata();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// @Tag("메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨앞에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_prefix() {
		Test.test_object_set_get_metadata_empty_to_unreadable_prefix();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA에서는 메타데이터에 특수문자 사용시 예외처리되어 에러가 발생하지 않음")
	@Tag("Metadata")
	// @Tag("메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨뒤에 사용할 경우 실패 확인
	public void test_object_set_get_metadata_empty_to_unreadable_suffix() {
		Test.test_object_set_get_metadata_empty_to_unreadable_suffix();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인
	public void test_object_metadata_replaced_on_put() {
		Test.test_object_metadata_replaced_on_put();
	}

	@Test
	@Tag("KSAN")
	@Tag("Incoding")
	// @Tag("body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인
	public void test_object_write_file() {
		Test.test_object_write_file();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialKeyName")
	// @Tag("오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인
	public void test_bucket_create_special_key_names() {
		Test.test_bucket_create_special_key_names();
	}

	@Tag("SpecialKeyName")
	// @Tag("[_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지
	// 확인
	public void test_bucket_list_special_prefix() {
		Test.test_bucket_list_special_prefix();
	}

	@Tag("Lock")
	// @Tag("[버킷의 Lock옵션을 활성화] LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지
	// 메타데이터를 통해 확인
	public void test_object_lock_uploading_obj() {
		Test.test_object_lock_uploading_obj();
	}

	@Test
	@Tag("KSAN")
	@Tag("Space")
	// @Tag("오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_infix_space() {
		Test.test_object_infix_space();
	}

	@Test
	@Tag("KSAN")
	@Tag("Space")
	// @Tag("오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
	public void test_object_suffix_space() {
		Test.test_object_suffix_space();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialCharacters")
	// @Tag("[SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_2() {
		Test.test_put_empty_object_signature_version_2();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialCharacters")
	// @Tag("[SignatureVersion4] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인
	public void test_put_empty_object_signature_version_4() {
		Test.test_put_empty_object_signature_version_4();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialCharacters")
	// @Tag("[SignatureVersion2] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_2() {
		Test.test_put_object_signature_version_2();
	}

	@Test
	@Tag("KSAN")
	@Tag("SpecialCharacters")
	// @Tag("[SignatureVersion4] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_signature_version_4() {
		Test.test_put_object_signature_version_4();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// @Tag("[SignatureVersion4, UseChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding() {
		Test.test_put_object_use_chunk_encoding();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// @Tag("[SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning =
	// true] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_use_chunk_encoding_and_disable_payload_signing() {
		Test.test_put_object_use_chunk_encoding_and_disable_payload_signing();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// @Tag("[SignatureVersion4, UseChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding() {
		Test.test_put_object_not_chunk_encoding();
	}

	@Test
	@Tag("KSAN")
	@Tag("Encoding")
	// @Tag("[SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning =
	// true] 특수문자를 포함한 오브젝트 업로드 성공 확인
	public void test_put_object_not_chunk_encoding_and_disable_payload_signing() {
		Test.test_put_object_not_chunk_encoding_and_disable_payload_signing();
	}

	@Test
	@Tag("KSAN")
	@Tag("Directory")
	// @Tag("폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인
	public void test_put_object_dir_and_file() {
		Test.test_put_object_dir_and_file();
	}

}
