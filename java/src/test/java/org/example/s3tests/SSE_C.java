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

public class SSE_C {

	org.example.test.SSE_C Test = new org.example.test.SSE_C();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1b()
	{
		Test.test_encrypted_transfer_1b();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1kb()
	{
		Test.test_encrypted_transfer_1kb();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1MB()
	{
		Test.test_encrypted_transfer_1MB();
	}

	@Test
	@Tag("KSAN")
	@Tag("PutGet")
	// @Tag("13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_13b()
	{
		Test.test_encrypted_transfer_13b();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
	public void test_encryption_sse_c_method_head()
	{
		Test.test_encryption_sse_c_method_head();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
	public void test_encryption_sse_c_present()
	{
		Test.test_encryption_sse_c_present();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_other_key()
	{
		Test.test_encryption_sse_c_other_key();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
	public void test_encryption_sse_c_invalid_md5()
	{
		Test.test_encryption_sse_c_invalid_md5();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 성공 확인
	public void test_encryption_sse_c_no_md5()
	{
		Test.test_encryption_sse_c_no_md5();
	}

	@Test
	@Tag("KSAN")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_sse_c_no_key()
	{
		Test.test_encryption_sse_c_no_key();
	}

	@Test
	@Tag("KSAN")
	@Disabled("JAVA 에서는 algorithm값을 누락해도 기본값이 지정되어 있어 에러가 발생하지 않음")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_key_no_sse_c()
	{
		Test.test_encryption_key_no_sse_c();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// @Tag("멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
	public void test_encryption_sse_c_multipart_upload()
	{
		Test.test_encryption_sse_c_multipart_upload();
	}

	@Test
	@Tag("KSAN")
	@Tag("Multipart")
	// @Tag("SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_multipart_bad_download()
	{
		Test.test_encryption_sse_c_multipart_bad_download();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	// @Tag("Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
	public void test_encryption_sse_c_post_object_authenticated_request()
	{
		Test.test_encryption_sse_c_post_object_authenticated_request();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_encryption_sse_c_get_object_many()
	{
		Test.test_encryption_sse_c_get_object_many();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	// @Tag("SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_encryption_sse_c_range_object_many()
	{
		Test.test_encryption_sse_c_range_object_many();
	}

	@Test
	@Tag("KSAN")
	@Tag( "Multipart")
	//SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_c_encryption_multipart_copypart_upload()
	{
		Test.test_sse_c_encryption_multipart_copypart_upload();
	}
}
