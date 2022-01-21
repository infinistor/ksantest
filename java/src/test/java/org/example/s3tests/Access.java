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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Access{

    org.example.test.Access Test = new org.example.test.Access();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@DisplayName("test_put_public_block")
	@Tag("Check")
	//@Tag("버킷의 접근권한 블록 설정 확인")
	public void test_put_public_block()
    {
        Test.test_put_public_block();
	}

	@Test
	@DisplayName("test_block_public_put_bucket_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인")
	public void test_block_public_put_bucket_acls()
    {
        Test.test_block_public_put_bucket_acls();
	}

	@Test
	@DisplayName("test_block_public_object_canned_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인")
	public void test_block_public_object_canned_acls()
    {
        Test.test_block_public_object_canned_acls();
	}

	@Test
	@DisplayName("test_block_public_policy")
	@Tag("Denied")
	//@Tag("버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인")
	public void test_block_public_policy()
    {
        Test.test_block_public_policy();
	}

	@Test
	@DisplayName("test_ignore_public_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한블록으로 개인버킷처럼 설정한뒤 버킷의acl권한을 public-read로 변경해도 적용되지 않음을 확인")
	public void test_ignore_public_acls()
    {
        Test.test_ignore_public_acls();
	}

	@Test
	@DisplayName("test_delete_public_block")
	@Tag("Check")
	//@Tag("버킷의 접근권한 블록 삭제 확인")
	public void test_delete_public_block()
    {
        Test.test_delete_public_block();
	}
}