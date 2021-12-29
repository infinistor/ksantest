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