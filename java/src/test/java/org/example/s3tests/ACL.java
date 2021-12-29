package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ACL
{
    org.example.test.ACL Test = new org.example.test.ACL();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
	@DisplayName("test_object_raw_get")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지 확인")
    public void test_object_raw_get()
    {
        Test.test_object_raw_get();
    }

    @Test
	@DisplayName("test_object_raw_get_bucket_gone")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")
    public void test_object_raw_get_bucket_gone()
    {
        Test.test_object_raw_get_bucket_gone();
    }

    @Test
	@DisplayName("test_object_delete_key_bucket_gone")
    @Tag("Delete")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트를 삭제할때 에러 확인")
    public void test_object_delete_key_bucket_gone()
    {
        Test.test_object_delete_key_bucket_gone();
    }

    @Test
	@DisplayName("test_object_raw_get_object_gone")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때 에러 확인")
    public void test_object_raw_get_object_gone()
    {
        Test.test_object_raw_get_object_gone();
    }

    @Test
	@DisplayName("test_object_raw_get_bucket_acl")
    @Tag("Get")
    //@Tag("[Bucket_ACL = private, Object_ACL = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")
    public void test_object_raw_get_bucket_acl()
    {
        Test.test_object_raw_get_bucket_acl();
    }

    @Test
	@DisplayName("test_object_raw_get_object_acl")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에 접근할때 에러확인")
    public void test_object_raw_get_object_acl()
    {
        Test.test_object_raw_get_object_acl();
    }

    @Test
	@DisplayName("test_object_raw_authenticated")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 공용 버킷의 공용 오브젝트에 접근 가능한지 확인")
    public void test_object_raw_authenticated()
    {
        Test.test_object_raw_authenticated();
    }

    @Test
	@DisplayName("test_object_raw_response_headers")
    @Tag("Header")
    //@Tag("[Bucket_ACL = priavte, Object_ACL = priavte] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인")
    public void test_object_raw_response_headers()
    {
        Test.test_object_raw_response_headers();
    }

    @Test
	@DisplayName("test_object_raw_authenticated_bucket_acl")
    @Tag("Get")
    //@Tag("[Bucket_ACL = private, Object_ACL = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")
    public void test_object_raw_authenticated_bucket_acl()
    {
        Test.test_object_raw_authenticated_bucket_acl();
    }

    @Test
	@DisplayName("test_object_raw_authenticated_object_acl")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근 가능한지 확인")
    public void test_object_raw_authenticated_object_acl()
    {
        Test.test_object_raw_authenticated_object_acl();
    }

    @Test
	@DisplayName("test_object_raw_authenticated_bucket_gone")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인")
    public void test_object_raw_authenticated_bucket_gone()
    {
        Test.test_object_raw_authenticated_bucket_gone();
    }

    @Test
	@DisplayName("test_object_raw_authenticated_object_gone")
    @Tag("Get")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때 에러 확인")
    public void test_object_raw_authenticated_object_gone()
    {
        Test.test_object_raw_authenticated_object_gone();
    }

    @Test
	@DisplayName("test_object_raw_get_x_amz_expires_not_expired")
    @Tag("Post")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 가능한지 확인")
    public void test_object_raw_get_x_amz_expires_not_expired()
    {
        Test.test_object_raw_get_x_amz_expires_not_expired();
    }

    @Test
	@DisplayName("test_object_raw_get_x_amz_expires_out_range_zero")
    @Tag("Post")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")
    public void test_object_raw_get_x_amz_expires_out_range_zero()
    {
        Test.test_object_raw_get_x_amz_expires_out_range_zero();
    }

    @Test
	@DisplayName("test_object_raw_get_x_amz_expires_out_positive_range")
    @Tag("Post")
    //@Tag("[Bucket_ACL = public-read, Object_ACL = public-read] 로그인 유효주기가 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인")
    public void test_object_raw_get_x_amz_expires_out_positive_range()
    {
        Test.test_object_raw_get_x_amz_expires_out_positive_range();
    }

    @Test
	@DisplayName("test_object_anon_put")
    @Tag("Update")
    //@Tag("[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인")
    public void test_object_anon_put()
    {
        Test.test_object_anon_put();
    }

    @Test
	@DisplayName("test_object_anon_put_write_access")
    @Tag("Update")
    //@Tag("[Bucket_ACL = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트했을때 올바르게 적용 되는지 확인")
    public void test_object_anon_put_write_access()
    {
        Test.test_object_anon_put_write_access();
    }

    @Test
	@DisplayName("test_object_put_authenticated")
    @Tag("Default")
    //@Tag("[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드")
    public void test_object_put_authenticated()
    {
        Test.test_object_put_authenticated();
    }

    @Test
	@DisplayName("test_object_raw_put_authenticated_expired")
    @Tag("Default")
    //@Tag("[Bucket_ACL = Default, Object_ACL = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트 업데이트 실패 확인")
    public void test_object_raw_put_authenticated_expired()
    {
        Test.test_object_raw_put_authenticated_expired();
    }
    
    @Test
	@DisplayName("test_acl_private_bucket_public_read_object")
    @Tag("Get")
    //@Tag("[Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인")
    public void test_acl_private_bucket_public_read_object()
    {
        Test.test_acl_private_bucket_public_read_object();
    }
}
