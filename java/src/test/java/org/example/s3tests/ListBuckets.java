package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ListBuckets {

    org.example.test.ListBuckets Test = new org.example.test.ListBuckets();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}
    
    @Test
    @DisplayName("test_buckets_create_then_list")
    @Tag("Get")
    @Tag("KSAN")
    // @Tag("여러개의 버킷 생성해서 목록 조회 확인")
    public void test_buckets_create_then_list()
    {
        Test.test_buckets_create_then_list();
    }

    @Test
    @DisplayName("test_list_buckets_invalid_auth")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("존재하지 않는 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_invalid_auth()
    {
        Test.test_list_buckets_invalid_auth();
    }

    @Test
    @DisplayName("test_list_buckets_bad_auth")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인")
    public void test_list_buckets_bad_auth()
    {
        Test.test_list_buckets_bad_auth();
    }

    @Test
    @DisplayName("test_head_bucket")
    @Tag("Metadata")
    @Tag("KSAN")
    // Tag("버킷의 메타데이터를 가져올 수 있는지 확인")
    public void test_head_bucket()
    {
        Test.test_head_bucket();
    }
}
