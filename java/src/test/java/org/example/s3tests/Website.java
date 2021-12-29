package org.example.s3tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;

public class Website
{
    org.example.test.Website Test = new org.example.test.Website();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
	@DisplayName("test_webiste_get_buckets")
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정 조회 확인")
    public void test_webiste_get_buckets()
    {
        Test.test_webiste_get_buckets();
    }

    @Test
	@DisplayName("test_webiste_put_buckets")
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정이 가능한지 확인")
    public void test_webiste_put_buckets()
    {
        Test.test_webiste_put_buckets();
    }

    @Test
	@DisplayName("test_webiste_delete_buckets")
    @Tag("Delete")
    @Tag("KSAN")
    //@Tag("버킷의 Websize 설정이 삭제가능한지 확인")
    public void test_webiste_delete_buckets()
    {
        Test.test_webiste_delete_buckets();
    }
}
