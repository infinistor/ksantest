package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Cors
{
    org.example.test.Cors Test = new org.example.test.Cors();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_set_cors")
    @Tag("Check")
    @Tag("KSAN")
    // @Tag("버킷의 cors정보 세팅 성공 확인")
    public void test_set_cors()
    {
        Test.test_set_cors();
    }

    @Test
    @DisplayName("test_cors_origin_response")
    @Tag("Post")
    @Tag("KSAN")
    // @Tag("버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인")
    public void test_cors_origin_response()
    {
        Test.test_cors_origin_response();
    }

    @Test
    @DisplayName("test_cors_origin_wildcard")
    @Tag("Post")
    @Tag("KSAN")
    // @Tag("와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인")
    public void test_cors_origin_wildcard()
    {
        Test.test_cors_origin_wildcard();
    }

    @Test
    @DisplayName("test_cors_header_option")
    @Tag("Post")
    @Tag("KSAN")
    // @Tag("cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인")
    public void test_cors_header_option()
    {
        Test.test_cors_header_option();
    }
}