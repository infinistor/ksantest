package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Taggings {

    org.example.test.Taggings Test = new org.example.test.Taggings();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_set_tagging")
    @Tag("Check")
    @Tag("KSAN")
    // @Tag("버킷에 사용자 추가 태그값을 설정할경우 성공확인")
    public void test_set_tagging()
    {
        Test.test_set_tagging();
    }

    @Test
    @DisplayName("test_get_obj_tagging")
    @Tag("Check")
    @Tag("KSAN")
    // @Tag("오브젝트에 태그 설정이 올바르게 적용되는지 확인")
    public void test_get_obj_tagging()
    {
        Test.test_get_obj_tagging();
    }


    @Test
    @DisplayName("test_get_obj_head_tagging")
    @Tag("Check")
    // @Tag("오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인")
    public void test_get_obj_head_tagging()
    {
        Test.test_get_obj_head_tagging();
    }

    @Test
    @DisplayName("test_put_max_tags")
    @Tag("Max")
    @Tag("KSAN")
    // @Tag("추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)")
    public void test_put_max_tags()
    {
        Test.test_put_max_tags();
    }

    @Test
    @DisplayName("test_put_excess_tags")
    @Tag("Overflow")
    @Tag("KSAN")
    // @Tag("추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인")
    public void test_put_excess_tags()
    {
        Test.test_put_excess_tags();
    }

    @Test
    @DisplayName("test_put_max_kvsize_tags")
    @Tag("Max")
    @Tag("KSAN")
    // @Tag("태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인")
    public void test_put_max_kvsize_tags()
    {
        Test.test_put_max_kvsize_tags();
    }

    @Test
    @DisplayName("test_put_excess_key_tags")
    @Tag("Overflow")
    @Tag("KSAN")
    // @Tag("태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인")
    public void test_put_excess_key_tags()
    {
        Test.test_put_excess_key_tags();
    }

    @Test
    @DisplayName("test_put_excess_val_tags")
    @Tag("Overflow")
    @Tag("KSAN")
    // @Tag("태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인")
    public void test_put_excess_val_tags()
    {
        Test.test_put_excess_val_tags();
    }

    @Test
    @DisplayName("test_put_modify_tags")
    @Tag("Overwrite")
    @Tag("KSAN")
    // @Tag("오브젝트의 태그목록을 덮어쓰기 가능한지 확인")
    public void test_put_modify_tags()
    {
        Test.test_put_modify_tags();
    }

    @Test
    @DisplayName("test_put_delete_tags")
    @Tag("Delete")
    @Tag("KSAN")
    // @Tag("오브젝트의 태그를 삭제 가능한지 확인")
    public void test_put_delete_tags()
    {
        Test.test_put_delete_tags();
    }


    @Test
    @DisplayName("test_put_obj_with_tags")
    @Tag("PutObject")
    // @Tag("헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인")
    public void test_put_obj_with_tags()
    {
        Test.test_put_obj_with_tags();
    }


    @Test
    @DisplayName("test_post_object_tags_authenticated_request")
    @Tag("Post")
    // @Tag("로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인")
    public void test_post_object_tags_authenticated_request()
    {
        Test.test_post_object_tags_authenticated_request();
    }

}
