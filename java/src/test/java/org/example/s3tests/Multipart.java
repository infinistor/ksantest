package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Multipart {

    org.example.test.Multipart Test = new org.example.test.Multipart();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_multipart_upload_empty")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("비어있는 오브젝트를 멀티파트로 업로드 실패 확인")
    public void test_multipart_upload_empty()
    {
        Test.test_multipart_upload_empty();
    }
    
    @Test
    @DisplayName("test_multipart_upload_small")
    @Tag("Check")
    @Tag("KSAN")
    // @Tag("파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인")
    public void test_multipart_upload_small()
    {
        Test.test_multipart_upload_small();
    }
    
    @Test
    @DisplayName("test_multipart_copy_small")
    @Tag("Copy")
    @Tag("KSAN")
    // @Tag("버킷a에서 버킷b로 멀티파트 복사 성공확인")
    public void test_multipart_copy_small()
    {
        Test.test_multipart_copy_small();
    }
    
    @Test
    @DisplayName("test_multipart_copy_invalid_range")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("범위설정을 잘못한 멀티파트 복사 실패 확인")
    public void test_multipart_copy_invalid_range()
    {
        Test.test_multipart_copy_invalid_range();
    }
    
    @Test
    @DisplayName("test_multipart_copy_without_range")
    @Tag("Range")
    @Tag("KSAN")
    // @Tag("범위를 지정한 멀티파트 복사 성공확인")
    public void test_multipart_copy_without_range()
    {
        Test.test_multipart_copy_without_range();
    }
    
    @Test
    @DisplayName("test_multipart_copy_special_names")
    @Tag("SpecialNames")
    @Tag("KSAN")
    // @Tag("특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인")
    public void test_multipart_copy_special_names()
    {
        Test.test_multipart_copy_special_names();
    }
    
    @Test
    @DisplayName("test_multipart_upload")
    @Tag("Put")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드 확인")
    public void test_multipart_upload()
    {
        Test.test_multipart_upload();
    }
    

    @Test
    @DisplayName("test_multipart_copy_versioned")
    @Tag("Copy")
    // @Tag("버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인")
    public void test_multipart_copy_versioned()
    {
        Test.test_multipart_copy_versioned();
    }
    
    @Test
    @DisplayName("test_multipart_upload_resend_part")
    @Tag("Duplicate")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인")
    public void test_multipart_upload_resend_part()
    {
        Test.test_multipart_upload_resend_part();
    }
    
    @Test
    @DisplayName("test_multipart_upload_multiple_sizes")
    @Tag("Put")
    @Tag("KSAN")
    // @Tag("한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인")
    public void test_multipart_upload_multiple_sizes()
    {
        Test.test_multipart_upload_multiple_sizes();
    }
    

    @Test
    @DisplayName("test_multipart_copy_multiple_sizes")
    @Tag("Copy")
    // @Tag("한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인")
    public void test_multipart_copy_multiple_sizes()
    {
        Test.test_multipart_copy_multiple_sizes();
    }
    
    @Test
    @DisplayName("test_multipart_upload_size_too_small")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인")
    public void test_multipart_upload_size_too_small()
    {
        Test.test_multipart_upload_size_too_small();
    }
    
    @Test
    @DisplayName("test_multipart_upload_contents")
    @Tag("Check")
    @Tag("KSAN")
    // @Tag("내용물을 채운 멀티파트 업로드 성공 확인")
    public void test_multipart_upload_contents()
    {
        Test.test_multipart_upload_contents();
    }
    
    @Test
    @DisplayName("test_multipart_upload_overwrite_existing_object")
    @Tag("OverWrite")
    @Tag("KSAN")
    // @Tag("업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인")
    public void test_multipart_upload_overwrite_existing_object()
    {
        Test.test_multipart_upload_overwrite_existing_object();
    }
    
    @Test
    @DisplayName("test_abort_multipart_upload")
    @Tag("Cancel")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드하는 도중 중단 성공 확인")
    public void test_abort_multipart_upload()
    {
        Test.test_abort_multipart_upload();
    }
    
    @Test
    @DisplayName("test_abort_multipart_upload_not_found")
    @Tag("ERROR")
    @Tag("KSAN")
    // @Tag("존재하지 않은 멀티파트 업로드 중단 실패 확인")
    public void test_abort_multipart_upload_not_found()
    {
        Test.test_abort_multipart_upload_not_found();
    }
    
    @Test
    @DisplayName("test_list_multipart_upload")
    @Tag("List")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드 중인 목록 확인")
    public void test_list_multipart_upload()
    {
        Test.test_list_multipart_upload();
    }
    

    @Test
    @DisplayName("test_multipart_upload_missing_part")
    @Tag("ERROR")
    // @Tag("업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인")
    public void test_multipart_upload_missing_part()
    {
        Test.test_multipart_upload_missing_part();
    }
    

    @Test
    @DisplayName("test_multipart_upload_incorrect_etag")
    @Tag("ERROR")
    // @Tag("잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인")
    public void test_multipart_upload_incorrect_etag()
    {
        Test.test_multipart_upload_incorrect_etag();
    }
    
    @Test
    @DisplayName("test_atomic_multipart_upload_write")
    @Tag("Overwrite")
    @Tag("KSAN")
    // @Tag("버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인")
    public void test_atomic_multipart_upload_write()
    {
        Test.test_atomic_multipart_upload_write();
    }
    
    @Test
    @DisplayName("test_multipart_upload_list")
    @Tag("List")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드 목록 확인")
    public void test_multipart_upload_list()
    {
        Test.test_multipart_upload_list();
    }
    
    @Test
    @DisplayName("test_abort_multipart_upload_list")
    @Tag("Cancel")
    @Tag("KSAN")
    // @Tag("멀티파트 업로드하는 도중 중단 성공 확인")
    public void test_abort_multipart_upload_list()
    {
        Test.test_abort_multipart_upload_list();
    }
    
}
