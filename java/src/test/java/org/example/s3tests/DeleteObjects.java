package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class DeleteObjects {

    org.example.test.DeleteObjects Test = new org.example.test.DeleteObjects();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_multi_object_delete")
    @Tag("ListObject")
    @Tag("KSAN")
    // @Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete()
    {
        Test.test_multi_object_delete();
    }

    @Test
    @DisplayName("test_multi_objectv2_delete")
    @Tag("ListObjectsV2")
    @Tag("KSAN")
    // @Tag("버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")
    public void test_multi_objectv2_delete()
    {
        Test.test_multi_objectv2_delete();
    }

    @Test
    @DisplayName("test_multi_object_delete")
    @Tag("quiet")
    @Tag("KSAN")
    // @Tag("quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")
    public void test_multi_object_delete_quiet()
    {
        Test.test_multi_object_delete_quiet();
    }

    @Test
    @DisplayName("test_directory_delete")
    @Tag("Directory")
    @Tag("KSAN")
    // @Tag("업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")
    public void test_directory_delete()
    {
        Test.test_directory_delete();
    }
}
