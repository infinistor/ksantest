package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class DeleteBucket {

    org.example.test.DeleteBucket Test = new org.example.test.DeleteBucket();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}
    
	@Test
	@DisplayName("test_bucket_delete_notexist")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인")
	public void test_bucket_delete_notexist()
    {
        Test.test_bucket_delete_notexist();
	}

	@Test
	@DisplayName("test_bucket_delete_nonempty")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인")
	public void test_bucket_delete_nonempty()
    {
        Test.test_bucket_delete_nonempty();
	}

	@Test
	@DisplayName("test_bucket_create_delete")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인")
	public void test_bucket_create_delete()
    {
        Test.test_bucket_create_delete();
    }
}
