package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ListObjectsVersions {

    org.example.test.ListObjectsVersions Test = new org.example.test.ListObjectsVersions();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@DisplayName("test_bucket_list_return_data_versioning")
	@Tag("Metadata")
	@Tag("KSAN")
	//@Tag("Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")
	public void test_bucket_list_return_data_versioning()
    {
        Test.test_bucket_list_return_data_versioning();
    }
}
