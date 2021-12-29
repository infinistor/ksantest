package org.example.s3tests;

import com.amazonaws.SDKGlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class CSE {

    org.example.test.CSE Test = new org.example.test.CSE();

    @BeforeEach
    public void Init() {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
    }

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_cse_encrypted_transfer_1b")
    @Tag("PutGet")
    // @Tag("[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1b()
    {
        Test.test_cse_encrypted_transfer_1b();
    }

    @Test
    @DisplayName("test_cse_encrypted_transfer_1kb")
    @Tag("PutGet")
    // @Tag("[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1kb()
    {
        Test.test_cse_encrypted_transfer_1kb();
    }

    @Test
    @DisplayName("test_cse_encrypted_transfer_1MB")
    @Tag("PutGet")
    // @Tag("[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1MB()
    {
        Test.test_cse_encrypted_transfer_1MB();
    }

    @Test
    @DisplayName("test_cse_encrypted_transfer_13b")
    @Tag("PutGet")
    // @Tag("[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_13b()
    {
        Test.test_cse_encrypted_transfer_13b();
    }

    @Test
    @DisplayName("test_cse_encryption_method_head")
    @Tag("Metadata")
    // @Tag("[AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인 ")
    public void test_cse_encryption_method_head()
    {
        Test.test_cse_encryption_method_head();
    }

    @Test
    @DisplayName("test_cse_encryption_non_decryption")
    @Tag("ERROR")
    // @Tag("[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치")
    public void test_cse_encryption_non_decryption()
    {
        Test.test_cse_encryption_non_decryption();
    }

    @Test
    @DisplayName("test_cse_non_encryption_decryption")
    @Tag("ERROR")
    // @Tag("[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인")
    public void test_cse_non_encryption_decryption()
    {
        Test.test_cse_non_encryption_decryption();
    }

    @Test
    @DisplayName("test_cse_encryption_range_read")
    @Tag("RangeRead")
    // @Tag("[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공")
    public void test_cse_encryption_range_read()
    {
        Test.test_cse_encryption_range_read();
    }

    @Test
    @DisplayName("test_cse_encryption_multipart_upload")
    @Tag("Multipart")
    // @Tag("[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인")
    public void test_cse_encryption_multipart_upload()
    {
        Test.test_cse_encryption_multipart_upload();
    }

    @Test
    @DisplayName("test_cse_get_object_many")
    @Tag("Get")
    // @Tag("CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")
    public void test_cse_get_object_many()
    {
        Test.test_cse_get_object_many();
    }

    @Test
    @DisplayName("test_cse_range_object_many")
    @Tag("Get")
    // @Tag("CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")
    public void test_cse_range_object_many()
    {
        Test.test_cse_range_object_many();
    }
}
