package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SSE_S3 {

    org.example.test.SSE_S3 Test = new org.example.test.SSE_S3();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @DisplayName("test_sse_s3_encrypted_transfer_1b")
    @Tag("PutGet")
    // @Tag("1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")
    public void test_sse_s3_encrypted_transfer_1b()
    {
        Test.test_sse_s3_encrypted_transfer_1b();
    }

    @Test
    @DisplayName("test_sse_s3_encrypted_transfer_1kb")
    @Tag("PutGet")
    // @Tag("1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")
    public void test_sse_s3_encrypted_transfer_1kb()
    {
        Test.test_sse_s3_encrypted_transfer_1kb();
    }

    @Test
    @DisplayName("test_sse_s3_encrypted_transfer_1MB")
    @Tag("PutGet")
    // @Tag("1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")
    public void test_sse_s3_encrypted_transfer_1MB()
    {
        Test.test_sse_s3_encrypted_transfer_1MB();
    }

    @Test
    @DisplayName("test_sse_s3_encrypted_transfer_13b")
    @Tag("PutGet")
    // @Tag("13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")
    public void test_sse_s3_encrypted_transfer_13b()
    {
        Test.test_sse_s3_encrypted_transfer_13b();
    }

    @Test
    @DisplayName("test_sse_s3_encryption_method_head")
    @Tag("Metadata")
    // @Tag("SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인")
    public void test_sse_s3_encryption_method_head()
    {
        Test.test_sse_s3_encryption_method_head();
    }

    @Test
    @DisplayName("test_sse_s3_encryption_multipart_upload")
    @Tag("Multipart")
    // @Tag("멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인")
    public void test_sse_s3_encryption_multipart_upload()
    {
        Test.test_sse_s3_encryption_multipart_upload();
    }

    @Test
    @DisplayName("test_get_bucket_encryption")
    @Tag("encryption")
    // @Tag("버킷의 SSE-S3 설정 확인")
    public void test_get_bucket_encryption()
    {
        Test.test_get_bucket_encryption();
    }

    @Test
    @DisplayName("test_put_bucket_encryption")
    @Tag("encryption")
    @Tag("KSAN")
    // @Tag("버킷의 SSE-S3 설정이 가능한지 확인")
    public void test_put_bucket_encryption()
    {
        Test.test_put_bucket_encryption();
    }

    @Test
    @DisplayName("test_delete_bucket_encryption")
    @Tag("encryption")
    // @Tag("버킷의 SSE-S3 설정 삭제가 가능한지 확인")
    public void test_delete_bucket_encryption()
    {
        Test.test_delete_bucket_encryption();
    }

    @Test
    @DisplayName("test_put_bucket_encryption_and_object_set_check")
    @Tag("encryption")
    // @Tag("버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인")
    public void test_put_bucket_encryption_and_object_set_check()
    {
        Test.test_put_bucket_encryption_and_object_set_check();
    }

    @Test
    @DisplayName("test_copy_object_encryption_1kb")
    @Tag("CopyObject")
    // @Tag("버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인")
    public void test_copy_object_encryption_1kb()
    {
        Test.test_copy_object_encryption_1kb();
    }

    @Test
    @DisplayName("test_copy_object_encryption_256kb")
    @Tag("CopyObject")
    // @Tag("버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인")
    public void test_copy_object_encryption_256kb()
    {
        Test.test_copy_object_encryption_256kb();
    }

    @Test
    @DisplayName("test_copy_object_encryption_1mb")
    @Tag("CopyObject")
    // @Tag("버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인")
    public void test_copy_object_encryption_1mb()
    {
        Test.test_copy_object_encryption_1mb();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get()
    {
        Test.test_sse_s3_bucket_put_get();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get_v4")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get_v4()
    {
        Test.test_sse_s3_bucket_put_get_v4();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get_use_chunk_encoding")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get_use_chunk_encoding()
    {
        Test.test_sse_s3_bucket_put_get_use_chunk_encoding();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
    {
        Test.test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get_not_chunk_encoding")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get_not_chunk_encoding()
    {
        Test.test_sse_s3_bucket_put_get_not_chunk_encoding();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing")
    @Tag("PutGet")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
    {
        Test.test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_presignedurl_put_get")
    @Tag("PresignedURL")
    // @Tag("[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_presignedurl_put_get()
    {
        Test.test_sse_s3_bucket_presignedurl_put_get();
    }

    @Test
    @DisplayName("test_sse_s3_bucket_presignedurl_put_get_v4")
    @Tag("PresignedURL")
    // @Tag("[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_presignedurl_put_get_v4()
    {
        Test.test_sse_s3_bucket_presignedurl_put_get_v4();
    }

    @Test
    @DisplayName("test_sse_s3_get_object_many")
    @Tag("Get")
    // @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")
    public void test_sse_s3_get_object_many()
    {
        Test.test_sse_s3_get_object_many();
    }

    @Test
    @DisplayName("test_sse_s3_range_object_many")
    @Tag("Get")
    // @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")
    public void test_sse_s3_range_object_many()
    {
        Test.test_sse_s3_range_object_many();
    }

}
