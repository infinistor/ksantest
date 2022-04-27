/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SSE_S3 {

	org.example.test.SSE_S3 Test = new org.example.test.SSE_S3();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("PutGet")
	// @Tag("1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1b()
	{
		Test.test_sse_s3_encrypted_transfer_1b();
	}

	@Test
	@Tag("PutGet")
	// @Tag("1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1kb()
	{
		Test.test_sse_s3_encrypted_transfer_1kb();
	}

	@Test
	@Tag("PutGet")
	// @Tag("1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1MB()
	{
		Test.test_sse_s3_encrypted_transfer_1MB();
	}

	@Test
	@Tag("PutGet")
	// @Tag("13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_13b()
	{
		Test.test_sse_s3_encrypted_transfer_13b();
	}

	@Test
	@Tag("Metadata")
	// @Tag("SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	public void test_sse_s3_encryption_method_head()
	{
		Test.test_sse_s3_encryption_method_head();
	}

	@Test
	@Tag("Multipart")
	// @Tag("멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	public void test_sse_s3_encryption_multipart_upload()
	{
		Test.test_sse_s3_encryption_multipart_upload();
	}

	@Test
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정 확인
	public void test_get_bucket_encryption()
	{
		Test.test_get_bucket_encryption();
	}

	@Test
	@Tag("encryption")
	@Tag("KSAN")
	// @Tag("버킷의 SSE-S3 설정이 가능한지 확인
	public void test_put_bucket_encryption()
	{
		Test.test_put_bucket_encryption();
	}

	@Test
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정 삭제가 가능한지 확인
	public void test_delete_bucket_encryption()
	{
		Test.test_delete_bucket_encryption();
	}

	@Test
	@Tag("encryption")
	// @Tag("버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	public void test_put_bucket_encryption_and_object_set_check()
	{
		Test.test_put_bucket_encryption_and_object_set_check();
	}

	@Test
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1kb()
	{
		Test.test_copy_object_encryption_1kb();
	}

	@Test
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_256kb()
	{
		Test.test_copy_object_encryption_256kb();
	}

	@Test
	@Tag("CopyObject")
	// @Tag("버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1mb()
	{
		Test.test_copy_object_encryption_1mb();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get()
	{
		Test.test_sse_s3_bucket_put_get();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_v4()
	{
		Test.test_sse_s3_bucket_put_get_v4();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding()
	{
		Test.test_sse_s3_bucket_put_get_use_chunk_encoding();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
	{
		Test.test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding()
	{
		Test.test_sse_s3_bucket_put_get_not_chunk_encoding();
	}

	@Test
	@Tag("PutGet")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
	{
		Test.test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing();
	}

	@Test
	@Tag("PresignedURL")
	// @Tag("[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presignedurl_put_get()
	{
		Test.test_sse_s3_bucket_presignedurl_put_get();
	}

	@Test
	@Tag("PresignedURL")
	// @Tag("[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presignedurl_put_get_v4()
	{
		Test.test_sse_s3_bucket_presignedurl_put_get_v4();
	}

	@Test
	@Tag("Get")
	// @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_sse_s3_get_object_many()
	{
		Test.test_sse_s3_get_object_many();
	}

	@Test
	@Tag("Get")
	// @Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_sse_s3_range_object_many()
	{
		Test.test_sse_s3_range_object_many();
	}

	@Test
	@Tag( "Multipart")
	//SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_s3_encryption_multipart_copypart_upload()
	{
		Test.test_sse_s3_encryption_multipart_copypart_upload();
	}

	@Test
	@Tag( "Multipart")
	//SSE-S3 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void test_sse_s3_encryption_multipart_copy_many()
	{
		Test.test_sse_s3_encryption_multipart_copy_many();
	}
}
