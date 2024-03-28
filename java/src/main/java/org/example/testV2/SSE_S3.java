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
package org.example.testV2;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class SSE_S3 extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("SSE S3 SDK V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("SSE S3 SDK V2 End");
	}

	@Test
	@Tag("PutGet")
	//1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1b()
	{
		testEncryptionSSE_S3CustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	//1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1kb()
	{
		testEncryptionSSE_S3CustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	//1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1MB()
	{
		testEncryptionSSE_S3CustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	//13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_13b()
	{
		testEncryptionSSE_S3CustomerWrite(13);
	}

	@Test
	@Tag("Metadata")
	//SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	public void test_sse_s3_encryption_method_head()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var data = Utils.randomTextToLong(1000);
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");

		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		client.putObject(bucketName, key, createBody(data), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals("AES256", response.getSSEAlgorithm());
	}

	@Test
	@Tag("Multipart")
	//멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	public void test_sse_s3_encryption_multipart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadata));
		var uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, 5 * MainData.MB);
		var partETags = new ArrayList<PartETag>();
		int partNumber = 1;
		var data = "";
		for (var part : parts) {
			data += part;
			var partResPonse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(uploadId).withPartNumber(partNumber++).withInputStream(createBody(part))
					.withPartSize(part.length()));
			partETags.add(partResPonse.getPartETag());
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags));
		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());
		assertEquals(contentType, getResponse.getContentType());
		assertEquals("AES256", getResponse.getSSEAlgorithm());

		checkContentUsingRange(bucketName, key, data, MainData.MB);
		checkContentUsingRange(bucketName, key, data, 10 * MainData.MB);
		checkContentUsingRandomRange(bucketName, key, data, 100);
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정 확인
	public void test_get_bucket_encryption()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AmazonS3Exception.class, () -> client.getBucketEncryption(bucketName));
	}

	@Test
	@Tag("encryption")
	@Tag("KSAN")
	//버킷의 SSE-S3 설정이 가능한지 확인
	public void test_put_bucket_encryption()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정 삭제가 가능한지 확인
	public void test_delete_bucket_encryption()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(1, response.getServerSideEncryptionConfiguration().getRules().size());

		client.deleteBucketEncryption(bucketName);

		assertThrows(AmazonS3Exception.class, () -> client.getBucketEncryption(bucketName));
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
	public void test_put_bucket_encryption_and_object_set_check()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var keys = new ArrayList<>(Arrays.asList(new String[] { "for/bar", "test/" }));
		createObjectsToBody(bucketName, keys, "");

		var getHeadResponse = client.getObjectMetadata(bucketName, keys.get(1));
		assertEquals(SSEAlgorithm.AES256.toString(), getHeadResponse.getSSEAlgorithm());
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1kb()
	{
		testEncryptionSSE_S3Copy(1024);
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_256kb()
	{
		testEncryptionSSE_S3Copy(256 * 1024);
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1mb()
	{
		testEncryptionSSE_S3Copy(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_v4()
	{
		var bucketName = getNewBucket();
		var client = getClientV4(true);
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(true, false);
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(true, true);
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(false, false);
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(false, true);
		var data = Utils.randomTextToLong(1000);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var key = "bar";
		client.putObject(bucketName, key, data);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presigned_url_put_get()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(200, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(200, getResponse.getStatusLine().getStatusCode());

	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presigned_url_put_get_v4()
	{
		var bucketName = getNewBucket();
		var client = getClientV4(true);
		var key = "foo";

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(200, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(200, getResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_sse_s3_get_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		client.putObject(bucketName, key, data);
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_sse_s3_range_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSE_S3Config));

		client.putObject(bucketName, key, data);
		checkContentUsingRandomRange(bucketName, key, data, 100);
	}

	@Test
	@Tag("Multipart")
	//SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_s3_encryption_multipart_copypart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var sourceKey = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		//올바르게 업로드 되었는지 확인
		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObjectMetadata(bucketName, sourceKey);
		assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());
		assertEquals(contentType, getResponse.getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getSSEAlgorithm());

		checkContentUsingRange(bucketName, sourceKey, uploadData.getBody(), MainData.MB);

		// 멀티파트 복사
		var targetKey = "multipart_enc_copy";
		uploadData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey, size, metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, targetKey, uploadData.uploadId, uploadData.parts));

		//올바르게 복사 되었는지 확인
		checkCopyContentUsingRange(bucketName, sourceKey, bucketName, targetKey, MainData.MB);
	}

	@Test
	@Tag("Multipart")
	//SSE-S3 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void test_sse_s3_encryption_multipart_copy_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var sourceKey = "multipart_enc";
		var size = 10 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType(contentType);
		var body = new StringBuilder();

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, sourceKey, body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		uploadData = multipartCopy(client, bucketName, sourceKey, bucketName, TargetKey1, size, metadata);
		// 추가파츠 업로드
		uploadData = multipartUpload(client, bucketName, TargetKey1, size, uploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey1, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, TargetKey1, body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		uploadData = multipartCopy(client, bucketName, TargetKey1, bucketName, TargetKey2, size * 2, metadata);
		// 추가파츠 업로드
		uploadData = multipartUpload(client, bucketName, TargetKey2, size, uploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey2, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, TargetKey2, body.toString(), MainData.MB);
	}
	
	@Test
	@Tag("Retroactive")
	//sse-s3설정은 소급적용 되지 않음을 확인
	public void test_sse_s3_not_retroactive()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var data = Utils.randomTextToLong(1000);
		
		var putKey = "put";
		var copyKey = "copy";
		var multiKey = "multi";
		
		// 일반 파일 업로드
		client.putObject(bucketName, putKey, data);

		// 일반 파일 복사
		client.copyObject(bucketName, putKey, bucketName, copyKey);

		// 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, multiKey, 5 * MainData.MB, null);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, multiKey, uploadData.uploadId, uploadData.parts));

		//SSE-S3 설정
		var sseS3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(sseS3Config));

		//SSE-S3 설정이 올바르게 되었는지 확인
		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		//SSE-S3 설정이 적용되지 않은 파일들이 올바르게 복사되었는지 확인
		var getResponse = client.getObject(bucketName, putKey);
		var body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		getResponse = client.getObject(bucketName, copyKey);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);

		checkContentUsingRange(bucketName, multiKey, uploadData.body.toString(), MainData.MB);
		
		var putKey2 = "put2";
		var copyKey2 = "copy2";
		var multiKey2 = "multi2";
		var data2 = Utils.randomTextToLong(1000);
		client.putObject(bucketName, putKey2, data2);
		
		// 일반 파일 복사
		client.copyObject(bucketName, putKey, bucketName, copyKey2);

		// 멀티파트 업로드
		var uploadData2 = setupMultipartUpload(client, bucketName, multiKey2, 5 * MainData.MB, null);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, multiKey2, uploadData2.uploadId, uploadData2.parts));

		client.deleteBucketEncryption(bucketName);

		getResponse = client.getObject(bucketName, putKey2);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data2.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());

		getResponse = client.getObject(bucketName, copyKey2);
		body = getBody(getResponse.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSEAlgorithm());

		checkContentUsingRange(bucketName, multiKey2, uploadData2.body.toString(), MainData.MB);
	}
}
